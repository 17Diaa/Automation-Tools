"""
17D AutoPilot — Flask backend.
Multi-instance carousel uploader. Each instance has its own config,
image folders, and output folder.

Carousel flow per instance:
  Slide 1: Original image from instances/<id>/Images/ (cropped 9:16)
  Slide 2: Same queue image — either ImageTemplate (media player) or plain 9:16 (config)
  Slide 3: Image from instances/<id>/Playlist/ (cropped 9:16)

On Vercel: set BLOB_READ_WRITE_TOKEN so images + config + cron state live in Blob.
"""

import os
import json

from dotenv import load_dotenv

load_dotenv()
import glob
import random
import tempfile
import threading
import time
import uuid
from flask import Flask, Response, has_request_context, request, jsonify, send_from_directory, redirect
from werkzeug.exceptions import HTTPException
from werkzeug.utils import secure_filename

from blob_client import (
    blob_enabled,
    blob_urls_are_public,
    put_bytes,
    list_blobs,
    delete_blobs,
    fetch_url_bytes,
    get_blob_by_pathname,
    get_json,
    put_json,
)
from image_processor import create_template, create_simple_9_16
from tiktok_client import upload_carousel as tiktok_upload, get_upload_status as upload_post_get_status
from github_client import commit_file as github_commit_file, github_enabled

app = Flask(__name__, static_folder="static")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INSTANCES_DIR = os.path.join(BASE_DIR, "instances")
GLOBAL_CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
CRON_STATE_PATH = os.path.join(BASE_DIR, "cron_state.json")
BLOB_META_GLOBAL = "meta/global_config.json"
BLOB_META_CRON = "meta/cron_state.json"


def _instance_config_blob_path(instance_id):
    return f"instances/{instance_id}/config.json"


# None = not probed yet; True/False after first write attempt (e.g. Vercel read-only FS).
_CRON_FILE_PERSIST_OK = None


def _running_on_vercel():
    return bool(os.environ.get("VERCEL") or os.environ.get("VERCEL_ENV"))


# Vercel serverless FS is usually read-only; creating ./instances here crashes the whole import → HTML 500.
try:
    os.makedirs(INSTANCES_DIR, exist_ok=True)
except OSError:
    pass

SUPPORTED_EXT = (".png", ".jpg", ".jpeg", ".webp", ".bmp")


@app.before_request
def _require_blob_on_vercel():
    """Vercel serverless FS is read-only without Blob; fail fast with JSON instead of 500 HTML."""
    if not request.path.startswith("/api"):
        return None
    if not _running_on_vercel():
        return None
    if blob_enabled():
        return None
    return (
        jsonify(
            {
                "error": "Trūksta BLOB_READ_WRITE_TOKEN: Vercel negali rašyti į diską. "
                "Storage → Blob → prijunk prie projekto ir Redeploy."
            }
        ),
        503,
    )


@app.errorhandler(Exception)
def _api_json_errors(e):
    """So /api/* never returns Vercel HTML 500 — frontend can parse JSON."""
    if isinstance(e, HTTPException):
        return e
    if has_request_context() and request.path.startswith("/api"):
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e) or type(e).__name__}), 500
    raise e


def _blob_inst_path(instance_id, subfolder, filename):
    return f"instances/{instance_id}/{subfolder}/{filename}"


def _content_type_for_ext(ext):
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
    }.get(ext.lower(), "application/octet-stream")


# ---------------------------------------------------------------------------
# Global config (instances list)
# ---------------------------------------------------------------------------

def save_global_config(cfg):
    if blob_enabled():
        put_json(BLOB_META_GLOBAL, cfg)
        return
    with open(GLOBAL_CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def _discover_instance_ids_from_blob():
    """Instance ids that have instances/<id>/config.json (source of truth on Blob)."""
    ids = set()
    for b in list_blobs(prefix="instances/", limit=1000):
        p = (b.get("pathname") or "").strip("/")
        parts = p.split("/")
        if len(parts) == 3 and parts[0] == "instances" and parts[2] == "config.json":
            ids.add(parts[1])
    return ids


def _reconcile_blob_instance_registry(gcfg):
    """
    Merge meta/global_config.json with real Blob instance configs.
    Fixes empty UI when config.json exists but registry was lost or wiped.
    """
    if not blob_enabled():
        return gcfg
    inst = gcfg.get("instances")
    if not isinstance(inst, list):
        inst = []
    seen = set()
    out = []
    for i in inst:
        if not isinstance(i, dict) or not i.get("id"):
            continue
        iid = i["id"]
        if iid in seen:
            continue
        seen.add(iid)
        out.append({"id": iid, "name": i.get("name", iid)})
    blob_ids = _discover_instance_ids_from_blob()
    changed = False
    for iid in sorted(blob_ids):
        if iid in seen:
            continue
        cfg = get_json(_instance_config_blob_path(iid))
        name = (cfg or {}).get("name", iid)
        out.append({"id": iid, "name": name})
        seen.add(iid)
        changed = True
    gcfg = dict(gcfg)
    gcfg["instances"] = out
    if changed:
        save_global_config(gcfg)
    return gcfg


def load_global_config():
    if blob_enabled():
        data = get_json(BLOB_META_GLOBAL)
        gcfg = dict(data) if isinstance(data, dict) else {}
        if not isinstance(gcfg.get("instances"), list):
            gcfg["instances"] = []
        # Do not write empty meta on every failed read — that overwrote a good registry.
        gcfg = _reconcile_blob_instance_registry(gcfg)
        if data is None and not gcfg.get("instances"):
            put_json(BLOB_META_GLOBAL, {"instances": []})
        return gcfg
    if not os.path.exists(GLOBAL_CONFIG_PATH):
        save_global_config({"instances": []})
    with open(GLOBAL_CONFIG_PATH, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Randomized upload intervals (anti-fixed-schedule)
# ---------------------------------------------------------------------------

def _random_wait_seconds():
    """Sleep duration until next upload cycle: uniform between min–max minutes + optional jitter."""
    try:
        min_m = float(os.environ.get("UPLOAD_RANDOM_MIN_MINUTES", "15"))
        max_m = float(os.environ.get("UPLOAD_RANDOM_MAX_MINUTES", "60"))
    except ValueError:
        min_m, max_m = 15.0, 60.0
    if max_m < min_m:
        min_m, max_m = max_m, min_m
    base = random.uniform(min_m, max_m) * 60.0
    try:
        jitter = float(os.environ.get("UPLOAD_RANDOM_JITTER_SECONDS", "120"))
    except ValueError:
        jitter = 120.0
    if jitter > 0:
        base += random.uniform(0, jitter)
    return base


def _stateless_cron_probability():
    """If cron_state cannot be saved (serverless), run each tick with this probability (~same average rate)."""
    try:
        min_m = float(os.environ.get("UPLOAD_RANDOM_MIN_MINUTES", "15"))
        max_m = float(os.environ.get("UPLOAD_RANDOM_MAX_MINUTES", "60"))
        tick = float(os.environ.get("CRON_TICK_MINUTES", "15"))
    except ValueError:
        min_m, max_m, tick = 15.0, 60.0, 15.0
    mean = (min_m + max_m) / 2.0
    if mean <= 0:
        return 0.5
    p = tick / mean
    return max(0.05, min(0.85, p))


def load_cron_state():
    if blob_enabled():
        data = get_json(BLOB_META_CRON)
        if not data:
            return {"next_run_epoch": 0.0}
        try:
            return {"next_run_epoch": float(data.get("next_run_epoch", 0))}
        except (TypeError, ValueError):
            return {"next_run_epoch": 0.0}
    if not os.path.isfile(CRON_STATE_PATH):
        return {"next_run_epoch": 0.0}
    try:
        with open(CRON_STATE_PATH, "r") as f:
            data = json.load(f)
        return {
            "next_run_epoch": float(data.get("next_run_epoch", 0)),
        }
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {"next_run_epoch": 0.0}


def _probe_cron_state_writable():
    global _CRON_FILE_PERSIST_OK
    if blob_enabled():
        _CRON_FILE_PERSIST_OK = True
        return True
    if _CRON_FILE_PERSIST_OK is not None:
        return _CRON_FILE_PERSIST_OK
    try:
        st = load_cron_state()
        with open(CRON_STATE_PATH, "w") as f:
            json.dump(st, f, indent=2)
        _CRON_FILE_PERSIST_OK = True
    except OSError:
        _CRON_FILE_PERSIST_OK = False
    return _CRON_FILE_PERSIST_OK


def save_cron_state(next_run_epoch, last_run_epoch=None):
    global _CRON_FILE_PERSIST_OK
    payload = {"next_run_epoch": float(next_run_epoch)}
    if last_run_epoch is not None:
        payload["last_run_epoch"] = float(last_run_epoch)
    if blob_enabled():
        try:
            put_json(BLOB_META_CRON, payload)
            _CRON_FILE_PERSIST_OK = True
            return True
        except Exception:
            _CRON_FILE_PERSIST_OK = False
            return False
    try:
        with open(CRON_STATE_PATH, "w") as f:
            json.dump(payload, f, indent=2)
        _CRON_FILE_PERSIST_OK = True
        return True
    except OSError:
        _CRON_FILE_PERSIST_OK = False
        return False


# ---------------------------------------------------------------------------
# Instance helpers
# ---------------------------------------------------------------------------

def get_instance_dir(instance_id):
    return os.path.join(INSTANCES_DIR, instance_id)


def get_instance_config_path(instance_id):
    return os.path.join(get_instance_dir(instance_id), "config.json")


def load_instance_config(instance_id):
    if blob_enabled():
        data = get_json(_instance_config_blob_path(instance_id))
        if data is None:
            raise FileNotFoundError(_instance_config_blob_path(instance_id))
        return data
    path = get_instance_config_path(instance_id)
    with open(path, "r") as f:
        return json.load(f)


def save_instance_config(instance_id, cfg):
    if blob_enabled():
        put_json(_instance_config_blob_path(instance_id), cfg)
        return
    path = get_instance_config_path(instance_id)
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)


DEFAULT_INSTANCE_CONFIG = {
    "accounts": [],
    "upload_interval_minutes": 60,
    "default_title": "",
    "default_caption": "",
    "image_index": 0,
    "blur_amount": 60,
    "artist_name": "17Diamonds",
    "song_title": "Summer Techno 2026",
    "use_media_player_slide": True,
}


def create_instance(name):
    instance_id = uuid.uuid4().hex[:8]
    if not blob_enabled():
        inst_dir = get_instance_dir(instance_id)
        for sub in ["Images", "Playlist", "Output"]:
            os.makedirs(os.path.join(inst_dir, sub), exist_ok=True)
    cfg = dict(DEFAULT_INSTANCE_CONFIG)
    cfg["name"] = name
    save_instance_config(instance_id, cfg)

    # Register in global config
    gcfg = load_global_config()
    gcfg["instances"].append({"id": instance_id, "name": name})
    save_global_config(gcfg)
    return instance_id


def delete_instance(instance_id):
    import shutil
    if blob_enabled():
        prefix = f"instances/{instance_id}/"
        urls = [b["url"] for b in list_blobs(prefix=prefix)]
        delete_blobs(urls)
    else:
        inst_dir = get_instance_dir(instance_id)
        if os.path.exists(inst_dir):
            shutil.rmtree(inst_dir)
    gcfg = load_global_config()
    gcfg["instances"] = [i for i in gcfg["instances"] if i["id"] != instance_id]
    save_global_config(gcfg)


# ---------------------------------------------------------------------------
# Image helpers (instance-scoped)
# ---------------------------------------------------------------------------

def list_images(folder):
    files = []
    for ext in SUPPORTED_EXT:
        files.extend(glob.glob(os.path.join(folder, f"*{ext}")))
        files.extend(glob.glob(os.path.join(folder, f"*{ext.upper()}")))
    return sorted(set(files))


def list_folder_basenames(instance_id, subfolder):
    if blob_enabled():
        prefix = f"instances/{instance_id}/{subfolder}/"
        basenames = []
        for b in list_blobs(prefix=prefix):
            p = b.get("pathname") or ""
            if not p.startswith(prefix):
                continue
            rest = p[len(prefix) :]
            if "/" in rest:
                continue
            ext = os.path.splitext(rest)[1].lower()
            if ext not in SUPPORTED_EXT:
                continue
            basenames.append(rest)
        return sorted(set(basenames))
    folder = os.path.join(get_instance_dir(instance_id), subfolder)
    return [os.path.basename(x) for x in list_images(folder)]


def resolve_listed_file(instance_id, subfolder, requested_name):
    """Absolute path if basename matches a real image file in that folder (no path traversal)."""
    if blob_enabled():
        return None
    basename_req = os.path.basename(requested_name or "")
    if not basename_req:
        return None
    folder = os.path.join(get_instance_dir(instance_id), subfolder)
    mapping = {os.path.basename(p): p for p in list_images(folder)}
    return mapping.get(basename_req)


def delete_listed_file(instance_id, subfolder, requested_name):
    basename_req = os.path.basename(requested_name or "")
    if not basename_req:
        return False
    if blob_enabled():
        pathname = _blob_inst_path(instance_id, subfolder, basename_req)
        b = get_blob_by_pathname(pathname)
        if not b:
            return False
        delete_blobs([b["url"]])
        return True
    path = resolve_listed_file(instance_id, subfolder, requested_name)
    if not path:
        return False
    os.remove(path)
    return True


def get_next_image(instance_id, advance=True, queue_offset=0):
    """
    Pick a carousel source image from Images/.

    Returns (source, cfg, basename) where source is a filesystem path (local) or bytes (Blob).

    image_index = which slot is "next" for real uploads (round-robin).
    queue_offset = for preview only: 0 = that next slot, 1 = one after, etc.

    If advance=True (TikTok upload / cron), increment image_index after reading.
    If advance=False (preview), leave image_index unchanged.
    """
    cfg = load_instance_config(instance_id)
    if blob_enabled():
        basenames = list_folder_basenames(instance_id, "Images")
        if not basenames:
            return None, cfg, None
        n = len(basenames)
        idx = cfg.get("image_index", 0) % n
        off = int(queue_offset) % n if n else 0
        pick = (idx + off) % n
        name = basenames[pick]
        pathname = _blob_inst_path(instance_id, "Images", name)
        b = get_blob_by_pathname(pathname)
        if not b:
            return None, cfg, None
        data = fetch_url_bytes(b["url"])
        if advance:
            cfg["image_index"] = (idx + 1) % n
            save_instance_config(instance_id, cfg)
        return data, cfg, name

    images_dir = os.path.join(get_instance_dir(instance_id), "Images")
    images = list_images(images_dir)
    if not images:
        return None, cfg, None
    n = len(images)
    idx = cfg.get("image_index", 0) % n
    off = int(queue_offset) % n if n else 0
    pick = (idx + off) % n
    image_path = images[pick]
    if advance:
        cfg["image_index"] = (idx + 1) % n
        save_instance_config(instance_id, cfg)
    return image_path, cfg, os.path.basename(image_path)


def get_playlist_image(instance_id):
    """Returns (source_path_or_bytes, basename_or_None)."""
    if blob_enabled():
        basenames = list_folder_basenames(instance_id, "Playlist")
        if not basenames:
            return None, None
        name = basenames[0]
        pathname = _blob_inst_path(instance_id, "Playlist", name)
        b = get_blob_by_pathname(pathname)
        if not b:
            return None, None
        return fetch_url_bytes(b["url"]), name
    playlist_dir = os.path.join(get_instance_dir(instance_id), "Playlist")
    images = list_images(playlist_dir)
    if not images:
        return None, None
    p = images[0]
    return p, os.path.basename(p)


def _repo_path_for_instance_file(instance_id, subfolder, filename):
    return "/".join(["instances", instance_id, subfolder, filename])


def save_binary_upload(instance_id, subfolder, filename, raw_bytes):
    """Write file under instance folder or Blob; optional GitHub mirror (local only)."""
    safe = secure_filename(filename) or "image.bin"
    ext = os.path.splitext(safe)[1].lower()
    if ext not in SUPPORTED_EXT:
        return None, "__skip_bad_ext__"
    if blob_enabled():
        pathname = _blob_inst_path(instance_id, subfolder, safe)
        put_bytes(pathname, raw_bytes, content_type=_content_type_for_ext(ext))
        return safe, None
    folder = os.path.join(get_instance_dir(instance_id), subfolder)
    os.makedirs(folder, exist_ok=True)
    dest = os.path.join(folder, safe)
    with open(dest, "wb") as f:
        f.write(raw_bytes)
    github_detail = None
    if github_enabled():
        ok, detail = github_commit_file(
            _repo_path_for_instance_file(instance_id, subfolder, safe),
            raw_bytes,
            f"Upload {subfolder}/{safe} (instance {instance_id})",
        )
        github_detail = "ok" if ok else detail
    return safe, github_detail


# ---------------------------------------------------------------------------
# Core: prepare carousel
# ---------------------------------------------------------------------------

def prepare_carousel(instance_id, advance_index=True, queue_offset=0):
    cfg = load_instance_config(instance_id)
    source, cfg, src_bn = get_next_image(
        instance_id,
        advance=advance_index,
        queue_offset=queue_offset,
    )
    if source is None:
        return None, "No images in Images/ folder", None

    playlist_src, _pl_bn = get_playlist_image(instance_id)
    if not playlist_src:
        return None, "No images in Playlist/ folder", None

    base_name = os.path.splitext(src_bn or "slide")[0]

    img1 = create_simple_9_16(source)
    if cfg.get("use_media_player_slide", True):
        img2 = create_template(
            source,
            title=cfg.get("song_title", "Summer Techno 2026"),
            artist=cfg.get("artist_name", "17Diamonds"),
            blur_amount=cfg.get("blur_amount", 60),
        )
    else:
        img2 = create_simple_9_16(source)
    img3 = create_simple_9_16(playlist_src)

    if blob_enabled():
        remote_urls = []
        paths = []
        for pil_img, slot in ((img1, 1), (img2, 2), (img3, 3)):
            fd, path = tempfile.mkstemp(suffix=".png")
            os.close(fd)
            pil_img.save(path)
            paths.append(path)
            out_name = f"{base_name}_slide{slot}.png"
            pathname = _blob_inst_path(instance_id, "Output", out_name)
            with open(path, "rb") as f:
                raw = f.read()
            info = put_bytes(pathname, raw, content_type="image/png")
            if blob_urls_are_public():
                remote_urls.append(info.get("url") or f"/instances/{instance_id}/output/{out_name}")
            else:
                remote_urls.append(f"/instances/{instance_id}/output/{out_name}")
        return paths, None, remote_urls

    output_dir = os.path.join(get_instance_dir(instance_id), "Output")
    os.makedirs(output_dir, exist_ok=True)
    img1_path = os.path.join(output_dir, f"{base_name}_slide1.png")
    img2_path = os.path.join(output_dir, f"{base_name}_slide2.png")
    img3_path = os.path.join(output_dir, f"{base_name}_slide3.png")
    img1.save(img1_path)
    img2.save(img2_path)
    img3.save(img3_path)
    return [img1_path, img2_path, img3_path], None, None


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def do_upload(instance_id, account):
    paths, err, _remote = prepare_carousel(instance_id)
    if err:
        return {"error": err}
    username = account.get("username", "")
    if not username:
        return {"error": "No username set"}
    cfg = load_instance_config(instance_id)
    try:
        return tiktok_upload(
            image_paths=paths,
            user=username,
            title=cfg.get("default_title", ""),
            caption=cfg.get("default_caption", ""),
        )
    finally:
        if blob_enabled():
            for p in paths:
                try:
                    os.unlink(p)
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Scheduler (local)
# ---------------------------------------------------------------------------

schedulers = {}  # instance_id -> {"running": bool, "thread": Thread}


def _scheduler_run_uploads(instance_id):
    """
    One upload pass for all enabled accounts (same as one instance slice of /api/cron).
    Used: immediately on scheduler Start, then on each background loop tick after waiting.
    """
    out = []
    try:
        cfg = load_instance_config(instance_id)
    except FileNotFoundError:
        return out
    for acct in cfg.get("accounts", []):
        if not acct.get("enabled", True):
            continue
        username = acct.get("username", "")
        try:
            result = do_upload(instance_id, acct)
            print(f"[Scheduler:{instance_id}] @{username or '?'}: {result}")
        except Exception as e:
            result = {"error": str(e)}
            print(f"[Scheduler:{instance_id}] Error: {e}")
        out.append({"account": username, "result": result})
    return out


def scheduler_loop(instance_id):
    """Wait (randomized) first, then upload — so Start's immediate run is not duplicated."""
    state = schedulers.get(instance_id, {})
    while state.get("running"):
        wait_sec = _random_wait_seconds()
        print(f"[Scheduler:{instance_id}] Next batch in ~{wait_sec / 60:.1f} min (randomized)")
        deadline = time.time() + wait_sec
        while time.time() < deadline and state.get("running"):
            time.sleep(1)
        if not state.get("running"):
            break
        _scheduler_run_uploads(instance_id)


def start_scheduler(instance_id):
    if instance_id in schedulers and schedulers[instance_id].get("running"):
        return
    state = {"running": True}
    schedulers[instance_id] = state
    t = threading.Thread(target=scheduler_loop, args=(instance_id,), daemon=True)
    state["thread"] = t
    t.start()


def stop_scheduler(instance_id):
    if instance_id in schedulers:
        schedulers[instance_id]["running"] = False


# ---------------------------------------------------------------------------
# Routes — Static
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/style.css")
def style():
    return send_from_directory("static", "style.css")


# ---------------------------------------------------------------------------
# Routes — Instances
# ---------------------------------------------------------------------------

@app.route("/api/instances", methods=["GET"])
def api_list_instances():
    return jsonify(load_global_config().get("instances", []))


@app.route("/api/instances", methods=["POST"])
def api_create_instance():
    name = request.json.get("name", "New Instance")
    instance_id = create_instance(name)
    return jsonify({"id": instance_id, "name": name})


@app.route("/api/instances/<iid>", methods=["DELETE"])
def api_delete_instance(iid):
    delete_instance(iid)
    return jsonify({"deleted": iid})


@app.route("/api/instances/<iid>/destroy", methods=["POST"])
def api_destroy_instance_post(iid):
    """POST alternative to DELETE instance (same proxies / hosting quirks)."""
    delete_instance(iid)
    return jsonify({"deleted": iid})


@app.route("/api/instances/<iid>/rename", methods=["POST"])
def api_rename_instance(iid):
    name = request.json.get("name", "")
    cfg = load_instance_config(iid)
    cfg["name"] = name
    save_instance_config(iid, cfg)
    gcfg = load_global_config()
    for inst in gcfg["instances"]:
        if inst["id"] == iid:
            inst["name"] = name
    save_global_config(gcfg)
    return jsonify({"id": iid, "name": name})


# ---------------------------------------------------------------------------
# Routes — Instance config
# ---------------------------------------------------------------------------

@app.route("/api/instances/<iid>/config", methods=["GET"])
def api_get_config(iid):
    return jsonify(load_instance_config(iid))


@app.route("/api/instances/<iid>/config", methods=["POST"])
def api_set_config(iid):
    data = request.json
    cfg = load_instance_config(iid)
    for key in ("upload_interval_minutes", "default_title", "default_caption",
                "blur_amount", "artist_name", "song_title", "use_media_player_slide"):
        if key in data:
            cfg[key] = data[key]
    save_instance_config(iid, cfg)
    return jsonify(cfg)


# ---------------------------------------------------------------------------
# Routes — Accounts
# ---------------------------------------------------------------------------

@app.route("/api/instances/<iid>/accounts", methods=["GET"])
def api_get_accounts(iid):
    return jsonify(load_instance_config(iid).get("accounts", []))


@app.route("/api/instances/<iid>/accounts", methods=["POST"])
def api_add_account(iid):
    data = request.json
    cfg = load_instance_config(iid)
    cfg.setdefault("accounts", []).append({
        "username": data.get("username", ""),
        "enabled": True,
    })
    save_instance_config(iid, cfg)
    return jsonify(cfg["accounts"])


@app.route("/api/instances/<iid>/accounts/remove", methods=["POST"])
def api_remove_account_post(iid):
    """POST alternative to DELETE (some edge / serverless proxies mishandle DELETE)."""
    data = request.get_json(silent=True) or {}
    try:
        idx = int(data.get("index", data.get("idx", -1)))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid index"}), 400
    cfg = load_instance_config(iid)
    accounts = list(cfg.get("accounts", []))
    if not (0 <= idx < len(accounts)):
        return jsonify({"error": "Index out of range"}), 400
    accounts.pop(idx)
    cfg["accounts"] = accounts
    save_instance_config(iid, cfg)
    return jsonify(accounts)


@app.route("/api/instances/<iid>/accounts/set-enabled", methods=["POST"])
def api_set_account_enabled_post(iid):
    """POST alternative to PATCH for toggling enabled."""
    data = request.get_json(silent=True) or {}
    try:
        idx = int(data.get("index", -1))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid index"}), 400
    enabled = bool(data.get("enabled", True))
    cfg = load_instance_config(iid)
    accounts = list(cfg.get("accounts", []))
    if not (0 <= idx < len(accounts)):
        return jsonify({"error": "Index out of range"}), 400
    accounts[idx] = dict(accounts[idx])
    accounts[idx]["enabled"] = enabled
    cfg["accounts"] = accounts
    save_instance_config(iid, cfg)
    return jsonify(accounts)


@app.route("/api/instances/<iid>/accounts/<int:idx>", methods=["DELETE"])
def api_delete_account(iid, idx):
    cfg = load_instance_config(iid)
    accounts = cfg.get("accounts", [])
    if 0 <= idx < len(accounts):
        accounts.pop(idx)
        cfg["accounts"] = accounts
        save_instance_config(iid, cfg)
    return jsonify(cfg["accounts"])


@app.route("/api/instances/<iid>/accounts/<int:idx>", methods=["PATCH"])
def api_update_account(iid, idx):
    data = request.json
    cfg = load_instance_config(iid)
    accounts = cfg.get("accounts", [])
    if 0 <= idx < len(accounts):
        for key in ("username", "enabled"):
            if key in data:
                accounts[idx][key] = data[key]
        cfg["accounts"] = accounts
        save_instance_config(iid, cfg)
    return jsonify(cfg["accounts"])


# ---------------------------------------------------------------------------
# Routes — Images
# ---------------------------------------------------------------------------

@app.route("/api/instances/<iid>/images", methods=["GET"])
def api_list_images(iid):
    cfg = load_instance_config(iid)
    images = list_folder_basenames(iid, "Images")
    playlist = list_folder_basenames(iid, "Playlist")
    return jsonify({
        "images_folder": images,
        "playlist_folder": playlist,
        "current_index": cfg.get("image_index", 0),
        "total_images": len(images),
        "playlist_count": len(playlist),
        "github_configured": github_enabled(),
        "blob_configured": blob_enabled(),
    })


@app.route("/api/instances/<iid>/images/reset", methods=["POST"])
def api_reset_index(iid):
    cfg = load_instance_config(iid)
    cfg["image_index"] = 0
    save_instance_config(iid, cfg)
    return jsonify({"image_index": 0})


@app.route("/api/instances/<iid>/images/upload", methods=["POST"])
def api_upload_images(iid):
    try:
        load_instance_config(iid)
    except FileNotFoundError:
        return jsonify({"error": "Unknown instance"}), 404
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files (use field name \"files\")"}), 400
    saved = []
    github_notes = []
    for f in files:
        if not f or not f.filename:
            continue
        data = f.read()
        if not data:
            continue
        result = save_binary_upload(iid, "Images", f.filename, data)
        if result[0] is None:
            if result[1] != "__skip_bad_ext__":
                github_notes.append(result[1])
            continue
        name, gh = result
        saved.append(name)
        if gh and gh != "ok":
            github_notes.append(gh)
    if not saved and not github_notes:
        return jsonify({"error": "No valid image files"}), 400
    return jsonify({
        "saved": saved,
        "github_enabled": github_enabled(),
        "github_warnings": github_notes or None,
    })


@app.route("/api/instances/<iid>/images/delete-one", methods=["POST"])
def api_delete_one_image(iid):
    try:
        load_instance_config(iid)
    except FileNotFoundError:
        return jsonify({"error": "Unknown instance"}), 404
    data = request.get_json(silent=True) or {}
    name = (data.get("filename") or "").strip()
    if not name:
        return jsonify({"error": "filename required"}), 400
    if not delete_listed_file(iid, "Images", name):
        return jsonify({"error": "Not found"}), 404
    return jsonify({"deleted": os.path.basename(name)})


@app.route("/api/instances/<iid>/playlist/upload", methods=["POST"])
def api_upload_playlist(iid):
    try:
        load_instance_config(iid)
    except FileNotFoundError:
        return jsonify({"error": "Unknown instance"}), 404
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files (use field name \"files\")"}), 400
    saved = []
    github_notes = []
    for f in files:
        if not f or not f.filename:
            continue
        data = f.read()
        if not data:
            continue
        result = save_binary_upload(iid, "Playlist", f.filename, data)
        if result[0] is None:
            if result[1] != "__skip_bad_ext__":
                github_notes.append(result[1])
            continue
        name, gh = result
        saved.append(name)
        if gh and gh != "ok":
            github_notes.append(gh)
    if not saved and not github_notes:
        return jsonify({"error": "No valid image files"}), 400
    return jsonify({
        "saved": saved,
        "github_enabled": github_enabled(),
        "github_warnings": github_notes or None,
    })


@app.route("/api/instances/<iid>/playlist/delete-one", methods=["POST"])
def api_delete_one_playlist(iid):
    try:
        load_instance_config(iid)
    except FileNotFoundError:
        return jsonify({"error": "Unknown instance"}), 404
    data = request.get_json(silent=True) or {}
    name = (data.get("filename") or "").strip()
    if not name:
        return jsonify({"error": "filename required"}), 400
    if not delete_listed_file(iid, "Playlist", name):
        return jsonify({"error": "Not found"}), 404
    return jsonify({"deleted": os.path.basename(name)})


# ---------------------------------------------------------------------------
# Routes — Preview / Upload
# ---------------------------------------------------------------------------

@app.route("/api/instances/<iid>/preview", methods=["POST"])
def api_preview(iid):
    # Preview does not advance image_index; queue_offset rotates which queue item you see.
    data = request.get_json(silent=True) or {}
    try:
        queue_offset = int(data.get("queue_offset", 0))
    except (TypeError, ValueError):
        queue_offset = 0

    queue_basenames = list_folder_basenames(iid, "Images")
    queue_total = len(queue_basenames)
    if queue_total:
        queue_offset %= queue_total

    paths, err, remote_urls = prepare_carousel(iid, advance_index=False, queue_offset=queue_offset)
    if err:
        return jsonify({"error": err}), 400
    names = [os.path.basename(p) for p in paths]
    if remote_urls:
        slide_urls = remote_urls
        for p in paths:
            try:
                os.unlink(p)
            except OSError:
                pass
    else:
        slide_urls = [f"/instances/{iid}/output/{n}" for n in names]
    cfg = load_instance_config(iid)
    if queue_basenames:
        head = cfg.get("image_index", 0) % queue_total
        pick = (head + queue_offset) % queue_total
        source_label = queue_basenames[pick]
    else:
        source_label = ""
    return jsonify({
        "slides": names,
        "slide_urls": slide_urls,
        "queue_total": queue_total,
        "queue_offset": queue_offset,
        "source_image": source_label,
        "message": f"Preview +{queue_offset}/{queue_total}: source «{source_label}»",
    })


@app.route("/api/instances/<iid>/upload", methods=["POST"])
def api_upload(iid):
    cfg = load_instance_config(iid)
    accounts = [a for a in cfg.get("accounts", []) if a.get("enabled", True)]
    if not accounts:
        return jsonify({"error": "No enabled accounts"}), 400
    results = []
    for acct in accounts:
        result = do_upload(iid, acct)
        results.append({"account": acct.get("username", ""), "result": result})
    return jsonify(results)


@app.route("/api/upload-post/status", methods=["GET"])
def api_upload_post_status():
    """Upload-Post async job status (request_id from upload response)."""
    rid = (request.args.get("request_id") or "").strip()
    if not rid:
        return jsonify({"error": "Missing request_id query parameter"}), 400
    out = upload_post_get_status(rid)
    return jsonify(out)


@app.route("/api/deployment-check", methods=["GET"])
def api_deployment_check():
    """Patikrink ar šis deploy mato env (be slaptų reikšmių)."""
    env_name = "UPLOAD_POST_API_KEY"
    up = (os.environ.get(env_name) or "").strip().strip('"').strip("'")
    # Po Vercel env pakeitimo būtinas Redeploy — kitaip funkcija vis dar senoje versijoje be kintamojo.
    return jsonify(
        {
            "upload_post_configured": bool(up),
            "upload_post_env_name_defined": env_name in os.environ,
            "upload_post_non_empty_length": len(up),
            "blob_configured": blob_enabled(),
            "on_vercel": bool(os.environ.get("VERCEL") or os.environ.get("VERCEL_ENV")),
            "background_scheduler_supported": _background_scheduler_supported(),
        }
    )


# --- Vercel Cron: upload for ALL instances ---

@app.route("/api/cron", methods=["GET"])
def api_cron():
    """
    Intended to be hit by a short-interval cron (e.g. every 15 min).
    Uses cron_state.json for next_run_epoch when the filesystem is writable;
    otherwise falls back to a random probability each call so average spacing
    stays near (min+max)/2 minutes between runs.

    On Vercel: set env CRON_SECRET — the platform sends Authorization: Bearer <CRON_SECRET>.
    If CRON_SECRET is unset (local dev), the route stays open.
    """
    cron_secret = (os.environ.get("CRON_SECRET") or "").strip()
    if cron_secret:
        auth = (request.headers.get("Authorization") or "").strip()
        if auth != f"Bearer {cron_secret}":
            return jsonify({"error": "Unauthorized cron"}), 401

    _probe_cron_state_writable()
    now = time.time()
    st = load_cron_state()
    next_run = float(st.get("next_run_epoch", 0))

    if _CRON_FILE_PERSIST_OK:
        if now < next_run:
            ws = int(max(0, next_run - now))
            print(
                f"[Cron] skip before_next_random_slot: wait ~{ws}s "
                f"(next_run_epoch={next_run:.0f}, blob_state={blob_enabled()})",
                flush=True,
            )
            return jsonify({
                "executed": False,
                "skipped": True,
                "reason": "before_next_random_slot",
                "next_run_epoch": next_run,
                "wait_seconds": ws,
            })
    else:
        p = _stateless_cron_probability()
        if random.random() > p:
            print(
                f"[Cron] skip stateless_probability_gate: p={p:.4f} (no persistent cron state)",
                flush=True,
            )
            return jsonify({
                "executed": False,
                "skipped": True,
                "reason": "stateless_probability_gate",
                "p": round(p, 4),
                "hint": "Filesystem not writable — using random chance per cron tick; set CRON_TICK_MINUTES to match Vercel schedule.",
            })

    gcfg = load_global_config()
    inst_count = len(gcfg.get("instances", []))
    print(f"[Cron] run: {inst_count} instance(s), uploading if accounts+assets ok", flush=True)
    all_results = []
    for inst in gcfg.get("instances", []):
        iid = inst["id"]
        try:
            cfg = load_instance_config(iid)
        except FileNotFoundError:
            continue
        accounts = [a for a in cfg.get("accounts", []) if a.get("enabled", True)]
        for acct in accounts:
            result = do_upload(iid, acct)
            all_results.append({
                "instance": inst["name"],
                "account": acct.get("username", ""),
                "result": result,
            })
            print(f"[Cron] {inst['name']}/@{acct.get('username','?')}: {result}")

    wait_sec = _random_wait_seconds()
    new_next = now + wait_sec
    save_cron_state(new_next, last_run_epoch=now)

    payload = {
        "executed": True,
        "results": all_results,
        "next_run_epoch": new_next,
        "next_in_minutes_approx": round(wait_sec / 60.0, 2),
        "persisted_next_run": _CRON_FILE_PERSIST_OK,
    }
    if not all_results:
        payload["message"] = "No enabled accounts in any instance"
        print("[Cron] executed but no uploads: no enabled accounts in any instance", flush=True)
    return jsonify(payload)


# ---------------------------------------------------------------------------
# Routes — Scheduler (local)
# ---------------------------------------------------------------------------

def _background_scheduler_supported():
    """Vercel serverless ends the process after each request — threads cannot survive."""
    return not _running_on_vercel()


@app.route("/api/instances/<iid>/scheduler/start", methods=["POST"])
def api_start_scheduler(iid):
    immediate = _scheduler_run_uploads(iid)
    if not _background_scheduler_supported():
        return jsonify(
            {
                "status": "run_once",
                "background_supported": False,
                "results": immediate,
                "message": (
                    "Įkėlimas vykdomas dabar (vienkartinis). Automatikai toliau naudokite Vercel Cron → /api/cron."
                    if immediate
                    else "Nėra įjungtų paskyrų šiai instancijai — nieko neįkelta."
                ),
            }
        )
    start_scheduler(iid)
    return jsonify(
        {
            "status": "running",
            "background_supported": True,
            "immediate_results": immediate,
            "message": (
                "Įkėlimas paleistas iš karto; fonas tęs po atsitiktinio laukimo."
                if immediate
                else "Foninis scheduler paleistas; nėra aktyvių paskyrų šiam įkėlimui."
            ),
        }
    )


@app.route("/api/instances/<iid>/scheduler/stop", methods=["POST"])
def api_stop_scheduler(iid):
    if not _background_scheduler_supported():
        return jsonify({"status": "unsupported", "background_supported": False})
    stop_scheduler(iid)
    return jsonify({"status": "stopped", "background_supported": True})


@app.route("/api/instances/<iid>/scheduler/status", methods=["GET"])
def api_scheduler_status(iid):
    if not _background_scheduler_supported():
        return jsonify({"running": False, "background_supported": False})
    running = schedulers.get(iid, {}).get("running", False)
    return jsonify({"running": running, "background_supported": True})


# ---------------------------------------------------------------------------
# Serve instance images (thumbnails in UI) + output slides
# ---------------------------------------------------------------------------

def _serve_instance_blob_file(iid, subfolder, filename):
    """Private Blob store: stream bytes via server (token). Public store: 302 to CDN."""
    safe = secure_filename(os.path.basename(filename)) or ""
    if not safe:
        return jsonify({"error": "Not found"}), 404
    b = get_blob_by_pathname(_blob_inst_path(iid, subfolder, safe))
    if not b:
        return jsonify({"error": "Not found"}), 404
    if blob_urls_are_public():
        return redirect(b["url"], code=302)
    body = fetch_url_bytes(b["url"])
    if not body:
        return jsonify({"error": "Not found"}), 404
    ct = _content_type_for_ext(os.path.splitext(safe)[1])
    return Response(body, mimetype=ct or "application/octet-stream")


@app.route("/instances/<iid>/images/<path:filename>")
def serve_instance_images_file(iid, filename):
    if blob_enabled():
        return _serve_instance_blob_file(iid, "Images", filename)
    path = resolve_listed_file(iid, "Images", filename)
    if not path:
        return jsonify({"error": "Not found"}), 404
    d = os.path.dirname(path)
    return send_from_directory(d, os.path.basename(path))


@app.route("/instances/<iid>/playlist/<path:filename>")
def serve_instance_playlist_file(iid, filename):
    if blob_enabled():
        return _serve_instance_blob_file(iid, "Playlist", filename)
    path = resolve_listed_file(iid, "Playlist", filename)
    if not path:
        return jsonify({"error": "Not found"}), 404
    d = os.path.dirname(path)
    return send_from_directory(d, os.path.basename(path))


@app.route("/instances/<iid>/output/<path:filename>")
def serve_output(iid, filename):
    if blob_enabled():
        return _serve_instance_blob_file(iid, "Output", filename)
    path = resolve_listed_file(iid, "Output", filename)
    if not path:
        return jsonify({"error": "Not found"}), 404
    d = os.path.dirname(path)
    return send_from_directory(d, os.path.basename(path))


if __name__ == "__main__":
    print("=== 17D AutoPilot ===")
    print(f"Instances: {INSTANCES_DIR}")
    print("http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)

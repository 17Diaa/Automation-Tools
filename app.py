"""
17D AutoPilot — Flask backend.
Multi-instance carousel uploader. Each instance has its own config,
image folders, and output folder.

Carousel flow per instance:
  Slide 1: Original image from instances/<id>/Images/ (cropped 9:16)
  Slide 2: Same image processed through ImageTemplate (media player overlay)
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
from flask import Flask, request, jsonify, send_from_directory, redirect
from werkzeug.utils import secure_filename

from blob_client import (
    blob_enabled,
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

# None = not probed yet; True/False after first write attempt (e.g. Vercel read-only FS).
_CRON_FILE_PERSIST_OK = None

os.makedirs(INSTANCES_DIR, exist_ok=True)

SUPPORTED_EXT = (".png", ".jpg", ".jpeg", ".webp", ".bmp")


@app.before_request
def _require_blob_on_vercel():
    """Vercel serverless FS is read-only without Blob; fail fast with JSON instead of 500 HTML."""
    if not request.path.startswith("/api"):
        return None
    if not os.environ.get("VERCEL"):
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

def load_global_config():
    if blob_enabled():
        data = get_json(BLOB_META_GLOBAL)
        if data is None:
            init = {"instances": []}
            put_json(BLOB_META_GLOBAL, init)
            return init
        return data
    if not os.path.exists(GLOBAL_CONFIG_PATH):
        save_global_config({"instances": []})
    with open(GLOBAL_CONFIG_PATH, "r") as f:
        return json.load(f)


def save_global_config(cfg):
    if blob_enabled():
        put_json(BLOB_META_GLOBAL, cfg)
        return
    with open(GLOBAL_CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


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


def _instance_config_blob_path(instance_id):
    return f"instances/{instance_id}/config.json"


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
    img2 = create_template(
        source,
        title=cfg.get("song_title", "Summer Techno 2026"),
        artist=cfg.get("artist_name", "17Diamonds"),
        blur_amount=cfg.get("blur_amount", 60),
    )
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
            remote_urls.append(info["url"])
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


def scheduler_loop(instance_id):
    state = schedulers.get(instance_id, {})
    while state.get("running"):
        cfg = load_instance_config(instance_id)
        for acct in cfg.get("accounts", []):
            if not acct.get("enabled", True):
                continue
            try:
                result = do_upload(instance_id, acct)
                print(f"[Scheduler:{instance_id}] @{acct.get('username','?')}: {result}")
            except Exception as e:
                print(f"[Scheduler:{instance_id}] Error: {e}")
        wait_sec = _random_wait_seconds()
        print(f"[Scheduler:{instance_id}] Next batch in ~{wait_sec / 60:.1f} min (randomized)")
        deadline = time.time() + wait_sec
        while time.time() < deadline and state.get("running"):
            time.sleep(1)


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
                "blur_amount", "artist_name", "song_title"):
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
            return jsonify({
                "executed": False,
                "skipped": True,
                "reason": "before_next_random_slot",
                "next_run_epoch": next_run,
                "wait_seconds": int(max(0, next_run - now)),
            })
    else:
        p = _stateless_cron_probability()
        if random.random() > p:
            return jsonify({
                "executed": False,
                "skipped": True,
                "reason": "stateless_probability_gate",
                "p": round(p, 4),
                "hint": "Filesystem not writable — using random chance per cron tick; set CRON_TICK_MINUTES to match Vercel schedule.",
            })

    gcfg = load_global_config()
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
    return jsonify(payload)


# ---------------------------------------------------------------------------
# Routes — Scheduler (local)
# ---------------------------------------------------------------------------

@app.route("/api/instances/<iid>/scheduler/start", methods=["POST"])
def api_start_scheduler(iid):
    start_scheduler(iid)
    return jsonify({"status": "running"})


@app.route("/api/instances/<iid>/scheduler/stop", methods=["POST"])
def api_stop_scheduler(iid):
    stop_scheduler(iid)
    return jsonify({"status": "stopped"})


@app.route("/api/instances/<iid>/scheduler/status", methods=["GET"])
def api_scheduler_status(iid):
    running = schedulers.get(iid, {}).get("running", False)
    return jsonify({"running": running})


# ---------------------------------------------------------------------------
# Serve instance images (thumbnails in UI) + output slides
# ---------------------------------------------------------------------------

@app.route("/instances/<iid>/images/<path:filename>")
def serve_instance_images_file(iid, filename):
    if blob_enabled():
        safe = secure_filename(filename) or ""
        b = get_blob_by_pathname(_blob_inst_path(iid, "Images", safe))
        if not b:
            return jsonify({"error": "Not found"}), 404
        return redirect(b["url"], code=302)
    path = resolve_listed_file(iid, "Images", filename)
    if not path:
        return jsonify({"error": "Not found"}), 404
    d = os.path.dirname(path)
    return send_from_directory(d, os.path.basename(path))


@app.route("/instances/<iid>/playlist/<path:filename>")
def serve_instance_playlist_file(iid, filename):
    if blob_enabled():
        safe = secure_filename(filename) or ""
        b = get_blob_by_pathname(_blob_inst_path(iid, "Playlist", safe))
        if not b:
            return jsonify({"error": "Not found"}), 404
        return redirect(b["url"], code=302)
    path = resolve_listed_file(iid, "Playlist", filename)
    if not path:
        return jsonify({"error": "Not found"}), 404
    d = os.path.dirname(path)
    return send_from_directory(d, os.path.basename(path))


@app.route("/instances/<iid>/output/<path:filename>")
def serve_output(iid, filename):
    if blob_enabled():
        safe = secure_filename(filename) or ""
        b = get_blob_by_pathname(_blob_inst_path(iid, "Output", safe))
        if not b:
            return jsonify({"error": "Not found"}), 404
        return redirect(b["url"], code=302)
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

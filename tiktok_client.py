"""
TikTok photo carousel via Upload-Post POST /api/upload_photos.
OpenAPI: https://docs.upload-post.com/openapi.json

Official Python SDK does not attach TikTok ``post_mode`` / ``privacy_level`` for *photo*
uploads (only for video). The API schema allows them on ``upload_photos``; we send a
multipart request that matches the documented fields so ``auto_add_music`` is applied
for DIRECT_POST photo carousels.
"""

import mimetypes
import os
from pathlib import Path

import requests
from upload_post import UploadPostClient, UploadPostError

UPLOAD_PHOTOS_URL = "https://api.upload-post.com/api/upload_photos"


def _get_client():
    key = (os.environ.get("UPLOAD_POST_API_KEY") or "").strip()
    if not key:
        return None, (
            "Nenurodytas UPLOAD_POST_API_KEY. Dashboard → API key, "
            "arba PowerShell: $env:UPLOAD_POST_API_KEY='...'"
        )
    return UploadPostClient(api_key=key), None


def _api_headers():
    key = (os.environ.get("UPLOAD_POST_API_KEY") or "").strip()
    return {
        "Authorization": f"Apikey {key}",
        "User-Agent": "upload-post-python-client/2.0.0",
        "X-Upload-Post-Source": "17d-autopilot-tiktok-photos",
    }


def _finalize_upload_response(response):
    """Normalize Upload-Post JSON the same way for SDK and raw HTTP."""
    if not isinstance(response, dict):
        return {
            "status": "unknown",
            "message": "Netikėtas API atsakymas",
            "response": response,
        }

    if response.get("success") is False:
        msg = (
            response.get("message")
            or response.get("detail")
            or response.get("error")
            or "Upload-Post: success=false"
        )
        return {"error": msg, "response": response}

    request_id = response.get("request_id")
    msg = response.get("message") or ""

    if request_id:
        return {
            "status": "queued",
            "request_id": request_id,
            "message": msg or "Įkėlimas priimtas fone — patikrink TikTok po kelių minučių arba statusą.",
            "response": response,
        }

    return {
        "status": "ok",
        "message": msg or "Įkelta",
        "request_id": request_id,
        "response": response,
    }


def _upload_photos_tiktok_multipart(image_paths, username, title_clean, caption, auto_add_music: bool):
    """
    Multipart POST exactly as OpenAPI ``/upload_photos`` for TikTok:
    post_mode DIRECT_POST, privacy_level, auto_add_music, photo_cover_index,
    tiktok_title / tiktok_description.
    """
    key = (os.environ.get("UPLOAD_POST_API_KEY") or "").strip()
    if not key:
        return {"error": "Nenurodytas UPLOAD_POST_API_KEY"}

    try:
        cover_idx = int(os.environ.get("UPLOAD_POST_PHOTO_COVER_INDEX", "0") or "0")
    except ValueError:
        cover_idx = 0

    data = [
        ("user", username),
        ("title", title_clean),
        ("tiktok_title", title_clean),
        ("platform[]", "tiktok"),
        ("post_mode", "DIRECT_POST"),
        ("privacy_level", "PUBLIC_TO_EVERYONE"),
        ("photo_cover_index", str(max(0, cover_idx))),
        ("auto_add_music", "true" if auto_add_music else "false"),
    ]

    cap = (caption or "").strip()
    if cap:
        data.append(("description", cap))
        data.append(("tiktok_description", cap))

    files = []
    opened = []
    try:
        for photo in image_paths:
            p = Path(photo)
            if not p.exists():
                return {"error": f"Photo file not found: {p}"}
            fh = p.open("rb")
            opened.append(fh)
            mime, _ = mimetypes.guess_type(str(p))
            if not mime:
                mime = "image/png" if p.suffix.lower() == ".png" else "application/octet-stream"
            files.append(("photos[]", (p.name, fh, mime)))

        resp = requests.post(
            UPLOAD_PHOTOS_URL,
            headers=_api_headers(),
            data=data,
            files=files,
            timeout=300,
        )

        try:
            body = resp.json()
        except ValueError:
            return {
                "error": f"Upload-Post HTTP {resp.status_code}: {resp.text[:800]}",
            }

        if resp.status_code >= 400:
            msg = (
                body.get("message")
                or body.get("detail")
                or body.get("error")
                or resp.text[:500]
            )
            return {"error": f"HTTP {resp.status_code}: {msg}", "response": body}

        return _finalize_upload_response(body)
    finally:
        for fh in opened:
            try:
                fh.close()
            except OSError:
                pass


def upload_carousel(image_paths, user, title="", caption=""):
    """
    Upload photo carousel to TikTok via Upload-Post (documented multipart fields).

    Env:
        UPLOAD_POST_AUTO_MUSIC — default true; false / 0 / no / off = no auto sound.
        UPLOAD_POST_PHOTO_COVER_INDEX — 0-based cover slide index (default 0).
    """
    username = (user or "").strip()
    if not username:
        return {"error": "Nenurodytas Upload-Post profilis (user)"}

    title_clean = (title or "")[:90]

    auto_music = os.environ.get("UPLOAD_POST_AUTO_MUSIC", "true").strip().lower()
    auto_add_music = auto_music not in ("0", "false", "no", "off")

    return _upload_photos_tiktok_multipart(
        list(image_paths),
        username,
        title_clean,
        caption or "",
        auto_add_music,
    )


def get_upload_status(request_id):
    """Async įkėlimo būsena pagal request_id (GET /uploadposts/status)."""
    client, init_err = _get_client()
    if init_err:
        return {"error": init_err}
    rid = (request_id or "").strip()
    if not rid:
        return {"error": "Nenurodytas request_id"}
    try:
        return client.get_status(request_id=rid)
    except UploadPostError as e:
        return {"error": str(e)}


def list_users():
    try:
        client, init_err = _get_client()
        if init_err:
            return {"error": init_err}
        return client.list_users()
    except UploadPostError as e:
        return {"error": str(e)}

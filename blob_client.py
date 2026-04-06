"""
Vercel Blob via HTTP (same contract as @vercel/blob SDK).
Env: BLOB_READ_WRITE_TOKEN (required). Optional: BLOB_ACCESS=public|private (default private — matches Vercel „Private“ store).
      If the store is Public, set BLOB_ACCESS=public for direct CDN URLs + no image proxy.
Docs: https://vercel.com/docs/storage/vercel-blob/using-blob-sdk
"""

import json
import os
import urllib.parse
from typing import Any, Dict, List, Optional

import requests

BLOB_API_BASE = os.environ.get("VERCEL_BLOB_API_URL", "https://vercel.com/api/blob").rstrip("/")
API_VERSION = os.environ.get("VERCEL_BLOB_API_VERSION", "12")


def _strip_env(s: str) -> str:
    return (s or "").strip().strip('"').strip("'")


def _token() -> str:
    return _strip_env(os.environ.get("BLOB_READ_WRITE_TOKEN", ""))


def blob_enabled() -> bool:
    return bool(_token())


def _access() -> str:
    """Must match store type: private store → 'private' only (else Blob API 400)."""
    a = _strip_env(os.environ.get("BLOB_ACCESS", "")).lower()
    if not a:
        return "private"
    return a if a in ("public", "private") else "private"


def blob_urls_are_public() -> bool:
    """If False, browsers must load blobs via app routes (server proxies with token)."""
    return _access() == "public"


def _headers(**extra: str) -> Dict[str, str]:
    h = {
        "Authorization": f"Bearer {_token()}",
        "x-api-version": API_VERSION,
    }
    h.update(extra)
    return h


def put_bytes(pathname: str, data: bytes, content_type: Optional[str] = None) -> Dict[str, Any]:
    """Upload or overwrite blob. Returns SDK-shaped JSON (url, pathname, ...)."""
    qs = urllib.parse.urlencode({"pathname": pathname})
    url = f"{BLOB_API_BASE}/?{qs}"
    hdrs = _headers(
        **{
            "x-vercel-blob-access": _access(),
            "x-allow-overwrite": "1",
            "x-add-random-suffix": "0",
            "x-content-length": str(len(data)),
        }
    )
    if content_type:
        hdrs["x-content-type"] = content_type
    r = requests.put(url, data=data, headers=hdrs, timeout=180)
    if not r.ok:
        try:
            detail = r.json()
        except Exception:
            detail = r.text[:500]
        raise RuntimeError(f"Blob put failed {r.status_code}: {detail}")
    return r.json()


def list_blobs(prefix: str = "", limit: int = 1000) -> List[Dict[str, Any]]:
    """All blobs matching prefix (paginated)."""
    out: List[Dict[str, Any]] = []
    cursor = None
    while True:
        params: Dict[str, str] = {"limit": str(limit)}
        if prefix:
            params["prefix"] = prefix
        if cursor:
            params["cursor"] = cursor
        qs = urllib.parse.urlencode(params)
        r = requests.get(
            f"{BLOB_API_BASE}/?{qs}",
            headers=_headers(),
            timeout=60,
        )
        if not r.ok:
            raise RuntimeError(f"Blob list failed {r.status_code}: {r.text[:400]}")
        data = r.json()
        out.extend(data.get("blobs") or [])
        if not data.get("hasMore"):
            break
        cursor = data.get("cursor")
        if not cursor:
            break
    return out


def delete_blobs(urls: List[str]) -> None:
    if not urls:
        return
    r = requests.post(
        f"{BLOB_API_BASE}/delete",
        headers={**_headers(), "content-type": "application/json"},
        data=json.dumps({"urls": urls}),
        timeout=120,
    )
    if not r.ok:
        raise RuntimeError(f"Blob delete failed {r.status_code}: {r.text[:400]}")


def fetch_url_bytes(blob_url: str) -> bytes:
    """Download blob bytes (public URL works without auth; private needs token)."""
    headers = {}
    if _access() == "private":
        headers["Authorization"] = f"Bearer {_token()}"
    r = requests.get(blob_url, headers=headers, timeout=120)
    if r.status_code == 404:
        return b""
    r.raise_for_status()
    return r.content


def get_blob_by_pathname(pathname: str) -> Optional[Dict[str, Any]]:
    """Return one blob descriptor {pathname, url, ...} or None."""
    if "/" not in pathname:
        return None
    prefix = pathname.rsplit("/", 1)[0] + "/"
    for b in list_blobs(prefix=prefix):
        if b.get("pathname") == pathname:
            return b
    return None


def get_json(pathname: str) -> Optional[Any]:
    """Read JSON from Blob with retry (handles eventual consistency after overwrite)."""
    import time as _time
    for attempt in range(3):
        b = get_blob_by_pathname(pathname)
        if not b:
            if attempt < 2:
                _time.sleep(0.3)
                continue
            return None
        try:
            content = fetch_url_bytes(b["url"])
            if not content:
                if attempt < 2:
                    _time.sleep(0.3)
                    continue
                return None
            return json.loads(content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            if attempt < 2:
                _time.sleep(0.3)
                continue
            return None
    return None


# In-memory cache of the last written JSON per pathname (within one request/process).
_put_json_cache: Dict[str, Any] = {}


def put_json(pathname: str, obj: Any) -> Dict[str, Any]:
    raw = json.dumps(obj, indent=2).encode("utf-8")
    result = put_bytes(pathname, raw, content_type="application/json")
    _put_json_cache[pathname] = obj
    return result


def get_json_cached(pathname: str) -> Optional[Any]:
    """Return cached version if we wrote it in this process, else read from Blob."""
    if pathname in _put_json_cache:
        return _put_json_cache[pathname]
    return get_json(pathname)

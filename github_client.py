"""
Optional: commit files to a GitHub repo (Contents API).
Set GITHUB_TOKEN and GITHUB_REPO (owner/repo). Optional GITHUB_BRANCH (default main).
"""

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request


def _creds():
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPO", "").strip()
    branch = os.environ.get("GITHUB_BRANCH", "main").strip() or "main"
    if not token or not repo or "/" not in repo:
        return None
    owner, name = repo.split("/", 1)
    if not owner or not name:
        return None
    return token, owner, name, branch


def github_enabled():
    return _creds() is not None


def _request(method, url, token, data=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "17d-autopilot",
    }
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        try:
            err_json = json.loads(err_body)
            msg = err_json.get("message", err_body)
        except json.JSONDecodeError:
            msg = err_body or str(e)
        return e.code, {"message": msg, "_raw": err_body}


def commit_file(repo_relative_path: str, content: bytes, message: str):
    """
    Create or update a file in the repo. repo_relative_path uses forward slashes.
    Returns (ok: bool, detail: str).
    """
    c = _creds()
    if not c:
        return False, "GitHub not configured (set GITHUB_TOKEN and GITHUB_REPO)"
    token, owner, repo, branch = c
    path = repo_relative_path.strip("/").replace("\\", "/")
    api = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

    status, existing = _request("GET", f"{api}?ref={urllib.parse.quote(branch)}", token)
    sha = None
    if status == 200 and isinstance(existing, dict):
        sha = existing.get("sha")

    body = {
        "message": message,
        "content": base64.b64encode(content).decode("ascii"),
        "branch": branch,
    }
    if sha:
        body["sha"] = sha

    status_put, result = _request("PUT", api, token, body)
    if status_put in (200, 201):
        return True, result.get("commit", {}).get("html_url", "committed")
    msg = result.get("message", str(result)) if isinstance(result, dict) else str(result)
    return False, msg

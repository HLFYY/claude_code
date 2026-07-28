"""
law.wkinfo.com.cn login -- plain username/password, no encryption, no captcha.

Located by static analysis of main.<hash>.js (not guessed): the Angular
AccountService.login() does:

    this.http.post(API_URL + "/csi/account/validate/ex",
                    {username: t.username, password: t.password}, ...)

i.e. POST /csi/account/validate/ex with a plaintext JSON body. On success the
response body IS the full user profile, and the Set-Cookie header carries the
session (connect.sid) -- that cookie is all downstream authenticated requests
(e.g. /csi/search) need. Logout is GET /api/logout.

Session caching: this account enforces a single concurrent session
server-side -- logging in again while an earlier session is still alive gets
rejected with {"code":"C_002_001","message":"用户并发超标"}. So rather than
logging in every run, get_session() caches cookies + connect.sid's own
Expires to sessions/<username>.json and reuses them; it only hits the real
login endpoint when there's no cache, the cached cookie's stated expiry has
passed, or GET /api/autoLogin says the cookie isn't actually valid anymore
(server can invalidate a session before its cookie's Expires, e.g. someone
else logging in and kicking it, so the stated expiry alone isn't trustworthy).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import requests

BASE = "https://law.wkinfo.com.cn"
SESSIONS_DIR = Path(__file__).parent / "sessions"

HEADERS = {
    "content-type": "application/json;charset=UTF-8",
    "accept": "*/*",
    "origin": BASE,
    "referer": BASE + "/",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}


def _cache_path(username: str) -> Path:
    return SESSIONS_DIR / f"{username}.json"


def _cookie_expiry(session: requests.Session) -> float | None:
    """Earliest Expires among the session's cookies (unix timestamp), or None
    if any cookie has no expiry (session cookie)."""
    expirations = [c.expires for c in session.cookies if c.expires]
    return min(expirations) if expirations else None


def login(username: str, password: str, session: requests.Session | None = None) -> tuple[requests.Session, dict]:
    """POST /csi/account/validate/ex. Returns (session, profile) on success;
    raises RuntimeError with the server's own error body on failure (wrong
    password, or C_002_001 if a previous session on this account is still alive).
    """
    session = session or requests.Session()
    resp = session.post(
        f"{BASE}/csi/account/validate/ex",
        headers=HEADERS,
        json={"username": username, "password": password},
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"login failed: {resp.status_code} {resp.text}")
    return session, resp.json()


def logout(session: requests.Session) -> dict:
    """GET /api/logout -- frees this session's concurrency slot on the account."""
    resp = session.get(f"{BASE}/api/logout", headers=HEADERS, timeout=15)
    return resp.json()


def is_session_valid(session: requests.Session) -> bool:
    """GET /api/autoLogin -- {"login": false} if the cookie's no longer authenticated."""
    resp = session.get(f"{BASE}/api/autoLogin", headers=HEADERS, timeout=15)
    return resp.status_code == 200 and resp.json().get("login") is True


def save_session(username: str, session: requests.Session, profile: dict) -> None:
    SESSIONS_DIR.mkdir(exist_ok=True)
    data = {
        "cookies": requests.utils.dict_from_cookiejar(session.cookies),
        "expires_at": _cookie_expiry(session),
        "profile": profile,
        "saved_at": time.time(),
    }
    _cache_path(username).write_text(json.dumps(data, ensure_ascii=False, indent=2))


def load_cached_session(username: str) -> tuple[requests.Session, dict] | None:
    path = _cache_path(username)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    if data.get("expires_at") and data["expires_at"] < time.time():
        return None  # cookie's own Expires has passed
    session = requests.Session()
    session.cookies.update(data["cookies"])
    if not is_session_valid(session):
        return None  # server invalidated it before the cookie's stated expiry
    return session, data["profile"]


def get_session(username: str, password: str, force: bool = False) -> tuple[requests.Session, dict]:
    """Read the cached session if it's still valid; otherwise log in for real
    and cache the result. This is the function to actually call."""
    if not force:
        cached = load_cached_session(username)
        if cached is not None:
            return cached
    session, profile = login(username, password)
    save_session(username, session, profile)
    return session, profile


if __name__ == "__main__":
    username, password = "1558109546@qq.com", "315128abc"
    session, profile = get_session(username, password)
    print("userEmail:", profile.get("userEmail"), "| telephone:", profile.get("telephone"))
    print("cookies:", session.cookies.get_dict())

    body = {
        "query": {"queryString": "simple:((刑法))", "filterDates": [], "filterQueries": []},
        "searchScope": {"treeNodeIds": []},
        "relatedIndexQueries": [],
        "sortOrderList": [{"sortKey": "score", "sortDirection": "DESC"}],
        "pageInfo": {"limit": 5, "offset": 0},
        "chargingInfo": {"useBalance": True},
        "otherOptions": {
            "requireLanguage": "cn", "relatedIndexEnabled": True, "groupEnabled": False,
            "smartEnabled": True, "buy": False, "summaryLengthLimit": 100, "synonymEnabled": True,
            "advanced": False, "isHideBigLib": 0, "relatedIndexFetchRows": 5, "proximateCourtID": "",
            "module": "", "correctEnabled": True, "mappingEnabled": True, "webSearchEnabled": True,
            "defaultSearch": False, "rankKeyword": "",
        },
        "indexId": "law.legislation",
    }
    r = session.post(f"{BASE}/csi/search", headers=HEADERS, json=body, timeout=15)
    docs = r.json().get("documentList", [])
    print(f"search: {len(docs)} results")
    for d in docs[:3]:
        print(" -", d.get("title", "")[:60])

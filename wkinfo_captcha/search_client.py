"""
The actual /csi/search and /csi/document/{id}/html calls (shapes taken from
wkinfo.py and real capture), wrapped so every call goes through
scheduler.dispatch() (pick an account with quota, ensure login) and
quota_tracker (reserve before calling, sync to "exhausted" the moment the
server itself says so) and gets recorded via request_logger.

Confirmed real quota-exceeded shape (from a real 浏览/detail hit):
    {"code":"E_010_015","message":"已达该栏目当日浏览最大量，请24小时之后再进行浏览。"}
The matching search-side code hasn't actually been observed yet (only its
message text, from the site's own i18n strings), so _is_quota_exceeded()
checks the confirmed code AND falls back to matching either message's text --
covers both without needing to guess the second code.

Also worth knowing about (found in the same i18n dump, not yet handled here
-- see HANDOFF.md "风控信号" section): this site has separate per-second and
per-minute rate limiting at session/user/IP/gateway level (error keys
G_USERS_PER_SECOND_OVERWEIGHT etc.) with a progressive
warn-once/warn-twice/hard-block escalation (CURRENT_*_IS_RESTRICTED_ACCESS),
plus a per-column daily *download* limit (COLUMN_HAS_REACHED) distinct from
the search/browse ones this module handles. None of that is quota in the
sense this module deals with -- it's the actual anti-bot layer, out of scope
for this pass.
"""
from __future__ import annotations

import requests

import document_store
import quota_tracker
import request_logger
import scheduler

BASE = "https://law.wkinfo.com.cn"

HEADERS = {
    "content-type": "application/json;charset=UTF-8",
    "accept": "application/json, text/plain, */*",
    "origin": BASE,
    "referer": BASE + "/",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}

_QUOTA_EXCEEDED_CODES = {"E_010_015"}  # confirmed: detail/browse quota
_QUOTA_MESSAGES = ("已达该栏目当日搜索最大量", "已达该栏目当日浏览最大量")

DEFAULT_SORT = [{"sortKey": "score", "sortDirection": "DESC"}]


def _is_quota_exceeded(resp: requests.Response) -> bool:
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if isinstance(body, dict) and body.get("code") in _QUOTA_EXCEEDED_CODES:
        return True
    return any(msg in resp.text for msg in _QUOTA_MESSAGES)


def search(index_id: str, query_string: str, limit: int = 100, offset: int = 0,
           sort_order_list: list | None = None, max_account_attempts: int = 5) -> dict:
    """POST /csi/search. Returns the parsed JSON response on success."""
    body = {
        "query": {"queryString": f"simple:(({query_string}))", "filterDates": [], "filterQueries": []},
        "searchScope": {"treeNodeIds": []},
        "relatedIndexQueries": [],
        "sortOrderList": sort_order_list or DEFAULT_SORT,
        "pageInfo": {"limit": limit, "offset": offset},
        "chargingInfo": {"useBalance": True},
        "otherOptions": {
            "requireLanguage": "cn", "relatedIndexEnabled": True, "groupEnabled": False,
            "smartEnabled": True, "buy": False, "summaryLengthLimit": 100, "synonymEnabled": True,
            "advanced": False, "isHideBigLib": 0, "relatedIndexFetchRows": 5, "proximateCourtID": "",
            "module": "", "correctEnabled": True, "mappingEnabled": True, "webSearchEnabled": True,
            "defaultSearch": False, "rankKeyword": "",
        },
        "indexId": index_id,
    }
    return _dispatch_and_call(
        index_id, quota_tracker.SEARCH,
        lambda session: session.post(f"{BASE}/csi/search", headers=HEADERS, json=body, timeout=30),
        max_account_attempts,
    )


def view_detail(index_id: str, doc_id: str, search_id: str = "", max_account_attempts: int = 5,
                 force_refresh: bool = False) -> dict:
    """GET /csi/document/{docId}/html, cached in MongoDB (document_store.py).

    Cache-first: if this (index_id, doc_id) was already collected, returns
    the stored document straight away -- no HTTP request, no quota spent.
    Pass force_refresh=True to bypass the cache and re-fetch anyway.
    """
    if not force_refresh:
        cached = document_store.get_cached(index_id, doc_id)
        if cached is not None:
            return cached

    params = {"indexId": index_id, "searchId": search_id, "print": "false", "fromType": "", "useBalance": "true", "module": ""}
    raw = _dispatch_and_call(
        index_id, quota_tracker.DETAIL,
        lambda session: session.get(f"{BASE}/csi/document/{doc_id}/html", headers=HEADERS, params=params, timeout=30),
        max_account_attempts,
    )
    return document_store.save(index_id, doc_id, raw)


def _dispatch_and_call(index_id: str, action: str, do_request, max_account_attempts: int) -> dict:
    last_error = None
    for _ in range(max_account_attempts):
        email, session = scheduler.dispatch(index_id, action)
        quota_tracker.try_consume(email, index_id, action)  # reserve optimistically
        resp = do_request(session)

        if _is_quota_exceeded(resp):
            quota_tracker.mark_exhausted(email, index_id, action)
            request_logger.log_request(email, index_id, action, ok=False, message="quota_exceeded")
            last_error = f"{email} exhausted for {index_id}/{action}, trying another account"
            continue

        ok = resp.status_code == 200
        request_logger.log_request(email, index_id, action, ok=ok, message="" if ok else resp.text[:300])
        if not ok:
            last_error = f"{email}: HTTP {resp.status_code} {resp.text[:200]}"
            continue
        return resp.json()

    raise RuntimeError(f"search_client: gave up after {max_account_attempts} accounts -- {last_error}")

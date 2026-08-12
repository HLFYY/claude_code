"""
真实的 /csi/search 和 /csi/document/{id}/html 调用（请求形状来自 wkinfo.py
和真实抓包），包了一层让每次调用都走 core.scheduler.dispatch()（挑一个有
配额的账号，用 login.get_session 确保已登录）和 core.quota_tracker（调用前
预占配额，服务端一说超限就立刻同步为"已耗尽"），并通过 core.request_logger
记录。

已确认的真实配额超限响应格式（来自一次真实的 浏览/detail 请求）：
    {"code":"E_010_015","message":"已达该栏目当日浏览最大量，请24小时之后再进行浏览。"}
搜索侧对应的错误码还没有实际抓到过（只从网站自己的 i18n 文案里确认了消息
文本），所以 _is_quota_exceeded() 既检查这个已确认的错误码，也兜底匹配
这两条消息文本——不需要去猜第二个错误码是什么也能覆盖两种情况。

另外值得了解（同一份 i18n 文件里挖到的，这里还没处理——见 HANDOFF.md 的
"风控信号"章节）：这个网站在会话/用户/IP/网关四个维度分别有独立的每秒和
每分钟频率限制（错误key如 G_USERS_PER_SECOND_OVERWEIGHT 等），并且是
"警告一次->警告两次->硬封"的三级递进（CURRENT_*_IS_RESTRICTED_ACCESS），
还有一个跟这个模块处理的搜索/浏览配额完全独立的、按栏目算的每日*下载*
限额（COLUMN_HAS_REACHED）。这些都不属于这个模块所处理的"配额"范畴——
它们是真正的反爬层，这一版暂不处理。
"""
from __future__ import annotations

import requests

from core import quota_tracker, request_logger, scheduler

from . import config, document_store, login

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

_QUOTA_EXCEEDED_CODES = {"E_010_015"}  # 已确认：详情/浏览配额超限
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
    """POST /csi/search。成功时返回解析后的 JSON 响应。"""
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
        index_id, "search", config.SEARCH_LIMIT_PER_INDEX,
        lambda session: session.post(f"{BASE}/csi/search", headers=HEADERS, json=body, timeout=30),
        max_account_attempts,
    )


def view_detail(index_id: str, doc_id: str, search_id: str = "", max_account_attempts: int = 5,
                 force_refresh: bool = False) -> dict:
    """GET /csi/document/{docId}/html，缓存在 MongoDB 里（document_store.py）。

    缓存优先：如果这个 (index_id, doc_id) 已经采集过，直接返回存储的文档——
    不发 HTTP 请求，不消耗配额。传 force_refresh=True 可以绕过缓存强制
    重新抓取。
    """
    if not force_refresh:
        cached = document_store.get_cached(index_id, doc_id)
        if cached is not None:
            return cached

    params = {"indexId": index_id, "searchId": search_id, "print": "false", "fromType": "", "useBalance": "true", "module": ""}
    raw = _dispatch_and_call(
        index_id, "detail", config.DETAIL_LIMIT_PER_INDEX,
        lambda session: session.get(f"{BASE}/csi/document/{doc_id}/html", headers=HEADERS, params=params, timeout=30),
        max_account_attempts,
    )
    return document_store.save(index_id, doc_id, raw)


def _dispatch_and_call(index_id: str, action: str, limit: int, do_request, max_account_attempts: int) -> dict:
    last_error = None
    for _ in range(max_account_attempts):
        email, session = scheduler.dispatch(config.PLATFORM, index_id, action, limit, login.get_session,
                                             min_interval_seconds=config.REQUEST_MIN_INTERVAL_SECONDS)
        quota_tracker.try_consume(config.PLATFORM, email, index_id, action, limit, config.QUOTA_WINDOW_SECONDS)
        resp = do_request(session)

        if _is_quota_exceeded(resp):
            print(f"[quota_exceeded] {email} {index_id}/{action} HTTP {resp.status_code} "
                  f"响应原文: {resp.text[:500]!r}")
            quota_tracker.mark_exhausted(config.PLATFORM, email, index_id, action, limit, config.QUOTA_WINDOW_SECONDS)
            request_logger.log_request(config.PLATFORM, email, index_id, action, ok=False, message="quota_exceeded")
            last_error = f"{email} exhausted for {index_id}/{action}, trying another account"
            continue

        ok = resp.status_code == 200
        request_logger.log_request(config.PLATFORM, email, index_id, action, ok=ok, message="" if ok else resp.text[:300])
        if not ok:
            last_error = f"{email}: HTTP {resp.status_code} {resp.text[:200]}"
            continue
        return resp.json()

    raise RuntimeError(f"search_client: gave up after {max_account_attempts} accounts -- {last_error}")

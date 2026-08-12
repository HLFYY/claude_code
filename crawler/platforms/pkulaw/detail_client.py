"""
文章详情页采集，缓存优先（跟 wkinfo 平台 search_client.view_detail 是同一个套路）：
MongoDB 命中直接返回，不重复请求；没命中才用登录 session 真实请求，过程中如果撞上
EdgeOne 的 JS cookie 挑战会自动解掉重试（见 eo_challenge.py）。

正文提取用 lxml 的 xpath('//*[@class="content"]')，是从真实页面结构里核对出来的
（跟用户自己验证登录成功与否用的判断方法一致）。

两种调用方式：
- view_detail(category, doc_id, identifier)：用指定的账号（手机号/邮箱）请求，
  账号是调用方自己挑的，不消耗每日配额（config.DETAIL_LIMIT_PER_DAY 是账号池
  调度用的，这条路径不检查），但会记请求流水、会按账号做请求间隔冷却——
  适合调试/测一个特定账号，比如故意测试单账号请求频率的场景。
- view_detail_pooled(category, doc_id)：不指定账号，走 core.scheduler 从账号池里
  自动挑一个还有配额的账号（挑号逻辑、配额计数、请求流水都跟 wkinfo 平台的
  search_client._dispatch_and_call 是同一套），适合真正批量采集用。
"""
from __future__ import annotations

import re

import requests
from lxml import etree

from core import quota_tracker, request_logger, scheduler

from . import config, document_store, eo_challenge, login

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": config.UA,
}

# core.quota_tracker/scheduler 需要一个 resource/action 维度来记配额，pkulaw 这边
# 目前没有 wkinfo 那种"按栏目分别限额"的概念（也没观察到服务端有明确的次数限制，
# 见 config.DETAIL_LIMIT_PER_DAY 的注释），就用一个固定的维度，账号池调度靠的是
# "每个账号每天最多跑多少次"这一个总量。
_RESOURCE = "detail"
_ACTION = "view"

# 有时候服务端返回 200、正文 xpath 也能命中，但内容被截断到一半，后面跟着
# "剩余50%未阅读"这种提示（百分比不一定固定是50，用正则兜底）。**这不一定是
# cookie/登录失效**——真实核对过 https://mall.pkulaw.com/ 的商城页面，法律法规/
# 司法案例/法学期刊/专题参考/行政执法/检察文书这几个栏目都是独立付费产品
# （2500~4000元/年不等，价格都不一样），账号很可能只买了其中一部分，对没买的
# 栏目请求详情页大概率也是这种截断表现。两种原因（cookie失效 vs 没买这个模块）
# 目前没有从截断内容本身可靠区分的办法，所以只统一当"内容不完整"处理，不假设
# 具体原因，也不会因为看到这个就去清 session/重新登录（见下面 FAIL_TRUNCATED
# 的说明）。这种"看起来成功但内容不完整"的情况必须当失败处理，不然会把残缺
# 正文当成正常采集结果存进库里。
_TRUNCATED_CONTENT_RE = re.compile(r"剩余\d+%未阅读|依据授权继续访问")


def _extract_content(html: str) -> str | None:
    tree = etree.HTML(html)
    if tree is None:
        return None
    nodes = tree.xpath('//*[@class="content"]')
    if not nodes:
        return None
    return etree.tostring(nodes[0], encoding="unicode", method="html")


def _is_truncated_content(content_html: str) -> bool:
    return bool(_TRUNCATED_CONTENT_RE.search(content_html))


# fetch_and_save 失败原因的标识值之一：内容被截断（"剩余N%未阅读"）。故意叫
# 一个中性的名字，不叫 FAIL_COOKIE_EXPIRED——原因可能是 cookie 失效，也可能是
# 账号没买这个模块，见上面 _TRUNCATED_CONTENT_RE 的说明，调用方不应该假设是
# 哪一种。
FAIL_TRUNCATED = "content_truncated"


def fetch_and_save(category: str, doc_id: str, session, extra_fields: dict | None = None) -> tuple[dict | None, str]:
    """返回 (result, fail_reason)。result 是 None 表示这次请求没拿到有效正文，
    fail_reason 是空字符串表示成功，否则是失败原因的简短标识
    （FAIL_TRUNCATED / "challenge" / "http_{status}" / "parse_failed"）。
    不在这里抛异常，方便账号池调度那边换个账号重试。

    `extra_fields` 是调用方想顺带存进这份文档里的额外字段（比如从哪个频道页面
    发现的这篇文章），必须在这一次 save 里就带上——`core.document_store.save`
    是整份 `replace_one`，不是按字段合并，事后再单独存一次只带 extra_fields
    会把 contentHtml 这些正文字段覆盖掉。"""
    url = document_store.build_url(category, doc_id)
    resp = eo_challenge.get_with_challenge_retry(session, url, headers=_HEADERS, timeout=15)
    if eo_challenge.is_challenge_page(resp):
        return None, "challenge"
    if resp.status_code != 200:
        return None, f"http_{resp.status_code}"

    content_html = _extract_content(resp.text)
    if content_html is None:
        return None, "parse_failed"
    if _is_truncated_content(content_html):
        return None, FAIL_TRUNCATED

    fields = {
        "url": url,
        "contentHtml": content_html,
        "rawHtmlLength": len(resp.text),
    }
    if extra_fields:
        fields.update(extra_fields)
    return document_store.save(category, doc_id, fields), ""


def view_detail(category: str, doc_id: str, identifier: str, force_refresh: bool = False,
                 extra_fields: dict | None = None) -> dict:
    """缓存优先：MongoDB 命中直接返回，不发请求、不需要登录、不计入下面这些统计。
    没命中才用 `identifier`（手机号或邮箱，login.py 会自动判断）对应的 pkulaw
    登录 session（见 login.get_session）发起真实请求。传 force_refresh=True
    跳过缓存强制重新抓取。用指定账号，不走账号池调度（不会自动换账号），但
    跟 view_detail_pooled 共用同一套限流/统计基础设施：

    - 真实请求之间会按 config.DETAIL_REQUEST_MIN_INTERVAL_SECONDS 冷却
      （core.scheduler.wait_for_cooldown，同一账号请求太快会原地等待，不是
      跳过换账号——这里只有一个账号，没有"换一个"这个选项）。
    - 不管成功失败都会调 core.request_logger.log_request 记一条流水，
      request_logger.stats_by_day(config.PLATFORM) 能查到某天某账号的
      请求总数/成功数/失败数（登录失败这类没走到实际抓取的情况也算一次
      失败请求，不会被漏记）。
    - "内容被截断"（FAIL_TRUNCATED，正文里带"剩余N%未阅读"）目前只当成一次
      普通失败处理，**不会**自动清缓存重新登录——真实核对过
      https://mall.pkulaw.com/ 商城页面，这几个栏目都是独立付费产品（价格从
      2500到4000元/年不等），截断很可能是账号没买这个模块，不一定是 cookie
      失效，自动重登解决不了权限问题，还会白白多消耗一次登录、甚至反复触发。
      两种原因目前没有从截断内容本身可靠区分的办法，先按普通失败记录，不自动
      干预 session。
    """
    if not force_refresh:
        cached = document_store.get_cached(category, doc_id)
        if cached is not None:
            return cached

    scheduler.wait_for_cooldown(config.PLATFORM, _RESOURCE, _ACTION, identifier,
                                 config.DETAIL_REQUEST_MIN_INTERVAL_SECONDS)
    try:
        session = login.get_session(identifier)
        result, fail_reason = fetch_and_save(category, doc_id, session, extra_fields=extra_fields)
    except Exception as e:
        request_logger.log_request(config.PLATFORM, identifier, _RESOURCE, _ACTION, ok=False,
                                    message=str(e)[:300])
        raise

    ok = result is not None
    request_logger.log_request(config.PLATFORM, identifier, _RESOURCE, _ACTION, ok=ok,
                                message="" if ok else f"请求失败: {fail_reason}")
    if not ok:
        url = document_store.build_url(category, doc_id)
        raise RuntimeError(f"view_detail: 请求失败（{fail_reason}）-- {url}")
    return result


def view_detail_by_url(url: str, identifier: str, force_refresh: bool = False,
                        extra_fields: dict | None = None) -> dict:
    """传完整文章 URL 也行，比如
    "https://www.pkulaw.com/qikan/5c6347f6bc4c4866bdca50e0aff747f0bdfb.html"。"""
    category, doc_id = document_store.parse_url(url)
    return view_detail(category, doc_id, identifier, force_refresh=force_refresh, extra_fields=extra_fields)


def _dispatch_login(identifier: str, _password: str):
    """core.scheduler.dispatch 要求的 login_fn(email, password) -> (session, profile)
    形状；pkulaw 这边密码是 login.get_session 自己从账号记录里查的，不需要调用方
    传，这里的 _password 参数就没用上，纯粹是为了对上 scheduler 的签名。"""
    return login.get_session(identifier), {}


def view_detail_pooled(category: str, doc_id: str, force_refresh: bool = False, max_account_attempts: int = 5, is_login=True) -> dict:
    """不指定账号，从账号池里自动挑一个还有配额的账号（core.scheduler.dispatch），
    配额是"每个账号每天最多跑 config.DETAIL_LIMIT_PER_DAY 次"，超了就换下一个账号，
    每次请求成功与否都记进 core.request_logger 的流水。缓存优先，命中不消耗配额、
    不发请求。"""
    if not force_refresh:
        cached = document_store.get_cached(category, doc_id)
        if cached is not None:
            return cached
    last_error = None
    if not is_login:
        result, fail_reason = fetch_and_save(category, doc_id, requests.Session())
        ok = result is not None
        request_logger.log_request(config.PLATFORM, "", _RESOURCE, _ACTION, ok=ok,
                                   message="" if ok else f"请求失败: {fail_reason}")
        if ok:
            return result
        last_error = f"无账号: {fail_reason}"
    else:
        for _ in range(max_account_attempts):
            try:
                identifier, session = scheduler.dispatch(
                    config.PLATFORM, _RESOURCE, _ACTION, config.DETAIL_LIMIT_PER_DAY, _dispatch_login,
                    min_interval_seconds=config.DETAIL_REQUEST_MIN_INTERVAL_SECONDS,
                )
            except scheduler.NoAccountAvailable as e:
                raise RuntimeError(f"view_detail_pooled: 账号池里没有账号还有配额了 -- {e}") from e
            print(identifier)
            quota_tracker.try_consume(config.PLATFORM, identifier, _RESOURCE, _ACTION,
                                       config.DETAIL_LIMIT_PER_DAY, config.QUOTA_WINDOW_SECONDS)
            result, fail_reason = fetch_and_save(category, doc_id, session)
            ok = result is not None
            request_logger.log_request(config.PLATFORM, identifier, _RESOURCE, _ACTION, ok=ok,
                                        message="" if ok else f"请求失败: {fail_reason}")
            if ok:
                return result
            # FAIL_TRUNCATED（内容被截断）先按普通失败处理，不自动清 session——
            # 截断不一定是 cookie 失效，也可能是这个账号对该模块本来就没权限，见
            # view_detail() 文档字符串里的说明。账号池这边天然会换下一个账号重试，
            # 不需要额外干预。
            last_error = f"{identifier}: {fail_reason}，换下一个账号重试"

    raise RuntimeError(f"view_detail_pooled: 尝试 {max_account_attempts} 个账号后仍失败 -- {last_error}")


def view_detail_pooled_by_url(url: str, force_refresh: bool = False, max_account_attempts: int = 5, is_login=True) -> dict:
    category, doc_id = document_store.parse_url(url)
    return view_detail_pooled(category, doc_id, force_refresh=force_refresh, max_account_attempts=max_account_attempts, is_login=is_login)

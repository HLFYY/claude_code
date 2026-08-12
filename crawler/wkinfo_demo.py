"""
wkinfo 平台完整流程的验收脚本（跟 pkulaw_demo.py 是同一个套路），从 crawler/ 目录直接跑：

    cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
    /Users/houjie/venv/python3-forcrawl/bin/python wkinfo_demo.py

传入邮箱、手机号、密码，走一遍：

  1. 先看有没有还有效的缓存 session（login.load_cached_session）——有就直接用，
     不发任何登录请求。
  2. 没有缓存才需要真登录。先查这个账号在不在我们自己的 account_registry 里，
     不管在不在都直接尝试登录一次（同一个密码重试没有意义，login.login 本身
     已经不重试了，见 platforms/wkinfo/login.py 的说明）：
     - 登录成功 + 账号已经在 registry 里：刷新缓存的 session；如果这个账号
       之前从没绑过代理（比如是补登进来的老数据），把这次登录已经用过的
       proxy_id 转正绑定，并同步更新 account_registry 里的 proxyId 字段。
     - 登录成功 + 账号不在 registry 里：说明这是网站上真实存在、但不是通过这套
       账号池注册出来的账号（比如手工注册/别处导入），补进 account_registry
       （否则调度器/配额追踪找不到这个账号的记录）、绑定这次登录用的代理、
       缓存 session。
     - 登录失败 + 错误码是 E_020_002（账号不存在）：走
       registration_worker.register_one 的完整注册流程（验证码识别 + 短信 +
       提交），显式传入调用方指定的密码（而不是随机生成一个）——register_one
       内部会自己挑代理/绑定/存 registry/确认登录，不需要复用这里已经挑好但
       还没绑定成功的那个代理。
     - 登录失败 + 其它错误码（密码错误 E_020_001、并发超标 C_002_001 等）：
       直接抛出。这些原因换个 session/重试一次都不会变，更不能想当然地当成
       "账号未注册"去走注册流程——那样会给一个已经存在只是密码不对的账号
       又走一遍注册，白白浪费一个新手机号/邮箱。
  3. 不管走的是哪条分支，最后都用这次真实拿到的 session 测试一次详情访问：先搜索
     一个关键词拿到一条真实文档，再请求它的详情接口——直接用这一个指定账号的
     session 发请求，不走 core.scheduler 账号池调度（那是给批量采集用的，请求可能
     被派发到池子里其他账号身上，测不出"这个账号本身能不能访问详情"）。
  4. crawl_legislation_keyword()：批量采集入口，跟 pkulaw 以后的平台约定一致——
     搜索/详情都不指定账号，交给 core.scheduler.dispatch()（通过
     platforms/wkinfo/search_client.py 已经封装好的 search()/view_detail()）
     自动从账号池里挑一个还有配额的账号。详情页抓回来之后调用
     platforms/wkinfo/parse_detail.py 的 parse_detail_case() 解析 content 字段
     （HTML）为结构化字段，合并进详情原始数据里一并存回 MongoDB。

**代理终身绑定，不能中途换**（见 core/proxy_pool.py 的说明）：如果这个账号之前已经
绑定过代理（比如重复跑这个脚本），直接复用那个绑定关系；只有从没绑定过时才会新挑一个
并在登录成功后立刻绑定。
"""
from __future__ import annotations

import time

import requests

from core import account_registry, proxy_pool
from core.logger import log
from platforms.wkinfo import config, document_store, login, parse_detail, search_client
from platforms.wkinfo.registration_worker import register_one



def _step(n: int, title: str) -> None:
    print(f"\n{'=' * 60}\n第{n}步：{title}\n{'=' * 60}")


def _real_expiry_from_profile(profile: dict) -> float:
    """跟 registration_worker._real_expiry_from_profile 同一个逻辑：优先用服务端
    自己给出的 productsDetailList[0].endDate（毫秒），拿不到就退回"现在 + 3天"估算。"""
    try:
        return profile["productsDetailList"][0]["endDate"] / 1000
    except (KeyError, IndexError, TypeError):
        return time.time() + config.ACCOUNT_TRIAL_SECONDS


def _test_detail(session: requests.Session, index_id: str = "law.legislation", query: str = "宪法") -> None:
    body = {
        "query": {"queryString": f"simple:(({query}))", "filterDates": [], "filterQueries": []},
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
        "indexId": index_id,
    }
    r = session.post(f"{login.BASE}/csi/search", headers=login.HEADERS, json=body, timeout=15)
    docs = r.json().get("documentList", [])
    print(f"搜索 {index_id!r} 关键词 {query!r}：{len(docs)} 条结果")
    if not docs:
        print("没有搜索结果，跳过详情测试")
        return

    first = docs[0]
    print("第一条结果原始字段（确认详情接口该用哪个字段当 docId，字段名没有在代码里\n"
          "确认过，这里打印出来供人工核对）：", first)
    doc_id = first.get("id") or first.get("docId")
    if not doc_id:
        print("没能从结果里自动找到 id/docId 字段，详情测试跳过——对照上面打印的原始字段手动确认字段名")
        return

    params = {"indexId": index_id, "searchId": "", "print": "false", "fromType": "", "useBalance": "true", "module": ""}
    detail = session.get(f"{login.BASE}/csi/document/{doc_id}/html", headers=login.HEADERS, params=params, timeout=15)
    print("详情接口 HTTP 状态:", detail.status_code)
    print("详情响应片段:", detail.text[:200])


def _login_or_register(email: str, telephone: str, password: str) -> tuple[requests.Session, dict]:
    """没有有效缓存 session 时的入口：不管账号是否已经在 account_registry 里，
    都先直接尝试登录一次，再按结果分三种情况处理（见模块 docstring 第2步）。"""
    account = account_registry.get_account(config.PLATFORM, email)
    existing_proxy_id = proxy_pool.get_account_proxy_id(config.PLATFORM, email)
    proxy_id = existing_proxy_id or proxy_pool.pick_for_new_account(config.PLATFORM, config.MAX_ACCOUNTS_PER_IP)
    if proxy_id is None:
        raise RuntimeError(f"代理池没有空位（platform={config.PLATFORM}），无法继续")

    session = requests.Session()
    session.proxies = proxy_pool.requests_proxies(proxy_id)

    _step(1, f"直接登录 {email}")
    try:
        session, profile = login.login(email, password, session=session)
    except login.LoginError as e:
        if e.code != "E_020_002":
            raise  # 密码错误/并发超标之类，跟"账号没注册"是两回事，不能走注册流程
        log(config.PLATFORM, f"账号 {email} 未注册（{e}），走注册流程")
        _step(2, f"注册新账号 telephone={telephone} email={email}")
        record = register_one(telephone, email, password=password)
        print("注册完成，account_registry 记录:", record)
        # register_one 内部已经确认登录成功过一次并缓存了 session，这里直接复用缓存，
        # 不用再真登录一次。
        return login.get_session(email, password)

    if account:
        log(config.PLATFORM, f"账号 {email} 已在 account_registry 里，登录成功，刷新缓存 session")
        _step(2, "登录成功，刷新缓存 session")
        if not existing_proxy_id:
            # 账号在系统里但从没绑过代理（比如是补登进来的老数据）——这次登录
            # 已经临时用 proxy_id 走通了，直接把它转正，往后这个账号就固定
            # 走这个代理，不会下次又换一个。
            log(config.PLATFORM, f"账号 {email} 之前没绑定代理，补绑 proxy_id={proxy_id}")
            proxy_pool.bind_account(config.PLATFORM, email, proxy_id)
            account_registry.update_proxy_id(config.PLATFORM, email, proxy_id)
    else:
        log(config.PLATFORM, f"账号 {email} 登录成功但不在 account_registry 里，补登 registry + 绑定代理 + 缓存 session")
        _step(2, "登录成功，写入 account_registry + 绑定代理 + 缓存 session")
        account_registry.save_account(
            config.PLATFORM, email, _real_expiry_from_profile(profile),
            {"telephone": telephone, "password": password}, proxy_id=proxy_id,
        )
        if not existing_proxy_id:
            proxy_pool.bind_account(config.PLATFORM, email, proxy_id)
    login.save_session(email, session, profile)
    return session, profile


def run(email: str, telephone: str, password: str) -> None:
    cached = login.load_cached_session(email)
    if cached is not None:
        session, profile = cached
        log(config.PLATFORM, f"账号 {email} 已有有效登录态，直接复用缓存 session（不发登录/注册请求）")
    else:
        session, profile = _login_or_register(email, telephone, password)

    print("userEmail:", profile.get("userEmail"), "| telephone:", profile.get("telephone"))

    # _step(3, "用这个账号的 session 测试详情访问")
    # _test_detail(session)


def crawl_legislation_keyword(query: str = "刑法", limit: int = 100) -> list[dict]:
    """搜索 law.legislation 下的关键词，按序请求每条结果的详情，用
    parse_detail.parse_detail_case() 解析详情的 content（HTML）字段，把解析
    结果合并进详情原始数据里一并存回 MongoDB。搜索和详情请求都不指定账号，
    交给 core.scheduler.dispatch()（通过 search_client.py 里已经封装好的
    search()/view_detail()）自动从账号池里挑一个还有配额的账号——这是以后
    其它平台都要遵循的统一调度方式，跟 pkulaw 那边的账号池调度是同一套
    core.scheduler.dispatch() 逻辑。

    parse_detail_case() 是照着一份法律法规详情写的，还没验证过对其它文档是否
    通用，所以这里对每条结果单独 try/except，解析失败不会中断整个批次，只是
    这一条不带解析字段、把错误打印出来方便排查。返回处理过的详情列表。
    """
    index_id = "law.legislation"
    _step(4, f"搜索 {index_id} 关键词 {query!r}（page size={limit}），自动账号调度")
    result = search_client.search(index_id, query, limit=limit)
    docs = result.get("documentList", [])
    print(f"共 {len(docs)} 条搜索结果")

    results = []
    for i, doc in enumerate(docs, 1):
        doc_id = doc.get("id") or doc.get("docId")
        if not doc_id:
            print(f"[{i}/{len(docs)}] 结果缺少 id/docId 字段，跳过：{doc}")
            continue

        raw = search_client.view_detail(index_id, doc_id)
        try:
            parsed = parse_detail.parse_detail_case(raw.get("content", ""))
        except Exception as e:
            print(f"[{i}/{len(docs)}] doc_id={doc_id} 解析失败: {e}")
            results.append(raw)
            continue

        merged = {**raw, **parsed}
        saved = document_store.save(index_id, doc_id, merged)
        results.append(saved)
        print(f"[{i}/{len(docs)}] doc_id={doc_id} 法规名称={parsed.get('法规名称')!r} "
              f"正文长度={len(parsed.get('正文纯文本') or '')} 结构化条数={len(parsed.get('正文结构化') or [])}")

    return results


if __name__ == "__main__":
    accs = [
        ['hj1558109546@gmail.com', '17717295039', '315128abc'],
        ['1558109546@qq.com', '18356966159', '315128abc'],
        ['1565655612@qq.com', '13032214030', '123456abc'],
    ]
    # for email, phone, password in accs:
    #     run(email, phone, password)
    crawl_legislation_keyword("刑法", limit=100)

"""
Session 缓存 + 登录调度 + 代理绑定。auth.py 只管"怎么登录/怎么改密码"（手机版/
邮箱版分开的具体接口），这一层管"给一个账号标识（手机号或邮箱），自动判断类型、
该走哪条逻辑、要不要重新登录、账号信息怎么存、用哪个代理"——跟用户确认过的模型：
"不管是注册、账户密码登录、验证码登录、还是修改密码，都自动判断账户类型，然后走
手机或邮箱的逻辑"。分发全部靠 account_type.detect(identifier)，调用方不需要自己
说是手机号还是邮箱。

三个业务流程（手机号+邮箱都已实现并用真实请求验证过，见 HANDOFF.md）：
1. 注册：邮箱未注册过时，走一遍验证码+邮箱验证码通常会"直接注册并登录"（跟登录
   是同一个接口，见 auth.complete_login）；但也可能被网站转人工审核——注意白名单
   邮箱后缀（gmail/qq/hotmail/icloud）不是可靠判据，真实测试中白名单域名的邮箱也
   被转过审核（大概率是按 IP/风控信号），所以 complete_login 失败一律当成"可能在
   待审核"处理，login_interactive() 会抛 RegistrationPendingReview 提示去检查邮箱，
   不是普通失败。手机号未注册过是"直接注册并登录"（auth.complete_login_by_phone），
   没有观察到手机号被转审核的情况。
   注册成功的判定标准就是"登录状态测出来是真的登录了"（auth.is_logged_in()）。
2. 改密码：change_password(identifier, new_password)。
3. 登录：账号记录里 password 字段是空的，就走验证码登录（需要真人读验证码）；
   有值就走密码登录（auth.password_login，手机号/邮箱同一个表单）。

改密码之外的 gateway.pkulaw.com 接口都要一个独立的 Bearer access_token（不是
Keycloak session cookie），30分钟过期，get_access_token() 负责拿到手/刷新。

跟 wkinfo 平台一样，"账号绑定一个固定代理，终身复用"：login_interactive()（第一次
登录/注册）和 password_login 都会检查这个 identifier 有没有绑定过代理，没有就从
core.proxy_pool 挑一个负载最少的绑上，之后这个账号所有请求（包括从 Redis 缓存
恢复 session 时）都固定走这个代理。

账号记录存在 core.account_registry 里（Redis Hash：`account:{platform}:{identifier}`），
跟 wkinfo 平台是同一个存储模块，但 pkulaw 账号没有"试用期"这个概念，expires_at
字段只是为了满足 core.account_registry 的数据模型，存了一个很远的时间，不代表账号
真的会过期。
"""
from __future__ import annotations

import json
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core import account_registry, proxy_pool
from core.logger import log
from core.redis_client import get_client, k

from . import account_type, auth, config

# pkulaw 账号不像 wkinfo 那样有真实的试用期，这里存一个很远的 expires_at 只是为了
# 满足 core.account_registry 的数据模型（它靠这个字段判断"账号是不是还有效"），
# 不代表账号真的会在这个时间点过期。
_NO_EXPIRY = time.time() + 100 * 365 * 24 * 60 * 60


class RegistrationPendingReview(RuntimeError):
    """邮箱后缀不在预设白名单（gmail/qq/hotmail/icloud）时，网站会把注册转成人工
    审核：真人需要回复收到的邮件，等审核通过后才能登录。这是正常的业务分支，不是
    登录失败，调用方不应该当成普通异常重试，而是提示用户去处理邮件、之后再重新
    调用 login_interactive()。"""


def _default_code_getter(identifier: str) -> str:
    return input(f"验证码已发送到 {identifier}，请输入收到的验证码: ").strip()


def _new_session_with_retry() -> requests.Session:
    """真实代理偶尔会有瞬时连接失败（实测遇到过一次 TLS 握手中途断开，代理本身
    紧接着单独测试是正常的，像是一次性抖动），给 session 挂一个连接层面的自动
    重试，不用每次都在业务代码里手动 try/except——只重试连接类错误
    （ConnectTimeout/ConnectionError 这类，且限定在网络层，不重试已经真的发出去
    又收到响应的请求，避免把验证码提交这种"一次性动作"重复提交两次）。"""
    session = requests.Session()
    retry = Retry(total=2, connect=2, read=0, backoff_factor=1.0,
                   status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _proxied_session(identifier: str) -> requests.Session:
    """一个新 session，如果这个账号已经绑定了代理就走它；没有绑定（还没注册过、
    或者注册时代理池是空的那种边缘情况）就直接直连。"""
    session = _new_session_with_retry()
    session.headers.update({"user-agent": config.UA})
    proxy_id = proxy_pool.get_account_proxy_id(config.PLATFORM, identifier)
    if proxy_id:
        session.proxies = proxy_pool.requests_proxies(proxy_id)
    return session


def _ensure_proxy_bound(identifier: str) -> None:
    """确保这个账号绑定了一个代理——已经绑过就什么都不做；没绑过就从代理池挑一个
    负载最少的绑上。代理池为空或都满了会直接抛异常，不会静默地在没有固定 IP 的
    情况下继续（跟 wkinfo 平台 registration_worker.py 是同一个设计原则）。"""
    if proxy_pool.get_account_proxy_id(config.PLATFORM, identifier):
        return
    proxy_id = proxy_pool.pick_for_new_account(config.PLATFORM, config.MAX_ACCOUNTS_PER_IP)
    if proxy_id is None:
        raise RuntimeError(
            f"_ensure_proxy_bound: 代理池没有可用代理给 {identifier}（为空或每个代理都绑满了 "
            f"MAX_ACCOUNTS_PER_IP={config.MAX_ACCOUNTS_PER_IP} 个账号）。"
        )
    proxy_pool.bind_account(config.PLATFORM, identifier, proxy_id)


def _session_from_cookies(identifier: str, cookies: dict) -> requests.Session:
    session = _proxied_session(identifier)
    session.cookies.update(cookies)
    return session


def save_session(identifier: str, session: requests.Session, access_token: str | None = None) -> None:
    data = {
        "cookies": json.dumps(requests.utils.dict_from_cookiejar(session.cookies), ensure_ascii=False),
        "saved_at": time.time(),
    }
    if access_token is not None:
        data["access_token"] = access_token
    get_client().hmset(k("session", config.PLATFORM, identifier), data)


def load_cached_session(identifier: str) -> requests.Session | None:
    data = get_client().hgetall(k("session", config.PLATFORM, identifier))
    if not data:
        return None
    session = _session_from_cookies(identifier, json.loads(data["cookies"]))
    if not auth.is_logged_in(session):
        return None  # 缓存的 cookie 已经失效了
    return session


def _decode_jwt_exp(access_token: str) -> float:
    import base64

    payload_b64 = access_token.split(".")[1]
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    payload = json.loads(base64.urlsafe_b64decode(padded))
    return payload["exp"]


def get_access_token(identifier: str, session: requests.Session) -> str:
    """确保拿到一个还没过期的 gateway.pkulaw.com 专用 Bearer access_token
    （modify-password-by-*/用户信息这类接口要用，跟浏览网页用的 Keycloak session
    cookie 是两回事，30分钟过期）。

    Redis 里缓存的 access_token 还有效（留 60 秒缓冲）就直接用；快过期或已过期就用
    同一个 session（必须带着登录时的 Keycloak cookie）调 auth.refresh_access_token
    刷新。如果 Redis 里连 access_token 都没有——说明这个 session 是很早以前缓存的、
    从来没换过 token，就直接用 auth.get_token_from_page(session) 现取一个（这个
    session 既然能通过 load_cached_session 的 is_logged_in 检查，就一定能用来
    请求一个已登录页面拿到当前有效的 token，不需要走完整的 login_interactive）。
    """
    data = get_client().hgetall(k("session", config.PLATFORM, identifier))
    access_token = data.get("access_token") if data else None

    if access_token and _decode_jwt_exp(access_token) - time.time() > 60:
        return access_token

    new_token = (
        auth.refresh_access_token(session, access_token)
        if access_token else
        auth.get_token_from_page(session)
    )
    get_client().hset(k("session", config.PLATFORM, identifier), "access_token", new_token)
    return new_token


def set_password(identifier: str, password: str) -> None:
    """改密码流程成功后调用，把新密码写回账号记录（只更新这一个字段，不动
    status/expires_at/其他字段——跟 core.account_registry.set_status() 是同一个
    "直接 hset 单个字段"的写法）。写完之后 get_session() 就会走密码登录而不是
    验证码登录。"""
    log(config.PLATFORM, f"账号 {identifier} 密码修改成功: {password}")
    get_client().hset(k("account", config.PLATFORM, identifier), "password", password)


def _save_account_if_new(identifier: str) -> None:
    if account_registry.get_account(config.PLATFORM, identifier) is None:
        account_registry.save_account(config.PLATFORM, identifier, _NO_EXPIRY, {"password": ""})


def login_interactive(identifier: str, code_getter=None) -> requests.Session:
    """完整走一遍验证码 + 短信/邮箱验证码 + 登录表单，需要真人读验证码。自动判断
    identifier 是手机号还是邮箱（account_type.detect），分别走对应逻辑：
    - 邮箱：白名单后缀未注册过时会直接注册并登录；其他后缀会被转人工审核，这种
      情况下抛 RegistrationPendingReview 而不是普通 RuntimeError。
    - 手机号：未注册过也是直接注册并登录。

    整个流程（含验证码）都通过这个账号绑定的代理发出——没绑定过代理就先绑一个
    （见 _ensure_proxy_bound），代理池没有空位会直接抛异常，不会静默直连。

    成功后把 session（+ gateway access_token）缓存进 Redis；如果
    core.account_registry 里还没有这个账号的记录，顺带存一条（password 留空，
    表示目前只能用验证码登录）。
    """
    getter = code_getter or _default_code_getter
    acc_type = account_type.detect(identifier)

    _ensure_proxy_bound(identifier)
    proxied = _proxied_session(identifier)
    session, login_ctx = auth.get_cas_session(proxied)

    verify_result = auth.solve_and_verify(session)
    if verify_result.get("errorCode") != "0":
        raise RuntimeError(f"login_interactive: 验证码没通过 {verify_result}")

    if acc_type == account_type.EMAIL:
        send_result = auth.send_email_code(session, identifier, verify_result["randstr"], verify_result["ticket"])
        if "error" in send_result:
            raise RuntimeError(f"login_interactive: 发送邮箱验证码失败 {send_result}")

        code = getter(identifier)

        # 原来这里只在邮箱后缀不在白名单（gmail/qq/hotmail/icloud）时才把失败包装成
        # RegistrationPendingReview——真实测试推翻了这个假设：xingchi660@gmail.com
        # 后缀在白名单里，一样被网站转了人工审核（大概率是按 IP/风控信号判断，不是
        # 单纯按域名后缀）。现在改成精确识别 auth.AccountPendingReviewError——这是
        # auth.py 从真实响应 HTML 里挖出来的可靠信号（页面里带"请回复邮件进行审核"
        # 这行提示才会抛这个异常类型），不是靠猜的，所以验证码真错了会照常抛普通
        # RuntimeError，只有真正命中审核信号才会转成 RegistrationPendingReview。
        try:
            _final_resp, _auth_code = auth.complete_login(session, login_ctx, identifier, code)
        except auth.AccountPendingReviewError as e:
            raise RegistrationPendingReview(
                f"{identifier} 被网站转了人工审核（响应页面里带着「请回复邮件进行审核」提示，"
                "即使邮箱后缀在白名单 gmail/qq/hotmail/icloud 里也可能发生）。请查收这个邮箱"
                "有没有收到 market@chinalawinfo.com 的审核邮件并回复，审核通过后再重新调用 "
                "login_interactive()。"
            ) from e
    else:  # PHONE
        send_result = auth.send_phone_code(session, identifier, verify_result["randstr"], verify_result["ticket"])
        if "error" in send_result:
            raise RuntimeError(f"login_interactive: 发送短信验证码失败 {send_result}")
        code = getter(identifier)
        try:
            _final_resp, _auth_code = auth.complete_login_by_phone(session, login_ctx, identifier, code)
        except auth.AccountPendingReviewError as e:
            raise RegistrationPendingReview(
                f"{identifier} 被网站转了人工审核（响应页面里带着「请回复邮件进行审核」提示）。"
                "没有实际收件箱可查（手机号没有邮件审核这条路），大概率是关联邮箱/风控信号触发，"
                "建议先换一个账号或代理重试。"
            ) from e

    if not auth.is_logged_in(session):
        raise RuntimeError("login_interactive: 登录表单提交完了但 is_logged_in 仍是 False")

    # 顺便拿一个 gateway.pkulaw.com 专用的 access_token（modify-password 这类接口
    # 要用）。不用 exchange_code_for_token 换那个 auth_code——实测确认它是一次性的，
    # complete_login 自己那次落地 GET 已经消费掉了，再拿去换 token 会 401；改用
    # get_token_from_page，直接从登录后的页面 HTML 里现取当前有效的 token，更可靠。
    # 这一步万一还是失败，不影响登录本身已经成功这件事，只打印警告，调用方后续要用
    # access_token 时会在 get_access_token() 里报错。
    try:
        access_token = auth.get_token_from_page(session)
    except Exception as e:  # noqa: BLE001 -- 这一步失败不应该让登录本身失败
        log(config.PLATFORM, f"login_interactive: 拿 gateway access_token 失败（不影响登录本身）: {e}")
        access_token = None

    save_session(identifier, session, access_token=access_token)
    _save_account_if_new(identifier)
    return session


def get_session(identifier: str, code_getter=None, force: bool = False) -> requests.Session:
    """先看 Redis 缓存的 session 还有没有效；没有的话看账号记录里存没存密码：
    有密码就走密码登录（auth.password_login），没有就走交互式验证码登录（会真实
    发一条短信/一封邮件，需要真人输入验证码）。密码登录也会走这个账号绑定的代理
    （理论上密码登录时账号一定已经注册过、也就一定绑过代理了，这里仍然调
    _ensure_proxy_bound 兜底一下，防止账号记录是别的渠道写进来的边缘情况）。"""
    if not force:
        cached = load_cached_session(identifier)
        if cached is not None:
            return cached

    account = account_registry.get_account(config.PLATFORM, identifier)
    password = (account or {}).get("password") or ""
    if password:
        _ensure_proxy_bound(identifier)
        proxied = _proxied_session(identifier)
        session, _auth_code = auth.password_login(identifier, password, proxied)
        try:
            access_token = auth.get_token_from_page(session)
        except Exception as e:  # noqa: BLE001 -- 这一步失败不应该让登录本身失败
            log(config.PLATFORM, f"get_session: 拿 gateway access_token 失败（不影响登录本身）: {e}")
            access_token = None
        save_session(identifier, session, access_token=access_token)
        return session

    return login_interactive(identifier, code_getter)


def invalidate_session(identifier: str) -> None:
    """只清掉本地缓存的 session（不调用网站的登出接口）——用于"探测到 cookie
    实际已经失效，但网站自己的 is_logged_in 检查/服务端可能还没反应过来"这种
    场景（比如详情页 HTTP 200 且 is_logged_in 仍是 True，但正文被截断带着
    "剩余N%未阅读"提示，见 detail_client.py）。跟 logout() 的区别：logout()
    是主动退出，会拿着（假定还有效的）session 去调网站登出接口；这里的前提
    正好相反——这个 session 已经不可信了，不应该再拿它去发任何请求，只清本地
    缓存，逼下一次 get_session() 老老实实走一遍真登录（有密码的会自动走密码
    登录，不需要人工介入）。"""
    get_client().delete(k("session", config.PLATFORM, identifier))


def logout(identifier: str) -> None:
    """退出登录：用 Redis 里缓存的 session 调 auth.logout()，然后把这条
    session 缓存删掉——下次 get_session() 就不会再命中一个已经失效的缓存，
    会老老实实重新登录一遍。缓存里本来就没有有效 session 就什么都不做。"""
    session = load_cached_session(identifier)
    if session is None:
        return
    auth.logout(session)
    get_client().delete(k("session", config.PLATFORM, identifier))


def change_password(identifier: str, new_password: str, code_getter=None) -> None:
    """改密码：必须是登录状态，且要改的账号就是当前登录的这个账号（跟网站本身的
    业务规则一致——modify-password-by-* 接口靠 access_token 反解出账号身份，不是
    显式传 userId）。自动判断 identifier 是手机号还是邮箱，分别走对应的
    modify-password-by-phone / modify-password-by-email。

    走一遍新的验证码 + 短信/邮箱验证码（跟登录用的是同一套接口，但这是单独触发的
    一次，不是复用登录时那次的验证码），拿到 code 后直接提交新密码
    （modify-password-by-* 不需要像登录那样先单独 verify 一次）。

    成功后调用 set_password() 把新密码写回账号记录，之后 get_session() 就会走密码
    登录而不是验证码登录。
    """
    acc_type = account_type.detect(identifier)
    getter = code_getter or _default_code_getter

    session = get_session(identifier, code_getter)
    access_token = get_access_token(identifier, session)

    verify_result = auth.solve_and_verify(session)
    if verify_result.get("errorCode") != "0":
        raise RuntimeError(f"change_password: 验证码没通过 {verify_result}")

    if acc_type == account_type.EMAIL:
        send_result = auth.send_email_code(session, identifier, verify_result["randstr"], verify_result["ticket"])
        if "error" in send_result:
            raise RuntimeError(f"change_password: 发送邮箱验证码失败 {send_result}")
        code = getter(identifier)
        result = auth.modify_password_by_email(access_token, identifier, code, new_password)
    else:  # PHONE
        send_result = auth.send_phone_code(session, identifier, verify_result["randstr"], verify_result["ticket"])
        if "error" in send_result:
            raise RuntimeError(f"change_password: 发送短信验证码失败 {send_result}")
        code = getter(identifier)
        result = auth.modify_password_by_phone(access_token, identifier, code, new_password)

    if result.get("code") != "200":
        raise RuntimeError(f"change_password: 改密码失败 {result}")

    set_password(identifier, new_password)

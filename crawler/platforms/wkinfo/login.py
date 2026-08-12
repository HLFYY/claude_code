"""
law.wkinfo.com.cn 登录接口——明文用户名/密码，正常情况不需要验证码。

通过静态分析 main.<hash>.js 定位到的（不是猜的）：Angular 的
AccountService.login() 是这样调的：

    this.http.post(API_URL + "/csi/account/validate/ex",
                    {username: t.username, password: t.password}, ...)

也就是 POST /csi/account/validate/ex，body 是明文 JSON。成功时响应体本身
就是完整的用户 profile，Set-Cookie 头带着 session（connect.sid）——后续所有
需要登录态的请求（比如 /csi/search）只需要这个 cookie。登出是
GET /api/logout。login() 只发一次这个请求，不重试——密码错了/账号不存在
这些错误换个 session 或者再试一次都不会变，重试没有意义，直接把服务端的
错误抛给调用方即可。

验证码（了解即可，login() 目前没接这个）：通过浏览器实测抓包 + 对照
main.<hash>.js 里 LoginComponent 的源码确认的。密码错误响应体是
{"code":"E_020_001","message":"密码错误"}（账号不存在是 E_020_002）——
验证码要不要弹出来，完全是网页前端自己在本地数一个 errorTimes 计数器（同一个
浏览器 session 里连续收到 3 次 E_020_001 就要求验证码），服务端的错误响应体
本身并不会告诉你"该去过验证码了"，所以没法单从一次登录失败的响应里判断出
是否需要验证码。也正因为 login() 只发一次请求、不会在同一个 session 上
反复拿同一个密码硬撞，这个阈值在实际场景里基本碰不到。

下面几个 _fetch_captcha_svg / _check_captcha / _solve_captcha 是验证码本身
三个接口的封装，留着当手动工具用（比如以后真遇到需要人工/脚本过一次验证码
的场景），但没有接入 login() 的默认流程：

    1. GET /api/captcha?width=85&height=48 拿一张验证码图（SVG，用
       <path> 描边画字符，不是 <text>，没法直接读文本，需要转成图片再
       识别，用的 ddddocr）。
    2. GET /api/checkCaptcha?captcha=<识别结果> 校验，返回
       {"data":{"status":true/false}}，状态记在这个 session 的
       connect.sid 上。
    3. status 为 true 之后，登录 POST 请求体本身还是只有
       username/password，不带验证码字段，正常重试登录接口就行。

Session 缓存：这个网站对账号强制单并发 session——如果之前的 session 还活着
就再登录一次，会被拒绝返回 {"code":"C_002_001","message":"用户并发超标"}。
所以 get_session() 不会每次都真登录，而是把 cookies + connect.sid 自己的
Expires 缓存到 Redis（session:{platform}:{username}）复用；只有在没有缓存、
缓存 cookie 标称的过期时间已经过了，或者 GET /api/autoLogin 说这个 cookie
其实已经失效了（服务端可能在 cookie 标称过期之前就让 session 失效，比如
被别处登录顶掉了，所以标称的过期时间本身不完全可信）这三种情况下，才会真的
走登录接口。

代理：每个账号终身绑定同一个固定代理（core/proxy_pool.py，在注册时设置）。
这个模块自己构建 session 时（没有显式传入 `session=`），都会去查这个绑定
关系并通过它路由——调用方完全不用操心代理的事，只管调
get_session(email, password)，拿到的 session 已经是对的 IP 了。唯一的例外
是注册本身：那时候账号还不存在，所以 registration_worker.py 会自己构建
带代理的 session 并显式传进来，因为这时候还没有绑定关系可查。
"""
from __future__ import annotations

import json
import time

import requests

from core import proxy_pool
from core.redis_client import get_client, k

from . import config

BASE = "https://law.wkinfo.com.cn"

CAPTCHA_SOLVE_ATTEMPTS = 3  # 手动跑 _solve_captcha 时，一张识别/校验失败就换下一张，最多试这么多次

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


def _cookie_expiry(session: requests.Session) -> float | None:
    """session 里所有 cookie 中最早的 Expires（unix 时间戳），如果有 cookie
    没有过期时间（会话级 cookie）就返回 None。"""
    expirations = [c.expires for c in session.cookies if c.expires]
    return min(expirations) if expirations else None


def _proxied_session(username: str) -> requests.Session:
    """一个全新的 session，如果这个账号已经绑定了代理就走它。如果还没有
    绑定关系（账号不是通过这套池子注册的，或者注册流程还没来得及绑定），
    就直接走直连。"""
    session = requests.Session()
    proxy_id = proxy_pool.get_account_proxy_id(config.PLATFORM, username)
    if proxy_id:
        session.proxies = proxy_pool.requests_proxies(proxy_id)
    return session


def _fetch_captcha_svg(session: requests.Session) -> str:
    """GET /api/captcha —— 拿一张验证码图，SVG 文本（<path> 描边画字符，
    不是 <text>），校验通过状态记在这个 session 的 connect.sid 上。"""
    resp = session.get(
        f"{BASE}/api/captcha", headers=HEADERS, params={"width": 85, "height": 48}, timeout=15
    )
    resp.raise_for_status()
    return resp.text


def _check_captcha(session: requests.Session, code: str) -> bool:
    """GET /api/checkCaptcha —— 校验识别结果，返回 {"data":{"status":bool}}。"""
    resp = session.get(
        f"{BASE}/api/checkCaptcha", headers=HEADERS, params={"captcha": code}, timeout=15
    )
    resp.raise_for_status()
    return bool(resp.json().get("data", {}).get("status"))


def _svg_to_png(svg_text: str) -> bytes:
    import cairosvg

    return cairosvg.svg2png(bytestring=svg_text.encode("utf-8"))


_ocr = None


def _get_ocr():
    global _ocr
    if _ocr is None:
        import ddddocr

        _ocr = ddddocr.DdddOcr(show_ad=False)
    return _ocr


def _solve_captcha(session: requests.Session) -> bool:
    """拉验证码图 -> 转 PNG -> ddddocr 识别 -> 校验，一张不中就换下一张，
    最多试 CAPTCHA_SOLVE_ATTEMPTS 次。校验通过（True）之后紧接着重试登录
    就会被服务端认，不需要把识别结果传给登录接口。"""
    ocr = _get_ocr()
    for _ in range(CAPTCHA_SOLVE_ATTEMPTS):
        svg = _fetch_captcha_svg(session)
        code = ocr.classification(_svg_to_png(svg))
        if _check_captcha(session, code):
            return True
    return False


class LoginError(RuntimeError):
    """登录失败，`code` 是服务端自己的错误码（比如 E_020_001 密码错误、
    E_020_002 账号不存在、C_002_001 并发超标），响应体解析不出 `code` 字段
    时是 None。调用方想按错误原因分别处理（比如只有账号不存在才需要走注册
    流程，密码错误就不该去注册）时用这个字段，不用去正则匹配异常文本。"""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.code = code


def login(username: str, password: str, session: requests.Session | None = None) -> tuple[requests.Session, dict]:
    """POST /csi/account/validate/ex，只发一次，不重试。成功返回
    (session, profile)；失败抛 LoginError（RuntimeError 的子类，向后兼容用
    `except RuntimeError` 的调用方），附带服务端自己的错误响应体和解析出来的
    `code`（密码错误 E_020_001、账号不存在 E_020_002，或者
    C_002_001——这个账号之前的 session 还活着）。同一个密码重试没有意义，
    交给调用方决定要不要换密码/换账号重试。

    密码错误/并发超标这些业务失败也是 HTTP 200，只是响应体带一个 `code` 字段
    （跟 search_client.py 的配额超限错误、registration_worker.py 的注册失败
    是同一个套路），所以不能只看 `resp.status_code`。但不能只看有没有顶层
    `code` 字段来判断——成功的 profile 响应体里也有一个 `code` 字段（客户
    编码，比如 "01BE01"，跟错误码是两码事），之前就是拿这个当错误码误判
    成功登录为失败，进而误走注册流程。错误响应体只有 `code`/`message` 两个
    字段，profile 里则一定有 `userEmail`，用它来区分。
    """
    session = session or _proxied_session(username)
    resp = session.post(
        f"{BASE}/csi/account/validate/ex",
        headers=HEADERS,
        json={"username": username, "password": password},
        timeout=15,
    )
    if resp.status_code != 200:
        try:
            code = resp.json().get("code")
        except ValueError:
            code = None
        raise LoginError(f"login failed: {resp.status_code} {resp.text}", code=code)
    body = resp.json()
    if isinstance(body, dict) and body.get("code") and "userEmail" not in body:
        raise LoginError(f"login failed: {body}", code=body.get("code"))
    return session, body


def logout(session: requests.Session) -> dict:
    """GET /api/logout —— 释放这个账号的并发session槽位。"""
    resp = session.get(f"{BASE}/api/logout", headers=HEADERS, timeout=15)
    return resp.json()


def is_session_valid(session: requests.Session) -> bool:
    """GET /api/autoLogin —— 如果 cookie 已经不再是登录态就返回 {"login": false}。"""
    resp = session.get(f"{BASE}/api/autoLogin", headers=HEADERS, timeout=15)
    return resp.status_code == 200 and resp.json().get("login") is True


def save_session(username: str, session: requests.Session, profile: dict) -> None:
    data = {
        "cookies": json.dumps(requests.utils.dict_from_cookiejar(session.cookies), ensure_ascii=False),
        "expires_at": _cookie_expiry(session) or "",
        "profile": json.dumps(profile, ensure_ascii=False),
        "saved_at": time.time(),
    }
    get_client().hmset(k("session", config.PLATFORM, username), data)


def load_cached_session(username: str) -> tuple[requests.Session, dict] | None:
    data = get_client().hgetall(k("session", config.PLATFORM, username))
    if not data:
        return None
    if data.get("expires_at") and float(data["expires_at"]) < time.time():
        return None  # cookie 自己标称的 Expires 已经过了
    session = _proxied_session(username)
    session.cookies.update(json.loads(data["cookies"]))
    if not is_session_valid(session):
        return None  # 服务端在 cookie 标称过期之前就让它失效了
    return session, json.loads(data["profile"])


def get_session(username: str, password: str, force: bool = False) -> tuple[requests.Session, dict]:
    """如果缓存的 session 还有效就直接用，否则真的登录一次并缓存结果。
    实际调用应该用这个函数。"""
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

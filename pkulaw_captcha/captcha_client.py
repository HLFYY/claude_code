"""
pkulaw.com 登录用的腾讯 Turing 验证码 + cas.pkulaw.com 邮箱验证码，全流程编排。

链路（详见 reverse-records/请求链路.md）：
  cap_union_prehandle（JSONP，拿挑战配置：sess/sid/pow_cfg/instruction/图片地址/tdc_path）
  -> 下载背景图 -> OCR 识别点击坐标（recognize.py）
  -> 下载 tdc.js -> 在 Node 沙箱里跑出 collect/eks（tdc_client.py）
  -> 本地爆破 pow_answer（pow_solver.py，纯 Python，跟 tdc.js 无关）
  -> POST cap_union_new_verify 提交，成功拿到 randstr + ticket
  -> POST send-email（需要 cas.pkulaw.com 的 Keycloak session cookie，见 get_cas_session()）
  -> POST verify-email （只校验验证码本身对不对，不会真正登录，见 verify_email_code）
  -> POST login-actions/authenticate （这一步才是真正完成 Keycloak 登录、种下
     pkulaw.com 正式 session cookie / access_token 的地方，见 complete_login）
"""
from __future__ import annotations

import json
import random
import re
import string

import requests
from lxml import etree

import config
import eo_challenge
import pow_solver
import recognize
import tdc_client

# kc-form-login5（邮箱登录 tab）的 action URL 长这样，session_code/execution/tab_id
# 三个值就在这个 URL 里，见 请求链路.md 的"完成登录"小节。
_LOGIN_FORM_ACTION_RE = re.compile(
    r"login-actions/authenticate\?session_code=([^&\"]+)&amp;execution=([^&\"]+)"
    r"&amp;client_id=pkulaw&amp;tab_id=([^\"&]+)"
)

_KNOWN_EMAIL_DOMAINS = {"gmail.com", "qq.com", "hotmail.com", "icloud.com"}


def _random_callback() -> str:
    return "_aq_" + "".join(random.choices(string.digits, k=6))


def get_cas_session() -> tuple[requests.Session, dict]:
    """访问 CAS 登录入口，拿到 Keycloak 的 AUTH_SESSION_ID 等 cookie
    （send-email/verify-email/complete_login 都需要，captcha 本身不需要，因为
    turing.captcha.qcloud.com 是完全独立的第三方域名）。

    同一个页面的 HTML 里也带着 complete_login() 要用的 session_code/execution/tab_id
    （登录表单 action URL 里的参数，见模块顶部 _LOGIN_FORM_ACTION_RE），一起解析出来，
    不用再多发一次请求。

    返回 (session, login_form_ctx)。
    """
    session = requests.Session()
    session.headers.update({"user-agent": config.UA})
    params = {
        "scope": "openid", "response_type": "code", "client_id": "pkulaw",
        "ui_locales": "zh-CN", "kc_locale": "zh-CN", "has_close": "true",
        "redirect_uri": (
            "https://static.pkulaw.com/statics/kc/index.html?"
            "redirect_path=https%3A%2F%2Fwww.pkulaw.com%2Fcase%3Fway%3DtopGuid"
        ),
    }
    resp = session.get(
        f"{config.CAS_BASE}/auth/realms/fabao/protocol/openid-connect/auth",
        params=params, timeout=15,
    )
    resp.raise_for_status()

    m = _LOGIN_FORM_ACTION_RE.search(resp.text)
    if not m:
        raise RuntimeError("get_cas_session: 登录表单页面里没找到 session_code/execution/tab_id，页面结构可能变了")
    login_ctx = {"session_code": m.group(1), "execution": m.group(2), "tab_id": m.group(3)}
    return session, login_ctx


def get_challenge(session: requests.Session | None = None) -> dict:
    """GET cap_union_prehandle，返回解析后的挑战信息字典。"""
    s = session or requests
    callback = _random_callback()
    params = {
        "aid": config.AID, "protocol": "https", "accver": "1", "showtype": "popup",
        "ua": __import__("base64").b64encode(config.UA.encode()).decode(),
        "noheader": "1", "fb": "0", "aged": "0", "enableAged": "0", "enableDarkMode": "0",
        "grayscale": "1", "clientype": "2", "cap_cd": "", "uid": "", "lang": "en",
        "entry_url": config.ENTRY_URL, "elder_captcha": "0", "js": config.JS_PATH,
        "login_appid": "", "wb": "1", "subsid": "1", "callback": callback, "sess": "",
    }
    resp = s.get(f"{config.CAPTCHA_BASE}/cap_union_prehandle", params=params,
                 headers=config.CAPTCHA_HEADERS, timeout=15)
    resp.raise_for_status()
    text = resp.text
    body = text[len(callback) + 1: -1]  # strip "callback(" ... ")"
    data = json.loads(body)

    comm_cfg = data["data"]["comm_captcha_cfg"]
    dyn_info = data["data"]["dyn_show_info"]
    return {
        "sess": data["sess"],
        "sid": data["sid"],
        "tdc_path": comm_cfg["tdc_path"],
        "pow_prefix": comm_cfg["pow_cfg"]["prefix"],
        "pow_md5": comm_cfg["pow_cfg"]["md5"],
        "instruction": dyn_info["instruction"],
        "img_url": dyn_info["bg_elem_cfg"]["img_url"],
    }


def solve_and_verify(session: requests.Session | None = None, max_attempts: int = 6) -> dict:
    """完整走一遍：拿挑战 -> OCR -> tdc collect/eks -> pow -> 提交 verify。
    OCR 识别不出全部目标字时换一张新挑战重试（跟 wkinfo 项目的 clickWord 套路一致，
    重新拉验证码不要钱，没必要在单张图上死磕）。
    返回 cap_union_new_verify 的解析后响应（成功时含 randstr / ticket）。"""
    s = session or requests

    challenge = points = None
    for _ in range(max_attempts):
        challenge = get_challenge(s)
        img_resp = s.get(config.CAPTCHA_BASE + challenge["img_url"],
                          headers=config.CAPTCHA_HEADERS, timeout=15)
        img_resp.raise_for_status()
        target_chars = recognize.parse_instruction(challenge["instruction"])
        points = recognize.detect_click_points(img_resp.content, target_chars)
        if points is not None:
            break
        print(f"recognize failed for instruction={challenge['instruction']!r}, retrying with a fresh challenge...")
    if points is None:
        raise RuntimeError(f"recognize kept failing after {max_attempts} attempts")

    tdc_src = tdc_client.fetch_tdc_js(challenge["tdc_path"], s)
    tdc_data = tdc_client.get_collect_and_eks(tdc_src)

    pow_answer, _nonce, pow_calc_time = pow_solver.solve(challenge["pow_prefix"], challenge["pow_md5"])

    ans = [
        {"elem_id": i + 1, "type": "DynAnswerType_POS", "data": f"{p['x']},{p['y']}"}
        for i, p in enumerate(points)
    ]
    body = {
        "collect": tdc_data["collect"],
        "tlg": len(tdc_data["collect"]),
        "eks": tdc_data["eks"],
        "sess": challenge["sess"],
        "ans": json.dumps(ans, ensure_ascii=False, separators=(",", ":")),
        "pow_answer": pow_answer,
        "pow_calc_time": pow_calc_time,
    }
    resp = s.post(f"{config.CAPTCHA_BASE}/cap_union_new_verify", data=body,
                  headers=config.VERIFY_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()


def send_email_code(session: requests.Session, email: str, randstr: str, ticket: str) -> dict:
    """需要先用 get_cas_session() 建立好 Keycloak session cookie 再调用。
    会真实触发一封邮件发送，谨慎调用。"""
    params = {"email": email, "randstr": randstr, "ticket": ticket}
    resp = session.post(
        f"{config.CAS_BASE}/auth/realms/fabao/sms/code/send-email",
        params=params, headers={"x-requested-with": "XMLHttpRequest", "user-agent": config.UA},
        timeout=15,
    )
    return resp.json()


def verify_email_code(session: requests.Session, email: str, code: str) -> dict:
    """验证码正确时服务端返回 204 No Content（真实测试确认过，空 body，不能直接 .json()）；
    验证码错误时返回 400 + {"error":"CodeNotExist"}（见 请求链路.md）。"""
    params = {"email": email, "code": code}
    resp = session.post(
        f"{config.CAS_BASE}/auth/realms/fabao/sms/code/verify-email",
        params=params, headers={"x-requested-with": "XMLHttpRequest", "user-agent": config.UA},
        timeout=15,
    )
    if resp.status_code == 204:
        return {"success": True}
    return {"success": False, "status_code": resp.status_code, "body": _safe_json(resp)}


def _safe_json(resp: requests.Response):
    try:
        return resp.json()
    except ValueError:
        return resp.text


def complete_login(session: requests.Session, login_ctx: dict, email: str, email_code: str) -> requests.Response:
    """提交 Keycloak 邮箱登录表单（登录页面 HTML 里的 id="kc-form-login5"）并走完最后
    一跳授权码兑换，这是真正让服务端完成登录、在 www.pkulaw.com 域下种下正式 session
    cookie（pkulaw_v6_sessionid/authormes/LoginAccount 等）和 access_token 的完整过程。
    跟 verify_email_code 是两个独立接口——那个只校验验证码本身对不对/是否过期，不会
    产生登录态，详见 请求链路.md。

    邮箱本地部分和后缀分开填：常见后缀（gmail/qq/hotmail/icloud）emailSuffix 留空，
    其他后缀按页面逻辑应该回填到 emailSuffix（页面提示"其他邮箱注册时需待人工审核"，
    实测过 163.com 这类冷门后缀确实卡在这一步，登录不成功，属于网站自己的限制，不是
    这份代码的问题）。

    两跳组成：
    1. POST login-actions/authenticate，requests 默认 allow_redirects=True 会自动跟到
       第一跳落地页 static.pkulaw.com/statics/kc/index.html?redirect_path=...&code=...
       （这一跳是纯 302，不需要 JS）。
    2. 落地页本身的 JS 是 `location.href = redirect_path + '?code=' + code`，但那样拼出来
       的 URL 会把 redirect_path 已有的 "?way=topGuid" 和 "?code=" 撞在一起变成非法的
       两个问号；实测真正生效、也是真实浏览器最终成功的形态是用 "&" 拼接
       （redirect_path + '&code=' + code），这里手动构造并再发一次 GET 完成真正的授权码
       兑换，落地后 session 里才会出现 pkulaw_v6_sessionid 这些 www.pkulaw.com 的正式
       登录 cookie。
    """
    email_prefix, _, email_domain = email.partition("@")
    email_suffix = "" if email_domain.lower() in _KNOWN_EMAIL_DOMAINS else email_domain

    params = {
        "session_code": login_ctx["session_code"],
        "execution": login_ctx["execution"],
        "client_id": "pkulaw",
        "tab_id": login_ctx["tab_id"],
    }
    body = {
        "email": email, "loginType": "5", "tabType": "emailValidate", "source": "",
        "emailPrefix": email_prefix, "emailSuffix": email_suffix, "emailCode": email_code,
    }
    resp = session.post(
        f"{config.CAS_BASE}/auth/realms/fabao/login-actions/authenticate",
        params=params, data=body,
        headers={"content-type": "application/x-www-form-urlencoded", "user-agent": config.UA, "origin": "null"},
        timeout=15,
    )
    resp.raise_for_status()

    parsed = requests.utils.urlparse(resp.url)
    if "statics/kc/index.html" not in parsed.path:
        # 没有落到中转页，八成是邮箱后缀不在白名单里（163.com 这种）导致登录被卡住，
        # 或者验证码/session_code 过期了 -- 表单会原样重新渲染登录页而不是跳转。
        raise RuntimeError(
            "complete_login: 没有走到 statics/kc/index.html 中转页，登录没有真正完成"
            "（常见原因：邮箱后缀不在白名单 gmail/qq/hotmail/icloud 里，或验证码已过期）。"
            f" 实际落地页: {resp.url}"
        )
    qs = dict(p.split("=", 1) for p in parsed.query.split("&") if "=" in p)
    redirect_path = requests.utils.unquote(qs["redirect_path"])
    auth_code = qs["code"]
    sep = "&" if "?" in redirect_path else "?"
    final_resp = session.get(f"{redirect_path}{sep}code={auth_code}",
                              headers={"user-agent": config.UA}, timeout=15)
    final_resp.raise_for_status()
    return final_resp


def is_logged_in(session: requests.Session) -> bool:
    """用当前 session 直接请求首页，靠响应头 islogin 判断（真实抓包确认过：已登录时
    islogin:1，见 请求链路.md）。"""
    resp = session.get(config.WWW_BASE + "/case?way=topGuid", headers={"user-agent": config.UA}, timeout=15)
    return resp.headers.get("islogin") == "1"


def dump_cookies(session: requests.Session) -> dict:
    """把 session 当前的 cookie 导出成普通 dict，方便打印/落盘复用
    （比如喂给别的脚本当 requests.get(..., cookies=...) 用）。"""
    return requests.utils.dict_from_cookiejar(session.cookies)


if __name__ == "__main__":
    email = input("邮箱: ").strip()

    session, login_ctx = get_cas_session()

    verify_result = solve_and_verify(session)
    print("captcha verify:", json.dumps(verify_result, ensure_ascii=False))
    if verify_result.get("errorCode") != "0":
        raise SystemExit("验证码没通过，退出")

    send_result = send_email_code(session, email, verify_result["randstr"], verify_result["ticket"])
    print("send_email_code:", json.dumps(send_result, ensure_ascii=False))

    code = input(f"邮箱验证码已发送到 {email}，请输入收到的验证码: ").strip()

    verify_code_result = verify_email_code(session, email, code)
    print("verify_email_code:", json.dumps(verify_code_result, ensure_ascii=False))

    complete_login(session, login_ctx, email, code)
    print("is_logged_in:", is_logged_in(session))

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    }

    article_url = 'https://www.pkulaw.com/qikan/5c6347f6bc4c4866bdca50e0aff747f0bdfb.html'
    # get_with_challenge_retry 跟 session.get 用法一样，多做的事：先按原样请求一次，
    # 如果响应是 eo_challenge.is_challenge_page() 判定的 EdgeOne JS cookie 挑战页
    # （短 + 含 EO_Bot_Ssid 字符串），就把页面里那段 <script> 丢进 Node 沙箱
    # （eo_challenge_bridge.js）真实跑一遍拿到它要设置的 cookie，塞进 session 后
    # 用新 cookie 重新请求同一个 URL，最多重试 max_retries 次。不是挑战页的话
    # 跟直接调 session.get 完全一样，没有额外开销。
    response = eo_challenge.get_with_challenge_retry(session, article_url, headers=headers, timeout=15)
    if eo_challenge.is_challenge_page(response):
        raise SystemExit(f"还是拿到 EdgeOne 挑战页，重试 {2} 次后仍未通过，可能触发了更严格的风控")

    # 走到这里 session.cookies 里除了登录态，还会带上 get_with_challenge_retry 解出来的
    # __tst_status/EO_Bot_Ssid（如果这次撞上了挑战的话）——这时候打印出来的才是"能直接
    # 拿去用的完整可用 cookie"，后面同一个 session 发的请求都会自动带上这两个值，
    # 不会再撞挑战。
    print("请求文章页之后的完整 cookies（含 EdgeOne 挑战 cookie，如果撞上过的话）:")
    print(json.dumps(dump_cookies(session), ensure_ascii=False, indent=2))

    html_res = etree.HTML(response.text)
    if not html_res.xpath('//*[@class="content"]'):
        print(response.text)
    content = html_res.xpath('//*[@class="content"]')[0]
    content_html = etree.tostring(content, encoding='unicode', method='html')
    print(content_html)
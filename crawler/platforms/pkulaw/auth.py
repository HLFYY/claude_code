"""
pkulaw.com 登录用的腾讯 Turing 验证码 + cas.pkulaw.com 邮箱验证码，全流程编排。

链路（详见 HANDOFF.md）：
  cap_union_prehandle（JSONP，拿挑战配置：sess/sid/pow_cfg/instruction/图片地址/tdc_path）
  -> 下载背景图 -> OCR 识别点击坐标（recognize.py）
  -> 下载 tdc.js -> 在 Node 沙箱里跑出 collect/eks（tdc_client.py）
  -> 本地爆破 pow_answer（pow_solver.py，纯 Python，跟 tdc.js 无关）
  -> POST cap_union_new_verify 提交，成功拿到 randstr + ticket
  -> POST send-email（需要 cas.pkulaw.com 的 Keycloak session cookie，见 get_cas_session()）
  -> POST verify-email （只校验验证码本身对不对，不会真正登录，见 verify_email_code）
  -> POST login-actions/authenticate （这一步才是真正完成 Keycloak 登录、种下
     pkulaw.com 正式 session cookie / access_token 的地方，见 complete_login）

这一层只管"怎么登录"，不管 session 要不要缓存/复用——那是 login.py 的事
（跟 wkinfo 平台 captcha_client.py / login.py 的分工一致）。
"""
from __future__ import annotations

import html
import json
import random
import re
import string

import requests
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from core import code_throttle
from core.logger import log

from . import config, pow_solver, recognize, tdc_client

# kc-form-login5（邮箱登录 tab）的 action URL 长这样，session_code/execution/tab_id
# 三个值就在这个 URL 里，见 HANDOFF.md 的"完成登录"小节。
_LOGIN_FORM_ACTION_RE = re.compile(
    r"login-actions/authenticate\?session_code=([^&\"]+)&amp;execution=([^&\"]+)"
    r"&amp;client_id=pkulaw&amp;tab_id=([^\"&]+)"
)

# 密码登录表单（kc-form-login，不带数字后缀）里的隐藏字段，服务端每次下发的
# 32字符密钥，用来加密 passwordFront，见 complete_login_by_password/encrypt_password。
_ENCRYPTION_KEY_RE = re.compile(r'id="encryptionKey"\s+name="encryptionKey"\s+value="([^"]+)"')

# 密码登录加密用的固定 IV，写死在登录页内联 JS 的 encryption() 函数里，不是每次变的
# （真实抓包逆向确认，2026-07-30）。
_PASSWORD_LOGIN_IV = b"5485693214587452"

# 登录页 HTML 里各个输入框下面的提示占位 div，形如
# <div id="phoneNumberTip" class="tips">具体提示文案</div>（正常情况下是空的
# <div id="xxxTip" class="tips"></div>，服务端判定这次提交有问题时才会把错误文案
# 塞进去）。真实抓包确认（2026-07-30）：服务端会把同一条错误文案同时塞进好几个
# tab 各自的 tip div 里（不只是当前激活 tab 那个），所以这里把所有非空的都提出来，
# 不挑 tab。
_TIP_DIV_RE = re.compile(r'id="(\w*Tip)"\s+class="tips">(.*?)</div>', re.DOTALL)


def _extract_tip_messages(resp_text: str) -> dict[str, str]:
    """解析登录页响应 HTML 里所有非空的 .tips 提示文案，键是 div id
    （phoneNumberTip/usernameTip/smsCodeTip/userPasswordTip/emailCodeTip 等），
    值是 HTML 反转义、去空白后的文案本身。解析不到就返回空 dict——不是所有失败
    都会有文案（比如纯网络错误），调用方要按"能解析到就用，解析不到就退化成
    _response_snippet"处理。"""
    messages = {}
    for div_id, raw_text in _TIP_DIV_RE.findall(resp_text):
        text = html.unescape(raw_text).strip()
        if text:
            messages[div_id] = text
    return messages


def _response_snippet(resp: requests.Response, length: int = 300) -> str:
    """登录表单提交失败时（没走到中转页）没有结构化的错误信息可读——响应是整页
    HTML，不是 JSON——先把开头一段原样带进异常里，方便下次失败时不用重新抓包
    就能看出是"验证码错误"还是别的原因。只在 _extract_tip_messages 解析不到任何
    文案时才需要退化到这个兜底片段。"""
    return " ".join(resp.text.split())[:length]


# 账号被网站转人工审核时，登录失败响应的整页 HTML 里会带这行提示（真实抓包确认，
# 2026-07-30，xingchi660@gmail.com——注意这个邮箱后缀在白名单 gmail/qq/hotmail/
# icloud 里，证明"白名单域名不会被转审核"是错误假设，只有这行文字才是可靠信号）。
# 原文是 "请回复邮件进行审核，如有疑问可联系客服。"（页面里逗号是 HTML 实体
# &#xff0c;），这里只匹配不含逗号的稳定前缀，不依赖具体转义方式。
_PENDING_REVIEW_MARKER = "请回复邮件进行审核"


class AccountPendingReviewError(RuntimeError):
    """登录表单响应页面里带着 _PENDING_REVIEW_MARKER 这行提示——账号被网站转了
    人工审核，不是验证码错误，也跟邮箱域名在不在白名单无关（白名单域名一样会被转
    审核，真实撞到过）。调用方（login.py）应该识别这个异常类型，提示用户去查收
    审核邮件，而不是当成普通登录失败重试。"""


def _random_callback() -> str:
    return "_aq_" + "".join(random.choices(string.digits, k=6))


def get_cas_session(session: requests.Session | None = None) -> tuple[requests.Session, dict]:
    """访问 CAS 登录入口，拿到 Keycloak 的 AUTH_SESSION_ID 等 cookie
    （send-email/verify-email/complete_login 都需要，captcha 本身不需要，因为
    turing.captcha.qcloud.com 是完全独立的第三方域名）。

    同一个页面的 HTML 里也带着 complete_login() 要用的 session_code/execution/tab_id
    （登录表单 action URL 里的参数，见模块顶部 _LOGIN_FORM_ACTION_RE），一起解析出来，
    不用再多发一次请求。

    传 `session` 用调用方已经建好的 session（比如 login.py 里已经挂好账号绑定代理
    的那个），不传就现建一个直连的；两种情况都会在这个 session 上继续发后续请求。

    返回 (session, login_form_ctx)。
    """
    session = session or requests.Session()
    session.headers.update({"user-agent": config.UA})
    params = {
        "scope": "openid", "response_type": "code", "client_id": "pkulaw",
        "ui_locales": "zh-CN", "kc_locale": "zh-CN", "has_close": "true",
        "redirect_uri": config.KC_REDIRECT_URI,
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

    # 密码登录用的 encryptionKey 也在同一个页面里（kc-form-login 表单的隐藏字段），
    # 邮箱/手机验证码登录用不到，解析不到就不强求（不是所有页面变体都一定有这个表单）。
    key_m = _ENCRYPTION_KEY_RE.search(resp.text)
    if key_m:
        login_ctx["encryption_key"] = key_m.group(1)

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
    OCR 识别不出全部目标字时换一张新挑战重试（跟 wkinfo 平台 clickWord 套路一致，
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
        log(config.PLATFORM, f"recognize failed for instruction={challenge['instruction']!r}, "
                              f"retrying with a fresh challenge...")
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
    会真实触发一封邮件发送，谨慎调用。

    实测遇到过两种限流错误，都是 {"error": "..."} 这个形状：
    - "limit_day"：同一个邮箱当天发送验证码次数到上限了，跟 wkinfo 平台"手机号
      当日注册次数上限"是同一类防滥用限流。
    - "limit_minute"：分钟级频率限制——短时间内对同一个账号连续发了两次验证码
      （比如登录发一次、紧接着改密码又要发一次）就会撞上，跟 limit_day 是两个
      独立的限流维度。send_phone_code 也是同一套（真实撞到过，2026-07-30：
      手机号登录后立刻改密码，改密码那次发验证码直接返回
      {"error":"limit_minute"}）。

    现在会先调 core.code_throttle.wait_before_send() 主动防一手——同一个账号
    距上次发验证码不到 config.CODE_SEND_MIN_INTERVAL_SECONDS（75秒）就会先打日志
    说明还要等多久、真的睡够那么久再发，而不是被动等服务端拒绝了再处理。
    """
    code_throttle.wait_before_send(config.PLATFORM, email, config.CODE_SEND_MIN_INTERVAL_SECONDS)
    params = {"email": email, "randstr": randstr, "ticket": ticket}
    resp = session.post(
        f"{config.CAS_BASE}/auth/realms/fabao/sms/code/send-email",
        params=params, headers={"x-requested-with": "XMLHttpRequest", "user-agent": config.UA},
        timeout=15,
    )
    code_throttle.mark_sent(config.PLATFORM, email, config.CODE_SEND_MIN_INTERVAL_SECONDS)
    return resp.json()


def verify_email_code(session: requests.Session, email: str, code: str) -> dict:
    """验证码正确时服务端返回 204 No Content（真实测试确认过，空 body，不能直接 .json()）；
    验证码错误时返回 400 + {"error":"CodeNotExist"}（见 HANDOFF.md）。"""
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


def verify_phone_code(session: requests.Session, phone: str, code: str) -> dict:
    """跟 verify_email_code 是同一类接口，换成手机号。204=对，
    400+{"error":"CodeNotExist"}=错（推断跟邮箱版一致，形状还没专门抓包确认，
    但 modify-password-by-phone 走的是不单独 verify、直接带 code 提交那条路，
    这个函数目前没被用到，留着是为了跟 verify_email_code 对称、以后要单独校验
    手机验证码时能用）。"""
    params = {"phoneNumber": phone, "code": code}
    resp = session.post(
        f"{config.CAS_BASE}/auth/realms/fabao/sms/code/verify",
        params=params, headers={"x-requested-with": "XMLHttpRequest", "user-agent": config.UA},
        timeout=15,
    )
    if resp.status_code == 204:
        return {"success": True}
    return {"success": False, "status_code": resp.status_code, "body": _safe_json(resp)}


def complete_login_by_phone(session: requests.Session, login_ctx: dict, phone: str, phone_code: str) -> tuple[requests.Response, str]:
    """手机号验证码登录，跟 complete_login（邮箱版）是同一个 login-actions/authenticate
    机制，字段不一样：邮箱是 loginType=5/tabType=emailValidate/emailPrefix+
    emailSuffix+emailCode，手机号是 loginType=0/tabType=phoneValidate/phoneNumber+
    smsCode（真实抓包确认，2026-07-30，之前误以为 cas-ipv6.pkulaw.com/sms/ipv6-login
    是登录提交接口，实际只是个 IP 检测请求，真正的提交是这个）。

    后续的授权码兑换跳转（302 -> openid-connect/auth?...&sign=...&loginType=phone ->
    static.pkulaw.com/kc/index.html?...&code=...&loginType=phone）走的是跟邮箱完全
    一样的两跳机制，逻辑照抄 complete_login。

    返回 (final_resp, auth_code)，同 complete_login。
    """
    params = {
        "session_code": login_ctx["session_code"],
        "execution": login_ctx["execution"],
        "client_id": "pkulaw",
        "tab_id": login_ctx["tab_id"],
    }
    body = {
        "loginType": "0", "tabType": "phoneValidate", "source": "",
        "phoneNumber": phone, "smsCode": phone_code,
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
        tips = _extract_tip_messages(resp.text)
        reason = " / ".join(sorted(set(tips.values()))) if tips else None
        if reason and _PENDING_REVIEW_MARKER in reason:
            raise AccountPendingReviewError(
                f"complete_login_by_phone: {phone} 登录没有走到中转页，页面解析出的提示："
                f"「{reason}」，账号被网站转了人工审核。实际落地页: {resp.url}"
            )
        raise RuntimeError(
            "complete_login_by_phone: 没有走到 statics/kc/index.html 中转页，登录没有真正完成。"
            f" 实际落地页: {resp.url}\n"
            + (f"页面解析出的提示：{reason}" if reason else
               f"页面没解析到具体提示文案，响应片段: {_response_snippet(resp)}")
        )
    qs = dict(p.split("=", 1) for p in parsed.query.split("&") if "=" in p)
    redirect_path = requests.utils.unquote(qs["redirect_path"])
    auth_code = qs["code"]
    sep = "&" if "?" in redirect_path else "?"
    final_resp = session.get(f"{redirect_path}{sep}code={auth_code}",
                              headers={"user-agent": config.UA}, timeout=15)
    final_resp.raise_for_status()
    return final_resp, auth_code


def complete_login(session: requests.Session, login_ctx: dict, email: str, email_code: str) -> tuple[requests.Response, str]:
    """提交 Keycloak 邮箱登录表单（登录页面 HTML 里的 id="kc-form-login5"）并走完最后
    一跳授权码兑换，这是真正让服务端完成登录、在 www.pkulaw.com 域下种下正式 session
    cookie（pkulaw_v6_sessionid/authormes/LoginAccount 等）和 access_token 的完整过程。
    跟 verify_email_code 是两个独立接口——那个只校验验证码本身对不对/是否过期，不会
    产生登录态，详见 HANDOFF.md。

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

    返回 (final_resp, auth_code)——auth_code 就是这一跳兑换用的 OAuth 授权码，
    还可以再拿去换 gateway.pkulaw.com 接口用的 Bearer access_token（见
    exchange_code_for_token），modify-password-by-* 这类接口就需要这个。
    这个 code 能不能跟这里的 GET 兑换共用、还是单次消费型，目前没有实测确认，
    调用方如果两边都要用，建议先测一次。
    """
    email_prefix, _, email_domain = email.partition("@")
    email_suffix = "" if email_domain.lower() in config.KNOWN_EMAIL_DOMAINS else email_domain

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
        tips = _extract_tip_messages(resp.text)
        reason = " / ".join(sorted(set(tips.values()))) if tips else None
        if reason and _PENDING_REVIEW_MARKER in reason:
            raise AccountPendingReviewError(
                f"complete_login: {email} 登录没有走到中转页，页面解析出的提示：「{reason}」，"
                "账号被网站转了人工审核（即使邮箱后缀在白名单 gmail/qq/hotmail/icloud 里也可能"
                "发生）。请查收这个邮箱有没有收到 market@chinalawinfo.com 的审核邮件并回复，"
                f"审核通过后再重试。实际落地页: {resp.url}"
            )
        raise RuntimeError(
            "complete_login: 没有走到 statics/kc/index.html 中转页，登录没有真正完成。"
            f" 实际落地页: {resp.url}\n"
            + (f"页面解析出的提示：{reason}" if reason else
               f"页面没解析到具体提示文案，响应片段: {_response_snippet(resp)}")
        )
    qs = dict(p.split("=", 1) for p in parsed.query.split("&") if "=" in p)
    redirect_path = requests.utils.unquote(qs["redirect_path"])
    auth_code = qs["code"]
    sep = "&" if "?" in redirect_path else "?"
    final_resp = session.get(f"{redirect_path}{sep}code={auth_code}",
                              headers={"user-agent": config.UA}, timeout=15)
    final_resp.raise_for_status()
    return final_resp, auth_code


_PAGE_ACCESS_TOKEN_RE = re.compile(r'id="access_token"\s+value="([^"]+)"')


def is_logged_in(session: requests.Session) -> bool:
    """用当前 session 直接请求首页，同时看两个信号：响应头 islogin（真实抓包确认过：
    已登录时 islogin:1，见 HANDOFF.md）和页面 HTML 里有没有嵌入 access_token 隐藏
    字段（get_token_from_page() 用的就是这个字段，同一个响应体一次性取两个信号，
    不多发一次请求）。

    只看 islogin 头不够可靠——2026-08-06 实测碰到过 islogin:1、但页面里其实没有
    access_token 字段的"半失效" session（大概率是 Keycloak 后端 session 已经失效，
    但 islogin 头走的是另一条更松的校验路径，没跟着一起失效）：这种 session 看起来
    是登录状态，但后续请求 gateway.pkulaw.com 的任何接口（比如改密码要用的
    access_token，见 refresh_access_token/get_token_from_page 两条路都会失败）全部
    会失败。两个信号都成立才认为这个 session 真的还能用——这也是
    load_cached_session() 里一直以来的假设（旧代码注释写的"能过 is_logged_in 检查
    就一定能用 get_token_from_page 取到 token"），现在把这个假设做成显式校验，而不是
    停留在假设上。"""
    resp = session.get(config.WWW_BASE + "/case?way=topGuid", headers={"user-agent": config.UA}, timeout=15)
    if resp.headers.get("islogin") != "1":
        return False
    return _PAGE_ACCESS_TOKEN_RE.search(resp.text) is not None


def exchange_code_for_token(session: requests.Session, code: str, redirect_uri: str) -> dict:
    """用登录时拿到的 OAuth 授权码换一个 gateway.pkulaw.com 专用的 Bearer
    access_token（+ refresh_token，实测 refresh 接口其实没真的用它，见
    refresh_access_token）。这个 token 是浏览 www.pkulaw.com 页面用的 Keycloak
    session cookie 之外的另一套独立认证，modify-password-by-*/用户信息这类
    gateway.pkulaw.com 接口都要靠它，不认 cookie。

    真实抓包确认 exp-iat 是 1800 秒（30 分钟）。redirect_uri 必须跟当初登录时
    传给 Keycloak 的那个一致（这里默认传 complete_login 里用的
    www.pkulaw.com/case?way=topGuid，跟别的入口登录就要传别的值，比如从用户中心
    页面登录时抓到的是 https://www.pkulaw.com/cooperative/usercenter/person）。

    **实测确认（2026-07-30）：这个 code 是一次性消费的**——complete_login /
    complete_login_by_phone / complete_login_by_password 自己最后那一跳落地 GET
    已经把 code 消费掉了，这里再拿同一个 code 来换会 401
    "Authorization Required"。也就是说这个函数目前实际上换不到 token（除非能
    找到一个"只换 token、不做落地 GET"的调用方式，还没找到），modify-password
    这类需要 access_token 的场景改用 get_token_from_page()（不依赖这个一次性
    code，从任意已登录页面的 HTML 里现取当前有效的 token）更可靠。这个函数还留着
    是为了记录这条路径的真实结论，不建议依赖它拿到能用的 token。
    """
    body = {"clientId": "pkulaw", "code": code, "redirectUri": redirect_uri}
    resp = session.post(f"{config.WWW_BASE}/gateway/account/auth/token",
                         json=body, headers={"user-agent": config.UA}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_token_from_page(session: requests.Session, url: str | None = None) -> str:
    """不依赖那个一次性 OAuth code，直接从任意一个已登录页面的 HTML 里现取当前
    有效的 access_token——已登录状态下渲染的 www.pkulaw.com 页面（比如
    /case?way=topGuid）会在 `<input type="hidden" id="access_token" value="...">`
    里内嵌一份服务端刚生成、当前仍然有效的 token（真实抓包确认过，2026-07-30）。
    比 exchange_code_for_token 可靠，因为不涉及那个已知会被消费掉的一次性 code。
    """
    resp = session.get(url or f"{config.WWW_BASE}/case?way=topGuid",
                        headers={"user-agent": config.UA}, timeout=15)
    resp.raise_for_status()
    m = _PAGE_ACCESS_TOKEN_RE.search(resp.text)
    if not m:
        raise RuntimeError(f"get_token_from_page: 页面里没找到 access_token 隐藏字段（可能没登录成功）-- {url}")
    return m.group(1)


def refresh_access_token(session: requests.Session, access_token: str) -> str:
    """access_token 过期后刷新。注意这个接口实际靠的是 session 自带的 Keycloak
    cookie（AUTH_SESSION_ID/session_state 等）校验，不是 JWT 自身的 refresh_token
    字段真的生效——所以 session 必须是登录时那个还带着 cookie 的 session，不能是
    随便拿旧 access_token 配一个新 session。返回新的 access_token 字符串。"""
    body = {"access_token": access_token, "client_id": "pkulaw"}
    resp = session.post(f"{config.WWW_BASE}/gateway/account/auth/refreshtoken",
                         json=body, headers={"user-agent": config.UA}, timeout=15)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _auth_headers(access_token: str) -> dict:
    return {"authorization": f"Bearer {access_token}", "user-agent": config.UA}


def get_current_user(access_token: str) -> dict:
    """GET gateway.pkulaw.com/user-register/user，返回当前登录账号的信息
    （id/phone/phoneValidate/emailValidate/userName 等，真实字段见 HANDOFF.md）。"""
    resp = requests.get(f"{config.GATEWAY_BASE}/user-register/user",
                         headers=_auth_headers(access_token), timeout=15)
    resp.raise_for_status()
    return resp.json()


def send_phone_code(session: requests.Session, phone: str, randstr: str, ticket: str) -> dict:
    """跟 send_email_code 是同一类接口，换成手机号（改密码/登录都用得到），
    limit_day/limit_minute 两种限流、code_throttle 节流也是同一套，见
    send_email_code 的说明。"""
    code_throttle.wait_before_send(config.PLATFORM, phone, config.CODE_SEND_MIN_INTERVAL_SECONDS)
    params = {"phoneNumber": phone, "randstr": randstr, "ticket": ticket}
    resp = session.post(
        f"{config.CAS_BASE}/auth/realms/fabao/sms/code/send",
        params=params, headers={"x-requested-with": "XMLHttpRequest", "user-agent": config.UA},
        timeout=15,
    )
    code_throttle.mark_sent(config.PLATFORM, phone, config.CODE_SEND_MIN_INTERVAL_SECONDS)
    return resp.json()


def modify_password_by_phone(access_token: str, phone: str, code: str, new_password: str) -> dict:
    """PUT gateway.pkulaw.com/user-register/user/modify-password-by-phone。
    不需要像登录那样先单独 verify 一次验证码，code 直接和新密码一起提交。
    真实抓包确认成功返回 {"code":"200"}。"""
    body = {"phone": phone, "code": code, "password": new_password}
    resp = requests.put(f"{config.GATEWAY_BASE}/user-register/user/modify-password-by-phone",
                         json=body, headers=_auth_headers(access_token), timeout=15)
    resp.raise_for_status()
    return resp.json()


def modify_password_by_email(access_token: str, email: str, code: str, new_password: str) -> dict:
    """PUT gateway.pkulaw.com/user-register/user/modify-password-by-email。
    跟 modify_password_by_phone 是同一个接口的邮箱版本，字段形状按对称推断
    （还没有真实抓包确认过邮箱这一路，见 HANDOFF.md 待办）。"""
    body = {"email": email, "code": code, "password": new_password}
    resp = requests.put(f"{config.GATEWAY_BASE}/user-register/user/modify-password-by-email",
                         json=body, headers=_auth_headers(access_token), timeout=15)
    resp.raise_for_status()
    return resp.json()


def encrypt_password(password: str, encryption_key: str) -> str:
    """密码登录用的密码加密。AES-256-CBC：key = encryptionKey 的原始 UTF-8 字节
    （32字符=32字节），iv 固定是 "5485693214587452"（写死在登录页 JS 里，不是
    每次变的），PKCS7 填充，输出密文的十六进制——照抄登录页内联 JS 里的
    encryption() 函数，已用真实抓包的 (password, encryptionKey, 密文) 三元组
    验证完全匹配。"""
    key_bytes = encryption_key.encode("utf-8")
    cipher = AES.new(key_bytes, AES.MODE_CBC, _PASSWORD_LOGIN_IV)
    ciphertext = cipher.encrypt(pad(password.encode("utf-8"), AES.block_size))
    return ciphertext.hex()


def check_username_login(session: requests.Session, username: str, encrypted_password: str, encryption_key: str) -> dict:
    """GET sms/check-username-login。登录页 JS 在真正提交表单前会先调这个确认
    账号密码对不对（表单提交是这个请求成功后才触发的，是客户端流程的一部分），
    这里照抄这个前置检查，不直接跳过。"""
    params = {"username": username, "password": encrypted_password, "encryptionKey": encryption_key}
    resp = session.get(
        f"{config.CAS_BASE}/auth/realms/fabao/sms/check-username-login",
        params=params, headers={"x-requested-with": "XMLHttpRequest", "user-agent": config.UA},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def complete_login_by_password(session: requests.Session, login_ctx: dict, identifier: str, password: str) -> tuple[requests.Response, str]:
    """账号+密码登录，手机号和邮箱是同一个页面同一个表单（不像验证码登录分
    手机/邮箱两个 tab）。真实抓包确认请求体里 `password`（AES 加密过的）和
    `passwordFront`（明文）两个字段都传了，没细究服务端到底靠哪个校验，照抓包
    原样两个都带上最稳妥。

    需要 `login_ctx` 里带着 `encryption_key`（get_cas_session() 已经顺带解析出来
    了）。后面的两跳授权码兑换跟验证码登录是同一个机制，逻辑照抄 complete_login。
    """
    encryption_key = login_ctx["encryption_key"]
    encrypted_password = encrypt_password(password, encryption_key)

    check_username_login(session, identifier, encrypted_password, encryption_key)

    params = {
        "session_code": login_ctx["session_code"],
        "execution": login_ctx["execution"],
        "client_id": "pkulaw",
        "tab_id": login_ctx["tab_id"],
    }
    body = {
        "loginType": "1", "tabType": "passValidate",
        "redirect_uri": config.KC_REDIRECT_URI,
        "source": "",
        "encryptionKey": encryption_key,
        "password": encrypted_password,
        "email-phone": identifier,
        "passwordFront": password,
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
        tips = _extract_tip_messages(resp.text)
        reason = " / ".join(sorted(set(tips.values()))) if tips else None
        if reason and _PENDING_REVIEW_MARKER in reason:
            raise AccountPendingReviewError(
                f"complete_login_by_password: {identifier} 登录没有走到中转页，页面解析出的提示："
                f"「{reason}」，账号被网站转了人工审核。实际落地页: {resp.url}"
            )
        raise RuntimeError(
            "complete_login_by_password: 没有走到 statics/kc/index.html 中转页，登录没有真正完成。"
            f" 实际落地页: {resp.url}\n"
            + (f"页面解析出的提示：{reason}" if reason else
               f"页面没解析到具体提示文案，响应片段: {_response_snippet(resp)}")
        )
    qs = dict(p.split("=", 1) for p in parsed.query.split("&") if "=" in p)
    redirect_path = requests.utils.unquote(qs["redirect_path"])
    auth_code = qs["code"]
    sep = "&" if "?" in redirect_path else "?"
    final_resp = session.get(f"{redirect_path}{sep}code={auth_code}",
                              headers={"user-agent": config.UA}, timeout=15)
    final_resp.raise_for_status()
    return final_resp, auth_code


def password_login(identifier: str, password: str, session: requests.Session | None = None) -> tuple[requests.Session, str]:
    """账号设置过密码之后的登录方式（identifier 可以是手机号或邮箱，同一个表单）。
    走一遍 get_cas_session -> complete_login_by_password，返回 (session, auth_code)
    ——auth_code 留给调用方（不建议再拿去换 access_token，见
    exchange_code_for_token 的说明，改用 get_token_from_page）。

    传 `session` 用调用方已经建好的 session（比如 login.py 里带着账号绑定代理的
    那个），不传就现建一个直连的。
    """
    session, login_ctx = get_cas_session(session)
    if "encryption_key" not in login_ctx:
        raise RuntimeError("password_login: 登录表单页面里没找到 encryptionKey，页面结构可能变了")
    _final_resp, auth_code = complete_login_by_password(session, login_ctx, identifier, password)
    if not is_logged_in(session):
        raise RuntimeError("password_login: 登录表单提交完了但 is_logged_in 仍是 False")
    return session, auth_code


def logout(session: requests.Session) -> requests.Response:
    """GET /logout/?ReturnUrl=...，服务端会自动 302 到
    cas.pkulaw.com/.../sms/remove-sessions/{session_state}?redirect_uri=...
    （session_state 是服务端从当前 session 的 cookie 自己读出来拼的，不用我们
    自己传），再 302 回 www.pkulaw.com，沿途把登录态 cookie 都清空/置过期。
    requests 默认 allow_redirects=True 能自动跟完整条链路，不需要执行任何 JS。"""
    resp = session.get(f"{config.WWW_BASE}/logout/", params={"ReturnUrl": f"{config.WWW_BASE}/case"},
                        headers={"user-agent": config.UA}, timeout=15)
    resp.raise_for_status()
    return resp

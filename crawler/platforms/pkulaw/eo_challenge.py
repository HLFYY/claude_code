"""
Tencent EdgeOne（腾讯云 EO，CDN/WAF）的 JS Cookie 挑战：某些请求会被拦下来，响应体
不是真实页面，而是一段重度混淆的 <script>，核心逻辑是设置一两个 cookie
（__tst_status / EO_Bot_Ssid）然后 reload。真实浏览器执行这段 JS 后带着新 cookie
重新请求就能过去；这里不做逐层手动反混淆（obfuscator 的变量名/数组轮转量/case 顺序
每次大概率都不一样，手动扒一次不划算，而且实测 EO_Bot_Ssid 这个值本身也是每次挑战
不一样的，写死复用不安全，见 HANDOFF.md），而是直接把这段 <script> 丢进一个极简的
Node vm 沙箱真实跑一遍，拿它自己产生的 document.cookie 赋值结果（做法跟 tdc_bridge.js
一致），可读做法见 eo_challenge_bridge.js。

已用真实拦截页验证过：还原出的 __tst_status=538622729 跟这个站点其他地方（真实浏览器
手动登录时抓到的 cookie）出现过的同一个值完全一致，eo_challenge_bridge.js 的输出
是可信的。
"""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import requests

_BRIDGE_JS = Path(__file__).parent / "eo_challenge_bridge.js"

# 挑战页本体只有一个 <script>...</script>，且必然会出现 EO_Bot_Ssid 这个字符串
# （它就是要设置的 cookie 名之一），拿这个当"是不是撞上挑战页"的判定信号。
_CHALLENGE_MARKER = "EO_Bot_Ssid"
_SCRIPT_RE = re.compile(r"<script[^>]*>(.*?)</script>", re.S)


def is_challenge_page(resp: requests.Response) -> bool:
    return _CHALLENGE_MARKER in resp.text and len(resp.text) < 5000


def solve(html: str) -> dict[str, str]:
    """跑一遍挑战脚本，返回 {cookie_name: cookie_value} 字典（已经把 "name=value;"
    这种整条 Set-Cookie 风格字符串拆好了，方便直接塞进 requests.Session.cookies）。"""
    m = _SCRIPT_RE.search(html)
    if not m:
        raise RuntimeError("eo_challenge.solve: 没在页面里找到 <script>")
    script = m.group(1)

    proc = subprocess.run(["node", str(_BRIDGE_JS)], input=script,
                           capture_output=True, text=True, timeout=15)
    if proc.returncode != 0:
        raise RuntimeError(f"eo_challenge_bridge.js failed: {proc.stderr}")
    cookie_writes: list[str] = json.loads(proc.stdout)

    cookies = {}
    for write in cookie_writes:
        kv = write.split(";", 1)[0].strip()
        if "=" not in kv:
            continue
        name, _, value = kv.partition("=")
        cookies[name.strip()] = value.strip()
    return cookies


def prime(session: requests.Session, url: str, **kwargs) -> dict[str, str]:
    """主动预热一次：请求 url，如果撞上挑战就解掉、把 cookie 存进 session.cookies
    并返回；没撞上就返回空 dict。跟 get_with_challenge_retry 用同一份 solve()，区别
    只是调用时机（登录后主动调一次 vs 真实请求撞上再解）。"""
    resp = session.get(url, **kwargs)
    if not is_challenge_page(resp):
        return {}
    cookies = solve(resp.text)
    session.cookies.update(cookies)
    return cookies


def get_with_challenge_retry(session: requests.Session, url: str, max_retries: int = 2, **kwargs) -> requests.Response:
    """跟 session.get 一样用，但撞上 EdgeOne 的 JS cookie 挑战会自动解一次再重试。
    不是每次请求都会撞上，撞上了才会真的去跑 Node 沙箱，没撞上就跟直接
    session.get(...) 完全一样。"""
    resp = session.get(url, **kwargs)
    for _ in range(max_retries):
        if not is_challenge_page(resp):
            return resp
        cookies = solve(resp.text)
        if not cookies:
            return resp
        session.cookies.update(cookies)
        resp = session.get(url, **kwargs)
    return resp

"""
collect/eks 的来源已经在 dy-ele.js 里确认（见 HANDOFF.md）：
    collect = decodeURIComponent(window.TDC.getData(true))
    eks     = window.TDC.getInfo().info
TDC.getData/getInfo 的真实实现封在 tdc.js 的 JSVMP 字节码里，没有做逐 opcode 还原，
而是让 tdc.js 自己的代码在一个极简的 Node vm 沙箱里真实跑起来（不是浏览器，不是
Puppeteer/Selenium，只是手写了十几个 window/document/navigator 之类的 stub 对象，
详见 tdc_bridge.js 顶部注释）。这是经用户确认过的折中方案。
"""
from __future__ import annotations

import json
import subprocess
import tempfile
import urllib.parse
from pathlib import Path

import requests

from . import config

_BRIDGE_JS = Path(__file__).parent / "tdc_bridge.js"


def fetch_tdc_js(tdc_path: str, session: requests.Session | None = None) -> str:
    """tdc_path 是 prehandle 响应里 comm_captcha_cfg.tdc_path，形如
    "/tdc.js?app_data=...&t=...".返回源码文本。"""
    s = session or requests
    url = config.CAPTCHA_BASE + tdc_path
    resp = s.get(url, headers=config.CAPTCHA_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def get_collect_and_eks(tdc_js_source: str) -> dict:
    """跑一次 tdc_bridge.js，返回 {"collect": str, "eks": str, "tokenid": int}。"""
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(tdc_js_source)
        tmp_path = f.name
    try:
        proc = subprocess.run(
            ["node", str(_BRIDGE_JS), tmp_path],
            capture_output=True, text=True, timeout=15,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if proc.returncode != 0:
        raise RuntimeError(f"tdc_bridge.js failed (code={proc.returncode}): {proc.stderr}")
    data = json.loads(proc.stdout)
    # dy-ele.js: a = decodeURIComponent(getTdcData()) -- collect 是 URL 编码过的，先解码
    data["collect"] = urllib.parse.unquote(data["collect"])
    return data

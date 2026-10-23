#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mitm_mcd.py —— mitmdump addon: 抓麦当劳 + 数美(风控/注册)流量,原样落盘 + 打印关键字段。

目的: 抓云手机那条"能过"的发码/登录请求全貌(头+体+d+x-mcd-sec-verify),
      和我们服务器上 706070 的请求逐字段 diff;并抓数美 deviceprofile/v4 注册包。

服务器运行:
  pip install mitmproxy
  mitmdump -s mitm_mcd.py --listen-host 0.0.0.0 --listen-port 8080 --set block_global=false
  # 可选同时存原始流: 加  -w /tmp/mcd_flows.mitm   (之后可 mitmweb -r 回看)

手机侧:
  1. WiFi 代理设为  <服务器IP>:8080
  2. 浏览器打开 http://mitm.it 安装 mitmproxy CA 证书(Android 用户证书; 若 app 不认用户证书则需系统证书/脱壳,见下)
  3. 跑 app, 走发码/登录

落盘(默认 /tmp/mcd_cap/):
  每条命中请求存  <序号>_<host>_<path>.req / .resp (头+体; 二进制体同时存 .req.bin/.resp.bin)
  汇总打印到 stdout 和  _summary.log

⚠️ 证书固定(pinning): 麦当劳 BFF(api/*mcd*)多半 pinning, 没 frida 解 pin 时这些看不到(会报错或不出现);
   数美 fp-it.fengkongcloud.com 很可能不 pin → 至少能抓到注册包。要抓 BFF 需配合 frida SSL unpin。
"""
import os, time, json
from mitmproxy import http

OUTDIR = os.environ.get("MITM_OUT", "/tmp/mcd_cap")
os.makedirs(OUTDIR, exist_ok=True)

# 命中这些 host 关键字才记录(减噪)
HOST_HITS = ("fengkongcloud.com", "ishumei", "mcdonalds", "mcd", "4008-517-517", "fp-it")
# 特别关注的路径关键字(发码/登录/注册/风控)
PATH_HOT = ("deviceprofile", "sendcode", "send_code", "sms", "verify", "login", "captcha", "punish", "sec")

_seq = {"n": 0}
RUN = time.strftime("%m%d_%H%M%S")  # 本次 mitmdump 运行标识，加进文件名防跨次运行覆盖/混淆

def _hot(flow):
    host = (flow.request.pretty_host or "").lower()
    return any(h in host for h in HOST_HITS)

def _safe(s, n=120):
    return "".join(c if (32 <= ord(c) < 127) else "." for c in (s or "")[:n])

def _dump(flow: http.HTTPFlow, tag):
    _seq["n"] += 1
    n = _seq["n"]
    ts = time.strftime("%Y-%m-%d %H:%M:%S")   # 抓到该请求的本地时间
    host = flow.request.pretty_host
    path = (flow.request.path or "/").split("?")[0]
    safe_path = path.strip("/").replace("/", "_")[:60] or "root"
    base = os.path.join(OUTDIR, f"{RUN}_{n:04d}_{host}_{safe_path}")

    # 请求（首行记抓取时间，便于判断是不是当前最新的抓包）
    req_lines = [f"# captured_at: {ts}", f"{flow.request.method} {flow.request.pretty_url}"]
    for k, v in flow.request.headers.items():
        req_lines.append(f"{k}: {v}")
    req_lines.append("")
    body = flow.request.raw_content or b""
    try:
        txt = body.decode("utf-8")
        req_lines.append(txt)
    except Exception:
        req_lines.append(f"<binary {len(body)} bytes, 见 .req.bin>")
        open(base + ".req.bin", "wb").write(body)
    open(base + ".req", "w", encoding="utf-8", errors="replace").write("\n".join(req_lines))

    # 响应
    if flow.response:
        resp_lines = [f"# captured_at: {ts}", f"HTTP {flow.response.status_code}"]
        for k, v in flow.response.headers.items():
            resp_lines.append(f"{k}: {v}")
        resp_lines.append("")
        rbody = flow.response.raw_content or b""
        try:
            resp_lines.append(rbody.decode("utf-8"))
        except Exception:
            resp_lines.append(f"<binary {len(rbody)} bytes, 见 .resp.bin>")
            open(base + ".resp.bin", "wb").write(rbody)
        open(base + ".resp", "w", encoding="utf-8", errors="replace").write("\n".join(resp_lines))

    # 关键字段摘要
    h = flow.request.headers
    d = h.get("d") or h.get("D") or ""
    sec = h.get("x-mcd-sec-verify") or ""
    rc = flow.response.status_code if flow.response else "-"
    # 响应里的风控码(420/706070/707053...)
    code = ""
    if flow.response:
        rb = flow.response.get_text(strict=False) or ""
        for kw in ('"code"', '"resultCode"', '"status"'):
            i = rb.find(kw)
            if i >= 0:
                code = rb[i:i + 40].replace("\n", " "); break
    line = (f"[{n:04d}] {ts}  {flow.request.method} {host}{path}  HTTP{rc}"
            f"  d={_safe(d, 24)}  sec={_safe(sec, 40)}  {code}")
    print(line, flush=True)
    open(os.path.join(OUTDIR, "_summary.log"), "a", encoding="utf-8").write(line + "\n")

def response(flow: http.HTTPFlow):
    if _hot(flow):
        try:
            _dump(flow, "resp")
        except Exception as e:
            print("dump err:", e, flush=True)

def error(flow):
    # pinning 失败的连接会进这里, 记录 host 便于判断哪些被 pin
    try:
        if flow.request and _hot(flow):
            msg = f"[ERR] {time.strftime('%Y-%m-%d %H:%M:%S')}  {flow.request.pretty_host}{(flow.request.path or '').split('?')[0]}  (可能证书固定/连接失败)"
            print(msg, flush=True)
            open(os.path.join(OUTDIR, "_summary.log"), "a", encoding="utf-8").write(msg + "\n")
    except Exception:
        pass

_banner = f"==== mitmdump 启动 {time.strftime('%Y-%m-%d %H:%M:%S')} (run={RUN}) ===="
print(f"[mitm_mcd] 输出目录: {OUTDIR}  命中host: {HOST_HITS}", flush=True)
print(_banner, flush=True)
try:
    open(os.path.join(OUTDIR, "_summary.log"), "a", encoding="utf-8").write("\n" + _banner + "\n")
except Exception:
    pass

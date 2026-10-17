#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fake_device_poc.py —— 路线A PoC:注入假设备 → 清 smid 缓存 → 验证拿到与真机不同的新 smid
环境: /Users/houjie/venv/python3-forcrawl/bin/python  (frida 15.1.17)

流程: spawn app → 早期注入 bypass + hook_fake_device.js(假设备+清缓存)
      → resume → 等数美用【假 vdata】向服务端注册 → 轮询取回新 smid → 对比真机
用法: python fake_device_poc.py
"""
import os
import sys
import time
import frida

PKG = "com.mcdonalds.gma.cn"
DEV = os.environ.get("FRIDA_DEV") or "FA6AL0310909"
HERE = os.path.dirname(os.path.abspath(__file__))
FRIDA_DIR = os.path.join(HERE, "..", "frida")
BYPASS = os.path.join(FRIDA_DIR, "frida_bypass_combined_local.js")
HOOK = os.path.join(FRIDA_DIR, "hook_fake_device.js")

# 真机已知 smid(去掉 B 前缀),用于对比证明"假设备产生了不同身份"
REAL_SMIDS = {
    "NAT/vK23yLIEABV/PPqHN+Y5oSjkFS7zFmOCcRr8JvEViaB1LaQ90jOR46zsLQlIWAJZEWwxBDApeer6RfS9NA==",
    "C5R6EG/Y38rrUyYGJlJ2tHq4lAXOoXoeP3CD7vhLKh1/fVstLVnfN6sv4MJMkmvBcFs8/1nheHwhRESM/YM18Q==",
}


def _load(session, path):
    with open(path, "r", encoding="utf-8") as f:
        code = f.read()
    s = session.create_script(code)
    s.on("message", lambda m, d: print("[js]", m.get("payload") or m.get("stack") or m, flush=True))
    s.load()
    return s


def main():
    dev = frida.get_device(DEV, timeout=10)
    print("[*] spawn", PKG, "on", dev.name, flush=True)
    pid = dev.spawn([PKG])
    session = dev.attach(pid)
    _load(session, BYPASS)                 # 反调试绕过(最早)
    hook = _load(session, HOOK)            # 假设备注入 + 清缓存
    dev.resume(pid)
    exp = hook.exports_sync if hasattr(hook, "exports_sync") else hook.exports

    print("[*] 等数美用假 vdata 向服务端注册(最多 50s)…", flush=True)
    new_d, deadline = None, time.time() + 50
    first_d = None
    while time.time() < deadline:
        time.sleep(2)
        try:
            cur = exp.getdraw()            # 取注册回填后的缓存 smid
        except Exception as e:
            continue
        if not cur or cur.startswith("ERR"):
            continue
        if first_d is None:
            first_d = cur
            print("[*] 首个返回:", cur[:30], "前缀", cur[0], flush=True)
        if cur[0] == "B":
            new_d = cur
            break

    print("\n==================== PoC 结果 ====================", flush=True)
    if new_d:
        smid = new_d[1:]
        print("假设备拿到 d =", new_d, flush=True)
        print("smid        =", smid, flush=True)
        print("与真机 smid 不同? ", "✅ 是(假设备=独立身份)" if smid not in REAL_SMIDS
              else "❌ 否(仍是真机身份,清缓存未生效)", flush=True)
    else:
        print("未在时限内拿到 B+smid。首个返回:", first_d, flush=True)
        print("(可能注册慢/网络/被风控;可加大时限或检查 vdata)", flush=True)
    print("==================================================", flush=True)
    try:
        session.detach()
    except Exception:
        pass
    return 0 if new_d else 1


if __name__ == "__main__":
    sys.exit(main())

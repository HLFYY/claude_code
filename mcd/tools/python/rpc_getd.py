#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rpc_getd.py —— 通过 frida-RPC 生成麦当劳 d 值(数美设备指纹 B+smid)
环境: /Users/houjie/venv/python3-forcrawl/bin/python  (frida 15.1.17)

原理:
    接口头 d = SmAntiFraud.getDeviceId() = "B" + smid
    smid 由数美服务端注册后下发并本地持久化,对该设备稳定。
    vdata 盒子(data/tn/ep)在 native(libsmsdk)加密 → 纯算法复刻不现实,故现调现取。

用法(默认 spawn 启动 app,自动等注册完成):
    rpc_getd.py              # 取一次 d,打印并落盘 smid_cache.json
    rpc_getd.py --attach     # 不启动,attach 已在前台的 app
    rpc_getd.py serve        # 常驻,每 30s 刷新
    rpc_getd.py offline      # 离线:直接读 smid_cache.json 产出 d,不连设备

依赖同目录 ../frida/{frida_bypass_combined_local.js,hook_d_active.js}
"""
import os
import sys
import json
import time
import frida

PKG = "com.mcdonalds.gma.cn"
HERE = os.path.dirname(os.path.abspath(__file__))
FRIDA_DIR = os.path.join(HERE, "..", "frida")
BYPASS = os.path.join(FRIDA_DIR, "frida_bypass_combined_local.js")
HOOK = os.path.join(FRIDA_DIR, "hook_d_active.js")
CACHE = os.path.join(HERE, "smid_cache.json")


def _load(session, path, silent=False):
    with open(path, "r", encoding="utf-8") as f:
        code = f.read()
    s = session.create_script(code)
    if not silent:
        s.on("message", lambda m, d: print("[js]", m.get("payload") or m.get("stack") or m, flush=True))
    else:
        s.on("message", lambda m, d: None)
    s.load()
    return s


def _exports(script):
    return script.exports_sync if hasattr(script, "exports_sync") else script.exports


def get_d(attach=False, max_wait=60):
    """返回真机现取的 d(优先 B+smid)。"""
    dev_id = os.environ.get("FRIDA_DEV") or "FA6AL0310909"
    dev = frida.get_device(dev_id, timeout=10)
    pid = None
    if attach:
        proc = dev.get_process(PKG)   # 抛异常则 app 没在跑
        session = dev.attach(proc.pid)
        _load(session, BYPASS, silent=True)
        hook = _load(session, HOOK, silent=True)
    else:
        pid = dev.spawn([PKG])
        session = dev.attach(pid)
        _load(session, BYPASS, silent=True)       # 先反调试绕过
        hook = _load(session, HOOK, silent=True)
        dev.resume(pid)

    exp = _exports(hook)
    d, deadline = None, time.time() + max_wait
    while time.time() < deadline:
        time.sleep(1.5)
        try:
            cur = exp.getd()
        except Exception:
            continue
        if not cur:
            continue
        if cur[0] == "B":                 # 拿到服务端 smid,最佳
            d = cur
            break
        d = cur                            # 暂存 D(异常/未注册),继续等 B
    try:
        session.detach()
    except Exception:
        pass
    return d


def save_cache(d):
    if d and d[0] == "B":
        json.dump({"smid": d[1:], "d": d, "ts": int(time.time())},
                  open(CACHE, "w"), ensure_ascii=False, indent=2)
        print("[cache] 已落盘 smid →", CACHE, flush=True)


def offline():
    if not os.path.exists(CACHE):
        print("[!] 无 smid_cache.json,先联机跑一次 rpc_getd.py"); return 1
    c = json.load(open(CACHE))
    print("离线产出 d =", "B" + c["smid"], flush=True)
    return 0


def main():
    args = sys.argv[1:]
    if "offline" in args:
        return offline()
    attach = "--attach" in args
    serve = "serve" in args

    d = get_d(attach=attach)
    print("\n==================== RPC 结果 ====================", flush=True)
    print("d =", d, flush=True)
    print("类型:", "B+smid(服务端已注册,接口直接用这个)" if d and d[0] == "B"
          else "D+vdata(未注册/异常态)" if d else "(取不到)", flush=True)
    print("==================================================\n", flush=True)
    save_cache(d)

    if serve and d:
        print("[*] 常驻中(--attach 复用),Ctrl+C 退出。", flush=True)
        try:
            while True:
                time.sleep(30)
                print("[d]", get_d(attach=True, max_wait=15), flush=True)
        except KeyboardInterrupt:
            pass
    return 0 if d else 1


if __name__ == "__main__":
    sys.exit(main())

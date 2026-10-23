#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dumpbox_cloud.py —— 在能过风控的设备(如多多云手机)上一键 dump 数美 box/shortbox 明文模板。

做法: spawn + frida_bypass_combined_local.js(绕原生壳) + hook_dump_box_plain.js(dump v1入参),
      崩了自动重启重试(竞速 DexHelper patch), 抓到 v1-arg 就解析存盘并停。

前提:
  1. frida 15.1.17 (PY=~/venv/python3-forcrawl/bin/python), frida-server 15.1.17 已在设备跑。
     设备 adb shell 若已 root 可直接: adb -s <dev> shell "setsid /data/local/tmp/frida-server-15.1.17-android-arm64 >/dev/null 2>&1 &"
  2. 远程设备(多多云手机在 Windows 上): 先按 ../../docs/Mac连接Windows共享adb手机.md 打通 adb,
     再跑本目录 adb_relay.py(让 frida 借道 Windows adb server)。见该文件注释。
  3. FRIDA_DEV=设备序列号 (如 127.0.0.1:63373)

用法:
  FRIDA_DEV=127.0.0.1:63373 PY dumpbox_cloud.py [tries=12] [per秒=30] [outdir=.]
  成功后在 outdir 生成 cloud_v1_arg1.json(完整box) / cloud_v1_arg2.json(shortbox,注册用) / cloud_v1_arg3.json(配置)
"""
import os, sys, time, json, ast, re, frida

DIR = os.path.dirname(os.path.abspath(__file__))
DEV = os.environ.get("FRIDA_DEV", "127.0.0.1:63373")
PKG = "com.mcdonalds.gma.cn"
BYPASS = os.path.join(DIR, "frida_bypass_combined_local.js")
HOOK = os.path.join(DIR, "hook_dump_box_plain.js")
TRIES = int(sys.argv[1]) if len(sys.argv) > 1 else 12
PER = int(sys.argv[2]) if len(sys.argv) > 2 else 30
OUTDIR = sys.argv[3] if len(sys.argv) > 3 else "."

codes = [("bypass", open(BYPASS, encoding="utf-8").read()),
         ("hook", open(HOOK, encoding="utf-8").read())]
got = {"args": {}}   # i -> json str

def _save_one(i, val):
    names = {1: "cloud_v1_arg1.json", 2: "cloud_v1_arg2.json", 3: "cloud_v1_arg3.json"}
    fn = os.path.join(OUTDIR, names.get(i, "cloud_v1_arg%d.json" % i))
    try:
        obj = json.loads(val)
        open(fn, "w", encoding="utf-8").write(json.dumps(obj, ensure_ascii=False, indent=1))
    except Exception:
        open(fn, "w", encoding="utf-8").write(val)
    print("[SAVED-now] %s" % fn, flush=True)

def save():
    names = {1: "cloud_v1_arg1.json", 2: "cloud_v1_arg2.json", 3: "cloud_v1_arg3.json"}
    for i, val in got["args"].items():
        fn = os.path.join(OUTDIR, names.get(i, "cloud_v1_arg%d.json" % i))
        try:
            obj = json.loads(val)
            open(fn, "w", encoding="utf-8").write(json.dumps(obj, ensure_ascii=False, indent=1))
            print("[SAVED] %s  字段数=%d" % (fn, len(obj) if isinstance(obj, dict) else -1), flush=True)
        except Exception:
            open(fn, "w", encoding="utf-8").write(val)
            print("[SAVED] %s (非标准JSON原样存)" % fn, flush=True)

def on_message(name):
    def h(msg, data):
        t = msg.get("type")
        if t == "send":
            p = msg.get("payload")
            if isinstance(p, dict) and p.get("__dump") == "v1-arg":
                d = p["data"]
                got["args"][d["i"]] = d["val"]
                print("[CAP] v1-arg i=%d len=%d" % (d["i"], d["len"]), flush=True)
                _save_one(d["i"], d["val"])   # 捕获即刻落盘(防隧道在存盘前断掉丢数据)
            else:
                print("[%s][send] %s" % (name, str(p)[:200]), flush=True)
        elif t == "log":
            print("[%s] %s" % (name, msg.get("payload")), flush=True)
        elif t == "error":
            print("[%s][ERR] %s" % (name, msg.get("stack") or msg.get("description")), flush=True)
    return h

dev = frida.get_device(DEV, timeout=10)
for attempt in range(1, TRIES + 1):
    if got["args"]:
        break
    print("\n========== try %d/%d ==========" % (attempt, TRIES), flush=True)
    try:
        for p in dev.enumerate_processes():
            if p.name == PKG:
                dev.kill(p.pid)
    except Exception:
        pass
    time.sleep(1.0)
    alive = {"v": True}
    try:
        pid = dev.spawn([PKG])
        session = dev.attach(pid)
        session.on("detached", lambda *a: alive.update(v=False))
        for name, code in codes:
            s = session.create_script(code)
            s.on("message", on_message(name))
            s.load()
        dev.resume(pid)
        print("[*] resumed, 盯 %ds" % PER, flush=True)
        t0 = time.time()
        # shortbox(arg i=2) 是注册用的关键; 抓到它(或任一 arg)即可停
        while time.time() - t0 < PER and alive["v"] and 2 not in got["args"]:
            time.sleep(0.4)
        try:
            session.detach()
        except Exception:
            pass
        if got["args"]:
            print("[try %d] *** 抓到 %d 个 v1 入参! ***" % (attempt, len(got["args"])), flush=True)
            break
        if not alive["v"]:
            print("[try %d] 崩了, 重试" % attempt, flush=True)
    except Exception as e:
        print("[try %d] 异常: %s" % (attempt, e), flush=True)
        time.sleep(0.5)

if got["args"]:
    save()
else:
    print("\n[FAIL] 没抓到 v1 入参; 多试几次(调大 tries), 或确认 app 数据非全新清空态/frida-server在跑", flush=True)

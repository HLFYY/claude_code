#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
device_pool.py —— 数美设备身份池(路线A 封装:批量造假设备→注册独立 smid→存库→按账号领用)
环境: /Users/houjie/venv/python3-forcrawl/bin/python  (frida 15.1.17)

原理: 每个设备 = 一套自洽的假设备指纹(随机 android_id + 真实机型四件套/指纹/序列号)。
      spawn app,把 PROFILE 注入采集源,让 app 自己的 native 加密 vdata 向数美注册,
      拿回一个独立 smid(d="B"+smid)。不同 android_id → 服务端认成不同设备。

命令:
    device_pool.py build <N>          # 造 N 个新设备身份入库
    device_pool.py list               # 列出设备池
    device_pool.py stats              # 统计 free/assigned
    device_pool.py claim <account>    # 为账号领一个空闲设备(返回 d),标记 assigned
    device_pool.py release <dev_id>   # 释放设备回池
    device_pool.py get <account>      # 查账号已领的设备

存储: 同目录 device_pool.json
"""
import os
import sys
import json
import time
import uuid
import random
import string
import frida

PKG = "com.mcdonalds.gma.cn"
DEV = os.environ.get("FRIDA_DEV") or "FA6AL0310909"
HERE = os.path.dirname(os.path.abspath(__file__))
FRIDA_DIR = os.path.join(HERE, "..", "frida")
BYPASS = os.path.join(FRIDA_DIR, "frida_bypass_combined_local.js")
HOOK = os.path.join(FRIDA_DIR, "hook_fake_device.js")
POOL = os.path.join(HERE, "device_pool.json")

# 真实自洽机型库(fingerprint 控制在 PROP_VALUE_MAX=92 内;可自行扩充)
MODELS = [
    {"brand": "Redmi", "manufacturer": "Xiaomi", "model": "M2012K11AC", "device": "alioth", "hardware": "qcom",
     "fp": "Redmi/alioth/alioth:11/RKQ1.200826.002/V12.5.1.0.RKHCNXM:user/release-keys"},
    {"brand": "Xiaomi", "manufacturer": "Xiaomi", "model": "M2011K2C", "device": "venus", "hardware": "qcom",
     "fp": "Xiaomi/venus/venus:12/SKQ1.211006.001/V13.0.5.0.SKBCNXM:user/release-keys"},
    {"brand": "Redmi", "manufacturer": "Xiaomi", "model": "M1908C3JG", "device": "ginkgo", "hardware": "qcom",
     "fp": "Redmi/ginkgo/ginkgo:11/RKQ1.200826.002/V12.5.6.0.RCOCNXM:user/release-keys"},
    {"brand": "OnePlus", "manufacturer": "OnePlus", "model": "KB2000", "device": "kebab", "hardware": "qcom",
     "fp": "OnePlus/kebab/OnePlus8T:11/RKQ1.201105.002/2109091800:user/release-keys"},
    {"brand": "OPPO", "manufacturer": "OPPO", "model": "PEDM00", "device": "OP4BA7", "hardware": "qcom",
     "fp": "OPPO/PEDM00/OP4BA7:11/RKQ1.201217.002/1632812841:user/release-keys"},
    {"brand": "vivo", "manufacturer": "vivo", "model": "V2055A", "device": "PD2055", "hardware": "mt6889",
     "fp": "vivo/PD2055/PD2055:11/RP1A.200720.012/compiler03091511:user/release-keys"},
]


def rand_android_id():
    return os.urandom(8).hex()                      # 16 hex


def rand_serial():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))


def gen_profile():
    import uuid as _uuid
    m = random.choice(MODELS)
    serial = rand_serial()
    props = {
        "ro.serialno": serial,
        "ro.boot.serialno": serial,
        "ro.product.model": m["model"],
        "ro.product.brand": m["brand"],
        "ro.product.manufacturer": m["manufacturer"],
        "ro.product.device": m["device"],
        "ro.product.name": m["device"],
        "ro.build.fingerprint": m["fp"],
        "ro.hardware": m["hardware"],
        "ro.boot.hardware": m["hardware"],
        "ro.boot.bootloader": "unknown",
    }
    return {"android_id": rand_android_id(), "token": _uuid.uuid4().hex, "props": props, "_model": m["model"]}


# ---------- frida: 用指定 profile 注册一个 smid ----------
def register_one(profile, timeout=55):
    dev = frida.get_device(DEV, timeout=10)
    pid = dev.spawn([PKG])
    session = dev.attach(pid)
    for p in (BYPASS,):
        s = session.create_script(open(p, encoding="utf-8").read())
        s.on("message", lambda m, d: None)
        s.load()
    js = "var INJECT_PROFILE = " + json.dumps({"android_id": profile["android_id"], "props": profile["props"]}) + ";\n"
    js += open(HOOK, encoding="utf-8").read()
    hook = session.create_script(js)
    hook.on("message", lambda m, d: None)
    hook.load()
    dev.resume(pid)
    exp = hook.exports_sync if hasattr(hook, "exports_sync") else hook.exports

    d, deadline = None, time.time() + timeout
    while time.time() < deadline:
        time.sleep(2)
        try:
            cur = exp.getdraw()
        except Exception:
            continue
        if cur and not cur.startswith("ERR") and cur[0] == "B":
            d = cur
            break
    try:
        session.detach()
    except Exception:
        pass
    try:
        dev.kill(pid)
    except Exception:
        pass
    return d


# ---------- 存储 ----------
def load_pool():
    if os.path.exists(POOL):
        return json.load(open(POOL, encoding="utf-8"))
    return []


def save_pool(pool):
    json.dump(pool, open(POOL, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


def known_smids(pool):
    return {e["smid"] for e in pool}


# ---------- 命令 ----------
def cmd_build(n):
    pool = load_pool()
    seen = known_smids(pool)
    ok = 0
    for i in range(n):
        p = gen_profile()
        print("[build %d/%d] model=%s android_id=%s 注册中…" % (i + 1, n, p["_model"], p["android_id"]), flush=True)
        d = register_one(p)
        if not d:
            print("   ✗ 未拿到 smid(注册超时/网络),跳过", flush=True); continue
        smid = d[1:]
        if smid in seen:
            print("   ⚠ smid 撞库(与已有重复),跳过", flush=True); continue
        entry = {
            "id": uuid.uuid4().hex[:12], "model": p["_model"], "android_id": p["android_id"],
            "token": p.get("token"), "props": p["props"], "smid": smid, "d": d,
            "created": int(time.time()), "status": "free", "account": None,
        }
        pool.append(entry); seen.add(smid); ok += 1
        print("   ✓ 入库 id=%s d=%s…" % (entry["id"], d[:24]), flush=True)
        save_pool(pool)
    print("\n完成: 新增 %d 个,池内共 %d 个" % (ok, len(pool)), flush=True)


def cmd_list():
    for e in load_pool():
        print("%s | %-10s | %-7s | %s | d=%s…" % (
            e["id"], e["model"], e["status"], e.get("account") or "-", e["d"][:20]))


def cmd_stats():
    pool = load_pool()
    free = sum(1 for e in pool if e["status"] == "free")
    print("总计 %d | 空闲 %d | 已领 %d" % (len(pool), free, len(pool) - free))


def cmd_claim(account):
    pool = load_pool()
    for e in pool:
        if e["status"] == "free":
            e["status"] = "assigned"; e["account"] = account; e["assigned_at"] = int(time.time())
            save_pool(pool)
            print("账号 %s 领到设备 %s" % (account, e["id"]))
            print("d =", e["d"])
            return
    print("[!] 无空闲设备,先 build")


def cmd_release(dev_id):
    pool = load_pool()
    for e in pool:
        if e["id"] == dev_id:
            e["status"] = "free"; e["account"] = None
            save_pool(pool); print("已释放", dev_id); return
    print("[!] 没找到", dev_id)


def cmd_get(account):
    for e in load_pool():
        if e.get("account") == account:
            print("账号 %s 的设备 %s" % (account, e["id"])); print("d =", e["d"]); return
    print("[!] 账号未领设备")


def main():
    if len(sys.argv) < 2:
        print(__doc__); return 2
    c = sys.argv[1]
    if c == "build":
        cmd_build(int(sys.argv[2]) if len(sys.argv) > 2 else 1)
    elif c == "list":
        cmd_list()
    elif c == "stats":
        cmd_stats()
    elif c == "claim":
        cmd_claim(sys.argv[2])
    elif c == "release":
        cmd_release(sys.argv[2])
    elif c == "get":
        cmd_get(sys.argv[2])
    else:
        print("未知命令:", c); print(__doc__); return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
device_builder.py —— 造数美假设备并注册独立 smid(mcd_project 自包含版)
供 device_pool_redis.refill 调用。依赖 frida + 真机;js 在同目录 frida/ 下。
环境: /Users/houjie/venv/python3-forcrawl/bin/python  (frida 15.1.17)

原理: 每个设备 = 一套自洽假设备指纹(随机 android_id + 真实机型四件套/指纹/序列号)。
      spawn app,把 PROFILE 注入采集源(Java android_id + native __system_property_get),
      让 app 自己的 native 加密 vdata 向数美注册,拿回独立 smid(d="B"+smid)。
"""
import os
import json
import time
import random
import string
import uuid
import frida

PKG = "com.mcdonalds.gma.cn"
DEV = os.environ.get("FRIDA_DEV") or "FA6AL0310909"
HERE = os.path.dirname(os.path.abspath(__file__))
FRIDA_DIR = os.path.join(HERE, "frida")                       # mcd_project/frida/(自带)
BYPASS = os.path.join(FRIDA_DIR, "frida_bypass_combined_local.js")
HOOK = os.path.join(FRIDA_DIR, "hook_fake_device.js")

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
    # token: 等价真机持久化 UUID(AppInfoUtil.getUUID 存 SharedPreferences),一设备一个固定值
    return {"android_id": rand_android_id(), "token": uuid.uuid4().hex, "props": props, "_model": m["model"]}


def register_one(profile, timeout=55):
    """spawn app + 注入该 profile + 等数美用假 vdata 注册,返回 d="B"+smid(超时返回 None)。"""
    dev = frida.get_device(DEV, timeout=10)
    pid = dev.spawn([PKG])
    session = dev.attach(pid)
    s = session.create_script(open(BYPASS, encoding="utf-8").read())
    s.on("message", lambda m, d: None)
    s.load()
    js = "var INJECT_PROFILE = " + json.dumps({"android_id": profile["android_id"], "token": profile.get("token"), "props": profile["props"]}) + ";\n"
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

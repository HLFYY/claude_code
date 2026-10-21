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
import re
import json
import time
import random
import string
import uuid
import subprocess
import frida

ADB = os.path.expanduser('~/Library/Android/sdk/platform-tools/adb')


def _agree_popup(dev_serial, tries=8):
    """pm clear 后首启会弹隐私协议,必须点"同意"数美才会 initShumei 注册。
    动态 uiautomator 找"同意"按钮点击,找不到回退该机型固定坐标(540,1340)。"""
    for _ in range(tries):
        time.sleep(2)
        subprocess.run([ADB, '-s', dev_serial, 'shell', 'uiautomator', 'dump', '/sdcard/ui.xml'],
                       capture_output=True)
        xml = subprocess.run([ADB, '-s', dev_serial, 'shell', 'cat', '/sdcard/ui.xml'],
                             capture_output=True, text=True).stdout
        m = (re.search(r'text="同意"[^>]*bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"', xml)
             or re.search(r'bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"[^>]*text="同意"', xml))
        if m:
            x = (int(m.group(1)) + int(m.group(3))) // 2
            y = (int(m.group(2)) + int(m.group(4))) // 2
            subprocess.run([ADB, '-s', dev_serial, 'shell', 'input', 'tap', str(x), str(y)], capture_output=True)
            return True
    subprocess.run([ADB, '-s', dev_serial, 'shell', 'input', 'tap', '540', '1340'], capture_output=True)  # 回退
    return False

PKG = "com.mcdonalds.gma.cn"
DEV = os.environ.get("FRIDA_DEV") or "FA6AL0310909"
HERE = os.path.dirname(os.path.abspath(__file__))
FRIDA_DIR = os.path.join(HERE, "frida")                       # mcd_project/frida/(自带)
BYPASS = os.path.join(FRIDA_DIR, "frida_bypass_combined_local.js")
HOOK = os.path.join(FRIDA_DIR, "hook_fake_device_v2.js")   # v2: 增补 Build字段/property_find/文件重定向伪造

# frida-server 15.1.17（与 getd.sh 一致；关键：不是 16.1.9，见 [[project_mcd_frida_emulator]]）
FRIDA_SERVER = "/data/local/tmp/frida-server-15.1.17-android-arm64"
FRIDA_SERVER_TAG = "frida-server-15.1.17"   # ps 全名匹配，避开 pidof 15字符截断

# 假 /proc 文件内容生成(堵 boot_id/cpuinfo/arp 真机泄漏;exp9 探针实证数美会读这些)
SOC = {"qcom": "Qualcomm Technologies, Inc SM8250", "mt6889": "MT6889"}


def _fake_cpuinfo(hardware):
    soc = SOC.get(hardware, "Qualcomm Technologies, Inc SM8250")
    out = []
    for i in range(8):
        out += ["processor\t: %d" % i, "BogoMIPS\t: 38.40",
                "Features\t: fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp",
                "CPU implementer\t: 0x51", "CPU architecture: 8", "CPU variant\t: 0x7",
                "CPU part\t: 0x804", "CPU revision\t: 14", ""]
    out.append("Hardware\t: %s" % soc)
    return "\n".join(out) + "\n"


def _push_fake_files(profile, dev_serial):
    """把假 boot_id/cpuinfo/arp 写到设备 /data/local/tmp(chmod 644 可读),返回 {真实路径: 设备假路径}。"""
    import tempfile
    boot_id = str(uuid.uuid4())
    profile["boot_id"] = boot_id
    contents = {
        "/proc/sys/kernel/random/boot_id": boot_id + "\n",
        "/proc/cpuinfo": _fake_cpuinfo(profile["props"].get("ro.hardware", "qcom")),
        "/proc/net/arp": "IP address       HW type     Flags       HW address            Mask     Device\n",
    }
    mapping = {}
    for real, text in contents.items():
        devpath = "/data/local/tmp/fkdev_" + real.strip("/").replace("/", "_")
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt") as tf:
            tf.write(text)
            local = tf.name
        subprocess.run([ADB, "-s", dev_serial, "push", local, devpath], capture_output=True)
        subprocess.run([ADB, "-s", dev_serial, "shell", "chmod", "644", devpath], capture_output=True)
        os.unlink(local)
        mapping[real] = devpath
    return mapping

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
    # widevine_id: 伪造 MediaDrm deviceUniqueId(硬件级设备唯一ID,否则所有假设备连回同一台真机→706070)
    # env: 伪造整机环境指纹(数美 SMSDK.v1 明文里泄漏真机 Pixel 的屏幕/内存/桌面/输入法/安装时间),与机型自洽且每台不同
    scr = m.get("screen", {"w": 1080, "h": 2340, "dpi": 440})
    install_base = 1577836800000 + random.randint(0, 60 * 86400) * 1000   # 2020起随机安装时间
    LAUNCHERS = {
        "Xiaomi": ("com.miui.home", "系统桌面"), "Redmi": ("com.miui.home", "系统桌面"),
        "OnePlus": ("net.oneplus.launcher", "桌面"), "OPPO": ("com.oppo.launcher", "桌面"),
        "vivo": ("com.bbk.launcher2", "i桌面"),
    }
    lp, ll = LAUNCHERS.get(m["brand"], LAUNCHERS.get(m["manufacturer"], ("com.miui.home", "系统桌面")))
    env = {
        "screen": scr,                                   # a36/a92/a34/a90
        "total_mem": m.get("total_mem", 8 * 1024**3) + random.randint(0, 400) * 1024**2,  # a48
        "total_storage": m.get("total_storage", 128 * 1024**3),           # a71
        "launcher_pkg": lp,                              # a154(别再暴露 Pixel nexuslauncher)
        "launcher_label": ll,
        "first_install": install_base,                   # a125 fit
        "last_update": install_base + random.randint(0, 30 * 86400) * 1000,  # a125 lut
        "clear_ime": True,                               # a63 输入法列表清空
    }
    return {"android_id": rand_android_id(), "token": uuid.uuid4().hex, "props": props,
            "widevine_id": os.urandom(16).hex(), "env": env, "_model": m["model"]}


def _server_running(dev_serial):
    """frida-server 15.1.17 是否在跑(root ps 全名匹配)。"""
    out = subprocess.run([ADB, '-s', dev_serial, 'shell', "su -c 'ps -A'"],
                         capture_output=True, text=True).stdout
    return FRIDA_SERVER_TAG in (out or '')


def ensure_frida_server(dev_serial, wait=3):
    """检测 frida-server 15.1.17，没在跑则 root 启动(仿 getd.sh)。返回是否就绪。"""
    if _server_running(dev_serial):
        print("[build] frida-server 15.1.17 已在运行，跳过启动", flush=True)
        return True
    print("[build] 启动 frida-server 15.1.17 (root)…", flush=True)
    subprocess.run([ADB, '-s', dev_serial, 'shell', f"su -c 'setsid {FRIDA_SERVER} >/dev/null 2>&1 &'"],
                   capture_output=True)
    time.sleep(wait)
    if _server_running(dev_serial):
        print("[build] server 已就绪", flush=True)
        return True
    print(f"[build] ⚠️ server 启动失败，检查 {FRIDA_SERVER} 是否存在/可执行", flush=True)
    return False


def _pidof(dev_serial, tries=25):
    for _ in range(tries):
        out = subprocess.run([ADB, '-s', dev_serial, 'shell', 'pidof', PKG], capture_output=True, text=True).stdout.strip()
        if out:
            try:
                return int(out.split()[0])
            except Exception:
                pass
        time.sleep(0.3)
    return None


def register_one(profile, timeout=90, pm_clear=True, retries=5):
    """造一台全新设备并注册,返回 d="B"+smid(失败返回 None)。
    采用 attach(先正常启动 app 再附加,规避 spawn 期反调试闪退)+ 崩溃检测重试(仿 getd.sh)。
    每次尝试都 pm clear(全新设备状态,否则复用旧存储→706070);启动后 attach+注入,再点"同意"放行数美注册。
    ⚠️ 换机型点"同意"坐标(_agree_popup)按分辨率要调。"""
    if not ensure_frida_server(DEV):                 # 先确保 frida-server 在跑(仿 getd.sh),没起则自动启动
        print("[build] frida-server 未就绪，放弃本次造设备", flush=True)
        return None
    fake_files = _push_fake_files(profile, DEV)      # 写假 /proc 文件(boot_id/cpuinfo/arp)
    inj = json.dumps({"android_id": profile["android_id"], "token": profile.get("token"),
                      "props": profile["props"], "fake_files": fake_files,
                      "widevine_id": profile.get("widevine_id"), "env": profile.get("env")})
    hook_src = open(HOOK, encoding="utf-8").read()
    bypass_src = open(BYPASS, encoding="utf-8").read()

    for attempt in range(1, retries + 1):
        subprocess.run([ADB, '-s', DEV, 'shell', 'am', 'force-stop', PKG], capture_output=True)
        if pm_clear:                                  # 每次都清存储=全新设备
            subprocess.run([ADB, '-s', DEV, 'shell', 'pm', 'clear', PKG], capture_output=True)
            time.sleep(1)
        # 正常启动(非 spawn)→ app 自身启动期反调试在无 frida 下通过,更稳
        subprocess.run([ADB, '-s', DEV, 'shell', 'monkey', '-p', PKG, '-c',
                        'android.intent.category.LAUNCHER', '1'], capture_output=True)
        pid = _pidof(DEV)
        if not pid:
            print("[build] 第%d次: app 没起来,重试" % attempt, flush=True)
            continue
        session = hook = None
        try:
            dev = frida.get_device(DEV, timeout=10)
            session = dev.attach(pid)
            s = session.create_script(bypass_src); s.on("message", lambda m, d: None); s.load()
            hook = session.create_script("var INJECT_PROFILE = " + inj + ";\n" + hook_src)
            hook.on("message", lambda m, d: None); hook.load()
        except Exception as e:
            print("[build] 第%d次: attach/注入失败(%s),重试" % (attempt, e), flush=True)
            try: session and session.detach()
            except Exception: pass
            continue
        if pm_clear:
            _agree_popup(DEV)                         # 点隐私协议"同意",数美才 initShumei 注册
        exp = hook.exports_sync if hasattr(hook, "exports_sync") else hook.exports

        d, deadline, crashed = None, time.time() + timeout, False
        while time.time() < deadline:
            time.sleep(2)
            try:
                cur = exp.getdraw()
            except Exception as e:
                es = str(e)
                if any(k in es for k in ("destroyed", "detached", "not been able", "died", "gone")):
                    crashed = True; break           # app 闪退/脚本销毁 → 重试
                continue
            if cur and not cur.startswith("ERR") and cur[0] == "B":
                d = cur; break
        try: session.detach()
        except Exception: pass
        if d:
            return d
        print("[build] 第%d次: %s,重试" % (attempt, "闪退" if crashed else "超时未拿到smid"), flush=True)
    return None

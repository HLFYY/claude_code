#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""local_device_gen.py —— 本地纯 HTTP 造数美设备(不用真机/frida)。

原理:一份真机 hook 来的 shortbox 模板(device_templates/ 下,见 ACTIVE_TEMPLATE),替换身份字段
     (android_id/机型/时间) → mcd_shumei_d.make_box 打包 → POST 数美注册接口 → 拿 B+smid。
     shortbox 不含硬件根字段(a84 等),故可批量造独立设备,绕开真机 706070 的物理机累计。
     硬件根/反篡改层只在 fullbox:a84(Widevine DRM 唯一ID)/a19(WiFi BSSID)/a144(字段完整性映射)
     /a74(调试器)/a75~a78(Xposed)/a172(虚拟输入设备)等。shortbox 白名单(35 字段)一个都不含,
     别往 box 里补这些——会被数美白名单过滤、补了也发不出,混入未知键还可能触发 1902。
     全字段含义/来源/真机值见 docs/数美box字段含义.md。
供 device_pool_redis.refill(build_mode='local') 调用;每台走新神龙短效代理注册。
"""
import os
import json
import time
import random
import uuid

import requests

from mcd_shumei_d import make_box, _box_json
from device_builder import MODELS
import utils
import mcd_api

HERE = os.path.dirname(os.path.abspath(__file__))
# 设备模板统一放 device_templates/,命名「类型_设备_日期.json」(如 shortbox_MOA-AL00_20261021.json)。
# ACTIVE_TEMPLATE = 当前使用的干净 shortbox;换新云手机 hook 到新模板后,改这里指向新文件即可。
# ⚠️ 一份模板只配少量号:a2/a97 是数美「每次注册唯一」的现签 token,复用一份灌多台会被识别为克隆→整组连坐封。
#    详见 ../mcd/docs/踩坑与结论日志.md(a2/a97、706070 批量封号根源)。
TEMPLATE_DIR = os.path.join(HERE, 'device_templates')
# ACTIVE_TEMPLATE = 'shortbox_STK-AL00_20261022.json'  # 当前干净模板(MOA-AL00 已烧)
ACTIVE_TEMPLATE = 'shortbox_J510FN-dalao_20261030.json'
# ACTIVE_TEMPLATE = 'shortbox_A156E-dalao-au_20261102.json'
TEMPLATE = json.load(open(os.path.join(TEMPLATE_DIR, ACTIVE_TEMPLATE), encoding='utf-8'))

# ── shortbox 35 字段白名单(jadx 实证,数美 l111l1111lIl.l1l1I1l 这个 Set)──
# 只保留白名单内字段:脏模板带入的未知键会被数美丢弃,混进去还可能触发 1902;见 docs/数美box字段含义.md。
SHORTBOX_WHITELIST = {
    'a1', 'a2', 'a4', 'a6', 'a7', 'a9', 'a10', 'a18', 'a21', 'a24', 'a38', 'a39',
    'a40', 'a44', 'a46', 'a60', 'a62', 'a85', 'a96', 'a97', 'a98', 'a103', 'a108',
    'a109', 'a110', 'a111', 'a121', 'a125', 'a127', 'a133', 'a135', 'a136', 'a155',
    'a167', 'a168',
}
# ── 全新设备"首次注册"本不该带的字段(带了=替别的设备背现签token/空壳=克隆/关联 tell)──
# a2/a97/a108/a109/a110 = 注册态 holder 产出(服务器握手后才有),全新设备为空 → 删。
# a98/a111/a21 = 未注册设备为空的列表/占位 → 为空则删(别送空壳)。
# 注:a103(puuid) 不在此列——它是本地安装期 uuid,全新设备本就有,gen 里每台重随机。
FRESH_DROP = ('a2', 'a97', 'a108', 'a109', 'a110')
FRESH_DROP_IF_EMPTY = ('a98', 'a111', 'a21')

REGISTER_URL = "https://fp-it.fengkongcloud.com/deviceprofile/v4"
REGISTER_HEADERS = {
    "Content-Type": "application/octet-stream",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 10; Pixel Build/QP1A.190711.020)",
    "Host": "fp-it.fengkongcloud.com", "Accept-Encoding": "gzip", "Connection": "close",
}


def gen_shortbox():
    """模板 + 替换身份字段(每台不同) → 新设备 shortbox。返回 (box, model_dict)。"""
    m = random.choice(MODELS)
    box = json.loads(json.dumps(TEMPLATE))               # 深拷贝模板
    # ── ① 每台必须唯一的身份字段:随机(否则多台被判同一设备) ──
    box['a24'] = os.urandom(8).hex()                     # android_id
    box['a46'] = {"model": m["model"], "serial": "", "brand": m["brand"],
                  "manufacturer": m["manufacturer"], "fingerprint": m["fp"]}  # 机型四件套
    # a10(系统大版本)必须与机型指纹自洽:否则 a10 vs fingerprint 版本矛盾 = 风控 tell
    # (当前 A156E 模板 a10="16",但 MODELS 指纹是 Android 11/12,不改就是矛盾)
    try:
        box['a10'] = m["fp"].split(':', 1)[1].split('/', 1)[0]   # 取指纹里的 Android 版本,如 "11"
    except Exception:
        pass
    if 'a103' in box:
        box['a103'] = str(uuid.uuid4())                  # 每安装唯一 uuid
    if 'a60' in box:  box['a60'] = 'u0_a' + str(random.randint(100, 250))  # app 的 Linux UID(每安装不同)
    # ── ② 时间字段:取当下/近期(真机态) ──
    now = int(time.time() * 1000)
    box['a9'] = now
    box['a40'] = now - random.randint(1000, 99999)
    base = 1577836800000 + random.randint(0, 60 * 86400) * 1000
    box['a125'] = {"fit": base, "lut": base + random.randint(0, 30 * 86400) * 1000}  # 安装/更新时间
    # ── ③ 正常手机特征:写成真机常见值,别随机成异常/云机特征 ──
    # SIM 必须"有卡"(LOADED/READY),并去掉 sys.usb.state=mtp、adb 等 USB 调试痕迹
    # (红手指等云机实测 gsm.sim.state=ABSENT → 直接暴露无卡,这里强制有卡)
    box['a18'] = {"gsm.sim.state": random.choice(["LOADED", "READY"])}
    # a44 网络类型:只用抓包确认过的真机值 WIFI;没拿到蜂窝的确切字符串,别凭空造 "4G" 反而成新 tell
    box['a44'] = "WIFI"
    # a4 = app 启动计数,新装机取小值(真机实测 1/3/4),别用 1~9 大随机
    if 'a4' in box:   box['a4'] = str(random.randint(1, 4))
    # a127/a133:收敛到真机观测区间(真机 a127≈55~585,a133≈329~1219),别用 100~3000 的大随机
    if 'a127' in box: box['a127'] = random.randint(50, 600)
    if 'a133' in box: box['a133'] = random.randint(300, 1300)
    # a167==a168:两台真机实测均为 0(非 50万~200万随机),保持真机态
    if 'a167' in box: box['a167'] = 0
    if 'a168' in box: box['a168'] = 0
    # a121 = Settings.Secure "adb_enabled"(jadx 实证):正常手机 adb 关 = 0(别漏带脏模板的 1)
    if 'a121' in box: box['a121'] = 0
    # ── ④ 清理:删服务端注册态字段 + 只留白名单(保证像"全新真机首次注册") ──
    # 全新设备没注册过 → 没有 a2/a97/a108~a110;模板若是云机 dump 的(含这些)会把别的设备的现签
    # token 背进来 → 克隆/关联 tell。无条件删掉(注册期只校验 a1,删它们仍 1100,已实验证明)。
    for k in FRESH_DROP:
        box.pop(k, None)
    for k in FRESH_DROP_IF_EMPTY:
        if k in box and box[k] in (None, '', [], {}):
            box.pop(k, None)
    # 只保留 shortbox 白名单字段(脏模板带入的未知键 → 丢弃,防 1902)
    box = {k: v for k, v in box.items() if k in SHORTBOX_WHITELIST}
    return box, m


def new_short_proxy(tries=3):
    """提一个神龙短效代理(随机支持城市,每次新)。返回 (proxies_dict, 'city/ip:port') 或 (None, None)。"""
    for _ in range(tries):
        city = '上海'
        ips = utils.extract_short_proxy_by_city(city, count=1)
        if ips:
            p = ips[0]
            return {'http': f'http://{p}', 'https': f'http://{p}'}, f"{city}/{p}"
    return None, None


def register_local(use_proxy=True, proxies=None, pinfo=None, proxy_retries=5, timeout=20):
    """
    本地造一台设备并注册数美,拿 d="B"+smid。

    Args:
        use_proxy: True=走神龙短效代理(不同出口IP,防关联)。
        proxies/pinfo: 外部传入【固定代理】(注册与后续发码共用同一出口IP);传了就不自动换。
        proxy_retries: 未传固定代理时,代理不可用最多换几次。
        timeout: 单次注册请求超时(秒)。

    Returns:
        {d, smid, model, android_id, token, shortbox, proxy} 或 None(被拒/代理不可用)。
    """
    box, m = gen_shortbox()
    body = _box_json(make_box(
        json.dumps(box, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')))
    print(box)
    print(body)
    fixed = proxies is not None                          # 外部固定代理模式(与发码同IP)
    attempts = 1 if (fixed or not use_proxy) else proxy_retries
    for _ in range(attempts):
        if fixed:
            pr, pf = proxies, pinfo
        elif use_proxy:
            pr, pf = new_short_proxy()
            if not pr:
                continue
        else:
            pr, pf = None, None
        try:
            r = requests.post(REGISTER_URL, data=body.encode('utf-8'),
                              headers=REGISTER_HEADERS, proxies=pr, timeout=timeout)
        except Exception as e:
            print(f"[local] 代理不可用({pf}): {str(e)[:60]}", flush=True)
            if fixed:
                return None                              # 固定代理失败不换(要与发码同IP)
            continue
        try:
            j = r.json()
        except Exception:
            print(f"[local] 响应非JSON code={r.status_code}: {r.text[:80]}", flush=True)
            return None
        print(j)
        code = j.get('code')
        did = (j.get('detail') or {}).get('deviceId')
        if code == 1100 and did:
            return {'d': 'B' + did, 'smid': did, 'model': m['model'],
                    'android_id': box['a24'], 'token': mcd_api.generate_token(),
                    'shortbox': box, 'proxy': pf}
        # code 非 1100(如 1902 字段问题) = 业务拒绝,非代理问题,不重试
        print(f"[local] 注册被拒 code={code} msg={j.get('msg')} proxy={pf}", flush=True)
        return None

    print("[local] 短效代理多次不可用,放弃本次", flush=True)
    return None


if __name__ == '__main__':
    import sys
    up = '--no-proxy' not in sys.argv
    res = register_local(use_proxy=up)
    if res:
        print(f"✓ 本地造设备 model={res['model']} android_id={res['android_id']} "
              f"proxy={res['proxy']}\n  d={res['d']}")
    else:
        print("✗ 失败")

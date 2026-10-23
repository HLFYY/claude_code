#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""local_device_gen.py —— 本地纯 HTTP 造数美设备(不用真机/frida)。

原理:一份真机 hook 来的 shortbox 模板(device_templates/ 下,见 ACTIVE_TEMPLATE),替换身份字段
     (android_id/机型/时间) → mcd_shumei_d.make_box 打包 → POST 数美注册接口 → 拿 B+smid。
     shortbox 不含硬件根字段(a84 等),故可批量造独立设备,绕开真机 706070 的物理机累计。
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
ACTIVE_TEMPLATE = 'shortbox_STK-AL00_20261022.json'  # 当前干净模板(MOA-AL00 已烧)
TEMPLATE = json.load(open(os.path.join(TEMPLATE_DIR, ACTIVE_TEMPLATE), encoding='utf-8'))

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
    box['a24'] = os.urandom(8).hex()                     # android_id
    box['a46'] = {"model": m["model"], "serial": "", "brand": m["brand"],
                  "manufacturer": m["manufacturer"], "fingerprint": m["fp"]}  # 机型四件套
    now = int(time.time() * 1000)
    box['a9'] = now
    box['a40'] = now - random.randint(1000, 99999)
    base = 1577836800000 + random.randint(0, 60 * 86400) * 1000
    box['a125'] = {"fit": base, "lut": base + random.randint(0, 30 * 86400) * 1000}  # 安装/更新时间
    if 'a103' in box:
        box['a103'] = str(uuid.uuid4())                  # 每安装唯一 uuid(云模板带, 逐台刷新)
    # 跨设备实测会变的字段也逐台伪造,让每台更独立(减少被归为一组的概率)。取值按两台干净机观测范围。
    # ⚠️ a2/a97 是数美现签 token、随机会 706070,无法伪造;这是本地造的天花板(复用同一 a2 会碰撞连坐)。
    if 'a4' in box:   box['a4'] = str(random.randint(1, 9))                 # SDK 调用计数之类
    if 'a60' in box:  box['a60'] = 'u0_a' + str(random.randint(100, 250))  # app 的 Linux UID(每安装不同)
    if 'a127' in box: box['a127'] = random.randint(100, 2000)
    if 'a133' in box: box['a133'] = random.randint(100, 3000)
    if 'a167' in box:                                                       # a167==a168(观测),保持相等
        v = random.randint(500000, 2000000)
        box['a167'] = v
        box['a168'] = v
    return box, m


def new_short_proxy(tries=3):
    """提一个神龙短效代理(随机支持城市,每次新)。返回 (proxies_dict, 'city/ip:port') 或 (None, None)。"""
    for _ in range(tries):
        city = random.choice(list(utils.config.SHENLONG_SUPPORTED))
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证码(数美滑块)登录 worker —— 常驻消费 Redis 队列，用 DrissionPage 自动过滑块并触发发码。

流程(方案①，worker 全包)：
  BLPOP mcd:captcha:login_queue 取 {phone, city, force_new_device}
   → LoginManager 取/建 token(存 Redis，供 verify 复用)
   → 直连/代理 发 sms/send
       · 200          → 无滑块，直接标 code_sent
       · 420          → DrissionPage 自动过滑块拿 PASS rid → 拼 x-mcd-sec-verify 重发 → 200 标 code_sent
       · 其它(如707053) → 标 failed
   → 写 mcd:captcha:status:{phone}
  不做登录(登录仍由人工填 code 走 mcd_server /api/login/verify)。

依赖 DrissionPage/ddddocr，用 venv 跑：
  /Users/houjie/venv/python3-forcrawl/bin/python captcha_login_worker.py [--show|--headless]
  浏览器默认按 hostname：本机开发有头(方便看)、线上服务器无头；--show 强制有头，--headless 强制无头
"""
import io
import re
import sys
import json
import time
import base64
import random
from urllib.parse import urlparse

import redis
import requests
from PIL import Image
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage._units.actions import Actions
from ddddocr import DdddOcr

from config import REDIS_SET_URL, USE_PROXY, IS_LOCAL, CAPTCHA_LOGIN_CONFIG
from mcd_api import aes_encrypt, build_headers, AES_KEY, API2_BASE, generate_token, activate_token
from login_manager import LoginManager
from utils import log_info

SM_URL = 'https://castatic.fengkongcloud.com/pr/v1.0.4/index.html'
SM_ORG = 'qvdTr7A9k1DIU04ha4eP'
SEND_PATH = '/bff/passport/verifyCode/sms/send'
SEND_URL = f'{API2_BASE}{SEND_PATH}'
IMG_HEADERS = {'Referer': 'https://castatic.fengkongcloud.com/',
               'User-Agent': 'Mozilla/5.0 (Linux; Android 10; Pixel) AppleWebKit/537.36'}
DELIVER_JS = f'''
window.deliverNativeParams(JSON.stringify({{organization:'{SM_ORG}',appId:'default',channel:'DEFAULT',data:{{os:'android',sdkver:'1.5.1'}},https:true}}));
return (typeof window.deliverNativeParams);
'''

_redis = redis.Redis(host=REDIS_SET_URL['host'], port=REDIS_SET_URL['port'],
                     db=REDIS_SET_URL['db'], password=REDIS_SET_URL['password'], decode_responses=True)
_ocr = DdddOcr(det=False, ocr=False, show_ad=False)


# ---------- Redis 状态/冻结 ----------
def set_status(phone, status, msg='', rid=''):
    key = CAPTCHA_LOGIN_CONFIG['status_key'].format(phone=phone)
    _redis.setex(key, CAPTCHA_LOGIN_CONFIG['status_ttl'],
                 json.dumps({'status': status, 'msg': msg, 'rid': rid, 'ts': int(time.time())}, ensure_ascii=False))
    log_info(f"[worker] {phone} status={status} {msg}")


def unfreeze(phone):
    _redis.delete(CAPTCHA_LOGIN_CONFIG['freezing_key'].format(phone=phone))


# ---------- 数美滑块(DrissionPage) ----------
def get_tracks(distance):
    trace, start, v0, t = [], 0.0, 0.5, 0.2
    faster = distance * 3 / 5
    while start < distance:
        a = 1.8 if start < faster else round(random.uniform(-0.7, -0.9), 2)
        move = max(1, int(v0 * t + 0.5 * a * t * t))
        v0 += a * t
        if start + move >= distance:
            trace.append(round(distance - start)); start = distance
        else:
            start += move; trace.append(round(move))
    return trace


def _slide_once(page):
    bg_ele = page.ele('.shumei_captcha_loaded_img_bg')
    fg_ele = page.ele('.shumei_captcha_loaded_img_fg')
    bg_bytes = requests.get(bg_ele.attr('src'), headers=IMG_HEADERS, timeout=10).content
    fg_bytes = requests.get(fg_ele.attr('src'), headers=IMG_HEADERS, timeout=10).content
    res = _ocr.slide_match(fg_bytes, bg_bytes, simple_target=True)
    native_w = Image.open(io.BytesIO(bg_bytes)).size[0]
    zoom = bg_ele.size[0] / native_w
    distance = res['target'][0] * zoom
    distance -= random.randint(0, max(1, int(distance * 0.02)))
    tracks = get_tracks(distance)

    btn = page.ele('.shumei_captcha_slide_btn')
    page.listen.start('ca/v2/fverify')
    ac = Actions(page)
    ac.hold(btn)
    time.sleep(random.uniform(0.1, 0.25))
    for tr in tracks:
        ac.move(tr, random.randint(-4, 4), duration=0.05)
    time.sleep(random.uniform(0.1, 0.2))
    ac.release(btn)
    pkt = page.listen.wait(timeout=12)
    page.listen.stop()
    if not pkt:
        return None, None
    body = pkt.response.body if isinstance(pkt.response.body, str) else str(pkt.response.body)
    risk = re.search(r'riskLevel"\s*:\s*"(\w+)"', body)
    rid = re.search(r'[?&]rid=([0-9a-zA-Z]+)', pkt.url)
    return (risk.group(1) if risk else None), (rid.group(1) if rid else None)


def get_pass_rid(browser_proxy=None, headless=True, max_try=5):
    co = ChromiumOptions()
    co.auto_port(True)  # 独立端口+临时用户目录，避免连到残留浏览器
    if headless:
        co.headless(True)
        co.set_argument('--window-size=1280,800')
    if browser_proxy:
        co.set_proxy(browser_proxy)  # 白名单 IP:端口，无账密
    page = ChromiumPage(co)
    try:
        page.get(SM_URL)
        time.sleep(2)
        page.run_js(DELIVER_JS)
        time.sleep(5)
        for i in range(1, max_try + 1):
            if not page.ele('.shumei_captcha_slide_btn', timeout=5):
                time.sleep(3); continue
            risk, rid = _slide_once(page)
            log_info(f"[worker] 滑动#{i} risk={risk} rid={rid}")
            if risk == 'PASS' and rid:
                return rid
            rf = page.ele('.shumei_captcha_img_refresh_btn', timeout=3)
            if rf:
                rf.click(); time.sleep(3)
        return None
    finally:
        page.quit()


# ---------- 发码 ----------
def browser_proxy_from(proxies):
    """requests 代理字典 → 浏览器用的 http://host:port(去账密，靠白名单)"""
    if not proxies:
        return None
    u = urlparse(proxies.get('http') or proxies.get('https'))
    return f"http://{u.hostname}:{u.port}" if u.hostname else None


def raw_send(phone, token, proxies, sec_verify=None):
    body = json.dumps({"regionCode": "86", "tel": aes_encrypt(phone, AES_KEY), "type": 1}, separators=(',', ':'))
    h = build_headers(token, sid='', body=body, method='POST', path=SEND_PATH)
    h['Host'] = 'api2.mcd.cn'
    if sec_verify:
        h['x-mcd-sec-verify'] = sec_verify
    return requests.post(SEND_URL, headers=h, data=body, proxies=proxies, timeout=15).json()


def handle_login(phone, city=None, force_new_device=False, headless=True):
    set_status(phone, 'sending', '开始发码')
    manager = LoginManager(phone, use_redis=True, city=city)
    proxies = manager._get_proxy_dict() or None  # None=直连；USE_PROXY 关时为 None
    if force_new_device or not manager.token:
        manager.token = generate_token()
        ok, tid, msg = activate_token(manager.token, proxies=proxies)
        if not ok:
            set_status(phone, 'failed', f'token激活失败:{msg}'); unfreeze(phone); return
        manager.save_credentials(manager.token, None, None)  # 存 token 供 verify 复用
    token = manager.token

    r1 = raw_send(phone, token, proxies)
    code = r1.get('code')
    if r1.get('success') or code == 200:
        set_status(phone, 'code_sent', '无滑块直接发送'); return  # 保持冻结到 verify
    if code != 420:
        set_status(phone, 'failed', f"{code}:{r1.get('message')}"); unfreeze(phone); return

    sc = (r1.get('data') or {}).get('securityContext', {})
    punish_id, verify_sdk = sc.get('punishId'), sc.get('verifySdk')
    rid = get_pass_rid(browser_proxy=browser_proxy_from(proxies), headless=headless)
    if not rid:
        set_status(phone, 'failed', '滑块多次未通过'); unfreeze(phone); return

    ts = int(time.time() * 1000)
    sec = base64.b64encode(f"{punish_id};{verify_sdk};{ts};{rid}".encode()).decode()
    r2 = raw_send(phone, token, proxies, sec_verify=sec)
    if r2.get('success') or r2.get('code') == 200:
        set_status(phone, 'code_sent', '滑块通过已发码', rid=rid)  # 保持冻结到 verify
    else:
        set_status(phone, 'failed', f"重发失败 {r2.get('code')}:{r2.get('message')}"); unfreeze(phone)


# ---------- 主循环 ----------
def main():
    # 默认按环境：本机开发有头(方便看)、线上服务器无头；--show / --headless 手动覆盖
    headless = not IS_LOCAL
    if '--show' in sys.argv:
        headless = False
    if '--headless' in sys.argv:
        headless = True
    log_info(f"[worker] 启动，IS_LOCAL={IS_LOCAL} USE_PROXY={USE_PROXY} headless={headless}，监听 {CAPTCHA_LOGIN_CONFIG['queue_key']}")
    while True:
        item = _redis.blpop(CAPTCHA_LOGIN_CONFIG['queue_key'], timeout=30)
        if not item:
            continue
        try:
            payload = json.loads(item[1])
        except Exception:
            payload = {'phone': item[1]}
        phone = payload.get('phone')
        if not phone:
            continue
        log_info(f"[worker] 取到任务 {phone} city={payload.get('city')}")
        try:
            handle_login(phone, city=payload.get('city'),
                         force_new_device=payload.get('force_new_device', False), headless=headless)
        except Exception as e:
            import traceback
            log_info(f"[worker] {phone} 异常: {traceback.format_exc()}")
            set_status(phone, 'failed', f'异常:{e}'); unfreeze(phone)


if __name__ == '__main__':
    main()

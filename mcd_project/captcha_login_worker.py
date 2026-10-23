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
import os
import re
import sys
import json
import time
import shutil
import base64
import random
import tempfile
import traceback
from urllib.parse import urlparse

import redis
import requests
from PIL import Image
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage._units.actions import Actions
from ddddocr import DdddOcr

from config import REDIS_SET_URL, USE_PROXY, IS_LOCAL, CAPTCHA_LOGIN_CONFIG, PROXY_BROWSER_AUTH, make_redis, DATA_DIR
from mcd_api import aes_encrypt, build_headers, AES_KEY, API2_BASE, generate_token, activate_token, ectrade_order_create
from login_manager import LoginManager
from utils import log_info, ensure_egress_whitelisted

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

_redis = make_redis()  # 带超时/重试/keepalive，Redis 慢也不卡 60s
_ocr = DdddOcr(det=False, ocr=False, show_ad=False)
_last_beat = 0.0  # beat 节流用


# ---------- Redis 状态/冻结 ----------
def set_status(phone, status, msg='', rid=''):
    key = CAPTCHA_LOGIN_CONFIG['status_key'].format(phone=phone)
    _redis.setex(key, CAPTCHA_LOGIN_CONFIG['status_ttl'],
                 json.dumps({'status': status, 'msg': msg, 'rid': rid, 'ts': int(time.time())}, ensure_ascii=False))
    log_info(f"[worker] {phone} status={status} {msg}")


def unfreeze(phone):
    _redis.delete(CAPTCHA_LOGIN_CONFIG['freezing_key'].format(phone=phone))


# ---------- 心跳（防"冻了但 worker 没在跑"）----------
def touch_worker_alive():
    _redis.setex(CAPTCHA_LOGIN_CONFIG['worker_alive_key'],
                 CAPTCHA_LOGIN_CONFIG['worker_alive_ttl'], str(int(time.time())))


def touch_processing(phone):
    _redis.setex(CAPTCHA_LOGIN_CONFIG['processing_key'].format(phone=phone),
                 CAPTCHA_LOGIN_CONFIG['processing_ttl'], str(int(time.time())))


def clear_processing(phone):
    _redis.delete(CAPTCHA_LOGIN_CONFIG['processing_key'].format(phone=phone))


def beat(phone):
    """处理某账号时上报心跳：同时刷新全局 worker_alive 和该账号 processing。
    节流：两次写最小间隔 beat_min_interval 秒，减少对慢 Redis 的压力。"""
    global _last_beat
    now = time.time()
    if now - _last_beat < CAPTCHA_LOGIN_CONFIG['beat_min_interval']:
        return
    _last_beat = now
    touch_worker_alive()
    touch_processing(phone)


def cleanup_zombie_freezes():
    """启动时清理上次 worker 崩溃残留的僵尸冻结：有 freezing、但没有 processing 且状态非 code_sent。"""
    try:
        pattern = CAPTCHA_LOGIN_CONFIG['freezing_key'].format(phone='*')
        proc_tpl = CAPTCHA_LOGIN_CONFIG['processing_key']
        status_tpl = CAPTCHA_LOGIN_CONFIG['status_key']
        cleared = 0
        for key in _redis.scan_iter(match=pattern, count=100):
            phone = key.split(':')[-1]
            if _redis.exists(proc_tpl.format(phone=phone)):
                continue  # 正在处理，保留
            raw = _redis.get(status_tpl.format(phone=phone))
            status = json.loads(raw).get('status') if raw else None
            if status == 'code_sent':
                continue  # 已发码待 verify，保留冻结
            _redis.delete(key)
            cleared += 1
        log_info(f"[worker] 启动清理僵尸冻结: {cleared} 个")
    except Exception as e:
        log_info(f"[worker] 清理僵尸冻结失败: {e}")


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


# 失败现场取证目录（服务器上滑块抓不到 fverify 时用）
DEBUG_DIR = os.path.join(DATA_DIR, 'captcha_debug')


def _dbg_dump(page, tag):
    """失败现场取证：截图 + 存 HTML + 打印 url/title。返回截图路径。"""
    shot = None
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        name = f'{tag}_{time.strftime("%H%M%S")}.png'
        page.get_screenshot(path=DEBUG_DIR, name=name)
        shot = os.path.join(DEBUG_DIR, name)
    except Exception as e:
        log_info(f"[worker][dbg] 截图失败: {e}")
    try:
        html_path = os.path.join(DEBUG_DIR, f'{tag}_{time.strftime("%H%M%S")}.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(page.html or '')
    except Exception as e:
        log_info(f"[worker][dbg] 存HTML失败: {e}")
    try:
        log_info(f"[worker][dbg] url={page.url} title={page.title!r} 截图={shot}")
    except Exception:
        pass
    return shot


def _fetch_img(ele):
    """取滑块图:优先经浏览器(走代理、支持 data-uri、不依赖 Pi 直连),失败再 requests 兜底。"""
    try:
        b = ele.src(timeout=8, base64_to_bytes=True)   # DrissionPage 经浏览器会话取图
        if b:
            return b
    except Exception as e:
        log_info(f"[worker][dbg] ele.src 取图失败: {str(e)[:60]}")
    try:
        src = ele.attr('src')
        if src and src.startswith('http'):
            return requests.get(src, headers=IMG_HEADERS, timeout=10).content
    except Exception as e:
        log_info(f"[worker][dbg] requests 兜底取图失败: {str(e)[:60]}")
    return None


def _slide_once(page):
    bg_ele = page.ele('.shumei_captcha_loaded_img_bg', timeout=3)
    fg_ele = page.ele('.shumei_captcha_loaded_img_fg', timeout=3)
    if not bg_ele or not fg_ele:
        log_info(f"[worker][dbg] 滑块图元素缺失 bg={bool(bg_ele)} fg={bool(fg_ele)}")
        return None, None
    bg_bytes = _fetch_img(bg_ele)
    fg_bytes = _fetch_img(fg_ele)
    if not bg_bytes or not fg_bytes:
        log_info(f"[worker][dbg] 取滑块图失败 bg={bool(bg_bytes)} fg={bool(fg_bytes)}")
        return None, None
    res = _ocr.slide_match(fg_bytes, bg_bytes, simple_target=True)
    native_w = Image.open(io.BytesIO(bg_bytes)).size[0]
    disp_w = bg_ele.size[0]
    zoom = disp_w / native_w
    distance = res['target'][0] * zoom
    distance -= random.randint(0, max(1, int(distance * 0.02)))
    log_info(f"[worker][dbg] slide_match target={res['target']} native_w={native_w} "
             f"disp_w={disp_w} zoom={zoom:.3f} distance={distance:.0f}")
    tracks = get_tracks(distance)

    btn = page.ele('.shumei_captcha_slide_btn')
    btn_x0 = None
    try:
        btn_x0 = btn.rect.location[0]
    except Exception:
        pass
    page.listen.start('ca/v2/fverify')
    ac = Actions(page)
    ac.hold(btn)
    time.sleep(random.uniform(0.1, 0.25))
    for tr in tracks:
        ac.move(tr, random.randint(-4, 4), duration=0.05)
    time.sleep(random.uniform(0.1, 0.2))
    ac.release(btn)
    pkt = page.listen.wait(timeout=15)  # 15s（短效代理~180s TTL，要在150s内完成5次重试）
    page.listen.stop()
    if not pkt:
        # 诊断:按钮到底动没动 + 是否被数美判定机器人(webdriver)
        btn_x1 = None
        try:
            btn_x1 = page.ele('.shumei_captcha_slide_btn').rect.location[0]
        except Exception:
            pass
        try:
            wd = page.run_js('return navigator.webdriver')
        except Exception:
            wd = '?'
        moved = (btn_x0 is not None and btn_x1 is not None and abs(btn_x1 - btn_x0) > 5)
        log_info(f"[worker][dbg] 未捕获 fverify: 按钮移动={moved}(x {btn_x0}->{btn_x1}) navigator.webdriver={wd} "
                 f"(移动=True→多半数美判机器人不下发校验,需隐身/真Chrome;移动=False→拖拽未生效)")
        return None, None
    body = pkt.response.body if isinstance(pkt.response.body, str) else str(pkt.response.body)
    risk = re.search(r'riskLevel"\s*:\s*"(\w+)"', body)
    rid = re.search(r'[?&]rid=([0-9a-zA-Z]+)', pkt.url)
    if not risk:
        log_info(f"[worker][dbg] fverify 已捕获但未解出 riskLevel，body 前200: {body[:200]}")
    return (risk.group(1) if risk else None), (rid.group(1) if rid else None)


def make_proxy_auth_extension(host, port, user, pwd):
    """生成临时 Chrome 扩展：用 chrome.proxy 配代理 + onAuthRequired 回填账密。返回扩展目录。"""
    d = tempfile.mkdtemp(prefix='pxauth_')
    manifest = {
        "name": "pxauth", "version": "1.0", "manifest_version": 2,
        "permissions": ["proxy", "webRequest", "webRequestBlocking", "<all_urls>", "tabs"],
        "background": {"scripts": ["background.js"]},
    }
    bg = (
        'var cfg={mode:"fixed_servers",rules:{singleProxy:{scheme:"http",host:"%s",port:%s},'
        'bypassList:["localhost","127.0.0.1"]}};'
        'chrome.proxy.settings.set({value:cfg,scope:"regular"},function(){});'
        'chrome.webRequest.onAuthRequired.addListener('
        'function(details){return {authCredentials:{username:"%s",password:"%s"}};},'
        '{urls:["<all_urls>"]},["blocking"]);'
    ) % (host, port, user, pwd)
    with open(os.path.join(d, 'manifest.json'), 'w', encoding='utf-8') as f:
        json.dump(manifest, f)
    with open(os.path.join(d, 'background.js'), 'w', encoding='utf-8') as f:
        f.write(bg)
    return d


def get_pass_rid(proxy_parts=None, headless=True, max_try=5, on_beat=None, tag='sm'):
    """proxy_parts: {host,port,user,pwd} 或 None。
    有账密且 PROXY_BROWSER_AUTH=True → 动态扩展带账密；否则白名单(只用 IP:端口)。
    tag: 失败截图/HTML 文件名前缀（一般传 phone），便于服务器上定位现场。"""
    co = ChromiumOptions()
    co.auto_port(True)  # 独立端口+临时用户目录，避免连到残留浏览器
    ext_dir = None
    use_ext = bool(proxy_parts and proxy_parts.get('user') and PROXY_BROWSER_AUTH)
    # Linux 服务器(尤其 root)起 Chrome 必需；mac 下也无害
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-dev-shm-usage')  # 容器 /dev/shm 太小会崩
    co.set_argument('--disable-blink-features=AutomationControlled')  # 去自动化标记,navigator.webdriver 不为 true(数美 bot 检测常命中点)
    if headless:
        co.set_argument('--headless=new')  # 新无头(扩展/服务器都更稳)
        co.set_argument('--disable-gpu')
        co.set_argument('--window-size=1280,800')
    if proxy_parts:
        if use_ext:
            ext_dir = make_proxy_auth_extension(proxy_parts['host'], proxy_parts['port'],
                                                proxy_parts['user'], proxy_parts['pwd'])
            co.add_extension(ext_dir)
            co.set_argument('--disable-features=DisableLoadExtensionCommandLineSwitch')  # 允许 --load-extension
            log_info(f"[worker] 浏览器走账密代理(扩展) {proxy_parts['host']}:{proxy_parts['port']}")
        else:
            co.set_proxy(f"http://{proxy_parts['host']}:{proxy_parts['port']}")  # 白名单
            log_info(f"[worker] 浏览器走白名单代理 {proxy_parts['host']}:{proxy_parts['port']}")
    page = ChromiumPage(co)
    try:
        # 页面加载超时设短：坏/慢代理上 page.get 默认会死等~2min，改 30s 快速失败→账号回队列换新代理重来
        try:
            page.set.timeouts(page_load=30)
            page.set.load_mode.eager()  # DOMContentLoaded 即返回，不等全部资源(SDK能更早就绪)
        except Exception:
            pass
        try:
            page.get(SM_URL)
        except Exception as e:
            log_info(f"[worker][dbg] 打开数美页面失败(代理/网络不通?): {e}")
            _dbg_dump(page, f'{tag}_openfail')
            return None
        deadline = time.time() + 155  # 短效代理 ~180s TTL：整个过滑块(加载+5次重试)155s 内完成，留余量
        # 等数美 SDK 注入 window.deliverNativeParams 再调用（慢代理/Pi 上 SDK 加载慢，sleep 死等会 is not a function）
        ok = False
        _t0 = time.time()
        while time.time() - _t0 < 15:
            try:
                if page.run_js('return typeof window.deliverNativeParams === "function"'):
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(0.4)
        if not ok:
            log_info("[worker][dbg] deliverNativeParams 15s 未就绪（SDK 没加载完/页面没正常加载/代理慢）")
            _dbg_dump(page, f'{tag}_nodeliver')
            return None
        page.run_js(DELIVER_JS)
        time.sleep(3)
        # 首屏取证：确认滑块是否真的渲染出来（服务器上常见就是根本没出滑块）
        if not page.ele('.shumei_captcha_slide_btn', timeout=8):
            log_info("[worker][dbg] 首屏 8s 未出现滑块按钮，dump 现场")
            _dbg_dump(page, f'{tag}_noslider')
        for i in range(1, max_try + 1):
            if time.time() > deadline:
                log_info(f"[worker] 已达 155s 总时限，停止重试（短效代理将过期），已尝试 {i-1} 次")
                break
            if on_beat:
                try:
                    on_beat()  # 过滑块每次尝试上报心跳（最耗时段，避免被判僵尸）
                except Exception:
                    pass
            if not page.ele('.shumei_captcha_slide_btn', timeout=5):
                time.sleep(3); continue
            try:
                risk, rid = _slide_once(page)
            except Exception as e:
                log_info(f"[worker][dbg] 滑动#{i} 异常: {e}\n{traceback.format_exc()}")
                _dbg_dump(page, f'{tag}_exc{i}')
                risk, rid = None, None
            log_info(f"[worker] 滑动#{i} risk={risk} rid={rid}")
            if risk == 'PASS' and rid:
                return rid
            _dbg_dump(page, f'{tag}_fail{i}')  # 失败现场：截图+HTML
            rf = page.ele('.shumei_captcha_img_refresh_btn', timeout=3)
            if rf:
                rf.click(); time.sleep(3)
        return None
    finally:
        page.quit()
        if ext_dir:
            shutil.rmtree(ext_dir, ignore_errors=True)


# ---------- 发码 ----------
def browser_proxy_parts(proxies):
    """requests 代理字典 → {host,port,user,pwd}（供浏览器用：有账密走扩展，无则白名单）"""
    if not proxies:
        return None
    u = urlparse(proxies.get('http') or proxies.get('https'))
    if not u.hostname:
        return None
    return {'host': u.hostname, 'port': u.port, 'user': u.username, 'pwd': u.password}


def _proxy_hp(proxies):
    """代理 host:port(隐藏账密)，无代理返回 直连"""
    if not proxies:
        return '直连'
    u = urlparse(proxies.get('http') or proxies.get('https'))
    if not u.hostname:
        return '直连'
    return f"{u.username + '@' if u.username else ''}{u.hostname}:{u.port}"


def raw_send(phone, token, proxies, tid='', d='', sec_verify=None):
    body = json.dumps({"regionCode": "86", "tel": aes_encrypt(phone, AES_KEY), "type": 1}, separators=(',', ':'))
    # 登录/发码场景头：biz 500/1011 + tid + st/nonce(自动) + d(有则带)，尽量贴近真机
    h = build_headers(token, sid='', body=body, method='POST', path=SEND_PATH,
                      tid=tid, d=d, biz_scenario='500', biz_from='1011')
    h['Host'] = 'api2.mcd.cn'
    if sec_verify:
        h['x-mcd-sec-verify'] = sec_verify
    return requests.post(SEND_URL, headers=h, data=body, proxies=proxies, timeout=15).json()


def handle_login(phone, city=None, force_new_device=False, headless=True,
                 province=None, merchant_id=None, proxy_source=None):
    beat(phone)  # 开始处理，上报心跳
    try:
        set_status(phone, 'sending', '开始发码')
        manager = LoginManager(phone, use_redis=True, city=city,
                               province=province, merchant_id=merchant_id, proxy_source=proxy_source)
        # 发码场景：未绑定→每次新短效代理(force_new)；已绑定→静态；USE_PROXY 关=None 直连。
        # 整个发码流程(activate/发码/滑块/重发)全程复用这同一个 proxies，保证 IP 一致性。
        proxies = manager.get_login_proxy(is_send_code=True)
        newly = force_new_device or not manager.token
        if newly:
            manager.token = generate_token()
        # 激活拿 tid（登录场景头用）；新 token 激活失败则中止，复用 token 失败则 tid 置空继续
        ok, tid, amsg = activate_token(manager.token, proxies=proxies)
        beat(phone)  # activate 后
        if newly and not ok:
            set_status(phone, 'failed', f'token激活失败:{amsg}'); unfreeze(phone); return
        if not ok:
            tid = ''
        manager.save_credentials(manager.token, None, None)  # 存 token 供 verify 复用
        token = manager.token

        # 打印本次账号相关数据
        log_info(f"[worker] 账号数据 phone={phone} city={city} newly={newly} "
                 f"token={token} tid={tid or '(无)'} "
                 f"d={manager.d or '(无)'} sid={'有' if manager.sid else '无'} "
                 f"proxy={_proxy_hp(proxies)}")

        r1 = raw_send(phone, token, proxies, tid=tid)
        beat(phone)  # 发码后
        code = r1.get('code')
        if r1.get('success') or code == 200:
            set_status(phone, 'code_sent', '无滑块直接发送'); return  # 保持冻结到 verify
        if code != 420:
            set_status(phone, 'failed', f"{code}:{r1.get('message')}"); unfreeze(phone); return

        sc = (r1.get('data') or {}).get('securityContext', {})
        punish_id, verify_sdk = sc.get('punishId'), sc.get('verifySdk')
        log_info(f"[worker] {phone} 命中 420  punishId={punish_id} verifyType={sc.get('verifyType')} verifySdk={verify_sdk}")
        rid = get_pass_rid(proxy_parts=browser_proxy_parts(proxies), headless=headless,
                           on_beat=lambda: beat(phone), tag=phone)  # 过滑块每次尝试刷心跳
        if not rid:
            set_status(phone, 'failed', '滑块多次未通过'); unfreeze(phone); return

        beat(phone)  # 重发前
        ts = int(time.time() * 1000)
        sec = base64.b64encode(f"{punish_id};{verify_sdk};{ts};{rid}".encode()).decode()
        r2 = raw_send(phone, token, proxies, tid=tid, sec_verify=sec)
        if r2.get('success') or r2.get('code') == 200:
            set_status(phone, 'code_sent', f'滑块通过已发码 rid={rid}', rid=rid)  # 保持冻结到 verify
        else:
            set_status(phone, 'failed', f"重发失败 {r2.get('code')}:{r2.get('message')}"); unfreeze(phone)
    finally:
        clear_processing(phone)  # 处理结束(成功/失败/异常)清掉该账号处理心跳


def set_mcgold_status(phone, status, msg='', **extra):
    """写麦金卡开通状态(queued/processing/done/failed)，done 时带 orderId/payId。"""
    data = {'status': status, 'msg': msg, 'ts': int(time.time())}
    data.update(extra)
    _redis.setex(CAPTCHA_LOGIN_CONFIG['mcgold_status_key'].format(phone=phone),
                 CAPTCHA_LOGIN_CONFIG['mcgold_status_ttl'],
                 json.dumps(data, ensure_ascii=False))


def handle_mcgold(payload, headless=True):
    """
    异步开通麦金卡/电商下单：order/create → 420 则过滑块(get_pass_rid,可内部重试4-5次)带
    x-mcd-sec-verify 重试 → 成功写 done(orderId/payId)。与发码共用同一滑块方案。
    """
    phone = payload.get('phone')
    sku_id = payload.get('sku_id')
    shop_id = payload.get('shop_id', 1)
    city_code = payload.get('city_code', '310100')
    city = payload.get('city')
    province = payload.get('province')
    merchant_id = payload.get('merchant_id')
    proxy_source = payload.get('proxy_source')
    beat(phone)
    try:
        set_mcgold_status(phone, 'processing', '开始开通')
        manager = LoginManager(phone, use_redis=True, city=city,
                               province=province, merchant_id=merchant_id, proxy_source=proxy_source)
        token, sid, mcd_id = manager.load_credentials()
        manager.apply_location(province, city)
        try:
            manager.ensure_device()
        except Exception as e:
            log_info(f"[worker] {phone} ensure_device: {e}")
        if not (token and sid):
            set_mcgold_status(phone, 'failed', '未登录'); return
        proxies = manager.get_login_proxy(is_send_code=True)  # 取新短效,滑块与下单同 IP
        log_info(f"[worker] 麦金卡 {phone} sku_id={sku_id} shop_id={shop_id} proxy={_proxy_hp(proxies)}")

        # 1) 下单；2) 命中 420 过滑块带 sec 重试(最多 2 轮外层)
        r = ectrade_order_create(token, sid, mcd_id, sku_id, city_code=city_code,
                                 shop_id=shop_id, proxies=proxies)
        beat(phone)
        for _ in range(2):
            if r.get('success'):
                break
            if r.get('code') != 420:
                break
            sc = (r.get('data') or {}).get('securityContext', {})
            punish_id, verify_sdk = sc.get('punishId'), sc.get('verifySdk')
            log_info(f"[worker] 麦金卡 {phone} 命中420 punishId={punish_id} 过滑块…")
            rid = get_pass_rid(proxy_parts=browser_proxy_parts(proxies), headless=headless,
                               on_beat=lambda: beat(phone), tag=f'mcgold-{phone}')
            if not rid:
                set_mcgold_status(phone, 'failed', '滑块多次未通过'); return
            beat(phone)
            sec = base64.b64encode(f"{punish_id};{verify_sdk};{int(time.time()*1000)};{rid}".encode()).decode()
            r = ectrade_order_create(token, sid, mcd_id, sku_id, city_code=city_code,
                                     shop_id=shop_id, sec_verify=sec, proxies=proxies)
            beat(phone)

        if r.get('success'):
            d = r.get('data') or {}
            set_mcgold_status(phone, 'done', '开通下单成功',
                              orderId=d.get('orderId'), payId=d.get('payId'))
            log_info(f"[worker] 麦金卡 {phone} 成功 orderId={d.get('orderId')} payId={d.get('payId')}")
        else:
            set_mcgold_status(phone, 'failed', f"{r.get('code')}:{r.get('message')}")
            log_info(f"[worker] 麦金卡 {phone} 失败 {r.get('code')}:{r.get('message')}")
    except Exception as e:
        import traceback
        log_info(f"[worker] 麦金卡 {phone} 异常: {traceback.format_exc()}")
        set_mcgold_status(phone, 'failed', f'异常:{e}')
    finally:
        clear_processing(phone)


# ---------- 主循环 ----------
def main():
    # 默认按环境：本机开发有头(方便看)、线上服务器无头；--show / --headless 手动覆盖
    headless = not IS_LOCAL
    if '--show' in sys.argv:
        headless = False
    if '--headless' in sys.argv:
        headless = True
    log_info(f"[worker] 启动，IS_LOCAL={IS_LOCAL} USE_PROXY={USE_PROXY} headless={headless}，监听 {CAPTCHA_LOGIN_CONFIG['queue_key']}")
    try:
        ensure_egress_whitelisted()  # 启动自动把本机出口IP加进 zdtps 白名单(多出口全加),免代理407
    except Exception as e:
        log_info(f"[worker] 自动加白名单跳过: {e}")
    cleanup_zombie_freezes()  # 启动清理上次崩溃残留的僵尸冻结
    while True:
        touch_worker_alive()  # 每轮上报全局心跳（含空闲），BLPOP 超时短，保证及时刷新
        item = _redis.blpop(CAPTCHA_LOGIN_CONFIG['queue_key'], timeout=CAPTCHA_LOGIN_CONFIG['blpop_timeout'])
        if not item:
            continue
        try:
            payload = json.loads(item[1])
        except Exception:
            payload = {'phone': item[1]}
        phone = payload.get('phone')
        if not phone:
            continue
        task = payload.get('type', 'login')   # 同队列按 type 分发：login(发码) / mcgold(开通麦金卡)
        log_info(f"[worker] 取到任务 type={task} {phone} city={payload.get('city')} "
                 f"province={payload.get('province')} merchant_id={payload.get('merchant_id')} "
                 f"proxy_source={payload.get('proxy_source')}")
        try:
            if task == 'mcgold':
                handle_mcgold(payload, headless=headless)
            else:
                handle_login(phone, city=payload.get('city'),
                             force_new_device=payload.get('force_new_device', False), headless=headless,
                             province=payload.get('province'), merchant_id=payload.get('merchant_id'),
                             proxy_source=payload.get('proxy_source'))
        except Exception as e:
            import traceback
            log_info(f"[worker] {phone} 异常: {traceback.format_exc()}")
            if task == 'mcgold':
                set_mcgold_status(phone, 'failed', f'异常:{e}')
            else:
                set_status(phone, 'failed', f'异常:{e}'); unfreeze(phone)


if __name__ == '__main__':
    main()

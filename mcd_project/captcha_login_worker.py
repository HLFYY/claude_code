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
import math
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
    """人类化滑动轨迹：ease-out 加速 → 轻微过冲 → 回拉修正，并带手腕弧线 Y 抖动与变速。
    返回 [(dx, dy, dt), ...]：dx/dy=该步相对位移(px)，dt=该步后停顿(s)。"""
    if distance <= 0:
        return []
    tracks = []
    overshoot = random.uniform(8, 22)                    # 先冲过目标一点(人类强特征)
    peak = distance + overshoot
    steps = max(25, int(distance / random.uniform(6.0, 9.0)))
    cur = 0.0
    for i in range(steps):
        p = (i + 1) / steps
        pos = peak * (1 - (1 - p) ** 3)                  # ease-out cubic：先快后慢
        dx = pos - cur
        cur = pos
        dy = math.sin(p * math.pi) * random.uniform(0.5, 1.6)   # 手腕弧线(中间起伏最大)
        tracks.append((dx, dy, random.uniform(0.006, 0.018)))
    # 回拉：peak → distance，2~5 小步(模拟过冲后往回修正)
    back = peak - distance
    bn = random.randint(2, 5)
    for i in range(bn):
        tracks.append((-back / bn + random.uniform(-0.6, 0.6),
                       random.uniform(-0.6, 0.6), random.uniform(0.02, 0.06)))
    return tracks


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


def _imgs_ready(page):
    """滑块前景/背景图是否真正加载完(complete 且 naturalWidth>0)。
    Windows 上常拖到"图片加载中…"的占位图→OCR 对空白算→缺口错，拖前必须先确认加载完。"""
    try:
        return bool(page.run_js(
            'var b=document.querySelector(".shumei_captcha_loaded_img_bg");'
            'var f=document.querySelector(".shumei_captcha_loaded_img_fg");'
            'if(!(b&&f&&b.complete&&f.complete&&b.naturalWidth>0&&f.naturalWidth>0))return false;'
            'var rb=b.getBoundingClientRect(),rf=f.getBoundingClientRect();'      # 还要真正布局出尺寸(>1)，否则 w=0→zoom错/按钮不动
            'return rb.width>1&&rb.height>1&&rf.width>1;'))
    except Exception:
        return False


def _slide_once(page):
    bg_ele = page.ele('.shumei_captcha_loaded_img_bg', timeout=3)
    fg_ele = page.ele('.shumei_captcha_loaded_img_fg', timeout=3)
    if not bg_ele or not fg_ele:
        log_info(f"[worker][dbg] 滑块图元素缺失 bg={bool(bg_ele)} fg={bool(fg_ele)}")
        return None, None
    # 下载/识别缺口【之前】先等图真正加载完(不是"图片加载中"占位)——否则 OCR 对着空白算，缺口必错。
    # 最多 2 轮、每轮轮询 ~5s：加载好→直接继续；没好→再等 1 轮；仍没好→本轮跳过(交回去点刷新换新图重试)。
    loaded = False
    for _rnd in range(2):
        _t = time.time()
        while time.time() - _t < 5:
            if _imgs_ready(page):
                loaded = True
                break
            time.sleep(0.3)
        if loaded:
            break
        log_info(f"[worker][dbg] 滑块图第{_rnd + 1}轮未加载完(图片加载中…)，继续等…")
    if not loaded:
        log_info("[worker][dbg] 滑块图2轮仍未加载完，本轮跳过刷新重试")
        return None, None
    bg_bytes = _fetch_img(bg_ele)
    fg_bytes = _fetch_img(fg_ele)
    if not bg_bytes or not fg_bytes:
        log_info(f"[worker][dbg] 取滑块图失败 bg={bool(bg_bytes)} fg={bool(fg_bytes)}")
        return None, None
    # 不用 simple_target：不同 ddddocr 版本(1.4.7/1.6.1)的 simple 返回基准点不一致(左边缘 vs 中心)，
    # 统一取完整框 [x1,y1,x2,y2] 的 x1=缺口左边缘，跨版本一致(1.6.1 python3.12 装不了 1.4.7，故用此法对齐)。
    res = _ocr.slide_match(fg_bytes, bg_bytes)
    _tgt = res.get('target') or []
    if len(_tgt) >= 4:
        gap_x = _tgt[0]                       # x1 = 缺口左边缘
    elif len(_tgt) >= 1:
        gap_x = _tgt[0]                       # 兜底(2元素时仍取首值)
    else:
        gap_x = res.get('target_x', 0)
    log_info(f"[worker][dbg] OCR raw target={res.get('target')} -> 缺口左边缘gap_x={gap_x}")
    # 下载图(OCR 坐标系)的原生尺寸
    _bgi = Image.open(io.BytesIO(bg_bytes)); _fgi = Image.open(io.BytesIO(fg_bytes))
    native_w, native_h = _bgi.size
    dl_fg_w, dl_fg_h = _fgi.size
    # 一次性量取全部几何(CSS 像素)：每个元素都给 {x,y,w,h}，含 bg图/拼图块/滑块按钮/滑条(父)/外层(祖父)。
    geo = {}
    try:
        geo = json.loads(page.run_js(
            'var q=function(s){return document.querySelector(s);};'
            'var R=function(e){if(!e)return null;var r=e.getBoundingClientRect();'
            'return {x:+r.left.toFixed(1),y:+r.top.toFixed(1),w:+r.width.toFixed(1),h:+r.height.toFixed(1)};};'
            'var N=function(e){return e?{nw:e.naturalWidth||0,nh:e.naturalHeight||0}:null;};'
            'var bg=q(".shumei_captcha_loaded_img_bg"),fg=q(".shumei_captcha_loaded_img_fg"),'
            'btn=q(".shumei_captcha_slide_btn");'
            'var bar=btn?btn.parentElement:null,bar2=bar?bar.parentElement:null;'
            'return JSON.stringify({dpr:window.devicePixelRatio,win_w:window.innerWidth,'
            'bg_cnt:document.querySelectorAll(".shumei_captcha_loaded_img_bg").length,'
            'fg_cnt:document.querySelectorAll(".shumei_captcha_loaded_img_fg").length,'
            'bg:R(bg),bg_nat:N(bg),fg:R(fg),fg_nat:N(fg),btn:R(btn),bar:R(bar),bar2:R(bar2)});'))
    except Exception as e:
        log_info(f"[worker][dbg] 量几何失败: {e}")
    _bg = geo.get('bg') or {}
    _fg = geo.get('fg') or {}
    _btn = geo.get('btn') or {}
    _bar = geo.get('bar') or {}
    bg_w = _bg.get('w', 0) or 0
    fg_w = _fg.get('w', 0) or 0
    btn_w = _btn.get('w', 0) or 0
    bar_w = _bar.get('w', 0) or 0
    # 派生量：拼图块相对 bg 的左偏移、按钮相对滑条的左偏移、轨道实际行程
    fg_left = round((_fg.get('x', 0) - _bg.get('x', 0)), 1) if (_fg and _bg) else 0
    btn_left = round((_btn.get('x', 0) - _bar.get('x', 0)), 1) if (_btn and _bar) else 0
    track = round(bar_w - btn_w, 1) if (bar_w > 1 and btn_w > 0 and bar_w > btn_w) else 0
    # 正确模型(物理)：
    #   zoom       = 背景显示宽/原生宽
    #   piece_move = 缺口在显示图里的 x(相对背景左) = target_x × zoom，也就是拼图块左边缘要移动到的位置
    #   换算       = 按钮行程(track) / 拼图块行程(bg_w - fg_w)，拼图块比按钮少走一个自身宽度
    #   distance   = piece_move × track/(bg_w - fg_w)
    zoom = (bg_w / native_w) if bg_w > 1 else 1.0
    piece_move = gap_x * zoom - fg_left                   # 缺口左边缘×zoom - 拼图块初始偏移
    if track > 1 and bg_w > fg_w > 0:
        distance = piece_move * track / (bg_w - fg_w)
        how = 'piece->btn'
    elif bg_w > 1:
        distance = piece_move                             # 退回按钮与拼图块 1:1
        how = 'direct'
    else:
        distance = gap_x                                   # 最差兜底(用原生像素)
        how = 'native'
    if distance < 0:
        distance = 0
    distance += random.uniform(-2, 2)                     # 轻微抖动(对称，不再系统性少走)
    # 预测落点(用于核对)：拖完拼图块左边缘应到 ≈ 背景左 + piece_move
    gap_abs_x = round(_bg.get('x', 0) + piece_move, 1)
    log_info(
        f"[worker][dbg] 滑块诊断:\n"
        f"  OCR: target={res.get('target')} 缺口左边缘gap_x={gap_x}\n"
        f"  下载图: bg={native_w}x{native_h} fg={dl_fg_w}x{dl_fg_h}\n"
        f"  bg背景图:  {geo.get('bg')} naturalWH={geo.get('bg_nat')} cnt={geo.get('bg_cnt')}\n"
        f"  fg拼图块:  {geo.get('fg')} naturalWH={geo.get('fg_nat')} cnt={geo.get('fg_cnt')} 相对bg左偏移={fg_left}\n"
        f"  滑块按钮:  {geo.get('btn')} 相对滑条左偏移={btn_left}\n"
        f"  滑条(父):  {geo.get('bar')}  外层(祖父):{geo.get('bar2')}\n"
        f"  dpr={geo.get('dpr')} win_w={geo.get('win_w')} 轨道行程={track}\n"
        f"  zoom={zoom:.3f} 拼图块需移动piece_move={piece_move:.1f} 缺口绝对x≈{gap_abs_x}\n"
        f"  => 用={how} 拖动distance={distance:.0f}")
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
    time.sleep(random.uniform(0.12, 0.3))        # 按下后的起手停顿
    for dx, dy, dt in tracks:                     # 人类化轨迹(ease-out+过冲+回拉+弧线抖动)
        ac.move(dx, dy, duration=0)
        time.sleep(dt)
    time.sleep(random.uniform(0.05, 0.12))        # 停稳一下再(必要时)微调
    # 末段微调：量落点，只有残差偏大(>5px)时缓慢修一次(人也会最后轻推一下)；小残差不动，避免生硬补偿被判机器。
    btn_per_piece = (track / (bg_w - fg_w)) if (track > 1 and bg_w > fg_w > 0) else 1.0
    _curpos = None
    try:
        _curpos = page.run_js(
            'var b=document.querySelector(".shumei_captcha_loaded_img_bg");'
            'var f=document.querySelector(".shumei_captcha_loaded_img_fg");'
            'return (f&&b)?(f.getBoundingClientRect().left-b.getBoundingClientRect().left):null;')
        if _curpos is not None:
            resid = piece_move - _curpos
            if abs(resid) > 5:
                step = resid * btn_per_piece
                # 分 2~3 小步缓慢推过去，带轻微抖动
                parts = random.randint(2, 3)
                for _k in range(parts):
                    ac.move(step / parts, random.uniform(-1, 1), duration=0)
                    time.sleep(random.uniform(0.03, 0.08))
        log_info(f"[worker][dbg] 末段微调: 落点={_curpos} 期望{piece_move:.1f} btn_per_piece={btn_per_piece:.3f}")
    except Exception as _e:
        log_info(f"[worker][dbg] 末段微调异常: {_e}")
    time.sleep(random.uniform(0.12, 0.25))        # 释放前的停顿
    ac.release(btn)
    # 落点核对：拖完拼图块左边缘相对 bg 的位移 vs 期望(piece_move)。误差大=公式/拖拽没到位；误差小还REJECT=行为被判。
    try:
        _land = page.run_js(
            'var b=document.querySelector(".shumei_captcha_loaded_img_bg");'
            'var f=document.querySelector(".shumei_captcha_loaded_img_fg");'
            'return (f&&b)?+(f.getBoundingClientRect().left-b.getBoundingClientRect().left).toFixed(1):-999;')
        log_info(f"[worker][dbg] 拖后拼图块落点(相对bg)={_land} 期望piece_move≈{piece_move:.1f} 误差={(_land - piece_move):.1f}")
    except Exception as _e:
        log_info(f"[worker][dbg] 落点测量失败: {_e}")
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


# Windows 下浏览器候选：优先 Chrome，退回 Edge（同为 Chromium 内核，DrissionPage 可直接驱动）
# 可用环境变量 MCD_BROWSER_PATH 强制指定，优先级最高
_BROWSER_CANDIDATES = [
    (r'C:\Program Files\Google\Chrome\Application\chrome.exe', 'Chrome'),
    (r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe', 'Chrome'),
    (r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe', 'Edge'),
    (r'C:\Program Files\Microsoft\Edge\Application\msedge.exe', 'Edge'),
]


def resolve_browser_path():
    """返回 (exe_path, 显示名)。exe_path 为 None 表示交给 DrissionPage 自己找（Mac/Linux）。"""
    env = os.environ.get('MCD_BROWSER_PATH', '').strip()
    if env:
        return env, ('Edge' if 'msedge' in env.lower() else 'Chrome(自定义)')
    if os.name != 'nt':
        return None, 'Chrome(自动查找)'
    for p, name in _BROWSER_CANDIDATES:
        if os.path.isfile(p):
            return p, name
    return None, 'Chrome(自动查找)'


def get_pass_rid(proxy_parts=None, headless=True, max_try=5, on_beat=None, tag='sm'):
    """proxy_parts: {host,port,user,pwd} 或 None。
    有账密且 PROXY_BROWSER_AUTH=True → 动态扩展带账密；否则白名单(只用 IP:端口)。
    tag: 失败截图/HTML 文件名前缀（一般传 phone），便于服务器上定位现场。"""
    co = ChromiumOptions()
    _bp, _bname = resolve_browser_path()
    if _bp:
        co.set_browser_path(_bp)
    log_info(f"[worker] 浏览器: {_bname}" + (f" -> {_bp}" if _bp else ""))
    co.auto_port(True)  # 独立端口+临时用户目录，避免连到残留浏览器
    ext_dir = None
    use_ext = bool(proxy_parts and proxy_parts.get('user') and PROXY_BROWSER_AUTH)
    # Linux 服务器(尤其 root)起 Chrome 必需；mac 下也无害
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-dev-shm-usage')  # 容器 /dev/shm 太小会崩
    co.set_argument('--disable-blink-features=AutomationControlled')  # 去自动化标记,navigator.webdriver 不为 true(数美 bot 检测常命中点)
    co.set_argument('--force-device-scale-factor=1')  # 强制 DPR=1：避免 Windows 显示缩放(125%/150%)把测量放大→缺口偏，三端一致
    co.set_argument('--window-size=1280,800')         # 固定窗口尺寸(非无头也固定)
    if headless:
        co.set_argument('--headless=new')  # 新无头(扩展/服务器都更稳)
        co.set_argument('--disable-gpu')
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


_VIP_CODE_VALID = '420'        # 要求过滑块：账号没被拦，只是要人机验证
_VIP_CODE_INVALID = '6000232'  # 系统检测您的账号环境存在异常，暂时无法下单
_VIP_MSG_INVALID = '异常'      # 服务端明确说异常 → 账号/设备侧不能开


def _vip_verdict(r):
    """判定「该账号能否开麦金卡」，返回 'valid' / 'invalid' / 'error'。

    口径要严格：**只有拿到一手证据才下结论**，其余一律 error，留给下一轮重测。

    - 'valid'   ← r.success（真开出来了）
                 或 code == 420（要求过滑块，说明账号本身没被拦）
    - 'invalid' ← code == 6000232（「系统检测您的账号环境存在异常，暂时无法下单」）
                 或 message 里含「异常」（服务端明确拒绝）
    - 'error'   ← 其余全部：代理异常、网络超时、连接重置、未知错误码……
                  这类结果是"没测出来"，不能当作账号资质写进凭证。
    """
    if not isinstance(r, dict):
        return 'error'
    if r.get('success'):
        return 'valid'
    code = str(r.get('code') if r.get('code') is not None else '').strip()
    if code == _VIP_CODE_VALID:
        return 'valid'
    msg = str(r.get('message') or '')
    if code == _VIP_CODE_INVALID or _VIP_MSG_INVALID in msg:
        return 'invalid'
    return 'error'


def handle_mcgold(payload, headless=True):
    """
    异步开通麦金卡/电商下单：order/create → 420 则过滑块(get_pass_rid,可内部重试4-5次)带
    x-mcd-sec-verify 重试 → 成功写 done(orderId/payId)。与发码共用同一滑块方案。

    凭证回写(vip_status/vip_time)的口径：**账号能否开麦金卡**，不是本次下单成败。
    success 或 420 → valid；6000232 或 message 含「异常」 → invalid；
    其余（代理/网络异常、未知码） → error，不当作账号资质，留给下一轮重测。
    成功/失败/异常都在 finally 统一写。
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
    manager = None      # 供 finally 回写凭证状态(构造失败时为 None)
    vip_result = None   # 'valid'/'invalid'/'error'，finally 里统一写 vip_status/vip_time
    try:
        set_mcgold_status(phone, 'processing', '开始开通')
        manager = LoginManager(phone, use_redis=True, city=city,
                               province=province, merchant_id=merchant_id, proxy_source=proxy_source)
        token, sid, mcd_id = manager.load_credentials()
        manager.apply_location(province, city)
        if sid and not token:
            token = manager.ensure_token()  # 只有 sid 无 token → 自动生成+激活+绑定
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
                # 能走到这里说明 r.code == 420 → 账号本身可开，只是本次滑块没过
                vip_result = _vip_verdict(r)
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
            vip_result = 'valid'   # 成功状态由 finally 统一回写凭证
            log_info(f"[worker] 麦金卡 {phone} 成功 orderId={d.get('orderId')} payId={d.get('payId')}")
        else:
            # 严格三分类：420 → valid(账号可开)；6000232 / message 含「异常」 → invalid；
            # 其余（代理/网络抖动、未知码） → error，本次不算结论，下轮重测
            vip_result = _vip_verdict(r)
            set_mcgold_status(phone, 'failed', f"{r.get('code')}:{r.get('message')}")
            log_info(f"[worker] 麦金卡 {phone} 失败 {r.get('code')}:{r.get('message')} "
                     f"(vip_status={vip_result})")
    except Exception as e:
        import traceback
        vip_result = 'error'   # 异常 ≠ 账号不能开：可能是代理/网络问题，不下结论
        log_info(f"[worker] 麦金卡 {phone} 异常: {traceback.format_exc()}")
        set_mcgold_status(phone, 'failed', f'异常:{e}')
    finally:
        # 成功/失败都回写凭证(vip_status + vip_time)，供 batch 脚本按状态分流
        if manager is not None and vip_result:
            try:
                manager.record_vip(vip_result)
                log_info(f"[worker] 麦金卡 {phone} 已写 vip_status={vip_result}")
            except Exception as _e:
                log_info(f"[worker] 麦金卡 {phone} 写 vip 状态失败: {_e}")
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

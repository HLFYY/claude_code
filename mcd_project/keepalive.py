# -*- coding: utf-8 -*-
"""账号保活（keepalive）：会话校验 + 模拟启动（打开 APP）。

2026-11-24 新增。背景：
  1) session/refresh 实测（2026-11-28）：只校验当前 sid 是否被服务端接受，
     sid 原样返回、不轮换、不续命；alive 仅代表接口 200，不代表会话寿命被延长。
     ★ 早期"refresh 每天激活一次就不掉/App 后台进程自动续期"系未经证实的猜测，已作废：
       真机 sid 静置数天不掉，说明寿命不由这类请求决定。本模块仅做"校验 + 痕迹"，不承诺续命。
  2) 11-23 真机抓包（cap_20261123_164049_70451）显示 App 启动时会并发打一批接口，
     其中 /bff/portal/home/index（主页）真机调了两次 —— 模拟真机打开 App 的痕迹。

本模块提供两件事：
  - keepalive_account()：对一个账号做「校验 + 假启动」，返回结论明细
  - run_keepalive()：批量跑（按商户/限额），把结果写回 Redis（供后台展示）

★ 与订单客户端 warmup 的区别：
  - 订单客户端的下单 warmup 在 mcd_order_client.py（客户端侧，走 HTTP 调 mcd_server）
  - 本模块在 mcd_server 进程内直接调 mcd_api（少一跳 HTTP，且能拿到 sid/rt 原始值）

★ 耗时控制（重要）：
  真机启动那 15 秒几乎全是人的停顿，接口本身只要 1~8ms；单请求走代理 1.4~3 秒。
  所以：预热（拉配置）串行 1 个把代理绑住，其余并发，整段 ≤ 2~3 秒（用户 2026-11-24 要求）。
"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout

import mcd_api
from utils import log_info
from config import make_redis, CREDENTIALS_KEY_TMPL
from pulse_protocol import (
    PULSE_WS_URL, PULSE_HEARTBEAT, PULSE_INTERVAL, build_pulse_headers,
    pulse_message_type as _pulse_message_type, pulse_proxy_options,
)

# ========== 平台回写（2026-11-27）==========
# 保活发现 sid 明确失效（dead / 401）时回写平台 verify-result(ok=false)，让平台把账号
# 标成「不可用」(status=2)，下次入队不再派它 —— 避免坏号每轮保活白跑 9 个 401。
# 令牌从共享 Redis mkt:{env}:{商户}:client:token 读（平台维护，与下单客户端同源）。
PLATFORM_API = os.environ.get('MCD_PLATFORM_API',
                              'https://connorguo.top/mkt_api/api/v1/client').rstrip('/')
CLIENT_TOKEN_TMPL = 'mkt:{env}:{merchant_id}:client:token'


def report_dead_to_platform(phone, merchant_id, msg='', redis_client=None):
    """sid 明确失效 → 回写平台标不可用。返回 (ok, note)；任何异常静默，不影响保活主流程。

    只在 dead（明确 401/登录失效）时调用；unknown（代理/网络抖动）绝不回写，
    否则一次代理故障就把好号标死。凭据 field（手机号）即平台 account_no（绝大多数一致）。
    """
    try:
        import requests
        r = redis_client or make_redis()
        env = os.environ.get('MCD_REGISTER_ENV', 'prod')
        tok = (r.get(CLIENT_TOKEN_TMPL.format(env=env, merchant_id=merchant_id)) or '').strip()
        if not tok:
            return False, '平台令牌为空'
        payload = {'account_no': phone, 'ok': False,
                   'error': ('保活检测 sid 失效: ' + str(msg or ''))[:300]}
        resp = requests.post(PLATFORM_API + '/accounts/verify-result',
                             json=payload, headers={'X-Client-Token': tok}, timeout=10)
        if resp.status_code != 200:
            return False, 'HTTP %s' % resp.status_code
        body = resp.json() if resp.headers.get('content-type', '').startswith('application/json') else {}
        if body and body.get('error'):
            return False, str(body.get('error'))[:80]
        return True, ''
    except Exception as e:
        return False, '%s: %s' % (type(e).__name__, str(e)[:60])


# ========== 可调参数（用户 2026-11-24 定：并发最多 2-3 秒） ==========
WARMUP_PREHEAT_TIMEOUT = 3.0     # 预热（拉配置）单请求最多等这么久
WARMUP_CONCURRENT_TIMEOUT = 2.5  # 并发段每个动作最多等这么久
WARMUP_HARD_CAP = 6.0            # 整段硬上限（兜底，防止线程卡死拖垮批处理）
WARMUP_SLEEP = 0.2               # 每个号之间的间隔，别把麦当劳打太急

# 号级并发数（2026-11-24）：各号互相独立，并发跑。
# 4 是保守值 —— 够把「最慢那个号」决定总耗时，又不会把麦当劳/代理打太急。
KEEPALIVE_WORKERS = 4

# 友盟设备级 token（与下单客户端 UMENG_DEVICE_TOKEN 同值；设备级、非账号级）
UMENG_DEVICE_TOKEN = 'AmbPB-Ggfah3sGoPm8Eb3F7sj40My522jpZEuI6VjoGt'

# 模拟启动要发的动作（按真机 11-23 抓包 16:42:01~16:42:10 的启动段）
# 顺序不重要（并发发），但「拉配置」单独预热、其余并发。
#   拉配置   /bff/common/normal/config       真机 16:42:01.505（第一个业务请求）
#   主页     /bff/portal/home/index          真机 16:42:09.404 + 09.483（调了两次）
#   会员     /bff/member/user/portal/v2      真机 16:42:09.962
#   通知上报 /bff/mc/remind/login/submit     真机 16:42:06.797
#   设备上报 /bff/member/device/collect      真机 16:42:10.870
#   （附近门店 /bff/store/stores/vicinity 用户 2026-11-24 要求去掉：保活只需要
#     「这个号今天打开过 App」的痕迹，附近门店既慢又和账号无关）
BOOT_ACTIONS = ('拉配置', '主页', '会员', '通知', '设备')

# 结果在 Redis 里的 key（24 小时过期，后台「保活设置」弹窗读它展示上次结果）
KEEPALIVE_LAST_KEY_TMPL = 'mkt:prod:{merchant_id}:keepalive:last'
KEEPALIVE_LAST_TTL = 86400  # 24 小时

# 城市上下文缓存（city → code/lat/lng），get_all_cities 拉一次，缓存 24h
_CITY_CTX = {'at': 0.0, 'map': {}}
_CITY_CTX_TTL = 86400

# ★ 静态城市码表（2026-11-24）：注册/保活必定带城市，city_code 不该依赖 get_all_cities
#   这个网络接口（冷启动第一次拉失败 → city_code 空 → 主页/会员/设备三个动作被跳过，
#   新号假启动只跑成 2/2）。city_codes.json 是从上游 /bff/store/cities/group 拉的
#   323 个城市真实 code + 中心经纬度，命中即用、零网络、零延迟。
#   lookup_city 优先查这里；查不到（新城市/表缺失）才回落 get_all_cities。
_STATIC_CITY_CTX = None


def _load_static_city_ctx():
    global _STATIC_CITY_CTX
    if _STATIC_CITY_CTX is not None:
        return _STATIC_CITY_CTX
    out = {}
    try:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'city_codes.json')
        with open(path, encoding='utf-8') as f:
            raw = json.load(f) or {}
        for name, entry in raw.items():
            e = {'code': str(entry.get('code') or '').strip(),
                 'lat': entry.get('lat'), 'lng': entry.get('lng')}
            if not e['code']:
                continue
            out[name] = e
            short = _normalize_city(name)
            if short and short not in out:
                out[short] = e
    except Exception:
        pass
    _STATIC_CITY_CTX = out
    return out


_redis = None


def _r():
    global _redis
    if _redis is None:
        _redis = make_redis()
    return _redis


# ========== 城市上下文 ==========

# 省 → 代表城市（短名）。注册任务常只给省、city 为空，用它兜底出 city_code。
# 这里的城市都已确认在 city_codes.json(323城) 和 mkt:prod:verify_store 门店表里。
_PROVINCE_CITY = {
    '北京': '北京', '天津': '天津', '上海': '上海', '重庆': '重庆',
    '河北': '石家庄', '山西': '太原', '内蒙古': '呼和浩特', '辽宁': '沈阳',
    '吉林': '长春', '黑龙江': '哈尔滨', '江苏': '南京', '浙江': '杭州',
    '安徽': '合肥', '福建': '厦门', '江西': '南昌', '山东': '青岛',
    '河南': '郑州', '湖北': '武汉', '湖南': '长沙', '广东': '深圳',
    '广西': '南宁', '海南': '海口', '四川': '成都', '贵州': '贵阳',
    '云南': '昆明', '西藏': '拉萨', '陕西': '西安', '甘肃': '兰州',
    '青海': '西宁', '宁夏': '银川', '新疆': '乌鲁木齐',
    '香港': '香港', '澳门': '澳门', '台湾': '台北',
}


def _city_by_province(province):
    """省名 → 代表城市短名；兼容「广东」「广东省」「广东省XX」写法。取不到返回 ''。"""
    p = (province or '').strip()
    if not p:
        return ''
    if p in _PROVINCE_CITY:
        return _PROVINCE_CITY[p]
    for k, v in _PROVINCE_CITY.items():
        if p.startswith(k):
            return v
    return ''


def _normalize_city(name):
    """账号属地写「深圳」，上游返回「深圳市」/「深圳市XX区」→ 统一成短名做匹配。"""
    n = (name or '').strip()
    for suf in ('特别行政区', '自治州', '地区', '盟', '县', '区', '市'):
        if n.endswith(suf):
            n = n[:-len(suf)]
            break
    return n


def get_city_context(token, sid, proxies=None):
    """拉全国城市表 → {短名: {code, lat, lng}}，缓存 24 小时。

    数据源 /bff/store/cities/group（get_all_cities）：每个城市带 code/latitude/longitude。
    ★ 这个接口走一次要 2~3 秒（代理往返），所以必须缓存；
      批量保活时第一个号拉一次，后面全部命中缓存。
    """
    now = time.time()
    if _CITY_CTX['map'] and (now - _CITY_CTX['at']) < _CITY_CTX_TTL:
        return _CITY_CTX['map']

    ok, data, msg = mcd_api.get_all_cities(token, sid, proxies=proxies)
    if not ok:
        return _CITY_CTX['map']

    out = {}
    for ct in (data.get('hotCities') or []):
        _add_city(out, ct)
    for g in (data.get('groups') or []):
        for ct in (g.get('cities') or []):
            _add_city(out, ct)
    if out:
        _CITY_CTX['map'] = out
        _CITY_CTX['at'] = now
    return out


def _add_city(out, ct):
    name = (ct.get('name') or '').strip()
    if not name:
        return
    code = str(ct.get('code') or '').strip()
    lat, lng = ct.get('latitude'), ct.get('longitude')
    if not code:
        return
    entry = {'code': code, 'lat': lat, 'lng': lng}
    short = _normalize_city(name)
    # 短名优先（广州→440100）；同时保留全名，便于「XX区」这类直接命中
    if short and short not in out:
        out[short] = entry
    if name not in out:
        out[name] = entry


def lookup_city(city_name, token=None, sid=None, proxies=None):
    """按城市名取 {code, lat, lng}；取不到返回 None（该号就跳过需要城市参数的请求）。

    2026-11-24：先查静态码表（323 城、零网络）—— 注册/保活的城市是确定的，
    city_code 不该因 get_all_cities 冷启动抖动而拿不到。静态表命中就直接用；
    只有表里没有（新城市/文件缺失）才回落网络接口。
    """
    name = (city_name or '').strip()
    if not name:
        return None
    static = _load_static_city_ctx()
    for cand in (name, _normalize_city(name), name + '市'):
        if cand and cand in static:
            return static[cand]
    # 静态表没命中，回落在线接口
    ctx = get_city_context(token, sid, proxies) if (token and sid) else _CITY_CTX['map']
    if not ctx:
        return None
    for cand in (name, _normalize_city(name), name + '市'):
        if cand and cand in ctx:
            return ctx[cand]
    return None


# ========== 门店 ==========

def lookup_store(city_name, redis_client=None):
    """按城市取验活探针门店 → (store_code, be_code)。

    来源 Redis mkt:prod:verify_store（hash，field=城市 → 'code' 或 'code;code2'，值可带 |be_code）。
    与下单客户端 probe_stores() 同源，保证验活/保活探的是同一家店。
    """
    r = redis_client or _r()
    city = (city_name or '').strip()
    if not city:
        return None
    try:
        raw = r.hget('mkt:prod:verify_store', city)
    except Exception:
        raw = None
    if not raw:
        return None
    first = str(raw).split(';')[0]
    parts = first.replace(',', '|').split('|')
    code = parts[0].strip()
    if not code:
        return None
    be = parts[1].strip() if len(parts) > 1 else ''
    return (code, be)


# ========== 模拟启动（假启动） ==========

def fake_launch(token, sid, d='', city='', province='', store_code='', city_code='',
                lat=None, lng=None, proxies=None):
    """模拟真机打开 APP：预热 1 个 + 其余并发，整段 ≤ 2~3 秒。

    返回 {'ok': int, 'total': int, 'detail': '动作 状态 耗时;...', 'elapsed': float}
    任一动作失败只记录，不抛异常 —— 保活不该因为某个动作失败就整体判死。
    """
    t_all = time.time()
    detail = []

    def _call(name, fn, **kw):
        t0 = time.time()
        try:
            ok, _d, msg = fn(token, sid, proxies=proxies, **kw)
            return name, bool(ok), (time.time() - t0) * 1000, str(msg)[:50]
        except Exception as e:
            return name, False, (time.time() - t0) * 1000, type(e).__name__ + ':' + str(e)[:40]

    # ① 预热：串行发一个 get_normal_config，把代理绑定下来
    #    （不预热的话并发请求会互相排队抢代理锁，比串行还慢）
    t_pre = time.time()
    name, ok, ms, msg = _call('拉配置', mcd_api.get_normal_config)
    detail.append((name, ok, ms, msg))
    ok_n = 1 if ok else 0
    if ok:
        t_par = time.time()
        specs = []
        # 主页：真机调两次，我们发一次即可（保活只要留下「访问过主页」的痕迹）
        if city_code and store_code:
            specs.append(('主页', mcd_api.get_home_index,
                          {'city_code': city_code, 'store_code': store_code}))
        if city_code:
            specs.append(('会员', mcd_api.get_member_portal, {'city_code': city_code}))
        specs.append(('通知', mcd_api.post_remind_login_submit, {}))
        if city_code and lat is not None and lng is not None:
            specs.append(('设备', mcd_api.post_device_collect,
                          {'city_code': city_code, 'latitude': lat, 'longitude': lng,
                           'um_token': UMENG_DEVICE_TOKEN}))
        if lat is not None and lng is not None:
            specs.append(('附近门店', mcd_api.get_nearby_stores,
                          {'latitude': lat, 'longitude': lng, 'show_type': '4'}))
        # ---- 2026-11-26：与下单 warmup 对齐，补进店批 + 画像批 ----
        if store_code:
            specs.append(('门店', mcd_api.get_store_info, {'store_code': store_code}))
            specs.append(('菜单', mcd_api.get_store_menu, {'store_code': store_code}))
            specs.append(('购物车', mcd_api.get_cart, {'store_code': store_code}))
            specs.append(('券卡', mcd_api.get_coupons_card,
                          {'store_code': store_code, 'city_code': city_code or ''}))
            specs.append(('店内活动', mcd_api.post_campaign_materials,
                          {'store_code': store_code}))
        specs.append(('模块上报1', mcd_api.post_version_mdl, {}))
        specs.append(('模块上报2', mcd_api.post_version_mdl, {}))
        specs.append(('实验分组', mcd_api.get_ab_experiments, {}))
        specs.append(('订单摘要', mcd_api.get_home_orderinfo, {}))
        specs.append(('热门活动', mcd_api.get_hot_activity, {}))
        # 会话刷新不在这里做——主流程已按 rt 有无选 login_session_refresh/login_refresh

        with ThreadPoolExecutor(max_workers=max(len(specs), 1)) as ex:
            futs = {ex.submit(_call, n, f, **kw): (n, f, kw) for n, f, kw in specs}
            # ⚠️ as_completed 超时会抛 TimeoutError，且「已完成但还没迭代到」的 future 会被丢掉
            #    → 必须自己兜住，并逐个 future.result() 收结果（2026-11-24 实测踩到）
            try:
                for fut in as_completed(futs, timeout=WARMUP_CONCURRENT_TIMEOUT + 1.5):
                    n, f, kw = futs[fut]
                    try:
                        nm, ok2, ms2, msg2 = fut.result(timeout=WARMUP_CONCURRENT_TIMEOUT)
                    except Exception:
                        nm, ok2, ms2, msg2 = n, False, WARMUP_CONCURRENT_TIMEOUT * 1000, '超时未返回'
                    detail.append((nm, ok2, ms2, msg2))
                    if ok2:
                        ok_n += 1
            except FuturesTimeout:
                pass
            # 补收：as_completed 抛异常时，已完成的 future 可能一个都没被迭代到
            for fut, (n, f, kw) in futs.items():
                if fut.done() and not any(d[0] == n for d in detail):
                    try:
                        nm, ok2, ms2, msg2 = fut.result(timeout=0.01)
                        detail.append((nm, ok2, ms2, msg2))
                        if ok2:
                            ok_n += 1
                    except Exception:
                        detail.append((n, False, WARMUP_CONCURRENT_TIMEOUT * 1000, '超时未返回'))
            # 未完成的 future 记一笔，别让统计凭空少一片
            for fut, (n, f, kw) in futs.items():
                if not fut.done() and not any(d[0] == n for d in detail):
                    detail.append((n, False, WARMUP_CONCURRENT_TIMEOUT * 1000, '超时未返回'))
        _ = t_par

    elapsed = time.time() - t_all
    total = len(detail)
    return {
        'ok': ok_n,
        'total': total,
        'elapsed': round(elapsed, 2),
        'detail': '；'.join('%s %s %.0fms%s' % (n, 'ok' if o else 'fail', ms,
                                               (' ' + m) if (not o and m) else '')
                            for n, o, ms, m in detail),
    }


# ---- pulse WebSocket 心跳会话（2026-11-26，与下单客户端 PULSE_* 常量同源）----
# 真机打开 APP 即建 pulse 长连接（15s 心跳，固定 39B gzip 包）——保活假启动成功后
# 模拟「用户打开 APP 停留几分钟」，daemon 线程到点自动断开，不阻塞保活主流程。
PULSE_KEEP_SECONDS = 180   # 保活场景：打开 APP 停留约 3 分钟


def _pulse_headers(sid, meddy, profile, st=None):
    """按 SocketRequest.kt 构造握手头；偏移默认 0，不额外发起校时请求。"""
    return build_pulse_headers(sid, meddy, profile, st=st, now=time.time())


def _pulse_keepalive_loop(phone, sid, meddy, proxy_ip, close_at):
    """限时后台心跳；close_at 使用 monotonic，失败停止且不影响保活校验结果。"""
    try:
        import websocket
    except ImportError:
        log_info('💓 保活pulse跳过(无websocket库) %s' % phone)
        return
    import zlib

    ws = None
    sent = acknowledged = other = invalid = 0
    reason = 'deadline'
    try:
        remaining = close_at - time.monotonic()
        if remaining <= 0:
            return
        headers = _pulse_headers(sid, meddy, mcd_api.CLIENT_PROFILE)
        kw = {'header': headers, 'timeout': min(20, remaining)}
        if proxy_ip:
            kw.update(pulse_proxy_options(proxy_ip), http_proxy_timeout=min(20, remaining))
        ws = websocket.create_connection(PULSE_WS_URL, **kw)
        log_info('💓 保活pulse连接建立 %s（保持%ds）' % (phone, PULSE_KEEP_SECONDS))
        next_send = time.monotonic() + PULSE_INTERVAL
        while time.monotonic() < close_at:
            now = time.monotonic()
            if now >= next_send:
                ws.send(PULSE_HEARTBEAT, opcode=websocket.ABNF.OPCODE_BINARY)
                sent += 1
                next_send = time.monotonic() + PULSE_INTERVAL
            remaining = min(close_at, next_send) - time.monotonic()
            if remaining <= 0:
                continue
            ws.settimeout(min(1, remaining))
            try:
                opcode, payload = ws.recv_data(control_frame=True)
            except websocket.WebSocketTimeoutException:
                continue
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                reason = 'peer_closed'
                break
            if opcode in (websocket.ABNF.OPCODE_PING, websocket.ABNF.OPCODE_PONG):
                continue
            try:
                if _pulse_message_type(payload) == 'HEARTBEAT_RESPONSE':
                    acknowledged += 1
                else:
                    other += 1
            except (ValueError, OSError, EOFError, zlib.error):
                invalid += 1
    except Exception as error:
        reason = type(error).__name__
    finally:
        if ws is not None:
            try:
                ws.close(timeout=3)
            except Exception:
                pass
        log_info('💓 保活pulse结束 %s reason=%s sent=%d ack=%d other=%d invalid=%d'
                 % (phone, reason, sent, acknowledged, other, invalid))


# ========== 单号保活 ==========

def keepalive_account(phone, merchant_id, city='', province='', proxies=None):
    """对一个账号做「会话校验 + 假启动」。

    会话校验优先用 session/refresh + refreshToken（真机链路，2026-11-24 用户定）；
    没有 rt 的号回落到空体 login/refresh（闸门校验）。
    ★ 2026-11-28 实测：两种 refresh 都只校验 sid 是否被服务端接受，sid 不轮换、不续命，
      alive 仅代表接口 200，不代表会话寿命延长 —— 别把"保活成功"当成"sid 被续期"。

    返回 dict：
      {phone, ok, state, msg, launch: {...}}
      state ∈ alive / dead / unknown
        alive   = 校验通过（session/refresh 返回 200；sid 未轮换、未被真正续命）
        dead    = 明确 401（登录失效或在其他设备登录）→ 建议删号
        unknown = 代理/网络/服务端异常 → 结论不确定，不能当掉线
    """
    from login_manager import LoginManager

    mgr = LoginManager(phone, None, True, city or None, None, None, None,
                       merchant_id, province or None, None)
    token, sid, meddy = mgr.load_credentials()
    rt = str(getattr(mgr, 'refresh_token', '') or '').strip()
    d = mgr.d or ''
    if not (token and sid):
        return {'phone': phone, 'ok': False, 'state': 'unknown', 'msg': '缺少 token 或 sid',
                'launch': None}

    try:
        mgr.apply_location(province or None, city or None)
    except Exception:
        pass

    # ====================================================================
    # 动作顺序（2026-11-27 用户定）：先假启动 → 再 session/refresh 校验 → pulse
    #
    # 真人逻辑：点亮手机打开 APP、产生一批访问痕迹（拉配置/主页/会员/设备…）。
    # 【修正 11-28】session/refresh 实测不换 sid、不续命，alive 仅代表校验通过；
    # "App 后台进程自动续期"系未证实猜测、已作废，本模块不承诺任何续命效果。
    # 原实现是「先校验、成功才假启动」，顺序反了：
    # 失效号连启动痕迹都不留，也不符合"模仿真人打开一次手机"。
    # 代理：假启动拿一次（不探活，探活与「拉配置」是同一个请求、白等 1~7s），
    # 会话校验复用同一个代理 = 同一出口 IP（真人一次打开全程一个出口）。
    # ====================================================================

    # ---- 1) 假启动（模拟打开 APP）----
    launch = None
    launch_dead = False  # 假启动动作里出现明确 401 → sid 失效信号，供后面 dead 判定
    for attempt in range(2):
        try:
            if proxies is None:
                proxies = mgr.get_login_proxy(is_send_code=False,
                                             force_new=(attempt > 0), probe=False)
            ctx = lookup_city(city, token, sid, proxies)
            store = lookup_store(city)
            store_code = store[0] if store else ''
            be_code = store[1] if store else ''
            launch = fake_launch(
                token, sid, d=d, city=city, province=province,
                store_code=store_code, city_code=(ctx or {}).get('code', ''),
                lat=(ctx or {}).get('lat'), lng=(ctx or {}).get('lng'), proxies=proxies)
            if be_code:
                launch['be_code'] = be_code
            detail = launch.get('detail') or ''
            if any(x in detail.lower() for x in ('401', '登录失效', '其他设备登录', '重新登录')):
                launch_dead = True
            # 预热（拉配置）失败且是代理层故障 → 换新代理重跑
            if attempt < 1 and launch.get('ok', 0) == 0 and any(
                    x in detail for x in ('代理连接失败', '请求超时', '网络连接失败',
                                          'ProxyError', 'ConnectionError')):
                proxies = None
                continue
            break
        except Exception as e:
            launch = {'ok': 0, 'total': 0, 'elapsed': 0,
                      'detail': '假启动异常: ' + str(e)[:60]}
            break

    # ---- 2) 会话校验（复用假启动的代理，同一出口 IP；不走代理探活）----
    last_proxy_err = ''
    ok = False
    msg = ''
    data = {}
    refresh_kind = 'session/refresh'
    for attempt in range(2):
        try:
            if proxies is None:
                proxies = mgr.get_login_proxy(is_send_code=False,
                                             force_new=(attempt > 0), probe=False)
        except Exception as e:
            proxies = None
            last_proxy_err = str(e)[:60]
        if rt:
            ok, data, msg = mcd_api.login_session_refresh(token, sid, rt, d=d, proxies=proxies)
        else:
            ok, msg = mcd_api.login_refresh(token, sid, d=d, proxies=proxies)
            refresh_kind = 'login/refresh'
        if ok:
            break
        # 代理层故障 → 换新代理再试一次；其它错误（如 401）直接定性，不浪费代理
        low = str(msg or '')
        is_proxy_fail = any(x in low for x in ('代理连接失败', '请求超时', '网络连接失败',
                                                'ProxyError', 'ConnectionError', '隧道连接失败'))
        if not is_proxy_fail or attempt >= 1:
            break
        proxies = None  # 强制重新取代理

    if ok and rt and isinstance(data, dict):
        # 上游可能返回新 sid/rt（真机样本是不轮换，但要真拿到才写回）
        new_sid = str(data.get('sid') or '').strip()
        new_rt = str(data.get('refreshToken') or '').strip()
        if new_sid and new_sid != sid:
            try:
                mgr.sid = new_sid
                mgr.save_credentials(token, new_sid, meddy, refresh_token=new_rt or rt)
            except Exception:
                pass

    low = str(msg or '').lower()
    dead = (not ok) and (any(x in low for x in ['401', '登录失效', '其他设备登录', '重新登录'])
                         or launch_dead)
    state = 'alive' if ok else ('dead' if dead else 'unknown')
    res = {'phone': phone, 'ok': bool(ok), 'state': state, 'msg': str(msg)[:120],
           'refresh_kind': refresh_kind, 'has_rt': bool(rt), 'launch': launch,
           'launch_dead': launch_dead}
    if last_proxy_err:
        res['msg'] = (res['msg'] + ' | 代理: ' + last_proxy_err)[:160]

    # ---- 3) pulse 心跳会话（校验通过 + 假启动成功才建；daemon 到点自动断）----
    # 会话没校验通过（dead/unknown）就不建心跳了 —— 会话都没了，心跳没意义。
    try:
        if ok and launch and launch.get('ok', 0) > 0 and PULSE_KEEP_SECONDS > 0:
            _px = ''
            if proxies:
                _px = proxies.get('http') or proxies.get('https') or ''
            threading.Thread(target=_pulse_keepalive_loop, daemon=True,
                             args=(phone, mgr.sid, mgr.meddy_id, _px,
                                   time.monotonic() + PULSE_KEEP_SECONDS),
                             name='pulse-ka-%s' % phone).start()
    except Exception as e:
        log_info('💓 保活pulse启动失败(静默) %s: %s' % (phone, str(e)[:80]))

    return res


def launch_account(phone, merchant_id, city='', province='', proxies=None):
    """单号**只做假启动**（新号注册成功后养号用，2026-11-24 用户定）。

    与 keepalive_account() 的区别：
      - 不做会话校验：注册流程已调过 /api/login/session_refresh（register_worker 4.5 步），
        这里只补「打开 APP」那一段，避免二次校验。
      - 入参极简：只要 phone + merchant_id；city/province/d/token/sid 全部从 Redis 凭据读。

    返回 {'phone', 'merchant_id', 'city', 'province', 'ok', 'total', 'detail', 'elapsed'}
    任一动作失败只记录、不抛异常 —— 养号增强不该阻断注册。
    """
    from login_manager import LoginManager

    cred = {}
    try:
        raw = _r().hget(CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id), phone)
        if raw:
            cred = json.loads(raw) or {}
    except Exception:
        pass
    city = (city or str(cred.get('city') or '')).strip()
    province = (province or str(cred.get('province') or '')).strip()
    # 注册任务常下发「省有市空」（如 province=广东、city=''）。city 空会导致
    # lookup_city/lookup_store 查不到 → 主页/会员/设备三个动作被跳过（只剩拉配置+通知=2/2）。
    # 用省名兜底出一个代表城市，保证新号第一次假启动也能跑全 5 个动作。
    if not city and province:
        city = _city_by_province(province) or city

    mgr = LoginManager(phone, None, True, city or None, None, None, None,
                       merchant_id, province or None, None)
    token, sid, _meddy = mgr.load_credentials()
    d = str(cred.get('d') or '') or (mgr.d or '')
    if not (token and sid):
        return {'phone': phone, 'merchant_id': merchant_id, 'city': city,
                'province': province, 'ok': 0, 'total': 0,
                'detail': '缺少 token 或 sid（凭据未写入）', 'elapsed': 0}
    try:
        mgr.apply_location(province or None, city or None)
    except Exception:
        pass

    # 代理最多试 2 个：第一个请求撞代理层故障就换代理整段重跑
    launch = {'ok': 0, 'total': 0, 'elapsed': 0, 'detail': ''}
    for attempt in range(2):
        try:
            if proxies is None:
                proxies = mgr.get_login_proxy(is_send_code=False,
                                             force_new=(attempt > 0), probe=False)
            ctx = lookup_city(city, token, sid, proxies)
            store = lookup_store(city)
            store_code = store[0] if store else ''
            be_code = store[1] if store else ''
            launch = fake_launch(
                token, sid, d=d, city=city, province=province,
                store_code=store_code, city_code=(ctx or {}).get('code', ''),
                lat=(ctx or {}).get('lat'), lng=(ctx or {}).get('lng'), proxies=proxies)
            if be_code:
                launch['be_code'] = be_code
            detail = launch.get('detail') or ''
            if attempt < 1 and launch.get('ok', 0) == 0 and any(
                    x in detail for x in ('代理连接失败', '请求超时', '网络连接失败',
                                          'ProxyError', 'ConnectionError')):
                proxies = None
                continue
            break
        except Exception as e:
            launch = {'ok': 0, 'total': 0, 'elapsed': 0,
                      'detail': '假启动异常: ' + str(e)[:60]}
            break
    launch['phone'] = phone
    launch['merchant_id'] = merchant_id
    launch['city'] = city
    launch['province'] = province
    return launch


# ========== 批量保活 ==========

def _prewarm_city_ctx(r, merchant_id=''):
    """用任意一个带 rt 的已登录号拉一次城市表，把上下文缓存住。

    目的：批量保活时每个号都查一次城市要 2~3 秒（代理往返）→ 整体慢一倍。
    预热一次后其余号都命中进程内缓存（24h TTL）。
    """
    if _CITY_CTX['map'] and (time.time() - _CITY_CTX['at']) < _CITY_CTX_TTL:
        return True
    pattern = CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id or '*')
    for key in r.scan_iter(match=pattern, count=50):
        mid = str(key).split(':')[2]
        if merchant_id and mid != str(merchant_id):
            continue
        try:
            creds = r.hgetall(key)
        except Exception:
            continue
        for phone, raw in creds.items():
            try:
                cred = json.loads(raw)
            except Exception:
                continue
            token, sid = str(cred.get('token') or ''), str(cred.get('sid') or '')
            if not (token and sid):
                continue
            try:
                from login_manager import LoginManager
                mgr = LoginManager(phone, None, True, cred.get('city') or None, None, None,
                                   None, mid, cred.get('province') or None, None)
                mgr.load_credentials()
                mgr.apply_location(cred.get('province') or None, cred.get('city') or None)
                px = mgr.get_login_proxy(is_send_code=False)
                ctx = get_city_context(token, sid, px)
                if ctx:
                    return True
            except Exception:
                continue
    return False


def run_keepalive(merchant_id='', limit=0, min_interval_hours=20, do_launch=True,
                  only_with_rt=False, redis_client=None, workers=0, only_phone='',
                  eligible=None):
    """批量保活。

    只做有 refreshToken 的号（用户 2026-11-24 定）：有 rt 才走真机那条 session/refresh 链路；
    没有 rt 的号 session/refresh 调不了，留给验活链路处理。

    ★ 号级并发（用户 2026-11-24 要求「整体就是较快的，完成每个号的保活请求」）：
      各号之间完全独立（不同号、不同凭据、不同代理），所以号级并发而不是号级串行。
      单号内部已经是「预热串行 + 其余并发」，号级再并发就是两层并发。
      保守取 workers=4：既能压掉串行等待，又不会把麦当劳/代理打太急。
      94 的 gunicorn --timeout 100：并发后总耗时 ≈ 最慢的那个号（而不是 N 个号之和），
      15 个号实测 17.2s → 并发后约 5~8s，完全不撞超时。

    结果写回 Redis mkt:prod:{商户}:keepalive:last（24h 过期），后台弹窗读它。
    """
    r = redis_client or _r()
    t_start = time.time()  # 结果写回 Redis 时要带上耗时，后台弹窗显示「用时 Xs」
    stats = {'total': 0, 'alive': 0, 'dead': 0, 'unknown': 0,
             'launch_ok': 0, 'launch_total': 0}
    details = []
    errors = []
    total_creds = 0
    with_rt = 0
    pattern = CREDENTIALS_KEY_TMPL.format(merchant_id='*')

    # 预热城市上下文（get_all_cities 一次 2~3 秒，批量时只拉一次，后面命中缓存）
    if do_launch:
        try:
            _prewarm_city_ctx(r, merchant_id)
        except Exception as e:
            errors.append('城市上下文预热失败: %s' % str(e)[:80])

    # ---- 1) 先把要保活的号收齐（读 Redis 很快，这步不占时间）----
    # eligible（2026-11-27）：平台入队时给的「可用账号」凭据 field 名单。
    # 非空时只保活名单里的号（跳过不可用/封禁/权益受限号）；None/空 = 老任务，照旧全扫。
    eligible_set = set(str(x) for x in eligible) if eligible else None
    targets = []
    for key in r.scan_iter(match=pattern, count=200):
        parts = str(key).split(':')
        if len(parts) < 3:
            continue
        mid = parts[2]
        if merchant_id and mid != str(merchant_id):
            continue
        try:
            creds = r.hgetall(key)
        except Exception as e:
            errors.append('读 %s 失败: %s' % (key, e))
            continue
        for phone, raw in creds.items():
            if only_phone and phone != only_phone:
                continue
            if eligible_set is not None and phone not in eligible_set:
                continue
            if limit and len(targets) >= limit:
                break
            try:
                cred = json.loads(raw)
            except Exception:
                continue
            total_creds += 1
            has_rt = bool(str(cred.get('refresh_token') or '').strip())
            if has_rt:
                with_rt += 1
            if only_with_rt and not has_rt:
                continue
            token = str(cred.get('token') or '')
            sid = str(cred.get('sid') or '')
            if not (token and sid):
                continue
            city = str(cred.get('city') or '')
            province = str(cred.get('province') or '')
            # 去重：min_interval_hours 内已保活过就跳过
            last = str(cred.get('keepalive_at') or '')
            if last and min_interval_hours > 0:
                try:
                    from datetime import datetime
                    if (datetime.now() - datetime.fromisoformat(last)).total_seconds() < min_interval_hours * 3600:
                        stats['skipped'] = stats.get('skipped', 0) + 1
                        continue
                except Exception:
                    pass
            targets.append((key, mid, phone, city, province, cred))
        if limit and len(targets) >= limit:
            break

    # 没挑到号时给后台一个明确说明，否则弹窗只显示「共 0 个 / 成功 0 / 失败 0」像坏了。
    if not targets:
        msg = ('本次没有可保活的账号：该商户凭据 %d 个，带 refreshToken 的 %d 个'
               % (total_creds, with_rt))
        if only_with_rt:
            msg += '；保活只做带 refreshToken 的号（无 RT 无法走 session/refresh）'
        errors.append(msg)
        details.append({
            'phone': '—', 'state': 'unknown', 'msg': msg[:100],
            'refresh_kind': '', 'launch_ok': 0, 'launch_total': 0,
            'launch_detail': '',
        })

    # ---- 2) 号级并发保活 ----
    def _one(t):
        key, mid, phone, city, province, cred = t
        try:
            res = keepalive_account(phone, mid, city=city, province=province)
        except Exception as e:
            res = {'phone': phone, 'ok': False, 'state': 'unknown',
                   'msg': '%s: %s' % (type(e).__name__, str(e)[:80]), 'launch': None}
        # 回写凭据的保活标记
        try:
            from datetime import datetime
            cred['keepalive_at'] = datetime.now().isoformat()
            cred['keepalive_ok'] = bool(res.get('ok'))
            cred['keepalive_state'] = res.get('state')
            r.hset(key, phone, json.dumps(cred, ensure_ascii=False))
        except Exception:
            pass
        # sid 明确失效（dead）→ 回写平台标「不可用」，下次入队不再派它。
        # unknown（代理/网络抖动）不回写，避免误杀好号。verify-result 幂等，重复跑安全。
        if res.get('state') == 'dead':
            dok, dnote = report_dead_to_platform(phone, mid, res.get('msg', ''), r)
            res['dead_reported'] = dok
            if not dok:
                log_info('⚠ 保活 dead 回写平台失败 %s: %s' % (phone, dnote))
        return res

    n_workers = workers if workers and workers > 0 else KEEPALIVE_WORKERS
    results = []
    if n_workers > 1 and len(targets) > 1:
        with ThreadPoolExecutor(max_workers=min(n_workers, len(targets))) as ex:
            for res in ex.map(_one, targets):
                results.append(res)
    else:
        for t in targets:
            results.append(_one(t))

    # ---- 3) 汇总 ----
    for res in results:
        st = res.get('state')
        stats['total'] += 1
        if st == 'alive':
            stats['alive'] += 1
        elif st == 'dead':
            stats['dead'] += 1
        else:
            stats['unknown'] += 1
        lz = res.get('launch') or {}
        if lz:
            stats['launch_ok'] += lz.get('ok', 0)
            stats['launch_total'] += lz.get('total', 0)
        if len(details) < 200:
            details.append({
                'phone': res.get('phone', ''), 'state': st, 'msg': res.get('msg', '')[:100],
                'refresh_kind': res.get('refresh_kind', ''),
                'launch_ok': lz.get('ok', 0), 'launch_total': lz.get('total', 0),
                'launch_detail': lz.get('detail', '')[:300],
                'dead_reported': bool(res.get('dead_reported')),
            })

    payload = {
        'ran_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'merchant_id': merchant_id,
        'stats': stats,
        'details': details,
        'errors': errors[:10],
    }
    # ⚠️ 必须在写 Redis 之前算好：mcd_server.py 是在 run_keepalive() 返回后才补
    # payload['elapsed_ms']，那份只进 HTTP 响应；后台读的是 Redis 里的这一份。
    # 之前漏了这段，所以弹窗「用时」一直是 "-"。
    payload['elapsed_ms'] = int((time.time() - t_start) * 1000)
    try:
        r.set(KEEPALIVE_LAST_KEY_TMPL.format(merchant_id=merchant_id or '*'),
              json.dumps(payload, ensure_ascii=False), ex=KEEPALIVE_LAST_TTL)
    except Exception as e:
        payload['write_error'] = str(e)
    return payload


def read_last_result(merchant_id, redis_client=None):
    """读上一次保活结果（后台弹窗用）。"""
    r = redis_client or _r()
    try:
        raw = r.get(KEEPALIVE_LAST_KEY_TMPL.format(merchant_id=merchant_id or '*'))
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

# -*- coding: utf-8 -*-
"""账号保活（keepalive）：续期 SID + 模拟启动（打开 APP）。

2026-11-24 新增。背景：
  1) 麦当劳方说明「会话每天要用 refresh 激活一次，App 后台进程会自动发；不做就掉登录」。
  2) 11-23 真机抓包（cap_20261123_164049_70451）显示 App 启动时会并发打一批接口，
     其中 /bff/portal/home/index（主页）真机调了两次 —— 单纯续期不够，
     要让风控看到「这个号今天真的打开过 App」。

本模块提供两件事：
  - keepalive_account()：对一个账号做「续期 + 假启动」，返回结论明细
  - run_keepalive()：批量跑（按商户/限额），把结果写回 Redis（供后台展示）

★ 与订单客户端 warmup 的区别：
  - 订单客户端的下单 warmup 在 mcd_order_client.py（客户端侧，走 HTTP 调 mcd_server）
  - 本模块在 mcd_server 进程内直接调 mcd_api（少一跳 HTTP，且能拿到 sid/rt 原始值）

★ 耗时控制（重要）：
  真机启动那 15 秒几乎全是人的停顿，接口本身只要 1~8ms；单请求走代理 1.4~3 秒。
  所以：预热（拉配置）串行 1 个把代理绑住，其余并发，整段 ≤ 2~3 秒（用户 2026-11-24 要求）。
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout

import mcd_api
from config import make_redis, CREDENTIALS_KEY_TMPL

# ========== 可调参数（用户 2026-11-24 定：并发最多 2-3 秒） ==========
WARMUP_PREHEAT_TIMEOUT = 3.0     # 预热（拉配置）单请求最多等这么久
WARMUP_CONCURRENT_TIMEOUT = 2.5  # 并发段每个动作最多等这么久
WARMUP_HARD_CAP = 6.0            # 整段硬上限（兜底，防止线程卡死拖垮批处理）
WARMUP_SLEEP = 0.2               # 每个号之间的间隔，别把麦当劳打太急

# 友盟设备级 token（与下单客户端 UMENG_DEVICE_TOKEN 同值；设备级、非账号级）
UMENG_DEVICE_TOKEN = 'AmbPB-Ggfah3sGoPm8Eb3F7sj40My522jpZEuI6VjoGt'

# 模拟启动要发的动作（按真机 11-23 抓包 16:42:01~16:42:10 的启动段）
# 顺序不重要（并发发），但「拉配置」单独预热、其余并发。
#   拉配置   /bff/common/normal/config       真机 16:42:01.505（第一个业务请求）
#   主页     /bff/portal/home/index          真机 16:42:09.404 + 09.483（调了两次）
#   会员     /bff/member/user/portal/v2      真机 16:42:09.962
#   通知上报 /bff/mc/remind/login/submit     真机 16:42:06.797
#   设备上报 /bff/member/device/collect      真机 16:42:10.870
#   附近门店 /bff/store/stores/vicinity      真机 16:42:08.982
BOOT_ACTIONS = ('拉配置', '主页', '会员', '通知', '设备', '附近门店')

# 结果在 Redis 里的 key（24 小时过期，后台「保活设置」弹窗读它展示上次结果）
KEEPALIVE_LAST_KEY_TMPL = 'mkt:prod:{merchant_id}:keepalive:last'
KEEPALIVE_LAST_TTL = 86400  # 24 小时

# 城市上下文缓存（city → code/lat/lng），get_all_cities 拉一次，缓存 24h
_CITY_CTX = {'at': 0.0, 'map': {}}
_CITY_CTX_TTL = 86400

_redis = None


def _r():
    global _redis
    if _redis is None:
        _redis = make_redis()
    return _redis


# ========== 城市上下文 ==========

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
    """按城市名取 {code, lat, lng}；取不到返回 None（该号就跳过需要城市参数的请求）。"""
    name = (city_name or '').strip()
    if not name:
        return None
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


# ========== 单号保活 ==========

def keepalive_account(phone, merchant_id, city='', province='', proxies=None):
    """对一个账号做「续期 + 假启动」。

    续期优先用 session/refresh + refreshToken（真机链路，2026-11-24 用户定）；
    没有 rt 的号回落到空体 login/refresh（闸门校验，能续期但不是真机那条路）。

    返回 dict：
      {phone, ok, state, msg, launch: {...}}
      state ∈ alive / dead / unknown
        alive   = 续期成功
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
    try:
        if proxies is None:
            proxies = mgr.get_login_proxy(is_send_code=False)
    except Exception as e:
        proxies = None
        proxy_err = str(e)[:60]
    else:
        proxy_err = ''

    # ---- 1) 续期 ----
    if rt:
        ok, data, msg = mcd_api.login_session_refresh(token, sid, rt, d=d, proxies=proxies)
        if ok and isinstance(data, dict):
            # 上游可能返回新 sid/rt（真机样本是不轮换，但要真拿到才写回）
            new_sid = str(data.get('sid') or '').strip()
            new_rt = str(data.get('refreshToken') or '').strip()
            if new_sid and new_sid != sid:
                try:
                    mgr.sid = new_sid
                    mgr.save_credentials(token, new_sid, meddy, refresh_token=new_rt or rt)
                except Exception:
                    pass
        refresh_kind = 'session/refresh'
    else:
        ok, msg = mcd_api.login_refresh(token, sid, d=d, proxies=proxies)
        refresh_kind = 'login/refresh'

    low = str(msg or '').lower()
    dead = (not ok) and any(x in low for x in ['401', '登录失效', '其他设备登录', '重新登录'])
    unknown = (not ok) and not dead

    state = 'alive' if ok else ('dead' if dead else 'unknown')
    res = {'phone': phone, 'ok': bool(ok), 'state': state, 'msg': str(msg)[:120],
           'refresh_kind': refresh_kind, 'has_rt': bool(rt), 'launch': None}
    if proxy_err:
        res['msg'] = (res['msg'] + ' | 代理: ' + proxy_err)[:160]

    if not ok:
        # 续期就失败了 → 假启动没意义（会话本身就不被接受），直接返回
        return res

    # ---- 2) 假启动（模拟打开 APP）----
    try:
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
        res['launch'] = launch
    except Exception as e:
        res['launch'] = {'ok': 0, 'total': 0, 'elapsed': 0, 'detail': '假启动异常: ' + str(e)[:60]}

    return res


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
                  only_with_rt=False, redis_client=None):
    """批量保活。

    只做有 refreshToken 的号（用户 2026-11-24 定）：有 rt 才走真机那条 session/refresh 链路；
    没有 rt 的号 session/refresh 调不了，留给验活链路处理。

    结果写回 Redis mkt:prod:{merchant}:keepalive:last（24h 过期），后台弹窗读它。
    """
    r = redis_client or _r()
    stats = {'total': 0, 'alive': 0, 'dead': 0, 'unknown': 0,
             'launch_ok': 0, 'launch_total': 0}
    details = []
    errors = []
    pattern = CREDENTIALS_KEY_TMPL.format(merchant_id='*')

    # 预热城市上下文（get_all_cities 一次 2~3 秒，批量时只拉一次，后面命中缓存）
    if do_launch:
        try:
            _prewarm_city_ctx(r, merchant_id)
        except Exception as e:
            errors.append('城市上下文预热失败: %s' % str(e)[:80])

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
            if limit and stats['total'] >= limit:
                break
            try:
                cred = json.loads(raw)
            except Exception:
                continue
            if only_with_rt and not str(cred.get('refresh_token') or '').strip():
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

            stats['total'] += 1
            try:
                res = keepalive_account(phone, mid, city=city, province=province)
            except Exception as e:
                res = {'phone': phone, 'ok': False, 'state': 'unknown',
                       'msg': '%s: %s' % (type(e).__name__, str(e)[:80]), 'launch': None}
            st = res.get('state')
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
                    'phone': phone, 'state': st, 'msg': res.get('msg', '')[:100],
                    'refresh_kind': res.get('refresh_kind', ''),
                    'launch_ok': lz.get('ok', 0), 'launch_total': lz.get('total', 0),
                    'launch_detail': lz.get('detail', '')[:300],
                })
            # 回写凭据的保活标记
            try:
                from datetime import datetime
                cred['keepalive_at'] = datetime.now().isoformat()
                cred['keepalive_ok'] = bool(res.get('ok'))
                cred['keepalive_state'] = st
                r.hset(key, phone, json.dumps(cred, ensure_ascii=False))
            except Exception:
                pass
            time.sleep(WARMUP_SLEEP)

    payload = {
        'ran_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'merchant_id': merchant_id,
        'stats': stats,
        'details': details,
        'errors': errors[:10],
    }
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

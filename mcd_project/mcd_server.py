#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重构后的 Flask API
通用接口: /api/mcd/<function_name>
登录接口: /api/login/*
"""
import re
import logging
import subprocess
import sys
import traceback

from flask import Flask, request, jsonify, g
import mcd_api
import json
import os
import redis
from datetime import datetime
import inspect
import time
import requests
from config import (CODE_DIR, LOG_DIR, REDIS_SET_URL, USE_PROXY, CAPTCHA_LOGIN_CONFIG, make_redis,
                    MERCHANT_ID_DEFAULT, PROXY_SOURCE_DEFAULT, CREDENTIALS_KEY_TMPL, GLOBAL_FIXED_IPS)
from login_manager import LoginManager, login_refresh
import utils

app = Flask(__name__)

# ========== 全局单例 ==========
# 全局单例实例（避免每次请求都重新初始化）
_failure_handler = None
_proxy_pool = None
_binding_manager = None

def get_proxy_pool():
    """获取全局 ProxyPool 单例"""
    global _proxy_pool
    if _proxy_pool is None:
        from proxy_pool import ProxyPool
        _proxy_pool = ProxyPool()
    return _proxy_pool

def get_binding_manager():
    """获取全局 AccountProxyBinding 单例"""
    global _binding_manager
    if _binding_manager is None:
        from account_proxy_binding import AccountProxyBinding
        # 传入 ProxyPool 单例，避免重复初始化
        _binding_manager = AccountProxyBinding(proxy_pool=get_proxy_pool())
    return _binding_manager

def get_failure_handler():
    """获取全局 ProxyFailureHandler 单例"""
    global _failure_handler
    if _failure_handler is None:
        from proxy_failure_handler import ProxyFailureHandler
        # 传入 ProxyPool 和 AccountProxyBinding 单例，避免重复初始化
        _failure_handler = ProxyFailureHandler(
            proxy_pool=get_proxy_pool(),
            binding_manager=get_binding_manager()
        )
    return _failure_handler

CONTROL_CHAR_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]')

class NoiseFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        # 1) 过滤协议层扫描产生的乱码/畸形请求
        if CONTROL_CHAR_RE.search(msg):
            return False
        if 'Bad request version' in msg or 'Invalid HTTP version' in msg:
            return False
        # 2) 过滤 /api/mcd/ 下未知接口产生的 404
        if record.args and len(record.args) >= 2:
            requestline = str(record.args[0])
            code = str(record.args[1])
            if '/api/mcd/' in requestline and code == '404':
                return False
        return True

# 配置日志输出（确保 print 和日志都能输出）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # 输出到标准输出（会被 crontab 重定向）
        # logging.FileHandler(os.path.join(LOG_DIR, 'mcd_server.log'), encoding='utf-8')  # 同时写入文件
    ]
)

# 配置 Flask 和 werkzeug 日志
app.logger.setLevel(logging.INFO)
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.addFilter(NoiseFilter())
werkzeug_logger.setLevel(logging.INFO)

# 使用 Redis 存储的 LoginManager
login_manager_cache = {}

def get_login_manager(phone, city=None, province=None, merchant_id=None, proxy_source=None):
    """获取或创建指定手机号的 LoginManager 实例（使用 Redis）"""
    merchant_id = merchant_id or MERCHANT_ID_DEFAULT
    proxy_source = proxy_source or PROXY_SOURCE_DEFAULT
    # 缓存 key 含 商户/城市/省份/代理源，避免不同参数共享实例(凭证 key 随商户不同)
    cache_key = f"{phone}:{merchant_id}:{city}:{province}:{proxy_source}"

    if cache_key not in login_manager_cache:
        # 传入全局单例，避免重复初始化
        manager = LoginManager(
            phone,
            use_redis=True,
            city=city,  # 传递城市参数
            merchant_id=merchant_id, province=province, proxy_source=proxy_source,
            proxy_pool=get_proxy_pool(),
            binding_manager=get_binding_manager(),
            failure_handler=get_failure_handler()
        )
        login_manager_cache[cache_key] = manager
    return login_manager_cache[cache_key]


def _biz_params(src):
    """从请求参数(dict/args)取 商户/省份/代理源 3 个新参数(缺省走 config 默认)。返回 (province, merchant_id, proxy_source)。"""
    get = src.get
    return (get('province'),
            get('merchant_id') or MERCHANT_ID_DEFAULT,
            get('proxy_source') or PROXY_SOURCE_DEFAULT)

# 配置
CREDENTIALS_FILE = 'data/credentials.json'

# ========== 验证码(滑块)登录队列/冻结 ==========
_cap_redis = make_redis()  # 带超时/重试/keepalive

# 启动时把代理源默认配置补进 Redis(mkt:prod:<默认商户>:proxy),只补缺失不覆盖已改的。
try:
    utils.seed_proxy_sources(MERCHANT_ID_DEFAULT)
except Exception as _e:
    print(f'⚠️  seed 代理源跳过: {_e}', flush=True)
# 注:自动加白名单放在 start_gunicorn() 启动前只跑一次(避免每个 gunicorn worker 都探测/加一遍)。


def _freeze_key(phone):
    return CAPTCHA_LOGIN_CONFIG['freezing_key'].format(phone=phone)


def _status_key(phone):
    return CAPTCHA_LOGIN_CONFIG['status_key'].format(phone=phone)


def is_frozen(phone):
    """账号是否处于验证码登录冻结期"""
    return _cap_redis.exists(_freeze_key(phone)) == 1


def enqueue_login(phone, city=None, force_new_device=False, province=None, merchant_id=None,
                  proxy_source=None, pinned_proxy=''):
    """冻结账号(兜底TTL) + 入队交给 captcha_login_worker + 置 queued 状态。
    province/merchant_id/proxy_source 一并入队,worker 发码时用同一商户(凭证 key 一致)、同一代理源/地域。"""
    _cap_redis.setex(_freeze_key(phone), CAPTCHA_LOGIN_CONFIG['freeze_ttl'], '1')
    # 入队即置处理心跳：防"刚入队还没被 worker 取走"时被业务接口误判僵尸解冻；worker 取到后会持续刷新
    _cap_redis.setex(_processing_key(phone), CAPTCHA_LOGIN_CONFIG['processing_ttl'], str(int(time.time())))
    _cap_redis.setex(_status_key(phone), CAPTCHA_LOGIN_CONFIG['status_ttl'],
                     json.dumps({'status': 'queued', 'msg': '已入队', 'ts': int(time.time())}, ensure_ascii=False))
    _cap_redis.rpush(CAPTCHA_LOGIN_CONFIG['queue_key'],
                     json.dumps({'phone': phone, 'city': city, 'force_new_device': force_new_device,
                                 'province': province, 'merchant_id': merchant_id or MERCHANT_ID_DEFAULT,
                                 'proxy_source': proxy_source or PROXY_SOURCE_DEFAULT,
                                 # pinned_proxy：现场造设备时钉住的出口（发码必须同 IP）
                                 'pinned_proxy': pinned_proxy or ''}, ensure_ascii=False))


def unfreeze_login(phone):
    _cap_redis.delete(_freeze_key(phone))


def _processing_key(phone):
    return CAPTCHA_LOGIN_CONFIG['processing_key'].format(phone=phone)


def worker_alive():
    """worker 进程是否在跑（全局心跳未过期）"""
    return _cap_redis.exists(CAPTCHA_LOGIN_CONFIG['worker_alive_key']) == 1


def processing_exists(phone):
    """该账号是否正在被 worker 处理/排队（按手机号的处理心跳未过期）"""
    return _cap_redis.exists(_processing_key(phone)) == 1


def _status_of(phone):
    raw = _cap_redis.get(_status_key(phone))
    if not raw:
        return None
    try:
        return json.loads(raw).get('status')
    except Exception:
        return None


# ========== 登录相关接口 ==========

@app.route('/api/login/send_code', methods=['POST'])
def login_send_code():
    """发送验证码"""
    data = request.json or {}
    phone = data.get('phone')
    use_new_device = data.get('use_new_device', False)
    # fresh_device（2026-11-17 注册场景）：现场用该城市代理造一台全新设备 ——
    # 造 d 的出口 IP 与发码出口 IP 完全对齐（设备池预造的 d 是批量+随机城市造的，
    # 注册判 706070「设备环境异常」）。发码入队时带 pinned_proxy，worker 用同一代理。
    fresh_device = bool(data.get('fresh_device', False))
    city = data.get('city')  # 新增：获取城市参数
    province, merchant_id, proxy_source = _biz_params(data)  # 省份/商户ID/代理源

    app.logger.info(f"[send_code] 请求参数: phone={phone}, use_new_device={use_new_device}, "
                    f"city={city}, province={province}, merchant_id={merchant_id}, proxy_source={proxy_source}")

    if not phone:
        error_msg = '缺少 phone 参数'
        app.logger.error(f"[send_code] ❌ 失败: {error_msg}")
        return jsonify({'success': False, 'message': error_msg}), 400

    # 方案①：服务器不直接发码，统一入队交给 captcha_login_worker（发码+过滑块+重发），客户端轮询 /api/login/captcha_status
    if not worker_alive():
        app.logger.error(f"[send_code] {phone} worker 未运行，拒绝入队")
        return jsonify({'success': False, 'message': '登录服务(captcha worker)未运行，请先启动 captcha_login_worker 再试'}), 503

    # 只有"真正在处理中"(有处理心跳 且 状态是排队/发码中)才不重复入队；
    # 其余情况(上次 code_sent 已发完待验证 / failed / 僵尸冻结 / 全新)都视为用户想重发 → 清旧状态并重新入队
    status = _status_of(phone)
    if is_frozen(phone) and processing_exists(phone) and status in ('queued', 'sending'):
        app.logger.info(f"[send_code] {phone} 正在处理中(status={status})，返回 queued")
        return jsonify({'success': True, 'data': {'status': status}, 'message': '该账号正在登录处理中，请轮询状态'})

    # 确保账号已绑定数美设备指纹 d：新账号自动从设备池领一个;已有但无 d 的也自动补领并写入凭证。
    # use_new_device=True 时换新设备(旧设备标记失效)。失败不阻断发码(worker 侧仍会用现有 d)。
    eff_province, eff_city = province, city
    pinned_proxy = ''
    if fresh_device:
        # 现场造设备：用该城市代理调数美 deviceprofile/v4 拿新 d，绑定到本号，
        # 并把该代理钉给 worker（发码必须同 IP，否则 IP 对齐白做）
        try:
            from local_device_gen import register_local
            _lm = get_login_manager(phone, city, province, merchant_id, proxy_source)
            eff_province, eff_city = _lm.apply_location(province, city)
            _pxy = _lm.get_login_proxy(is_send_code=True)
            _dev = register_local(use_proxy=False, proxies=_pxy, timeout=25)
            if not _dev:
                return jsonify({'success': False,
                                'message': '现场造设备失败（数美拒绝或代理不可用），请重试'}), 502
            _lm.d = _dev['d']
            _lm.token = _dev.get('token') or _lm.token
            _lm.device_id = str(_dev.get('smid') or '')[:12]
            try:
                from mcd_api import set_active_d
                set_active_d(_lm.d)
            except Exception:
                pass
            _lm._persist_device(_lm.d, _lm.device_id)
            _px = (_pxy or {}).get('http') or (_pxy or {}).get('https') or ''
            pinned_proxy = _px if _px.startswith('http') else (('http://' + _px) if _px else '')
            app.logger.info(f"[send_code] {phone} 现场造设备 新d={_lm.d[:16]}… model={_dev.get('model')} "
                            f"proxy={_px}（发码将钉同一出口）")
        except Exception as e:
            app.logger.error(f"[send_code] {phone} 现场造设备异常: {e}")
            return jsonify({'success': False, 'message': f'现场造设备异常: {e}'}), 502
    else:
        try:
            _lm = get_login_manager(phone, city, province, merchant_id, proxy_source)
            # 定本次生效省/市(没传取凭证已绑定;账号未绑定则绑定写入)
            eff_province, eff_city = _lm.apply_location(province, city)
            _d = _lm.ensure_device(force_new=use_new_device)
            app.logger.info(f"[send_code] {phone} 省市 province={eff_province} city={eff_city} "
                            f"设备绑定 d={'有' if _d else '无(池空?)'} device_id={_lm.device_id}")
        except Exception as e:
            app.logger.warning(f"[send_code] {phone} 设备/省市绑定失败(继续入队): {e}")

    _cap_redis.delete(_status_key(phone))  # 清掉上次残留的 code_sent，避免轮询误判"已发送"而实际没重发
    # 入队用【生效】省市,worker 发码按同一省市取代理
    enqueue_login(phone, city=eff_city, force_new_device=use_new_device,
                  province=eff_province, merchant_id=merchant_id, proxy_source=proxy_source,
                  pinned_proxy=pinned_proxy)
    app.logger.info(f"[send_code] {phone} 已(重新)入队验证码登录 (prev status={status})")
    return jsonify({'success': True, 'data': {'status': 'queued'}, 'message': '已入队，请轮询 /api/login/captcha_status'})

@app.route('/api/login/verify', methods=['POST'])
def login_verify():
    """验证登录"""
    data = request.json or {}
    phone = data.get('phone')
    verify_code = data.get('verify_code')
    city = data.get('city')  # 新增：获取城市参数
    province, merchant_id, proxy_source = _biz_params(data)

    app.logger.info(f"[login_verify] 请求参数: phone={phone}, verify_code={verify_code}, city={city}, "
                    f"province={province}, merchant_id={merchant_id}, proxy_source={proxy_source}")

    if not phone or not verify_code:
        error_msg = '缺少 phone 或 verify_code 参数'
        app.logger.error(f"[login_verify] ❌ 参数错误: {error_msg}, 收到的数据: {data}")
        return jsonify({'success': False, 'message': error_msg}), 400

    login_manager = get_login_manager(phone, city, province, merchant_id, proxy_source)
    login_manager.load_credentials()
    login_manager.apply_location(province, city)  # 定生效省/市 + 未绑定则绑定
    login_manager.ensure_device()  # 确保有设备指纹 d(没有则从设备池补领),否则登录会被判设备异常
    # 调用登录
    success, message = login_manager.do_login(verify_code)

    if success:
        app.logger.info(f"[login_verify] ✓ 登录成功: phone={phone}")
        login_manager.record_check('valid')  # 登录成功即记一次检测时间+状态
        # 登录成功：解冻 + 清理验证码状态
        unfreeze_login(phone)
        _cap_redis.delete(_status_key(phone))
        return jsonify({
            'success': True,
            'data': {
                'token': login_manager.token,
                'sid': login_manager.sid,
                'meddy_id': login_manager.meddy_id,
                # refreshToken：平台/注册脚本要能拿到并持久化（掉登录时用它续期，免重新接码）
                'refresh_token': getattr(login_manager, 'refresh_token', '') or ''
            },
            'message': message
        })
    else:
        login_manager.record_check('invalid')  # 登录失败也记一次检测时间+状态(invalid)
        app.logger.error(f"[login_verify] ❌ 登录失败: phone={phone}, 原因: {message}")
        return jsonify({'success': False, 'message': message}), 400

@app.route('/api/login/captcha_status', methods=['GET'])
def login_captcha_status():
    """查询验证码(滑块)登录处理状态：queued/sending/code_sent/failed/none"""
    phone = request.args.get('phone')
    if not phone:
        return jsonify({'success': False, 'message': '缺少 phone 参数'}), 400
    raw = _cap_redis.get(_status_key(phone))
    if not raw:
        return jsonify({'success': True, 'data': {'status': 'none'}, 'message': '无状态(未入队或已过期)'})
    return jsonify({'success': True, 'data': json.loads(raw)})


@app.route('/api/maintenance/keepalive', methods=['POST'])
def maintenance_keepalive():
    """账号保活：续期 SID + 模拟启动（打开 APP）。

    2026-11-24 重写。原来只调空体 /bff/passport/login/refresh 做续期；
    现在改成：
      1) 续期优先走 **session/refresh + refreshToken**（真机链路，11-23 抓包实测真机在用这条）
      2) 续期成功后按该号属地做一次「模拟启动」：拉配置/主页/会员/通知/设备上报/附近门店，
         并发发，整段 ≤ 2~3 秒（真机启动日志 16:42:01~16:42:10 的启动段）
      3) 只处理有 refreshToken 的号（没 rt 的号 session/refresh 调不了）
    结果写 Redis mkt:prod:{商户}:keepalive:last（24h 过期），后台「保活设置」弹窗读它。

    入参（均可选）：merchant_id / limit(0=不限) / min_interval_hours(默认20) /
                   do_launch(默认 true，是否做模拟启动) / only_with_rt(默认 true)
    返回：stats + details
    """
    import keepalive as ka
    body = request.get_json(silent=True) or {}
    only_merchant = str(body.get('merchant_id') or '').strip()
    only_phone = str(body.get('only_phone') or '').strip()
    try:
        limit = int(body.get('limit') or 0)
    except Exception:
        limit = 0
    # ⚠️ 不能用 `body.get(...) or 20`：显式传 0（=不去重，本次全跑）会被当成「没传」变成 20，
    #    导致「立即执行一次」看起来跑了、实际把号全 skip 掉（2026-11-24 实测踩到）。
    if body.get('min_interval_hours') is None:
        min_hours = 20.0
    else:
        try:
            min_hours = float(body.get('min_interval_hours'))
        except Exception:
            min_hours = 20.0
    do_launch = body.get('do_launch')
    do_launch = True if do_launch is None else bool(do_launch)
    only_with_rt = body.get('only_with_rt')
    only_with_rt = True if only_with_rt is None else bool(only_with_rt)

    t0 = time.time()
    try:
        payload = ka.run_keepalive(merchant_id=only_merchant, limit=limit, only_phone=only_phone,
                                   min_interval_hours=min_hours, do_launch=do_launch,
                                   only_with_rt=only_with_rt)
    except Exception as e:
        app.logger.error('[keepalive] 执行失败: %s' % e)
        return jsonify({'success': False, 'message': '保活执行失败: %s' % e}), 500

    st = payload.get('stats') or {}
    app.logger.info(
        '[keepalive] 完成 total=%d alive=%d dead=%d unknown=%d skipped=%d '
        '模拟启动 %d/%d 用时 %dms'
        % (st.get('total', 0), st.get('alive', 0), st.get('dead', 0), st.get('unknown', 0),
           st.get('skipped', 0), st.get('launch_ok', 0), st.get('launch_total', 0),
           int((time.time() - t0) * 1000)))
    payload['elapsed_ms'] = int((time.time() - t0) * 1000)
    return jsonify({'success': True, 'data': payload})


@app.route('/api/maintenance/keepalive/last', methods=['GET'])
def maintenance_keepalive_last():
    """读上一次保活结果（后台弹窗展示用）。

    GET /api/maintenance/keepalive/last?merchant_id=100014
    """
    import keepalive as ka
    mid = str(request.args.get('merchant_id') or '').strip()
    data = ka.read_last_result(mid)
    if not data:
        return jsonify({'success': True, 'data': None, 'message': '暂无保活记录'})
    return jsonify({'success': True, 'data': data})


@app.route('/api/maintenance/launch', methods=['POST'])
def maintenance_launch():
    """单号假启动（新号注册成功后养号，2026-11-24 用户定）。

    入参极简：{"phone":"17375788810","merchant_id":"100014"}
    city/province 可选，不传则从 Redis 凭据读；token/sid/d/rt 全部服务端读凭据。
    只做「打开 APP」5 个动作（拉配置/主页/会员/通知/设备），不做续期
    —— 注册流程已调过 session/refresh，避免二次刷新。

    单号测试：curl -X POST http://127.0.0.1:5001/api/maintenance/launch \
      -H 'Content-Type: application/json' -d '{"phone":"17375788810","merchant_id":"100014"}'
    """
    import keepalive as ka
    body = request.get_json(silent=True) or {}
    phone = str(body.get('phone') or '').strip()
    merchant_id = str(body.get('merchant_id') or '').strip()
    city = str(body.get('city') or '').strip()
    province = str(body.get('province') or '').strip()
    if not phone or not merchant_id:
        return jsonify({'success': False, 'message': '缺少 phone 或 merchant_id'}), 400
    try:
        res = ka.launch_account(phone, merchant_id, city=city, province=province)
    except Exception as e:
        app.logger.error('[launch] 执行失败 phone=%s: %s' % (phone, e))
        return jsonify({'success': False, 'message': '假启动失败: %s' % e}), 500
    app.logger.info('[launch] phone=%s ok=%s/%s 用时 %ss' % (
        phone, res.get('ok'), res.get('total'), res.get('elapsed')))
    return jsonify({'success': True, 'data': res})


@app.route('/api/login/status', methods=['GET'])
def login_status():
    """检测登录状态"""
    phone = request.args.get('phone')
    city = request.args.get('city')  # 带上 city：首次会据此分配指定城市代理并建绑定
    province, merchant_id, proxy_source = _biz_params(request.args)

    app.logger.info(f"[login_status] 请求参数: phone={phone}, city={city}, "
                    f"province={province}, merchant_id={merchant_id}, proxy_source={proxy_source}")

    if not phone:
        error_msg = '缺少 phone 参数'
        app.logger.error(f"[login_status] ❌ 参数错误: {error_msg}")
        return jsonify({'success': False, 'message': error_msg}), 400

    login_manager = get_login_manager(phone, city, province, merchant_id, proxy_source)

    # 加载凭证
    token, sid, meddy_id = login_manager.load_credentials()
    login_manager.apply_location(province, city)  # 定生效省/市 + 未绑定则绑定
    if sid and not token:
        token = login_manager.ensure_token()  # 只有 sid 无 token → 自动生成+激活+绑定

    if not (token and sid):
        app.logger.info(f"[login_status] 未找到登录凭证: phone={phone}")
        return jsonify({
            'success': True,
            'data': {'is_logged_in': False},
            'message': '未登录'
        })

    # 检查登录状态
    try:
        is_valid, user_info = login_manager.check_login_status()
        # 记录本次检测时间+结果到凭证(有sid也可能掉线)
        login_manager.record_check('valid' if is_valid else 'invalid')

        if is_valid:
            app.logger.info(f"[login_status] ✓ 已登录: phone={phone}")
            return jsonify({
                'success': True,
                'data': {
                    'is_logged_in': True,
                    'token': login_manager.token,
                    'sid': login_manager.sid,
                    'meddy_id': login_manager.meddy_id,
                    'user_info': user_info,
                    'checked_at': login_manager.checked_at,
                    'check_status': login_manager.check_status
                },
                'message': '已登录'
            })
        else:
            app.logger.info(f"[login_status] 登录凭证已失效: phone={phone}")
            return jsonify({
                'success': True,
                'data': {'is_logged_in': False,
                         'checked_at': login_manager.checked_at,
                         'check_status': login_manager.check_status},
                'message': '登录已失效'
            })
    except Exception as e:
        # 检测异常(网络等) → 记 error(不确定在线/掉线),不误判
        login_manager.record_check('error')
        app.logger.error(f"[login_status] ❌ 检查登录状态失败: phone={phone}, 错误: {str(e)}")
        return jsonify({
            'success': True,
            'data': {'is_logged_in': False,
                     'checked_at': login_manager.checked_at,
                     'check_status': login_manager.check_status},
            'message': f'检查登录状态失败: {str(e)}'
        })


@app.route('/api/login/session_refresh', methods=['GET', 'POST'])
def login_session_refresh():
    """按账号标识读取刷新凭证，刷新成功后自动保存会话。"""
    data = request.args.to_dict() if request.method == 'GET' else request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({'success': False, 'data': {}, 'message': '请求体必须是 JSON 对象'}), 400
    phone = data.get('phone')
    if not isinstance(phone, str) or not phone.strip():
        return jsonify({'success': False, 'data': {}, 'message': 'phone 必须是非空字符串账号标识'}), 400
    if is_frozen(phone):
        return jsonify({'success': False, 'data': {}, 'message': '账号正在登录处理中，请稍后刷新'}), 423
    province, merchant_id, proxy_source = _biz_params(data)
    try:
        manager = get_login_manager(phone, data.get('city'), province, merchant_id, proxy_source)
        success, result, message = manager.refresh_session(province=province, city=data.get('city'))
        return jsonify({'success': success, 'data': result, 'message': message})
    except Exception:
        app.logger.error('[session_refresh] 会话刷新或凭证保存异常')
        return jsonify({'success': False, 'data': {}, 'message': '会话刷新或凭证保存异常，请检查服务端存储及代理配置'}), 500


@app.route('/api/login/bind_city', methods=['POST'])
def login_bind_city():
    """绑定/修改账号省市(显式覆盖凭证里的 province/city)。
    body: {phone, province, city, merchant_id?}。已绑定账号换城市须调本接口。"""
    data = request.json or {}
    phone = data.get('phone')
    province = (data.get('province') or '').strip()
    city = (data.get('city') or '').strip()
    _, merchant_id, proxy_source = _biz_params(data)

    if not phone or not (province or city):
        return jsonify({'success': False, 'message': '缺少 phone 或 province/city 参数'}), 400

    app.logger.info(f"[bind_city] phone={phone} province={province} city={city} merchant_id={merchant_id}")
    try:
        manager = get_login_manager(phone, city, province, merchant_id, proxy_source)
        p, c = manager.set_location(province, city)
        return jsonify({'success': True, 'data': {'phone': phone, 'province': p, 'city': c},
                        'message': '省市已绑定/修改'})
    except Exception as e:
        app.logger.error(f"[bind_city] ❌ {phone} 失败: {e}")
        return jsonify({'success': False, 'message': f'绑定省市失败: {str(e)}'}), 500


@app.route('/api/accounts', methods=['GET'])
def accounts_query():
    """
    查询账号凭证数据。?phone= 查单个;不传查全部。?merchant_id= 可选(默认100001)。
    返回 list,每项 = 凭证 value(含 token/sid/d/province/city/checked_at/check_status/saved_at…) + phone。
    """
    phone = request.args.get('phone')
    merchant_id = request.args.get('merchant_id') or MERCHANT_ID_DEFAULT
    key = CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id)
    out = []
    try:
        if phone:
            raw = _cap_redis.hget(key, phone)
            if raw:
                try:
                    d = json.loads(raw)
                except Exception:
                    d = {'_raw': raw}
                d['phone'] = phone
                out.append(d)
        else:
            for ph, raw in (_cap_redis.hgetall(key) or {}).items():
                try:
                    d = json.loads(raw)
                except Exception:
                    continue
                if not d.get('sid'):   # 不传手机号时,只返回有 sid(已登录)的账号
                    continue
                d['phone'] = ph
                out.append(d)
    except Exception as e:
        return jsonify({'success': False, 'message': f'查询账号失败: {str(e)}'}), 500
    return jsonify({'success': True, 'data': out, 'total': len(out)})


def _mcgold_status_key(phone):
    return CAPTCHA_LOGIN_CONFIG['mcgold_status_key'].format(phone=phone)


def enqueue_mcgold(phone, sku_id, shop_id=1, city_code='310100', city=None,
                   province=None, merchant_id=None, proxy_source=None):
    """麦金卡开通入队(与登录同一队列,type=mcgold,同一 worker 串行处理) + 置 queued 状态 + 处理心跳。"""
    _cap_redis.setex(_processing_key(phone), CAPTCHA_LOGIN_CONFIG['processing_ttl'], str(int(time.time())))
    _cap_redis.setex(_mcgold_status_key(phone), CAPTCHA_LOGIN_CONFIG['mcgold_status_ttl'],
                     json.dumps({'status': 'queued', 'msg': '已入队', 'ts': int(time.time())}, ensure_ascii=False))
    _cap_redis.rpush(CAPTCHA_LOGIN_CONFIG['queue_key'], json.dumps({
        'type': 'mcgold', 'phone': phone, 'sku_id': sku_id, 'shop_id': shop_id, 'city_code': city_code,
        'city': city, 'province': province,
        'merchant_id': merchant_id or MERCHANT_ID_DEFAULT, 'proxy_source': proxy_source or PROXY_SOURCE_DEFAULT,
    }, ensure_ascii=False))


@app.route('/api/member/open', methods=['POST'])
def member_open_order():
    """
    开通麦金卡 / 电商下单(【异步】,自动过滑块) —— 入队交给 captcha_login_worker(与发码同一 worker)。
    body: {phone, sku_id, shop_id?=1, city_code?=310100, city?/province?/merchant_id?/proxy_source?}
    返回 queued；之后轮询 GET /api/member/open_status(done 时含 orderId/payId),再走支付拿支付宝数据。
    """
    data = request.json or {}
    phone = data.get('phone')
    sku_id = data.get('sku_id')
    city = data.get('city')
    province, merchant_id, proxy_source = _biz_params(data)
    shop_id = data.get('shop_id', 1)
    city_code = data.get('city_code', '310100')

    if not phone or not sku_id:
        return jsonify({'success': False, 'message': '缺少 phone 或 sku_id 参数'}), 400
    if not worker_alive():
        return jsonify({'success': False, 'message': '开通服务(captcha worker)未运行，请先启动 captcha_login_worker 再试'}), 503

    # 确保有设备 d(与发码链路一致);失败不阻断入队
    try:
        _lm = get_login_manager(phone, city, province, merchant_id, proxy_source)
        _lm.apply_location(province, city)
        _lm.ensure_device()
    except Exception as e:
        app.logger.warning(f"[member_open] {phone} 设备绑定失败(继续入队): {e}")

    enqueue_mcgold(phone, sku_id, shop_id=shop_id, city_code=city_code, city=city,
                   province=province, merchant_id=merchant_id, proxy_source=proxy_source)
    app.logger.info(f"[member_open] {phone} sku_id={sku_id} 已入队，轮询 /api/member/open_status")
    return jsonify({'success': True, 'data': {'status': 'queued'}, 'message': '已入队，请轮询 /api/member/open_status'})


@app.route('/api/member/open_status', methods=['GET'])
def member_open_status():
    """查询麦金卡开通状态：queued/processing/done(含 orderId/payId)/failed/none。"""
    phone = request.args.get('phone')
    if not phone:
        return jsonify({'success': False, 'message': '缺少 phone 参数'}), 400
    raw = _cap_redis.get(_mcgold_status_key(phone))
    if not raw:
        return jsonify({'success': True, 'data': {'status': 'none'}, 'message': '无状态(未入队或已过期)'})
    return jsonify({'success': True, 'data': json.loads(raw)})


# ========== 通用 mcd_api 接口 ==========

# ---------- 提单 420 过滑块（2026-11-16）----------
# 架构：mcd_server(94) 收到提单 420 → 返回 securityContext + 本次代理(X-Proxy-Used)
#  → 客户端调 /api/captcha/submit_solve 派任务（win 机器 worker 过滑块，必须同出口 IP）
#  → 客户端轮询 /api/captcha/submit_status 拿 PASS rid
#  → 客户端拼 sec_verify + pin_proxy(同一代理) 重提 submit_order

@app.route('/api/captcha/submit_solve', methods=['POST'])
def api_captcha_submit_solve():
    """派发提单滑块任务到 win 机器 worker（mcd:captcha:login_queue, type=submit）。

    body: {phone, order_id, punish_id, verify_sdk, proxy}
    立即返回，不阻塞（过滑块要 10~60s，客户端轮询 submit_status）。
    """
    p = request.get_json(silent=True) or {}
    phone = str(p.get('phone') or '').strip()
    punish_id = str(p.get('punish_id') or '').strip()
    if not phone or not punish_id:
        return jsonify({'success': False, 'message': '缺少 phone / punish_id'}), 400
    payload = {
        'type': 'submit', 'phone': phone,
        'punish_id': punish_id,
        'verify_sdk': p.get('verify_sdk') or 1,
        'order_id': p.get('order_id') or '',
        'proxy': str(p.get('proxy') or '').strip(),
        'merchant_id': p.get('merchant_id') or '',
    }
    try:
        rds = make_redis()
        rds.lpush(CAPTCHA_LOGIN_CONFIG['queue_key'], json.dumps(payload, ensure_ascii=False))
        st_key = f'mcd:captcha:submit_status:{phone}'
        rds.delete(st_key)   # 清旧状态，避免客户端轮询拿到上一次的结果
        rds.setex(f'mcd:captcha:submit_pending:{phone}', 300, punish_id)
    except Exception as e:
        return jsonify({'success': False, 'message': f'派发失败: {e}'}), 503
    return jsonify({'success': True, 'message': '滑块任务已派发', 'status_key': f'mcd:captcha:submit_status:{phone}'})


@app.route('/api/captcha/submit_status', methods=['GET'])
def api_captcha_submit_status():
    """轮询提单滑块结果：?phone=acc_xxx → {status: processing/done/failed, rid}"""
    phone = request.args.get('phone', '').strip()
    if not phone:
        return jsonify({'success': False, 'message': '缺少 phone'}), 400
    try:
        rds = make_redis()
        raw = rds.get(f'mcd:captcha:submit_status:{phone}')
    except Exception as e:
        return jsonify({'success': False, 'message': f'Redis 读取失败: {e}'}), 503
    if not raw:
        return jsonify({'success': True, 'status': 'pending'})
    try:
        st = json.loads(raw)
    except Exception:
        st = {'status': 'failed', 'msg': '状态解析失败'}
    return jsonify({'success': True, **st})


# 提单失败原始响应留档（排查 420 是否带 data.securityContext 用）
SUBMIT_RAW_LOG = os.path.join(LOG_DIR, 'submit_order_raw.log')


def _log_submit_order_raw(phone, data, message):
    """提单失败时把本次原始 data 落一行到 ~/logs/submit_order_raw.log。

    为什么单独落文件：提单 420 极低频（100002 两天才 2 次），混在 access log 里翻不到。
    这里单行 JSON（时间/账号/失败消息/是否带 securityContext/原始 data 全量），
    `grep securityContext` 就能确认到底带不带滑块凭证。

    不落 token/sid（data 里只有业务字段），phone 本身是脱敏号或 acc_ 编号。
    纯网络层失败（代理/超时，无麦当劳响应）不写，避免噪音。
    """
    try:
        code_like = bool(re.match(r'^\d{3,6},', str(message or '')))
        if not data and not code_like:
            return                      # 代理/超时类失败，没有原始响应可留
        rec = {
            'ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'phone': phone,
            'message': str(message or '')[:200],
            'has_security_context': bool((data or {}).get('securityContext')),
            'data': data if data else None,
        }
        line = json.dumps(rec, ensure_ascii=False)
        if len(line) > 6000:
            line = line[:6000] + '...(截断)'
        with open(SUBMIT_RAW_LOG, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception as e:
        app.logger.warning(f'写 submit_order 原始响应日志失败: {e}')


@app.route('/api/mcd/<function_name>', methods=['GET', 'POST'])
def universal_mcd_api(function_name):
    """
    通用接口，调用 mcd_api 中的函数

    路径: /api/mcd/<function_name>
    参数:
        - phone (从 Redis 读取 token+sid) 或
        - token + sid (直接使用)
        - 其他参数根据具体函数而定
    """
    # 1. 获取所有参数
    if request.method == 'POST':
        params = request.json or {}
    else:
        params = request.args.to_dict()

    # 5. 检查函数是否存在
    if not hasattr(mcd_api, function_name):
        return '', 404  # 不返回具体的错误信息，避免让扫描者知道你的路由结构

    # 2. 提取登录信息
    phone = params.pop('phone', None)
    city = params.pop('city', None)  # 新增：提取城市参数
    # 弹出商户/省份/代理源(不透传给 mcd_api 业务函数)
    province = params.pop('province', None)
    merchant_id = params.pop('merchant_id', None) or MERCHANT_ID_DEFAULT
    proxy_source = params.pop('proxy_source', None) or PROXY_SOURCE_DEFAULT
    # 调用方要求换新短效代理（如清车失败后重试）：不落业务函数，只影响本次取代理
    force_new_proxy = bool(params.pop('force_new_proxy', False))

    if not phone:
        return jsonify({'success': False, 'message': '缺少登录信息: 需要 phone 或 (token+sid)'}), 400

    # 账号在验证码登录冻结期 → 按手机号的处理心跳判断是真在处理还是僵尸冻结
    if is_frozen(phone):
        status = _status_of(phone)
        if status == 'code_sent':
            return jsonify({'success': False, 'data': {'status': 'code_sent'}, 'message': '验证码已发送，待完成登录(verify)'}), 423
        if processing_exists(phone) and worker_alive():
            return jsonify({'success': False, 'data': {'status': 'logging_in'}, 'message': '账号登录处理中，请稍后重试'}), 423
        # 僵尸冻结（该账号没在处理 / worker 没在跑）→ 自动解冻后继续本次请求
        unfreeze_login(phone)
        _cap_redis.delete(_processing_key(phone))
        app.logger.warning(f"[{function_name}] {phone} 僵尸冻结(status={status}, worker_alive={worker_alive()})，已自动解冻并继续")

    login_manager = get_login_manager(phone, city, province, merchant_id, proxy_source)
    token, sid, meddy_id_from_redis = login_manager.load_credentials()
    # 真机 80/80 请求都带账号级 meddyid；不参与签名，从凭证注入到当前请求上下文
    mcd_api.set_active_meddy(meddy_id_from_redis)
    login_manager.apply_location(province, city)  # 定生效省/市 + 未绑定则绑定(取代理按此省市)
    if sid and not token:
        token = login_manager.ensure_token()  # 只有 sid 无 token → 自动生成+激活+绑定

    if not (token and sid):
        return jsonify({'success': False, 'message': f'手机号 {phone} 未登录'}), 401

    # 确保 token/sid 在参数中
    params['token'] = token
    params['sid'] = sid
    # 检查目标函数是否支持 proxies 和 mcd_id参数
    func = getattr(mcd_api, function_name)
    sig = inspect.signature(func)
    if 'mcd_id' in sig.parameters:
        params['mcd_id'] = meddy_id_from_redis
    # refresh_token 自动注入（2026-11-26）：login_session_refresh 需要凭据里的 rt，
    # 客户端不持有凭据 → 签名含 refresh_token 且调用方未传时自动从凭据补。
    # rt 不轮换（11-24 实测），返回 sid/rt 与现值相同，纯「会话活跃度」画像信号。
    if 'refresh_token' in sig.parameters and not str(params.get('refresh_token') or '').strip():
        if getattr(login_manager, 'refresh_token', ''):
            params['refresh_token'] = login_manager.refresh_token
    # 5. 自动获取代理（如果有 phone 参数且函数支持 proxies）
    proxy_used = None
    # pin_proxy（2026-11-16）：客户端指定的代理（提单过滑块重提必须与第一次同出口 IP，
    # 否则 punishId 校验失效再次 420）。优先于自动代理。
    if 'pin_proxy' in sig.parameters and str(params.get('pin_proxy') or '').strip():
        pp = str(params['pin_proxy']).strip()
        if not pp.startswith('http'):
            pp = 'http://' + pp
        params['proxies'] = {'http': pp, 'https': pp}
        proxy_used = params['proxies']
    elif USE_PROXY and phone and 'proxies' not in params and 'proxies' in sig.parameters:
        # 统一入口：已绑定→静态；未绑定+已登录→绑定静态(无可用代理会抛错)；未绑定+未登录→短效复用
        try:
            proxy_dict = login_manager.get_login_proxy(is_send_code=False,
                                                      force_new=force_new_proxy)
        except Exception as e:
            # 登录成功后无可用静态代理 → 报错暴露，提示补充代理资源(不降级直连)
            app.logger.error(f"[{function_name}] {phone} 代理分配失败: {str(e)}")
            return jsonify({'success': False, 'message': f'代理分配失败(请补充代理资源): {str(e)}'}), 503
        if proxy_dict:
            params['proxies'] = proxy_dict
            proxy_used = proxy_dict
        else:
            # 代理提取失败（该城市短效池空 / 神龙 206 暂无可用IP）→ 明确报错，**绝不静默直连**。
            # 直连走的是本机（94）机房出口 IP，与真机住宅 IP 差异极大，是明显的风控特征；
            # 静默直连还会让「代理分配失败」在订单轨迹里看不出来（轨迹只显示「使用当前代理 - -」）。
            # 用户 2026-11-24 定：池空时直接报错，让本次请求失败（换号/换城市重试），不要偷偷直连。
            app.logger.error(f"[{function_name}] {phone} 无可用代理（城市={city}），已中止本次请求（不直连）")
            return jsonify({'success': False,
                            'message': f'无可用代理（{city or "未知城市"} 代理池为空），本次请求已中止，请稍后重试'}), 503

    # 记录本次实际用的代理，供响应头回传（见 _inject_proxy_headers）
    g.proxy_meta = getattr(login_manager, 'last_proxy_meta', None)

    # 打印请求参数（包括自动获得的代理）
    log_params = params.copy()
    # 隐藏敏感信息
    if 'token' in log_params:
        log_params['token'] = f"{log_params['token'][:10]}..." if log_params['token'] else None
    if 'sid' in log_params:
        log_params['sid'] = f"{log_params['sid'][:10]}..." if log_params['sid'] else None
    if 'refresh_token' in log_params:
        log_params['refresh_token'] = '***'

    app.logger.info(f"[{function_name}] 账号({'有代理' if proxy_used else '无代理'}):{phone}, 请求参数: {log_params}")

    # 6. 调用 mcd_api 函数
    try:
        func = getattr(mcd_api, function_name)
        success, data, message = func(**params)
        # 代理/网络层失败 → 把【这一个】ip:port 拉黑，下次轮巡不再用它（mcd_api 里把异常吞成了
        # 字符串，不在这里记账的话，坏端口会在池 TTL 内被反复复用，每个请求白等 15s 超时）
        if not success and proxy_used and utils.is_proxy_error(message):
            utils.mark_proxy_bad((g.get('proxy_meta') or {}).get('proxy'),
                                 source=proxy_source, merchant_id=merchant_id,
                                 reason=str(message))
        if not success and function_name == 'submit_order':
            _log_submit_order_raw(phone, data, message)
        return jsonify({'success': success, 'data': data, 'message': message})
    except TypeError as e:
        app.logger.error(f'[{function_name}] 请求参数错误: {traceback.format_exc()}')
        return jsonify({'success': False, 'message': f'参数错误: {str(e)}'}), 400
    except Exception as e:
        app.logger.error(f'[{function_name}] 请求异常: {traceback.format_exc()}')
        return jsonify({'success': False, 'message': f'请求异常: {str(e)}'}), 500


@app.after_request
def _inject_proxy_headers(response):
    """给 /api/mcd/* 的响应回传本次【实际使用】的代理，供客户端记录到订单里排查。

    只覆盖 /api/mcd/ 前缀（登录/开卡/代理管理等其它接口不加）。
    直连（未取到代理）时两个头都不出，客户端以"头缺失"表示直连。

    X-Proxy-Used: 43.226.63.205:11074   —— 出口代理 ip:port（不含账号密码）
    X-Proxy-City: 北京                  —— 代理所属城市

    注意：城市是中文，HTTP 头按 UTF-8 字节直出（curl/浏览器直接可读）。
      Python requests 取到的会是 latin-1 解码后的乱码，需还原：
        resp.headers['X-Proxy-City'].encode('latin-1').decode('utf-8')
    """
    if not request.path.startswith('/api/mcd/'):
        return response

    meta = g.get('proxy_meta') if hasattr(g, 'get') else getattr(g, 'proxy_meta', None)
    if not meta:
        return response

    ip_port = (meta.get('proxy') or '').strip()
    if ip_port:
        response.headers['X-Proxy-Used'] = ip_port

    city = (meta.get('city') or '').strip()
    if city:
        # utf-8 → latin-1 桥接：写入的是 UTF-8 原始字节，绕开 Werkzeug 的 latin-1 限制
        response.headers['X-Proxy-City'] = city.encode('utf-8').decode('latin-1')

    return response


# ========== 代理管理接口 ==========

@app.route('/api/proxy/pool/status', methods=['GET'])
def proxy_pool_status():
    """查看代理池状态"""
    try:
        pool = get_proxy_pool()
        binding_mgr = get_binding_manager()

        # 获取代理池统计
        all_proxies = pool.get_all_proxies()
        active_count = len([p for p in all_proxies if p['status'] == 'active'])
        failed_count = len([p for p in all_proxies if p['status'] == 'failed'])
        observation_count = len([p for p in all_proxies if p['status'] == 'observation'])

        # 获取绑定统计
        all_bindings = binding_mgr.get_all_bindings()
        bound_accounts = len(all_bindings)

        status = {
            'total_proxies': len(all_proxies),
            'active_proxies': active_count,
            'failed_proxies': failed_count,
            'observation_proxies': observation_count,
            'bound_accounts': bound_accounts
        }

        return jsonify({'success': True, 'data': status})
    except Exception as e:
        return jsonify({'success': False, 'message': f'获取代理池状态失败: {str(e)}'}), 500


@app.route('/api/proxy/pool/list', methods=['GET'])
def proxy_pool_list():
    """获取所有代理列表"""
    try:
        pool = get_proxy_pool()
        proxies = pool.get_all_proxies()

        return jsonify({'success': True, 'data': proxies})
    except Exception as e:
        return jsonify({'success': False, 'message': f'获取代理列表失败: {str(e)}'}), 500


@app.route('/api/proxy/pool/add', methods=['POST'])
def proxy_pool_add():
    """添加代理到代理池"""
    data = request.json or {}
    host = data.get('host')
    port = data.get('port')
    username = data.get('username', '')
    password = data.get('password', '')
    protocol = data.get('protocol', 'http')
    city = data.get('city')

    if not host or not port:
        return jsonify({'success': False, 'message': '缺少 host 或 port 参数'}), 400

    try:
        pool = get_proxy_pool()
        proxy_info = pool.add_proxy(
            host=host,
            port=int(port),
            username=username,
            password=password,
            protocol=protocol,
            city=city
        )

        return jsonify({'success': True, 'data': proxy_info, 'message': '代理添加成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'添加代理失败: {str(e)}'}), 500


@app.route('/api/proxy/pool/remove', methods=['POST'])
def proxy_pool_remove():
    """从代理池移除代理"""
    data = request.json or {}
    proxy_id = data.get('proxy_id')

    if not proxy_id:
        return jsonify({'success': False, 'message': '缺少 proxy_id 参数'}), 400

    try:
        pool = get_proxy_pool()
        proxy = pool.get_proxy(proxy_id)

        if not proxy:
            return jsonify({'success': False, 'message': '代理不存在'}), 404

        # 标记为 disabled
        pool.update_proxy(proxy_id, {'status': 'disabled'})

        return jsonify({'success': True, 'message': '代理已禁用'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'移除代理失败: {str(e)}'}), 500


@app.route('/api/proxy/account/binding', methods=['GET'])
def proxy_account_binding():
    """查看账号代理绑定关系"""
    try:
        binding_mgr = get_binding_manager()
        bindings = binding_mgr.get_all_bindings()

        return jsonify({'success': True, 'data': bindings})
    except Exception as e:
        return jsonify({'success': False, 'message': f'获取绑定关系失败: {str(e)}'}), 500


@app.route('/api/proxy/account/switch', methods=['POST'])
def proxy_account_switch():
    """手动切换账号代理"""
    data = request.json or {}
    phone = data.get('phone')

    if not phone:
        return jsonify({'success': False, 'message': '缺少 phone 参数'}), 400

    try:
        binding_mgr = get_binding_manager()
        pool = get_proxy_pool()

        # 获取当前绑定
        binding = binding_mgr.get_binding(phone)
        if not binding:
            return jsonify({'success': False, 'message': '账号未绑定代理'}), 404

        current_proxy_id = binding['proxy_id']

        # 获取新代理（排除当前代理）
        new_proxy = pool.get_available_proxy(exclude=[current_proxy_id])
        if not new_proxy:
            return jsonify({'success': False, 'message': '没有可用的代理'}), 400

        # 永久切换
        binding_mgr.permanent_switch(phone, new_proxy['id'])

        return jsonify({
            'success': True,
            'data': {'new_proxy_id': new_proxy['id']},
            'message': f'代理切换成功: {new_proxy["id"]}'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'切换代理失败: {str(e)}'}), 500


@app.route('/api/proxy/statistics', methods=['GET'])
def proxy_statistics():
    """获取代理池统计数据"""
    try:
        pool = get_proxy_pool()
        binding_mgr = get_binding_manager()

        # 代理统计
        all_proxies = pool.get_all_proxies()
        stats = {
            'total_proxies': len(all_proxies),
            'by_status': {},
            'by_city': {},
            'total_bound_accounts': 0,
            'total_requests': 0,
            'total_success': 0,
            'total_fail': 0
        }

        # 从绑定关系中实时统计账号数（修复缓存计数不准确的问题）
        all_bindings = binding_mgr.get_all_bindings()

        # 按代理ID统计实际绑定的账号数
        proxy_binding_count = {}
        for binding in all_bindings:
            proxy_id = binding.get('proxy_id')
            if proxy_id:
                proxy_binding_count[proxy_id] = proxy_binding_count.get(proxy_id, 0) + 1

        # 按状态统计
        for proxy in all_proxies:
            status = proxy['status']
            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1

            # 获取实际绑定账号数（从绑定关系统计，而非代理缓存）
            proxy_id = proxy['id']
            actual_bound_count = proxy_binding_count.get(proxy_id, 0)

            # 按城市统计
            city = proxy.get('city', '未知')
            if city not in stats['by_city']:
                stats['by_city'][city] = {
                    'count': 0,
                    'bound_accounts': 0
                }
            stats['by_city'][city]['count'] += 1
            stats['by_city'][city]['bound_accounts'] += actual_bound_count

            # 累计统计（使用实际绑定数）
            stats['total_bound_accounts'] += actual_bound_count
            stats['total_success'] += proxy.get('success_count', 0)
            stats['total_fail'] += proxy.get('fail_count', 0)

        # 总请求数
        stats['total_requests'] = stats['total_success'] + stats['total_fail']

        # 成功率
        if stats['total_requests'] > 0:
            stats['success_rate'] = f"{stats['total_success'] / stats['total_requests'] * 100:.2f}%"
        else:
            stats['success_rate'] = "0.00%"

        return jsonify({'success': True, 'data': stats})
    except Exception as e:
        return jsonify({'success': False, 'message': f'获取统计数据失败: {str(e)}'}), 500


@app.route('/api/proxy/health/check', methods=['POST'])
def proxy_health_check():
    """手动触发健康检查"""
    data = request.json or {}
    proxy_id = data.get('proxy_id')

    if not proxy_id:
        return jsonify({'success': False, 'message': '缺少 proxy_id 参数'}), 400

    try:
        pool = get_proxy_pool()

        # 执行健康检查
        result = pool.check_proxy_health(proxy_id)

        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'message': f'健康检查失败: {str(e)}'}), 500


# ========== 站大爷(zdtps)代理白名单管理 ==========

@app.route('/api/proxy/egress_ip', methods=['GET'])
def proxy_egress_ip():
    """查本机(运行本服务的机器)公网出口 IP(直连探测,多出口全列)。用于确认要加哪些 IP 进白名单。"""
    try:
        ips = utils.get_egress_ip(want_all=True)
        return jsonify({'success': True, 'data': {'egress_ips': ips}})
    except Exception as e:
        return jsonify({'success': False, 'message': f'探测出口IP失败: {str(e)}'}), 500


@app.route('/api/proxy/whitelist', methods=['GET'])
def proxy_whitelist_get():
    """查站大爷当前白名单 IP 列表。可选 ?merchant_id=。"""
    merchant_id = request.args.get('merchant_id') or MERCHANT_ID_DEFAULT
    lst = utils.zdtps_whitelist_list(merchant_id)
    if lst is None:
        return jsonify({'success': False, 'message': '查询白名单失败(见日志)'}), 502
    return jsonify({'success': True, 'data': {'whitelist': lst}})


@app.route('/api/proxy/whitelist/add', methods=['POST'])
def proxy_whitelist_add():
    """加白名单。body: {ip?(不传=加本机当前出口IP; 传则 str/逗号分隔多个), merchant_id?}。

    被「在其他实例/其他用户有绑定」拒绝时，会自动从 config.ZDTPS_LEGACY_INSTANCES 的旧实例解绑后重试。
    """
    data = request.json or {}
    merchant_id = data.get('merchant_id') or MERCHANT_ID_DEFAULT
    ok, msg, released = utils.zdtps_whitelist_add_smart(ip=data.get('ip'), merchant_id=merchant_id)
    return jsonify({'success': ok, 'message': msg,
                    'data': {'released_from': released,
                             'whitelist': utils.zdtps_whitelist_list(merchant_id)}}), (200 if ok else 502)


@app.route('/api/proxy/whitelist/auto', methods=['POST'])
def proxy_whitelist_auto():
    """出口 IP 自愈(定时任务 / 人工排查用)：探测本机出口IP → 查白名单 → 缺的补上。

    和 ProxyError 后的后台刷新走同一个 utils.ensure_egress_whitelisted：
    涵盖「新 IP 不在白名单」和「新 IP 被历史实例占着(AddWhiteIp 被拒)」两种情况。
    """
    data = request.json or {}
    merchant_id = data.get('merchant_id') or MERCHANT_ID_DEFAULT
    try:
        egress = utils.ensure_egress_whitelisted(merchant_id)
        lst = utils.zdtps_whitelist_list(merchant_id)
        ok = bool(egress) and lst is not None and all(ip in lst for ip in egress)
        return jsonify({'success': ok,
                        'data': {'egress': egress, 'whitelist': lst}}), (200 if ok else 502)
    except Exception as e:
        return jsonify({'success': False, 'message': f'自愈失败: {e}'}), 500


@app.route('/api/proxy/whitelist/del', methods=['POST'])
def proxy_whitelist_del():
    """删白名单 IP。body: {ip(必填, str/逗号分隔多个), merchant_id?}。"""
    data = request.json or {}
    ip = data.get('ip')
    if not ip:
        return jsonify({'success': False, 'message': '缺少 ip 参数'}), 400
    merchant_id = data.get('merchant_id') or MERCHANT_ID_DEFAULT
    ok, msg = utils.zdtps_whitelist_del(ip, merchant_id=merchant_id)
    return jsonify({'success': ok, 'message': msg,
                    'data': {'whitelist': utils.zdtps_whitelist_list(merchant_id)}}), (200 if ok else 502)


# 白名单同步节流窗口（秒）：外部每分钟调一次，接口内按商户 30 分钟一次
WHITELIST_SYNC_INTERVAL = 1800


def _whitelist_call(url: str, timeout: int = 15):
    """调一次 URL 模板型白名单接口（神龙/站大爷）。

    不同代理商成功表达不同（神龙 {"code":200}、站大爷 {"code":"10001"}），
    判定放宽为：HTTP 200 且 body 里 code=200 / code=1007 / code=10001 / success=true。
    1007=已存在（幂等成功，重复同步必然遇到），不能当失败，否则 last_ip 永远更新不了。
    """
    try:
        r = requests.get(url, timeout=timeout)
        body = (r.text or '')[:200]
        good = r.status_code == 200 and (
            '"code":200' in body or '"code": 200' in body
            or '"code":1007' in body or '"code": 1007' in body
            or '"code":"10001"' in body or '"code": "10001"' in body
            or '"success":true' in body or '"success": true' in body
        )
        return good, 'HTTP %s %s' % (r.status_code, body)
    except Exception as e:
        return False, '%s: %s' % (type(e).__name__, str(e)[:120])


# ---------- 星宇通道白名单（2026-11-25）----------
# 官方开放 API：https://open.xingyuip.com ，通道级鉴权 sign=md5("{channel_id}-{channel_secret}")
# 与提取接口 api.xingyuip.com:6002 不是同一个地址。增删都是 POST + JSON body {ips:[...]}。
import hashlib as _hashlib


def _xingyu_sign(channel_id: str, secret: str) -> str:
    return _hashlib.md5(('%s-%s' % (channel_id, secret)).encode('utf-8')).hexdigest()


def _xingyu_call(base: str, path: str, payload: dict, timeout: int = 20):
    """调一次星宇开放 API（增删白名单）。返回 (ok, msg, ips)。成功判据 code==0。"""
    url = base.rstrip('/') + path
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        body = (r.text or '')[:400]
        try:
            data = r.json()
        except Exception:
            data = {}
        code = data.get('code')
        ok = r.status_code == 200 and (code == 0 or code == '0')
        ips = (data.get('data') or {}).get('ips') or []
        return ok, 'HTTP %s %s' % (r.status_code, body), ips
    except Exception as e:
        return False, '%s: %s' % (type(e).__name__, str(e)[:120]), []


def _xingyu_get_ips(base: str, payload: dict, timeout: int = 20):
    """查星宇通道白名单列表。返回 (ok, ips, msg)。"""
    url = base.rstrip('/') + '/api/channel/whitelist'
    try:
        r = requests.get(url, params={'channel_id': payload['channel_id'], 'sign': payload['sign']}, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = {}
        code = data.get('code')
        ok = r.status_code == 200 and (code == 0 or code == '0')
        ips = (data.get('data') or {}).get('ips') or []
        return ok, ips, 'HTTP %s' % r.status_code
    except Exception as e:
        return False, [], '%s: %s' % (type(e).__name__, str(e)[:120])


def _sync_xingyu(cfg, cur, last_ip, fixed, ip_key):
    """星宇一家配置的加白同步。返回 (ok_all, steps)。

    逻辑：算 sign → GET 查名单 → 出口 IP 不在则 POST add（加完等几秒复查，容忍缓存延迟）
    → 换 IP 则 POST delete 旧 IP（当前 IP 绝不删）→ 固定 IP 每次 ensure。
    """
    steps, ok_all = [], True
    base = str(cfg.get('open_api_base') or 'https://open.xingyuip.com').strip()
    cid = str(cfg.get('channel_id') or '').strip()
    secret = str(cfg.get('channel_secret') or '').strip()
    if not cid or not secret:
        return False, [{'action': 'skip', 'msg': '星宇配置缺 channel_id/channel_secret'}]
    payload = {'channel_id': int(cid) if cid.isdigit() else cid,
               'sign': _xingyu_sign(cid, secret)}

    ok_q, ips, msg_q = _xingyu_get_ips(base, payload)
    steps.append({'action': 'query', 'ok': ok_q, 'count': len(ips), 'msg': msg_q})
    if not ok_q:
        return False, steps

    # 1) 固定 IP：不在名单就加（幂等）
    for ip in fixed:
        if ip in ips:
            steps.append({'action': 'add_fixed', 'ip': ip, 'ok': True, 'msg': '已在名单'})
            continue
        ok, msg, new_ips = _xingyu_call(base, '/api/channel/whitelist/add',
                                        dict(payload, ips=[ip]))
        steps.append({'action': 'add_fixed', 'ip': ip, 'ok': ok, 'msg': msg})
        ok_all = ok_all and ok
        if ok:
            ips = new_ips if new_ips else ips + [ip]

    # 2) 动态出口：不在名单就加；加完等几秒复查（星宇加白到提取节点有缓存延迟，约 8s）
    if cur:
        if cur in ips:
            steps.append({'action': 'ensure_cur', 'ip': cur, 'ok': True, 'msg': '已在名单'})
        else:
            ok, msg, new_ips = _xingyu_call(base, '/api/channel/whitelist/add',
                                            dict(payload, ips=[cur]))
            steps.append({'action': 'add_new', 'ip': cur, 'ok': ok, 'msg': msg})
            if not ok:
                ok_all = False
            else:
                ips = new_ips if new_ips else ips + [cur]
                # 容忍缓存延迟：等 6 秒后查一次，若仍未生效按成功处理（add 接口已返回 code=0）
                try:
                    time.sleep(6)
                    ok2, ips2, _ = _xingyu_get_ips(base, payload)
                    if ok2:
                        ips = ips2
                        steps.append({'action': 'recheck', 'ok': True, 'in_list': cur in ips})
                except Exception:
                    pass
        # 换 IP 才删旧的（当前 IP 绝不删）——用户明确要求保留此逻辑
        if last_ip and cur != last_ip and last_ip in ips:
            ok_del, msg_del, _ = _xingyu_call(base, '/api/channel/whitelist/delete',
                                              dict(payload, ips=[last_ip]))
            steps.append({'action': 'del_old', 'ip': last_ip, 'ok': ok_del, 'msg': msg_del})
        try:
            _cap_redis.set(ip_key, cur)  # 仅 add 成功后更新（ok_all 为真或当前 IP 已在名单）
        except Exception:
            pass
    else:
        steps.append({'action': 'skip', 'ip': '', 'ok': False, 'msg': '取不到出口 IP'})
        ok_all = False
    return ok_all, steps


@app.route('/api/proxy/whitelist/sync', methods=['POST'])
def proxy_whitelist_sync():
    """商户代理白名单同步（商户 × 代理商多配置，2026-11-25）。

    配置来源：Redis Hash `mkt:prod:{商户}:whitelist:{provider}`（平台保存时写入，94 不连平台 DB）
      mode=url_template：add_url 必填含 {ip}、del_url 选填（神龙/站大爷，原逻辑）
      mode=xingyu：channel_id/channel_secret/open_api_base（星宇专用 JSON API）
      fixed_ips 常驻 IP（只加不删）、enabled='1' 才同步

    body：{merchant_id?(不传=扫全部), force?(忽略节流)}
    动态出口变化时：仅当 last_ip 存在且 cur != last_ip 才删旧 IP，当前 IP 绝不删。
    """
    data = request.get_json(silent=True) or {}
    only = str(data.get('merchant_id') or '').strip()
    force = bool(data.get('force'))

    targets = []
    try:
        # 新格式（按 provider 分 key）：mkt:prod:{商户}:whitelist:{provider}
        # 排除运行态后缀 key（:last_ip / :last_sync / :sync_requested）
        skip_suffix = (':last_ip', ':last_sync', ':sync_requested', ':last_sync_msg')
        for key in _cap_redis.scan_iter(match='mkt:prod:*:whitelist:*', count=200):
            s = str(key)
            if s.endswith(skip_suffix):
                continue
            parts = s.split(':')
            if len(parts) < 5:
                continue
            if only and parts[2] != only:
                continue
            targets.append((parts[2], parts[4], s))
        # 兼容旧格式（单 Hash，无 provider）：按 url_template 处理
        for key in _cap_redis.scan_iter(match='mkt:prod:*:whitelist', count=200):
            s = str(key)
            parts = s.split(':')
            if len(parts) < 4:
                continue
            if only and parts[2] != only:
                continue
            targets.append((parts[2], 'legacy', s))
    except Exception as e:
        return jsonify({'success': False, 'message': '扫描白名单配置失败: %s' % e}), 500

    if only and not targets:
        return jsonify({'success': False, 'message': '该商户没有启用的白名单配置'}), 400

    results = []
    for m, provider, key in targets:
        try:
            cfg = _cap_redis.hgetall(key)
        except Exception as e:
            results.append({'merchant_id': m, 'provider': provider, 'ok': False, 'msg': '读配置失败: %s' % e})
            continue
        if str(cfg.get('enabled') or '') != '1':
            continue
        mode = str(cfg.get('mode') or ('xingyu' if provider == 'xingyu' else 'url_template'))

        req_key, sync_key, ip_key = key + ':sync_requested', key + ':last_sync', key + ':last_ip'
        try:
            requested = _cap_redis.exists(req_key) == 1
            last_ts = float(_cap_redis.get(sync_key) or 0)
        except Exception:
            requested, last_ts = False, 0
        if not force and not requested and (time.time() - last_ts) < WHITELIST_SYNC_INTERVAL:
            continue

        del_url = str(cfg.get('del_url') or '').strip()
        # 商户自定义固定 IP + 全局常驻 IP（config.GLOBAL_FIXED_IPS），去重且顺序稳定。
        # 常驻 IP 无条件加白（只加不删），商户不用在后台手填。
        fixed = []
        for ip in [x.strip() for x in str(cfg.get('fixed_ips') or '').split(',') if x.strip()] + list(GLOBAL_FIXED_IPS):
            if ip not in fixed:
                fixed.append(ip)
        cur = utils.get_egress_ip()
        try:
            last_ip = _cap_redis.get(ip_key) or ''
        except Exception:
            last_ip = ''

        if mode == 'xingyu':
            ok_all, steps = _sync_xingyu(cfg, cur, last_ip, fixed, ip_key)
        else:
            add_url = str(cfg.get('add_url') or '').strip()
            if not add_url or '{ip}' not in add_url:
                results.append({'merchant_id': m, 'provider': provider, 'ok': False,
                                'msg': 'add_url 缺失或不含 {ip}'})
                continue
            steps, ok_all = [], True
            # 固定 IP：每次 ensure（幂等，防止被别人删掉）
            for ip in fixed:
                ok, msg = _whitelist_call(add_url.replace('{ip}', ip))
                steps.append({'action': 'add_fixed', 'ip': ip, 'ok': ok, 'msg': msg})
                ok_all = ok_all and ok
            # 动态出口：每次 ensure（神龙对已存在 IP 返回 1007，幂等安全）
            if cur:
                ok, msg = _whitelist_call(add_url.replace('{ip}', cur))
                action = 'add_new' if (cur != last_ip) else 'ensure_cur'
                steps.append({'action': action, 'ip': cur, 'ok': ok, 'msg': msg})
                if ok:
                    try:
                        _cap_redis.set(ip_key, cur)
                    except Exception:
                        pass
                else:
                    ok_all = False
                # 仅真换过 IP 才删旧（当前 IP 绝不删）
                if last_ip and cur != last_ip and del_url:
                    ok_del, msg_del = _whitelist_call(del_url.replace('{ip}', last_ip))
                    steps.append({'action': 'del_old', 'ip': last_ip, 'ok': ok_del, 'msg': msg_del})
            else:
                steps.append({'action': 'skip', 'ip': '', 'ok': False, 'msg': '取不到出口 IP'})
                ok_all = False

        try:
            _cap_redis.set(sync_key, str(int(time.time())))
            if requested:
                _cap_redis.delete(req_key)
            _cap_redis.hset(key, 'last_sync_msg',
                            ('OK ' if ok_all else 'FAIL ') + time.strftime('%Y-%m-%d %H:%M:%S'))
        except Exception:
            pass
        results.append({'merchant_id': m, 'provider': provider, 'ok': ok_all,
                        'egress': cur, 'last_ip': last_ip, 'steps': steps[:8]})

    app.logger.info('[whitelist-sync] 同步配置数=%d' % len(results))
    return jsonify({'success': True, 'data': {'synced': len(results), 'details': results}})


@app.route('/api/proxy/stats', methods=['GET'])
def proxy_stats_query():
    """
    查短效代理用量(按 源+省+市+日期)。默认近7天;?days=N 近N天;?date=YYYY-MM-DD 只查某天。
    可选过滤 ?source= ?province= ?city= ?merchant_id=。
    返回 {tree:{源:{省:{市:{日期:{req,proxy}}}}}, totals:{req,proxy}, dates:[...]}。
    req=请求(使用)次数, proxy=消耗(新提取)的短效代理数。
    """
    args = request.args
    merchant_id = args.get('merchant_id') or MERCHANT_ID_DEFAULT
    try:
        days = int(args.get('days', 7))
    except (TypeError, ValueError):
        days = 7
    res = utils.query_proxy_stats(
        merchant_id=merchant_id, days=days, date=args.get('date'),
        source=args.get('source'), province=args.get('province'), city=args.get('city'))
    return jsonify({'success': True, 'data': res})


# ========== 健康检查 ==========

@app.route('/health', methods=['GET'])
def health():
    """健康检查"""
    return jsonify({
        'status': 'ok',
        'service': 'mcd-api-refactored',
        'app_version': mcd_api.CLIENT_PROFILE.version,
        'upstream_api_base': mcd_api.API_BASE,
    })


def start_gunicorn():
    bind_address = '0.0.0.0:5001'
    worker_count = 5

    # 启动 server 前只加一次白名单(多 worker 不重复探测/添加)
    try:
        utils.ensure_egress_whitelisted(MERCHANT_ID_DEFAULT)
    except Exception as _e:
        print(f'⚠️  自动加白名单跳过: {_e}', flush=True)

    cmd = [
        sys.executable, '-m', 'gunicorn',
        '-w', str(worker_count),
        '-b', bind_address,
        '--access-logfile', os.path.join(LOG_DIR, 'mcd_server.log'),
        '--timeout', '100',
        'mcd_server:app'   # 改成实际文件名
    ]
    os.system("kill -9 $(ps -ef|grep mcd_server:app|grep -v grep|awk '{print $2}')")
    process = subprocess.Popen(cmd, cwd=CODE_DIR)
    return process

if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=5001, debug=False)
    start_gunicorn()

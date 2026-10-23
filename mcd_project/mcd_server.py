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

from flask import Flask, request, jsonify
import mcd_api
import json
import os
import redis
from datetime import datetime
import inspect
import time
from config import (CODE_DIR, LOG_DIR, REDIS_SET_URL, USE_PROXY, CAPTCHA_LOGIN_CONFIG, make_redis,
                    MERCHANT_ID_DEFAULT, PROXY_SOURCE_DEFAULT, CREDENTIALS_KEY_TMPL)
from login_manager import LoginManager
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


def _freeze_key(phone):
    return CAPTCHA_LOGIN_CONFIG['freezing_key'].format(phone=phone)


def _status_key(phone):
    return CAPTCHA_LOGIN_CONFIG['status_key'].format(phone=phone)


def is_frozen(phone):
    """账号是否处于验证码登录冻结期"""
    return _cap_redis.exists(_freeze_key(phone)) == 1


def enqueue_login(phone, city=None, force_new_device=False, province=None, merchant_id=None, proxy_source=None):
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
                                 'proxy_source': proxy_source or PROXY_SOURCE_DEFAULT}, ensure_ascii=False))


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
                  province=eff_province, merchant_id=merchant_id, proxy_source=proxy_source)
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
                'meddy_id': login_manager.meddy_id
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


# ========== 通用 mcd_api 接口 ==========

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
    login_manager.apply_location(province, city)  # 定生效省/市 + 未绑定则绑定(取代理按此省市)

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
    # 5. 自动获取代理（如果有 phone 参数且函数支持 proxies）
    proxy_used = None
    if USE_PROXY and phone and 'proxies' not in params and 'proxies' in sig.parameters:
        # 统一入口：已绑定→静态；未绑定+已登录→绑定静态(无可用代理会抛错)；未绑定+未登录→短效复用
        try:
            proxy_dict = login_manager.get_login_proxy(is_send_code=False)
        except Exception as e:
            # 登录成功后无可用静态代理 → 报错暴露，提示补充代理资源(不降级直连)
            app.logger.error(f"[{function_name}] {phone} 代理分配失败: {str(e)}")
            return jsonify({'success': False, 'message': f'代理分配失败(请补充代理资源): {str(e)}'}), 503
        if proxy_dict:
            params['proxies'] = proxy_dict
            proxy_used = proxy_dict

    # 打印请求参数（包括自动获得的代理）
    log_params = params.copy()
    # 隐藏敏感信息
    if 'token' in log_params:
        log_params['token'] = f"{log_params['token'][:10]}..." if log_params['token'] else None
    if 'sid' in log_params:
        log_params['sid'] = f"{log_params['sid'][:10]}..." if log_params['sid'] else None

    app.logger.info(f"[{function_name}] 账号({'有代理' if proxy_used else '无代理'}):{phone}, 请求参数: {log_params}")

    # 6. 调用 mcd_api 函数
    try:
        func = getattr(mcd_api, function_name)
        success, data, message = func(**params)
        return jsonify({'success': success, 'data': data, 'message': message})
    except TypeError as e:
        app.logger.error(f'[{function_name}] 请求参数错误: {traceback.format_exc()}')
        return jsonify({'success': False, 'message': f'参数错误: {str(e)}'}), 400
    except Exception as e:
        app.logger.error(f'[{function_name}] 请求异常: {traceback.format_exc()}')
        return jsonify({'success': False, 'message': f'请求异常: {str(e)}'}), 500


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
    """加白名单。body: {ip?(不传=加本机当前出口IP; 传则 str/逗号分隔多个), merchant_id?}。"""
    data = request.json or {}
    merchant_id = data.get('merchant_id') or MERCHANT_ID_DEFAULT
    ok, msg = utils.zdtps_whitelist_add(ip=data.get('ip'), merchant_id=merchant_id)
    return jsonify({'success': ok, 'message': msg,
                    'data': {'whitelist': utils.zdtps_whitelist_list(merchant_id)}}), (200 if ok else 502)


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
    return jsonify({'status': 'ok', 'service': 'mcd-api-refactored'})


def start_gunicorn():
    bind_address = '0.0.0.0:5001'
    worker_count = 5

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
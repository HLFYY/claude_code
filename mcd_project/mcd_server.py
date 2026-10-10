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
from config import CODE_DIR, LOG_DIR, REDIS_SET_URL
from login_manager import LoginManager

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
        logging.FileHandler(os.path.join(LOG_DIR, 'mcd_server.log'), encoding='utf-8')  # 同时写入文件
    ]
)

# 配置 Flask 和 werkzeug 日志
app.logger.setLevel(logging.INFO)
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.addFilter(NoiseFilter())
werkzeug_logger.setLevel(logging.INFO)

# 使用 Redis 存储的 LoginManager
login_manager_cache = {}

def get_login_manager(phone):
    """获取或创建指定手机号的 LoginManager 实例（使用 Redis）"""
    if phone not in login_manager_cache:
        # 传入全局单例，避免重复初始化
        manager = LoginManager(
            phone,
            use_redis=True,
            proxy_pool=get_proxy_pool(),
            binding_manager=get_binding_manager(),
            failure_handler=get_failure_handler()
        )
        login_manager_cache[phone] = manager
    return login_manager_cache[phone]

# 配置
CREDENTIALS_FILE = 'data/credentials.json'

# ========== 登录相关接口 ==========

@app.route('/api/login/send_code', methods=['POST'])
def login_send_code():
    """发送验证码"""
    data = request.json or {}
    phone = data.get('phone')
    use_new_device = data.get('use_new_device', False)

    app.logger.info(f"[send_code] 请求参数: phone={phone}, use_new_device={use_new_device}")

    if not phone:
        error_msg = '缺少 phone 参数'
        app.logger.error(f"[send_code] ❌ 失败: {error_msg}")
        return jsonify({'success': False, 'message': error_msg}), 400

    login_manager = get_login_manager(phone)
    login_manager.load_credentials()
    # 发送验证码
    success, message = login_manager.send_verify_code(force_new_device=use_new_device)

    if success:
        return jsonify({
            'success': True,
            'data': {'token': login_manager.token},
            'message': message
        })
    else:
        app.logger.error(f"[send_code] ❌ 失败: {message}")
        return jsonify({'success': False, 'message': message}), 400

@app.route('/api/login/verify', methods=['POST'])
def login_verify():
    """验证登录"""
    data = request.json or {}
    phone = data.get('phone')
    verify_code = data.get('verify_code')

    app.logger.info(f"[login_verify] 请求参数: phone={phone}, verify_code={verify_code}")

    if not phone or not verify_code:
        error_msg = '缺少 phone 或 verify_code 参数'
        app.logger.error(f"[login_verify] ❌ 参数错误: {error_msg}, 收到的数据: {data}")
        return jsonify({'success': False, 'message': error_msg}), 400

    login_manager = get_login_manager(phone)
    login_manager.load_credentials()
    # 调用登录
    success, message = login_manager.do_login(verify_code)

    if success:
        app.logger.info(f"[login_verify] ✓ 登录成功: phone={phone}")
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
        app.logger.error(f"[login_verify] ❌ 登录失败: phone={phone}, 原因: {message}")
        return jsonify({'success': False, 'message': message}), 400

@app.route('/api/login/status', methods=['GET'])
def login_status():
    """检测登录状态"""
    phone = request.args.get('phone')

    app.logger.info(f"[login_status] 请求参数: phone={phone}")

    if not phone:
        error_msg = '缺少 phone 参数'
        app.logger.error(f"[login_status] ❌ 参数错误: {error_msg}")
        return jsonify({'success': False, 'message': error_msg}), 400

    login_manager = get_login_manager(phone)

    # 加载凭证
    token, sid, meddy_id = login_manager.load_credentials()

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

        if is_valid:
            app.logger.info(f"[login_status] ✓ 已登录: phone={phone}")
            return jsonify({
                'success': True,
                'data': {
                    'is_logged_in': True,
                    'token': login_manager.token,
                    'sid': login_manager.sid,
                    'meddy_id': login_manager.meddy_id,
                    'user_info': user_info
                },
                'message': '已登录'
            })
        else:
            app.logger.info(f"[login_status] 登录凭证已失效: phone={phone}")
            return jsonify({
                'success': True,
                'data': {'is_logged_in': False},
                'message': '登录已失效'
            })
    except Exception as e:
        app.logger.error(f"[login_status] ❌ 检查登录状态失败: phone={phone}, 错误: {str(e)}")
        return jsonify({
            'success': True,
            'data': {'is_logged_in': False},
            'message': f'检查登录状态失败: {str(e)}'
        })


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

    if not phone:
        return jsonify({'success': False, 'message': '缺少登录信息: 需要 phone 或 (token+sid)'}), 400

    login_manager = get_login_manager(phone)
    token, sid, meddy_id_from_redis = login_manager.load_credentials()

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
    if phone and 'proxies' not in params:
        try:
            if 'proxies' in sig.parameters:
                # 使用全局单例获取账号绑定的代理
                binding_mgr = get_binding_manager()
                pool = get_proxy_pool()

                # 获取账号的代理绑定
                binding = binding_mgr.get_binding(phone)
                if binding:
                    proxy_id = binding.get('proxy_id') or binding.get('temp_proxy_id')
                    if proxy_id:
                        proxy_url = pool.get_proxy_url(proxy_id)
                        if proxy_url:
                            # 构造 requests 库需要的代理字典格式
                            proxy_dict = {
                                'http': proxy_url,
                                'https': proxy_url
                            }
                            params['proxies'] = proxy_dict
                            proxy_used = proxy_dict
        except Exception as e:
            # 代理获取失败不影响主流程，记录日志即可
            app.logger.warning(f"获取代理失败: {str(e)}")

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

        # 按状态统计
        for proxy in all_proxies:
            status = proxy['status']
            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1

            # 按城市统计
            city = proxy.get('city', '未知')
            if city not in stats['by_city']:
                stats['by_city'][city] = {
                    'count': 0,
                    'bound_accounts': 0
                }
            stats['by_city'][city]['count'] += 1
            stats['by_city'][city]['bound_accounts'] += proxy.get('bound_accounts', 0)

            # 累计统计
            stats['total_bound_accounts'] += proxy.get('bound_accounts', 0)
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
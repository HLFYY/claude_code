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

from flask import Flask, request, jsonify
import mcd_api
import json
import os
import redis
from datetime import datetime

from config import CODE_DIR, LOG_DIR
from login_manager import LoginManager

app = Flask(__name__)

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

logging.getLogger('werkzeug').addFilter(NoiseFilter())

# 使用 Redis 存储的 LoginManager
login_manager_cache = {}

def get_login_manager(phone):
    """获取或创建指定手机号的 LoginManager 实例（使用 Redis）"""
    if phone not in login_manager_cache:
        login_manager_cache[phone] = LoginManager(phone, use_redis=True)
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

    if not phone:
        return jsonify({'success': False, 'message': '缺少 phone 参数'}), 400

    login_manager = get_login_manager(phone)

    # 发送验证码
    success, message = login_manager.send_verify_code(force_new_device=use_new_device)

    if success:
        return jsonify({
            'success': True,
            'data': {'token': login_manager.token},
            'message': message
        })
    else:
        return jsonify({'success': False, 'message': message}), 400

@app.route('/api/login/verify', methods=['POST'])
def login_verify():
    """验证登录"""
    data = request.json or {}
    phone = data.get('phone')
    verify_code = data.get('verify_code')

    if not phone or not verify_code:
        return jsonify({'success': False, 'message': '缺少 phone 或 verify_code 参数'}), 400

    login_manager = get_login_manager(phone)

    # 调用登录
    success, message = login_manager.do_login(verify_code)

    if success:
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
        return jsonify({'success': False, 'message': message}), 400

@app.route('/api/login/status', methods=['GET'])
def login_status():
    """检测登录状态"""
    phone = request.args.get('phone')

    if not phone:
        return jsonify({'success': False, 'message': '缺少 phone 参数'}), 400

    login_manager = get_login_manager(phone)

    # 加载凭证
    token, sid, meddy_id = login_manager.load_credentials()

    if not (token and sid):
        return jsonify({
            'success': True,
            'data': {'is_logged_in': False},
            'message': '未登录'
        })

    # 检查登录状态
    try:
        is_valid, user_info = login_manager.check_login_status()

        if is_valid:
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
            return jsonify({
                'success': True,
                'data': {'is_logged_in': False},
                'message': '登录已失效'
            })
    except Exception as e:
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
    token = params.get('token')
    sid = params.get('sid')

    # 3. 如果没有 token/sid，用 phone 从 Redis 读取
    meddy_id_from_redis = None
    if not (token and sid):
        if not phone:
            return jsonify({'success': False, 'message': '缺少登录信息: 需要 phone 或 (token+sid)'}), 400

        login_manager = get_login_manager(phone)
        token, sid, meddy_id_from_redis = login_manager.load_credentials()

        if not (token and sid):
            return jsonify({'success': False, 'message': f'手机号 {phone} 未登录'}), 401

        # 确保 token/sid 在参数中
        params['token'] = token
        params['sid'] = sid

    # 4. 如果参数中 mcd_id 为 None 或缺失，使用从 Redis 读取的 meddy_id
    if 'mcd_id' in params and params.get('mcd_id') is None:
        # 如果之前没有加载过，现在加载
        if meddy_id_from_redis is None and phone:
            login_manager = get_login_manager(phone)
            _, _, meddy_id_from_redis = login_manager.load_credentials()

        if meddy_id_from_redis:
            params['mcd_id'] = meddy_id_from_redis


    # 6. 调用 mcd_api 函数
    try:
        func = getattr(mcd_api, function_name)
        success, data, message = func(**params)
        return jsonify({'success': success, 'data': data, 'message': message})
    except TypeError as e:
        return jsonify({'success': False, 'message': f'参数错误: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'请求异常: {str(e)}'}), 500


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
    os.system("pkill -f 'mcd_server:app' || true")
    process = subprocess.Popen(cmd, cwd=CODE_DIR)
    return process

if __name__ == '__main__':
    gunicorn_process = start_gunicorn()
    gunicorn_process.wait()   # 让主进程保持前台，便于进程管理器接管

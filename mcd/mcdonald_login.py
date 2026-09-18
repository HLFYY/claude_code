#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳完整登录流程实现
包括: Token生成 → 发送验证码 → 登录 → 获取SID

需要填充密钥:
- AES_KEY: 从 SecBox.getAesKey() 提取
- SIGN_KEY: 从 SecBox.getSignKey() 提取
"""

import hmac
import hashlib
import base64
import uuid
import json
import requests
from datetime import datetime
from email.utils import formatdate
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad


# ========== 配置区域 ==========

# ⚠️ 需要从 SO 库中提取的密钥
AES_KEY = None   # SecBox.INSTANCE.getAesKey()
SIGN_KEY = None  # SecBox.INSTANCE.getSignKey()

# 固定配置
ACCESS_KEY = 'HJ7YLqOY06F61FPEhF7H'
AUTH_VERSION = 'hmac-auth-v1'
ALGORITHM = 'hmac-sha256'
SIGNED_HEADERS = ['ct', 'language', 'p', 'sid', 'sv', 'token', 'v', 'x-mcd-gw-v']

# API 端点
API_BASE = 'https://api.mcd.cn'
API2_BASE = 'https://api2.mcd.cn'


# ========== 工具函数 ==========

def generate_token():
    """
    生成 Token (模拟 AppInfoUtil.getToken())

    优先级:
    1. DeviceId (IMEI 或 Android ID) - 需要设备权限
    2. UUID - 随机生成

    Returns:
        32字符十六进制字符串
    """
    # 生成 UUID 并去掉连字符
    return uuid.uuid4().hex


def aes_encrypt(plaintext, key):
    """
    AES/ECB/PKCS5Padding 加密 (模拟 SecurityUtils.aesEncrypt())

    Args:
        plaintext: 明文字符串 (手机号或验证码)
        key: AES 密钥

    Returns:
        32字符十六进制字符串
    """
    if not key:
        raise ValueError("AES_KEY 未设置！需要从 SO 库中提取")

    # 创建 AES cipher (ECB 模式)
    cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)

    # PKCS5 填充
    padded = pad(plaintext.encode('utf-8'), AES.block_size)

    # 加密
    encrypted = cipher.encrypt(padded)

    # 转换为十六进制字符串 (小写)
    return encrypted.hex()


def generate_authorization(headers, date, sign_key):
    """
    生成 Authorization 签名头

    Args:
        headers: 包含所有签名参数的字典
        date: GMT 格式时间字符串
        sign_key: HMAC 签名密钥

    Returns:
        完整的 authorization 字符串
    """
    if not sign_key:
        raise ValueError("SIGN_KEY 未设置！需要从 SO 库中提取")

    # 按顺序拼接请求头键值对
    parts = [f"{k}={headers.get(k, '')}" for k in SIGNED_HEADERS]
    message = "&".join(parts)

    # 计算 HMAC-SHA256 签名
    signature = base64.b64encode(
        hmac.new(
            sign_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode('ascii')

    # 组装完整的 authorization
    return '#'.join([
        AUTH_VERSION,
        ACCESS_KEY,
        signature,
        ALGORITHM,
        date,
        ';'.join(SIGNED_HEADERS)
    ])


def generate_x_hmac_digest(body, sign_key):
    """
    生成 X-Hmac-Digest 签名头

    Args:
        body: JSON 请求体字符串
        sign_key: HMAC 签名密钥

    Returns:
        Base64 编码的签名
    """
    if not sign_key:
        raise ValueError("SIGN_KEY 未设置！需要从 SO 库中提取")

    signature = hmac.new(
        sign_key.encode('utf-8'),
        body.encode('utf-8'),
        hashlib.sha256
    ).digest()

    return base64.b64encode(signature).decode('ascii')


def build_headers(token, sid='', body=None):
    """
    构建完整的请求头

    Args:
        token: 设备 Token
        sid: 会话 SID (登录后获得)
        body: JSON 请求体 (用于 x-hmac-digest 签名)

    Returns:
        完整的请求头字典
    """
    # 生成 GMT 时间
    date = formatdate(timeval=None, localtime=False, usegmt=True)

    # 基础请求头
    headers = {
        'ct': '102',
        'language': 'cn',
        'p': '102',
        'sid': sid,
        'sv': 'v4',
        'token': token,
        'v': '7.0.41.0',
        'x-mcd-gw-v': '1',
    }

    # 生成 authorization
    authorization = generate_authorization(headers, date, SIGN_KEY)

    # 生成 x-hmac-digest
    body_str = body if body else ''
    x_hmac_digest = generate_x_hmac_digest(body_str, SIGN_KEY)

    # 返回完整请求头
    result = {
        'Host': 'api.mcd.cn',
        'user-agent': 'mcdonald_Android/7.0.41.0 (Android)',
        'ct': headers['ct'],
        'language': headers['language'],
        'p': headers['p'],
        'sid': headers['sid'],
        'sv': headers['sv'],
        'token': headers['token'],
        'v': headers['v'],
        'x-mcd-gw-v': headers['x-mcd-gw-v'],
        'mcdtoken': token,
        'authorization': authorization,
        'x-hmac-digest': x_hmac_digest,
        'biz_scenario': '102',
        'biz_from': '1006',
        'routingid': '',
        'meddyid': '',
        'Content-Type': 'application/json',
    }

    return result


# ========== 主要功能 ==========

def send_verification_code(phone, token):
    """
    步骤 2: 发送验证码

    Args:
        phone: 手机号 (明文)
        token: 设备 Token

    Returns:
        (success, message)
    """
    # 加密手机号
    encrypted_phone = aes_encrypt(phone, AES_KEY)

    # 构建请求体
    body_data = {
        "regionCode": "86",
        "tel": encrypted_phone,
        "type": 1
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    # 构建请求头
    headers = build_headers(token, sid='', body=body_str)

    # 发送请求
    url = f'{API_BASE}/bff/passport/verifyCode/sms/send'
    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            return True, "验证码已发送"
        else:
            return False, result.get('message', '发送失败')
    except Exception as e:
        return False, f"请求异常: {str(e)}"


def login(phone, verify_code, token):
    """
    步骤 3: 提交登录

    Args:
        phone: 手机号 (明文)
        verify_code: 短信验证码 (明文)
        token: 设备 Token

    Returns:
        (success, sid, meddy_id, message)
    """
    # 加密手机号和验证码
    encrypted_phone = aes_encrypt(phone, AES_KEY)
    encrypted_code = aes_encrypt(verify_code, AES_KEY)

    # 构建请求体
    body_data = {
        "citicRegister": True,
        "code": encrypted_code,
        "deviceInfoId": token,
        "regionCode": "86",
        "tel": encrypted_phone,
        "secondPhoneFlag": False
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    # 构建请求头 (使用 API2)
    headers = build_headers(token, sid='', body=body_str)
    headers['Host'] = 'api2.mcd.cn'

    # 发送请求
    url = f'{API2_BASE}/bff/passport/login/mobile'
    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            sid = data.get('sid', '')
            meddy_id = data.get('meddyId', '')
            return True, sid, meddy_id, "登录成功"
        else:
            return False, '', '', result.get('message', '登录失败')
    except Exception as e:
        return False, '', '', f"请求异常: {str(e)}"


def test_api(token, sid):
    """
    步骤 4: 测试已登录的 API 请求

    Args:
        token: 设备 Token
        sid: 会话 SID

    Returns:
        (success, data, message)
    """
    # 构建请求头 (GET 请求，body 为空字符串)
    headers = build_headers(token, sid=sid, body='')

    # 测试获取门店信息
    url = f'{API_BASE}/bff/store/stores/vicinity'
    params = {
        'showType': '4',
        'latitude': '31.15413',
        'longitude': '121.554804',
        'simulationTest': '0'
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "API 调用成功"
        else:
            return False, {}, result.get('message', 'API 调用失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


# ========== 完整流程 ==========

def complete_login_flow(phone):
    """
    完整的登录流程

    流程:
    1. 生成 Token
    2. 发送验证码
    3. 等待用户输入验证码
    4. 提交登录
    5. 获取 SID
    6. 测试 API 调用

    Args:
        phone: 手机号 (11位数字)

    Returns:
        (success, token, sid, meddy_id, message)
    """
    print("=" * 60)
    print("麦当劳完整登录流程")
    print("=" * 60)

    # 检查密钥
    if not AES_KEY or not SIGN_KEY:
        print("\n❌ 密钥未设置！")
        print("\n需要从 SO 库中提取以下密钥：")
        print("  - AES_KEY  = SecBox.INSTANCE.getAesKey()")
        print("  - SIGN_KEY = SecBox.INSTANCE.getSignKey()")
        print("\n推荐方案：委托专业 SO 脱壳服务（500-1000元，3-5天）")
        print("=" * 60)
        return False, '', '', '', '密钥未设置'

    # 步骤 1: 生成 Token
    print("\n[步骤 1/5] 生成 Token...")
    token = generate_token()
    print(f"✅ Token: {token}")

    # 步骤 2: 发送验证码
    print("\n[步骤 2/5] 发送验证码...")
    success, message = send_verification_code(phone, token)
    if not success:
        print(f"❌ 失败: {message}")
        return False, token, '', '', message
    print(f"✅ {message}")

    # 步骤 3: 等待用户输入验证码
    print("\n[步骤 3/5] 请输入收到的验证码:")
    verify_code = input("验证码: ").strip()

    # 步骤 4: 提交登录
    print("\n[步骤 4/5] 提交登录...")
    success, sid, meddy_id, message = login(phone, verify_code, token)
    if not success:
        print(f"❌ 失败: {message}")
        return False, token, '', '', message
    print(f"✅ {message}")
    print(f"   SID: {sid}")
    print(f"   MeddyID: {meddy_id}")

    # 步骤 5: 测试 API
    print("\n[步骤 5/5] 测试 API 调用...")
    success, data, message = test_api(token, sid)
    if not success:
        print(f"❌ 失败: {message}")
        return True, token, sid, meddy_id, message
    print(f"✅ {message}")
    print(f"   门店信息: {data.get('pickupStore', {}).get('name', 'N/A')}")

    print("\n" + "=" * 60)
    print("🎉 完整流程执行成功！")
    print("=" * 60)
    print(f"\n保存以下信息用于后续 API 调用：")
    print(f"  Token: {token}")
    print(f"  SID: {sid}")
    print(f"  MeddyID: {meddy_id}")
    print("=" * 60)

    return True, token, sid, meddy_id, '登录成功'


# ========== 主程序 ==========

if __name__ == '__main__':
    # 示例：使用测试手机号
    test_phone = "16752934813"

    # 执行完整登录流程
    success, token, sid, meddy_id, message = complete_login_flow(test_phone)

    if success and sid:
        print("\n🎉 所有功能已就绪，可以调用任何麦当劳 API！")
    else:
        print(f"\n❌ 流程未完成: {message}")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳完整登录流程实现
包括: Token生成 → 发送验证码 → 登录 → 获取SID

需要填充密钥:
- AES_KEY: 从 SecBox.getAesKey() 提取
- V4SK: 从 SecBox.getV4sk() 提取（用于签名）
- V4AK: 从 SecBox.getV4ak() 提取（访问密钥）
"""

import hmac
import hashlib
import base64
import uuid
import json
from urllib.parse import quote

import requests
from datetime import datetime
from email.utils import formatdate
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad


# ========== 配置区域 ==========

# 从 SecBox 提取的密钥 (通过 Frida hook 获取)
AES_KEY = 'w8ZJ4wrUl7dDB1A7'    # SecBox.getAesKey() - 用于 AES 加密
V4SK = 'JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp'  # SecBox.getV4sk() - 用于 HMAC 签名
V4AK = 'HJ7YLqOY06F61FPEhF7H'  # SecBox.getV4ak() - 访问密钥

# 固定配置
AUTH_VERSION = 'hmac-auth-v1'
ALGORITHM = 'hmac-sha256'
SIGNED_HEADERS = ['ct', 'language', 'p', 'sid', 'sv', 'token', 'v', 'x-mcd-gw-v']

# API 端点
API_BASE = 'https://api.mcd.cn'
API2_BASE = 'https://api2.mcd.cn'


# ========== 工具函数 ==========

def get_current_daypart_code():
    """
    根据当前时间自动获取时段代码

    Returns:
        str: 时段代码
    """
    now = datetime.now()
    hour = now.hour
    minute = now.minute

    # 根据时间判断时段
    if (hour == 5 and minute >= 0) or (5 < hour < 10) or (hour == 10 and minute < 30):
        return '1'  # 早餐 05:00-10:29
    elif (hour == 10 and minute >= 30) or (10 < hour < 14) or (hour == 14 and minute < 30):
        return '8'  # 午餐 10:30-14:29
    elif (hour == 14 and minute >= 30) or (14 < hour < 17):
        return '4'  # 下午茶 14:30-16:59
    elif 17 <= hour < 22:
        return '5'  # 夜市 17:00-21:59
    else:  # 22:00-04:59
        return '6'  # 宵夜 22:00-04:59


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


def generate_authorization(method, path, query_string, headers, date, sign_key):
    """
    生成 Authorization 签名头

    关键发现（通过 Frida Native Hook 获取）：
    1. 签名消息格式：method\npath\nquery_string\nv4ak\ndate\ncanonical_headers\n\n
    2. 查询参数必须按字母顺序排序
    3. 规范化请求头格式：key:value（用 \n 连接，不是分号）
    4. 签名消息末尾有一个空行

    Args:
        method: HTTP 方法 (GET/POST/PUT)
        path: API 路径 (如 /bff/common/proxy/tid)
        query_string: 查询参数字符串（已排序）
        headers: 包含所有签名参数的字典
        date: GMT 格式时间字符串
        sign_key: HMAC 签名密钥

    Returns:
        完整的 authorization 字符串
    """
    if not sign_key:
        raise ValueError("V4SK 未设置！需要从 SO 库中提取")

    # 构建规范化请求头（key:value 格式，用 \n 连接）
    canonical_headers_list = []
    for k in SIGNED_HEADERS:
        value = headers.get(k, '')
        canonical_headers_list.append(f"{k}:{value}")
    canonical_headers = '\n'.join(canonical_headers_list)

    # 构建签名消息（注意：末尾有空行）
    message = '\n'.join([
        method.upper(),
        path,
        query_string if query_string else '',
        V4AK,
        date,
        canonical_headers,
        ''  # 重要：最后有一个空行
    ])

    # 计算 HMAC-SHA256 签名
    signature = base64.b64encode(
        hmac.new(
            sign_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode('ascii')

    # 组装完整的 authorization
    signed_headers_str = ';'.join(SIGNED_HEADERS)
    return f"{AUTH_VERSION}#{V4AK}#{signature}#{ALGORITHM}#{date}#{signed_headers_str}"


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
        raise ValueError("V4SK 未设置！需要从 SO 库中提取")

    signature = hmac.new(
        sign_key.encode('utf-8'),
        body.encode('utf-8'),
        hashlib.sha256
    ).digest()

    return base64.b64encode(signature).decode('ascii')


def build_headers(token, sid='', body=None, method='GET', path='/', query_params=None):
    """
    构建完整的请求头

    Args:
        token: 设备 Token
        sid: 会话 SID (登录后获得)
        body: JSON 请求体 (用于 x-hmac-digest 签名)
        method: HTTP 方法 (GET/POST/PUT)
        path: API 路径
        query_params: 查询参数字典 (用于签名，会自动排序)

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

    # 构建查询字符串（按字母顺序排序）
    query_string = ''
    if query_params:
        sorted_params = sorted(query_params.items())
        query_string = '&'.join([f"{k}={quote(str(v), 'utf-8')}" for k, v in sorted_params])

    # 生成 authorization
    authorization = generate_authorization(method, path, query_string, headers, date, V4SK)

    # 生成 x-hmac-digest
    body_str = body if body else ''
    x_hmac_digest = generate_x_hmac_digest(body_str, V4SK)

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

def activate_token(token):
    """
    步骤 1.5: Token 激活 (获取 tid)

    API: GET /bff/common/proxy/tid

    Args:
        token: 设备 Token

    Returns:
        (success, tid, message)
    """
    # 构建请求参数
    params = {
        'channelId': '96',
        'campaignId': '0',
        'exposureId': '0',
        'groupId': '0',
        'salerId': '0'
    }

    # 构建请求头（传递 query_params 用于签名）
    headers = build_headers(token, sid='', body=None, method='GET', path='/bff/common/proxy/tid', query_params=params)

    # 发送请求
    url = f'{API_BASE}/bff/common/proxy/tid'
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            tid = result.get('data', {}).get('tid', '')
            return True, tid, "Token 激活成功"
        else:
            return False, '', result.get('message', '激活失败')
    except Exception as e:
        return False, '', f"请求异常: {str(e)}"


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

    # 构建请求头（POST 请求到 API2，需传递 body 和 path）
    path = '/bff/passport/verifyCode/sms/send'
    headers = build_headers(token, sid='', body=body_str, method='POST', path=path)
    headers['Host'] = 'api2.mcd.cn'

    # 发送请求
    url = f'{API2_BASE}{path}'
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

    # 构建请求头（POST 请求到 API2，需传递 method 和 path）
    path = '/bff/passport/login/mobile'
    headers = build_headers(token, sid='', body=body_str, method='POST', path=path)
    headers['Host'] = 'api2.mcd.cn'

    # 发送请求
    url = f'{API2_BASE}{path}'
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


# ========== 店铺相关 API ==========

def get_all_cities(token, sid):
    """
    获取所有城市信息 (对应 curl: /bff/store/cities/group)

    Args:
        token: 设备 Token
        sid: 会话 SID

    Returns:
        (success, cities_data, message)
    """
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/store/cities/group')
    url = f'{API_BASE}/bff/store/cities/group'

    try:
        response = requests.get(url, headers=headers, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取城市信息成功"
        else:
            return False, {}, result.get('message', '获取城市信息失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_city_by_location(token, sid, latitude, longitude):
    """
    通过经纬度获取当前城市信息 (对应 curl: /bff/store/cities)

    Args:
        token: 设备 Token
        sid: 会话 SID
        latitude: 纬度
        longitude: 经度

    Returns:
        (success, city_data, message)
    """
    params = {
        'latitude': str(latitude),
        'longitude': str(longitude)
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/store/cities', query_params=params)
    url = f'{API_BASE}/bff/store/cities'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            city = data.get('city', {})
            return True, city, f"获取城市成功: {city.get('name', '')}"
        else:
            return False, {}, result.get('message', '获取城市失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def search_stores(token, sid, city_code, keyword, page_no=1, page_size=10, hot_tag_code='', be_type='', daypart_codes=''):
    """
    通过搜索获取店铺列表 (对应 curl: /bff/store/stores)

    Args:
        token: 设备 Token
        sid: 会话 SID
        city_code: 城市代码
        keyword: 搜索关键词
        page_no: 页码 (默认1)
        page_size: 每页数量 (默认10)
        hot_tag_code: 热门标签代码
        be_type: BE类型
        daypart_codes: 时段代码

    Returns:
        (success, stores_data, message)
    """
    params = {
        'pageNo': str(page_no),
        'pageSize': str(page_size),
        'cityCode': city_code,
        'keyword': keyword,
        'hotTagCode': hot_tag_code,
        'beType': be_type,
        'dayPartCodes': daypart_codes
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/store/stores', query_params=params)
    url = f'{API_BASE}/bff/store/stores'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            stores = data.get('stores', [])
            total_size = data.get('totalSize', 0)
            return True, data, f"找到 {total_size} 家店铺"
        else:
            return False, {}, result.get('message', '搜索店铺失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_nearby_stores(token, sid, latitude, longitude, show_type='2', be_type='', order_type=1, keyword=''):
    """
    获取附近店铺

    Args:
        token: 设备 Token
        sid: 会话 SID
        latitude: 纬度
        longitude: 经度
        show_type: 显示类型 (默认2)
        be_type: BE类型
        order_type: 订单类型 (1=堂食, 2=外卖)
        keyword: 搜索关键词

    Returns:
        (success, stores_list, message)
    """
    params = {
        'showType': show_type,
        'beType': be_type,
        'orderType': str(order_type),
        'latitude': str(latitude),
        'dayPartCodes': '',
        'longitude': str(longitude),
        'keyword': keyword
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/store/stores/vicinity', query_params=params)
    url = f'{API_BASE}/bff/store/stores/vicinity'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            stores = data.get('stores', [])
            return True, stores, f"找到 {len(stores)} 家店铺"
        else:
            return False, [], result.get('message', '获取店铺失败')
    except Exception as e:
        return False, [], f"请求异常: {str(e)}"


def get_store_menu(token, sid, store_code, be_code='', order_type=1, daypart_code=None, order_mode='0', pin_id=''):
    """
    获取店铺菜单 (对应 curl: /bff/spc/menu)

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        be_code: BE编码
        order_type: 订单类型 (1=堂食, 2=外卖)
        daypart_code: 时段编码 (None=自动获取)
        order_mode: 订单模式 (默认'0')
        pin_id: Pin ID (默认'')

    Returns:
        (success, menu_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    # beType: 1=堂食, 2=外卖 (注意：curl中beType为1表示堂食)
    be_type = '1' if order_type == 1 else '2'

    params = {
        'storeCode': store_code,
        'orderType': str(order_type),
        'beCode': be_code,
        'beType': be_type,
        'orderMode': order_mode,
        'pinId': pin_id,
        'dayPartCode': str(daypart_code)
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/spc/menu', query_params=params)
    url = f'{API_BASE}/bff/spc/menu'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取菜单成功"
        else:
            return False, {}, result.get('message', '获取菜单失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_product_detail(token, sid, product_code, store_code, be_code='', order_type=1, daypart_code=None,
                       card_id='', coupon_code='', coupon_id='', channel_code='03', order_mode='0',
                       page_source=1, pin_id='', product_promotions=None):
    """
    获取商品详情 (对应 curl: /bff/spc/products/detail/{product_code})

    Args:
        token: 设备 Token
        sid: 会话 SID
        product_code: 商品编码
        store_code: 店铺编码
        be_code: BE编码
        order_type: 订单类型 (1=堂食, 2=外卖)
        daypart_code: 时段编码 (None=自动获取)
        card_id: 卡片ID
        coupon_code: 优惠券代码
        coupon_id: 优惠券ID
        channel_code: 渠道代码 (默认'03')
        order_mode: 订单模式 (默认'0')
        page_source: 页面来源 (默认1)
        pin_id: Pin ID
        product_promotions: 商品促销信息列表

    Returns:
        (success, product_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    # 默认促销信息
    if product_promotions is None:
        product_promotions = []

    body_data = {
        "cardId": card_id,
        "cartBoGo": False,
        "cartType": "1",
        "channelCode": channel_code,
        "couponCode": coupon_code,
        "couponId": coupon_id,
        "daypartCode": str(daypart_code),
        "hasCard": False,
        "id": f"1-{product_code}",
        "orderMode": order_mode,
        "orderType": order_type,
        "pageSource": page_source,
        "pinId": pin_id,
        "productCode": product_code,
        "productPromotions": product_promotions,
        "storeCode": store_code
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    headers = build_headers(token, sid=sid, body=body_str, method='POST', path=f'/bff/spc/products/detail/{product_code}')
    url = f'{API_BASE}/bff/spc/products/detail/{product_code}'

    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取商品详情成功"
        else:
            return False, {}, result.get('message', '获取详情失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


# ========== 购物车相关 API ==========

def get_cart(token, sid, store_code, be_code='', order_type=1, daypart_code=None):
    """
    查询购物车

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        be_code: BE编码
        order_type: 订单类型 (1=堂食, 2=外卖)
        daypart_code: 时段编码 (None=自动获取)

    Returns:
        (success, cart_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    params = {
        'cartType': '1',
        'channelCode': '03',
        'daypartCode': str(daypart_code),
        'orderType': str(order_type),
        'storeCode': store_code,
        'beCode': be_code,
        'orderMode': '0',
        'pinId': '',
        'pickupTimeType': ''
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/cart/carts', query_params=params)
    url = f'{API_BASE}/bff/cart/carts'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取购物车成功"
        else:
            return False, {}, result.get('message', '获取购物车失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def clear_cart(token, sid, store_code, be_code='', order_type=1, daypart_code=None,
               store_name='', channel_code='03', order_mode='0', pin_id='', pickup_time_type=''):
    """
    清空购物车 (对应 curl: /bff/cart/carts/empty)

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        be_code: BE编码
        order_type: 订单类型 (1=堂食, 2=外卖)
        daypart_code: 时段编码 (None=自动获取)
        store_name: 店铺名称
        channel_code: 渠道代码 (默认'03')
        order_mode: 订单模式 (默认'0')
        pin_id: Pin ID
        pickup_time_type: 取餐时间类型

    Returns:
        (success, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    # beType: 0=堂食, 1=外卖 (清空购物车curl中用的是0)
    be_type = 0 if order_type == 1 else 1

    body_data = {
        "beCode": be_code,
        "beType": be_type,
        "canReverse": False,
        "cartType": "1",
        "changeAddress": 1,
        "channelCode": channel_code,
        "daypartCode": str(daypart_code),
        "orderMode": order_mode,
        "orderType": order_type,
        "pickupTimeType": pickup_time_type,
        "pinId": pin_id,
        "storeCode": store_code,
        "storeName": store_name,
        "supportGroupMealPromotion": False
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    headers = build_headers(token, sid=sid, body=body_str, method='PUT', path='/bff/cart/carts/empty')
    url = f'{API_BASE}/bff/cart/carts/empty'

    try:
        response = requests.put(url, headers=headers, data=body_str, timeout=10)
        result = response.json()
        if result.get('success'):
            data = result.get('data', {})
            return True, data, "清空购物车成功"
        else:
            return False, {}, result.get('message', '清空购物车失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def add_to_cart(token, sid, store_code, be_code='', product_code='', product_name='', product_image='',
                quantity=1, order_type=1, daypart_code=None, product_type='1', combo_items=None,
                store_name='', channel_code='03', order_mode='0', pin_id='', pickup_time_type='',
                card_id='', card_type=0, coupon_code='', coupon_id='', gm_assist_service_code='',
                membership_code='', sequence=-1, animation_id=''):
    """
    添加商品到购物车 (对应 curl: /bff/cart/carts PUT)

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        be_code: BE编码
        product_code: 商品编码
        product_name: 商品名称
        product_image: 商品图片
        quantity: 数量
        order_type: 订单类型 (1=堂食, 2=外卖)
        daypart_code: 时段编码 (None=自动获取)
        product_type: 商品类型 (1=单品, 7=套餐)
        combo_items: 套餐子项列表 (仅套餐需要)
        store_name: 店铺名称
        channel_code: 渠道代码 (默认'03')
        order_mode: 订单模式 (默认'0')
        pin_id: Pin ID
        pickup_time_type: 取餐时间类型
        card_id: 卡片ID
        card_type: 卡片类型
        coupon_code: 优惠券代码
        coupon_id: 优惠券ID
        gm_assist_service_code: GM助手服务代码
        membership_code: 会员代码
        sequence: 序列号
        animation_id: 动画ID

    Returns:
        (success, cart_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    # beType: 1=堂食, 2=外卖 (curl中beType为1表示堂食)
    be_type = 1 if order_type == 1 else 2

    # 构建商品数据
    product_data = {
        "animationId": animation_id,
        "cardId": card_id,
        "cardType": card_type,
        "code": product_code,
        "couponCode": coupon_code,
        "couponId": coupon_id,
        "gmAssistServiceCode": gm_assist_service_code,
        "id": f"1-{product_code}",
        "image": product_image,
        "membershipCode": membership_code,
        "name": product_name,
        "quantity": quantity,
        "sequence": sequence,
        "type": product_type
    }

    # 如果是套餐，添加套餐子项
    if combo_items:
        product_data["comboItems"] = combo_items

    body_data = {
        "beCode": be_code,
        "beType": be_type,
        "cartType": "1",
        "channelCode": channel_code,
        "dataSource": 1,
        "daypartCode": str(daypart_code),
        "hasCustomized": False,
        "maxPurchaseQuantity": 999,
        "orderMode": order_mode,
        "orderType": order_type,
        "pickupTimeType": pickup_time_type,
        "pinId": pin_id,
        "products": [product_data],
        "storeCode": store_code,
        "storeName": store_name,
        "supportGroupMealPromotion": False
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    headers = build_headers(token, sid=sid, body=body_str, method='PUT', path='/bff/cart/carts')
    url = f'{API_BASE}/bff/cart/carts'

    try:
        response = requests.put(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "添加购物车成功"
        else:
            return False, {}, result.get('message', '添加购物车失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


# ========== 订单相关 API ==========

def get_order_validation_info(token, sid, store_code, channel_code='03', order_type=1, cart_type=1,
                               daypart_code=None, be_code='', table_id='', date='', time='',
                               pickup_time_type='', pin_id='', order_mode=0):
    """
    获取订单验证信息 (对应 curl: /bff/order/confirmation/validationinfo)
    购物车点击去结算请求1

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        channel_code: 渠道代码 (默认'03')
        order_type: 订单类型 (1=堂食, 2=外卖)
        cart_type: 购物车类型 (默认1)
        daypart_code: 时段编码 (None=自动获取)
        be_code: BE编码
        table_id: 桌号
        date: 日期
        time: 时间
        pickup_time_type: 取餐时间类型
        pin_id: Pin ID
        order_mode: 订单模式 (默认0)

    Returns:
        (success, validation_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    params = {
        'storeCode': store_code,
        'channelCode': channel_code,
        'orderType': str(order_type),
        'cartType': str(cart_type),
        'dayPartCode': str(daypart_code),
        'beCode': be_code,
        'tableId': table_id,
        'date': date,
        'time': time,
        'pickupTimeType': pickup_time_type,
        'pinId': pin_id,
        'orderMode': str(order_mode)
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/order/confirmation/validationinfo', query_params=params)
    url = f'{API_BASE}/bff/order/confirmation/validationinfo'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取订单验证信息成功"
        else:
            return False, {}, result.get('message', '获取订单验证信息失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_order_promotion_info(token, sid, store_code, cart_items, be_type='1', daypart_code=None,
                             order_type='1', eat_type_code='eat-in', tableware_code='no',
                             order_mode='0', pin_id='', real_total_amount='0'):
    """
    获取促销/优惠券信息 (对应 curl: /bff/order/confirmation/promotion)
    购物车点击去结算请求2

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        cart_items: 购物车商品列表
        be_type: BE类型 (1=堂食, 2=外卖)
        daypart_code: 时段编码 (None=自动获取)
        order_type: 订单类型 ('1'=堂食, '2'=外卖)
        eat_type_code: 就餐方式代码 (eat-in=堂食, locker-in=外带)
        tableware_code: 餐具代码 (no=不需要, yes=需要)
        order_mode: 订单模式 (默认'0')
        pin_id: Pin ID
        real_total_amount: 实际总金额

    Returns:
        (success, promotion_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    body_data = {
        "activityOrder": 0,
        "autoMatch": False,
        "beType": be_type,
        "cartItems": cart_items,
        "customerConfirm": False,
        "date": "",
        "dayPartCode": str(daypart_code),
        "driveDuration": -1,
        "driveDurationNew": -1,
        "eatTypeCode": eat_type_code,
        "expectDeliveryDateCode": "",
        "latitude": 0.0,
        "longitude": 0.0,
        "menuCardList": [],
        "orderMode": order_mode,
        "orderType": order_type,
        "pinId": pin_id,
        "pinType": 0,
        "realTotalAmount": real_total_amount,
        "simulationTest": 0,
        "skipDtTimeCheck": False,
        "skipPriceChange": False,
        "source": 0,
        "storeCode": store_code,
        "tablewareCode": tableware_code,
        "time": ""
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    headers = build_headers(token, sid=sid, body=body_str, method='POST', path='/bff/order/confirmation/promotion')
    url = f'{API_BASE}/bff/order/confirmation/promotion'

    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取促销信息成功"
        else:
            return False, {}, result.get('message', '获取促销信息失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_nearest_store_info(token, sid, store_code, latitude, longitude, be_code=''):
    """
    获取最近门店信息 (对应 curl: /bff/store/stores/getNearest/{store_code})
    点击去支付时验证门店

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        latitude: 纬度
        longitude: 经度
        be_code: BE编码

    Returns:
        (success, store_info, message)
    """
    body_data = {
        "beCode": be_code,
        "latitude": latitude,
        "longitude": longitude
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    headers = build_headers(token, sid=sid, body=body_str, method='POST', path=f'/bff/store/stores/getNearest/{store_code}')
    url = f'{API_BASE}/bff/store/stores/getNearest/{store_code}'

    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取门店信息成功"
        else:
            return False, {}, result.get('message', '获取门店信息失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def submit_order(token, sid, store_code, cart_items, be_type='1', daypart_code=None,
                order_type='1', eat_type_code='eat-in', tableware_code='no',
                order_mode='0', pin_id='', latitude=0.0, longitude=0.0,
                date='', time='', menu_card_list=None):
    """
    提交订单 (对应 curl: /bff/order/orders)
    使用 v5 加密

    Args:
        token: 设备 Token
        sid: 会话 SID
        store_code: 店铺编码
        cart_items: 购物车商品列表
        be_type: BE类型 ('1'=堂食, '2'=外卖)
        daypart_code: 时段编码 (None=自动获取)
        order_type: 订单类型 ('1'=堂食, '2'=外卖)
        eat_type_code: 就餐方式代码 (eat-in=堂食, locker-in=外带)
        tableware_code: 餐具代码 (no=不需要, yes=需要)
        order_mode: 订单模式 (默认'0')
        pin_id: Pin ID
        latitude: 纬度
        longitude: 经度
        date: 预约日期
        time: 预约时间
        menu_card_list: 菜单卡列表

    Returns:
        (success, order_data, message)
    """
    # 自动获取时段代码
    if daypart_code is None:
        daypart_code = get_current_daypart_code()

    if menu_card_list is None:
        menu_card_list = []

    body_data = {
        "activityOrder": 0,
        "autoMatch": False,
        "beType": be_type,
        "cartItems": cart_items,
        "customerConfirm": False,
        "date": date,
        "dayPartCode": str(daypart_code),
        "driveDuration": -1,
        "driveDurationNew": -1,
        "eatTypeCode": eat_type_code,
        "expectDeliveryDateCode": "",
        "latitude": latitude,
        "longitude": longitude,
        "menuCardList": menu_card_list,
        "orderMode": order_mode,
        "orderType": order_type,
        "pinId": pin_id,
        "pinType": 0,
        "realTotalAmount": "0",  # 会自动计算
        "simulationTest": 0,
        "skipDtTimeCheck": False,
        "skipPriceChange": False,
        "source": 0,
        "storeCode": store_code,
        "tablewareCode": tableware_code,
        "time": time
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    # 注意：提交订单使用 v5 加密，需要特殊处理
    # 这里暂时使用 v4，实际使用时需要实现 v5 签名
    headers = build_headers(token, sid=sid, body=body_str, method='POST', path='/bff/order/orders')
    url = f'{API_BASE}/bff/order/orders'

    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            order_id = data.get('orderId')
            pay_id = data.get('payId')
            return True, data, f"订单提交成功，订单号: {order_id}"
        else:
            return False, {}, result.get('message', '提交订单失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_payment_channels(token, sid, order_id, pay_id, mcd_id, source=0):
    """
    获取支付渠道 (对应 curl: /bff/cashier/channels)

    Args:
        token: 设备 Token
        sid: 会话 SID
        order_id: 订单ID
        pay_id: 支付ID
        mcd_id: 麦当劳用户ID (meddyId)
        source: 来源 (默认0)

    Returns:
        (success, channels_data, message)
    """
    params = {
        'orderId': order_id,
        'payId': pay_id,
        'mcdId': mcd_id,
        'source': str(source)
    }
    headers = build_headers(token, sid=sid, body='', method='GET', path='/bff/cashier/channels', query_params=params)
    url = f'{API_BASE}/bff/cashier/channels'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取支付渠道成功"
        else:
            return False, {}, result.get('message', '获取支付渠道失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def create_payment(token, sid, pay_id, pay_channel='ALI', free_pay_flg=False, source=0):
    """
    创建支付订单 (对应 curl: /bff/cashier/preorder)

    Args:
        token: 设备 Token
        sid: 会话 SID
        pay_id: 支付ID
        pay_channel: 支付渠道 (ALI=支付宝, WX=微信, UNION=银联)
        free_pay_flg: 是否免密支付
        source: 来源 (默认0)

    Returns:
        (success, payment_data, message)
    """
    body_data = {
        "freePayFlg": free_pay_flg,
        "payChannel": pay_channel,
        "payId": pay_id,
        "source": source
    }
    body_str = json.dumps(body_data, separators=(',', ':'))

    headers = build_headers(token, sid=sid, body=body_str, method='POST', path='/bff/cashier/preorder')
    url = f'{API_BASE}/bff/cashier/preorder'

    try:
        response = requests.post(url, headers=headers, data=body_str, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "创建支付订单成功"
        else:
            return False, {}, result.get('message', '创建支付订单失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"


def get_order_detail(token, sid, order_id):
    """
    获取订单详情

    Args:
        token: 设备 Token
        sid: 会话 SID
        order_id: 订单ID

    Returns:
        (success, order_data, message)
    """
    params = {'isShowMLandCover': 'false'}
    headers = build_headers(token, sid=sid, body='', method='GET', path=f'/bff/order/orders/{order_id}', query_params=params)
    url = f'{API_BASE}/bff/order/orders/{order_id}'

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            return True, data, "获取订单详情成功"
        else:
            return False, {}, result.get('message', '获取订单详情失败')
    except Exception as e:
        return False, {}, f"请求异常: {str(e)}"

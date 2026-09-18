#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳API签名实现 - 框架代码
需要从SO库中提取SecretKey后填充
"""

import hmac
import hashlib
import base64
from urllib.parse import urlparse, urlencode
from datetime import datetime, timezone
from email.utils import formatdate
import requests


class McDonaldAPIAuth:
    """
    麦当劳API HMAC-SHA256签名认证
    """

    # 常量配置
    ACCESS_KEY = 'HJ7YLqOY06F61FPEhF7H'
    SECRET_KEY = None  # ⚠️ 需要从SO库中提取
    ALGORITHM = 'hmac-sha256'
    AUTH_VERSION = 'hmac-auth-v1'
    SIGNED_HEADERS = ['ct', 'language', 'p', 'sid', 'sv', 'token', 'v', 'x-mcd-gw-v']

    def __init__(self, token, secret_key=None):
        """
        初始化认证

        Args:
            token: 用户token（必需）
            secret_key: HMAC密钥（需要逆向获取）
        """
        self.token = token
        if secret_key:
            self.SECRET_KEY = secret_key

    def generate_authorization(self, headers, date):
        """
        生成 authorization 签名（基于 APiInfo.md 分析）

        格式：hmac-auth-v1#ACCESS_KEY#SIGNATURE#hmac-sha256#DATE#SIGNED_HEADERS

        测试发现：
        - authorization 会在一二十分钟后失效
        - 说明签名中包含时间戳验证

        Args:
            headers: 包含签名所需请求头的字典
            date: GMT格式的时间戳字符串

        Returns:
            完整的 authorization 字符串
        """
        if not self.SECRET_KEY:
            raise ValueError("SecretKey未设置！需要从SO库中提取")

        # 按照固定顺序拼接参与签名的请求头
        # 基于 authorization 字段中的列表: ct;language;p;sid;sv;token;v;x-mcd-gw-v
        header_keys = self.SIGNED_HEADERS

        # ⚠️ 消息格式待验证，这里提供几种可能的格式

        # 格式1: 简单的键值对拼接
        parts = [f"{k}={headers.get(k, '')}" for k in header_keys]
        message_format1 = "&".join(parts)

        # 格式2: 加上 accesskey 和 date
        message_format2 = f"accesskey={self.ACCESS_KEY}&date={date}&" + "&".join(parts)

        # 格式3: 只用请求头值（不带键名）
        message_format3 = "&".join([headers.get(k, '') for k in header_keys])

        # TODO: 需要通过测试确定哪种格式正确
        # 暂时使用格式1
        message = message_format1

        # 计算 HMAC-SHA256 签名
        signature = base64.b64encode(
            hmac.new(
                self.SECRET_KEY.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('ascii')

        # 组装完整的 authorization
        authorization = '#'.join([
            self.AUTH_VERSION,           # hmac-auth-v1
            self.ACCESS_KEY,             # HJ7YLqOY06F61FPEhF7H
            signature,                   # 签名值（Base64）
            self.ALGORITHM,              # hmac-sha256
            date,                        # GMT时间戳
            ';'.join(header_keys)        # ct;language;p;sid;sv;token;v;x-mcd-gw-v
        ])

        return authorization

    def generate_digest(self, body):
        """
        生成 x-hmac-digest 签名（基于 APiInfo.md 测试）

        对请求体（JSON字符串）进行 HMAC-SHA256 签名

        测试证明：
        - "换了另一个商品的请求参数postdata，headers不变，请求失败"
        - "只把x-hmac-digest的值换成抓包的值，就可以请求成功"
        - 说明 x-hmac-digest 是对请求体内容的签名

        Args:
            body: 请求体字符串（通常是JSON）

        Returns:
            Base64编码的 HMAC-SHA256 签名
        """
        if not self.SECRET_KEY:
            raise ValueError("SecretKey未设置！需要从SO库中提取")

        # 对请求体进行 HMAC-SHA256 签名
        signature = hmac.new(
            self.SECRET_KEY.encode('utf-8'),
            body.encode('utf-8'),
            hashlib.sha256
        ).digest()

        # Base64 编码
        return base64.b64encode(signature).decode('ascii')

    def sign_request(self, method, url, params=None, body=None, extra_headers=None):
        """
        为请求生成签名

        Args:
            method: HTTP方法（GET, POST等）
            url: 完整URL
            params: query参数字典
            body: 请求体字符串（POST/PUT的JSON字符串）
            extra_headers: 额外的headers（如sid等）

        Returns:
            包含所有必需headers的字典
        """
        # 生成GMT时间
        date = formatdate(timeval=None, localtime=False, usegmt=True)

        # 构造必需headers
        headers = {
            'ct': '102',
            'language': 'cn',
            'p': '102',
            'sid': '',
            'sv': 'v4',
            'token': self.token,
            'v': '7.0.41.0',
            'x-mcd-gw-v': '1',
        }

        # 合并额外headers
        if extra_headers:
            headers.update(extra_headers)

        # 生成 authorization 签名
        authorization = self.generate_authorization(headers, date)

        # 生成 x-hmac-digest 签名
        # 对于 GET 请求，body 为空字符串
        # 对于 POST/PUT 请求，body 是 JSON 字符串
        body_str = body if body else ''
        x_hmac_digest = self.generate_digest(body_str)

        # 返回完整headers
        result_headers = headers.copy()
        result_headers.update({
            'Host': urlparse(url).netloc,
            'user-agent': 'mcdonald_Android/7.0.41.0 (Android)',
            'authorization': authorization,
            'x-hmac-digest': x_hmac_digest,
            'mcdtoken': self.token,
            'biz_scenario': '102',
            'biz_from': '1006',
            'routingid': '',
            'meddyid': '',
        })

        return result_headers


def example_usage():
    """
    使用示例
    """
    # ⚠️ 需要先从SO库中提取SecretKey
    SECRET_KEY = None  # TODO: 填入提取的密钥

    if not SECRET_KEY:
        print("=" * 60)
        print("❌ SecretKey未设置！")
        print("=" * 60)
        print("\n📋 当前状态:")
        print("  - ✅ 签名算法已分析（基于 APiInfo.md）")
        print("  - ✅ 双重签名机制已确认")
        print("  - ✅ 消息格式已推测（待验证）")
        print("  - ❌ 密钥仍未获取（在加密的 SO 中）")
        print("\n🎯 下一步:")
        print("  1. 委托专业 SO 脱壳服务（500-1000元，3-5天）")
        print("  2. 从脱壳后的 libcsiipowerenter.so 中提取密钥")
        print("  3. 填入 SECRET_KEY 变量")
        print("  4. 测试签名生成")
        print("\n📖 参考文档:")
        print("  - SIGNATURE_ANALYSIS.md - 完整签名机制分析")
        print("  - HANDOVER_DOCUMENT.md - 技术文档")
        print("  - APiInfo.md - 测试数据")
        print("=" * 60)
        return

    # 创建认证实例
    auth = McDonaldAPIAuth(
        token='749adb7088124d29af47b74f52bed1b5',
        secret_key=SECRET_KEY
    )

    print("=" * 60)
    print("测试1: GET 请求（菜单接口）")
    print("=" * 60)

    # 测试 GET 请求
    headers = auth.sign_request(
        method='GET',
        url='https://api.mcd.cn/bff/spc/menu',
        params={
            'storeCode': '1450688',
            'orderType': '2',
            'beCode': '145068802',
            'beType': '2',
            'orderMode': '0',
            'pinId': '',
            'dayPartCode': '1',
        },
        extra_headers={
            'sid': 'de03f6c78eda9303d83025f3de2f90fb_',
            'meddyid': 'MEDDY163321681473498257'
        }
    )

    print(f"authorization: {headers['authorization']}")
    print(f"x-hmac-digest: {headers['x-hmac-digest']}")

    response = requests.get(
        'https://api.mcd.cn/bff/spc/menu',
        headers=headers,
        params={
            'storeCode': '1450688',
            'orderType': '2',
            'beCode': '145068802',
            'beType': '2',
            'orderMode': '0',
            'pinId': '',
            'dayPartCode': '1',
        }
    )

    print(f"\n状态码: {response.status_code}")
    if response.status_code == 200:
        print("✅ 请求成功！")
    else:
        print("❌ 请求失败")
    print(f"响应: {response.text[:200]}...")

    print("\n" + "=" * 60)
    print("测试2: POST 请求（登录接口）")
    print("=" * 60)

    import json
    login_body = {
        "citicRegister": True,
        "code": "3023015ccc44d88009999c7acf3f0d1b",
        "deviceInfoId": "749adb7088124d29af47b74f52bed1b5",
        "regionCode": "86",
        "tel": "cd56d8d91f6d92b6520686df3fbe32c8",
        "secondPhoneFlag": False
    }

    headers = auth.sign_request(
        method='POST',
        url='https://api2.mcd.cn/bff/passport/login/mobile',
        body=json.dumps(login_body, separators=(',', ':'))
    )

    print(f"authorization: {headers['authorization']}")
    print(f"x-hmac-digest: {headers['x-hmac-digest']}")

    response = requests.post(
        'https://api2.mcd.cn/bff/passport/login/mobile',
        headers=headers,
        json=login_body
    )

    print(f"\n状态码: {response.status_code}")
    if response.status_code == 200:
        print("✅ 请求成功！")
    else:
        print("❌ 请求失败")
    print(f"响应: {response.text[:200]}...")

    print("\n" + "=" * 60)


if __name__ == '__main__':
    example_usage()

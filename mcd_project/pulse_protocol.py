"""pulse 协议工具：仅构造/解析数据，不访问业务 API、Redis 或网络。"""

import base64
import gzip
import hashlib
import hmac
import io
import os
import time
from urllib.parse import unquote, urlsplit


PULSE_WS_URL = 'wss://ws.mcd.cn/bff/pulse/ws'
PULSE_HEARTBEAT = bytes.fromhex(
    '1f8b08000000000000007312f470750c0a7172750c890f720d0c750d0e01003c955b8113000000')
PULSE_INTERVAL = 15


def build_pulse_headers(sid, meddy, profile, st=None, now=None):
    """按 SocketRequest.kt 签名；可选整数毫秒偏移默认 0，不发起校时请求。"""
    if not sid:
        raise ValueError('缺少 sid')
    if not profile.wskey:
        raise ValueError('当前版本未配置 wskey')
    if st is None:
        offset_ms = int(os.environ.get('MCD_PULSE_TIME_OFFSET_MS', '0'))
        st = str(int((time.time() if now is None else now) * 1000) + offset_ms)
    headers = {
        'sid': sid, 'ct': profile.client_type, 'v': profile.version,
        'meddyId': meddy or '', 'scene': '', 'sv': 'v4', 'st': str(st),
        'User-Agent': 'okhttp/4.9.2', 'Accept-Encoding': 'gzip',
    }
    message = ''.join(headers[field] for field in
                      ('ct', 'meddyId', 'scene', 'sid', 'st', 'sv', 'v'))
    headers['sign'] = base64.b64encode(hmac.new(
        profile.wskey.encode('utf-8'), message.encode('utf-8'),
        hashlib.sha256).digest()).decode('ascii')
    return headers


def pulse_message_type(payload):
    """限长解压并读取 protobuf 字段 8；不返回或记录账号、业务正文。"""
    if not isinstance(payload, bytes) or len(payload) > 1024 * 1024:
        raise ValueError('消息格式或长度异常')
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError('解压消息过大')
    position = 0
    message_type = ''

    def read_varint():
        nonlocal position
        value = 0
        for shift in range(0, 70, 7):
            if position >= len(data):
                raise ValueError('截断的 varint')
            byte = data[position]
            position += 1
            value |= (byte & 127) << shift
            if byte < 128:
                if shift == 63 and byte > 1:
                    raise ValueError('varint 溢出')
                return value
        raise ValueError('varint 过长')

    while position < len(data):
        tag = read_varint()
        field, wire = tag >> 3, tag & 7
        if not field or field >= 1 << 29:
            raise ValueError('字段编号异常')
        if wire == 0:
            read_varint()
            continue
        if wire == 2:
            length = read_varint()
        elif wire in (1, 5):
            length = 8 if wire == 1 else 4
        else:
            raise ValueError('不支持的 protobuf wire type')
        end = position + length
        if end > len(data):
            raise ValueError('截断的字段')
        if field == 8 and wire == 2:
            message_type = data[position:end].decode('utf-8')
        position = end
    return message_type


def pulse_proxy_options(proxy_url):
    """显式 HTTP 代理配置；无效地址抛异常，不静默改为直连。"""
    if not proxy_url:
        return {}
    proxy = urlsplit(proxy_url if '://' in proxy_url else 'http://' + proxy_url)
    if proxy.scheme != 'http' or not proxy.hostname or not proxy.port:
        raise ValueError('不支持或无效的 HTTP 代理')
    options = {'http_proxy_host': proxy.hostname, 'http_proxy_port': proxy.port,
               'proxy_type': 'http'}
    if proxy.username is not None:
        options['http_proxy_auth'] = (unquote(proxy.username), unquote(proxy.password or ''))
    return options

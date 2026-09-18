import time
import json
import hashlib
import requests
from datetime import datetime
from .redis_client import get_client, k

r = get_client()
# ─────────────────────────────────────────────
#  配置
# ─────────────────────────────────────────────
REDIS_KEY = "crawler:proxy:ip:current"

ORDER_ID = "17466"
API_SECRET = "vD6OM5Evcp"  # sign 字段的生成依赖，具体算法需要按你们接口文档确认

REFRESH_AHEAD_SECONDS = 5  # 过期前多少秒提前刷新


def _generate_sign(params: dict) -> str:
    """
    生成接口签名。这里只是占位示例（常见做法是把参数按 key 排序后拼接 + 密钥再做 md5），
    具体算法必须按你们接口文档实现，否则会签名校验失败。
    """
    return hashlib.md5(f'{ORDER_ID}{params["time"]}{API_SECRET}'.encode('utf-8')).hexdigest()


def _fetch_new_ip() -> dict:
    """请求接口获取一个新的代理 IP，返回解析后的 ip 信息 dict"""
    now = int(time.time())
    params = {
        "orderId": ORDER_ID,
        "dataType": 0,
        "unbindTime": 60,
        "num": 1,
        "time": now,
        "cid": -1,
        "pid": 2,  # -1 全国 2-上海
    }
    params["sign"] = _generate_sign(params)

    resp = requests.get(
        "https://forward-pools.vpsnb.net/api/getIp",
        params=params,
        timeout=10,
        proxies={'http': None, 'https': None},
    )
    resp.raise_for_status()
    result = resp.json()

    if result.get("code") != 0 or not result.get("data"):
        raise RuntimeError(f"获取代理 IP 失败: {result}")

    ip_info = result["data"][0]
    return ip_info


def _cache_ip(ip_info: dict) -> None:
    """把 IP 信息写入 Redis，并设置过期时间正好等于接口返回的 expireAt"""
    expire_at = datetime.strptime(ip_info["expireAt"], "%Y-%m-%d %H:%M:%S")
    ttl_seconds = int((expire_at - datetime.now()).total_seconds())

    if ttl_seconds <= 0:
        # 理论上不应该发生（刚拿到的新IP不该已经过期），防御性处理
        ttl_seconds = 60

    r.set(REDIS_KEY, json.dumps(ip_info), ex=ttl_seconds)


def get_proxy_ip() -> dict:
    """
    获取当前可用的代理 IP 信息。
    - Redis 里有缓存且距离过期还有 REFRESH_AHEAD_SECONDS 以上余量 → 直接返回缓存
    - 否则（缓存不存在 / 即将过期）→ 请求接口拿新 IP，写入 Redis，返回新值
    返回值格式: {"ip": "...", "port": ..., "expireAt": "..."}
    """
    cached_raw = r.get(REDIS_KEY)

    if cached_raw:
        cached = json.loads(cached_raw)
        expire_at = datetime.strptime(cached["expireAt"], "%Y-%m-%d %H:%M:%S")
        remaining = (expire_at - datetime.now()).total_seconds()

        if remaining > REFRESH_AHEAD_SECONDS:
            return cached
        # 否则说明快过期了，走下面重新获取的逻辑

    # 缓存不存在，或者已经临近过期，重新请求接口
    new_ip_info = _fetch_new_ip()
    _cache_ip(new_ip_info)
    return new_ip_info


def get_proxy_dict() -> dict:
    """直接返回可以传给 requests 的 proxies 参数"""
    ip_info = get_proxy_ip()
    proxy_url = f"http://{ip_info['ip']}:{ip_info['port']}"
    return {"http": proxy_url, "https": proxy_url}


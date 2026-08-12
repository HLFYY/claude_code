"""单例共享 Redis 连接，配置读自 config.py。

这里的 Redis 服务器版本是 5.0，没有 `EXPIRE ... NX` / `SET ... KEEPTTL`
（Redis 6.0+ 才有）。需要"保留 TTL"语义的代码都是手动实现的（先读 TTL，
再 SET，再把 TTL 重新 EXPIRE 回去），而不是用这两个参数。
这里的 redis-py 版本是 3.2.1，没有 `hset(key, mapping=...)`，要用 `hmset()`。
"""
from __future__ import annotations

import redis

from . import config

_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True,
        )
    return _client


def k(*parts: str) -> str:
    """拼接带命名空间的 key：k('account', platform, email) ->
    'crawler:account:<platform>:<email>'。"""
    return config.KEY_PREFIX + ":".join(str(p) for p in parts)

"""Single shared Redis connection, built from config.py."""
from __future__ import annotations

import redis

import config

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
    """Build a namespaced key: k('account', email) -> 'wkinfo:account:<email>'."""
    return config.KEY_PREFIX + ":".join(parts)

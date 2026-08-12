"""
按平台、账号、资源、动作类型划分的配额，用 Redis 计数器实现，TTL 本身就是
滚动窗口：`quota:{platform}:{account}:{resource}:{action}`。key 第一次
INCR 时设置 EXPIRE，之后这个 key（连同计数）到期自动消失——不需要另外写
重置任务。

"resource" 故意起了个通用的名字（不叫 "indexId"）——wkinfo 管它叫 indexId，
换一个平台可能叫别的名字；这个模块不关心这个字符串具体代表什么，只关心
配额是按它划分的。"action" 和限额/窗口数值也是同理：全部是平台策略，由
调用方（platforms/<name>/config.py）提供，这里永远不硬编码。

本地计数只是一个预测值，不是绝对真相——平台自己返回的"配额超限"响应才是
权威判断。平台一旦这么说了就立刻调用 mark_exhausted()，哪怕本地计数还没
到限额（这样即使因为手工测试或者这套池子被多处同时运行导致的计数偏差，
也不会对一个实际已经用满的账号反复发出注定失败的请求）。
"""
from __future__ import annotations

from .redis_client import get_client, k


def _quota_key(platform: str, account: str, resource: str, action: str) -> str:
    return k("quota", platform, account, resource, action)


def remaining(platform: str, account: str, resource: str, action: str, limit: int) -> dict:
    """返回 {"used": int, "limit": int, "ttl_seconds": int|None}（ttl_seconds
    为 None 表示当前还没有生效的窗口）。"""
    r = get_client()
    key = _quota_key(platform, account, resource, action)
    used = int(r.get(key) or 0)
    ttl = r.ttl(key)
    return {"used": used, "limit": limit, "ttl_seconds": ttl if ttl and ttl > 0 else None}


def has_quota(platform: str, account: str, resource: str, action: str, limit: int) -> bool:
    return remaining(platform, account, resource, action, limit)["used"] < limit


def try_consume(platform: str, account: str, resource: str, action: str, limit: int, window_seconds: int) -> bool:
    """如果还有配额就自增，返回本次是否被允许。这不是严格原子操作
    （先 get 再 incr），但调用方都是顺序调度的，实际上不构成问题。"""
    if not has_quota(platform, account, resource, action, limit):
        return False
    r = get_client()
    key = _quota_key(platform, account, resource, action)
    new_val = r.incr(key)
    if new_val == 1:
        r.expire(key, window_seconds)
    return True


def mark_exhausted(platform: str, account: str, resource: str, action: str, limit: int, window_seconds: int) -> None:
    """强制把这个 (account, resource, action) 标记为已达上限，比如平台自己
    刚返回了配额超限的响应之后。

    这里的 Redis 服务器是 5.0（没有 KEEPTTL，6.0 才加入），所以 TTL 的保留
    是手动做的：先读出来，SET（会清空 TTL），再手动把它重新设置回去。"""
    r = get_client()
    key = _quota_key(platform, account, resource, action)
    ttl = r.ttl(key)
    r.set(key, limit)
    r.expire(key, ttl if ttl and ttl > 0 else window_seconds)

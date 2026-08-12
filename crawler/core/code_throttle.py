"""
验证码发送节流，所有平台共用：同一个 (platform, identifier) 两次"发验证码"之间
必须间隔至少 min_interval_seconds，不够就打日志说明还要等多少秒，然后真的睡够，
避免撞上网站自己的分钟级发送频率限制（pkulaw 平台真实撞到过
{"error":"limit_minute"}，见 platforms/pkulaw/HANDOFF.md）。

用 Redis key 的 TTL 本身当"还要等多久"的计时器，跟 core/quota_tracker.py
"TTL 本身就是滚动窗口"是同一个思路：mark_sent() 发送成功后把 key 的 TTL 设成
min_interval_seconds；wait_before_send() 发送前检查这个 key 还在不在——还在
说明还没到冷却时间，用它剩下的 TTL 当"还要等多久"，睡完再返回；不在了说明已经
过了冷却期，立刻返回不睡。
"""
from __future__ import annotations

import time

from .logger import log
from .redis_client import get_client, k


def wait_before_send(platform: str, identifier: str, min_interval_seconds: int) -> None:
    """在"确定要真的调发送验证码接口"之前调用：如果距离上次给这个账号发验证码
    还没过 min_interval_seconds，打印日志说明还要等多少秒，然后真的 sleep 那么久
    再返回；已经过了冷却期就立刻返回，不打印、不睡。返回之后调用方应该紧接着真的
    去发验证码，然后调 mark_sent() 记录这次发送时间，形成"节流->发送->记录"的
    固定顺序。"""
    r = get_client()
    key = k("code_send_cooldown", platform, identifier)
    ttl = r.ttl(key)
    if ttl and ttl > 0:
        log(platform, f"{identifier} 距上次发验证码还没到 {min_interval_seconds} 秒冷却时间，"
                       f"还要等 {ttl} 秒才能再发验证码，等待中...")
        time.sleep(ttl)


def mark_sent(platform: str, identifier: str, min_interval_seconds: int) -> None:
    """记录这次发送验证码的时间，配合 wait_before_send 用。不管这次发送业务上
    成不成功（比如撞上了 limit_day）都应该调用——只要真的调用了发送接口，就会
    计入网站自己的分钟级频率限制，所以节流计时器也要跟着重新计时。"""
    r = get_client()
    key = k("code_send_cooldown", platform, identifier)
    r.set(key, str(time.time()))
    r.expire(key, min_interval_seconds)

"""
给定 (platform, resource, action, limit)，挑一个还有剩余配额的有效账号，
确保它已经通过该平台自己的登录函数登录，然后返回 (email, session)。这个
模块不了解 search/detail 的 HTTP 请求形状——那是各平台自己的
search_client.py 的事，它调用这里做账号选择，自己实现"配额超限就重试下一个
账号"的循环（从响应里识别"配额超限"是平台特定的业务逻辑，这个模块无权
也不应该知道）。

两个调度策略，都是平台无关的通用逻辑，所以放在这里而不是各平台目录下：

1. **优先选剩余配额多的账号**（而不是纯随机）——让账号池里各账号的使用量
   尽量平均，不会出现"总是先耗尽某一个账号，其他账号一直没怎么用"的情况。
   剩余配额相同时才随机打散，避免固定顺序。
2. **可选的单账号请求间隔冷却**（`min_interval_seconds`，默认 0 = 不启用，
   完全向后兼容——不传这个参数，行为跟以前一模一样）：账号一旦被这里选中
   派发出去，就进入冷却期，冷却期内不会再被选中，防止同一个账号被高频
   连续请求（这跟“每日总配额”是两个维度：配额管的是“今天还能用几次”，
   冷却管的是“刚用过，得歇一下”）。如果这一轮候选账号全部在冷却中（但
   都还有配额），不是立刻报错，而是等剩余冷却时间最短的那个解冻后再重试，
   最多等 `max_wait_seconds` 秒——超过这个等待上限才真的抛
   NoAccountAvailable。如果账号是真的没配额了（不是冷却，等多久也没用），
   直接报错，不做无意义的等待。
"""
from __future__ import annotations

import random
import time
from typing import Callable

from . import account_registry, quota_tracker
from .logger import log
from .redis_client import get_client, k


class NoAccountAvailable(RuntimeError):
    pass


def _cooldown_key(platform: str, resource: str, action: str, account: str) -> str:
    return k("dispatch_cooldown", platform, resource, action, account)


def _cooldown_ttl(platform: str, resource: str, action: str, account: str) -> int:
    ttl = get_client().ttl(_cooldown_key(platform, resource, action, account))
    return ttl if ttl and ttl > 0 else 0


def _mark_cooldown(platform: str, resource: str, action: str, account: str, min_interval_seconds: int) -> None:
    r = get_client()
    key = _cooldown_key(platform, resource, action, account)
    r.set(key, "1")
    r.expire(key, min_interval_seconds)


def wait_for_cooldown(platform: str, resource: str, action: str, account: str, min_interval_seconds: int) -> None:
    """给不经过 dispatch() 挑账号、而是调用方自己指定了固定账号发请求的路径用
    （比如 pkulaw 的 view_detail(identifier=...)——这条路径压根不涉及"从多个
    账号里选一个"，`dispatch()` 的"冷却中就跳过换一个"这套逻辑在这里不适用，
    因为根本没有"换一个"这个选项）。

    如果这个账号还在冷却中，原地睡到冷却结束再返回；不管冷不冷却，返回前都会
    重新标记冷却（下一次调用又要等 min_interval_seconds），保证"同一个账号
    两次真实请求之间至少间隔这么久"这条规则不会被绕过。`min_interval_seconds
    <= 0` 直接跳过，不冷却（向后兼容，没有主动传这个值的调用方行为不变）。"""
    if min_interval_seconds <= 0:
        return
    ttl = _cooldown_ttl(platform, resource, action, account)
    if ttl > 0:
        # log(platform, f"{resource}/{action}/{account} 还在冷却中，等 {ttl} 秒后再请求...")
        time.sleep(ttl)
    _mark_cooldown(platform, resource, action, account, min_interval_seconds)


def dispatch(platform: str, resource: str, action: str, limit: int,
             login_fn: Callable[[str, str], tuple],
             min_interval_seconds: int = 0, max_wait_seconds: int = 60):
    """login_fn(email, password) -> (session, profile)，即该平台自己的
    login.get_session 等价物。返回一个还有剩余配额、且已登录的账号
    (email, session)。

    `min_interval_seconds` 不传或传 0 就是老行为（不冷却，随时可能连续选中
    同一账号）。传大于 0 的值就会启用"选中后冷却 N 秒"，见模块开头的说明。

    如果池子里没有账号能满足这次请求（真的没配额，不是在冷却），抛出
    NoAccountAvailable；如果账号都在冷却但等了 max_wait_seconds 秒仍没有
    解冻，也抛这个异常——调用方不应该在拿到这个异常后立刻紧接着死循环重试。
    """
    waited = 0
    while True:
        ready: list[tuple[int, str, dict]] = []  # (remaining_quota, email, account)
        cooling_ttls: list[int] = []

        emails = account_registry.list_active_emails(platform)
        random.shuffle(emails)  # 剩余配额相同时靠这个打散顺序，不固定先后
        for email in emails:
            account = account_registry.get_account(platform, email)
            if not account or account.get("status") != account_registry.STATUS_ACTIVE:
                continue
            info = quota_tracker.remaining(platform, email, resource, action, limit)
            remaining_quota = limit - info["used"]
            if remaining_quota <= 0:
                continue
            ttl = _cooldown_ttl(platform, resource, action, email) if min_interval_seconds > 0 else 0
            if ttl > 0:
                cooling_ttls.append(ttl)
            else:
                ready.append((remaining_quota, email, account))

        # 剩余配额多的优先——让各账号用量尽量平均，不是每次都挑到同一个。
        ready.sort(key=lambda c: c[0], reverse=True)

        for _remaining_quota, email, account in ready:
            try:
                session, _profile = login_fn(email, account["password"])
            except RuntimeError:
                # 比如触发了并发session限制，或者账号被服务端封了——
                # 这次跳过它，试下一个候选账号。
                continue
            if min_interval_seconds > 0:
                _mark_cooldown(platform, resource, action, email, min_interval_seconds)
            return email, session

        if not cooling_ttls:
            raise NoAccountAvailable(f"no account with remaining {action} quota for {resource} on {platform}")

        if waited >= max_wait_seconds:
            raise NoAccountAvailable(
                f"{platform}/{resource}/{action}: 候选账号都在冷却期内，"
                f"等了 {waited} 秒仍没有账号解冻（上限 {max_wait_seconds} 秒）"
            )
        sleep_for = min(min(cooling_ttls), max_wait_seconds - waited)
        log(platform, f"{resource}/{action} 候选账号都在冷却中，"
                       f"等 {sleep_for} 秒后重试（已等 {waited} 秒，上限 {max_wait_seconds} 秒）...")
        time.sleep(sleep_for)
        waited += sleep_for

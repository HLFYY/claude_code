"""
账号存储在 Redis 里，按 platform 分区：

  account:{platform}:{email}          Hash —— 账号的所有字段
  accounts:by_expiry:{platform}       ZSET —— score=expires_at（unix时间戳），member=email
                                       用 ZRANGEBYSCORE 就能低成本地查"哪些账号
                                       还有效"，不用扫描每个账号的 Hash。

试用期多长、账号还携带哪些字段，都是平台策略——这个模块不关心也不需要知道。
调用方（platforms/<name>/registration_worker.py）自己决定要存哪些字段，
连同一个明确的 expires_at 一起传进来。
"""
from __future__ import annotations

import time

from . import proxy_pool
from .redis_client import get_client, k

STATUS_ACTIVE = "active"
STATUS_EXPIRED = "expired"
STATUS_BANNED = "banned"  # 例如服务端拒绝了这个账号的登录/操作


def save_account(platform: str, email: str, expires_at: float, fields: dict, proxy_id: str = "") -> dict:
    r = get_client()
    now = time.time()
    record = dict(fields)
    record.update({
        "email": email,
        "proxyId": proxy_id,  # 仅作记录用；真正的绑定关系由 proxy_pool.py 管理
        "created_at": now,
        "expires_at": expires_at,
        "status": STATUS_ACTIVE,
    })
    r.hmset(k("account", platform, email), record)
    r.zadd(k("accounts", "by_expiry", platform), {email: expires_at})
    return record


def get_account(platform: str, email: str) -> dict | None:
    data = get_client().hgetall(k("account", platform, email))
    return data or None


def set_status(platform: str, email: str, status: str) -> None:
    get_client().hset(k("account", platform, email), "status", status)


def update_expiry(platform: str, email: str, expires_at: float) -> None:
    """覆盖存储的过期时间，比如拿到了平台自己权威的试用到期值之后
    （比自己算出来的估算值更可信）。"""
    r = get_client()
    r.hset(k("account", platform, email), "expires_at", expires_at)
    r.zadd(k("accounts", "by_expiry", platform), {email: expires_at})


def update_proxy_id(platform: str, email: str, proxy_id: str) -> None:
    """账号记录里的 `proxyId` 字段仅作记录用（真正的绑定关系由 proxy_pool.py
    管理）——比如账号是补登进来的老数据，登记时还没绑代理，后来才补绑，用
    这个同步一下这个字段，避免跟 proxy_pool 里的真实绑定关系不一致。"""
    get_client().hset(k("account", platform, email), "proxyId", proxy_id)


def delete_account(platform: str, email: str) -> None:
    r = get_client()
    r.delete(k("account", platform, email))
    r.zrem(k("accounts", "by_expiry", platform), email)


def list_active_emails(platform: str) -> list[str]:
    """还没过期的账号邮箱列表，按 ZSET 里的过期时间升序排列。"""
    r = get_client()
    now = time.time()
    return r.zrangebyscore(k("accounts", "by_expiry", platform), now, "+inf")


def list_expired_emails(platform: str) -> list[str]:
    r = get_client()
    now = time.time()
    return r.zrangebyscore(k("accounts", "by_expiry", platform), "-inf", now)


def list_all_accounts(platform: str) -> list[dict]:
    r = get_client()
    emails = r.zrange(k("accounts", "by_expiry", platform), 0, -1)
    return [a for a in (get_account(platform, e) for e in emails) if a]


def sweep(platform: str) -> dict:
    """周期性维护任务（daily 或每几天跑一次即可，不是每次挑账号都跑）——
    为什么代理"绑定账号数"不是实时扣减的，见 proxy_pool.py 里的说明。

    1. 把新过期的账号（状态还是 ACTIVE，但 expires_at 已经过了）标记为 EXPIRED。
    2. 对所有非 ACTIVE 状态的账号——刚过期的、之前已经过期的，或者被封的
       BANNED（不管什么原因）——如果它还占着 proxy_pool 的绑定槽位，就释放掉，
       这样代理的负载计数才只反映当前有效账号，槽位也能腾给新注册的账号用。

    这里故意扫描这个平台的所有账号（不只是按时间过期的那些），这样即使
    BANNED 账号并不是因为到期被封的，它占用的代理槽位也会一并释放。
    返回 {"expired": n, "proxy_freed": n}。
    """
    now = time.time()
    expired_count = 0
    freed_count = 0
    for account in list_all_accounts(platform):
        email = account["email"]
        if account.get("status") == STATUS_ACTIVE and float(account.get("expires_at", 0)) < now:
            set_status(platform, email, STATUS_EXPIRED)
            account["status"] = STATUS_EXPIRED
            expired_count += 1
        if account["status"] != STATUS_ACTIVE and proxy_pool.get_account_proxy_id(platform, email):
            proxy_pool.unbind_account(platform, email)
            freed_count += 1
    return {"expired": expired_count, "proxy_freed": freed_count}

"""
Account storage in Redis:

  account:{email}      Hash -- all the account's fields
  accounts:by_expiry    ZSET -- score=expires_at (unix ts), member=email
                         lets us cheaply ask "which accounts are still valid"
                         via ZRANGEBYSCORE, without scanning every account hash.

Trial accounts are only good for ACCOUNT_TRIAL_SECONDS (3 days) -- expiry is
computed at registration time and stored, not re-derived, so it survives
config changes to the trial length after the fact.
"""
from __future__ import annotations

import time

import config
from redis_client import get_client, k

STATUS_ACTIVE = "active"
STATUS_EXPIRED = "expired"
STATUS_BANNED = "banned"  # e.g. server rejected login/actions for this account


def save_account(email: str, telephone: str, password: str, company_name: str,
                  province: str, post_id: str, last_name: str, first_name: str,
                  proxy_id: str = "") -> dict:
    r = get_client()
    now = time.time()
    expires_at = now + config.ACCOUNT_TRIAL_SECONDS
    record = {
        "email": email,
        "telephone": telephone,
        "password": password,
        "companyName": company_name,
        "province": province,
        "postId": post_id,
        "lastName": last_name,
        "firstName": first_name,
        "proxyId": proxy_id,  # informational copy; proxy_pool.py owns the real binding
        "created_at": now,
        "expires_at": expires_at,
        "status": STATUS_ACTIVE,
    }
    r.hmset(k("account", email), record)
    r.zadd(k("accounts", "by_expiry"), {email: expires_at})
    return record


def get_account(email: str) -> dict | None:
    r = get_client()
    data = r.hgetall(k("account", email))
    return data or None


def set_status(email: str, status: str) -> None:
    get_client().hset(k("account", email), "status", status)


def update_expiry(email: str, expires_at: float) -> None:
    """Overwrite the stored expiry, e.g. once the server's own authoritative
    trial endDate is known (see registration_worker.py)."""
    r = get_client()
    r.hset(k("account", email), "expires_at", expires_at)
    r.zadd(k("accounts", "by_expiry"), {email: expires_at})


def delete_account(email: str) -> None:
    r = get_client()
    r.delete(k("account", email))
    r.zrem(k("accounts", "by_expiry"), email)


def list_active_emails() -> list[str]:
    """Emails whose trial hasn't expired yet, in ZSET (expiry-ascending) order."""
    r = get_client()
    now = time.time()
    return r.zrangebyscore(k("accounts", "by_expiry"), now, "+inf")


def list_expired_emails() -> list[str]:
    r = get_client()
    now = time.time()
    return r.zrangebyscore(k("accounts", "by_expiry"), "-inf", now)


def sweep_expired() -> int:
    """Mark accounts past their trial as expired (status only, keeps the
    record for audit). Returns how many were newly marked."""
    r = get_client()
    count = 0
    for email in list_expired_emails():
        account = get_account(email)
        if account and account.get("status") == STATUS_ACTIVE:
            set_status(email, STATUS_EXPIRED)
            count += 1
    return count


def list_all_accounts() -> list[dict]:
    r = get_client()
    emails = r.zrange(k("accounts", "by_expiry"), 0, -1)
    return [a for a in (get_account(e) for e in emails) if a]

"""
Per-account, per-indexId, per-action-type quota, backed by Redis counters
with a TTL that IS the rolling window: `quota:{email}:{indexId}:{action}`.
First INCR on a key sets its EXPIRE, so the key (and the count) evaporates on
its own -- no separate reset job needed. Confirmed 2026-07-29: this really is
a rolling 24h window from first-hit, not a calendar-day reset (see config.py).

The local count is a prediction, not ground truth -- the server's own
quota-exceeded response is authoritative. Call mark_exhausted() the moment
the server says so, even if the local counter hadn't reached the limit yet
(keeps drift from manual testing, or from this pool running from multiple
places, from causing repeated wasted requests against an account that's
actually already capped).
"""
from __future__ import annotations

import config
from redis_client import get_client, k

SEARCH = "search"
DETAIL = "detail"

_LIMITS = {
    SEARCH: config.SEARCH_LIMIT_PER_INDEX,
    DETAIL: config.DETAIL_LIMIT_PER_INDEX,
}


def _window_seconds() -> int:
    return config.QUOTA_WINDOW_SECONDS


def _quota_key(email: str, index_id: str, action: str) -> str:
    assert action in _LIMITS, f"unknown action type: {action}"
    return k("quota", email, index_id, action)


def remaining(email: str, index_id: str, action: str) -> dict:
    """{"used": int, "limit": int, "ttl_seconds": int|None (None = no active window yet)}"""
    r = get_client()
    key = _quota_key(email, index_id, action)
    used = int(r.get(key) or 0)
    ttl = r.ttl(key)
    return {"used": used, "limit": _LIMITS[action], "ttl_seconds": ttl if ttl and ttl > 0 else None}


def has_quota(email: str, index_id: str, action: str) -> bool:
    return remaining(email, index_id, action)["used"] < _LIMITS[action]


def try_consume(email: str, index_id: str, action: str) -> bool:
    """Increment if there's quota left; returns whether it was allowed.
    Not perfectly atomic (get-then-incr), but this pool dispatches
    sequentially so that's not a real concern here."""
    if not has_quota(email, index_id, action):
        return False
    r = get_client()
    key = _quota_key(email, index_id, action)
    new_val = r.incr(key)
    if new_val == 1:
        r.expire(key, _window_seconds())
    return True


def mark_exhausted(email: str, index_id: str, action: str) -> None:
    """Force this (account, indexId, action) to read as at-limit, e.g. right
    after the server itself returns the quota-exceeded message.

    Redis server here is 5.0 (no KEEPTTL, added in 6.0), so TTL preservation
    is done manually: read it, SET (which clears it), then re-apply it."""
    r = get_client()
    key = _quota_key(email, index_id, action)
    ttl = r.ttl(key)
    r.set(key, _LIMITS[action])
    r.expire(key, ttl if ttl and ttl > 0 else _window_seconds())

"""
Fixed proxy pool with per-platform account binding.

Why an account keeps its proxy forever once assigned (see HANDOFF.md "IP
代理池"): the login session cookie is used across many requests over the
account's whole 3-day life, and switching IP mid-session is both a red flag
and a practical risk of breaking the WAF cookie handshake. So the binding
happens once, at registration, and every later login/search/detail call for
that account reuses the same proxy automatically (wired into login.py).

Redis layout, everything keyed by `platform` (config.PLATFORM) so the same
physical proxy list can be shared across platforms later with independently
tracked bound-account counts and limits per platform:

  proxy:{proxy_id}                    Hash -- host/port/username/password
                                       (NOT platform-scoped: the proxy resource
                                       itself is shared; only its *binding* is
                                       platform-scoped)
  proxies_by_load:{platform}          ZSET -- score=bound account count,
                                       member=proxy_id. Picking the
                                       least-loaded proxy under the cap is one
                                       ZRANGEBYSCORE call.
  account_proxy:{platform}:{email}    String -- which proxy_id this account
                                       on this platform is bound to.

There's no fallback to a direct (proxyless) connection if the pool is empty
or every proxy is at its cap -- pick_for_new_account() returns None and
callers (registration_worker.py) are expected to fail loudly rather than
silently register an account with no consistent IP, which would quietly
break the whole point of this module.
"""
from __future__ import annotations

import config
from redis_client import get_client, k


def add_proxy(proxy_id: str, host: str, port: int, username: str | None = None, password: str | None = None) -> None:
    """Register a proxy resource (idempotent, overwrites if proxy_id already
    exists). Doesn't bind it to any platform/account by itself."""
    get_client().hmset(k("proxy", proxy_id), {
        "host": host, "port": port,
        "username": username or "", "password": password or "",
    })


def remove_proxy(proxy_id: str) -> None:
    get_client().delete(k("proxy", proxy_id))


def get_proxy(proxy_id: str) -> dict | None:
    data = get_client().hgetall(k("proxy", proxy_id))
    return data or None


def list_proxy_ids() -> list[str]:
    r = get_client()
    prefix = k("proxy", "")
    return [key[len(prefix):] for key in r.keys(prefix + "*")]


def requests_proxies(proxy_id: str) -> dict:
    """{"http": "...", "https": "..."} ready for requests' `proxies=` kwarg."""
    proxy = get_proxy(proxy_id)
    if not proxy:
        raise KeyError(f"unknown proxy: {proxy_id}")
    auth = f"{proxy['username']}:{proxy['password']}@" if proxy.get("username") else ""
    url = f"http://{auth}{proxy['host']}:{proxy['port']}"
    return {"http": url, "https": url}


def _load_key(platform: str) -> str:
    return k("proxies_by_load", platform)


def _account_key(platform: str, email: str) -> str:
    return k("account_proxy", platform, email)


def _ensure_ranked(platform: str) -> None:
    """Every known proxy participates in this platform's load ranking, even
    ones never bound here yet (score starts at 0)."""
    r = get_client()
    load_key = _load_key(platform)
    for proxy_id in list_proxy_ids():
        if r.zscore(load_key, proxy_id) is None:
            r.zadd(load_key, {proxy_id: 0})


def pick_for_new_account(platform: str = config.PLATFORM, max_accounts_per_ip: int = config.MAX_ACCOUNTS_PER_IP) -> str | None:
    """Least-loaded proxy on this platform that's still under the cap, or
    None if the pool is empty or every proxy is full."""
    _ensure_ranked(platform)
    r = get_client()
    candidates = r.zrangebyscore(_load_key(platform), "-inf", max_accounts_per_ip - 1, start=0, num=1)
    return candidates[0] if candidates else None


def bind_account(platform: str, email: str, proxy_id: str) -> None:
    r = get_client()
    r.set(_account_key(platform, email), proxy_id)
    r.zincrby(_load_key(platform), 1, proxy_id)


def get_account_proxy_id(platform: str, email: str) -> str | None:
    return get_client().get(_account_key(platform, email))


def unbind_account(platform: str, email: str) -> None:
    """Free this account's slot, e.g. if the account gets retired/banned."""
    r = get_client()
    proxy_id = get_account_proxy_id(platform, email)
    if proxy_id:
        r.zincrby(_load_key(platform), -1, proxy_id)
        r.delete(_account_key(platform, email))


def load_report(platform: str = config.PLATFORM) -> list[dict]:
    """[{"proxy_id":..., "bound_accounts": n}, ...] for visibility into how
    evenly the pool is loaded."""
    r = get_client()
    pairs = r.zrange(_load_key(platform), 0, -1, withscores=True)
    return [{"proxy_id": pid, "bound_accounts": int(score)} for pid, score in pairs]

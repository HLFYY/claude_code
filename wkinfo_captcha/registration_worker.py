"""
Orchestrates one account registration: random profile fields (everything
except telephone/email, which must be real and are supplied by the caller)
+ captcha_client's full_registration_flow (captcha solve -> SMS -> submit)
+ account_registry.save_account().

After a successful registration, logs in once for real to read the account's
actual trial productsDetailList[].endDate from the server (milliseconds) --
that's the authoritative expiry, more trustworthy than computing
"registered_at + 3 days" ourselves (registration and first login aren't
necessarily the same instant, and it's one less assumption to keep in sync
with whatever the site actually configures per account).

Proxy binding happens here, first, before anything else -- a proxy is picked
from the pool and used for the *entire* registration flow (captcha included),
then bound to the account as soon as registration succeeds, so every later
login (login.py looks the binding up automatically) uses the same IP the
account was born on. If the pool has no room, this refuses to register the
account at all rather than silently proceeding without a consistent IP.
"""
from __future__ import annotations

import random

import requests

import account_registry
import config
import login
import proxy_pool
from captcha_client import WkinfoCaptcha
from random_profile import random_registration_fields


def _real_expiry_from_profile(profile: dict) -> float | None:
    """profile["productsDetailList"][0]["endDate"] is a ms-epoch timestamp
    for this trial's expiry, straight from the server. Returns seconds, or
    None if the shape isn't what's expected (caller falls back to the
    registration-time + ACCOUNT_TRIAL_SECONDS estimate)."""
    try:
        return profile["productsDetailList"][0]["endDate"] / 1000
    except (KeyError, IndexError, TypeError):
        return None


def register_one(telephone: str, user_email: str, captcha_type: str | None = None) -> dict:
    """Registers a new trial account with the given real telephone/email and
    everything else randomized. Returns the saved account_registry record.
    Raises on any failure (captcha, registration, the confirming login, or
    if the proxy pool has no room for a new account).
    """
    proxy_id = proxy_pool.pick_for_new_account()
    if proxy_id is None:
        raise RuntimeError(
            "no proxy available: pool is empty or every proxy is at "
            f"MAX_ACCOUNTS_PER_IP={config.MAX_ACCOUNTS_PER_IP} for platform={config.PLATFORM}"
        )
    proxied_session = requests.Session()
    proxied_session.proxies = proxy_pool.requests_proxies(proxy_id)

    fields = random_registration_fields()
    captcha_type = captcha_type or random.choice(["blockPuzzle", "clickWord"])

    client = WkinfoCaptcha(captcha_type=captcha_type, session=proxied_session)
    result = client.full_registration_flow(
        telephone=telephone,
        user_email=user_email,
        password=fields["password"],
        company_name=fields["company_name"],
        province=fields["province"],
        last_name=fields["last_name"],
        first_name=fields["first_name"],
        post_id=fields["post_id"],
    )
    register_resp = result["register"]
    if register_resp.get("code") not in (None, "0000") and "code" in register_resp:
        # e.g. E_035_003 wrong sms code, E_000_003 captcha verification failed, etc.
        raise RuntimeError(f"registration failed: {register_resp}")

    record = account_registry.save_account(
        email=user_email,
        telephone=telephone,
        password=fields["password"],
        company_name=fields["company_name"],
        province=fields["province"],
        post_id=fields["post_id"],
        last_name=fields["last_name"],
        first_name=fields["first_name"],
        proxy_id=proxy_id,
    )
    # Bind BEFORE the confirming login below -- login.py looks this binding
    # up to decide which proxy to route through.
    proxy_pool.bind_account(config.PLATFORM, user_email, proxy_id)

    # Confirm login works and pick up the server's real trial expiry.
    _, profile = login.get_session(user_email, fields["password"], force=True)
    real_expiry = _real_expiry_from_profile(profile)
    if real_expiry:
        account_registry.update_expiry(user_email, real_expiry)
        record["expires_at"] = real_expiry

    return record


if __name__ == "__main__":
    tel = input("真实手机号: ").strip()
    email = input("真实邮箱: ").strip()
    account = register_one(tel, email)
    print("registered:", account)

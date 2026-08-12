"""
Given (indexId, action), picks an active account that still has quota for it,
makes sure it's logged in, and hands back a ready-to-use (email, session).
Doesn't know anything about search/detail HTTP shapes -- that's search_client.py.
"""
from __future__ import annotations

import random

import account_registry
import login
import quota_tracker

STATUS_ACTIVE = account_registry.STATUS_ACTIVE


class NoAccountAvailable(RuntimeError):
    pass


def _eligible_accounts(index_id: str, action: str) -> list[str]:
    emails = account_registry.list_active_emails()
    random.shuffle(emails)  # spread load instead of always hammering the same one first
    eligible = []
    for email in emails:
        account = account_registry.get_account(email)
        if not account or account.get("status") != STATUS_ACTIVE:
            continue
        if quota_tracker.has_quota(email, index_id, action):
            eligible.append(email)
    return eligible


def dispatch(index_id: str, action: str):
    """Returns (email, session) for an account with quota remaining, logged in.
    Raises NoAccountAvailable if nothing in the pool can serve this request
    right now (caller should NOT keep retrying in a tight loop -- it means
    every active account is genuinely out of quota for this column today)."""
    for email in _eligible_accounts(index_id, action):
        account = account_registry.get_account(email)
        try:
            session, _profile = login.get_session(email, account["password"])
        except RuntimeError:
            # e.g. C_002_001 concurrent-session, or the account got banned
            # server-side -- skip it for this call, try the next candidate.
            continue
        return email, session
    raise NoAccountAvailable(f"no account with remaining {action} quota for {index_id}")

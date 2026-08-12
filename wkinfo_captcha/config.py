"""
Central config for the multi-account pool: Redis connection + Mongo connection
+ quota limits + key naming. Kept isolated on purpose so swapping a storage
backend or tuning limits later only touches this one file.
"""
from __future__ import annotations

import os
from pathlib import Path

# --- Redis (live operational state: accounts, sessions, quota counters) ---
REDIS_HOST = os.environ.get("WKINFO_REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("WKINFO_REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("WKINFO_REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("WKINFO_REDIS_PASSWORD") or None

# Namespace every key under this so the pool can share a Redis instance with
# other stuff without key collisions.
KEY_PREFIX = "wkinfo:"

# --- MongoDB (collected document data -- separate from Redis's live state
# on purpose: this is the actual crawled content, grows large, gets queried
# differently, no reason to force it through Redis) ---
MONGO_URI = os.environ.get("WKINFO_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.environ.get("WKINFO_MONGO_DB", "wkinfo")
MONGO_DOCUMENTS_COLLECTION = "documents"

# --- Quota (per account, per indexId, per action type) ---
# Confirmed 2026-07-29: rolling 24h from the moment the limit was first hit,
# NOT a calendar-day reset (an account blocked ~13:00 the day before was still
# blocked at 10:00 the next day -- >21h but <24h, consistent with rolling,
# not with a midnight reset). The site's own error message always says "24
# 小时之后" regardless of actual remaining time -- that's just static text,
# it doesn't reflect a real countdown, so don't try to parse a real remaining
# time out of the message itself.
QUOTA_WINDOW_SECONDS = 24 * 60 * 60
SEARCH_LIMIT_PER_INDEX = 40
DETAIL_LIMIT_PER_INDEX = 20

# The 6 columns/indexIds this pool operates over (confirmed via browser capture).
INDEX_IDS = [
    "law.legislation",              # 法律法规
    "law.case",                     # 裁判文书
    "law.administrativeSupervision",# 行政处罚
    "law.procuratorialCase",        # 检察案例
    "law.editorial",                # 实务文章
    "law.utilityWriting",           # 实用文书
]

# --- Trial account lifetime ---
ACCOUNT_TRIAL_SECONDS = 3 * 24 * 60 * 60

# --- Proxy pool ---
# PLATFORM namespaces every proxy-binding key (see proxy_pool.py) so the same
# physical proxy can be shared across platforms later with independently
# tracked/limited bound-account counts per platform -- set once here now
# rather than needing a rework when a second platform shows up.
PLATFORM = "wkinfo"
MAX_ACCOUNTS_PER_IP = int(os.environ.get("WKINFO_MAX_ACCOUNTS_PER_IP", 5))

# --- Request log (JSONL, NOT in Redis -- see HANDOFF.md for why) ---
LOG_DIR = Path(__file__).parent / "logs"
REQUEST_LOG_PATH = LOG_DIR / "requests.jsonl"

"""
Append-only request log, deliberately NOT in Redis (Redis holds live
operational state -- accounts, sessions, quota counters; this is history for
later analysis, a local file suits that better and doesn't bloat Redis).

One JSON object per line (JSONL) in config.REQUEST_LOG_PATH. stats_by_day()
aggregates it into per-day/per-account/per-indexId success/fail counts --
this is the "统计每天各账号请求次数、成功失败数量" piece.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import config


def log_request(email: str, index_id: str, action: str, ok: bool, message: str = "") -> None:
    config.LOG_DIR.mkdir(exist_ok=True)
    entry = {
        "ts": time.time(),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "email": email,
        "indexId": index_id,
        "action": action,
        "ok": ok,
        "message": message,
    }
    with open(config.REQUEST_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def iter_log_entries():
    if not config.REQUEST_LOG_PATH.exists():
        return
    with open(config.REQUEST_LOG_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def stats_by_day() -> dict:
    """{date: {email: {"total": n, "ok": n, "fail": n}}}"""
    stats: dict = defaultdict(lambda: defaultdict(lambda: {"total": 0, "ok": 0, "fail": 0}))
    for entry in iter_log_entries():
        bucket = stats[entry["date"]][entry["email"]]
        bucket["total"] += 1
        bucket["ok" if entry["ok"] else "fail"] += 1
    return {date: dict(accounts) for date, accounts in stats.items()}


if __name__ == "__main__":
    import pprint
    pprint.pprint(stats_by_day())

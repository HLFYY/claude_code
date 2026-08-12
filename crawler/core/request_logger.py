"""
只追加写入的请求日志，每个平台一个 JSONL 文件（config.LOG_ROOT /
{platform}/requests.jsonl）——故意不放 Redis（Redis 存的是实时运行状态；
这是给后续分析用的历史数据，本地文件更合适，也不会把 Redis 撑大）。

stats_by_day() 把某个平台的日志聚合成按天/按账号的成功/失败次数统计——
"统计每天各账号请求次数、成功失败数量"。
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone

from . import config


def _log_path(platform: str):
    path = config.LOG_ROOT / platform
    path.mkdir(parents=True, exist_ok=True)
    return path / "requests.jsonl"


def log_request(platform: str, account: str, resource: str, action: str, ok: bool, message: str = "") -> None:
    entry = {
        "ts": time.time(),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "account": account,
        "resource": resource,
        "action": action,
        "ok": ok,
        "message": message,
    }
    with open(_log_path(platform), "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def iter_log_entries(platform: str):
    path = _log_path(platform)
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def stats_by_day(platform: str) -> dict:
    """返回 {date: {account: {"total": n, "ok": n, "fail": n}}}"""
    stats: dict = defaultdict(lambda: defaultdict(lambda: {"total": 0, "ok": 0, "fail": 0}))
    for entry in iter_log_entries(platform):
        bucket = stats[entry["date"]][entry["account"]]
        bucket["total"] += 1
        bucket["ok" if entry["ok"] else "fail"] += 1
    return {date: dict(accounts) for date, accounts in stats.items()}

"""
周期性维护任务——建议每天或每几天跑一次（比如用 cron），不是每次挑账号时
都跑：

    cd crawler && /path/to/python -m platforms.wkinfo.maintenance

把已经过了试用期的账号标记为 EXPIRED，并释放它们占用的 proxy_pool 绑定，
这样代理的"绑定账号数"才只反映当前有效账号（为什么这个不能实时做，
见 core/proxy_pool.py 和 core/account_registry.py 里的说明）。
"""
from __future__ import annotations

from core import account_registry, proxy_pool

from . import config

if __name__ == "__main__":
    result = account_registry.sweep(config.PLATFORM)
    print(f"swept {config.PLATFORM}: {result['expired']} newly expired, {result['proxy_freed']} proxy slots freed")
    print("current proxy load:")
    for row in proxy_pool.load_report(config.PLATFORM):
        print(f"  {row['proxy_id']}: {row['bound_accounts']} bound accounts")

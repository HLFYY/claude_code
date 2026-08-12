"""
批量检查各平台账号的登录状态：session 缓存还有效就跳过（不发登录请求），失效了
就用账号记录里存的密码自动重新登录一次，最后打印一份汇总报告。从 crawler/ 目录
直接跑：

    cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
    /Users/houjie/venv/python3-forcrawl/bin/python check_accounts.py              # 检查全部平台
    /Users/houjie/venv/python3-forcrawl/bin/python check_accounts.py --platform wkinfo
    /Users/houjie/venv/python3-forcrawl/bin/python check_accounts.py --platform pkulaw
    /Users/houjie/venv/python3-forcrawl/bin/python check_accounts.py --force      # 不管缓存有没有效，强制全部重新登录一次

账号来源是 core.account_registry.list_all_accounts(platform)——每个平台自己的
login.py 已经实现了"缓存的 session 还有效就直接用"（wkinfo/pkulaw 都是拿缓存的
cookie 发一个真实请求验证，不是只看本地 TTL），这里复用的就是这个检查，不用重新
发明。

不是 ACTIVE 状态的账号（过期/被封）直接跳过，不检查也不重新登录——道理跟
core.scheduler.dispatch() 一样，这些账号本来就不该被派发使用。

账号记录里没有存密码的（pkulaw 特有：验证码登录的账号本来就没有密码，见
platforms/pkulaw/login.py 的说明），失效了也没法自动重新登录（需要真人读一次
短信/邮箱验证码）——这种情况只报告"需要人工"，不会卡在这里等终端输入，不然批量
检查会被第一个这样的账号卡住。
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from core import account_registry

ACTION_LABELS = {
    "reused": "有效，直接复用",
    "relogged_in": "已失效，重新登录成功",
    "force_relogged_in": "强制重新登录成功",
    "failed": "已失效，重新登录失败",
    "skipped_inactive": "跳过（账号状态非 active）",
    "skipped_no_password": "跳过（没有存密码，需人工验证码登录）",
}


@dataclass
class CheckResult:
    platform: str
    account: str
    action: str  # ACTION_LABELS 的 key 之一
    detail: str = ""


def check_wkinfo(force: bool = False) -> list[CheckResult]:
    from platforms.wkinfo import config, login

    results = []
    for account in account_registry.list_all_accounts(config.PLATFORM):
        email = account["email"]
        if account.get("status") != account_registry.STATUS_ACTIVE:
            results.append(CheckResult(config.PLATFORM, email, "skipped_inactive",
                                        f"status={account.get('status')}"))
            continue

        if not force and login.load_cached_session(email) is not None:
            results.append(CheckResult(config.PLATFORM, email, "reused"))
            continue

        password = account.get("password")
        if not password:
            results.append(CheckResult(config.PLATFORM, email, "skipped_no_password"))
            continue

        try:
            login.get_session(email, password, force=True)
            results.append(CheckResult(config.PLATFORM, email, "force_relogged_in" if force else "relogged_in"))
        except Exception as e:  # noqa: BLE001 -- 一个账号登录失败不该中断整批检查
            results.append(CheckResult(config.PLATFORM, email, "failed", str(e)))

    return results


def check_pkulaw(force: bool = False) -> list[CheckResult]:
    from platforms.pkulaw import config, login

    results = []
    for account in account_registry.list_all_accounts(config.PLATFORM):
        identifier = account["email"]  # pkulaw 账号（手机号或邮箱）也存在这个字段里，见 account_registry.save_account
        if account.get("status") != account_registry.STATUS_ACTIVE:
            results.append(CheckResult(config.PLATFORM, identifier, "skipped_inactive",
                                        f"status={account.get('status')}"))
            continue

        if not force and login.load_cached_session(identifier) is not None:
            results.append(CheckResult(config.PLATFORM, identifier, "reused"))
            continue

        if not account.get("password"):
            results.append(CheckResult(config.PLATFORM, identifier, "skipped_no_password"))
            continue

        try:
            login.get_session(identifier, force=True)  # 有密码的会自动走密码登录，不需要人工介入
            results.append(CheckResult(config.PLATFORM, identifier, "force_relogged_in" if force else "relogged_in"))
        except Exception as e:  # noqa: BLE001
            results.append(CheckResult(config.PLATFORM, identifier, "failed", str(e)))

    return results


PLATFORM_CHECKERS = {
    "wkinfo": check_wkinfo,
    "pkulaw": check_pkulaw,
}


def run(platform: str | None = None, force: bool = False) -> list[CheckResult]:
    checkers = PLATFORM_CHECKERS if platform is None else {platform: PLATFORM_CHECKERS[platform]}
    all_results: list[CheckResult] = []

    for name, checker in checkers.items():
        print(f"\n{'=' * 60}\n检查平台: {name}{'（强制全部重新登录）' if force else ''}\n{'=' * 60}")
        results = checker(force=force)
        all_results.extend(results)
        for r in results:
            line = f"  [{ACTION_LABELS[r.action]}] {r.account}"
            if r.detail:
                line += f" —— {r.detail}"
            print(line)

    print(f"\n{'=' * 60}\n汇总\n{'=' * 60}")
    for name in checkers:
        subset = [r for r in all_results if r.platform == name]
        counts = {action: sum(1 for r in subset if r.action == action) for action in ACTION_LABELS}
        summary = " | ".join(f"{ACTION_LABELS[a]} {n}" for a, n in counts.items() if n)
        print(f"{name}: 共 {len(subset)} 个账号 -- {summary or '无'}")

    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="批量检查各平台账号登录状态，失效自动重新登录")
    parser.add_argument("--platform", choices=list(PLATFORM_CHECKERS.keys()), default=None,
                         help="只检查指定平台，不传就检查全部平台")
    parser.add_argument("--force", action="store_true",
                         help="不管缓存的 session 有没有效，都强制重新登录一次（没存密码的账号仍然会跳过，见模块说明）")
    args = parser.parse_args()
    run(args.platform, force=args.force)

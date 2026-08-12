"""
pkulaw 平台完整流程的手动验收脚本，从 crawler/ 目录直接跑：

    cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
    /Users/houjie/venv/python3-forcrawl/bin/python pkulaw_demo.py

走一遍完整链路（跟真实业务顺序一致，全部是我们自己的代码真实发请求，不是照抓包
对出来的）：

  1. 手机号 + 验证码登录（login.login_interactive，需要真人读验证码）
  2. 校验登录状态 + 请求一次详情页（view_detail_by_url，用这一个指定账号，不走
     账号池）
  3. 手机验证码修改密码（login.change_password）
  4. 退出登录（login.logout）
  5. 用刚改好的新密码，账号+密码重新登录（login.get_session 会自动识别出账号
     记录里已经有密码了，走密码登录这条路，不需要再输验证码）
  6. 校验登录状态 + 强制重新请求一次详情页（force_refresh=True，证明第5步这个
     新 session 是真的能用，不是从 MongoDB 缓存里拿的旧结果）

同时顺带验证了代理池绑定（第1步会自动给这个账号绑一个代理池里负载最少的代理，
之后所有步骤都固定走这个代理）。

**重复跑这个脚本时会跳过没必要的步骤**（不用每次都真人读一遍验证码）：
- 账号已经有密码了（说明之前跑过一次、第3步已经成功过）：跳过第1-3步，直接从
  第4步（退出登录）开始，走"密码登录"那半条链路。
- 账号还没有密码，但 Redis 里缓存的 session 仍然有效（`login.load_cached_session`
  确认过，不是单纯没过期）：跳过第1、2步（不需要真人重新读验证码登录一次），
  直接跳到第3步用这个还有效的 session 去改密码。
- 都不满足（第一次跑，或者缓存已经失效）：老老实实从第1步开始。
"""
from __future__ import annotations

import json
import re
import time
import urllib.parse

from lxml import etree

from core import account_registry, proxy_pool
from core.logger import log
from platforms.pkulaw import auth, config, detail_client, login

# PHONE = "hj1558109546@gmail.com"
# PHONE = "1558109546@qq.com"
# PHONE = '18356966159@163.com'
PHONE = 'xingchi660@gmail.com'
DETAIL_URL = "https://www.pkulaw.com/specialtopic/020c46304569867e0b99ea7333927115bdfb.html"
NEW_PASSWORD = "123456asd"


def _step(n: int, title: str) -> None:
    print(f"\n{'=' * 60}\n第{n}步：{title}\n{'=' * 60}")


def main() -> None:
    account = account_registry.get_account(config.PLATFORM, PHONE)
    has_password = bool((account or {}).get("password"))
    cached_session = login.load_cached_session(PHONE)  # 内部已经用 is_logged_in 二次确认过

    if has_password:
        log(config.PLATFORM, f"账号 {PHONE} 已经存了密码（说明之前跑过一次），跳过第1-3步，直接从第4步开始")
    elif cached_session is not None:
        log(config.PLATFORM, f"账号 {PHONE} 还没设置密码，但缓存的 session 仍然有效，跳过第1、2步")

    if not has_password:
        if cached_session is None:
            _step(1, "手机号 + 验证码登录")
            session = login.login_interactive(PHONE)
            print("is_logged_in:", auth.is_logged_in(session))

            proxy_id = proxy_pool.get_account_proxy_id(config.PLATFORM, PHONE)
            print(f"账号 {PHONE} 绑定的代理: proxy_id={proxy_id}")

            _step(2, "请求详情页")
            result = detail_client.view_detail_by_url(DETAIL_URL, PHONE)
            print("详情页 _id:", result.get("_id"))
            print("正文长度:", len(result.get("contentHtml") or ""))
            print("采集时间:", result.get("crawl_time"))

        _step(3, f"修改密码为 {NEW_PASSWORD!r}（手机验证码）")
        login.change_password(PHONE, NEW_PASSWORD)
        print("change_password 完成")

    _step(4, "退出登录")
    login.logout(PHONE)
    cached = login.load_cached_session(PHONE)
    print("退出后 Redis 缓存的 session（应为 None）:", cached)

    _step(5, "用新密码重新登录（账号+密码）")
    session2 = login.get_session(PHONE)
    print("is_logged_in:", auth.is_logged_in(session2))

    _step(6, "强制重新请求详情页（跳过 MongoDB 缓存，证明新 session 真的可用）")
    result2 = detail_client.view_detail_by_url(DETAIL_URL, PHONE, force_refresh=True)
    print("详情页 _id:", result2.get("_id"))
    print("正文长度:", len(result2.get("contentHtml") or ""))
    print("采集时间:", result2.get("crawl_time"))

    print("\n" + "=" * 60)
    print("全部完成。代理池当前负载:")
    for row in proxy_pool.load_report(config.PLATFORM):
        print(" ", json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    # main()
    acc_permission = {}
    for acc in account_registry.list_all_accounts("pkulaw"):
        email = acc['email']
        # if email not in ['xingchi660@gmail.com']:
        #     continue
        map1 = {
            # '法学期刊': 'https://www.pkulaw.com/qikan/69ad6ab011fe8ac86adab9db17eb821fbdfb.html',
            '法律法规': 'https://www.pkulaw.com/chl/4c30ef181435145fbdfb.html?way=listView',
            '司法案例': 'https://www.pkulaw.com/gac/f4b18d978bc0d1c765c5e832a9f26f9ff19b7b96c2d99453bdfb.html',
            '专题参考': 'https://www.pkulaw.com/specialtopic/020c46304569867e0b99ea7333927115bdfb.html',
            '法学期刊': 'https://www.pkulaw.com/qikauthor_qikan/e538ec04cab38297ba372400ebe27e31bdfb.html',
            '检查文书': 'https://www.pkulaw.com/procuratoratedoc/f4b18d978bc0d1c7adcf5a777de88c76f9773f5102d3e48cbdfb.html?way=listView',
            '行政执法': 'https://www.pkulaw.com/apy/9445d6e6deb7c9d2dc3dfd47d80becda92321bb072c6a2a9bdfb.html?way=listView',
            'English': 'https://www.pkulaw.com/en_chl/b77a0432e538c33eafc83e74aaf0bd2fbdfb.html',
            '': '',
        }
        print(map1.keys())
        acc_permission[email] = []
        for name, url in map1.items():
            if not url:
                continue
            try:
                result2 = detail_client.view_detail_by_url(url, email, force_refresh=True)
                status = 'suc'
                print(result2["contentHtml"][-300:])
                acc_permission[email].append(name)
            except Exception as e:
                status = str(e)
            print(f"{name}：", acc['email'], status)
            time.sleep(1)
    print(acc_permission)
    # res = detail_client.view_detail_pooled_by_url("https://www.pkulaw.com/gac/f4b18d978bc0d1c765c5e832a9f26f9ff19b7b96c2d99453bdfb.html", True)
    # print(res)

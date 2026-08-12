"""
pkulaw 平台特有的策略：这个平台的名字、腾讯 Turing 验证码参数、CAS 登录相关常量。
跟其他平台一样，跟别的平台不共享，共享的部分（Redis/Mongo 连接）在 core/config.py。
"""
from __future__ import annotations

import os

from core import config as core_config

PLATFORM = "pkulaw"

CAPTCHA_BASE = "https://turing.captcha.qcloud.com"
CAS_BASE = "https://cas.pkulaw.com"
WWW_BASE = "https://www.pkulaw.com"
GATEWAY_BASE = "https://gateway.pkulaw.com"

AID = "195551051"
ENTRY_URL = "https://cas.pkulaw.com/auth/realms/fabao/protocol/openid-connect/auth"
JS_PATH = "/tcaptcha-frame.91efdf16.js"

# get_cas_session() 用它当登录入口的 redirect_uri；exchange_code_for_token() 换
# access_token 时也必须传同一个值（OAuth 惯例是 authorize/token 两步的 redirect_uri
# 要对得上）。两处都从这里读，避免各写各的导致不一致。
KC_REDIRECT_URI = (
    "https://static.pkulaw.com/statics/kc/index.html?"
    "redirect_path=https%3A%2F%2Fwww.pkulaw.com%2Fcase%3Fway%3DtopGuid"
)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

CAPTCHA_HEADERS = {
    "user-agent": UA,
    "accept": "*/*",
    "referer": "https://turing.captcha.gtimg.com/",
}

VERIFY_HEADERS = {
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "origin": "https://turing.captcha.gtimg.com",
    "referer": "https://turing.captcha.gtimg.com/",
    "user-agent": UA,
}

# 登录表单只预设了这几个邮箱后缀（其他后缀会走"需人工审核"，登录不会成功，见
# platforms/pkulaw/HANDOFF.md）。
KNOWN_EMAIL_DOMAINS = {"gmail.com", "qq.com", "hotmail.com", "icloud.com"}

# session 在 Redis 里缓存多久就认为"大概率还有效，值得先拿去试一下"，
# 实际有效性最终都靠 is_logged_in() 二次确认，这个只是避免缓存永久不过期。
SESSION_CACHE_SECONDS = 6 * 60 * 60

# --- 配额（详情页请求，账号池调度用，见 detail_client.view_detail_pooled） ---
# 目前没有观察到这个平台对详情页请求有明确的次数限制（不像 wkinfo 那样确认过
# 40次/20次这种硬限额），这里的额度是我们自己定的"每个账号每天最多主动跑多少次"
# 上限，纯粹是为了避免单账号高频到引起注意，不是照着服务端实测出来的真实限额，
# 数字可以按需调整。
DETAIL_LIMIT_PER_DAY = 5000
QUOTA_WINDOW_SECONDS = 24 * 60 * 60

# 账号池调度时，同一个账号被派发去请求详情页之后，冷却这么多秒内不会再被
# core.scheduler.dispatch() 选中（防止同一账号被高频连续请求，跟上面的
# 每日总量配额是两个维度）。没有自己的特殊需求，直接用 core 里那个通用的
# 保守默认值就够了；如果以后发现 pkulaw 需要不一样的间隔，把这行换成具体
# 数字覆盖掉即可，不用改 core。
DETAIL_REQUEST_MIN_INTERVAL_SECONDS = core_config.DEFAULT_DISPATCH_MIN_INTERVAL_SECONDS

# --- 代理池 ---
MAX_ACCOUNTS_PER_IP = int(os.environ.get("PKULAW_MAX_ACCOUNTS_PER_IP", 5))

# 同一个账号两次发验证码（短信/邮箱）之间至少要等这么多秒，见 core/code_throttle.py。
# 真实撞到过网站自己的分钟级限流（{"error":"limit_minute"}，登录发一次验证码后
# 紧接着改密码又要发一次，间隔太短触发的），75 秒是留了余量的保守值。
CODE_SEND_MIN_INTERVAL_SECONDS = 75

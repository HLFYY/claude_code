"""
wkinfo 平台特有的策略：这个平台的名字（用于给每个 core/ 的 Redis key 和
Mongo collection 分区）、配额限额、6个内容分类、试用账号生命周期、代理绑定
上限。这里的内容不和其他平台共享——共享的部分（仅限 Redis/Mongo 连接配置）
是 core/config.py 的事。
"""
from __future__ import annotations

import os

from core import config as core_config

PLATFORM = "wkinfo"

# --- 配额（按账号、按 indexId、按动作类型划分） ---
# 已于 2026-07-29 确认：是从首次触发限制那一刻起算的滚动24小时，不是自然日
# 0点重置（一个账号前一天下午13点左右被限制，第二天上午10点左右仍然被限制——
# 超过21小时但不到24小时，跟"滚动窗口"吻合，跟"过零点重置"不吻合）。网站自己
# 的错误提示永远显示"24小时之后"，不管实际还剩多少时间——这只是固定文案，
# 不是真实倒计时，所以不要指望从这段文案里解析出真实剩余时间。
QUOTA_WINDOW_SECONDS = 24 * 60 * 60

# 下面两个不是"真实配额"，是本地计数的安全上限——真实配额由平台自己的响应
# 权威判定：search_client.py 每次请求后都会检查响应是不是"配额超限"
# （_is_quota_exceeded），一旦是就立刻 quota_tracker.mark_exhausted()，把这个
# (账号, indexId, action) 在本地计数里直接标成"已经用满"，不管这时候本地数的
# 用量是多少——这样不同账号的真实限额不一样（比如以后换成付费账号，限额会比
# 免费试用账号高很多）也不需要改代码，本地计数会自动被服务端的真实判定纠正。
# 这两个数字只是防止在服务端信号出问题/没触发的极端情况下无限重试的兜底上限，
# 定得足够大，不会在真实限额之前提前拦住正常请求。
SEARCH_LIMIT_PER_INDEX = 1000
DETAIL_LIMIT_PER_INDEX = 1000

# 账号池调度时，同一个账号被派发去搜索/请求详情之后，冷却这么多秒内不会再被
# core.scheduler.dispatch() 选中（防止同一账号被高频连续请求）。没有自己的
# 特殊需求，直接用 core 里那个通用的保守默认值，跟 pkulaw 平台是同一个数字，
# 想要不一样的间隔就把这行换成具体数字覆盖掉。
REQUEST_MIN_INTERVAL_SECONDS = core_config.DEFAULT_DISPATCH_MIN_INTERVAL_SECONDS

# 这个池子操作的6个栏目/indexId（已通过浏览器抓包确认）。
INDEX_IDS = [
    "law.legislation",               # 法律法规
    "law.case",                      # 裁判文书
    "law.administrativeSupervision", # 行政监管
    "law.procuratorialCase",         # 检察文书
    "law.editorial",                 # 专业解读
    "law.utilityWriting",            # 文书模板
]

# --- 试用账号生命周期 ---
ACCOUNT_TRIAL_SECONDS = 3 * 24 * 60 * 60

# --- 代理池 ---
MAX_ACCOUNTS_PER_IP = int(os.environ.get("WKINFO_MAX_ACCOUNTS_PER_IP", 5))

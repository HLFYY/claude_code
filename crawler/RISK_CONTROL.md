# 风控与调度逻辑一览

简要汇总"账号怎么被挑出来用、什么情况下会被限制"这类风控/调度逻辑，分通用（`core/`，
所有平台共享）和各平台特有两部分。只讲现状和配置在哪，具体实现细节看代码注释；
每个平台更完整的踩坑记录看各自的 `platforms/<name>/HANDOFF.md`。

## 通用调度逻辑（`core/`，两个平台共享）

| 机制 | 模块 | 配置在哪 | 现状 |
|---|---|---|---|
| 账号选择优先级 | `core/scheduler.py` `dispatch()` | 无需配置，内置行为 | 优先选**剩余配额多**的账号（不是纯随机），让账号池用量尽量平均；剩余配额相同才随机打散。两个平台都生效。 |
| 每日总配额 | `core/quota_tracker.py` | 各平台 `config.py` 的 `xxx_LIMIT_xxx` + `QUOTA_WINDOW_SECONDS` | Redis 计数器 + TTL 实现滚动窗口，到点自动清零。`try_consume` 本地预占，服务端一说超限就调 `mark_exhausted` 立刻同步（本地计数只是预测值，服务端响应才是权威）。 |
| 单账号请求冷却 | `core/scheduler.py` `dispatch(..., min_interval_seconds=N)` | 各平台自己传，默认 `0`=不启用；没有特殊需求可以直接引用 `core/config.py` 的 `DEFAULT_DISPATCH_MIN_INTERVAL_SECONDS`（默认5秒） | **2026-07-30 新增**：账号被 `dispatch()` 选中派发出去后，`N` 秒内不会再被选中；候选账号全部在冷却中（但都还有配额）就等剩余冷却最短的那个解冻，最多等 `max_wait_seconds`（默认60秒）；账号是真没配额了（不是冷却）就立刻报错，不做无意义等待。跟"每日总配额"是两个维度：配额管"今天还能用几次"，冷却管"刚用过、得歇一下"。`dispatch()` 自身参数默认值仍是 `0`，没主动接入的平台不受影响。 |
| 验证码发送冷却 | `core/code_throttle.py` | 各平台 `config.py` 的 `CODE_SEND_MIN_INTERVAL_SECONDS` | 同一账号两次发验证码（短信/邮箱）之间最少间隔这么久，没到点直接原地 sleep 到点，同时打日志。跟上面"单账号请求冷却"是**不同维度**——这个管的是"发验证码"这个动作本身，不是账号池调度选账号。 |
| 代理绑定 | `core/proxy_pool.py` | 各平台 `config.py` 的 `MAX_ACCOUNTS_PER_IP` | 账号绑定一个固定代理终身复用；每个代理能绑的账号数按平台分别限制、分别计数（同一个代理 IP 在不同平台的负载互不影响）。 |
| 账号过期维护 | `core/account_registry.py` `sweep()` | 各平台自己的 `maintenance.py`，需要配 cron | 扫描过期/封禁账号，释放它们占用的代理槽位。不是热路径依赖，**不跑也不会导致误派发**，只是代理槽位会看起来更"满"，建议一天或几天跑一次。 |

## wkinfo（威科先行）特有

- **配额**：按 6 个栏目（`indexId`）分别算，搜索 40 次/天、浏览 20 次/天，`QUOTA_WINDOW_SECONDS`
  是从触发限制那刻起算的**滚动 24 小时**（已实测确认，不是自然日 0 点重置）。
- **真实配额超限信号**：`{"code":"E_010_015", "message":"已达该栏目当日浏览最大量..."}`，
  `search_client._is_quota_exceeded()` 识别后立刻 `mark_exhausted`。
- **单账号请求冷却**：目前**未启用**（`search_client.py` 调 `scheduler.dispatch()` 时没传
  `min_interval_seconds`，默认 0）。
- **代理上限**：每个代理默认最多绑 5 个账号（`WKINFO_MAX_ACCOUNTS_PER_IP` 环境变量可覆盖）。
- **账号试用期**：3 天。
- **已知但还没处理的风控信号**（从网站 i18n 文件里挖到的真实错误码，触发阈值未实测）：
  - 会话/用户/IP/网关四个维度的每秒、每分钟频率限制（`G_USERS_PER_SECOND_OVERWEIGHT` 等）
  - 对应的三级递进封禁（警告一次→警告两次→硬封，`CURRENT_*_IS_RESTRICTED_ACCESS`）
  - 独立的栏目下载日限额（`COLUMN_HAS_REACHED`，区别于搜索/浏览配额）
  - 同一手机号一天注册次数上限（`REGISTER_TIP_PHONE_COUNT_LIMIT`）

## pkulaw（北大法宝）特有

- **配额**：详情页请求每账号每天 5000 次（`DETAIL_LIMIT_PER_DAY`）——**这是自己拍的保守值，
  不是服务端实测出来的真实限额**，目前没观察到这个平台对详情页请求有明确的次数限制。
- **单账号请求冷却**：**2026-07-30 新增并已启用**，`DETAIL_REQUEST_MIN_INTERVAL_SECONDS` 直接引用
  `core/config.py` 的通用默认值 `DEFAULT_DISPATCH_MIN_INTERVAL_SECONDS`（5 秒，`detail_client.
  view_detail_pooled` 已接入）——没有自己特殊数字，想调整就在 pkulaw 自己的 `config.py` 里
  覆盖成具体数字。
- **验证码发送冷却**：75 秒（`CODE_SEND_MIN_INTERVAL_SECONDS`）——真实撞到过网站的分钟级限流
  `{"error":"limit_minute"}` 后加的。
- **代理上限**：每个代理默认最多绑 5 个账号（`PKULAW_MAX_ACCOUNTS_PER_IP` 环境变量可覆盖）。
- **账号转人工审核**：邮箱注册/登录时，即使邮箱后缀在白名单（gmail/qq/hotmail/icloud）也可能
  被转人工审核（真实撞到过），白名单不可靠。可靠判据是登录失败响应页面里解析出的提示文案
  含"请回复邮件进行审核"，命中会抛 `AccountPendingReviewError`（`login.py` 转成
  `RegistrationPendingReview`），需要真人去查收邮件回复。
- **EdgeOne（腾讯云 WAF）JS Cookie 挑战**：`detail_client`/`eo_challenge.py` 撞上会自动解一次
  再重试，但具体触发条件（请求频率/IP 信誉/header 缺失）还没摸清楚，目前策略是"撞上了就解"。
- **配额超限信号识别**：目前**没有**像 wkinfo 那样识别"服务端明确说超限"再调 `mark_exhausted`
  ——`detail_client._fetch_and_save` 把"请求失败"和"真超限"混在一起当同一种失败处理，
  如果以后发现服务端真的有限流响应特征，需要单独补上。

## 已知缺口（简要待办）

- wkinfo 还没启用单账号请求冷却，需要的话直接给 `search_client._dispatch_and_call` 里的
  `scheduler.dispatch()` 调用传 `min_interval_seconds` 即可（`core/` 逻辑已经通用，
  不需要改 `core/`）。
- wkinfo 那一批"已知但未处理"的风控信号（频率限制/三级封禁/下载配额/手机号注册限制）都还
  没实测阈值，也没接入代码。
- pkulaw 的详情页请求还没有识别服务端"真超限"信号的机制，`DETAIL_LIMIT_PER_DAY` 和
  `DETAIL_REQUEST_MIN_INTERVAL_SECONDS` 都是保守猜测值，不是实测出来的真实限制。

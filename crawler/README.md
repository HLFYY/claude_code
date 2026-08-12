# crawler — 通用多平台采集框架

把"注册、登录、cookie管理、配额调度、代理绑定、请求统计、原始数据存储"这些跟具体网站无关的能力
抽成 `core/`，每个具体网站（平台）作为 `platforms/<name>/` 下的一个插件，通过统一的 `platform`
字符串在 Redis / MongoDB / 日志里分区存储。目前实现了两个平台：
- `wkinfo`（威科先行法律数据库）——账号池+配额调度+代理绑定的完整范例，见 `platforms/wkinfo/HANDOFF.md`
- `pkulaw`（北大法宝）——腾讯 Turing 验证码 + 邮箱登录 + EdgeOne WAF 挑战，登录需要真人读邮箱验证码
  所以没有账号池自动调度，见 `platforms/pkulaw/HANDOFF.md`

## 目录结构

```
crawler/
├── core/                      # 平台无关，所有函数第一个参数都是 platform: str
│   ├── config.py              # 仅基础设施配置：Redis/Mongo 连接、key前缀、日志根目录
│   ├── redis_client.py        # get_client() 单例 + k(*parts) key拼接
│   ├── mongo_client.py        # get_db() 单例 + get_collection(platform) -> 每平台一个集合
│   ├── account_registry.py    # 账号存储(Redis Hash+ZSET) + sweep()周期维护
│   ├── proxy_pool.py          # 固定代理池，按platform分别统计绑定数、挑选、封顶
│   ├── quota_tracker.py       # 通用滚动窗口配额计数器(Redis INCR+EXPIRE)
│   ├── request_logger.py      # 按platform分文件的JSONL请求日志 + 按天统计
│   ├── scheduler.py           # 按配额+登录状态挑一个可用账号
│   ├── document_store.py      # 通用MongoDB缓存优先存取(get_cached/save)
│   ├── code_throttle.py       # 验证码发送节流(同一账号两次发送最少间隔多久，TTL计时)
│   └── platform_base.py       # typing.Protocol，定义平台插件对外统一形状
└── platforms/
    ├── wkinfo/                 # 账号池+配额调度+代理绑定范例，见该目录 HANDOFF.md
    │   ├── config.py           # wkinfo 自己的策略：配额限额、6个indexId、试用期、代理上限
    │   ├── platform.py         # core.platform_base.Platform 协议的具体实现，外部统一入口
    │   ├── login.py / search_client.py / registration_worker.py / ...
    │   ├── maintenance.py      # 周期维护入口(账号过期清理+代理槽位释放)
    │   └── HANDOFF.md          # wkinfo 平台的完整交接文档(踩坑/风控信号/待办)
    └── pkulaw/                 # 验证码逆向+反爬挑战范例，登录需真人参与，见该目录 HANDOFF.md
        ├── config.py           # pkulaw 自己的策略：验证码aid、CAS登录常量、邮箱后缀白名单
        ├── auth.py             # 验证码+邮箱验证码+登录表单全流程（"怎么登录"）
        ├── login.py            # Redis session缓存 + 交互式登录调度（"要不要重新登录"）
        ├── tdc_client.py / eo_challenge.py   # Node沙箱跑第三方反爬JS本身，拿collect/eks和挑战cookie
        ├── detail_client.py    # 文章详情页采集，缓存优先，自动处理EdgeOne挑战
        ├── platform.py         # core.platform_base.Platform 协议的具体实现
        └── HANDOFF.md          # pkulaw 平台的完整交接文档
```

## 分层原则

- **`core/` 不认识任何具体网站**：不存 indexId 列表、不存配额数值、不存代理上限、不存 wkinfo 这个
  名字本身以外的任何东西。所有这些"策略"都由调用方（`platforms/<name>/config.py`）显式传入。判断
  一段逻辑该放 `core/` 还是 `platforms/<name>/` 的标准很简单：**这段逻辑如果明天接入第二个平台，
  还成立吗？** 成立就放 `core/`，不成立（比如"错误码 E_010_015 代表配额超限"这种具体网站的响应格式）
  就放平台目录。
- **`core/` 里所有函数第一个参数都是 `platform: str`**，Redis key 和 Mongo collection 都按这个值
  分区，不同平台的账号池、代理绑定、配额计数、请求日志、原始数据完全互相隔离——即使将来两个平台
  共用同一批代理IP，各自的"这个IP绑了几个账号"也是分开算的（`proxies_by_load:{platform}` 这个
  ZSET 是按 platform 单独开的，代理资源本身 `proxy:{proxy_id}` 才是跨平台共享的）。
- **平台插件不需要继承任何基类**：`core/platform_base.py` 用 `typing.Protocol` 定义的是"外部统一
  调用形状"（`register`/`login`/`search`/`view_detail`），平台内部实现完全自由，直接调 `core.*`
  的函数并传自己的 `config.PLATFORM` 常量即可，不需要也不应该为了"通用"而过度抽象。

## 新增一个平台该怎么做

1. `platforms/<name>/` 下建 `config.py`，定义这个平台自己的策略常量（参考
   `platforms/wkinfo/config.py`：`PLATFORM` 名字、配额限额、限额窗口、内容分类列表、账号试用期、
   代理绑定上限）。
2. 实现该平台自己的注册/登录/搜索/详情逻辑（验证码、加密算法、HTTP 请求形状全是这个网站独有的，
   `core/` 帮不上忙，参考 `platforms/wkinfo/` 下对应文件）。这些函数内部按需调用：
   - `core.account_registry.save_account/get_account/...` 存取账号
   - `core.proxy_pool.pick_for_new_account/bind_account/requests_proxies` 挑代理、绑定、取用
   - `core.quota_tracker.try_consume/mark_exhausted` 配额预占与服务端响应同步
   - `core.scheduler.dispatch` 一步拿到"有配额+已登录"的账号
   - `core.request_logger.log_request` 记录每次请求成败
   - `core.document_store.get_cached/save`（或像 `platforms/wkinfo/document_store.py` 一样包一层，
     自定义 `_id` 命名规则）缓存原始数据
3. 写 `platforms/<name>/platform.py`，实现一个满足 `core.platform_base.Platform` 协议的类，作为
   外部统一调用入口（不是必须的，但方便以后写跨平台的通用 runner/CLI）。
4. 写 `platforms/<name>/maintenance.py`，调 `core.account_registry.sweep(platform)`，安排成
   cron 定期跑（daily 或每几天一次即可，不需要实时）——这一步保证 `core.proxy_pool.load_report`
   反映的"代理绑定账号数"只算当前有效账号，见下一节。
5. 写 `platforms/<name>/HANDOFF.md`，记录这个平台特有的踩坑、加密算法、风控信号——这部分天然无法
   通用，每个网站都不一样。

## 代理池：为什么"绑定账号数"需要周期性 sweep 而不是实时

`core/proxy_pool.py` 里代理的负载计数（`proxies_by_load:{platform}` ZSET 的 score）只在
`bind_account()` 时 +1，在 `unbind_account()` 时 -1；账号过期本身**不会**自动触发 -1。
原因：如果要实时保证准确，就得在每次挑代理之前先检查一遍所有已绑定账号是不是恰好在这一刻过期，
这个检查成本和"直接定期批量扫一遍"比没有优势，还会让挑代理这个热路径变慢。所以设计成两步：

- `core.account_registry.sweep(platform)`：扫这个平台所有账号，把过期的标记为 `expired`，然后对
  所有"非 active"状态的账号（刚过期的、之前就过期的、或者被封的 `banned`）检查是否还占着代理槽位，
  占着就调 `proxy_pool.unbind_account` 释放。
- 各平台的 `maintenance.py` 是这个 sweep 的 cron 入口，**跑的频率不需要高**（一天或几天一次足够），
  因为账号试用期通常是以天为单位的，槽位晚释放几个小时不影响实际调度（`core.scheduler.dispatch`
  本来就只从 `list_active_emails` 里挑账号，一个账号即使代理槽位还没释放，只要它自己过期了也不会
  被派发任务，只是不能马上腾出代理槽位给新账号用而已）。

不跑 sweep 的后果：代理池会看起来比实际更"满"（`pick_for_new_account` 可能因为旧账号占着槽位一直
返回 None，即使那些账号早就过期不再使用），但不会导致派发到已过期账号或者出现两个账号误用同一个
代理这类正确性问题——所以这是一个"不跑也不出错，但要记得跑"的维护任务，不是关键路径依赖。

## 运行方式

所有平台模块都要以包的方式从 `crawler/` 目录用 `-m` 运行（内部用的是 `from . import` /
`from core import` 这种相对/包导入，不能直接 `python some_file.py`）：

```bash
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
/Users/houjie/venv/python3-forcrawl/bin/python -m platforms.wkinfo.registration_worker
/Users/houjie/venv/python3-forcrawl/bin/python -m platforms.wkinfo.search_client
/Users/houjie/venv/python3-forcrawl/bin/python -m platforms.wkinfo.maintenance
```

依赖：本地 Redis（5.0.5，无 `KEEPTTL`/`EXPIRE NX`，`core/quota_tracker.py` 已做兼容处理）、
本地 MongoDB、`redis-py==3.2.1`（无 `hset(mapping=...)`，全用 `hmset`）、`pymongo==4.4.1`。

各平台具体依赖的第三方库（如 wkinfo 用到的 `ddddocr`/`opencv`/`pillow`）记在各自平台目录下，
`core/` 本身只依赖 `redis` 和 `pymongo`。

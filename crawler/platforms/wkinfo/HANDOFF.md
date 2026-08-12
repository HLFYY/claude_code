# wkinfo 平台交接文档

目标网站：`https://law.wkinfo.com.cn`（威科先行法律数据库）。目标是脱离浏览器、纯 Node.js/Python 算法还原其验证码、注册、登录相关接口，最终能稳定拿到已登录 session 去访问数据接口，并且是**多平台通用采集框架**（`crawler/`）里的第一个、也是目前唯一的平台实现。

本文档面向"接着做风控/反爬处理"的后续工作，先说清楚**现在做到了什么**、**怎么做到的**、**踩过哪些坑**，最后列出**还没做、值得优先看的风控相关点**。逐条细节证据见同目录 `请求链路.md`（抓包记录+踩坑原始记录），这里是提炼总结。多平台框架本身（`core/` 那一层）的说明见 `crawler/README.md`。

## 目录结构（本平台部分；`core/` 见 `crawler/README.md`）

```
crawler/
├── core/                       # 平台无关公共层，见 crawler/README.md
└── platforms/wkinfo/
    ├── aes_encrypt.js          # Node，唯一职责：AES-128-ECB/PKCS7 加密（生成加密参数）
    ├── recognize.py            # Python(ddddocr)，验证码图像识别
    ├── captcha_client.py       # Python，验证码全流程编排 + 注册全流程编排
    ├── login.py                # Python，登录 + session 缓存(存 Redis) + 自动应用绑定代理
    ├── config.py                # 本平台策略配置：PLATFORM名/配额限制(40/20)/6个indexId/3天试用期/代理上限
    ├── sms_provider.py         # 验证码获取方法(现在 input()，接口可替换)
    ├── random_profile.py       # 随机生成注册字段(company/姓名/省份/职位/密码)，除手机号邮箱外全随机
    ├── registration_worker.py  # 编排单个账号的注册：挑代理 + 随机资料 + captcha_client 全流程 + 存入 registry
    ├── search_client.py        # 真实 search/detail 调用，自动走 core.scheduler+quota+MongoDB缓存+日志
    ├── document_store.py       # wkinfo 的 _id 命名规则(category_docId)，包一层 core.document_store
    ├── maintenance.py          # 定期维护入口：过期账号清理 + 代理槽位释放
    ├── platform.py             # core.platform_base.Platform 协议的 wkinfo 实现（外部统一调用入口）
    ├── HANDOFF.md               # 本文档
    └── 请求链路.md              # 详细抓包证据 + 踩坑记录（按时间线，本文档是它的摘要+索引）
```

运行方式（**必须用 `-m` 从 `crawler/` 目录跑，不能直接 `python login.py`**——因为现在是包结构，相对导入需要包上下文）：
```bash
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
/Users/houjie/venv/python3-forcrawl/bin/python -m platforms.wkinfo.login
/Users/houjie/venv/python3-forcrawl/bin/python -m platforms.wkinfo.registration_worker
/Users/houjie/venv/python3-forcrawl/bin/python -m platforms.wkinfo.maintenance
```

运行环境：
- Node：系统自带即可，`aes_encrypt.js` 只用内置 `crypto` 模块，**没有任何 npm 依赖**。
- Python：必须用 `/Users/houjie/venv/python3-forcrawl/bin/python`（已装 ddddocr/opencv/numpy/pillow/redis/pymongo），不是系统 python3。
- 本地 Redis 服务，`redis-cli ping` 能通即可。**注意服务器版本是 5.0.5**，不支持 `EXPIRE ... NX`/`SET ... KEEPTTL`（Redis 6.0+ 才有），`core/quota_tracker.py` 里手动实现了等价逻辑（先 GET/TTL 读出来，SET 之后再手动 EXPIRE 回去），升级 Redis 服务器版本后可以简化成原生的 NX/KEEPTTL 写法。
- redis-py 版本是 3.2.1（老版本），`hset(..., mapping=...)` 这种新语法不支持，得用 `hmset()`。
- 本地 MongoDB 服务（默认 `mongodb://localhost:27017`）。**每个平台一个 collection**，wkinfo 的数据在库 `crawler`、collection `wkinfo` 里（`core.mongo_client.get_collection("wkinfo")`）。`pymongo` 4.4.1。
- `captcha_client.py`/`login.py`/`search_client.py` 全是同步阻塞的 `requests` 调用，没有做异步/并发，调度也是单进程顺序调用（`core.scheduler.dispatch()` 里 email 选取的 shuffle 只是分散负载，不是真并发）。

## 已实现能力

### 1. 验证码识别 + 加密（`recognize.py` + `aes_encrypt.js` + `captcha_client.py`）

网站验证码是开源 **AJ-Captcha**（anji-plus/captcha）的定制部署，两种类型：`blockPuzzle`（滑块）和 `clickWord`（点选文字），共用同一套接口和加密算法。

**加密算法**（源码定位于 `assets/js/verify-slipping/ase.js`，非猜测）：
- AES-128-ECB / PKCS7 padding，key = `secretKey`（服务端下发）的原始 UTF-8 字节，无 IV
- `pointJson` 明文：blockPuzzle 是 `{"x":..,"y":5}`；clickWord 是 `[{"x":..,"y":..}, ...]`（按 wordList 顺序）
- `captchaVerification`（提交给 `/csi/captcha/verify` 时用）= AES(`token + '---' + 上面那个明文`, secretKey)
- **明文格式必须和 JS 的 `JSON.stringify` 完全一致**（见下面踩坑）

**图像识别**（`recognize.py`，用 ddddocr，说明见该文件 docstring）：
- blockPuzzle 缺口：`ddddocr.slide_match(piece, bg, simple_target=True)`。注意 `simple_target=True`（不裁剪透明边）比默认的 `False` 准得多（实测 8/8 vs 6/8）——网站背景图有 1 个真缺口 + 2 个干扰假缺口，早期用手写边缘检测/相关系数被骗过一次。
- clickWord 点选：`ddddocr.detection()` 找字符框很稳，但单个分类模型（default/old/beta）识别这种旋转+彩色+自然背景的汉字准确率不高，改成三个模型识别结果**取并集**（+裁剪时四周留 4px），整单命中率从 0~33% 提到 60%。因为重新拉验证码不要钱，`solve()` 用"不 confident 或 check 失败就换一张重试"策略（`max_attempts=6`），不是死磕单张图片精度。

**接口链路**：`POST /csi/captcha/get` → 识别 → `POST /csi/captcha/check` → `POST /csi/captcha/verify`（verifyType=register 时带 phoneNumber）。

### 2. 完整注册流程（`captcha_client.py` 的 `full_registration_flow`）

`captcha/verify` 成功后：
- `GET /csi/user/captcha?telephone=&password=&email=` —— 这一步才是真正触发发短信的调用
- 用户输入手机收到的真实验证码（`sms_provider.get_sms_code()`，现在是 `input()`，接口可替换）
- `POST /csi/user` —— 最终提交注册，body 里 `code` 字段固定值 `"01BE01"`（观察到两次抓包都一样，像是这个"law"分组试用渠道的固定值，不是动态生成的）

### 3. 登录 + Session 缓存（`login.py`）

静态分析 `main.<hash>.js` 定位到（不是抓包猜的）：
- **登录**：`POST /csi/account/validate/ex`，body 明文 `{"username","password"}`（不加密、不需要验证码！和注册流程完全不同）。成功直接返回完整用户 profile，`Set-Cookie: connect.sid=...`
- **登出**：`GET /api/logout`
- **session 有效性检查**：`GET /api/autoLogin`，返回 `{"login":false}` 说明当前 cookie 已失效；返回 `{"login":true}` 时服务端还会顺带补发一批 cookie（`userInfo`/`autologin`/`cinfo` 等）

`get_session(username, password)` 逻辑：先读 Redis `session:wkinfo:{email}` 缓存 → 检查本地记的 cookie 过期时间 → 用 `/api/autoLogin` 二次确认真的还有效 → 都过才复用；否则才真正调登录接口并覆盖缓存。**二次确认这一步是必须的**，因为服务端有单并发限制（见下），别处一登录可能直接把老 session 踢了，光看 cookie 自带的 Expires 不可靠。

已验证账号：`1558109546@qq.com` / `315128abc`。**注意** `18356966159` 是这个账号的手机号，不是登录用户名，登录必须用邮箱。

### 4. 多账号池：注册/存储/登录调度/配额/统计

在上面 1-3 的基础上，通过 `core/` 的通用层做多账号管理和配额调度（详细数据结构见 `crawler/README.md`，这里只讲 wkinfo 特有的部分）。

**6 个栏目 indexId**（浏览器抓包确认，`config.py` 里的 `INDEX_IDS`）：
`law.legislation`(法律法规) / `law.case`(裁判文书) / `law.administrativeSupervision`(行政监管) /
`law.procuratorialCase`(检察文书) / `law.editorial`(专业解读) / `law.utilityWriting`(文书模板)

**账号 3 天有效期是从服务端读的，不是自己算的**：登录成功后 `profile.productsDetailList[0].endDate`
就是服务端记的试用到期时间（毫秒时间戳），跟 `startDate` 相减正好是 3 天。`registration_worker.py`
里注册完会额外登录一次，把这个权威值写回 `account_registry`，而不是用"注册时间+3天"自己估算的值
（这样万一某个账号的试用期不是标准 3 天，也能自动跟服务端保持一致）。

**配额判定用真实响应同步，不完全信本地计数**：`try_consume()` 是"乐观预占"（调用前先 INCR），
一旦服务端真的返回配额用尽的响应（见下面确认的错误码），`mark_exhausted()` 会把本地计数强制打满，
`search_client.py` 的 `_dispatch_and_call` 立刻换下一个账号重试——这样即使本地计数和服务端有偏差
（比如账号被别处手工测试用掉了配额），也不会白白浪费请求去撞墙。限额数值：搜索40次/浏览20次，每个
(账号, indexId) 独立计数。

**配额超限的真实响应格式（已抓包确认）**：
```json
{"code":"E_010_015","message":"已达该栏目当日浏览最大量，请24小时之后再进行浏览。"}
```
这是"浏览"（detail）超限的确切错误码。"搜索"（search）超限的对应错误码还没实测抓到，只从网站自己的
i18n 文件里确认了消息文本 `"已达该栏目当日搜索最大量，请24小时之后再进行搜索。"`——`search_client.py`
的 `_is_quota_exceeded()` 优先匹配已确认的 `E_010_015` 错误码，兜底再匹配这两条消息文本。

**滚动窗口 vs 自然日重置：已确认是滚动 24 小时，不是自然日重置**（2026-07-29 验证）——一个账号昨天
下午1点多被限流，今天上午10点多还是无法访问（超过21小时但不到24小时），跟"从首次触发限流开始算24小时"
完全吻合，如果是自然日重置早该在今天0点后恢复了。另外接口的错误文案里"24小时之后"是固定文案，**不会显示
真实剩余时间**，不要指望从消息文本里解析出准确的恢复时间——`core/quota_tracker.py` 里 Redis key 自身的
TTL 才是准确的剩余时间来源（`remaining()` 返回的 `ttl_seconds`）。这条已经是最终实现，不用再改。

### 5. 详情页采集 + MongoDB 缓存（`document_store.py` + `search_client.py`）

`search_client.view_detail(index_id, doc_id, search_id)` 现在是"缓存优先"：先查 MongoDB
(`document_store.get_cached`)，命中直接返回——**不发 HTTP 请求、不消耗配额**；没命中才走
`core.scheduler`+`core.quota_tracker` 真实请求，请求成功后存进 MongoDB (`document_store.save`) 再返回。
需要强制重新抓取时传 `force_refresh=True`。

存储结构（库 `crawler`，collection `wkinfo`）：
```
_id: "{category}_{docId}"          例如 "legislation_MTAxMDA1MDY0MzE="  (category+docId 天然去重)
...                                  # /csi/document/{docId}/html 原始响应的所有字段，原样铺开
category: "legislation"             # 从 indexId 去掉"law."前缀得到，冗余存一份方便按栏目查询
docId: "MTAxMDA1MDY0MzE="           # 同上，冗余存一份
cctime: 1785291807                  # 采集时刻，10位unix秒级时间戳
crawl_time: "2026-07-29 10:23:27"   # 同一时刻，人类可读格式
```
已用真实请求测试过：第一次调用消耗配额+落库，第二次调用同一个 (index_id, doc_id) 直接命中缓存、
配额计数没有变化。

### 6. 固定代理池 + 账号级绑定，按"有效账号数"计负载（`registration_worker.py` + `core/proxy_pool.py`）

"账号绑定一个固定代理"（不是"一个IP绑多个账号"那种反过来的颗粒度——`connect.sid` session 要跨多次
请求持续用，中途换IP既是风控信号也可能直接搞断 WAF 的 cookie 流程）。数据结构和挑选逻辑是 `core/`
通用层的（见 `crawler/README.md`），这里记 wkinfo 侧接入方式：

- `core.proxy_pool.add_proxy(host, port, username=None, password=None)` 往池子里加真实代理，
  按 host+port 去重、proxy_id 自动分配（自增计数器），不用自己起名字，返回这次生效的 proxy_id。
  **这一步需要你提供真实代理信息**，现在池子是空的，加了代理之后 `registration_worker.py` 才能实际用起来。
- `registration_worker.register_one()` 会先挑代理（`pick_for_new_account(config.PLATFORM, config.MAX_ACCOUNTS_PER_IP)`，
  选绑定数最少且没超上限的），**整个注册流程（含验证码）都通过这个代理的 session 发出**，注册成功后
  立刻绑定，然后才做确认登录——如果代理池是空的或者全满了，直接抛异常，不会静默地不走代理去注册。
- `login.py` 自动查账号绑定的代理并用上（`_proxied_session()`），`search_client.py` 完全不用管代理的事，
  因为都是通过 `login.get_session()` 拿 session，代理是在 `login.py` 内部自动应用的。
- **向后兼容**：已经用旧版代码注册的账号（比如 `1558109546@qq.com`）没有代理绑定记录，`login.py`
  查不到绑定时就直接用空 `proxies`（直连），已经测过不会报错，只是这些老账号没有走代理。

**"绑定账号数"只按有效账号算，不是所有历史注册过的账号**——`core.account_registry.sweep(platform)`
是个周期性维护任务（不是实时的，"可能一天或几天跑一次"，入口是 `maintenance.py`）：扫一遍这个平台
所有账号，把过期的（`expires_at` 已过）标记成 `expired`，然后对**所有非 active 状态**的账号（刚过期的、
之前已经过期的、或者被封的）检查有没有还占着代理槽位，占着就释放（`proxy_pool.unbind_account`）。
不是实时扣减的原因：账号一过期就想着去释放，等于每次挑代理前都要检查一遍所有账号是不是刚好过期，
没必要这么频繁，定期跑一次批量清理更简单也够用。

已用假代理（本地端口）测过挑选逻辑本身（负载均衡、封顶）和 sweep 逻辑（模拟一个过期账号，跑
`sweep` 后代理负载正确减1、账号状态变 expired、代理绑定记录被删）。**没用真实代理测过完整注册流程**，
因为现在池子里没有真实代理——这个需要你提供实际的代理服务商信息（host/port/账密）之后再联调一次。

## 关键踩坑（后续加风控逻辑时容易踩到同样的坑）

1. **JSON 数字格式必须和 JS 字节级一致**：Python `json.dumps({"y":5.0})` 输出 `"y": 5.0`（带空格、保留 `.0`），JS `JSON.stringify` 输出 `"y":5`。这个差异让服务端在 AES 解密后解析明文时直接抛 `NullPointerException`（HTTP 500），而不是正常的业务错误码——**服务端对解密后明文的处理不是纯粹的 JSON 解析**，格式必须做到跟 JS 输出一模一样。`captcha_client.py` 里 `_js_num()`/`_point_str()` 就是干这个的。
2. **"更像浏览器"的请求头反而更容易触发异常**：给 `/csi/user/captcha` 和 `/csi/user` 加上 `identification`/`module`/`ucv`/`appversion`（模仿 Angular HTTP 拦截器实际发的那套）之后，直接返回 `E_000_003 注册验证码校验失败`；换回 `captcha/get|check|verify` 用的极简 header 集合（`verify.js` 原生 `$.ajax` 那套）就完全正常。**没深挖是哪个字段导致的**，只是发现"更完整的头"不代表"更安全/更像人"，可能触发了服务端另一条校验路径。这点对接下来做风控处理很重要——不能想当然地认为"头越全越好"。
3. **默认 `python-requests` 的 User-Agent 会被 WAF 拦**：早期测试中直接用 `requests` 默认 UA 打接口，收到过 `403 Forbidden ... denied by UA ACL = blacklist`（Tengine 层的 UA 黑名单）。所有代码里都已经固定用真实 Chrome UA 字符串，没有再复现过，但这是**目前唯一实锤过的、纯 UA 层面的风控拦截**，说明至少有一层基于 UA 的黑名单存在。
4. **验证码背景图里的干扰缺口**：blockPuzzle 有 2 个假缺口专门用来骗自动化识别，clickWord 的字符也是刻意做了旋转/变色/自然照片背景来干扰 OCR。这两个本身就是这个站点风控体系的一部分（AJ-Captcha 的"干扰"配置项），已经用相关系数模板匹配 + 多模型 OCR 取并集的方式绕过。
5. **账号单并发登录限制**：同一账号同时只能有一个有效 session，重复登录会收到 `{"code":"C_002_001","message":"用户并发超标"}`。不是 bug，是业务规则，但如果后续要做"多账号池轮换"之类的风控对抗，这个限制要考虑进去。
6. **相对导入要求包结构**：这一层全是 `crawler/` 下的包（`core`、`platforms.wkinfo`），文件之间用 `from . import xxx` / `from core import xxx` 这种写法，**不能再像以前那样 `python login.py` 直接跑**，必须 `cd crawler && python -m platforms.wkinfo.login`，否则会报"attempted relative import with no known parent package"。

## 观察到但还没处理的风控信号（后续重点）

**这一节是重点，从网站自己的 i18n 语言文件里挖到了一整套风控错误码/文案，这是真实存在的机制，不是猜的**——
但目前只是"知道有这些代码"，**触发条件、具体阈值、响应的确切 JSON 结构都还没实测验证过**，因为目前所有
测试都是低频、人工触发的，没有主动压测过。这些是后续做反爬处理时最该优先啃的东西：

### i18n 里挖到的风控错误码体系（`WK_CONCENTRATION`模块的语言包，key 是 i18n key 不是 code，但能按名字反查）

**频率限制**（会话/用户/IP/网关 四个维度，每秒和每分钟两档）：
```
G_IN_A_SECOND_SESSION              每秒操作次数太多（会话）
G_THE_SESSION                      每分钟操作次数太多（会话）
G_USERS_PER_SECOND_OVERWEIGHT      每秒操作次数太多（用户）
G_USERS_PER_MINUTE_OVERWEIGHT      每分钟操作次数太多（用户）
G_IP_PER_SECOND_OVERWEIGHT         每秒操作次数太多（IP）
G_IP_MINUTES_OVERWEIGHT            每分钟操作次数太多（IP）
G_GATEWAY_PER_SECOND_OVERWEIGHT    每秒操作次数太多（网关）
G_GATEWAY_PER_MINUTES_OVERWEIGHT   每分钟操作次数太多（网关）
```
**分级封禁**（每个维度都是"警告一次→警告两次→硬封"三级递进）：
```
CURRENT_SESSION_IS_RESTRICTED_ACCESS / _1 / ALL_THE_CURRENT_SESSION   会话维度
CURRENT_USER_IS_RESTRICTED_ACCESS / _1 / ALL_THE_CURRENT_USER          用户维度
CURRENT_IP_IS_RESTRICTED_ACCESS / _1 / ALL_THE_CURRENT_IP              IP维度
CURRENT_GATEWAY_IS_RESTRICTED_ACCESS / _1 / ALL_THE_CURRENT_GATEWAY     网关维度
```
**人工/管理员封禁**：`CURRENT_USER_IS_A_MANUAL_LIMIT_ACCESS`(当前用户被管理员限制访问)、
`CURRENT_IP_BY_MANUAL_LIMIT_ACCESS`(当前ip被管理员限制访问)——这两个大概率是人工拉黑，不是自动风控。

**这套体系意味着**：多账号池光轮换账号还不够，如果调度器请求太密集，**IP 维度和网关维度的限制会跨账号生效**
（不管换多少个账号，同一个 IP/同一个网关打太快照样会被限）。这也是现在做固定代理池的另一层价值——不同
账号用不同代理IP，天然把IP维度的限流风险摊开了，但具体每秒/每分钟能打多少次还是要通过压测搞清楚阈值。

**另外两个顺手发现，不是这次要做的配额但记录一下**：
- `COLUMN_HAS_REACHED`：`"已达该栏目当日下载最大量，请24小时之后再进行下载。"`——每个栏目居然还有独立的
  **下载**日限额（区别于搜索40/浏览20），如果以后调度器要支持下载接口，这个配额也要单独算。
- `REGISTER_TIP_PHONE_COUNT_LIMIT`：`"该手机号今天申请次数已达系统最大值，请换个手机号或明天再申请"`——
  同一手机号一天能注册的账号数也有上限，批量注册攒账号池时如果复用手机号会撞到这个。

- **`acw_tc` cookie**：阿里云 WAF（Tengine 反爬）常见的第一跳 cookie。目前所有接口调用里都能拿到这个 cookie（服务端 Set-Cookie 下发），且没有遇到需要额外 JS challenge 才能拿到有效值的情况——但这可能只是因为请求频率低、UA 正常，没有触发更严格的二跳校验（`acw_sc__v2` 之类）。高频调用时需要重点盯这个。
- **`x-alicdn-da-ups-status` 响应头**：在 `/csi/user` 的 400 响应里见过 `x-alicdn-da-ups-status: endOs,0,400`，阿里云"用户保护系统"相关的状态头，具体含义和触发条件没有深挖。
- **`uber-trace-id` / `traceparent` / `b3` 请求头**：真实浏览器每次 XHR 都会带（Zipkin/OpenTelemetry 格式的分布式追踪 ID），我们的 Python 请求里完全没带这些头，目前看不影响功能——但如果服务端有"检查这些 trace id 内部一致性/是否存在"的风控逻辑，这可能是一个能被识别出"不是真实浏览器"的信号，值得后续验证要不要伪造。
- **`boldrum-trace`（阿里云 SLS）行为埋点**：真实浏览器会不断上报点击、页面性能、错误等行为数据到 `boldrum-trace.cn-beijing.log.aliyuncs.com`。我们的请求完全没有这些伴随的埋点流量。如果服务端有"这次 API 调用前后有没有对应的行为埋点"这类关联风控，纯 API 调用会很显眼——目前没证据表明服务端真的做了这层关联，但这是最大的一个未知项。
- **请求频率/间隔**：目前脚本里 `solve()` 的重试是没有 sleep/退避的，连续失败会很快重试。如果之后要跑更大量级的验证码识别测试，建议加上退避策略，避免因为频率触发新的风控（目前没触发过，但也没测过高频场景）。

### 建议的下一步

1. 针对上面挖到的 `G_*` 频率限制错误码，做一次**可控的压测**——用 `search_client.search()`/`view_detail()` 连续快速调用（比如 1 秒内打 10+次），观察触发的是哪个维度（会话/用户/IP/网关）、具体在第几次触发、返回的确切错误码和 JSON 结构，这是目前最大的未知项。压测时**只用一个测试账号，别拿正式账号池里的账号去试**，触发了"警告两次"级别不确定会不会伤到账号本身。
2. 根据压测结果，在 `search_client.py`/`core/scheduler.py` 里加对应的识别 + 退避处理（不只是现在 `_dispatch_and_call` 里"换账号重试"这一种策略——如果是 IP/网关维度限制，换账号没用，得加请求间隔或者暂停）。
3. `COLUMN_HAS_REACHED`（下载限额）和 `REGISTER_TIP_PHONE_COUNT_LIMIT`（手机号注册次数限额）目前完全没处理，等真的要用到下载接口/批量注册规模上去了再补。
4. 评估要不要伪造 `uber-trace-id`/`traceparent`/`b3` 和配套的 `boldrum-trace` 埋点流量，让请求"看起来"更完整——但先确认服务端是否真的关联校验这些，不要没验证就加复杂度。
5. `acw_tc`/`acw_sc__v2` 和 `x-alicdn-da-ups-status` 这两个阿里云 WAF 相关的信号，建议跟第1点的压测一起做，因为触发条件很可能也是高频请求。
6. **真实代理接进来**：`core.proxy_pool.add_proxy(...)` 加真实代理信息，然后完整跑一遍 `registration_worker.register_one()`，确认真实代理下注册+登录+采集全链路没问题（现在只用本地假端口测过挑选逻辑，没测过真代理下的实际网络请求）。

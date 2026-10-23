# 麦当劳 API 项目

一个完整的麦当劳中国 App API 逆向工程项目，实现了从登录、选店、点餐到支付的完整下单流程。

> 📒 **踩坑与结论日志**：`../mcd/docs/踩坑与结论日志.md` ——
> 记录每次测试遇到的问题、排查过程、最终结论和解法（706070 根因、a2/a97 封号根源、云手机 hook/ddyun 隧道坑、滑块失败、签名版本锁、mitm 等）。
> **开工前先查它的"快速索引"**，避免重复踩已验证过的坑；每完成一轮测试/排查，请往对应主题追加一条。

## 项目特性

✅ **完整的三步流程**
- 店铺选择（附近店铺/搜索店铺）
- 商品点餐（支持多商品、规格选择、自动处理优惠券）
- 订单支付（自动提交订单、生成支付宝支付链接/二维码）

✅ **登录管理**
- 多账号支持
- 自动检测登录状态
- Token 过期自动重新登录

✅ **代理池管理系统** 🎉 v2.0 新增
- 代理池管理（增删查改、状态管理、健康检查）
- 账号-代理长期绑定（Redis 持久化）
- 四级智能容错（自动重试、临时切换、观察期、永久切换）
- 城市优先分配（自动查询代理 IP 城市）
- 负载均衡（单代理最多绑定5个账号）
- 定时任务调度（全量检查、观察期检查、失败清理）
- HTTP API 接口（8个代理管理接口）

✅ **API 完整实现**
- V4 签名算法（HMAC-SHA256）
- AES-128-ECB 加密
- 所有核心业务接口（支持代理）

## 项目结构

```
mcd_project/
├── 【核心 API 与登录】
│   ├── mcd_api.py                 # 核心 API：v4 签名 / AES 加密 / 全部业务接口；set_active_d / set_active_tid 注入 d/tid
│   ├── login_manager.py           # 登录管理：多账号/凭证/自动代理/ensure_device(领设备)/ensure_tid(激活 tid)
│   ├── mcd_server.py              # HTTP API 服务：登录(/api/login/*) + 业务(/api/mcd/*) + 代理管理
│   ├── config.py                  # 配置：Redis、代理(PROXY_*)、验证码(CAPTCHA_LOGIN_CONFIG)、设备池(DEVICE_POOL_CONFIG)
│   ├── exceptions.py              # 自定义异常类
│   └── utils.py                   # 公共工具函数
├── 【设备指纹池 · v4.0 新增】
│   ├── device_builder.py          # 造假设备：frida 注入(android_id+native属性+token)→ app 自己注册拿真 smid；内置机型库
│   ├── device_pool_redis.py       # Redis 设备池服务：refill/serve 补池、claim/release/stats 领用
│   └── frida/
│       ├── hook_fake_device.js    # 假设备注入(android_id + native 属性 + token) + 清 smid 缓存 + RPC
│       └── frida_bypass_combined_local.js   # 反调试绕过
├── 【验证码 / 滑块登录 · v3.0】
│   └── captcha_login_worker.py    # 自动过滑块 worker：DrissionPage + ddddocr，BLPOP Redis 队列异步执行
├── 【代理池 · v2.0】
│   ├── proxy_pool.py              # 代理池管理器
│   ├── account_proxy_binding.py   # 账号-代理绑定管理器
│   ├── proxy_failure_handler.py   # 四级容错失败处理器
│   ├── proxy_manager.py           # 代理管理统一入口
│   ├── ip_location.py             # IP 地理位置查询
│   └── scheduler.py               # 定时任务调度器
├── 【脚本 / 测试】
│   ├── run.py                     # 主流程脚本（直接调 mcd_api：store/order/payment 三步）
│   ├── run_api_client.py          # HTTP API 客户端（经 mcd_server 走完整下单流程）
│   ├── use_login_manager.py       # 登录管理器测试脚本
│   └── demo.py                    # 代理池初始化脚本
└── 【文档 / 依赖】
    ├── requirements.txt           # 项目依赖包
    ├── README.md                  # 项目使用文档（本文件）
    ├── CHANGELOG.md               # 更新日志
    └── 麦当劳API接口文档.md        # 接口抓包 / 参数文档
```

> 说明：补池(`device_builder`/`device_pool_redis` 的 refill/serve)与过滑块(`captcha_login_worker`)须跑在接真机的本机；领用(`claim`)和业务请求任意机只连 Redis 即可。

**运行时自动生成的文件：**
- `credentials.json` - 登录凭证存储（Redis 模式下不生成）
- `selected_store.json` - 当前选择的店铺信息
- `order_data.json` - 待支付订单数据

## 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

### 初始化代理池（可选）

```bash
# 编辑 demo.py 添加您的代理信息，然后运行
python demo.py
```

### 完整下单流程（推荐）

```bash
# 第一步：选择店铺（附近店铺或搜索）
python run.py store

# 第二步：点餐添加商品到购物车
python run.py order

# 第三步：提交订单并生成支付链接
python run.py payment
```

每步执行完会自动保存状态，下一步自动读取。所有流程开始前会自动验证登录状态。

**v2.0 新增：** 所有请求自动使用账号绑定的代理，失败自动触发四级容错机制。

### 测试脚本

```bash
# 测试每个分类的第一个商品能否添加购物车
python test_add_all_categories.py

# 测试登录管理器（5个场景）
python use_login_manager.py

# 清空购物车演示
python demo.py
```

## 登录管理器 API

### 基本用法

```python
from login_manager import LoginManager

# 创建登录管理器
manager = LoginManager(phone="17717295039")

# 自动检查并登录（如失效会提示输入验证码）
token, sid, meddy_id = manager.ensure_login(auto_relogin=True)

if token:
    print(f"登录成功")
```

### 使用场景

**1. 检查登录状态**

```python
from login_manager import LoginManager

manager = LoginManager(phone="16752934813")

# 步骤1: 发送验证码
success, message = manager.send_verify_code()
if success:
    print(f"验证码已发送")
    
    # 步骤2: 输入验证码并登录
    verify_code = input("请输入验证码: ")
    success, message = manager.do_login(verify_code)
    
    if success:
        print("登录成功")
        token, sid, meddy_id = manager.get_credentials()
```

### 3. 自动重新登录

```python
from login_manager import LoginManager

manager = LoginManager(phone="16752934813")

# 自动检查登录状态，如果失效会自动重新登录（需要手动输入验证码）
token, sid, meddy_id = manager.ensure_login(auto_relogin=True)
```

### 4. 多账号管理

```python
from login_manager import LoginManager

# 账号1
manager1 = LoginManager(phone="16752934813")
token1, sid1, meddy_id1 = manager1.ensure_login()

# 账号2
manager2 = LoginManager(phone="13800138000")
token2, sid2, meddy_id2 = manager2.ensure_login()

# 凭证会自动保存到 credentials.json，支持多个账号
```

### 5. 仅检查登录状态

```python
from login_manager import LoginManager

manager = LoginManager(phone="16752934813")

# 加载已保存的凭证
token, sid, meddy_id = manager.load_credentials()

if token and sid:
    # 检查凭证是否仍然有效
    is_valid, user_info = manager.check_login_status()
    
    if is_valid:
        print(f"昵称: {user_info['name']}")
        print(f"积分: {user_info['points']}")
```

## mcd_api.py 核心接口

### 登录相关
- `generate_token()` - 生成设备 Token
- `activate_token(token)` - 激活 Token 获取 tid
- `send_verify_code(phone, token, sid)` - 发送验证码
- `login_with_code(phone, code, token, sid)` - 验证码登录
- `check_login_status(token, sid, meddy_id)` - 检查登录状态

### 城市和店铺
- `get_all_cities(token, sid, meddy_id)` - 获取所有城市信息
- `get_city_by_location(latitude, longitude, token, sid, meddy_id)` - 通过经纬度获取当前城市
- `search_stores(city_code, keyword, token, sid, meddy_id, ...)` - 搜索店铺
- `get_nearby_stores(token, sid, latitude, longitude, ...)` - 获取附近店铺

### 商品和菜单
- `get_store_menu(token, sid, store_code, be_code, ...)` - 获取店铺商品菜单
- `get_product_detail(token, sid, product_code, store_code, ...)` - 获取商品详情

### 购物车
- `clear_cart(token, sid, store_code, be_code, ...)` - 清空购物车
- `add_to_cart(token, sid, store_code, be_code, product_code, ...)` - 添加商品到购物车

### 订单相关
- `get_order_validation_info(token, sid, store_code, ...)` - 获取订单验证信息
- `get_order_promotion_info(token, sid, store_code, cart_items, ...)` - 获取促销/优惠券信息
- `get_nearest_store_info(token, sid, store_code, ...)` - 获取门店信息
- `submit_order(token, sid, ...)` - 提交订单
- `get_payment_channels(token, sid, order_id, pay_id, ...)` - 获取支付渠道
- `create_payment(token, sid, order_id, pay_id, ...)` - 创建支付

## 技术实现

### 签名算法

使用 HMAC-SHA256 签名（V4 版本）：

- **V4AK**: `HJ7YLqOY06F61FPEhF7H`（Access Key）
- **V4SK**: `JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp`（Sign Key）

签名消息格式：
```
<HTTP方法>
<路径>
<查询字符串（按字母排序）>
<V4AK>
<GMT时间>
<规范化请求头>
<空行>
```

### AES 加密

手机号和验证码使用 AES-128-ECB 加密：

- **密钥**: `w8ZJ4wrUl7dDB1A7`
- **模式**: ECB
- **填充**: PKCS7
- **编码**: Base64

### 购物车 API 实现细节

通过抓包对比发现，购物车接口使用 **PUT** 方法而非 POST，且需要：

1. **完整的商品规格信息**：包括 `modification.optionGroups` 和 `modification.values`
2. **daypartCode**：时段代码，通过 `get_product_detail` 获取
3. **正确的请求体结构**：完整的商品、门店、订单信息

## 流程说明

### 1. 店铺选择流程 (store)

- 选择附近店铺或搜索店铺
- 自动保存到 `selected_store.json`
- 保存店铺代码、名称、BE代码、经纬度等信息

### 2. 点餐流程 (order)

- 验证登录状态（自动刷新 token）
- 加载店铺信息
- 获取菜单并选择分类和商品
- 获取商品详情（包含规格、daypartCode）
- 支持循环添加多个商品
- 自动处理优惠券和会员卡
- 保存订单数据到 `order_data.json`

### 3. 支付流程 (payment)

- 验证登录状态
- 加载订单数据
- 提交订单获取订单ID和支付ID
- 获取支付渠道
- 创建支付并生成支付宝支付数据
- **保存支付信息到 `payment_info.json`**

**支付方式**：
```bash
# 正常模式：提交订单并保存支付信息
python run.py payment

# 使用已有订单：直接读取已保存的订单ID和支付ID
python run.py payment old
```

**⚠️ 支付数据说明**：
- 麦当劳返回的 `channelPayData` 是**支付宝 APP 支付**专用字符串
- **不能**通过二维码扫描或支付链接完成支付
- 只能在以下场景使用：
  1. **麦当劳官方 APP** 内调用支付宝 SDK
  2. **自己开发的移动应用**中集成支付宝 SDK 后调用
- 支付字符串会自动保存到 `alipay_payment_*.txt` 文件供移动端使用

## 注意事项

1. **登录凭证管理**
   - 所有流程开始前会自动验证登录状态
   - Token 过期会自动触发重新登录
   - 多账号支持，凭证存储在 `credentials.json`

2. **商品规格处理**
   - 某些商品（咖啡、饮品）必须选择规格才能添加
   - 需要先调用 `get_product_detail` 获取完整规格信息
   - `modification` 结构必须完整才能添加成功

3. **时段代码 (daypartCode)**
   - 不同时段商品可能不同
   - 通过 `get_product_detail` 获取当前时段的 daypartCode

4. **支付流程**
   - 支持支付宝支付
   - 生成的支付链接可在手机浏览器打开
   - 或生成二维码用支付宝扫码支付

## 登录风控与滑块验证码 🎉 v3.0（2026-10-16）

> 完整逆向过程见 `../mcd/docs/滑块验证码逆向进度.md`，结论与铁律见 `../mcd/FINAL_HANDOVER.md` 第十一节。

### 一句话结论

**干净住宅/本机 IP + 干净账号 → 发码直接出滑块（code 420），自动滑过即可登录，全链路（选店→下单→支付数据）已跑通，不需要 d 值。**

### 发码/登录风控码

| code | 含义 | 处理 |
|------|------|------|
| `420` | 正常出滑块（期望） | 滑过继续 |
| `707053` | 账号安全风险 | 换干净 IP + 合法 d + 好号 |
| `706070` | 设备环境异常 | 停手冷却 + 换独立环境（代码解决不了） |
| `801005` | 发码超限 | 单号降频 |

### 设备指纹 d

- `d` = 数美设备指纹（`"B"+smid` 或 `"D"+base64(...)`），native 生成，取值最省事是 frida hook `SmAntiFraud.getDeviceId()`。
- `d` 与 `token` 相互独立；**重启 APP 换新 d，token 不变**。
- 代码里 `d` **存在才加入请求**（`LoginManager` 从凭证读 `self.d` → `set_active_d()` → `build_headers` 用 ContextVar 注入），requests 结构不变。
- ⚠️ **铁律：一个 d 只配一个号、按真人频率用**。同一个 d 跨多账号/高频会被升级到 706070，重装重启都清不掉。规模化多 d 轮换 = 不做。

### 自动过滑块（生产化）

- `captcha_login_worker.py`：DrissionPage（真实 Chromium + CDP Actions，事件 `isTrusted:true`）+ `ddddocr` 识别缺口 + 物理轨迹，Redis 队列异步执行，带心跳/僵尸冻结清理。
- 必须走**住宅/干净 IP**，滑块与发码走同一出口 IP；headless 可行。
- 浏览器账密代理：配置 `PROXY_BROWSER_AUTH`（True=动态扩展带账密 / False=IP 白名单）。
- `x-mcd-sec-verify` = `base64("{punishId};{verifySdk};{毫秒时间戳};{rid}")`；mcd 的 punishId 与数美 PASS 的 rid **跨设备可配对**重发。
- 失败排查：worker 在滑块失败时自动存截图 + HTML 到 `captcha_debug/`。

### frida 真机取 d（2026-10-17 跑通）

想从真机抓数美设备指纹 `d`（或 SO 层密钥）时用 frida。**坑：frida/hluda 16.1.9 在本环境注入必崩**（`bootstrapper crashed signal 11 @ 0x1514`；spawn 则超时）。**解法 = frida 15.1.17 + attach（不是 spawn）**，实测真机成功 hook 到真实 d。

一键脚本（`../mcd/tools/frida/`）：
```bash
cd ../mcd/tools/frida
./getd.sh hook_sm_deviceid.js        # 抓 d
./getd.sh hook_native_hmac.js 90     # 抓 SO 层密钥
```
它自动：检测 frida-server 15.1.17(在跑就跳过)→启动 app→attach 注入「绕过脚本+你的 hook」→崩了自动重试。前提：**手机已解锁+联网**。细节见 `../mcd/FINAL_HANDOVER.md` 第十二节。

## 设备指纹池系统 🎉 v4.0（2026-10-17）

> 完整技术细节见 `../mcd/FINAL_HANDOVER.md` 第十三节。

### 一句话

不改机、单进程**批量伪造设备身份**：frida 在采集源注入假设备（android_id + native 系统属性 + token），让 app 自己的 native 加密 vdata 向数美注册，拿回独立 d（`B`+smid）。每个设备 = 固定 `token` + 独立 `d` + `tid` 成一套，入 Redis 设备池，登录时按账号领用（**一账号一设备，规避"同设备绑多号被封"**）。

### 组成

- `device_builder.py` — 造设备（frida 注入假设备 → app 自己注册拿真 smid；内置机型库，自带 `frida/` 下 js）
- `device_pool_redis.py` — Redis 设备池服务（补池 refill/serve、领用 claim/release/stats）
- `login_manager.ensure_device() / ensure_tid()` — 登录时自动领设备 / 无 d 补领 / `force_new` 换设备 + tid 7 天刷新
- `config.DEVICE_POOL_CONFIG`

### Redis 结构

| key | 类型 | 作用 |
|-----|------|------|
| `mcd:device:free` | List | 空闲设备 id 队列（RPUSH 入 / LPOP 领 = 先生成先取 FIFO） |
| `mcd:device:info` | Hash | field=设备id，value=该设备全部参数 JSON（**单 key，不膨胀**） |
| `mcd:device:account:{account}` | List | 账号领用过的设备 id（一账号可多设备） |
| `mcd:device:index` | Set | 已注册 smid，生成查重 |

### 用法

```bash
PY=/Users/houjie/venv/python3-forcrawl/bin/python   # frida 15.1.17
# 补池造设备（必须跑在接真机的本机）
$PY device_pool_redis.py serve        # 每 15 分钟补到 min_free；或 refill 补一次
# 领用（任意机，只连 Redis）
$PY device_pool_redis.py claim <账号>
$PY device_pool_redis.py stats
```

登录接口 `/api/login/send_code`（`use_new_device:true` 换设备）、`/api/login/verify` 会自动 `ensure_device` + `ensure_tid`；账号凭证（`mcd:credentials`）新增 `d` / `device_id` / `token` / `tid` / `tid_expire` 字段。

### token / d / tid 要点

- **token**：每设备 `uuid4().hex` 伪造，hook `AppInfoUtil.getToken/getUUID/getMcdDeviceToken` 注入（等价真机持久化 UUID），与 d 成一套。
- **d**：假设备**真实注册**拿回的 smid（`B`+smid），不是编造的。
- **tid**：服务端签发、**7 天有效**，用时 `activate_token` 激活、过期/换设备刷新（不在造设备时激活）。

### 端到端联调结论（2026-10-17，本地 Redis + server + captcha worker）

- 假号发码 **code_sent** → 链路 / 设备池 / token / tid / 代理全部有效 ✓
- `17717295039` → **707053**：换代理 / 直连住宅 IP / 全新设备 / token-d 成套 **都不变** → 老账号 + 陌生设备的**数美关联风控**（手机能登是因手机是信任设备），非代码问题。
- `18356966159` → **420 出滑块** → worker 滑动 #1~#4 全 `REJECT`：滑块**行为检测**拒绝自动轨迹（滑块逆向难点，独立问题）。
- 结论：设备池 / token / tid 工程全部正确；卡点是数美账号↔设备关联风控 + 滑块轨迹，属风控策略 / 滑块逆向范畴。
- `706070`「设备环境异常」：⚠️ **旧结论"pm clear 规避"已更正**——pm clear 只规避**注册**环节，**发码仍 706070**，详见下方 v5.0 章节。

---

## 本地造设备 🎉 v5.0（2026-10-21）

> 不用真机/frida，纯 Python + HTTP 本地批量造数美设备 d。核心文件 `mcd_shumei_d.py` / `shumei_shortbox_template.json` / `local_device_gen.py`。

### 原理（已跑通，不用真机）

```
真机 hook 一次拿 shortbox 模板(shumei_shortbox_template.json, 26字段)
  → 替换身份字段(a24 android_id / a46 机型 / a9/a40/a125 时间)
  → mcd_shumei_d.make_box 打包(deflate + AES-256-CBC + RSA-OAEP)
  → POST https://fp-it.fengkongcloud.com/deviceprofile/v4 (走新短效代理,无签名头)
  → 响应 {"code":1100,"detail":{"deviceId":"..."}} → d = "B"+deviceId
```
明文设备 JSON(a1~a175) = Java 层 `com.ishumei.smantifraud.dfp.SMSDK.v1` 入参（`hook_fake_device_v2.js` 的 `DUMP_V1` 开关 dump `[v1-IN-BOX]`/`[v1-IN-SHORTBOX]`，注册实际用 shortbox）。shortbox **不含 a84(Widevine) 等硬件根字段**，故可批量造独立设备、绕开真机**注册环节**的 706070。

### device_pool_redis 集成（调用方式不变）

- `config.DEVICE_POOL_CONFIG['build_mode']` = `'local'`(默认,本地造) / `'frida'`(真机造)；`refill`/`claim`/`serve` 用法不变。
- 新增 `python device_pool_redis.py test <真实手机号>` = 本地造 B 设备 + 走发码验证。

### 短效代理（utils / config 新增）

- `utils.get_short_proxy(city=None, count=1, force_new=False, as_dict=False)` / `extract_short_proxy_by_city(city)`：神龙短效按城市提取(area=国标6位码/sign不绑area/白名单)，Redis 池 `RPOPLPUSH` 多进程轮巡，不支持城市自动取最近(haversine)。
- 配置：`config.SHENLONG_*`、`SHORT_PROXY_CONFIG`、`SHENLONG_SUPPORTED`(城市→国标码)、`CITY_GEO`(经纬度)。
- 登录代理接入：`login_manager.get_login_proxy` —— 登录成功(sid)后才绑静态代理；未登录用短效(发码 force_new、其余复用)。
- `device_builder.register_one` 新增 `ensure_frida_server`(仿 getd.sh 自启 frida-server 15.1.17)。

### ⚠️ 706070 认知更正（重要，推翻 v4.0 旧结论）

实测：本地造 d **注册 code 1100 成功、d 格式有效**，但**发码仍 706070**。排查全清单都不是主因：

| 排查项 | 结论 |
|---|---|
| a18(adb / 无 SIM) | A/B 实测改了没用 ❌ |
| 移动流量开关 | shortbox 注册不含网络字段 ❌ |
| 号历史(新旧真号) | 全 706070 ❌ |
| 发码请求头(qf.a + qf.b) | 只差无关的 UA/v ❌ |
| 本地 vs 真机 | 都 706070 ❌ |
| tid | 00003TuN 正常值 ❌ |

真机(Pixel/Mi6/华为非root/全新 Google)、本地造的、所有真号**全 706070**，唯一能过的是**白名单测试号 13800138000**；天津朋友正常真机(没碰过测试环境)能过。

**结论：706070 是数美发码时的服务端「设备指纹组合 / IP / 行为环境」风控，不是单个客户端字段能解决；真机也绕不过，超出"造 d"范围。** 两个待验证根因(需干净 root 真机 + 干净住宅 IP)：① shortbox 模板来自被 frida 反复注册的 Pixel，不变字段组合被关联到已标记设备 → 需干净真机重新 hook 模板；② 神龙代理 IP 被识别 + 刚注册秒发码无正常行为。

---

## 云手机抓干净 shortbox 模板 🎉 v5.1（2026-10-21）

> 针对上面根因①:在**能过数美风控的多多云手机**上 hook 出干净 box/shortbox 明文,替换掉被 frida 污染的 Pixel 模板。
> 完整方法/用法见 `../mcd/tools/frida/云手机dumpbox用法.md`;连接见 `../mcd/docs/Mac连接Windows共享adb手机.md`。
>
> **★结论(2026-10-21 实测)**:706070 根因是 **d 的设备信誉**,不是 IP。换上这份干净云模板后,**本地造的 d 发码得 420(出滑块可过),不再 706070**;神龙代理 IP 无关。`local_device_gen.TEMPLATE` 已默认切到 `shumei_shortbox_template_cloud.json`。
> ⚠️ 签名版本坑:我们的密钥 `V4AK=HJ7..` 属 **7.0.41.0**,`v`/`p` 必须保持 `7.0.41.0`/`102`(声明 43.1 会被网关 403,因无 5DLU 密钥)。

### biz_scenario/biz_from 按接口对齐(2026-10-21 全流程抓包)

mitm 抓真机完整下单流程(`mcd/tools/mitm/mitm_mcd.py` + `compare_capture.py`)发现:真机每个接口的 `biz_scenario`/`biz_from` **按页面/业务分**,我们原来全写死 `102/1006` 是指纹破绽。已在 `mcd_api.BIZ_MAP` 按 path 对齐,`build_headers` 不传 biz 时自动查表:

| 接口 | biz_scenario/biz_from |
|---|---|
| passport/login/mobile | 500/1004 |
| passport/verifyCode/sms/send | 500/1011 |
| store/cities | 100/1002 ·· store/stores 103/1013 ·· vicinity 103/1012 ·· stores/{id} 101/1004 |
| spc/menu 101/1009 ·· products/detail/{code} | 101/1001 |
| cart/carts | 101/1007 |
| order/confirmation/validationinfo 200/1004 ·· promotion 202/1020 ·· rightcard 202/1019 | |
| store/getNearest/{id} 202/1021 ·· order/orders | 202/1012 |
| cashier/channels 501/1001 ·· cashier/preorder | 501/1002 |

biz_* 不参与 HMAC,安全可改。`v`/`p`(签名头)因密钥锁定仍保持 7.0.41.0/102。

### 产出(实测)

- `shumei_shortbox_template_cloud.json`(29字段,注册实际用它) / `shumei_fullbox_cloud.json`(82字段,参考)。
- **干净 vs 旧 Pixel 脏模板差异**:`a18` 由 `{ABSENT, adb}`→`{LOADED, mtp}`;**多出 `a2/a97`(同一 base64 签名值)、`a103`(uuid)**;`a167/a168` 由 0→787552;`a4`、`a85(+apps)`、`a121/a135` 也不同。即旧模板不止 a18 脏,还缺字段 + 有可疑零值。
- ⚠️ `a2/a97` 疑似对 box 的签名,换 a24/a46 后可能要重算,**勿直接抄,待 jadx 验证**。

### 方法要点(别重试很多次)

- **两套防护**:数美(云手机过)vs app 原生壳 `libmsaoaidsec`(反frida,检测到就故意崩)。
- **配方**:`spawn + frida_bypass_combined_local.js + hook_dump_box_plain.js(单脚本) + 重试竞速`。只能 2 个脚本(DexHelper patch 要在 25ms 内赢竞速,多脚本必崩);frida-server **不用改名**,但 **frida 不认 ADB_SERVER_SOCKET**,远程设备要跑 `adb_relay.py` 把本机 5037 中继到 Windows adb server。
- 一键:`FRIDA_DEV=127.0.0.1:63373 PY mcd/tools/frida/dumpbox_cloud.py 12 30 <outdir>`,全自动主动触发注册,抓到 v1 入参自动解析存 JSON。

---

## 代理池管理系统 🎉 v2.0

### 功能概览

1. **代理池管理**
   - 增删查改代理
   - 自动查询代理 IP 城市
   - 5种状态管理（active/slow/observation/failed/disabled）
   - 健康检查（支持缓存、批量检查）

2. **账号代理绑定**
   - 账号-代理长期绑定（Redis 持久化）
   - 城市优先分配（同城市优先）
   - 负载均衡（单代理最多5个账号）
   - 失败计数追踪（时间窗口10分钟）

3. **四级智能容错**
   - 阶段1：首次失败 → 立即重试
   - 阶段2：连续失败3次 → 临时切换 + 观察期（5分钟）
   - 阶段2.5：观察期内连续失败10次 → 立即永久切换
   - 阶段3：观察期结束 → 健康检查决定（恢复/永久切换）

4. **定时任务调度**
   - 全量健康检查：每12小时
   - 观察期检查：每5分钟
   - 失败记录清理：每天凌晨3点

### 使用示例

#### 1. 添加代理到代理池

```python
from proxy_pool import ProxyPool

pool = ProxyPool()

# 添加代理（自动查询城市）
proxy = pool.add_proxy(
    host='61.155.141.105',
    port=20080,
    username='user',
    password='pass',
    protocol='http'
)
```

#### 2. 使用代理登录（自动）

```python
from login_manager import LoginManager

# 创建登录管理器（自动分配代理）
manager = LoginManager(
    phone="13800138000",
    use_redis=True,
    city="上海"  # 可选：优先使用上海的代理
)

# 登录（自动使用绑定的代理）
token, sid, meddy_id = manager.ensure_login(auto_relogin=True)
```

#### 3. 启动 HTTP API 服务器

```bash
# 启动服务器
python mcd_server.py

# 或使用 gunicorn（推荐）
gunicorn -w 4 -b 0.0.0.0:5001 mcd_server:app
```

#### 4. HTTP API 调用

```bash
# 查看代理池状态
curl http://localhost:5001/api/proxy/pool/status

# 添加代理
curl -X POST http://localhost:5001/api/proxy/pool/add \
  -H "Content-Type: application/json" \
  -d '{
    "host": "61.155.141.105",
    "port": 20080,
    "username": "user",
    "password": "pass",
    "protocol": "http"
  }'

# 查看账号绑定关系
curl http://localhost:5001/api/proxy/account/binding

# 获取代理池统计
curl http://localhost:5001/api/proxy/statistics
```

#### 5. 启动定时任务调度器

```bash
# 前台运行
python scheduler.py

# 后台运行
nohup python scheduler.py > scheduler.log 2>&1 &

# 或使用 crontab
# crontab -e
# @reboot cd /path/to/mcd_project && python scheduler.py >> scheduler.log 2>&1
```

### HTTP API 接口列表

| 接口路径 | 方法 | 功能 |
|---------|------|------|
| `/api/proxy/pool/status` | GET | 查看代理池状态 |
| `/api/proxy/pool/list` | GET | 获取所有代理列表 |
| `/api/proxy/pool/add` | POST | 添加代理到代理池 |
| `/api/proxy/pool/remove` | POST | 移除/禁用代理 |
| `/api/proxy/account/binding` | GET | 查看账号代理绑定关系 |
| `/api/proxy/account/switch` | POST | 手动切换账号代理 |
| `/api/proxy/statistics` | GET | 获取代理池统计数据 |
| `/api/proxy/health/check` | POST | 手动触发健康检查 |

### 配置说明

所有配置项在 `config.py` 中可调整：

```python
# 请求超时时间（秒）
PROXY_REQUEST_TIMEOUT = 15

# 代理失败容错配置
PROXY_FAILURE_CONFIG = {
    'observation_threshold': 3,           # 加入观察期的失败次数阈值
    'permanent_switch_threshold': 10,     # 观察期内永久换绑的失败次数阈值
    'observation_period': 5 * 60,         # 观察期时长（秒）
    'failure_window': 10 * 60,            # 失败计数时间窗口（秒）
}

# 代理池配置
PROXY_POOL_CONFIG = {
    'slow_threshold': 3.0,                # 响应时间阈值（秒）
    'max_fail_count': 5,                  # 连续失败多少次标记为failed
    'max_accounts_per_proxy': 5,          # 单个代理最大账号负载
}
```

### 技术亮点

1. **全局单例模式** - 避免重复初始化
2. **Redis 持久化** - 所有状态数据存储在 Redis
3. **无侵入集成** - LoginManager 自动处理，业务代码无需修改
4. **城市感知** - 自动查询代理 IP 城市
5. **健康检查缓存** - 30秒 TTL，避免重复检查
6. **批量处理** - 每批10个代理，间隔6秒
7. **日志增强** - 统一 log_info()，支持 flush=True

---

## 更新日志

详见 [CHANGELOG.md](CHANGELOG.md)

---

## License

MIT

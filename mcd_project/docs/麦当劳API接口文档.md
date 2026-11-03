# 麦当劳 API 项目接口文档

服务端：`mcd_server.py`（Flask，默认监听 `0.0.0.0:5001`）
底层实现：`mcd_api.py`（所有具体业务逻辑）
登录管理：`login_manager.py`（基于 Redis/文件的多账号凭证管理）
客户端示例：`run_api_client.py`

统一响应格式（Flask 层）：
```json
{
  "success": true/false,
  "data": { ... },
  "message": "文字说明"
}
```
`data` 的具体字段来自麦当劳官方上游接口的透传结果，代码中未对其做二次封装，故以下每个接口的"响应结果"均为 `mcd_api.py` 中对应函数返回的 `data` 字典内容。

---

## 一、登录相关接口

> **公共参数（v2.1 新增，登录与 `/api/mcd/` 下单业务接口通用）**
> | 参数 | 默认 | 说明 |
> |---|---|---|
> | `province` | 无 | 省份（如"江苏"）。zdtps 按 `"省,市"` 提代理，取不到退仅省 |
> | `city` | 无 | 城市（如"南京"）。短效代理按省+市隔离/复用 |
> | `merchant_id` | `100001` | 商户ID。决定凭证 key `mkt:prod:<商户>:credentials` 与代理配置/统计隔离 |
> | `proxy_source` | `zdtps` | 代理源：`zdtps`（站大爷短效）/ `shenlong`（神龙短效） |
>
> **省/市 生效与绑定规则**：请求**没传** province 和 city → 取凭证里**已绑定**的省市；**传了其一** → 用传入值（本次不读凭证）。账号**首次（未绑定）**会把生效省市写入凭证绑定；**已绑定**则不再改动这两字段，换城市需显式调 `POST /api/login/bind_city`。

### 1. 发送验证码
`POST /api/login/send_code`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号（11位） |
| use_new_device | bool | 否 | 是否强制生成新设备 Token，默认 false |
| province / city | string | 否 | 省份/城市，见上方公共参数 |
| merchant_id | string | 否 | 商户ID，默认 100001 |
| proxy_source | string | 否 | 代理源，默认 zdtps |

内部逻辑：
- 方案①：服务端不直接发码，统一入队交给 `captcha_login_worker`（自动过滑块→发码），客户端轮询 `/api/login/captcha_status`
- 入队前解析本次生效省/市并绑定（未绑定时）；生效省/市随队列下发给 worker，发码按同一省市取短效代理

响应：
```json
{"success": true, "data": {"status": "queued"}, "message": "已入队，请轮询 /api/login/captcha_status"}
```

### 2. 验证码登录
`POST /api/login/verify`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号 |
| verify_code | string | 是 | 6位短信验证码 |
| province / city / merchant_id / proxy_source | string | 否 | 见公共参数 |

内部调用 `login_manager.do_login()` → `mcd_api.login()`，登录成功后凭证自动落盘；并记录一次检测时间+状态（`check_status=valid`）。

响应：
```json
{
  "success": true,
  "data": {"token": "...", "sid": "会话SID", "meddy_id": "用户meddyId"},
  "message": "登录成功"
}
```

### 3. 查询登录状态
`GET /api/login/status`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号 |
| province / city / merchant_id / proxy_source | string | 否 | 见公共参数 |

逻辑：先从存储加载凭证，再调用上游校验有效性。**每次检测都会把结果写回凭证**：`checked_at`（检测时间，区别于 cookie 保存时间 `saved_at`）+ `check_status`（`valid` 在线 / `invalid` 掉线失效 / `error` 检测异常）。有 sid 也可能掉线，以 `check_status` 为准。

响应（已登录）：
```json
{
  "success": true,
  "data": {
    "is_logged_in": true,
    "token": "...", "sid": "...", "meddy_id": "...",
    "user_info": {"phone": "", "name": "", "points": "0", "meddy_id": "", "identity": ""},
    "checked_at": "2026-10-23T12:00:00", "check_status": "valid"
  },
  "message": "已登录"
}
```
响应（未登录/已失效）：
```json
{"success": true, "data": {"is_logged_in": false, "checked_at": "...", "check_status": "invalid"}, "message": "登录已失效"}
```

### 4. 绑定/修改账号省市（v2.1 新增）
`POST /api/login/bind_city`

显式覆盖凭证里的 `province`/`city`。已绑定账号换城市须调本接口。

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号 |
| province | string | 否* | 省份 |
| city | string | 否* | 城市（province/city 至少传一个） |
| merchant_id | string | 否 | 商户ID，默认 100001 |

响应：`{"success": true, "data": {"phone": "...", "province": "江苏", "city": "南京"}, "message": "省市已绑定/修改"}`

### 5. 查询账号数据（v2.1 新增）
`GET /api/accounts`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 否 | 传则查单个（原样返回）；**不传则只返回有 sid（已登录）的账号** |
| merchant_id | string | 否 | 商户ID，默认 100001 |

返回 `list`，每项 = 凭证 value（token/sid/d/province/city/checked_at/check_status/saved_at…）+ `phone`。

响应：`{"success": true, "data": [ {"phone":"...","sid":"...","check_status":"valid",...} ], "total": N}`

### 6. 开通麦金卡（异步，自动过滑块）（v2.1 新增）
`POST /api/member/open`

入队交给 `captcha_login_worker`（与发码同一 worker、同一队列，按 `type` 分发，串行处理；worker 自动过数美滑块、420 可重试多次）。

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号（须已登录） |
| sku_id | int | 是 | 套餐 skuId（见 `get_member_benefit_spec`） |
| shop_id | int | 否 | 默认 1 |
| city_code | string | 否 | 默认 310100 |
| province / city / merchant_id / proxy_source | string | 否 | 见公共参数 |

响应：`{"success": true, "data": {"status": "queued"}, "message": "已入队，请轮询 /api/member/open_status"}`（worker 未运行返回 503）

`GET /api/member/open_status?phone=` → 轮询开通状态：
- `{"status":"queued"}` / `{"status":"processing"}` / `{"status":"none"}`
- 成功：`{"status":"done","orderId":"ECS...","payId":"111...","msg":"开通下单成功"}`
- 失败：`{"status":"failed","msg":"..."}`（如滑块多次未通过、6000232 设备异常）

拿到 `orderId`/`payId` 后走 `get_payment_channels`(source=1,shop_id) + `create_payment`(ALI,order_id,shop_id,source=1) 拿支付宝支付数据。客户端一条龙：`python run_api_client.py mcgold`。

> 注：`/api/mcd/create_ectrade_order` 是**底层一次性**下单（不自动过滑块，返回 420 需自行处理），自动过滑块的开通走 `/api/member/open`。

---

## 二、通用接口 `/api/mcd/<function_name>`

`GET/POST /api/mcd/<function_name>`

这是一个反射式路由：`function_name` 对应 `mcd_api.py` 中同名的函数名，服务端用 `getattr(mcd_api, function_name)` 动态调用，参数原样透传给该函数。

鉴权参数（二选一）：
- `phone`：服务端会从 Redis 读取该手机号已保存的 `token`/`sid`/`meddy_id`
- 或直接传 `token` + `sid`

若函数需要 `mcd_id` 但未传，会自动用 Redis 中该手机号对应的 `meddy_id` 补全。

**v2.1 公共参数**：`province`/`city`/`merchant_id`/`proxy_source` 会被服务端提取（**不透传**给业务函数），用于定位凭证、决定取短效代理的地域与代理源，规则同登录接口（见第一节顶部公共参数）。`run_api_client.py` 里 `SEND_LOCATION` 开关控制业务请求是否自动带省市（True 带 / False 不带），`PROXY_SOURCE` 常量控制代理源（`zdtps`/`shenlong`/`''`=默认），方便测不同代理。

**v2.0 重要更新：代理自动管理**
- 所有通过此接口的请求会自动使用账号绑定的代理
- 首次请求时自动为账号分配代理（负载均衡）
- 请求失败自动触发四级容错机制（重试、临时切换、观察期、永久切换）
- 业务代码无需关心代理管理，完全透明

错误处理：
- 函数不存在 → 404，`{"success": false, "message": "接口 xxx 不存在"}`
- 参数错误（TypeError）→ 400
- 调用异常 → 500
- 代理请求失败 → 自动容错处理（不会直接返回错误）

以下是 `function_name` 可取值及各自参数说明（均可通过该通用接口调用）：

### 城市 / 门店

**get_all_cities** — 获取所有城市分组信息
- 参数：`phone`（或 token+sid）
- 对应上游：`GET /bff/store/cities/group`
- 响应：城市分组数据，含 `hotCities`（热门城市）、`groups`（按拼音分组的城市列表）

**get_city_by_location** — 按经纬度反查所在城市
- 参数：`latitude`、`longitude`
- 对应上游：`GET /bff/store/cities`
- 响应：`{"city": {"code": "", "name": ""}, ...}`

**search_stores** — 关键词搜索门店
- 参数：`city_code`、`keyword`、`page_no`(默认1)、`page_size`(默认10)、`hot_tag_code`、`be_type`、`daypart_codes`
- 对应上游：`GET /bff/store/stores`
- 响应：`{"stores": [...], "totalSize": N}`

**get_nearby_stores** — 获取附近门店
- 参数：`latitude`、`longitude`、`show_type`(默认'2')、`be_type`、`order_type`(默认1，1=堂食/2=外卖)、`keyword`
- 对应上游：`GET /bff/store/stores/vicinity`
- 响应：`{"stores": [...]}`

**get_nearest_store_info** — 下单前校验最近门店（点击去支付时用）
- 参数：`store_code`、`latitude`、`longitude`、`be_code`
- 对应上游：`POST /bff/store/stores/getNearest/{store_code}`
- 响应：门店详情对象

### 菜单 / 商品

**get_store_menu** — 获取门店菜单
- 参数：`store_code`、`be_code`、`order_type`(默认1)、`daypart_code`（不传则按当前时间自动计算，见下方"时段代码"说明）、`order_mode`(默认'0')、`pin_id`
- 对应上游：`GET /bff/spc/menu`
- 响应：菜单分类及商品列表

**get_product_detail** — 获取商品详情（含规格/时段/优惠券信息）
- 参数：`product_code`、`store_code`、`be_code`、`order_type`(默认1)、`daypart_code`、`card_id`、`coupon_code`、`coupon_id`、`channel_code`(默认'03')、`order_mode`(默认'0')、`page_source`(默认1)、`pin_id`、`product_promotions`
- 对应上游：`POST /bff/spc/products/detail/{product_code}`
- 响应：商品详情，含 `modification`（可选规格，如冰度/加料等）结构，用于后续 `add_to_cart`

### 购物车

**get_cart** — 查询购物车
- 参数：`store_code`、`be_code`、`order_type`(默认1)、`daypart_code`
- 对应上游：`GET /bff/cart/carts`
- 响应：购物车内容

**clear_cart** — 清空购物车
- 参数：`store_code`、`be_code`、`order_type`(默认1)、`daypart_code`、`store_name`、`channel_code`(默认'03')、`order_mode`(默认'0')、`pin_id`、`pickup_time_type`
- 对应上游：`PUT /bff/cart/carts/empty`
- 响应：清空后的购物车状态

**add_to_cart** — 添加商品到购物车
- 核心参数：`store_code`、`be_code`、`product_code`、`product_name`、`product_image`、`quantity`(默认1)、`order_type`(默认1)、`daypart_code`、`product_type`(默认'1'，1=单品/7=套餐)、`combo_items`（套餐子项，仅套餐需要）、`modification`（规格，来自 `get_product_detail` 返回结构提取）
- 附加参数：`suggestion_embedding`、`store_name`、`channel_code`(默认'03')、`order_mode`(默认'0')、`pin_id`、`pickup_time_type`、`card_id`、`card_type`、`coupon_code`、`coupon_id`、`gm_assist_service_code`、`membership_code`、`sequence`(默认-1)、`animation_id`
- 对应上游：`PUT /bff/cart/carts`
- 响应：更新后的购物车内容

### 订单

**get_order_validation_info** — 结算第一步：订单验证信息
- 参数：`store_code`、`channel_code`(默认'03')、`order_type`(默认1)、`cart_type`(默认1)、`daypart_code`、`be_code`、`table_id`、`date`、`time`、`pickup_time_type`、`pin_id`、`order_mode`(默认0)
- 对应上游：`GET /bff/order/confirmation/validationinfo`
- 响应：验证信息（如门店营业状态、可下单校验结果）

**get_order_promotion_info** — 结算第二步：促销/优惠券信息
- 参数：`store_code`、`cart_items`（购物车商品列表）、`be_type`(默认'1')、`daypart_code`、`order_type`(默认'1')、`eat_type_code`(默认'eat-in')、`tableware_code`(默认'no')、`order_mode`(默认'0')、`pin_id`、`real_total_amount`(默认'0')
- 对应上游：`POST /bff/order/confirmation/promotion`
- 响应：可用促销/优惠券及计算后金额

**submit_order** — 提交订单
- 参数：`store_code`、`cart_items`、`be_type`(默认'1')、`daypart_code`、`order_type`(默认'1')、`eat_type_code`(默认'eat-in')、`tableware_code`(默认'no')、`order_mode`(默认'0')、`pin_id`、`latitude`、`longitude`、`date`、`time`、`menu_card_list`、`real_total_amount`
- 对应上游：`POST /bff/order/orders`
- 签名说明：优先尝试通过 `v5_rpc_client`（若可用）获取 v5 签名（`x-mcd-sign` 头），失败则降级为 v4 签名（`authorization` + `x-hmac-digest`）
- 响应：`{"orderId": "...", "payId": "...", ...}`

**get_order_list** — 查询历史订单列表
- 参数：`cursor`(默认''，分页游标)、`page_size`(默认10)
- 对应上游：`POST /bff/order/orders/list`
- 响应：`{"list": [{"omsOrder": {"orderId", "createTime", "orderStatus", "storeName", "realTotalAmount", ...}}], "hasNext": bool}`

**get_order_detail** — 查询订单详情
- 参数：`order_id`
- 对应上游：`GET /bff/order/orders/{order_id}`
- 响应：订单详情对象

**get_member_benefit_spec** — 获取麦金卡(付费会员)开通规格（v2.1 新增）
- 参数：`benefit_id`(默认106=麦金卡)、`scene`(默认0)
- 对应上游：`GET /bff/myrewards/benefit/detail/spec/{benefit_id}?scene=0`
- 响应：`data.skuDetails`（skuId 17690=15天9.9 / 12534=30天19 / 12535=90天39 / 12536=365天118）+ `shopId`、`openRules` 等

**create_ectrade_order** — 电商下单 / 开通麦金卡（v2.1 新增）
- 参数：`sku_id`(套餐 skuId，必填)、`city_code`(默认310100)、`shop_id`(默认1)、`count`(默认1)、`scene`(默认1)、`execute_type`(默认1)、`gift`(默认0)、`sec_kill`(默认0)、`pre_commit_no`(不传自动=meddyId+毫秒时间戳)；`mcd_id` 由服务端自动注入
- 对应上游：`POST /bff/ectrade/order/create`，body `{cityCode, goods:[{count,skuId}], preCommitNo, shopId, scene, executeType, gift, secKill}`
- 开通麦金卡 = 调本接口下单拿 orderId/payId → 再走 get_payment_channels + create_payment 支付
- ⚠️ 设备 `d` 被风控标记会返回 `code=6000232`「账号环境存在异常」；需干净 d

### 支付

**get_payment_channels** — 获取可用支付渠道
- 参数：`order_id`、`pay_id`、`mcd_id`、`source`(默认0)
- 对应上游：`GET /bff/cashier/channels`
- 响应：`{"channelInfos": [{"name", "code", "freePayName", ...}]}`

**create_payment** — 创建支付（生成支付宝支付串）
- 参数：`pay_id`、`pay_channel`(默认'ALI'，ALI=支付宝/WX=微信/UNION=银联)、`free_pay_flg`(默认false，是否免密支付)、`source`(默认0)
- 对应上游：`POST /bff/cashier/preorder`
- 响应：`{"orderId", "payId", "payStatus", "channelPayDestination", "channelPayData"}`
  - `channelPayData` 是支付宝 APP 支付专用字符串，仅能被支付宝 SDK 调起，不能生成通用二维码/支付链接

---

## 三、代理池管理接口（v2.0 新增）

### 1. 查看代理池状态
`GET /api/proxy/pool/status`

无参数。

响应：
```json
{
  "success": true,
  "data": {
    "total_proxies": 3,
    "active_proxies": 2,
    "failed_proxies": 0,
    "observation_proxies": 1,
    "bound_accounts": 4
  }
}
```

### 2. 获取所有代理列表
`GET /api/proxy/pool/list`

无参数。

响应：
```json
{
  "success": true,
  "data": [
    {
      "id": "mcd_proxy_001",
      "host": "61.155.141.105",
      "port": 20080,
      "username": "user",
      "protocol": "http",
      "status": "active",
      "city": "上海",
      "bound_accounts": 2,
      "success_count": 150,
      "fail_count": 3,
      "response_time": 0.43,
      "last_check": "2026-10-10T16:57:56",
      "created_at": "2026-10-10T10:00:00"
    }
  ]
}
```

**代理状态说明：**
- `active` - 正常可用
- `slow` - 响应慢（>3秒）但可用
- `observation` - 观察期（临时故障，5分钟观察期）
- `failed` - 已失效
- `disabled` - 已禁用（手动或连续失败5次）

### 3. 添加代理到代理池
`POST /api/proxy/pool/add`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| host | string | 是 | 代理IP地址 |
| port | int | 是 | 代理端口 |
| username | string | 否 | 代理用户名（无密码代理可不传） |
| password | string | 否 | 代理密码 |
| protocol | string | 否 | 协议类型，默认 'http'（可选 'https'/'socks5'） |
| city | string | 否 | 代理所属城市（不传则自动查询） |

响应：
```json
{
  "success": true,
  "data": {
    "id": "mcd_proxy_003",
    "host": "61.155.141.105",
    "port": 20080,
    "city": "上海",
    "status": "active"
  },
  "message": "代理添加成功"
}
```

### 4. 移除/禁用代理
`POST /api/proxy/pool/remove`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| proxy_id | string | 是 | 代理ID（如 'mcd_proxy_001'） |

响应：
```json
{
  "success": true,
  "message": "代理已禁用"
}
```

**注意：** 此接口不会物理删除代理，而是将状态标记为 `disabled`，已绑定的账号会在下次请求时自动切换到其他代理。

### 5. 查看账号代理绑定关系
`GET /api/proxy/account/binding`

无参数。

响应：
```json
{
  "success": true,
  "data": [
    {
      "phone": "13800138000",
      "proxy_id": "mcd_proxy_001",
      "temp_proxy_id": null,
      "bound_at": "2026-10-10T10:00:00",
      "in_observation": false,
      "observation_start": null,
      "observation_until": null,
      "observation_fail_count": 0,
      "total_fail_count": 0,
      "fail_window_start": "2026-10-10T10:00:00",
      "last_fail_time": null,
      "request_count": 25,
      "last_request": "2026-10-10T15:30:00"
    }
  ]
}
```

**字段说明：**
- `proxy_id` - 主代理ID（长期绑定）
- `temp_proxy_id` - 临时代理ID（观察期时使用）
- `in_observation` - 是否在观察期
- `observation_fail_count` - 观察期内失败次数（≥10次会立即永久切换）
- `total_fail_count` - 失败窗口（10分钟）内的总失败次数
- `request_count` - 累计请求次数

### 6. 手动切换账号代理
`POST /api/proxy/account/switch`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号 |

响应：
```json
{
  "success": true,
  "data": {
    "new_proxy_id": "mcd_proxy_002"
  },
  "message": "代理切换成功: mcd_proxy_002"
}
```

**逻辑：** 自动排除当前代理，从可用代理池中选择负载最低的代理进行永久绑定。

### 7. 获取代理池统计数据
`GET /api/proxy/statistics`

无参数。

响应：
```json
{
  "success": true,
  "data": {
    "total_proxies": 3,
    "by_status": {
      "active": 2,
      "observation": 1
    },
    "by_city": {
      "上海": {
        "count": 2,
        "bound_accounts": 3
      },
      "北京": {
        "count": 1,
        "bound_accounts": 1
      }
    },
    "total_bound_accounts": 4,
    "total_requests": 523,
    "total_success": 508,
    "total_fail": 15,
    "success_rate": "97.13%"
  }
}
```

**字段说明：**
- `by_status` - 按状态分组的代理数量
- `by_city` - 按城市分组的代理数量和绑定账号数
- `total_bound_accounts` - 总绑定账号数（实时计算）
- `success_rate` - 全局成功率

### 8. 手动触发健康检查
`POST /api/proxy/health/check`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| proxy_id | string | 是 | 代理ID |

响应：
```json
{
  "success": true,
  "data": true
}
```

**返回值：** `true` 表示健康，`false` 表示失败。此接口会更新代理的健康状态和响应时间。

---

## 三续、短效代理源 / 白名单 / 用量统计（v2.1 新增）

短效代理源配置存于 Redis Hash `mkt:prod:<商户>:proxy`（field=源名 `zdtps`/`shenlong`，value=该源配置 JSON）；服务启动自动 seed 默认配置（只补缺失）。默认源 `zdtps`（站大爷），IP 存活 `timespan=3`（1-5 等级），池 TTL = 过期时间 `expire`(默认180) − 20 = 160s，与神龙短效(~3min)一致。

### 1. 查询本机出口 IP
`GET /api/proxy/egress_ip`

直连探测运行本服务机器的公网出口 IP（多出口全列），用于确认要加哪些 IP 进代理白名单。
响应：`{"success": true, "data": {"egress_ips": ["1.2.3.4"]}}`

### 2. 查询白名单
`GET /api/proxy/whitelist`（可选 `?merchant_id=`）→ `{"success": true, "data": {"whitelist": ["1.2.3.4", ...]}}`

### 3. 加白名单
`POST /api/proxy/whitelist/add`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| ip | string | 否 | 不传=加**本机当前出口IP**；传则加指定 IP（逗号分隔多个） |
| merchant_id | string | 否 | 商户ID，默认 100001 |

### 4. 删白名单
`POST /api/proxy/whitelist/del` — body `{ip（必填,逗号分隔多个）, merchant_id?}`

### 5. 短效代理用量统计
`GET /api/proxy/stats`

在**取代理时**按 `源+省+市+日期` 累计：`req`=请求（使用）次数（含池内复用）、`proxy`=消耗（从上游新提取）的短效代理数（复用不计）。

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| days | int | 否 | 近 N 天，默认 7 |
| date | string | 否 | 只查某天 `YYYY-MM-DD`（传了则忽略 days） |
| source / province / city | string | 否 | 过滤维度 |
| merchant_id | string | 否 | 商户ID，默认 100001 |

响应：
```json
{"success": true, "data": {
  "tree": {"zdtps": {"江苏": {"南通": {"2026-10-23": {"req": 3, "proxy": 3}}}}},
  "totals": {"req": 3, "proxy": 3},
  "dates": ["2026-10-17", "...", "2026-10-23"]
}}
```

---

## 四、健康检查

`GET /health`

无参数。

响应：
```json
{"status": "ok", "service": "mcd-api-refactored"}
```

---

## 五、通用说明

### 1. 代理池工作机制（v2.0 新增）

#### 自动代理分配
- 所有业务接口（通过 `/api/mcd/<function_name>` 调用）会自动使用账号绑定的代理
- 首次请求时自动分配代理（优先同城市代理，负载均衡）
- 单个代理最多绑定 5 个账号

#### 四级容错机制
代理请求失败时自动触发智能容错：

**阶段1：立即重试**
- 首次失败 → 等待 1 秒后使用相同代理重试
- 连续失败 2 次 → 再次等待 1 秒后重试

**阶段2：临时切换 + 观察期**
- 连续失败 3 次 → 临时切换到新代理，原代理进入观察期（5分钟）
- 观察期内使用临时代理，主代理暂停使用

**阶段2.5：观察期内连续失败保护**
- 观察期内连续失败 10 次 → 立即永久切换代理（不等观察期结束）

**阶段3：观察期结束判断**
- 观察期结束后自动健康检查主代理：
  - 健康 → 恢复使用主代理
  - 失败 → 永久切换到临时代理

#### 失败计数窗口
- 失败计数有 10 分钟滑动窗口
- 10 分钟内的失败会累计，超过 10 分钟的失败自动清零
- 避免历史失败永久影响代理状态

#### 定时任务（scheduler.py）
- **全量健康检查**：每 12 小时，检查所有代理状态
- **观察期检查**：每 5 分钟，检查观察期账号是否恢复
- **失败记录清理**：每天凌晨 3 点，清理过期失败记录
- **绑定计数同步**：每 3 分钟，同步代理绑定账号数缓存

### 2. 时段代码（daypartCode）
多数接口的 `daypart_code` 不传时会调用 `get_current_daypart_code()` 按服务器当前时间自动计算：

| 时间段 | 代码 | 含义 |
|---|---|---|
| 05:00–10:29 | 1 | 早餐 |
| 10:30–14:29 | 8 | 午餐 |
| 14:30–16:59 | 4 | 下午茶 |
| 17:00–21:59 | 5 | 夜市 |
| 22:00–04:59 | 6 | 宵夜 |

### 2. 时段代码（daypartCode）
多数接口的 `daypart_code` 不传时会调用 `get_current_daypart_code()` 按服务器当前时间自动计算：

| 时间段 | 代码 | 含义 |
|---|---|---|
| 05:00–10:29 | 1 | 早餐 |
| 10:30–14:29 | 8 | 午餐 |
| 14:30–16:59 | 4 | 下午茶 |
| 17:00–21:59 | 5 | 夜市 |
| 22:00–04:59 | 6 | 宵夜 |

### 3. 堂食/外卖类型
不同接口对 `order_type`/`be_type` 的取值含义略有差异（同一字段在不同上游接口中编码不一致，代码中已按各接口分别处理）：
- 多数接口：`1`=堂食，`2`=外卖
- `clear_cart` 中的 `be_type`：`0`=堂食，`1`=外卖

调用时建议按各函数文档中给出的取值为准，不要跨接口套用。

### 4. 鉴权凭证来源
所有业务接口（除登录接口外）都需要 `token` + `sid`，两者的获取方式：
- 通过 `phone` 参数由服务端自动从 Redis（`mcd:credentials` 哈希表，字段为手机号）读取，凭证由登录流程写入
- 或调用方直接传入已有的 `token`/`sid`

### 5. 请求签名机制（供理解 `mcd_api.py` 内部实现）
- v4 签名：`Authorization` 头由 `hmac-auth-v1` 格式的 HMAC-SHA256 签名组成，签名内容包含请求方法、路径、排序后的查询字符串、时间等；请求体额外用 `X-Hmac-Digest`（HMAC-SHA256）签名
- v5 签名：通过独立的 `v5_rpc_client` 模块获取 `x-mcd-sign`，仅 `submit_order` 优先尝试使用，不可用时自动降级为 v4
- 手机号、验证码在请求体中以 AES-128-ECB + PKCS7 加密后传输

### 6. 多进程/多实例说明
`mcd_server.py` 中的 `login_manager_cache` 是进程内内存字典（`{phone: LoginManager实例}`），仅用于避免重复创建对象；真正的凭证持久化在 `LoginManager(use_redis=True)` 对应的 Redis 中，因此多进程部署时凭证是共享一致的，但缓存对象本身不共享。

代理池管理同样基于 Redis 持久化，支持多进程/多实例部署：
- 代理池数据（`mcd:proxy:{id}`）存储在 Redis
- 账号绑定关系（`mcd:account_binding:{phone}`）存储在 Redis
- 全局单例模式（`get_proxy_pool()`、`get_binding_manager()`）确保进程内不重复初始化
- 多进程间通过 Redis 共享代理状态和绑定关系

---

## 六、使用示例

### 完整下单流程示例

```python
import requests

API_BASE = 'http://127.0.0.1:5001'
PHONE = '13800138000'

# 1. 发送验证码
response = requests.post(f'{API_BASE}/api/login/send_code', json={'phone': PHONE})
print(response.json())

# 2. 验证码登录
response = requests.post(f'{API_BASE}/api/login/verify', json={
    'phone': PHONE,
    'verify_code': '123456'
})
print(response.json())

# 3. 查询附近门店
response = requests.post(f'{API_BASE}/api/mcd/get_nearby_stores', json={
    'phone': PHONE,
    'latitude': '31.230416',
    'longitude': '121.473701'
})
stores = response.json()['data']['stores']
store = stores[0]

# 4. 获取门店菜单
response = requests.post(f'{API_BASE}/api/mcd/get_store_menu', json={
    'phone': PHONE,
    'store_code': store['code'],
    'be_code': store['beCode']
})
menu = response.json()['data']

# 5. 获取商品详情
product = menu['categories'][0]['products'][0]
response = requests.post(f'{API_BASE}/api/mcd/get_product_detail', json={
    'phone': PHONE,
    'product_code': product['code'],
    'store_code': store['code'],
    'be_code': store['beCode']
})
product_detail = response.json()['data']

# 6. 清空购物车
response = requests.post(f'{API_BASE}/api/mcd/clear_cart', json={
    'phone': PHONE,
    'store_code': store['code'],
    'be_code': store['beCode']
})

# 7. 添加商品到购物车
response = requests.post(f'{API_BASE}/api/mcd/add_to_cart', json={
    'phone': PHONE,
    'store_code': store['code'],
    'be_code': store['beCode'],
    'product_code': product['code'],
    'product_name': product['name'],
    'product_image': product['image'],
    'quantity': 1
})
cart = response.json()['data']

# 8. 提交订单
response = requests.post(f'{API_BASE}/api/mcd/submit_order', json={
    'phone': PHONE,
    'store_code': store['code'],
    'cart_items': cart['cartItems'],
    'latitude': '31.230416',
    'longitude': '121.473701'
})
order = response.json()['data']

# 9. 创建支付
response = requests.post(f'{API_BASE}/api/mcd/create_payment', json={
    'phone': PHONE,
    'pay_id': order['payId'],
    'pay_channel': 'ALI'
})
payment = response.json()['data']
print(f"支付宝支付串: {payment['channelPayData']}")
```

### 代理池管理示例

```python
# 1. 添加代理
response = requests.post(f'{API_BASE}/api/proxy/pool/add', json={
    'host': '61.155.141.105',
    'port': 20080,
    'username': 'user',
    'password': 'pass',
    'protocol': 'http'
})
print(response.json())

# 2. 查看代理池状态
response = requests.get(f'{API_BASE}/api/proxy/pool/status')
print(response.json())

# 3. 查看账号绑定关系
response = requests.get(f'{API_BASE}/api/proxy/account/binding')
print(response.json())

# 4. 手动切换账号代理
response = requests.post(f'{API_BASE}/api/proxy/account/switch', json={
    'phone': PHONE
})
print(response.json())

# 5. 获取统计数据
response = requests.get(f'{API_BASE}/api/proxy/statistics')
print(response.json())
```

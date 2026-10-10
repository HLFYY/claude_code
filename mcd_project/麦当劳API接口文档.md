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

### 1. 发送验证码
`POST /api/login/send_code`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号（11位） |
| use_new_device | bool | 否 | 是否强制生成新设备 Token，默认 false |

内部逻辑：优先复用 Redis/文件中已保存的设备 Token；若无则调用 `generate_token()` 生成新 Token 并通过 `activate_token()` 激活，再调用 `send_verification_code()` 发送验证码（手机号会先用 AES-128-ECB 加密）。

响应：
```json
{"success": true, "data": {"token": "设备Token"}, "message": "验证码已发送"}
```

### 2. 验证码登录
`POST /api/login/verify`

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| phone | string | 是 | 手机号 |
| verify_code | string | 是 | 6位短信验证码 |

内部调用 `login_manager.do_login()` → `mcd_api.login()`，登录成功后凭证会自动落盘（Redis 或 `data/credentials.json`）。

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

逻辑：先从存储加载凭证，再调用上游 `GET /bff/user/pin?scene=1` 校验有效性。

响应（已登录）：
```json
{
  "success": true,
  "data": {
    "is_logged_in": true,
    "token": "...", "sid": "...", "meddy_id": "...",
    "user_info": {"phone": "", "name": "", "points": "0", "meddy_id": "", "identity": ""}
  },
  "message": "已登录"
}
```
响应（未登录/已失效）：
```json
{"success": true, "data": {"is_logged_in": false}, "message": "未登录 / 登录已失效"}
```

---

## 二、通用接口 `/api/mcd/<function_name>`

`GET/POST /api/mcd/<function_name>`

这是一个反射式路由：`function_name` 对应 `mcd_api.py` 中同名的函数名，服务端用 `getattr(mcd_api, function_name)` 动态调用，参数原样透传给该函数。

鉴权参数（二选一）：
- `phone`：服务端会从 Redis 读取该手机号已保存的 `token`/`sid`/`meddy_id`
- 或直接传 `token` + `sid`

若函数需要 `mcd_id` 但未传，会自动用 Redis 中该手机号对应的 `meddy_id` 补全。

错误处理：
- 函数不存在 → 404，`{"success": false, "message": "接口 xxx 不存在"}`
- 参数错误（TypeError）→ 400
- 调用异常 → 500

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

## 三、健康检查

`GET /health`

无参数。

响应：
```json
{"status": "ok", "service": "mcd-api-refactored"}
```

---

## 四、通用说明

### 1. 时段代码（daypartCode）
多数接口的 `daypart_code` 不传时会调用 `get_current_daypart_code()` 按服务器当前时间自动计算：

| 时间段 | 代码 | 含义 |
|---|---|---|
| 05:00–10:29 | 1 | 早餐 |
| 10:30–14:29 | 8 | 午餐 |
| 14:30–16:59 | 4 | 下午茶 |
| 17:00–21:59 | 5 | 夜市 |
| 22:00–04:59 | 6 | 宵夜 |

### 2. 堂食/外卖类型
不同接口对 `order_type`/`be_type` 的取值含义略有差异（同一字段在不同上游接口中编码不一致，代码中已按各接口分别处理）：
- 多数接口：`1`=堂食，`2`=外卖
- `clear_cart` 中的 `be_type`：`0`=堂食，`1`=外卖

调用时建议按各函数文档中给出的取值为准，不要跨接口套用。

### 3. 鉴权凭证来源
所有业务接口（除登录接口外）都需要 `token` + `sid`，两者的获取方式：
- 通过 `phone` 参数由服务端自动从 Redis（`mcd:credentials` 哈希表，字段为手机号）读取，凭证由登录流程写入
- 或调用方直接传入已有的 `token`/`sid`

### 4. 请求签名机制（供理解 `mcd_api.py` 内部实现）
- v4 签名：`Authorization` 头由 `hmac-auth-v1` 格式的 HMAC-SHA256 签名组成，签名内容包含请求方法、路径、排序后的查询字符串、时间等；请求体额外用 `X-Hmac-Digest`（HMAC-SHA256）签名
- v5 签名：通过独立的 `v5_rpc_client` 模块获取 `x-mcd-sign`，仅 `submit_order` 优先尝试使用，不可用时自动降级为 v4
- 手机号、验证码在请求体中以 AES-128-ECB + PKCS7 加密后传输

### 5. 多进程/多实例说明
`mcd_server.py` 中的 `login_manager_cache` 是进程内内存字典（`{phone: LoginManager实例}`），仅用于避免重复创建对象；真正的凭证持久化在 `LoginManager(use_redis=True)` 对应的 Redis 中，因此多进程部署时凭证是共享一致的，但缓存对象本身不共享。

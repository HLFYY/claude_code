# 麦当劳 API 项目

一个完整的麦当劳中国 App API 逆向工程项目，实现了从登录、选店、点餐到支付的完整下单流程。

## 项目特性

✅ **完整的三步流程**
- 店铺选择（附近店铺/搜索店铺）
- 商品点餐（支持多商品、规格选择、自动处理优惠券）
- 订单支付（自动提交订单、生成支付宝支付链接/二维码）

✅ **登录管理**
- 多账号支持
- 自动检测登录状态
- Token 过期自动重新登录

✅ **API 完整实现**
- V4 签名算法（HMAC-SHA256）
- AES-128-ECB 加密
- 所有核心业务接口

## 项目结构

```
mcd_project/
├── mcd_api.py              # 核心 API 功能（签名、加密、所有业务接口）
├── login_manager.py        # 登录管理类（多账号、状态检查、凭证管理）
├── run.py                  # 主流程脚本（店铺/点餐/支付三步流程）
├── test_add_all_categories.py  # 测试每个分类第一个商品
├── use_login_manager.py    # 登录管理器测试脚本
├── demo.py                 # 清空购物车演示脚本
├── credentials.json        # 登录凭证存储（自动生成）
├── selected_store.json     # 当前选择的店铺信息（自动生成）
└── order_data.json         # 待支付订单数据（自动生成）
```

## 快速开始

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

## License

MIT

# 麦当劳 API 项目

一个完整的麦当劳 App API 逆向工程项目，支持登录、多账号管理、店铺查询、商品浏览、购物车管理、订单流程等功能。

## 项目结构

```
mcd_project/
├── mcd_api.py              # 核心 API 功能（签名、加密、所有业务接口、密钥配置）
├── login_manager.py        # 登录管理类（多账号、状态检查、凭证管理）
├── run.py                  # 店铺查找和下单流程测试脚本
├── use_login_manager.py    # 登录管理器测试脚本（5个测试场景）
├── demo.py                 # 清空购物车演示脚本
├── credentials.json        # 登录凭证存储（自动生成）
└── README.md              # 项目文档
```

## 快速开始

### 使用测试脚本

```bash
# 测试登录管理器（包含5个测试场景）
python3 use_login_manager.py

# 清空购物车演示
python3 demo.py
```

### 完整下单流程

```bash
# 第一步：选择店铺
python run.py store

# 第二步：测试下单流程（需要先执行 store）
python run.py order
```

```python
from login_manager import LoginManager

# 创建登录管理器
manager = LoginManager(phone="17717295039")

# 检查登录状态（如果凭证有效直接返回，否则返回 None）
token, sid, meddy_id = manager.ensure_login(auto_relogin=False)

if token:
    print(f"登录成功")
    print(f"Token: {token}")
else:
    print("需要重新登录")
```

### 2. 手动登录流程

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

## mcd_api.py 已实现的接口

### 登录相关
- `generate_token()` - 生成设备 Token
- `activate_token(token)` - 激活 Token 获取 tid
- `send_verify_code(phone, token, sid)` - 发送验证码
- `login_with_code(phone, code, token, sid)` - 验证码登录
- `check_login_status(token, sid, meddy_id)` - 检查登录状态

### 城市和店铺
- `get_all_cities(token, sid, meddy_id)` - 获取所有城市信息
- `get_current_city(latitude, longitude, token, sid, meddy_id)` - 通过经纬度获取当前城市
- `search_stores(city_code, keyword, token, sid, meddy_id, page_no=1, page_size=10)` - 搜索店铺
- `get_nearby_stores(latitude, longitude, token, sid, meddy_id, show_type=2, order_type=1)` - 获取附近店铺

### 商品和菜单
- `get_store_menu(store_code, token, sid, meddy_id, order_type=1, day_part_code="5")` - 获取店铺商品菜单
- `get_product_detail(product_code, store_code, token, sid, meddy_id, ...)` - 获取商品详情

### 购物车
- `empty_cart(store_code, token, sid, meddy_id, ...)` - 清空购物车
- `update_cart(products, store_code, token, sid, meddy_id, ...)` - 更新购物车（加入/删除商品）

### 订单相关
- `get_order_validation_info(store_code, token, sid, meddy_id, ...)` - 获取订单验证信息
- `get_order_promotion(cart_items, store_code, token, sid, meddy_id, ...)` - 获取促销/优惠券信息
- `get_nearest_store(store_code, latitude, longitude, token, sid, meddy_id)` - 获取门店信息
- `get_payment_channels(order_id, pay_id, mcd_id, token, sid, meddy_id)` - 获取支付渠道
- `submit_order(...)` - 提交订单（需要 v5 签名，暂不可用）
- `preorder_payment(...)` - 预支付（需先完成提交订单）

## LoginManager 类 API

### 构造函数

```python
LoginManager(phone: str, credentials_file: str = None)
```

- `phone`: 手机号（11位）
- `credentials_file`: 凭证文件路径（可选，默认为当前目录下的 credentials.json）

### 主要方法

#### `ensure_login(auto_relogin: bool = False)`

确保已登录状态。

- 参数：
  - `auto_relogin`: 如果凭证失效，是否自动重新登录（需要手动输入验证码）
- 返回：`(token, sid, meddy_id)` 或 `(None, None, None)`

#### `send_verify_code()`

发送验证码（会自动生成并激活 token）。

- 返回：`(success, message)`

#### `do_login(verify_code: str)`

使用验证码登录。

- 参数：
  - `verify_code`: 6位短信验证码
- 返回：`(success, message)`

#### `check_login_status()`

检查当前登录状态是否有效。

- 返回：`(is_valid, user_info)`

#### `load_credentials()`

从文件加载指定手机号的登录凭证。

- 返回：`(token, sid, meddy_id)` 或 `(None, None, None)`

#### `save_credentials(token, sid, meddy_id)`

保存登录凭证到文件。

#### `get_credentials()`

获取当前内存中的凭证。

- 返回：`(token, sid, meddy_id)`

#### `clear_credentials()`

清除当前手机号的凭证（从文件和内存中删除）。

## 测试

运行测试脚本：

```bash
python3 use_login_manager.py
```

测试包括：
1. 基本用法（检查登录状态）
2. 手动登录流程
3. 自动重新登录
4. 多账号管理
5. 仅检查登录状态

## 凭证文件格式

`credentials.json` 支持多账号存储：

```json
{
  "16752934813": {
    "token": "5a56a1ad7c1d48edbf8db6c209034712",
    "sid": "c79997abd0c02824e5178aac7d9a3dc1_",
    "meddy_id": "MEDDY163321681473498257",
    "saved_at": "2026-09-24T15:30:00"
  },
  "13800138000": {
    "token": "...",
    "sid": "...",
    "meddy_id": "...",
    "saved_at": "2026-09-24T16:00:00"
  }
}
```

### 技术实现

### 签名算法

使用 HMAC-SHA256 签名：

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

#### AES 加密

手机号和验证码使用 AES-128-ECB 加密：

- **密钥**: `w8ZJ4wrUl7dDB1A7`
- **模式**: ECB
- **填充**: PKCS7
- **编码**: Base64

## 注意事项

1. 登录凭证可能会失效（例如在其他设备登录）
2. 验证码有效期约 5 分钟
3. 建议在每次 API 请求前检查登录状态
4. 凭证文件包含敏感信息，请妥善保管

## License

MIT

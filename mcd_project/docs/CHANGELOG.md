# 麦当劳项目更新日志

## [2.0.0] - 2026-10-10

### 🎉 重大更新：代理池管理系统

本次更新为麦当劳项目新增了完整的**代理池管理系统**，实现了账号-代理绑定、智能故障切换和健康监控功能。

---

### ✨ 新增功能

#### 1. 代理池管理模块（8个新文件）

**核心文件：**
- `proxy_pool.py` (791行) - 代理池核心管理器
  - 代理增删查改、状态管理
  - 健康检查（支持缓存、批量检查）
  - 负载统计、城市信息管理
  - 支持5种状态：active/slow/observation/failed/disabled

- `account_proxy_binding.py` (533行) - 账号-代理绑定管理器
  - 账号-代理长期绑定（Redis 持久化）
  - 观察期管理
  - 失败计数追踪（时间窗口）
  - 临时代理切换逻辑

- `proxy_failure_handler.py` (278行) - 四级容错失败处理器
  - 阶段1：首次失败 → 立即重试
  - 阶段2：连续失败3次 → 临时切换 + 观察期（5分钟）
  - 阶段2.5：观察期内连续失败10次 → 立即永久切换
  - 阶段3：观察期结束 → 健康检查决定（恢复/永久切换）

- `proxy_manager.py` (394行) - 统一入口封装
  - 聚合所有代理相关功能
  - 提供统一 API 接口
  - 兼容 mcd_server 调用

**辅助模块：**
- `ip_location.py` (119行) - IP 地理位置查询
  - 支持3个免费API（ipapi.co、ip-api.com、ipinfo.io）
  - 自动重试机制
  - 自动查询代理所属城市

- `scheduler.py` (125行) - 定时任务调度器
  - 全量健康检查：每12小时
  - 观察期检查：每5分钟
  - 失败记录清理：每天凌晨3点

- `utils.py` (18行) - 公共工具函数
  - `log_info()` 统一日志输出（支持 flush=True）

- `requirements.txt` - 新增依赖包
  - `schedule==1.2.0` - 定时任务调度

---

#### 2. 新增 HTTP API 接口（8个）

**代理池管理：**
- `GET /api/proxy/pool/status` - 查看代理池状态
- `GET /api/proxy/pool/list` - 获取所有代理列表
- `POST /api/proxy/pool/add` - 添加代理到代理池
- `POST /api/proxy/pool/remove` - 移除/禁用代理

**账号绑定管理：**
- `GET /api/proxy/account/binding` - 查看账号代理绑定关系
- `POST /api/proxy/account/switch` - 手动切换账号代理

**监控统计：**
- `GET /api/proxy/statistics` - 获取代理池统计数据
- `POST /api/proxy/health/check` - 手动触发健康检查

---

### 🔧 功能增强

#### 1. login_manager.py (+167行核心逻辑)
- ✅ 集成代理池、绑定管理器、失败处理器（全局单例）
- ✅ 所有请求自动使用绑定的代理
- ✅ 新增 `_make_request_with_proxy()` 统一请求处理
- ✅ 请求失败自动触发容错机制
- ✅ 支持城市优先分配代理
- ✅ 构造函数新增参数：`city`、`proxy_pool`、`binding_manager`、`failure_handler`

#### 2. mcd_api.py (+47行 + 20+ API改造)
- ✅ 新增 `make_request()` 统一请求处理函数
- ✅ 所有 API 函数新增 `proxies` 参数（支持代理字典格式）
- ✅ 统一超时时间为 `PROXY_REQUEST_TIMEOUT`（15秒）
- ✅ 统一错误处理（超时、代理错误、网络连接失败）

**改造的 API 函数（20+个）：**
- `activate_token()`, `send_verification_code()`, `login()`
- `get_all_cities()`, `get_city_by_location()`, `search_stores()`, `get_nearby_stores()`
- `get_store_menu()`, `get_product_detail()`
- `get_cart()`, `clear_cart()`, `add_to_cart()`
- `get_order_validation_info()`, `get_order_promotion_info()`, `get_nearest_store_info()`
- `submit_order()`, `get_payment_channels()`, `create_payment()`
- `get_order_list()`, `get_order_detail()`

#### 3. mcd_server.py (+213行)
- ✅ 全局单例模式（`get_proxy_pool()`、`get_binding_manager()`、`get_failure_handler()`）
- ✅ 避免每次请求重复初始化代理模块
- ✅ 通用 API 接口自动获取账号绑定的代理
- ✅ 日志增强（包含代理状态、敏感参数隐藏）
- ✅ 配置日志同时输出到文件和标准输出

#### 4. config.py (+71行)
- ✅ 新增 `PROXY_REQUEST_TIMEOUT = 15` 请求超时时间
- ✅ 新增 `PROXY_FAILURE_CONFIG` 代理失败容错配置
- ✅ 新增 `PROXY_HEALTH_CHECK_CONFIG` 健康检查配置
- ✅ 新增 `PROXY_POOL_CONFIG` 代理池配置
- ✅ 新增 `ACCOUNT_PROXY_BINDING_CONFIG` 账号绑定配置

#### 5. demo.py (重构)
- ✅ 新增 `add_proxy()` 函数（批量添加代理到代理池）
- ✅ 测试代理池初始化

#### 6. run_api_client.py (小改)
- ✅ API_BASE 改回本地 `127.0.0.1:5001`
- ✅ 移除 `get_payment_channels` 的冗余 `mcd_id` 参数

---

### 📊 技术亮点

1. **全局单例模式** - 避免 ProxyPool/AccountProxyBinding/ProxyFailureHandler 重复初始化
2. **Redis 持久化** - 所有代理信息、绑定关系、失败计数均存储在 Redis
3. **无侵入集成** - `LoginManager` 自动处理代理分配和故障切换，业务代码无需修改
4. **城市感知** - 自动查询代理 IP 城市，支持账号按城市优先分配代理
5. **健康检查缓存** - 30秒 TTL，避免短时间重复检查同一代理
6. **批量处理** - 健康检查支持分批（每批10个，间隔6秒）避免瞬间大量请求
7. **日志增强** - 统一使用 `utils.log_info()`，支持 `flush=True` 确保 crontab 实时输出

---

### 🔑 关键配置

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

---

### 📖 使用示例

#### 示例1：添加代理到代理池

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

#### 示例2：使用代理登录（自动）

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

#### 示例3：启动定时任务调度器

```bash
# 启动调度器（后台运行）
nohup python scheduler.py > scheduler.log 2>&1 &

# 或使用 crontab
# crontab -e
# @reboot cd /path/to/mcd_project && python scheduler.py >> scheduler.log 2>&1
```

#### 示例4：HTTP API 调用

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

---

### 🐛 修复问题

- 修复 `mcd_api.py` 中所有 API 函数没有超时时间统一设置的问题
- 修复 `login_manager.py` 日志输出使用 `print()` 在 crontab 中缓冲的问题
- 修复 `mcd_server.py` 日志配置不完整的问题

---

### ⚠️ 破坏性变更

#### 1. `LoginManager` 构造函数签名变化

**旧版本：**
```python
LoginManager(phone, credentials_file=None, use_redis=False)
```

**新版本：**
```python
LoginManager(
    phone,
    credentials_file=None,
    use_redis=True,  # 默认值改为 True
    city=None,       # 新增：城市优先
    proxy_pool=None, # 新增：代理池实例
    binding_manager=None,  # 新增：绑定管理器实例
    failure_handler=None   # 新增：失败处理器实例
)
```

**影响：**
- 如果您之前明确传入 `use_redis=False`，需要修改为 `use_redis=True` 或删除该参数
- 旧代码仍然兼容，但建议使用新的全局单例模式避免重复初始化

#### 2. `mcd_api.py` 所有 API 函数签名变化

**所有 API 函数新增最后一个参数：`proxies=None`**

**示例：**
```python
# 旧版本
send_verification_code(phone, token)

# 新版本
send_verification_code(phone, token, proxies=None)
```

**影响：**
- 如果您直接调用 `mcd_api.py` 中的函数，现在可以传入 `proxies` 参数
- 不传入则不使用代理（向后兼容）

---

### 📦 依赖更新

新增依赖：
```
schedule==1.2.0
```

安装命令：
```bash
pip install -r requirements.txt
```

---

### 🔮 未来计划

- [ ] 代理池自动扩容/缩容
- [ ] 代理质量评分系统
- [ ] 代理供应商 API 对接
- [ ] Web 管理面板
- [ ] 代理使用成本统计

---

### 📝 相关文档

- [代理池管理系统使用文档](PROXY_POOL_GUIDE.md)（待创建）
- [四级容错机制详解](docs/proxy_failure_handler.md)（待创建）
- [API 接口文档](API_DOCUMENTATION.md)（待更新）

---

### 👥 贡献者

- 侯杰 - 代理池管理系统设计与实现

---

## [1.0.0] - 2026-09-24

### 初始版本

- ✅ 完整的登录流程（Token生成、激活、发送验证码、登录）
- ✅ 多账号支持（Redis 存储）
- ✅ 店铺查询（附近店铺、搜索店铺）
- ✅ 商品菜单（获取菜单、商品详情）
- ✅ 购物车管理（清空、添加商品）
- ✅ 订单流程（订单验证、促销信息、提交订单）
- ✅ 支付流程（获取支付渠道、创建支付）
- ✅ v4 签名算法完整实现
- ✅ v5 签名 RPC 对接

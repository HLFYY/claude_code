# 麦当劳项目 - 代理池管理系统详解 (v2.0)

**文档版本**: v1.0  
**创建时间**: 2026-10-10  
**适用项目**: 麦当劳 API 逆向工程项目 v2.0+

---

## 📋 目录

- [1. 系统概述](#1-系统概述)
- [2. 系统架构](#2-系统架构)
- [3. 核心模块](#3-核心模块)
- [4. 四级容错机制](#4-四级容错机制)
- [5. 使用指南](#5-使用指南)
- [6. HTTP API 接口](#6-http-api-接口)
- [7. 配置说明](#7-配置说明)
- [8. 监控运维](#8-监控运维)
- [9. 故障排查](#9-故障排查)
- [10. 性能优化](#10-性能优化)

---

## 1. 系统概述

### 1.1 背景

麦当劳 API 对单 IP 请求有频率限制，多账号批量操作需要使用代理池分散请求。v2.0 新增的代理池管理系统提供了完整的代理管理、账号绑定、智能容错和健康监控功能。

### 1.2 核心特性

- ✅ **代理池管理** - 增删查改、状态管理、健康检查
- ✅ **账号-代理长期绑定** - Redis 持久化、负载均衡
- ✅ **城市优先分配** - 自动查询代理 IP 城市，优先分配同城代理
- ✅ **四级智能容错** - 自动重试、临时切换、观察期、永久切换
- ✅ **定时任务调度** - 全量检查、观察期检查、失败清理
- ✅ **HTTP API 接口** - 8个代理管理接口
- ✅ **全局单例模式** - 避免重复初始化
- ✅ **无侵入集成** - LoginManager 自动处理，业务代码无需修改

### 1.3 技术栈

- **存储**: Redis (代理信息、绑定关系、失败计数)
- **调度**: schedule 库 (定时任务)
- **HTTP**: Flask + gunicorn (API 服务器)
- **地理位置**: 3个免费 API (自动重试)

---

## 2. 系统架构

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                     mcd_server.py (HTTP API)                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  8个代理管理接口（状态/列表/添加/移除/绑定/统计）  │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                   login_manager.py (业务层)                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  _make_request_with_proxy() 统一请求处理            │    │
│  │  - 自动获取账号绑定的代理                           │    │
│  │  - 失败触发容错机制                                 │    │
│  │  - 成功记录请求统计                                 │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                  proxy_manager.py (统一入口)                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  聚合 ProxyPool + AccountProxyBinding + Handler    │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
          ↓                    ↓                    ↓
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│   proxy_pool.py  │  │account_proxy_   │  │proxy_failure_    │
│                  │  │binding.py        │  │handler.py        │
│ • 代理池管理     │  │                  │  │                  │
│ • 健康检查       │  │ • 账号代理绑定   │  │ • 四级容错       │
│ • 状态管理       │  │ • 观察期管理     │  │ • 自动重试       │
│ • 城市查询       │  │ • 失败计数       │  │ • 临时/永久切换  │
└──────────────────┘  └──────────────────┘  └──────────────────┘
          ↓                    ↓                    ↓
┌─────────────────────────────────────────────────────────────┐
│                        Redis 存储层                          │
│  • mcd:proxy:{id} - 代理信息                                │
│  • mcd:proxy:all - 所有代理ID集合                           │
│  • mcd:proxy:available - 可用代理ID集合                     │
│  • mcd:proxy:health:{id} - 健康检查缓存                     │
│  • mcd:account_binding:{phone} - 账号绑定信息               │
│  • mcd:accounts_in_observation - 观察期账号集合             │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 模块职责

| 模块 | 职责 | 关键功能 |
|------|------|----------|
| `proxy_pool.py` | 代理池管理器 | 增删查改、状态管理、健康检查、城市查询 |
| `account_proxy_binding.py` | 账号绑定管理器 | 账号-代理绑定、观察期管理、失败计数 |
| `proxy_failure_handler.py` | 失败处理器 | 四级容错逻辑、重试决策、代理切换 |
| `proxy_manager.py` | 统一入口 | 聚合所有功能、提供统一 API |
| `ip_location.py` | IP 地理位置 | 查询代理城市（3个API自动重试） |
| `scheduler.py` | 定时任务 | 全量检查、观察期检查、失败清理 |
| `utils.py` | 公共工具 | log_info() 统一日志输出 |

---

## 3. 核心模块

### 3.1 proxy_pool.py - 代理池管理器

**主要功能：**
- 代理增删查改
- 5种状态管理（active/slow/observation/failed/disabled）
- 健康检查（支持缓存30秒、批量检查）
- 负载统计（绑定账号数）
- 城市信息自动查询

**关键方法：**
```python
# 添加代理（自动查询城市）
proxy = pool.add_proxy(host, port, username, password, protocol)

# 获取可用代理（支持城市筛选、排除列表）
proxy = pool.get_available_proxy(city=None, exclude=[])

# 健康检查（支持缓存）
is_healthy = pool.check_proxy_health(proxy_id, use_cache=True)

# 批量健康检查（分批处理）
results = pool.check_proxies_batch(proxy_ids, batch_size=10)

# 标记代理状态
pool.mark_proxy_success(proxy_id)
pool.mark_proxy_failed(proxy_id)
pool.mark_proxy_observation(proxy_id)
```

**代理状态流转：**
```
active → slow (响应>3秒)
active → observation (进入观察期)
observation → active (观察期结束恢复)
observation → failed (观察期结束仍失败)
failed → disabled (连续失败5次)
any → disabled (手动禁用)
```

### 3.2 account_proxy_binding.py - 账号绑定管理器

**主要功能：**
- 账号-代理长期绑定（Redis 持久化）
- 观察期管理
- 失败计数追踪（时间窗口10分钟）
- 临时代理切换
- 永久代理切换

**关键方法：**
```python
# 获取或分配代理（支持城市优先）
proxy = binding_mgr.get_or_assign_proxy(phone, city=None)

# 获取本次请求应该使用的代理（观察期逻辑）
proxy = binding_mgr.get_proxy_for_request(phone)

# 启动观察期
binding_mgr.start_observation(phone, main_proxy_id, temp_proxy_id)

# 增加失败计数
fail_count = binding_mgr.increment_fail_count(phone)

# 永久切换代理
binding_mgr.permanent_switch(phone, new_proxy_id)

# 回归主代理（观察期结束）
binding_mgr.revert_to_main_proxy(phone)
```

**绑定数据结构：**
```python
{
    'phone': '13800138000',
    'proxy_id': 'proxy_001',              # 主代理
    'temp_proxy_id': None,                 # 临时代理（观察期）
    'bound_at': '2026-10-10T10:00:00',
    
    # 观察期状态
    'in_observation': False,
    'observation_start': None,
    'observation_until': None,
    'observation_fail_count': 0,
    
    # 失败统计
    'total_fail_count': 0,
    'fail_window_start': '2026-10-10T10:00:00',
    'last_fail_time': None,
    
    # 请求统计
    'request_count': 0,
    'last_request': None
}
```

### 3.3 proxy_failure_handler.py - 失败处理器

**主要功能：**
- 四级容错逻辑
- 重试决策
- 代理切换判断

**关键方法：**
```python
# 处理请求失败
should_retry, new_proxy_id, action, message = handler.handle_request_failure(
    phone, proxy_id, error
)

# 处理请求成功
handler.handle_request_success(phone, proxy_id)
```

**返回的 action 类型：**
- `retry_same_proxy` - 使用相同代理重试
- `temp_switch` - 临时切换代理（进入观察期）
- `permanent_switch` - 永久切换代理
- `permanent_switch_immediate` - 立即永久切换（观察期内连续失败10次）
- `continue_temp_proxy` - 继续使用临时代理
- `error` - 无可用代理

---

## 4. 四级容错机制

### 4.1 流程图

```
开始请求
   ↓
┌─────────────────┐
│ 获取账号绑定代理│
└─────────────────┘
   ↓
┌─────────────────┐
│ 使用代理发送请求│
└─────────────────┘
   ↓
  成功? ────Yes──→ [标记代理成功] → 结束
   ↓ No
┌─────────────────┐
│ 标记代理失败    │
└─────────────────┘
   ↓
 在观察期? ────Yes──→ ┌──────────────────────┐
   ↓ No              │ 增加观察期失败计数    │
┌─────────────────┐  └──────────────────────┘
│ 增加失败计数    │             ↓
└─────────────────┘      失败≥10次? ──Yes──→ [立即永久切换] → 结束
   ↓                         ↓ No
失败次数=1? ──Yes→ [等1秒重试，同代理] → 继续
   ↓ No
失败次数=2? ──Yes→ [等1秒重试，同代理] → 继续
   ↓ No
失败次数=3? ──Yes→ ┌──────────────────────┐
   ↓               │ 临时切换新代理        │
 ≥3次              │ 启动观察期（5分钟）   │
   ↓               └──────────────────────┘
[异常永久切换]              ↓
   ↓                   [使用临时代理] → 结束
 结束

观察期后台任务（每5分钟）:
   ↓
┌─────────────────┐
│检查观察期是否到期│
└─────────────────┘
   ↓
 到期? ──No──→ 继续
   ↓ Yes
┌─────────────────┐
│健康检查主代理    │
└─────────────────┘
   ↓
 健康? ──Yes──→ [回归主代理] → 结束
   ↓ No
[永久切换到临时代理] → 结束
```

### 4.2 四个阶段详解

**阶段1：首次失败 → 立即重试**
- 触发条件：失败计数 = 1
- 处理逻辑：等待1秒，使用相同代理重试
- 设计原因：网络抖动、临时故障

**阶段2：连续失败3次 → 临时切换 + 观察期**
- 触发条件：失败计数 = 3
- 处理逻辑：
  1. 分配新的临时代理
  2. 启动观察期（5分钟）
  3. 主代理状态改为 observation
  4. 账号加入观察期集合
- 设计原因：主代理可能短暂故障，给5分钟恢复时间

**阶段2.5：观察期内连续失败10次 → 立即永久切换**
- 触发条件：观察期内失败计数 ≥ 10
- 处理逻辑：
  1. 立即永久切换到临时代理
  2. 主代理状态改为 failed
  3. 账号移出观察期集合
- 设计原因：临时代理也频繁失败说明主代理已彻底失效

**阶段3：观察期结束 → 健康检查决定**
- 触发条件：观察期到期（5分钟后）
- 处理逻辑：
  1. 健康检查主代理（不使用缓存）
  2. 如果健康 → 回归主代理
  3. 如果仍失败 → 永久切换到临时代理
- 设计原因：给主代理充分恢复时间后做最终判断

### 4.3 失败计数时间窗口

- **窗口时长**: 10分钟
- **窗口逻辑**: 10分钟内的失败累加，超过10分钟的失败自动清零
- **设计原因**: 避免历史失败永久累积，代理短暂故障后可恢复使用

---

## 5. 使用指南

### 5.1 添加代理到代理池

**方式1：Python 代码**
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

print(f"代理ID: {proxy['id']}")
print(f"城市: {proxy.get('city', '未知')}")
```

**方式2：HTTP API**
```bash
curl -X POST http://localhost:5001/api/proxy/pool/add \
  -H "Content-Type: application/json" \
  -d '{
    "host": "61.155.141.105",
    "port": 20080,
    "username": "user",
    "password": "pass",
    "protocol": "http"
  }'
```

### 5.2 使用代理登录（自动）

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

# 后续所有请求自动使用该账号绑定的代理
# 失败自动触发四级容错机制
```

### 5.3 多账号按城市分配

```python
from proxy_pool import ProxyPool
from login_manager import LoginManager

# 添加不同城市的代理
pool = ProxyPool()

# 上海代理
sh_proxy = pool.add_proxy(
    host='1.2.3.4', port=8080,
    username='u', password='p', protocol='http'
)

# 北京代理
bj_proxy = pool.add_proxy(
    host='5.6.7.8', port=8080,
    username='u', password='p', protocol='http'
)

# 为上海账号分配代理（优先上海）
manager_sh = LoginManager(phone="13800000001", use_redis=True, city="上海")
manager_sh.ensure_login()

# 为北京账号分配代理（优先北京）
manager_bj = LoginManager(phone="13800000002", use_redis=True, city="北京")
manager_bj.ensure_login()
```

### 5.4 启动 HTTP API 服务器

```bash
# 方式1：直接运行
python mcd_server.py

# 方式2：使用 gunicorn（推荐）
gunicorn -w 4 -b 0.0.0.0:5001 mcd_server:app

# 方式3：后台运行
nohup python mcd_server.py > logs/server.log 2>&1 &
```

### 5.5 启动定时任务调度器

```bash
# 前台运行（开发调试）
python scheduler.py

# 后台运行（生产环境）
nohup python scheduler.py > logs/scheduler.log 2>&1 &

# systemd 服务（推荐）
# 创建 /etc/systemd/system/mcd-scheduler.service
[Unit]
Description=MCD Proxy Pool Scheduler
After=network.target redis.service

[Service]
Type=simple
User=www-data
WorkingDirectory=/path/to/mcd_project
ExecStart=/usr/bin/python3 scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target

# 启动服务
sudo systemctl start mcd-scheduler
sudo systemctl enable mcd-scheduler
```

---

## 6. HTTP API 接口

### 6.1 接口列表

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

### 6.2 接口详解

#### 6.2.1 查看代理池状态

```bash
GET /api/proxy/pool/status
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "total": 10,
    "active": 8,
    "slow": 1,
    "observation": 1,
    "failed": 0,
    "disabled": 0
  }
}
```

#### 6.2.2 获取所有代理列表

```bash
GET /api/proxy/pool/list
```

**响应示例：**
```json
{
  "success": true,
  "data": [
    {
      "id": "proxy_001",
      "host": "61.155.141.105",
      "port": 20080,
      "protocol": "http",
      "status": "active",
      "city": "上海",
      "bound_count": 3,
      "success_count": 156,
      "fail_count": 2,
      "avg_response_time": 1.2,
      "last_check": "2026-10-10T10:30:00"
    }
  ]
}
```

#### 6.2.3 添加代理

```bash
POST /api/proxy/pool/add
Content-Type: application/json

{
  "host": "61.155.141.105",
  "port": 20080,
  "username": "user",
  "password": "pass",
  "protocol": "http"
}
```

**响应示例：**
```json
{
  "success": true,
  "message": "代理添加成功",
  "data": {
    "id": "proxy_abc123",
    "city": "上海"
  }
}
```

#### 6.2.4 移除/禁用代理

```bash
POST /api/proxy/pool/remove
Content-Type: application/json

{
  "proxy_id": "proxy_001",
  "action": "disable"  # 或 "remove"
}
```

#### 6.2.5 查看账号绑定关系

```bash
GET /api/proxy/account/binding
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "total_accounts": 25,
    "in_observation": 2,
    "normal": 23,
    "bindings": [
      {
        "phone": "138****0001",
        "proxy_id": "proxy_001",
        "in_observation": false,
        "request_count": 156,
        "last_request": "2026-10-10T10:30:00"
      }
    ]
  }
}
```

#### 6.2.6 手动切换账号代理

```bash
POST /api/proxy/account/switch
Content-Type: application/json

{
  "phone": "13800138000",
  "new_proxy_id": "proxy_002"
}
```

#### 6.2.7 获取代理池统计

```bash
GET /api/proxy/statistics
```

**响应示例：**
```json
{
  "success": true,
  "data": {
    "pool_stats": {
      "total": 10,
      "active": 8,
      "success_rate": 0.97
    },
    "binding_stats": {
      "total_accounts": 25,
      "in_observation": 2
    },
    "top_proxies": [
      {
        "id": "proxy_001",
        "bound_count": 5,
        "success_rate": 0.98,
        "avg_response_time": 1.2,
        "city": "上海"
      }
    ]
  }
}
```

#### 6.2.8 手动触发健康检查

```bash
POST /api/proxy/health/check
Content-Type: application/json

{
  "scope": "all",  # "all", "observation", "specific"
  "proxy_ids": ["proxy_001"]  # scope="specific" 时需要
}
```

---

## 7. 配置说明

### 7.1 完整配置（config.py）

```python
# ========== Redis 配置 ==========
REDIS_SET_URL = dict(
    host='127.0.0.1',
    port=26379,
    db=0,
    password='123.456.',
)

# ========== 代理配置 ==========

# 请求超时时间（秒）
PROXY_REQUEST_TIMEOUT = 15

# 代理失败容错配置
PROXY_FAILURE_CONFIG = {
    # 加入观察期的失败次数阈值
    'observation_threshold': 3,
    
    # 观察期内直接永久换绑的失败次数阈值
    'permanent_switch_threshold': 10,
    
    # 观察期时长（秒）
    'observation_period': 5 * 60,
    
    # 失败计数时间窗口（秒，超过窗口计数清零）
    'failure_window': 10 * 60,
}

# 健康检查配置
PROXY_HEALTH_CHECK_CONFIG = {
    # 健康检查缓存时间（秒）
    'cache_ttl': 30,
    
    # 批量检查每批数量
    'batch_size': 10,
    
    # 批次间延迟（秒）
    'batch_delay': 6,
    
    # 检查超时时间（秒）
    'check_timeout': 10,
}

# 代理池配置
PROXY_POOL_CONFIG = {
    # Redis Key 前缀
    'redis_key': 'mcd:proxy',
    
    # 响应时间阈值（秒），超过标记为slow
    'slow_threshold': 3.0,
    
    # 连续失败多少次标记为failed
    'max_fail_count': 5,
    
    # 单个代理最大账号负载
    'max_accounts_per_proxy': 5,
}

# 账号代理绑定配置
ACCOUNT_PROXY_BINDING_CONFIG = {
    # Redis Key 前缀
    'redis_key': 'mcd:account_binding',
    
    # 绑定关系 TTL（秒，0表示永不过期）
    'binding_ttl': 0,
}
```

### 7.2 配置调整建议

**增加容错次数：**
```python
PROXY_FAILURE_CONFIG = {
    'observation_threshold': 5,  # 从3改为5
    'permanent_switch_threshold': 15,  # 从10改为15
}
```

**缩短观察期：**
```python
PROXY_FAILURE_CONFIG = {
    'observation_period': 3 * 60,  # 从5分钟改为3分钟
}
```

**增加代理负载：**
```python
PROXY_POOL_CONFIG = {
    'max_accounts_per_proxy': 10,  # 从5改为10
}
```

**加快健康检查：**
```python
# scheduler.py 修改定时任务频率
schedule.every(3).minutes.do(observation_check)  # 从5分钟改为3分钟
```

---

## 8. 监控运维

### 8.1 查看代理池状态

```bash
curl http://localhost:5001/api/proxy/pool/status | jq
```

### 8.2 查看账号绑定关系

```bash
curl http://localhost:5001/api/proxy/account/binding | jq
```

### 8.3 查看代理详细信息

```bash
curl http://localhost:5001/api/proxy/pool/list | jq '.data[] | select(.id=="proxy_001")'
```

### 8.4 查看观察期账号

```bash
curl http://localhost:5001/api/proxy/account/binding | jq '.data.bindings[] | select(.in_observation==true)'
```

### 8.5 查看代理统计

```bash
curl http://localhost:5001/api/proxy/statistics | jq
```

### 8.6 手动触发健康检查

```bash
# 全量检查
curl -X POST http://localhost:5001/api/proxy/health/check \
  -H "Content-Type: application/json" \
  -d '{"scope": "all"}'

# 仅检查观察期代理
curl -X POST http://localhost:5001/api/proxy/health/check \
  -H "Content-Type: application/json" \
  -d '{"scope": "observation"}'

# 检查特定代理
curl -X POST http://localhost:5001/api/proxy/health/check \
  -H "Content-Type: application/json" \
  -d '{"scope": "specific", "proxy_ids": ["proxy_001", "proxy_002"]}'
```

### 8.7 日志监控

```bash
# 查看服务器日志
tail -f logs/mcd_server.log

# 查看调度器日志
tail -f logs/scheduler.log

# 搜索失败记录
grep "失败" logs/mcd_server.log

# 搜索代理切换记录
grep "切换" logs/mcd_server.log
```

---

## 9. 故障排查

### 9.1 代理一直失败无法使用

**症状：**
- 账号请求一直失败
- 代理状态为 failed

**排查步骤：**
1. 检查代理状态
```bash
curl http://localhost:5001/api/proxy/pool/list | jq '.data[] | select(.id=="proxy_xxx")'
```

2. 手动触发健康检查
```bash
curl -X POST http://localhost:5001/api/proxy/health/check \
  -H "Content-Type: application/json" \
  -d '{"scope": "specific", "proxy_ids": ["proxy_xxx"]}'
```

3. 查看 Redis 中的失败记录
```bash
redis-cli
> GET mcd:proxy:proxy_xxx
> GET mcd:proxy:health:proxy_xxx
```

4. 手动测试代理
```python
import requests

proxies = {
    'http': 'http://user:pass@host:port',
    'https': 'http://user:pass@host:port'
}

response = requests.get('https://httpbin.org/ip', proxies=proxies, timeout=10)
print(response.json())
```

**解决方案：**
- 如果代理本身有问题 → 移除代理
- 如果代理正常但被误标记 → 手动修改 Redis 状态

### 9.2 账号频繁切换代理

**症状：**
- 账号频繁进入观察期
- 代理频繁切换

**排查步骤：**
1. 查看账号绑定历史
```bash
curl http://localhost:5001/api/proxy/account/binding | jq '.data.bindings[] | select(.phone | contains("0001"))'
```

2. 检查该账号使用的代理健康状况
```bash
# 获取账号当前代理ID
PROXY_ID=$(curl -s http://localhost:5001/api/proxy/account/binding | jq -r '.data.bindings[] | select(.phone | contains("0001")) | .proxy_id')

# 查看代理详情
curl http://localhost:5001/api/proxy/pool/list | jq ".data[] | select(.id==\"$PROXY_ID\")"
```

3. 查看失败日志
```bash
grep "13800000001" logs/mcd_server.log | grep "失败"
```

**解决方案：**
- 增加容错次数（修改 `observation_threshold`）
- 换用更稳定的代理
- 检查是否触发了麦当劳的风控

### 9.3 定时任务未执行

**症状：**
- 观察期到期后账号未回归主代理
- 失败记录未清理

**排查步骤：**
1. 检查进程是否运行
```bash
ps aux | grep scheduler.py
```

2. 查看日志
```bash
tail -f logs/scheduler.log
```

3. 检查 Redis 连接
```bash
redis-cli PING
```

4. 手动执行任务
```python
from scheduler import observation_check, cleanup_failure_records

observation_check()
cleanup_failure_records()
```

**解决方案：**
- 重启调度器
- 检查 Redis 连接配置
- 检查日志权限

### 9.4 Redis 数据异常

**症状：**
- 绑定关系丢失
- 代理信息不正确

**排查步骤：**
1. 检查 Redis 连接
```bash
redis-cli -h 127.0.0.1 -p 26379 -a '123.456.' PING
```

2. 查看所有代理Key
```bash
redis-cli -h 127.0.0.1 -p 26379 -a '123.456.' KEYS 'mcd:proxy:*'
```

3. 查看特定数据
```bash
redis-cli -h 127.0.0.1 -p 26379 -a '123.456.' GET mcd:proxy:proxy_001
```

**解决方案：**
- 重新添加代理
- 重建绑定关系
- 检查 Redis 持久化配置（RDB/AOF）

---

## 10. 性能优化

### 10.1 已实现的优化

1. **健康检查缓存** - 30秒 TTL，避免短时间重复检查
2. **批量处理** - 每批10个代理，间隔6秒
3. **连接池复用** - Redis 连接池已启用
4. **全局单例模式** - 避免重复初始化
5. **代理预热** - 启动时执行一次全量健康检查

### 10.2 可优化方向

#### 10.2.1 异步健康检查

**当前：** 同步检查，阻塞主线程

**优化：** 使用 asyncio + aiohttp

```python
import asyncio
import aiohttp

async def check_proxy_async(proxy_id):
    proxy = get_proxy(proxy_id)
    proxy_url = f"{proxy['protocol']}://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                'https://httpbin.org/ip',
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                return response.status == 200
        except:
            return False

async def check_proxies_batch_async(proxy_ids):
    tasks = [check_proxy_async(pid) for pid in proxy_ids]
    results = await asyncio.gather(*tasks)
    return results
```

#### 10.2.2 代理质量评分

**当前：** 仅统计成功/失败次数

**优化：** 综合评分系统

```python
def calculate_proxy_score(proxy):
    """
    计算代理质量评分（0-100）
    """
    success_rate = proxy['success_count'] / (proxy['success_count'] + proxy['fail_count'] + 1)
    response_time_score = max(0, 100 - proxy['avg_response_time'] * 20)
    uptime_score = min(100, proxy['uptime_hours'] / 24 * 100)
    
    score = (
        success_rate * 0.5 +
        response_time_score * 0.3 +
        uptime_score * 0.2
    ) * 100
    
    return round(score, 2)
```

#### 10.2.3 智能代理选择

**当前：** 随机选择可用代理

**优化：** 加权随机选择（质量高的代理被选中概率更大）

```python
import random

def get_available_proxy_smart(city=None):
    """智能选择代理（加权随机）"""
    proxies = get_all_proxies(status='active', city=city)
    
    # 计算权重（质量评分）
    weights = [calculate_proxy_score(p) for p in proxies]
    
    # 加权随机选择
    selected = random.choices(proxies, weights=weights, k=1)[0]
    
    return selected
```

#### 10.2.4 代理自动扩容/缩容

**当前：** 手动添加/移除代理

**优化：** 根据负载自动调整

```python
def auto_scale_proxies():
    """自动扩容/缩容"""
    stats = get_binding_stats()
    
    # 计算平均负载
    avg_load = stats['total_accounts'] / stats['total_proxies']
    
    if avg_load > 4:
        # 负载过高，触发扩容告警
        notify_admin("代理池负载过高，建议添加代理")
    
    elif avg_load < 2:
        # 负载过低，可以移除部分代理
        # 找出负载最低的代理
        low_load_proxies = [p for p in get_all_proxies() if p['bound_count'] == 0]
        # 禁用部分代理
        for proxy in low_load_proxies[:5]:
            update_proxy(proxy['id'], {'status': 'disabled'})
```

#### 10.2.5 Redis 连接池优化

**当前：** 每个模块单独创建连接

**优化：** 全局连接池单例

```python
# redis_pool.py
import redis

_pool = None

def get_redis_pool():
    """获取全局 Redis 连接池"""
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool(
            host=REDIS_SET_URL['host'],
            port=REDIS_SET_URL['port'],
            db=REDIS_SET_URL['db'],
            password=REDIS_SET_URL['password'],
            max_connections=50,
            decode_responses=True
        )
    return _pool

def get_redis_client():
    """获取 Redis 客户端"""
    pool = get_redis_pool()
    return redis.Redis(connection_pool=pool)
```

---

## 附录

### A. Redis 数据结构

#### A.1 代理信息

```
Key: mcd:proxy:{proxy_id}
Type: String (JSON)
Value:
{
  "id": "proxy_001",
  "host": "61.155.141.105",
  "port": 20080,
  "username": "user",
  "password": "pass",
  "protocol": "http",
  "status": "active",
  "city": "上海",
  "bound_count": 3,
  "success_count": 156,
  "fail_count": 2,
  "avg_response_time": 1.2,
  "last_check": "2026-10-10T10:30:00",
  "created_at": "2027-01-01T00:00:00"
}
```

#### A.2 代理ID集合

```
Key: mcd:proxy:all
Type: Set
Members: ["proxy_001", "proxy_002", ...]
```

#### A.3 可用代理ID集合

```
Key: mcd:proxy:available
Type: Set
Members: ["proxy_001", "proxy_003", ...]  # 仅包含 status='active' 的代理
```

#### A.4 健康检查缓存

```
Key: mcd:proxy:health:{proxy_id}
Type: String
Value: "1" (健康) 或 "0" (不健康)
TTL: 30秒
```

#### A.5 账号绑定信息

```
Key: mcd:account_binding:{phone}
Type: String (JSON)
Value: （见 3.2 节）
TTL: 0（永不过期）
```

#### A.6 观察期账号集合

```
Key: mcd:accounts_in_observation
Type: Set
Members: ["13800138000", "13800138001", ...]
```

### B. 常见错误码

| 错误码 | 说明 | 解决方案 |
|-------|------|----------|
| `PROXY_NOT_FOUND` | 代理不存在 | 检查代理ID是否正确 |
| `NO_AVAILABLE_PROXY` | 无可用代理 | 添加更多代理或检查代理健康状况 |
| `BINDING_NOT_FOUND` | 账号绑定不存在 | 重新分配代理 |
| `PROXY_OVERLOAD` | 代理负载过高 | 增加代理或提高 max_accounts_per_proxy |
| `REDIS_CONNECTION_ERROR` | Redis 连接失败 | 检查 Redis 配置和网络连接 |

### C. 参考资料

- [代理池管理系统更新日志](../mcd_project/CHANGELOG.md)
- [麦当劳项目交接文档](FINAL_HANDOVER.md)
- [项目 README](../mcd_project/README.md)

---

**文档版本**: v1.0  
**最后更新**: 2026-10-10  
**维护者**: Claude Code + 用户协作

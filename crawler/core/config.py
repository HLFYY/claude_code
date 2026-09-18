"""
所有平台共用的基础设施配置：Redis/Mongo 连接参数和 Redis key 前缀。故意不包含
任何平台策略（配额限额、试用期长度、代理绑定上限、indexId 列表等）——这些因平台
而异，放在 platforms/<name>/config.py 里。core/ 下的代码永远接收一个
`platform: str` 参数，不硬编码任何具体平台。

本地/线上区分：按 hostname 里是否含 "houjie-"（用户本机 MacBook 的 hostname
前缀，如 houjie-MacBook-Pro14.local）判断，不用环境变量——这是用户在其它项目
（如 TwitterSpider）里的一贯写法。取不到 hostname（异常/空字符串）也按本地处理。

这个文件本身在 .gitignore 里（顶层 `.gitignore` 有一条通配所有路径下的
`config.py`），所以直接把线上连接信息（含密码）写死在这里是安全的，不会被
提交进 git。
"""
from __future__ import annotations

import platform as _platform
import socket
from pathlib import Path

try:
    _os_type = _platform.system()
    _socket_name = socket.gethostname()
except Exception:
    _socket_name = ""
    _os_type = ""

ONLINE = not ("houjie-" in _socket_name or not _socket_name)

# 本地/线上两份连接信息都留着（不是只留当前机器解析出来的那份）——
# migrate_data.py 的 sync_local_to_online() 要在本地机器上跑，但需要显式
# 拿到线上那份连接信息（这台机器上 ONLINE=False，下面的 REDIS_HOST 等解析出
# 来的是本地值），所以两份都得留一个能直接引用的地方，不能只留 if/else 选出
# 来的那一份。
LOCAL_DB = {
    "redis_host": "localhost",
    "redis_port": 6379,
    "redis_db": 0,
    "redis_password": None,
    "mongo_uri": "mongodb://localhost:27017",
    "mongo_db": "crawler",
    "log_root": Path(__file__).parent.parent / "logs",
}
ONLINE_DB = {
    "redis_host": "47.109.103.223",
    "redis_port": 6379,
    "redis_db": 0,
    "redis_password": "123.456.",
    # 密码非空时连接串按 user:pwd@host:port/{认证库} 拼，实际读写用的库由
    # "mongo_db" 决定（core/mongo_client.py 的 get_db() 是 client[MONGO_DB_NAME]，
    # 跟连接串里这个认证库路径是两回事）——跟用户在其它项目里 MONGO_SETTING 的
    # 拼法一致：有密码用 default_db（这里是 "admin"）当认证库，没密码就直接用
    # 目标库。
    "mongo_uri": "mongodb://root:yjOHGyX865uKqthF@210.14.142.252:8807/admin",
    "mongo_db": "crawler",
    "log_root": Path("/root/logs"),
}

_resolved = ONLINE_DB if ONLINE else LOCAL_DB
REDIS_HOST = _resolved["redis_host"]
REDIS_PORT = _resolved["redis_port"]
REDIS_DB = _resolved["redis_db"]
REDIS_PASSWORD = _resolved["redis_password"]
MONGO_URI = _resolved["mongo_uri"]
MONGO_DB_NAME = _resolved["mongo_db"]
LOG_ROOT = _resolved["log_root"]

# 所有 key 都挂在这个前缀下，这样这个 Redis 实例可以和其他用途共用而不会
# key 冲突。平台维度的区分在每个 key 内部实现（如 "account:{platform}:{email}"），
# 不在这里。
KEY_PREFIX = "crawler:"

# --- core.scheduler.dispatch() 的单账号请求冷却默认值 ---
# 严格说这是个例外：上面说了 core/ 不放平台策略，但这个数字是"调度节奏"这种通用
# 机制性的默认值，不是某个具体网站的业务限额（不像配额次数/试用期天数那样天然
# 跟单个网站强绑定）。是否使用这个默认值完全由各平台自己的 config.py 决定——
# 不想用就在自己的 config.py 里写具体数字覆盖掉，core 不会替平台做这个决定，
# scheduler.dispatch() 自己的参数默认值仍然是 0（不冷却），没有主动接入这个
# 功能的平台（比如目前的 wkinfo）行为完全不受影响。
DEFAULT_DISPATCH_MIN_INTERVAL_SECONDS = 5

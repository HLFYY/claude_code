import os
import platform
import socket
import time
import redis

CODE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.expanduser('~/datas/mcd')
os.makedirs(DATA_DIR, exist_ok=True)
LOG_DIR = os.path.expanduser('~/logs')
os.makedirs(LOG_DIR, exist_ok=True)

REDIS_SET_URL = dict(
    host='db.selfmediaai.cn',
    port=26379,
    db=0,
    password='123.456.',
)


def make_redis(**overrides):
    """统一的 Redis 客户端：慢/抖动时快速失败+重试+keepalive，别再卡 60 秒。
    兼容 redis-py 3.2.1(本地) 与 5.0.1(线上)。
    测试可用环境变量覆盖到本地 Redis：MCD_REDIS_HOST=127.0.0.1 [MCD_REDIS_PORT/DB/PASSWORD]。"""
    if os.environ.get('MCD_REDIS_HOST'):
        host = os.environ['MCD_REDIS_HOST']
        port = int(os.environ.get('MCD_REDIS_PORT', 6379))
        db = int(os.environ.get('MCD_REDIS_DB', 0))
        password = os.environ.get('MCD_REDIS_PASSWORD') or None  # 本地一般无密码
    else:
        host, port = REDIS_SET_URL['host'], REDIS_SET_URL['port']
        db, password = REDIS_SET_URL['db'], REDIS_SET_URL['password']
    kw = dict(
        host=host, port=port, db=db, password=password,
        decode_responses=True,
        socket_connect_timeout=15,   # 连接超时
        socket_timeout=25,          # 读写超时(别用系统默认~60s)
        socket_keepalive=True,
        retry_on_timeout=True,      # 超时自动重试一次
    )
    kw.update(overrides)
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")}, Redis配置:{kw}', flush=True)
    try:
        return redis.Redis(health_check_interval=30, **kw)  # 用前 PING 探活，剔除死连接(redis>=3.3)
    except TypeError:
        return redis.Redis(**kw)  # 旧版(3.2.1)无 health_check_interval

# ========== 全局代理开关 ==========

# 本机开发=True / 线上服务器=False（按 hostname 判定，沿用项目惯例）
IS_LOCAL = 'houjie-' in socket.gethostname()
# 本机(住宅IP)不走代理，线上服务器走白名单住宅代理；也可手动覆盖为 True/False
USE_PROXY = True

# 浏览器(自动化过滑块)代理认证方式：
#   True  = 动态 Chrome 扩展带账号密码(支持需认证的代理)
#   False = 白名单(浏览器只用 IP:端口，代理侧按 IP 放行)
# 注意：requests(发码/业务请求)始终用完整账密代理，不受此开关影响。
PROXY_BROWSER_AUTH = True

# ========== 验证码(滑块)登录 worker 配置 ==========

CAPTCHA_LOGIN_CONFIG = {
    'queue_key': 'mcd:captcha:login_queue',          # worker BLPOP 的登录队列
    'freezing_key': 'mcd:captcha:freezing:{phone}',  # 账号冻结标志
    'status_key': 'mcd:captcha:status:{phone}',      # 发码状态 JSON
    'freeze_ttl': 15 * 60,                            # 冻结兜底上限(秒)
    'status_ttl': 15 * 60,                            # 状态窗口(秒)，与冻结对齐
    'poll_interval': 10,                              # run_api_client 轮询间隔(秒)——Redis 慢，放长
    'poll_timeout': 15 * 60,                          # run_api_client 轮询上限(秒)

    # 心跳(防"冻结了但 worker 没在跑")——Redis 慢，间隔/TTL 放长，减少读写频次
    'worker_alive_key': 'mcd:captcha:worker_alive',   # 全局：worker 进程活着(TTL 内)
    'worker_alive_ttl': 30,                           # 全局心跳 TTL(秒)
    'processing_key': 'mcd:captcha:processing:{phone}',  # 按手机号：该账号登录正在进行/排队
    'processing_ttl': 120,                            # 单账号处理心跳 TTL(秒，够一次过滑块)
    'blpop_timeout': 10,                              # worker BLPOP 超时(秒)，兼顾刷心跳频次与空闲时的 Redis 压力
    'beat_min_interval': 5,                           # beat 节流：两次心跳写最小间隔(秒)
}

# ========== 代理配置 ==========

# 请求超时时间（秒）
PROXY_REQUEST_TIMEOUT = 15

# 代理失败容错配置
PROXY_FAILURE_CONFIG = {
    # 加入观察期的失败次数阈值
    'observation_threshold': 3,

    # 观察期内直接永久换绑的失败次数阈值
    'permanent_switch_threshold': 10,

    # 观察期时长（秒）- 5分钟
    'observation_period': 5 * 60,

    # 失败计数时间窗口（秒）- 10分钟
    # 超过此时间的失败记录会被清除
    'failure_window': 10 * 60,
}

# 代理健康检查配置
PROXY_HEALTH_CHECK_CONFIG = {
    # 全量健康检查间隔（秒）- 12小时
    'full_check_interval': 12 * 3600,

    # 观察期检查间隔（秒）- 5分钟
    'observation_check_interval': 5 * 60,

    # 健康检查超时时间（秒）
    'health_check_timeout': 10,

    # 健康检查缓存TTL（秒）- 避免短时间重复检查同一代理
    'health_check_cache_ttl': 30,

    # 健康检查批次大小（避免瞬间大量请求）
    'batch_size': 10,

    # 批次间隔（秒）
    'batch_delay': 6,
}

# 代理池配置
PROXY_POOL_CONFIG = {
    # 代理状态标记
    'status': {
        'active': 'active',      # 正常可用
        'slow': 'slow',          # 响应慢但可用
        'observation': 'observation',  # 观察期
        'failed': 'failed',      # 失效
        'disabled': 'disabled',  # 手动禁用
    },

    # 响应时间阈值（秒）
    'slow_threshold': 3.0,  # 超过3秒标记为slow

    # 连续失败多少次标记为failed
    'max_fail_count': 5,

    # 单个代理最大账号负载
    'max_accounts_per_proxy': 5,

    # Redis Key 前缀
    'redis_prefix': 'mcd:proxy',
}

# 账号代理绑定配置
ACCOUNT_PROXY_BINDING_CONFIG = {
    # Redis Key
    'redis_key': 'mcd:account_proxy_binding',

    # 绑定关系过期时间（秒）- 0表示永不过期
    'binding_ttl': 0,

    # 负载均衡策略
    'balance_strategy': 'least_bound',  # least_bound: 绑定账号数最少优先
                                        # round_robin: 轮询
                                        # random: 随机
}
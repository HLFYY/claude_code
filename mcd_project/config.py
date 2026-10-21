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

# 代理模式（USE_PROXY=True 时生效）：
#   'static_after_login' = 登录成功后绑定静态代理，之后所有请求走静态（原逻辑）
#   'always_short'       = 全程短效代理，不绑静态；/api/login/send_code 每次换新代理，
#                          其他接口用指定城市(city 参数)的复用代理(短效池轮巡)
PROXY_MODE = 'static_after_login'

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
    'max_accounts_per_proxy': 10,

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

# ========== 数美设备身份池配置 ==========
# 设备 = 一套自洽假设备指纹经 app 注册得到的独立 smid(d="B"+smid)。
# 空闲队列用 List 保证"先生成先取"(FIFO);每设备全部参数用 Hash 存,取用不删除只标记账号。
DEVICE_POOL_CONFIG = {
    'free_key': 'mcd:device:free',                  # List: 空闲设备 id 队列(RPUSH 入队, LPOP 领用=FIFO)
    'info_key': 'mcd:device:info',                  # Hash: field=设备id, value=该设备全部参数JSON(单 key,避免 key 膨胀)
    'account_key': 'mcd:device:account:{account}',  # List: 账号领用过的设备 id(一账号可多设备)
    'index_key': 'mcd:device:index',                # Set: 已有 smid(生成时查重,避免重复设备)
    'min_free': 10,                                 # 最小空闲设备数,低于则补足
    'build_batch_cap': 5,                          # 单轮补池上限(防一次造太多/触发风控)
    'check_interval': 15 * 60,                      # 巡检补池间隔(秒)
    'build_mode': 'local',                          # 'frida'=真机frida造 / 'local'=本地纯HTTP造(mcd_shumei_d+注册接口,不要真机)
}

# ========== 神龙短效代理配置 ==========
# 短效代理(提一次用~3分钟就失效,实测上海176s/大连181s),区别于手动 add_proxy 的长效代理。
# 提取接口: http://api.shenlongip.com/ip?key=..&protocol=1&mr=1&pattern=txt&count=1&area=310000&sign=..
#   area=国标6位行政区划码; mr=去重; pattern=txt/json; sign=账户签名(实测不绑area,一个sign通用所有城市)

SHENLONG_EXTRACT_URL = 'http://api.shenlongip.com/ip'

# 多个提取 key: 轮换使用分摊单 key 频率限制,每个 key 配自己的 sign。
SHENLONG_KEYS = [
    {'key': 'nplj5hbg', 'sign': '55cd7d441a3dfaf7498e0ff5d6995037'},
]

# 固定参数(key/sign 由 utils 轮换注入,不放这里)
SHENLONG_EXTRACT_PARAMS = {
    'protocol': 1,   # 1=HTTP/HTTPS
    'mr': 1,         # 去重
    'pattern': 'txt',
}

# 短效代理池(Redis)配置
SHORT_PROXY_CONFIG = {
    'pool_key': 'mcd:proxy:short:{city}',  # List: 该城市未过期代理池; RPOPLPUSH 原子轮巡(多进程安全), 整池 TTL 到期自动清->重提
    'ttl': 150,                            # 秒, 实测存活~3min 留余量
    # 提取数量由 get_short_proxy(count=) 传参(默认1),不放配置
}

# 神龙支持的节点城市 -> 国标6位行政区划码
# 来源: shenlongip.com/resourceCenter/pptp 节点清单(约70城,此处精选常用的,可按需补全)。
# 注意: 深圳/北京/杭州/武汉等大城市神龙【无节点】,会自动取最近支持城市(见 utils.nearest_supported_city)。
SHENLONG_SUPPORTED = {
    # 华东
    '上海': '310000', '南京': '320100', '苏州': '320500', '无锡': '320200', '南通': '320600',
    '宁波': '330200', '嘉兴': '330400', '绍兴': '330600', '济南': '370100', '烟台': '370600',
    # 华南
    '广州': '440100', '珠海': '440400', '桂林': '450300',
    '厦门': '350200', '福州': '350100', '泉州': '350500',
    # 华北
    '石家庄': '130100', '唐山': '130200', '廊坊': '131000', '秦皇岛': '130300',
    # 东北
    '大连': '210200', '鞍山': '210300', '锦州': '210700',
    # 华中
    '襄阳': '420600', '咸宁': '421200', '益阳': '430900', '郴州': '431000',
    # 西南
    '成都': '510100', '绵阳': '510700', '重庆': '500000',
    # 海南
    '三亚': '460200',
}

# 城市经纬度(lng, lat) —— 用于"目标城市不支持时取最近支持城市"的距离估算。
# 含支持城市 + 常见不支持大城市(北京/深圳等),市中心近似值即可。
CITY_GEO = {
    # —— 支持城市 ——
    '上海': (121.47, 31.23), '南京': (118.80, 32.06), '苏州': (120.62, 31.32),
    '无锡': (120.30, 31.57), '南通': (120.86, 31.98), '宁波': (121.55, 29.88),
    '嘉兴': (120.76, 30.75), '绍兴': (120.58, 30.03), '济南': (117.00, 36.65),
    '烟台': (121.39, 37.54), '广州': (113.26, 23.13), '珠海': (113.55, 22.27),
    '桂林': (110.29, 25.27), '厦门': (118.09, 24.48), '福州': (119.30, 26.08),
    '泉州': (118.68, 24.87), '石家庄': (114.51, 38.04), '唐山': (118.18, 39.63),
    '廊坊': (116.70, 39.52), '秦皇岛': (119.60, 39.94), '大连': (121.62, 38.92),
    '鞍山': (122.99, 41.11), '锦州': (121.13, 41.10), '襄阳': (112.14, 32.04),
    '咸宁': (114.32, 29.84), '益阳': (112.36, 28.55), '郴州': (113.01, 25.77),
    '成都': (104.07, 30.57), '绵阳': (104.68, 31.47), '重庆': (106.55, 29.56),
    '三亚': (109.51, 18.25),
    # —— 常见不支持大城市(供最近邻定位) ——
    '北京': (116.40, 39.90), '深圳': (114.06, 22.55), '天津': (117.20, 39.13),
    '杭州': (120.15, 30.28), '武汉': (114.30, 30.59), '西安': (108.94, 34.34),
    '郑州': (113.62, 34.75), '长沙': (112.94, 28.23), '青岛': (120.38, 36.07),
    '沈阳': (123.43, 41.80), '哈尔滨': (126.53, 45.80), '长春': (125.32, 43.82),
    '昆明': (102.83, 24.88), '南宁': (108.37, 22.82), '合肥': (117.23, 31.82),
    '南昌': (115.86, 28.68), '太原': (112.55, 37.87), '贵阳': (106.63, 26.65),
    '兰州': (103.84, 36.06), '海口': (110.33, 20.03),
}
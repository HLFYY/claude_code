"""已知客户端构建的 SecBox 配置，不从版本号推导未知密钥。

7.0.45.1 的配置来自运行时 Hook，字段用途按当前 JADX 工程中的调用点核对。
SecBox 的字段经 init() -> JniLib0.cV() 初始化，Java 中的初始空串不是实际值。
这些是应用级配置，不是账号的 token、sid 或 refreshToken，不按账号存入 Redis。
版本号仅用于选择已验证配置，不能保证同版本不同构建、渠道或热修复后的值相同。
未取得的可选字段保留空串，不代表对应功能可以使用空密钥或回退混用其他密钥。
新增字段中的 wskey 已接入保活及常驻守护心跳；其他新增字段仅备用，保存不等于接入业务。
备用字段从同目录的 mcd_profiles.local.json 按版本读取，该文件不提交 Git。
文件缺失、不可读、格式错误或字段无效时忽略对应备用配置，不影响业务及心跳。
密钥不应写入普通日志；repr=False 只隐藏默认对象展示，不防止主动序列化泄露。
"""

import json
import os
from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class ClientProfile:
    """客户端版本、请求标识及各功能独立使用的 SecBox 参数。

    version：客户端版本，对应请求头 v 和业务 User-Agent 中的版本号。
    aes_key：SecBox.getAesKey()。SecurityUtils 用于 AES 对称加解密，
        Java 默认算法为 AES/ECB/PKCS5Padding；具体业务字段需追踪调用方。
    v4ak：SecBox.getV4ak()。HTTP v4 签名的访问标识，写入
        Authorization 的 hmac-auth-v1 段，并参与签名明文构造；不是账号 token。
    v4sk：SecBox.getV4sk()。qf.e 使用的 HTTP v4 HMAC-SHA256 密钥，
        用于 Authorization 签名和 X-HMAC-DIGEST；不能替代 wskey。
    platform：业务请求头 p 的平台标识；本配置中 Android 为字符串 "102"。
    client_type：请求头 ct 的客户端类型；本配置中 Android 为字符串 "102"。
    api_base：该构建已配置的业务 API 基地址；不是 WebSocket 或第三方 SDK 地址。
    sign_key：SecBox.getSignKey() 的运行时值。当前 JADX 对该 getter 的直接
        交叉引用为 0，具体用途尚未确认；不能仅凭名称认定它用于业务签名。
    wskey：SecBox.getWskey()。de.i（SocketRequest.kt）用于 WebSocket
        握手签名：Base64_NO_WRAP(HMAC_SHA256(UTF8(wskey), UTF8(message)))。
        message = ct + meddyId + scene + sid + st + sv + v，无分隔符；
        本次 ct="102"、scene=""、sv="v4"，st 为服务端校正毫秒时间的字符串。
        密钥直接取字符串 UTF-8 字节，不先 Base64 解码，账号 token 不参与该签名。
        7.0.45.1 的该值已离线匹配三轮抓包握手；未据此完成主动连接验证。
    tmsk：SecBox.getTMSK()。腾讯地图搜索服务的 SecretKey，传入
        TencentSearch(context, tmsk) 后保存为 mSecretKey；用于路线规划、
        逆地理编码等请求，不是 TencentSearch 的 mApiKey，也不是业务 v4sk。
    ta_app_id：SecBox.getTaAppId()。MCDASRManager.doRealInit() 将其
        转为整数传入腾讯语音识别 AAIClient，属于语音应用 ID，不是账号标识。
    ta_sid：SecBox.getTaSID()。传入同一 AAIClient 的凭证标识（SecretId
        角色）；不是麦当劳账号登录 sid，不随账号会话刷新。
    ta_sk：SecBox.getTaSK()。传入腾讯语音识别 LocalCredentialProvider，
        保存为 secretKey 并用于语音识别签名，不是埋点或 WebSocket 密钥。

    Java 调用点确认的是当前默认路径的用途，不证明密钥永久不变，也不证明服务端
    权限或有效期。HTTP、WebSocket、地图及语音的参数不能互相替代。
    """

    version: str
    aes_key: str = field(repr=False)
    v4ak: str = field(repr=False)
    v4sk: str = field(repr=False)
    platform: str = '102'
    client_type: str = '102'
    api_base: str = 'https://api.mcd.cn'
    sign_key: str = field(default='', repr=False)
    wskey: str = field(default='', repr=False)
    tmsk: str = field(default='', repr=False)
    ta_app_id: str = field(default='', repr=False)
    ta_sid: str = field(default='', repr=False)
    ta_sk: str = field(default='', repr=False)

    @property
    def user_agent(self):
        return f'mcdonald_Android/{self.version} (Android)'


CLIENT_PROFILES = {
    '7.0.41.0': ClientProfile(
        '7.0.41.0', 'w8ZJ4wrUl7dDB1A7', 'HJ7YLqOY06F61FPEhF7H',
        'JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp',
    ),
    '7.0.45.1': ClientProfile(
        '7.0.45.1', 'w8ZJ4wrUl7dDB1A7', 'YmV1nJ6bCs5nmbxKTe9z',
        'ipxpa0xYshlRby1U1QU2oYsmQEtw3vpoHlJczAnz',
        api_base='https://api2.mcd.cn',
        wskey='vuqAv2DQaKVhyb74/1/6vtTTN0GPIdsfRAKidnK7SNg=',
    ),
}


def _load_local_backups():
    path = os.path.join(os.path.dirname(__file__), 'mcd_profiles.local.json')
    try:
        with open(path, encoding='utf-8') as config_file:
            backups = json.load(config_file)
    except (OSError, ValueError):
        return
    if not isinstance(backups, dict):
        return
    optional_fields = ('sign_key', 'tmsk', 'ta_app_id', 'ta_sid', 'ta_sk')
    for version, profile in CLIENT_PROFILES.items():
        values = backups.get(version)
        if not isinstance(values, dict):
            continue
        overrides = {name: values[name] for name in optional_fields
                     if isinstance(values.get(name), str)}
        CLIENT_PROFILES[version] = replace(profile, **overrides)


_load_local_backups()


def get_client_profile(version=None):
    selected = version if version is not None else os.environ.get('MCD_APP_VERSION', '7.0.45.1')
    if selected not in CLIENT_PROFILES:
        raise ValueError(f'未配置客户端版本 {selected!r} 的 SecBox 密钥，禁止回退混用')
    return CLIENT_PROFILES[selected]

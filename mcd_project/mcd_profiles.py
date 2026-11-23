"""已知客户端构建的 SecBox v4 配置，不从版本号推导未知密钥。"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ClientProfile:
    version: str
    aes_key: str = field(repr=False)
    v4ak: str = field(repr=False)
    v4sk: str = field(repr=False)
    platform: str = '102'
    client_type: str = '102'
    api_base: str = 'https://api.mcd.cn'

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
    ),
}


def get_client_profile(version=None):
    selected = version if version is not None else os.environ.get('MCD_APP_VERSION', '7.0.45.1')
    if selected not in CLIENT_PROFILES:
        raise ValueError(f'未配置客户端版本 {selected!r} 的 SecBox 密钥，禁止回退混用')
    return CLIENT_PROFILES[selected]

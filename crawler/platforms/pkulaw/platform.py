"""
pkulaw 对 core.platform_base.Platform 的实现。跟 wkinfo 不一样的地方：pkulaw 的
验证码登录本身就是"没有账号就注册、有账号就登录"，没有独立的注册接口/固定密码，所以
register() 直接等价于走一遍交互式登录（见 login.login_interactive）；search 还没
逆向，调用会直接报 NotImplementedError。

identifier 可以是手机号或邮箱，login.py 会自动判断类型分发到对应逻辑
（account_type.detect），调用方不需要自己说是手机号还是邮箱。
"""
from __future__ import annotations

import requests

from . import config, detail_client, login


class PkulawPlatform:
    name = config.PLATFORM

    def register(self, *, identifier: str, code_getter=None) -> dict:
        session = login.login_interactive(identifier, code_getter)
        return {"identifier": identifier, "cookies": requests.utils.dict_from_cookiejar(session.cookies)}

    def login(self, identifier: str, password: str | None = None):
        # password 留空就走验证码登录，有值就走密码登录，由 login.get_session 内部
        # 根据账号记录里存没存密码自动判断，这个参数本身在 pkulaw 这边不会被直接用到
        # （纯粹为了满足 core.platform_base.Platform 协议的形状）。
        session = login.get_session(identifier)
        return session, {}

    def change_password(self, identifier: str, new_password: str, code_getter=None) -> None:
        login.change_password(identifier, new_password, code_getter)

    def search(self, resource: str, query: str, **kwargs) -> dict:
        raise NotImplementedError("pkulaw 平台还没有逆向 search，目前只支持 view_detail")

    def view_detail(self, resource: str, doc_id: str, **kwargs) -> dict:
        identifier = kwargs.pop("identifier")
        return detail_client.view_detail(resource, doc_id, identifier, **kwargs)


platform = PkulawPlatform()

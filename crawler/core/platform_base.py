"""
每个平台插件对外应该呈现的结构化契约（供以后一个通用的多平台
runner、CLI，或者别的什么统一调用方式使用）。用 typing.Protocol——
结构化约束而不是继承——所以平台模块只需要有匹配的函数/属性，不需要继承
任何基类。平台内部的代码直接调用 core.*（account_registry、quota_tracker、
scheduler、proxy_pool），带上自己的 `name` 参数；这个 Protocol 只约束
*对外*的形状。
"""
from __future__ import annotations

from typing import Any, Protocol


class Platform(Protocol):
    name: str  # 比如 "wkinfo"——必须和 collection 名/Redis 分区用的 platform 字符串一致

    def register(self, **kwargs) -> dict:
        """创建一个新账号。注册需要什么平台特定的东西（手机号/邮箱/随机资料
        字段/验证码/……）完全由具体实现决定；返回保存后的 account_registry 记录。"""
        ...

    def login(self, email: str, password: str) -> tuple[Any, dict]:
        """返回已认证账号的 (session, profile)。"""
        ...

    def search(self, resource: str, query: str, **kwargs) -> dict:
        """resource 是这个平台对"内容分类范围"的叫法（wkinfo 管它叫 indexId）。"""
        ...

    def view_detail(self, resource: str, doc_id: str, **kwargs) -> dict:
        ...

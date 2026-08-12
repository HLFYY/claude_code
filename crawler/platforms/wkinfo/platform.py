"""
wkinfo 对 core.platform_base.Platform 的实现——这是一个统一的形状，供以后
一个通用的多平台 runner/CLI 使用，不需要了解 wkinfo 的内部细节。这里内部
只是一层薄封装：真正的逻辑都在 registration_worker.py / login.py /
search_client.py 里，它们已经直接带着 config.PLATFORM 调用 core.* 了。
"""
from __future__ import annotations

from . import config
from .registration_worker import register_one
from .login import get_session
from .search_client import search as _search, view_detail as _view_detail


class WkinfoPlatform:
    name = config.PLATFORM

    def register(self, *, telephone: str, user_email: str, **kwargs) -> dict:
        return register_one(telephone, user_email, **kwargs)

    def login(self, email: str, password: str):
        return get_session(email, password)

    def search(self, resource: str, query: str, **kwargs) -> dict:
        return _search(resource, query, **kwargs)

    def view_detail(self, resource: str, doc_id: str, **kwargs) -> dict:
        return _view_detail(resource, doc_id, **kwargs)


platform = WkinfoPlatform()

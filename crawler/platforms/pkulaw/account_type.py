"""
账号标识（手机号 or 邮箱）类型判断，供 login.py 统一按类型分发到对应的手机/邮箱
逻辑用。11 位、以 1 开头的纯数字当手机号；含 "@" 当邮箱；两者都不是就报错，不猜。
"""
from __future__ import annotations

import re

PHONE = "phone"
EMAIL = "email"

_PHONE_RE = re.compile(r"^1\d{10}$")


def detect(identifier: str) -> str:
    """返回 PHONE 或 EMAIL。"""
    if _PHONE_RE.match(identifier):
        return PHONE
    if "@" in identifier:
        return EMAIL
    raise ValueError(f"account_type.detect: 无法判断 {identifier!r} 是手机号还是邮箱")

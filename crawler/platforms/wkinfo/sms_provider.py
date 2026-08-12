"""
拿到真实的短信验证码是唯一没法自动化/逆向的一步——它是一条真实短信。这个
模块存在的意义是让"怎么拿到验证码"是一个单独的可替换函数，而不是散落在
registration_worker.py 各处。现在就是简单的 input()；等有了真正的接收机制
（接码平台、收件箱API之类），只需要替换 get_sms_code 的实现，不用改任何
调用方。
"""
from __future__ import annotations

from typing import Callable


def get_sms_code_via_input(telephone: str) -> str:
    return input(f"短信验证码已发送到 {telephone}，请输入收到的验证码: ").strip()


# 改这里就能改变所有调用方获取验证码的方式。
get_sms_code: Callable[[str], str] = get_sms_code_via_input

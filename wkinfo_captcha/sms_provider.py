"""
Getting the real SMS OTP is the one step that can't be automated/reverse
engineered -- it's an actual text message. This module exists so that "how do
we get the code" is a single swappable function, not scattered through
registration_worker.py. Right now it's just input(); when there's a real
receiving mechanism (a phone farm, an inbox API, whatever), swap
get_sms_code's body without touching any caller.
"""
from __future__ import annotations

from typing import Callable


def get_sms_code_via_input(telephone: str) -> str:
    return input(f"短信验证码已发送到 {telephone}，请输入收到的验证码: ").strip()


# Swap this to change how every caller gets the code.
get_sms_code: Callable[[str], str] = get_sms_code_via_input

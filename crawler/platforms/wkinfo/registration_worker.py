"""
编排单次账号注册的完整流程：随机资料字段（除手机号/邮箱之外的所有字段，
这两个必须是真实的，由调用方提供）+ captcha_client 的 full_registration_flow
（验证码识别 -> 短信 -> 提交）+ core.account_registry.save_account()。

注册成功之后，会真正登录一次，从服务端读取这个账号权威的试用到期时间
productsDetailList[].endDate（毫秒）——这比自己算"注册时间+3天"更可信
（注册和首次登录不一定是同一时刻，而且这样也少一个"网站到底给这个账号配了
多长试用期"的假设需要保持同步）。

代理绑定放在最前面：先从池子里挑一个代理，*整个*注册流程（包括验证码）都
用它，注册一成功就立刻绑定到账号，这样以后每次登录（login.py 会自动去查
这个绑定关系）都用账号出生时的同一个 IP。如果代理池没有空位，这里会直接
拒绝注册这个账号，而不是在没有固定 IP 的情况下悄悄继续。
"""
from __future__ import annotations

import random
import time

import requests

from core import account_registry, proxy_pool

from . import config, login
from .captcha_client import WkinfoCaptcha
from .random_profile import random_registration_fields


def _real_expiry_from_profile(profile: dict) -> float | None:
    """profile["productsDetailList"][0]["endDate"] 是服务端直接给出的这次
    试用到期的毫秒时间戳。返回秒，如果数据结构不是预期的样子就返回 None
    （调用方会回退用"注册时间 + ACCOUNT_TRIAL_SECONDS"来估算）。"""
    try:
        return profile["productsDetailList"][0]["endDate"] / 1000
    except (KeyError, IndexError, TypeError):
        return None


def register_one(telephone: str, user_email: str, password: str | None = None,
                  captcha_type: str | None = None) -> dict:
    """用给定的真实手机号/邮箱注册一个新试用账号，其他字段全部随机生成。
    `password` 可选——不传就跟以前一样随机生成一个；传了就用调用方指定的密码
    （比如调用方已经预先分配好了密码，要求跟登录时用的是同一个）。
    返回保存后的 account_registry 记录。任何一步失败都会抛异常（验证码、
    注册本身、确认登录，或者代理池没有空位）。
    """
    proxy_id = proxy_pool.pick_for_new_account(config.PLATFORM, config.MAX_ACCOUNTS_PER_IP)
    if proxy_id is None:
        raise RuntimeError(
            "no proxy available: pool is empty or every proxy is at "
            f"MAX_ACCOUNTS_PER_IP={config.MAX_ACCOUNTS_PER_IP} for platform={config.PLATFORM}"
        )
    proxied_session = requests.Session()
    proxied_session.proxies = proxy_pool.requests_proxies(proxy_id)

    fields = random_registration_fields()
    if password:
        fields["password"] = password
    captcha_type = captcha_type or random.choice(["blockPuzzle", "clickWord"])

    client = WkinfoCaptcha(captcha_type=captcha_type, session=proxied_session)
    result = client.full_registration_flow(
        telephone=telephone,
        user_email=user_email,
        password=fields["password"],
        company_name=fields["company_name"],
        province=fields["province"],
        last_name=fields["last_name"],
        first_name=fields["first_name"],
        post_id=fields["post_id"],
    )
    register_resp = result["register"]
    if register_resp.get("code") not in (None, "0000") and "code" in register_resp:
        # 比如 E_035_003 短信验证码错误、E_000_003 验证码校验失败等
        raise RuntimeError(f"registration failed: {register_resp}")

    account_fields = {
        "telephone": telephone,
        "password": fields["password"],
        "companyName": fields["company_name"],
        "province": fields["province"],
        "postId": fields["post_id"],
        "lastName": fields["last_name"],
        "firstName": fields["first_name"],
    }
    provisional_expiry = time.time() + config.ACCOUNT_TRIAL_SECONDS
    record = account_registry.save_account(
        config.PLATFORM, user_email, provisional_expiry, account_fields, proxy_id=proxy_id,
    )
    # 必须在下面的确认登录之前完成绑定——login.py 要查这个绑定关系才知道
    # 走哪个代理。
    proxy_pool.bind_account(config.PLATFORM, user_email, proxy_id)

    # 确认登录能成功，并拿到服务端真实的试用到期时间。
    _, profile = login.get_session(user_email, fields["password"], force=True)
    real_expiry = _real_expiry_from_profile(profile)
    if real_expiry:
        account_registry.update_expiry(config.PLATFORM, user_email, real_expiry)
        record["expires_at"] = real_expiry

    return record


if __name__ == "__main__":
    tel = input("真实手机号: ").strip()
    email = input("真实邮箱: ").strip()
    account = register_one(tel, email)
    print("registered:", account)

"""
McDonald's API 配置文件
保留必需的工具函数，签名逻辑统一使用 mcd_api.py
"""

import hashlib
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

# ============= 基础配置 =============
BASE_URL = "https://api.mcd.cn"
BASE_URL2 = "https://api2.mcd.cn"

# AES 加密密钥（用于加密手机号）
AES_KEY = 'w8ZJ4wrUl7dDB1A7'

# ============= 工具函数 =============

def aes_encrypt(plaintext: str, key: str = AES_KEY) -> str:
    """
    AES-128-ECB 加密

    Args:
        plaintext: 明文字符串
        key: AES 密钥（16字节）

    Returns:
        Base64 编码的密文
    """
    cipher = AES.new(key.encode('utf-8'), AES.MODE_ECB)
    padded_data = pad(plaintext.encode('utf-8'), AES.block_size)
    encrypted = cipher.encrypt(padded_data)
    return base64.b64encode(encrypted).decode('utf-8')


def encrypt_phone(phone: str) -> str:
    """
    加密手机号（用于登录）

    Args:
        phone: 手机号字符串

    Returns:
        加密后的手机号
    """
    return aes_encrypt(phone, AES_KEY)


# ============= 注意 =============
# 签名相关函数已移除，统一使用 mcd_api.py 中的实现：
# - build_headers()
# - generate_authorization()
# - generate_x_hmac_digest()

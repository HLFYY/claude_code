#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把一台真机的 数美设备指纹 d + 对应 token 写进某账号的登录凭证（自用单机单号）。

说明：
- d 是数美设备指纹，必须和注册它的那台设备的 token 配套（同一台真机抓包得到）。
- 建议在该账号【登录前】先设置，再走登录流程（登录会用这套 token+d）。
- 之后发码/登录/业务请求会自动带上该账号凭证里的 d（build_headers 兜底使用）。

用法: python set_device.py <手机号> <token> <d>
"""
import sys
import json
from datetime import datetime

from config import REDIS_SET_URL, make_redis

if len(sys.argv) < 4:
    print("用法: python set_device.py <手机号> <token> <d>")
    print("  token/d 为同一台真机抓包得到的配套值")
    sys.exit(1)

phone, token, d = sys.argv[1], sys.argv[2], sys.argv[3]

r = make_redis()
KEY = 'mcd:credentials'

raw = r.hget(KEY, phone)
rec = json.loads(raw) if raw else {}
old_token = rec.get('token')
rec['token'] = token      # 用真机设备 token（和 d 配套）
rec['d'] = d              # 数美设备指纹
rec.setdefault('sid', rec.get('sid', ''))
rec.setdefault('meddy_id', rec.get('meddy_id'))
rec['saved_at'] = datetime.now().isoformat()
r.hset(KEY, phone, json.dumps(rec, ensure_ascii=False))

print(f"✓ 已写入账号 {phone} 的凭证：")
print(f"    token = {token}")
print(f"    d     = {d[:40]}..（{len(d)} 字符）")
if old_token and old_token != token and rec.get('sid'):
    print("⚠️  token 变了，但凭证里还有旧 sid——该账号需要【重新登录】一次，sid 才和新 token/d 匹配。")

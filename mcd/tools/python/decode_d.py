#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
decode_d.py —— 解码麦当劳 d 值(数美设备指纹)
用法:
    decode_d.py <d值>
    echo <d值> | decode_d.py

d 结构:
    "B" + smid                      → 服务端已注册,直接是 smid
    "D" + base64({"os","encode","data":base64(设备JSON或密文),"tn","ep"})
"""
import sys
import base64
import json


def b64d(s):
    return base64.b64decode(s + "=" * (-len(s) % 4))


def main():
    d = (sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read()).strip()
    if not d:
        print("需要传入 d 值"); return 2
    prefix = d[0]
    print("d 前缀:", prefix, "总长:", len(d))
    if prefix == "B":
        print("类型: B+smid(服务端已注册)")
        print("smid =", d[1:])
        return 0
    if prefix != "D":
        print("未知前缀"); return 1

    outer = json.loads(b64d(d[1:]))
    print("\n--- 外层 ---")
    for k, v in outer.items():
        if k == "data":
            print("  data: <base64 len=%d>" % len(v))
        else:
            print("  %s: %r" % (k, v))

    raw = b64d(outer["data"])
    print("\n--- data 层 ---")
    try:
        data = json.loads(raw)
        print("  [明文 JSON] 字段共 %d 个:" % len(data))
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("\n>>> 结论: data 是明文 JSON,可纯代码复刻 d")
    except Exception:
        print("  [非明文] data 解 base64 后不是 JSON,极可能是 native 加密密文(short box)")
        print("  raw 字节长度:", len(raw))
        print("  前 48 字节(hex):", raw[:48].hex())
        print("\n>>> 结论: data 加密,第二步须走 frida-RPC 现调现取")
    return 0


if __name__ == "__main__":
    sys.exit(main())

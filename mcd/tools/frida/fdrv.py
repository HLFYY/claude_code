#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fdrv.py —— frida 注入驱动（getd.sh 调用的后端）
用法:
    fdrv.py attach <pid> <wait秒> <script1.js> [script2.js ...]
    fdrv.py spawn  <包名> <wait秒> <script1.js> [script2.js ...]
脚本按顺序加载（约定：先 bypass 反调试，再业务 hook）。
匹配 frida-server 15.1.17 / frida-python 15.1.17。
成功注入并加载完所有脚本返回 0；任一步失败返回非 0，便于 getd.sh 重试。
"""
import os
import sys
import time
import frida


def _device():
    # 多设备(如同时插 iPhone)时必须按序列号选 Android,避免 get_usb_device 选错
    dev_id = os.environ.get("FRIDA_DEV")
    return frida.get_device(dev_id, timeout=10) if dev_id else frida.get_usb_device(timeout=10)


def _on_message(script_name):
    def handler(message, data):
        if message.get("type") == "send":
            print("[{}] {}".format(script_name, message.get("payload")), flush=True)
        elif message.get("type") == "error":
            print("[{}][ERR] {}".format(script_name, message.get("stack") or message.get("description")), flush=True)
    return handler


def main():
    if len(sys.argv) < 5:
        print("用法: fdrv.py <attach|spawn> <pid|包名> <wait秒> <script.js>...", flush=True)
        return 2

    mode = sys.argv[1]
    target = sys.argv[2]
    wait = int(sys.argv[3])
    scripts = sys.argv[4:]

    dev = _device()

    pid = None
    if mode == "spawn":
        pid = dev.spawn([target])
        session = dev.attach(pid)
    else:  # attach
        session = dev.attach(int(target))

    loaded = []
    for path in scripts:
        with open(path, "r", encoding="utf-8") as f:
            code = f.read()
        s = session.create_script(code)
        name = path.rsplit("/", 1)[-1]
        s.on("message", _on_message(name))
        s.load()
        loaded.append(name)
        print("[fdrv] loaded {}".format(name), flush=True)

    if mode == "spawn" and pid is not None:
        dev.resume(pid)
        print("[fdrv] resumed {}".format(target), flush=True)

    print("[fdrv] 注入完成，保持 {}s，等待触发 [d-dump]…".format(wait), flush=True)
    time.sleep(wait)
    print("[fdrv] 时间到，分离。", flush=True)
    try:
        session.detach()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("[fdrv][FATAL] {}".format(e), flush=True)
        sys.exit(1)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# adb_relay.py —— 让 frida 借道"远端 adb server"连设备(frida 写死连本机 127.0.0.1:5037,不认 ADB_SERVER_SOCKET)。
#
# 场景: 多多云手机只在 Windows 客户端暴露, Mac 借 Windows 的 adb server(见 ../../docs/Mac连接Windows共享adb手机.md):
#   Windows: netsh portproxy 5555 -> 127.0.0.1:5037
#   Mac: 本脚本把本机 127.0.0.1:5037 中继到 <win-ip>:5555, frida 连本机5037即走到Windows的server。
#
# 用法:
#   先释放本机5037:  unset ADB_SERVER_SOCKET; adb kill-server
#   再跑:            python adb_relay.py <win-ip> [win-port=5555] [local-port=5037]
#   验证:            adb devices;  ~/venv/python3-forcrawl/bin/frida-ps -U
import socket, threading, sys

UP_HOST = sys.argv[1] if len(sys.argv) > 1 else "192.168.110.64"
UP_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5555
LISTEN_PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 5037
LISTEN = ("127.0.0.1", LISTEN_PORT)
UPSTREAM = (UP_HOST, UP_PORT)

def pipe(a, b):
    try:
        while True:
            d = a.recv(65536)
            if not d:
                break
            b.sendall(d)
    except Exception:
        pass
    finally:
        for s in (a, b):
            try: s.shutdown(socket.SHUT_RDWR)
            except Exception: pass

def handle(c):
    try:
        u = socket.create_connection(UPSTREAM, timeout=10)
    except Exception as e:
        print("upstream fail:", e, flush=True); c.close(); return
    threading.Thread(target=pipe, args=(c, u), daemon=True).start()
    threading.Thread(target=pipe, args=(u, c), daemon=True).start()

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(LISTEN); srv.listen(64)
    print("relay %s -> %s" % (LISTEN, UPSTREAM), flush=True)
    while True:
        c, _ = srv.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()

if __name__ == "__main__":
    main()

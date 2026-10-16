#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mitmdump 插件：从麦当劳请求里解析设备信息(token + d + tid)，并抓数美 deviceprofile 的 deviceId。
抓到后直接打印可用于 set_device.py 的命令。

用法：
  /Users/houjie/venv/python3-forcrawl/bin/mitmdump -s capture_device_info.py --listen-port 8080
  手机 WiFi 代理指到  本机IP:8080 （装过 mitm CA），正常打开麦当劳 App、随便点几下即可。

输出：
  - d 头有两种：'B'+smid = 已注册的设备指纹(要的就是这个)；'D'+base64 = 注册前的临时值(忽略)。
  - 结果同时存到 本仓库 mcd/resources/logs/device_info_latest.json
"""
import os
import json
from datetime import datetime

from mitmproxy import http

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'resources', 'logs')
OUT = os.path.abspath(os.path.join(LOG_DIR, 'device_info_latest.json'))


class DeviceInfoCapture:
    def __init__(self):
        self.seen = set()           # 去重 (token, d)
        self.best = None            # 最近一个 'B' 开头(已注册)的设备信息
        self.device_ids = set()     # 数美 deviceprofile 返回的 deviceId

    def request(self, flow: http.HTTPFlow):
        if 'mcd.cn' not in flow.request.pretty_host:
            return
        h = flow.request.headers
        d = h.get('d')
        token = h.get('token') or h.get('mcdtoken')
        tid = h.get('tid')
        if not d or not token:
            return
        key = (token, d)
        if key in self.seen:
            return
        self.seen.add(key)

        kind = '✅已注册(B+smid)' if d.startswith('B') else ('临时(D+vdata,忽略)' if d.startswith('D') else '未知')
        info = {
            'token': token, 'd': d, 'tid': tid,
            'kind': kind,
            'source': f"{flow.request.pretty_host}{flow.request.path.split('?')[0]}",
            'time': datetime.now().isoformat(),
        }
        self._print(info)
        if d.startswith('B'):
            self.best = info
            self._save()

    def response(self, flow: http.HTTPFlow):
        # 顺带抓数美 deviceprofile 返回的 deviceId (d = 'B' + deviceId)
        if 'fengkongcloud.com' not in flow.request.pretty_host or 'deviceprofile' not in flow.request.path:
            return
        try:
            dev = json.loads(flow.response.get_text()).get('detail', {}).get('deviceId')
            if dev and dev not in self.device_ids:
                self.device_ids.add(dev)
                print(f"\n[数美 deviceprofile] deviceId = {dev}")
                print(f"    => 对应 d 头应为:  B{dev}")
        except Exception:
            pass

    def _print(self, info):
        print("\n" + "=" * 90)
        print(f"📱 捕获设备信息  [{info['kind']}]")
        print("-" * 90)
        print(f"  token : {info['token']}")
        print(f"  d     : {info['d']}")
        print(f"  tid   : {info['tid']}")
        print(f"  来源  : {info['source']}")
        if info['d'].startswith('B'):
            print("-" * 90)
            print(f"  ✅ 写入账号(把 <手机号> 换成你的号)：")
            print(f"     python set_device.py <手机号> {info['token']} '{info['d']}'")
        print("=" * 90)

    def _save(self):
        try:
            with open(OUT, 'w', encoding='utf-8') as f:
                json.dump(self.best, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[保存失败] {e}")

    def done(self):
        if self.best:
            print(f"\n已保存最近的已注册设备信息到: {OUT}")
            print(f"  python set_device.py <手机号> {self.best['token']} '{self.best['d']}'")
        else:
            print("\n⚠️ 没抓到 'B' 开头的已注册 d —— 可能 App 刚启动还没注册完，多操作几下(登录/发码/浏览)再看。")


addons = [DeviceInfoCapture()]

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳 App 登录流程完整抓包脚本

用途：
1. 清理 APP 数据后重新打开，抓取完整的初始化流程
2. 找到 token (deviceInfoId) 的生成或获取逻辑
3. 找到手机号加密逻辑 (16752934813 -> cd56d8d91f6d92b6520686df3fbe32c8)
4. 找到短信验证码加密逻辑 (981447 -> 68b4a3a0ee88d2fa20271d35b5e6285b)

使用方法：
/Users/houjie/venv/python3-forcrawl/bin/mitmdump -s mitmproxy_capture_login.py

操作步骤：
1. 启动 mitmdump
2. 清理麦当劳 APP 数据（设置 -> 应用 -> 清除数据）
3. 重新打开 APP
4. 观察终端输出，找到关键请求
5. 进行登录流程
6. 查看保存的日志文件
"""

import json
import hashlib
from datetime import datetime
from mitmproxy import http
from mitmproxy import ctx


class McDonaldLoginCapture:
    """麦当劳登录流程抓包"""

    def __init__(self):
        self.request_count = 0
        self.log_file = f"mcd_login_capture_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.critical_requests = []

        # 关键词列表
        self.keywords = [
            'token', 'deviceId', 'deviceInfo',
            'login', 'passport', 'auth',
            'tel', 'phone', 'mobile',
            'code', 'sms', 'verify',
            'encrypt', 'crypto', 'sign',
            'cd56d8d91f6d92b6520686df3fbe32c8',  # 加密后的手机号
            '16752934813',  # 原始手机号
        ]

    def request(self, flow: http.HTTPFlow) -> None:
        """拦截请求"""
        self.request_count += 1
        request = flow.request

        # 只关注麦当劳的请求
        if 'mcd' not in request.pretty_host.lower():
            return

        # 提取关键信息
        url = request.pretty_url
        method = request.method
        headers = dict(request.headers)

        # 请求体
        body_text = ""
        if request.content:
            try:
                body_text = request.content.decode('utf-8')
            except:
                body_text = str(request.content)

        # 检查是否包含关键词
        is_critical = False
        matched_keywords = []

        search_text = f"{url} {str(headers)} {body_text}".lower()
        for keyword in self.keywords:
            if keyword.lower() in search_text:
                is_critical = True
                matched_keywords.append(keyword)

        # 记录请求
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'index': self.request_count,
            'method': method,
            'url': url,
            'headers': headers,
            'body': body_text,
            'is_critical': is_critical,
            'matched_keywords': matched_keywords
        }

        if is_critical:
            self.critical_requests.append(log_entry)

        # 输出到终端
        self._print_request(log_entry)

    def response(self, flow: http.HTTPFlow) -> None:
        """拦截响应"""
        request = flow.request
        response = flow.response

        # 只关注麦当劳的请求
        if 'mcd' not in request.pretty_host.lower():
            return

        # 提取响应信息
        status_code = response.status_code
        headers = dict(response.headers)

        # 响应体
        body_text = ""
        if response.content:
            try:
                body_text = response.content.decode('utf-8')
            except:
                body_text = str(response.content)

        # 检查响应中是否包含关键词
        is_critical = False
        matched_keywords = []

        search_text = f"{str(headers)} {body_text}".lower()
        for keyword in self.keywords:
            if keyword.lower() in search_text:
                is_critical = True
                matched_keywords.append(keyword)

        # 记录响应
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'index': self.request_count,
            'url': request.pretty_url,
            'status_code': status_code,
            'response_headers': headers,
            'response_body': body_text,
            'is_critical': is_critical,
            'matched_keywords': matched_keywords
        }

        if is_critical:
            self.critical_requests.append(log_entry)

        # 输出到终端
        self._print_response(log_entry)

        # 特殊处理：登录接口
        if 'login' in request.pretty_url.lower():
            self._analyze_login(request, response)

    def _print_request(self, entry):
        """打印请求到终端"""
        if entry['is_critical']:
            print("\n" + "="*80)
            print(f"🔍 [关键请求 #{entry['index']}] {entry['method']} {entry['url']}")
            print(f"⏰ 时间: {entry['timestamp']}")
            print(f"🏷️  匹配关键词: {', '.join(entry['matched_keywords'])}")
            print("-"*80)

            # 打印关键请求头
            critical_headers = ['token', 'authorization', 'x-hmac-digest', 'deviceid', 'mcdtoken']
            for key, value in entry['headers'].items():
                if any(h in key.lower() for h in critical_headers):
                    print(f"📋 {key}: {value}")

            # 打印请求体（如果有）
            if entry['body']:
                print(f"📦 Body: {entry['body'][:500]}")

            print("="*80)

    def _print_response(self, entry):
        """打印响应到终端"""
        if entry['is_critical']:
            print("\n" + "="*80)
            print(f"📥 [关键响应 #{entry['index']}] {entry['url']}")
            print(f"📊 状态码: {entry['status_code']}")
            print(f"🏷️  匹配关键词: {', '.join(entry['matched_keywords'])}")
            print("-"*80)

            # 打印响应体（限制长度）
            body = entry['response_body']
            if len(body) > 1000:
                print(f"📦 Response (前1000字符): {body[:1000]}")
            else:
                print(f"📦 Response: {body}")

            print("="*80)

    def _analyze_login(self, request, response):
        """分析登录请求"""
        print("\n" + "🔐"*40)
        print("🔐 登录接口详细分析")
        print("🔐"*40)

        # 分析请求
        print("\n【请求分析】")
        try:
            body = json.loads(request.content.decode('utf-8'))
            print(f"📱 Tel (加密): {body.get('tel', 'N/A')}")
            print(f"🔢 Code (加密): {body.get('code', 'N/A')}")
            print(f"📲 DeviceInfoId: {body.get('deviceInfoId', 'N/A')}")
            print(f"🌍 RegionCode: {body.get('regionCode', 'N/A')}")
        except:
            print("⚠️  无法解析请求体")

        # 分析响应
        print("\n【响应分析】")
        try:
            resp_body = json.loads(response.content.decode('utf-8'))
            if resp_body.get('success'):
                data = resp_body.get('data', {})
                print(f"✅ 登录成功")
                print(f"🆔 SID: {data.get('sid', 'N/A')}")
                print(f"👤 MeddyId: {data.get('meddyId', 'N/A')}")
                print(f"👶 NewUser: {data.get('newUser', 'N/A')}")
            else:
                print(f"❌ 登录失败: {resp_body.get('message', 'N/A')}")
        except:
            print("⚠️  无法解析响应体")

        print("🔐"*40 + "\n")

    def done(self):
        """保存日志到文件"""
        print(f"\n正在保存日志到: {self.log_file}")

        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write("="*100 + "\n")
            f.write("麦当劳 APP 登录流程完整抓包日志\n")
            f.write(f"生成时间: {datetime.now().isoformat()}\n")
            f.write(f"总请求数: {self.request_count}\n")
            f.write(f"关键请求数: {len(self.critical_requests)}\n")
            f.write("="*100 + "\n\n")

            for entry in self.critical_requests:
                f.write("\n" + "="*100 + "\n")
                f.write(json.dumps(entry, ensure_ascii=False, indent=2))
                f.write("\n" + "="*100 + "\n")

        print(f"✅ 日志已保存: {self.log_file}")
        print(f"📊 捕获了 {len(self.critical_requests)} 个关键请求")


# mitmdump 入口
addons = [McDonaldLoginCapture()]

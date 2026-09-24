#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳登录管理类
支持多账号管理、自动状态检查、凭证持久化
"""

import json
import os
import requests
from datetime import datetime
from typing import Optional, Tuple, Dict

from mcd_api import (
    generate_token,
    activate_token,
    send_verification_code,
    login,
    build_headers,
    API2_BASE
)


class LoginManager:
    """
    麦当劳登录管理器

    功能：
    - 多账号凭证管理
    - 登录状态验证
    - 自动登录流程
    - 凭证持久化

    使用示例：
        manager = LoginManager(phone="16752934813")
        token, sid, meddy_id = manager.ensure_login()
    """

    def __init__(self, phone: str, credentials_file: str = None):
        """
        初始化登录管理器

        Args:
            phone: 手机号（11位）
            credentials_file: 凭证保存文件路径（默认：当前目录下的 credentials.json）
        """
        if len(phone) != 11:
            raise ValueError("手机号必须是11位数字")

        self.phone = phone

        # 凭证文件路径
        if credentials_file is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            credentials_file = os.path.join(current_dir, 'credentials.json')

        self.credentials_file = credentials_file

        # 当前账号凭证
        self.token: Optional[str] = None
        self.sid: Optional[str] = None
        self.meddy_id: Optional[str] = None


    def save_credentials(self, token: str, sid: str, meddy_id: str) -> None:
        """
        保存登录凭证到文件（支持多账号）

        Args:
            token: 设备 Token
            sid: 会话 SID
            meddy_id: MeddyId
        """
        # 读取现有凭证
        all_credentials = {}
        if os.path.exists(self.credentials_file):
            try:
                with open(self.credentials_file, 'r', encoding='utf-8') as f:
                    all_credentials = json.load(f)
            except:
                all_credentials = {}

        # 更新当前手机号的凭证
        all_credentials[self.phone] = {
            'token': token,
            'sid': sid,
            'meddy_id': meddy_id,
            'saved_at': datetime.now().isoformat()
        }

        # 保存到文件
        with open(self.credentials_file, 'w', encoding='utf-8') as f:
            json.dump(all_credentials, f, indent=2, ensure_ascii=False)

        # 更新内存中的凭证
        self.token = token
        self.sid = sid
        self.meddy_id = meddy_id

        print(f"✓ 凭证已保存 (手机号: {self.phone})")


    def load_credentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        从文件加载指定手机号的登录凭证

        Returns:
            (token, sid, meddy_id) 或 (None, None, None)
        """
        if not os.path.exists(self.credentials_file):
            return None, None, None

        try:
            with open(self.credentials_file, 'r', encoding='utf-8') as f:
                all_credentials = json.load(f)

            # 获取当前手机号的凭证
            cred = all_credentials.get(self.phone)
            if not cred:
                return None, None, None

            token = cred.get('token')
            sid = cred.get('sid')
            meddy_id = cred.get('meddy_id')
            saved_at = cred.get('saved_at')

            # 更新内存中的凭证
            self.token = token
            self.sid = sid
            self.meddy_id = meddy_id

            print(f"✓ 找到已保存的凭证 (手机号: {self.phone}, 保存时间: {saved_at})")
            return token, sid, meddy_id

        except Exception as e:
            print(f"✗ 加载凭证失败: {e}")
            return None, None, None


    def check_login_status(self) -> Tuple[bool, Optional[Dict]]:
        """
        检查当前登录状态是否有效

        使用获取账号信息接口测试: GET /bff/member/user/portal/info?scene=1

        Returns:
            (is_valid, user_info)
        """
        if not self.token or not self.sid:
            return False, None

        try:
            path = '/bff/member/user/portal/info'
            params = {'scene': '1'}

            # 构建请求头
            headers = build_headers(
                self.token,
                sid=self.sid,
                body=None,
                method='GET',
                path=path,
                query_params=params
            )
            headers['Host'] = 'api2.mcd.cn'

            # 发送请求
            url = API2_BASE + path
            response = requests.get(url, headers=headers, params=params, timeout=10)
            result = response.json()

            if result.get('success'):
                data = result.get('data', {})
                user_info = {
                    'phone': data.get('phoneNumber', ''),
                    'name': data.get('fullName', ''),
                    'points': data.get('availablePoints', '0'),
                    'meddy_id': data.get('meddyId', ''),
                    'identity': data.get('identityName', '')
                }
                return True, user_info
            else:
                print(f"  API 返回: {result.get('message', '未知错误')}")
                return False, None

        except Exception as e:
            print(f"✗ 检查登录状态异常: {e}")
            return False, None


    def do_login(self, verify_code: str) -> Tuple[bool, str]:
        """
        执行登录流程（需要先调用 send_verify_code）

        Args:
            verify_code: 6位短信验证码

        Returns:
            (success, message)
        """
        if len(verify_code) != 6:
            return False, "验证码必须是6位数字"

        if not self.token:
            return False, "请先调用 send_verify_code 获取 token"

        print(f"\n正在登录 (手机号: {self.phone})...")
        success, sid, meddy_id, message = login(self.phone, verify_code, self.token)

        if success:
            # 保存凭证
            self.save_credentials(self.token, sid, meddy_id)
            return True, "登录成功"
        else:
            return False, message


    def send_verify_code(self) -> Tuple[bool, str]:
        """
        发送验证码（会自动生成并激活 token）

        Returns:
            (success, message)
        """
        print(f"\n发送验证码到 {self.phone}...")

        # 1. 生成 Token
        print("  [1/3] 生成 Token...")
        self.token = generate_token()

        # 2. 激活 Token
        print("  [2/3] 激活 Token...")
        success, tid, message = activate_token(self.token)
        if not success:
            return False, f"Token 激活失败: {message}"

        # 3. 发送验证码
        print("  [3/3] 发送验证码...")
        success, message = send_verification_code(self.phone, self.token)
        if not success:
            return False, f"发送验证码失败: {message}"

        return True, "验证码已发送"


    def ensure_login(self, auto_relogin: bool = False) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        确保已登录状态

        1. 尝试加载已保存的凭证
        2. 验证凭证是否有效
        3. 如果无效且 auto_relogin=True，自动重新登录

        Args:
            auto_relogin: 如果凭证失效，是否自动重新登录（需要手动输入验证码）

        Returns:
            (token, sid, meddy_id) 或 (None, None, None)
        """
        print("=" * 80)
        print(f"检查登录状态 (手机号: {self.phone})")
        print("=" * 80)

        # 1. 尝试加载已保存的凭证
        token, sid, meddy_id = self.load_credentials()

        if token and sid:
            # 2. 验证凭证是否有效
            print("\n验证登录状态...")
            is_valid, user_info = self.check_login_status()

            if is_valid:
                print("\n✅ 登录状态有效")
                print("-" * 80)
                print(f"手机号: {user_info['phone']}")
                print(f"昵称: {user_info['name']}")
                print(f"积分: {user_info['points']}")
                print(f"身份: {user_info['identity']}")
                print(f"MeddyId: {user_info['meddy_id']}")
                print("-" * 80)
                return self.token, self.sid, self.meddy_id
            else:
                print("\n✗ 登录状态已失效")
                if not auto_relogin:
                    print("提示: 调用 send_verify_code() 和 do_login() 重新登录")
                    return None, None, None
        else:
            print("\n✗ 未找到已保存的凭证")
            if not auto_relogin:
                print("提示: 调用 send_verify_code() 和 do_login() 进行登录")
                return None, None, None

        # 3. 自动重新登录
        if auto_relogin:
            print("\n" + "=" * 80)
            print("开始重新登录流程")
            print("=" * 80)

            # 发送验证码
            success, message = self.send_verify_code()
            if not success:
                print(f"\n❌ {message}")
                return None, None, None

            print(f"\n✅ {message}")

            # 等待用户输入验证码
            verify_code = input("\n请输入6位验证码: ").strip()

            # 执行登录
            success, message = self.do_login(verify_code)
            if not success:
                print(f"\n❌ {message}")
                return None, None, None

            print(f"\n✅ {message}")
            return self.token, self.sid, self.meddy_id

        return None, None, None


    def get_credentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        获取当前内存中的凭证

        Returns:
            (token, sid, meddy_id)
        """
        return self.token, self.sid, self.meddy_id


    def clear_credentials(self) -> None:
        """
        清除当前手机号的凭证（从文件和内存中删除）
        """
        # 清除内存
        self.token = None
        self.sid = None
        self.meddy_id = None

        # 从文件中删除
        if os.path.exists(self.credentials_file):
            try:
                with open(self.credentials_file, 'r', encoding='utf-8') as f:
                    all_credentials = json.load(f)

                if self.phone in all_credentials:
                    del all_credentials[self.phone]

                    with open(self.credentials_file, 'w', encoding='utf-8') as f:
                        json.dump(all_credentials, f, indent=2, ensure_ascii=False)

                    print(f"✓ 已清除凭证 (手机号: {self.phone})")
            except Exception as e:
                print(f"✗ 清除凭证失败: {e}")

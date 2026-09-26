#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳登录管理类
支持多账号管理、自动状态检查、凭证持久化、网络异常重试
"""

import json
import os
import requests
import time
import tempfile
import shutil
from datetime import datetime
from typing import Optional, Tuple, Dict
from functools import wraps
from requests.exceptions import ConnectionError, Timeout, RequestException

from config import DATA_DIR
from mcd_api import (
    generate_token,
    activate_token,
    send_verification_code,
    login,
    build_headers,
    API2_BASE
)
from exceptions import (
    LoginError,
    NetworkError,
    AuthenticationError,
    ServerError,
    ValidationError
)


def retry_on_network_error(max_attempts=3, base_delay=1.0, backoff=2.0):
    """
    网络请求重试装饰器

    Args:
        max_attempts: 最大重试次数
        base_delay: 基础延迟（秒）
        backoff: 指数退避系数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)

                except (ConnectionError, Timeout) as e:
                    # 网络连接问题，可以重试
                    last_error = NetworkError(
                        f"网络连接失败: {str(e)}",
                        original_error=e,
                        retry_after=base_delay * (backoff ** attempt)
                    )

                    if attempt < max_attempts - 1:
                        delay = base_delay * (backoff ** attempt)
                        print(f"  ⚠️  网络请求失败，{delay:.1f}秒后重试... ({attempt + 1}/{max_attempts})")
                        time.sleep(delay)

                except requests.HTTPError as e:
                    # HTTP 错误，根据状态码判断
                    status_code = e.response.status_code if e.response else None

                    if status_code in [401, 403]:
                        # 认证失败，不重试
                        raise AuthenticationError(f"认证失败 (HTTP {status_code})")

                    elif status_code in [500, 502, 503, 504]:
                        # 服务器错误，可以重试
                        last_error = ServerError(
                            f"服务器错误 (HTTP {status_code})",
                            status_code=status_code,
                            retry_after=base_delay * (backoff ** attempt)
                        )

                        if attempt < max_attempts - 1:
                            delay = base_delay * (backoff ** attempt)
                            print(f"  ⚠️  服务器错误，{delay:.1f}秒后重试... ({attempt + 1}/{max_attempts})")
                            time.sleep(delay)
                    else:
                        # 其他 HTTP 错误，不重试
                        raise LoginError(f"请求失败 (HTTP {status_code}): {e}")

                except RequestException as e:
                    # 其他请求异常
                    last_error = NetworkError(f"请求异常: {str(e)}", original_error=e)

                    if attempt < max_attempts - 1:
                        delay = base_delay * (backoff ** attempt)
                        print(f"  ⚠️  请求异常，{delay:.1f}秒后重试... ({attempt + 1}/{max_attempts})")
                        time.sleep(delay)

            # 所有重试都失败
            raise last_error

        return wrapper
    return decorator


class LoginManager:
    """
    麦当劳登录管理器

    功能：
    - 多账号凭证管理
    - 登录状态验证（带网络重试）
    - 自动登录流程
    - 凭证持久化（原子写入）
    - 智能错误处理（区分网络问题和凭证失效）

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
            credentials_file = os.path.join(DATA_DIR, 'credentials.json')

        self.credentials_file = credentials_file

        # 当前账号凭证
        self.token: Optional[str] = None
        self.sid: Optional[str] = None
        self.meddy_id: Optional[str] = None

    def save_credentials(self, token: str, sid: str, meddy_id: str) -> None:
        """
        保存登录凭证到文件（原子写入，支持多账号）

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
            except Exception as e:
                print(f"⚠️  警告: 读取旧凭证失败: {e}")
                # 继续执行，使用空字典

        # 更新当前手机号的凭证
        all_credentials[self.phone] = {
            'token': token,
            'sid': sid,
            'meddy_id': meddy_id,
            'saved_at': datetime.now().isoformat()
        }

        # 原子写入（先写临时文件，再重命名）
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.credentials_file), exist_ok=True)

            # 写入临时文件
            with tempfile.NamedTemporaryFile(
                mode='w',
                encoding='utf-8',
                dir=os.path.dirname(self.credentials_file),
                delete=False
            ) as tmp_file:
                json.dump(all_credentials, tmp_file, indent=2, ensure_ascii=False)
                tmp_path = tmp_file.name

            # 原子重命名（POSIX 系统保证原子性）
            shutil.move(tmp_path, self.credentials_file)

        except Exception as e:
            # 清理临时文件
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise LoginError(f"保存凭证失败: {e}")

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

    @retry_on_network_error(max_attempts=3, base_delay=1.0)
    def check_login_status(self) -> Tuple[bool, Optional[Dict]]:
        """
        检查当前登录状态是否有效（带网络重试）

        使用获取账号信息接口测试: GET /bff/member/user/portal/info?scene=1

        Returns:
            (is_valid, user_info)

        Raises:
            NetworkError: 网络连接问题（已重试失败）
            AuthenticationError: 认证失败（凭证失效）
            ServerError: 服务器错误（已重试失败）
        """
        if not self.token or not self.sid:
            return False, None

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

        # 发送请求（会被装饰器自动重试）
        url = API2_BASE + path
        response = requests.get(url, headers=headers, params=params, timeout=10)

        # 检查 HTTP 状态码
        response.raise_for_status()  # 会抛出 HTTPError

        # 解析响应
        try:
            result = response.json()
        except ValueError as e:
            raise LoginError(f"响应格式错误: {e}")

        # 检查业务状态
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
            # 业务失败，判断是否为认证问题
            error_code = result.get('code')
            error_msg = result.get('message', '未知错误')

            # 常见的认证失败错误码（根据实际 API 调整）
            auth_error_codes = ['AUTH_FAILED', 'TOKEN_EXPIRED', 'INVALID_SESSION', 'UNAUTHORIZED']

            if error_code in auth_error_codes or 'token' in error_msg.lower() or 'auth' in error_msg.lower():
                raise AuthenticationError(f"认证失败: {error_msg} (code: {error_code})")
            else:
                # 其他业务错误
                raise LoginError(f"业务错误: {error_msg} (code: {error_code})")

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

    def send_verify_code(self, force_new_device: bool = False) -> Tuple[bool, str]:
        """
        发送验证码（会自动生成并激活 token）

        Args:
            force_new_device: 是否强制生成新设备（默认 False，优先使用已保存的 token）

        Returns:
            (success, message)
        """
        print(f"\n发送验证码到 {self.phone}...")

        # 1. 尝试使用已保存的 token（如果存在且未强制生成新设备）
        if not force_new_device and self.token:
            print("  [1/2] 使用已保存的设备 Token（跳过激活）...")
            # 使用已有 token，无需激活
        else:
            print("  [1/2] 生成新设备 Token...")
            self.token = generate_token()

            # 2. 激活新生成的 Token
            print("  激活 Token...")
            success, tid, message = activate_token(self.token)
            if not success:
                return False, f"Token 激活失败: {message}"

        # 3. 发送验证码
        print("  [2/2] 发送验证码...")
        success, message = send_verification_code(self.phone, self.token)
        if not success:
            return False, f"发送验证码失败: {message}"

        return True, "验证码已发送"

    def ensure_login(self, auto_relogin: bool = False, force_new_device: bool = False) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        确保已登录状态（智能区分网络问题和凭证失效）

        1. 尝试加载已保存的凭证
        2. 验证凭证是否有效（带网络重试）
        3. 如果无效且 auto_relogin=True，自动重新登录

        Args:
            auto_relogin: 如果凭证失效，是否自动重新登录（需要手动输入验证码）
            force_new_device: 重新登录时是否强制生成新设备（默认 False，使用原设备信息）

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

            try:
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

            except AuthenticationError as e:
                # 凭证确实失效
                print(f"\n✗ 登录状态已失效: {e}")
                if not auto_relogin:
                    print("提示: 调用 send_verify_code() 和 do_login() 重新登录")
                    return None, None, None

            except (NetworkError, ServerError) as e:
                # 网络问题或服务器错误
                print(f"\n⚠️  无法验证登录状态: {e}")
                print("提示: 网络连接问题，请检查网络后重试")

                # 网络问题时，不清空凭证，返回现有凭证
                # （后续请求可能会成功）
                return self.token, self.sid, self.meddy_id

            except Exception as e:
                # 未预期的错误
                print(f"\n❌ 验证登录状态时发生未知错误: {e}")
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

            try:
                # 发送验证码（新账号或强制新设备时生成新token，否则使用原token）
                success, message = self.send_verify_code(force_new_device=force_new_device)
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

            except (NetworkError, ServerError) as e:
                print(f"\n❌ 登录失败: {e}")
                print("提示: 网络连接问题，请稍后重试")
                return None, None, None

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


if __name__ == '__main__':
    LoginManager('16752934813').ensure_login(True)

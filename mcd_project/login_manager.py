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
import redis
from datetime import datetime
from typing import Optional, Tuple, Dict
from functools import wraps
from requests.exceptions import ConnectionError, Timeout, RequestException

from config import DATA_DIR, REDIS_SET_URL, PROXY_REQUEST_TIMEOUT, USE_PROXY, make_redis
from mcd_api import (
    generate_token,
    activate_token,
    send_verification_code,
    login,
    build_headers,
    set_active_d,
    set_active_tid,
    API2_BASE
)
from exceptions import (
    LoginError,
    NetworkError,
    AuthenticationError,
    ServerError,
    ValidationError
)

# 导入代理管理模块
from proxy_pool import ProxyPool
from account_proxy_binding import AccountProxyBinding
from proxy_failure_handler import ProxyFailureHandler
from utils import log_info


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
                        log_info(f"  ⚠️  网络请求失败，{delay:.1f}秒后重试... ({attempt + 1}/{max_attempts})")
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
                            log_info(f"  ⚠️  服务器错误，{delay:.1f}秒后重试... ({attempt + 1}/{max_attempts})")
                            time.sleep(delay)
                    else:
                        # 其他 HTTP 错误，不重试
                        raise LoginError(f"请求失败 (HTTP {status_code}): {e}")

                except RequestException as e:
                    # 其他请求异常
                    last_error = NetworkError(f"请求异常: {str(e)}", original_error=e)

                    if attempt < max_attempts - 1:
                        delay = base_delay * (backoff ** attempt)
                        log_info(f"  ⚠️  请求异常，{delay:.1f}秒后重试... ({attempt + 1}/{max_attempts})")
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

    def __init__(self, phone: str, credentials_file: str = None, use_redis: bool = True, city: str = None,
                 proxy_pool=None, binding_manager=None, failure_handler=None):
        """
        初始化登录管理器

        Args:
            phone: 手机号（11位）
            credentials_file: 凭证保存文件路径（默认：当前目录下的 credentials.json）
            use_redis: 是否使用 Redis 存储（默认 True）
            city: 指定城市（可选），如果指定则优先分配该城市的代理
            proxy_pool: ProxyPool 实例（可选，不传则自动创建）
            binding_manager: AccountProxyBinding 实例（可选，不传则自动创建）
            failure_handler: ProxyFailureHandler 实例（可选，不传则自动创建）
        """
        if len(phone) != 11:
            raise ValueError("手机号必须是11位数字")

        self.phone = phone
        self.use_redis = use_redis
        self.city = city

        # 凭证文件路径
        if credentials_file is None:
            credentials_file = os.path.join(DATA_DIR, 'credentials.json')

        self.credentials_file = credentials_file

        # Redis 客户端（如果使用 Redis）
        if self.use_redis:
            self.redis_client = make_redis()  # 带超时/重试/keepalive
            self.redis_key = 'mcd:credentials'

        # 当前账号凭证
        self.token: Optional[str] = None
        self.sid: Optional[str] = None
        self.meddy_id: Optional[str] = None
        self.d: Optional[str] = None  # 数美设备指纹(设备池自动领取/set_device 写入)，请求头 d
        self.device_id: Optional[str] = None  # 设备池中的设备唯一 id(绑定来源,便于失效换设备)
        self.tid: Optional[str] = None          # Token 激活标识(7天有效,ensure_tid 刷新)
        self.tid_expire: Optional[float] = None  # tid 过期时间戳(秒)

        # ====== 新增：代理管理 ======
        # 使用传入的实例或创建新实例
        if proxy_pool is not None:
            self.proxy_pool = proxy_pool
        else:
            from proxy_pool import ProxyPool
            self.proxy_pool = ProxyPool()

        if binding_manager is not None:
            self.binding_manager = binding_manager
        else:
            from account_proxy_binding import AccountProxyBinding
            self.binding_manager = AccountProxyBinding()

        if failure_handler is not None:
            self.failure_handler = failure_handler
        else:
            from proxy_failure_handler import ProxyFailureHandler
            self.failure_handler = ProxyFailureHandler()

        # 为当前账号分配代理
        self._assign_proxy()

        # 自动加载已保存的凭证（如果存在）
        self.load_credentials()

    def _assign_proxy(self) -> None:
        """为当前账号分配代理"""
        proxy_info = self.binding_manager.get_or_assign_proxy(self.phone, city=self.city)
        self.current_proxy_id = proxy_info['id']

    def _get_proxy_dict(self) -> Dict[str, str]:
        """
        获取当前代理的 requests proxies 字典

        Returns:
            {'http': 'http://user:pass@host:port', 'https': '...'}
        """
        # 全局开关：不使用代理时直连
        if not USE_PROXY:
            return {}

        # 获取当前应该使用的代理（可能是主代理或临时代理）
        binding = self.binding_manager.get_binding(self.phone)

        if not binding:
            return {}

        # 如果在观察期，使用临时代理
        if binding.get('in_observation') and binding.get('temp_proxy_id'):
            proxy_id = binding['temp_proxy_id']
        else:
            proxy_id = binding['proxy_id']

        # 从代理池获取代理信息
        proxy = self.proxy_pool.get_proxy(proxy_id)

        if not proxy:
            return {}

        # 构建代理 URL
        if proxy.get('username') and proxy.get('password'):
            proxy_url = f"{proxy['protocol']}://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
        else:
            proxy_url = f"{proxy['protocol']}://{proxy['host']}:{proxy['port']}"

        return {
            'http': proxy_url,
            'https': proxy_url
        }

    def _make_request_with_proxy(
        self,
        method: str,
        url: str,
        max_retries: int = 3,
        **kwargs
    ) -> requests.Response:
        """
        发送请求（集成代理和失败处理）

        Args:
            method: HTTP 方法（GET/POST）
            url: 请求 URL
            max_retries: 最大重试次数
            **kwargs: requests 参数

        Returns:
            Response 对象

        Raises:
            NetworkError: 网络连接问题
        """
        last_error = None
        retry_count = 0

        while retry_count < max_retries:
            try:
                # 获取当前代理
                proxies = self._get_proxy_dict()

                # 设置超时
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = PROXY_REQUEST_TIMEOUT

                # 发送请求
                if method.upper() == 'GET':
                    response = requests.get(url, proxies=proxies, **kwargs)
                elif method.upper() == 'POST':
                    response = requests.post(url, proxies=proxies, **kwargs)
                else:
                    raise ValueError(f"不支持的 HTTP 方法: {method}")

                # 请求成功，标记代理成功
                binding = self.binding_manager.get_binding(self.phone)
                current_proxy_id = binding['temp_proxy_id'] if binding.get('in_observation') else binding['proxy_id']
                self.failure_handler.handle_request_success(self.phone, current_proxy_id)

                return response

            except (ConnectionError, Timeout, RequestException) as e:
                # 请求失败，使用失败处理器
                binding = self.binding_manager.get_binding(self.phone)
                current_proxy_id = binding['temp_proxy_id'] if binding.get('in_observation') else binding['proxy_id']

                should_retry, new_proxy_id, action, msg = self.failure_handler.handle_request_failure(
                    self.phone,
                    current_proxy_id,
                    str(e)
                )

                last_error = e

                if not should_retry:
                    raise NetworkError(f"网络连接失败: {str(e)}")

                retry_count += 1

                # 如果切换了代理，重置重试计数
                if action in ["temp_switch", "permanent_switch", "permanent_switch_immediate"]:
                    retry_count = 0

        # 所有重试都失败
        raise NetworkError(f"请求失败（已重试{max_retries}次）: {str(last_error)}")

    def ensure_device(self, force_new: bool = False) -> Optional[str]:
        """确保账号已绑定数美设备指纹 d：没有(或 force_new)就从设备池领一个并写入凭证。
        - 新账号：首次调用即领一个设备
        - 已有账号但无 d：自动补领并更新到登录信息
        - force_new=True：换新设备(旧设备标记失效)，对接 send_code 的 use_new_device
        返回 d；设备池空或不可用时返回当前 self.d(可能为 None)。"""
        if self.d and self.token and not force_new:
            return self.d
        try:
            from device_pool_redis import claim, release
        except Exception as e:
            log_info(f"✗ 设备池不可用，无法为 {self.phone} 分配设备: {e}")
            return self.d
        old_device_id = self.device_id
        dev = claim(self.phone)
        if not dev:
            log_info(f"✗ 设备池无空闲设备，账号 {self.phone} 暂未分配 d（请先 refill 补池）")
            return self.d
        if force_new and old_device_id:          # 换设备：旧的标记失效,不回收
            try:
                release(old_device_id)
            except Exception:
                pass
        self.d = dev['d']
        self.token = dev.get('token') or self.token   # 用设备池固定 token(与 d 成一套)
        self.device_id = dev['id']
        set_active_d(self.d)
        self._persist_device(dev['d'], dev['id'])
        log_info(f"✓ 账号 {self.phone} 绑定设备 {dev['id']} (model={dev.get('model')}) token={str(self.token)[:8]}… d={dev['d'][:16]}…")
        return self.d

    def _update_credential(self, fields: dict) -> None:
        """把若干字段合并进凭证(保留其它字段)；token 以当前 self.token 为准(设备池固定 token)。"""
        now = datetime.now().isoformat()
        if self.use_redis:
            try:
                old = self.redis_client.hget(self.redis_key, self.phone)
                cred = json.loads(old) if old else {}
            except Exception:
                cred = {}
            cred.update(fields)
            if self.token:
                cred['token'] = self.token
            cred.setdefault('sid', self.sid)
            cred.setdefault('meddy_id', self.meddy_id)
            cred['saved_at'] = now
            try:
                self.redis_client.hset(self.redis_key, self.phone, json.dumps(cred, ensure_ascii=False))
            except Exception as e:
                log_info(f"✗ 写入凭证失败: {e}")
        else:
            all_c = {}
            if os.path.exists(self.credentials_file):
                try:
                    with open(self.credentials_file, 'r', encoding='utf-8') as f:
                        all_c = json.load(f)
                except Exception:
                    all_c = {}
            cred = all_c.get(self.phone, {})
            cred.update(fields)
            if self.token:
                cred['token'] = self.token
            cred.setdefault('sid', self.sid)
            cred.setdefault('meddy_id', self.meddy_id)
            cred['saved_at'] = now
            all_c[self.phone] = cred
            try:
                os.makedirs(os.path.dirname(self.credentials_file), exist_ok=True)
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                                 dir=os.path.dirname(self.credentials_file), delete=False) as tf:
                    json.dump(all_c, tf, indent=2, ensure_ascii=False)
                    tmp = tf.name
                shutil.move(tmp, self.credentials_file)
            except Exception as e:
                log_info(f"✗ 写入凭证失败: {e}")

    def _persist_device(self, d: str, device_id: str) -> None:
        """把 d/device_id/token(设备池固定 token) 写入凭证。"""
        self._update_credential({'d': d, 'device_id': device_id})

    def _persist_tid(self, tid: str, tid_expire: float) -> None:
        """把 tid/tid_expire 写入凭证。"""
        self._update_credential({'tid': tid, 'tid_expire': tid_expire})

    def ensure_tid(self, force: bool = False) -> Optional[str]:
        """确保 tid 有效(7天)。过期或 force 时用设备 token 重新激活(activate_token /bff/common/proxy/tid)。
        tid 是设备级、7天有效的会话凭证——在账号用设备时激活/刷新,不在造设备时。返回 tid。"""
        import time as _t
        now = _t.time()
        if not force and self.tid and self.tid_expire and now < float(self.tid_expire):
            set_active_tid(self.tid)
            return self.tid                        # 7天内复用
        if not self.token:
            self.ensure_device()                   # 没 token 先领设备(拿固定 token)
        if not self.token:
            log_info(f"✗ 账号 {self.phone} 无 token，无法激活 tid")
            return self.tid
        try:
            proxies = self._get_proxy_dict()
            success, tid, message = activate_token(self.token, proxies=proxies)
        except Exception as e:
            log_info(f"✗ tid 激活异常: {e}")
            return self.tid
        if success and tid:
            self.tid = tid
            self.tid_expire = now + 7 * 86400      # 与真机 TidManager 一致:7天有效
            set_active_tid(self.tid)
            self._persist_tid(self.tid, self.tid_expire)
            log_info(f"✓ 账号 {self.phone} 激活 tid={str(tid)[:12]}… (7天有效)")
        else:
            log_info(f"✗ tid 激活失败: {message}")
        return self.tid

    def save_credentials(self, token: str, sid: str, meddy_id: str) -> None:
        """
        保存登录凭证到文件或 Redis（原子写入，支持多账号）

        Args:
            token: 设备 Token
            sid: 会话 SID
            meddy_id: MeddyId
        """
        if self.use_redis:
            # 保留已存的 d（数美设备指纹，由 set_device 写入，登录保存时别弄丢）
            d_keep = self.d
            if d_keep is None:
                try:
                    _old = self.redis_client.hget(self.redis_key, self.phone)
                    if _old:
                        d_keep = json.loads(_old).get('d')
                except Exception:
                    pass
            # Redis 存储
            credential_data = {
                'token': token,
                'sid': sid,
                'meddy_id': meddy_id,
                'd': d_keep,
                'device_id': self.device_id,
                'tid': self.tid,
                'tid_expire': self.tid_expire,
                'saved_at': datetime.now().isoformat()
            }
            try:
                self.redis_client.hset(
                    self.redis_key,
                    self.phone,
                    json.dumps(credential_data, ensure_ascii=False)
                )
            except Exception as e:
                raise LoginError(f"保存凭证失败: {e}")
        else:
            # 文件存储
            # 读取现有凭证
            all_credentials = {}
            if os.path.exists(self.credentials_file):
                try:
                    with open(self.credentials_file, 'r', encoding='utf-8') as f:
                        all_credentials = json.load(f)
                except Exception as e:
                    log_info(f"⚠️  警告: 读取旧凭证失败: {e}")
                    # 继续执行，使用空字典

            # 更新当前手机号的凭证（保留已存的 d）
            d_keep = self.d if self.d is not None else all_credentials.get(self.phone, {}).get('d')
            all_credentials[self.phone] = {
                'token': token,
                'sid': sid,
                'meddy_id': meddy_id,
                'd': d_keep,
                'device_id': self.device_id,
                'tid': self.tid,
                'tid_expire': self.tid_expire,
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
        self.d = d_keep
        set_active_d(self.d)  # 设为当前请求使用的 d

        log_info(f"✓ 凭证已保存 (手机号: {self.phone})")

    def load_credentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        从文件或 Redis 加载指定手机号的登录凭证

        Returns:
            (token, sid, meddy_id) 或 (None, None, None)
        """
        self.d = None
        self.tid = None
        self.tid_expire = None
        set_active_d('')  # 先重置，避免同 worker 上个账号的 d 残留
        set_active_tid('')
        if self.use_redis:
            # Redis 存储
            try:
                credential_json = self.redis_client.hget(self.redis_key, self.phone)
                if not credential_json:
                    return None, None, None

                cred = json.loads(credential_json)
                token = cred.get('token')
                sid = cred.get('sid')
                meddy_id = cred.get('meddy_id')
                saved_at = cred.get('saved_at')

                # 更新内存中的凭证
                self.token = token
                self.sid = sid
                self.meddy_id = meddy_id
                self.d = cred.get('d')
                self.device_id = cred.get('device_id')
                self.tid = cred.get('tid')
                self.tid_expire = cred.get('tid_expire')
                set_active_d(self.d)  # 设为当前请求使用的 d
                set_active_tid(self.tid)

                log_info(f"✓ 找到已保存的凭证 (手机号: {self.phone}, 保存时间: {saved_at})")
                return token, sid, meddy_id

            except Exception as e:
                log_info(f"✗ 加载凭证失败: {e}")
                return None, None, None
        else:
            # 文件存储
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
                self.d = cred.get('d')
                self.device_id = cred.get('device_id')
                self.tid = cred.get('tid')
                self.tid_expire = cred.get('tid_expire')
                set_active_d(self.d)  # 设为当前请求使用的 d
                set_active_tid(self.tid)

                log_info(f"✓ 找到已保存的凭证 (手机号: {self.phone}, 保存时间: {saved_at})")
                return token, sid, meddy_id

            except Exception as e:
                log_info(f"✗ 加载凭证失败: {e}")
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

        # 发送请求（使用代理和失败处理）
        url = API2_BASE + path
        response = self._make_request_with_proxy('GET', url, headers=headers, params=params)
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
            error_code = str(result.get('code'))
            error_msg = result.get('message', '未知错误')

            # 常见的认证失败错误码（根据实际 API 调整       ）
            auth_error_codes = ['401']

            if error_code in auth_error_codes or '登录失效' in error_msg.lower() or '重新登录' in error_msg.lower:
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

        log_info(f"\n正在登录 (手机号: {self.phone})...")
        proxies = self._get_proxy_dict()
        success, sid, meddy_id, message = login(self.phone, verify_code, self.token, proxies=proxies)

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
        log_info(f"\n发送验证码到 {self.phone}...")
        if not self.token:
            self.load_credentials()

        # 1. 绑定设备：token/d 来自设备池(一设备一套固定值)。force_new_device=True 换新设备。
        log_info("  [1/3] 绑定设备(设备池固定 token+d)...")
        self.ensure_device(force_new=force_new_device)
        if not self.token:
            return False, "无可用设备(设备池空)，请先 refill 补池"

        # 2. 激活 tid：7天有效,过期/换设备自动刷新(用时激活,不在造设备时)
        log_info("  [2/3] 激活 tid(7天有效,自动复用/刷新)...")
        self.ensure_tid(force=force_new_device)

        # 3. 发送验证码
        log_info("  [3/3] 发送验证码...")
        proxies = self._get_proxy_dict()
        success, message = send_verification_code(self.phone, self.token, proxies=proxies)
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
        log_info("=" * 80)
        log_info(f"检查登录状态 (手机号: {self.phone})")
        log_info("=" * 80)

        # 1. 尝试加载已保存的凭证
        token, sid, meddy_id = self.load_credentials()

        if token and sid:
            # 2. 验证凭证是否有效
            log_info("\n验证登录状态...")

            try:
                is_valid, user_info = self.check_login_status()

                if is_valid:
                    log_info("\n✅ 登录状态有效")
                    log_info("-" * 80)
                    log_info(f"手机号: {user_info['phone']}")
                    log_info(f"昵称: {user_info['name']}")
                    log_info(f"积分: {user_info['points']}")
                    log_info(f"身份: {user_info['identity']}")
                    log_info(f"MeddyId: {user_info['meddy_id']}")
                    log_info("-" * 80)
                    return self.token, self.sid, self.meddy_id

            except AuthenticationError as e:
                # 凭证确实失效
                log_info(f"\n✗ 登录状态已失效: {e}")
                if not auto_relogin:
                    log_info("提示: 调用 send_verify_code() 和 do_login() 重新登录")
                    return None, None, None

            except (NetworkError, ServerError) as e:
                # 网络问题或服务器错误
                log_info(f"\n⚠️  无法验证登录状态: {e}")
                log_info("提示: 网络连接问题，请检查网络后重试")

                # 网络问题时，不清空凭证，返回现有凭证
                # （后续请求可能会成功）
                return self.token, self.sid, self.meddy_id

            except Exception as e:
                # 未预期的错误
                log_info(f"\n❌ 验证登录状态时发生未知错误: {e}")
                return None, None, None

        else:
            log_info("\n✗ 未找到已保存的凭证")
            if not auto_relogin:
                log_info("提示: 调用 send_verify_code() 和 do_login() 进行登录")
                return None, None, None

        # 3. 自动重新登录
        if auto_relogin:
            log_info("\n" + "=" * 80)
            log_info("开始重新登录流程")
            log_info("=" * 80)

            try:
                # 发送验证码（新账号或强制新设备时生成新token，否则使用原token）
                success, message = self.send_verify_code(force_new_device=force_new_device)
                if not success:
                    log_info(f"\n❌ {message}")
                    return None, None, None

                log_info(f"\n✅ {message}")

                # 等待用户输入验证码
                verify_code = input("\n请输入6位验证码: ").strip()

                # 执行登录
                success, message = self.do_login(verify_code)
                if not success:
                    log_info(f"\n❌ {message}")
                    return None, None, None

                log_info(f"\n✅ {message}")
                return self.token, self.sid, self.meddy_id

            except (NetworkError, ServerError) as e:
                log_info(f"\n❌ 登录失败: {e}")
                log_info("提示: 网络连接问题，请稍后重试")
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
        清除当前手机号的凭证（从文件或 Redis 和内存中删除）
        """
        # 清除内存
        self.token = None
        self.sid = None
        self.meddy_id = None

        if self.use_redis:
            # Redis 存储
            try:
                self.redis_client.hdel(self.redis_key, self.phone)
                log_info(f"✓ 已清除凭证 (手机号: {self.phone})")
            except Exception as e:
                log_info(f"✗ 清除凭证失败: {e}")
        else:
            # 文件存储
            if os.path.exists(self.credentials_file):
                try:
                    with open(self.credentials_file, 'r', encoding='utf-8') as f:
                        all_credentials = json.load(f)

                    if self.phone in all_credentials:
                        del all_credentials[self.phone]

                        with open(self.credentials_file, 'w', encoding='utf-8') as f:
                            json.dump(all_credentials, f, indent=2, ensure_ascii=False)

                        log_info(f"✓ 已清除凭证 (手机号: {self.phone})")
                except Exception as e:
                    log_info(f"✗ 清除凭证失败: {e}")


if __name__ == '__main__':
    LoginManager('16752934813').ensure_login(True)

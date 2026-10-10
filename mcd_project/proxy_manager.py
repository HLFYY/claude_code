#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理池管理器统一入口
聚合 proxy_pool、account_proxy_binding、proxy_failure_handler 等模块的功能
提供统一的 API 接口供 mcd_server 和 scheduler 调用
"""

from proxy_pool import ProxyPool
from account_proxy_binding import AccountProxyBinding
from proxy_failure_handler import ProxyFailureHandler
from ip_location import IPLocationQuery


class ProxyManager:
    """
    代理池管理器统一入口
    封装所有代理相关的功能，提供简洁的调用接口
    """

    def __init__(self, **redis_config):
        """
        初始化代理管理器

        Args:
            **redis_config: Redis 连接配置（仅用于兼容性，实际各模块从 config.py 读取）
        """
        self.proxy_pool = ProxyPool()
        self.account_binding = AccountProxyBinding()
        self.failure_handler = ProxyFailureHandler()
        self.ip_location = IPLocationQuery()

    # ==================== 代理池管理 ====================

    def add_proxy(self, proxy_url, city_code, priority=0):
        """
        添加代理到代理池

        Args:
            proxy_url: 代理地址 (http://user:pass@host:port)
            city_code: 城市代码 (例如: 310100 上海)
            priority: 优先级 (0=普通, 1=高优先级)

        Returns:
            bool: 是否添加成功
        """
        return self.proxy_pool.add_proxy(proxy_url, city_code, priority)

    def remove_proxy(self, proxy_url):
        """
        从代理池移除代理

        Args:
            proxy_url: 代理地址

        Returns:
            bool: 是否移除成功
        """
        return self.proxy_pool.delete_proxy(proxy_url)

    def get_proxy(self, city_code=None):
        """
        获取代理（别名方法）

        Args:
            city_code: 可选，城市代码

        Returns:
            str: 代理URL，如果没有可用代理则返回 None
        """
        return self.proxy_pool.get_proxy(city_code)

    def get_proxy_by_city(self, city_code, account_phone=None):
        """
        根据城市代码获取代理

        Args:
            city_code: 城市代码
            account_phone: 可选，账号手机号（用于检查绑定关系）

        Returns:
            str: 代理URL，如果没有可用代理则返回 None
        """
        return self.proxy_pool.get_proxy(city_code)

    def get_all_proxies_info(self):
        """
        获取所有代理的详细信息

        Returns:
            dict: {proxy_url: {city_code, status, priority, ...}}
        """
        return self.proxy_pool.get_all_proxies()

    def get_pool_status(self):
        """
        获取代理池状态统计

        Returns:
            dict: 包含 total, active, observation, failed 等统计信息
        """
        all_proxies = self.proxy_pool.get_all_proxies()

        status_count = {'active': 0, 'observation': 0, 'failed': 0}
        for proxy_info in all_proxies.values():
            status = proxy_info.get('status', 'active')
            status_count[status] = status_count.get(status, 0) + 1

        return {
            'total': len(all_proxies),
            'active': status_count.get('active', 0),
            'observation': status_count.get('observation', 0),
            'failed': status_count.get('failed', 0)
        }

    # ==================== 账号代理绑定 ====================

    def bind_account_proxy(self, account_phone, proxy_url):
        """
        绑定账号和代理

        Args:
            account_phone: 账号手机号
            proxy_url: 代理地址

        Returns:
            bool: 是否绑定成功
        """
        binding_data = {
            'account_phone': account_phone,
            'proxy_url': proxy_url,
            'bind_time': time.time()
        }
        return self.account_binding.save_binding(account_phone, binding_data)

    def unbind_account_proxy(self, account_phone):
        """
        解绑账号的代理

        Args:
            account_phone: 账号手机号

        Returns:
            bool: 是否解绑成功
        """
        return self.account_binding.unbind_account(account_phone)

    def get_account_proxy(self, account_phone):
        """
        获取账号绑定的代理（返回 requests 库使用的字典格式）

        Args:
            account_phone: 账号手机号

        Returns:
            dict: {'http': '...', 'https': '...'} 格式的代理字典，如果未绑定则返回 None
        """
        binding = self.account_binding.get_binding(account_phone)
        if not binding:
            return None

        proxy_url = None

        # 如果绑定信息里有 proxy_url 字段（旧版本兼容）
        if 'proxy_url' in binding and binding['proxy_url']:
            proxy_url = binding['proxy_url']
        # 如果有 proxy_id 字段（新版本）
        elif 'proxy_id' in binding and binding['proxy_id']:
            proxy_id = binding['proxy_id']
            proxy_info = self.proxy_pool.get_proxy(proxy_id)
            if not proxy_info:
                return None

            # 从 proxy_info 构建 proxy_url
            # 格式: http://username:password@host:port
            if 'proxy_url' in proxy_info:
                proxy_url = proxy_info['proxy_url']
            elif all(k in proxy_info for k in ['host', 'port']):
                protocol = proxy_info.get('protocol', 'http')
                host = proxy_info['host']
                port = proxy_info['port']
                username = proxy_info.get('username')
                password = proxy_info.get('password')

                if username and password:
                    proxy_url = f"{protocol}://{username}:{password}@{host}:{port}"
                else:
                    proxy_url = f"{protocol}://{host}:{port}"

        if not proxy_url:
            return None

        # 返回 requests 库使用的字典格式
        return {
            'http': proxy_url,
            'https': proxy_url
        }

    def get_binding(self, account_phone):
        """
        获取账号的绑定信息（别名方法）

        Args:
            account_phone: 账号手机号

        Returns:
            dict: 绑定信息，包含 proxy_url, bind_time 等
        """
        return self.account_binding.get_binding(account_phone)

    def get_all_bindings(self):
        """
        获取所有账号代理绑定关系

        Returns:
            dict: {account_phone: proxy_url}
        """
        return self.account_binding.get_all_bindings()

    def switch_proxy_for_account(self, account_phone, new_city_code=None):
        """
        为账号切换代理

        Args:
            account_phone: 账号手机号
            new_city_code: 可选，指定新的城市代码

        Returns:
            dict: 包含 old_proxy, new_proxy 的信息
        """
        old_binding = self.account_binding.get_binding(account_phone)
        old_proxy = old_binding.get('proxy_url') if old_binding else None

        # 获取新代理
        new_proxy = self.proxy_pool.get_proxy(new_city_code)

        if new_proxy:
            # 保存新绑定
            binding_data = {
                'account_phone': account_phone,
                'proxy_url': new_proxy,
                'bind_time': time.time()
            }
            self.account_binding.save_binding(account_phone, binding_data)

        return {
            'old_proxy': old_proxy,
            'new_proxy': new_proxy
        }

    # ==================== 失败处理 ====================

    def record_failure(self, proxy_url, account_phone, error_type='network'):
        """
        记录代理失败

        Args:
            proxy_url: 代理地址
            account_phone: 账号手机号
            error_type: 错误类型 (network/timeout/authentication/http_error)

        Returns:
            dict: 失败记录信息，包含 should_switch 等
        """
        return self.failure_handler.handle_request_failure(proxy_url, account_phone, error_type)

    def record_success(self, proxy_url, account_phone):
        """
        记录代理成功

        Args:
            proxy_url: 代理地址
            account_phone: 账号手机号

        Returns:
            dict: 成功记录信息
        """
        return self.failure_handler.handle_request_success(proxy_url, account_phone)

    # ==================== 健康检查 ====================

    def check_proxy_health(self, proxy_url):
        """
        检查单个代理的健康状态

        Args:
            proxy_url: 代理地址

        Returns:
            bool: 是否健康
        """
        return self.proxy_pool.check_proxy_health(proxy_url)

    def check_all_proxies(self):
        """
        检查所有代理的健康状态

        Returns:
            dict: 检查结果统计
        """
        all_proxies = self.proxy_pool.get_all_proxies()
        results = {'healthy': 0, 'unhealthy': 0, 'total': len(all_proxies)}

        for proxy_url in all_proxies.keys():
            is_healthy = self.proxy_pool.check_proxy_health(proxy_url, use_cache=False)
            if is_healthy:
                results['healthy'] += 1
            else:
                results['unhealthy'] += 1

        return results

    # ==================== IP 定位 ====================

    def get_proxy_location(self, proxy_url):
        """
        获取代理的地理位置信息

        Args:
            proxy_url: 代理地址

        Returns:
            dict: 包含 country, region, city, isp 等信息
        """
        return self.ip_location.get_location(proxy_url)

    def verify_proxy_city(self, proxy_url, expected_city_code):
        """
        验证代理的城市是否匹配

        Args:
            proxy_url: 代理地址
            expected_city_code: 期望的城市代码

        Returns:
            bool: 是否匹配
        """
        return self.ip_location.verify_city(proxy_url, expected_city_code)

    # ==================== 别名方法（兼容 mcd_server.py） ====================

    def get_all_account_bindings(self):
        """
        获取所有账号绑定关系（别名方法）

        Returns:
            dict: {phone: {proxy_url, bind_time, city_code}}
        """
        return self.get_all_bindings()

    def switch_account_proxy(self, account_phone, proxy_url=None):
        """
        切换账号的代理（别名方法）

        Args:
            account_phone: 账号手机号
            proxy_url: 新的代理地址（可选，不传则自动选择）

        Returns:
            dict: 包含 success, old_proxy, new_proxy 等信息
        """
        if proxy_url:
            # 手动指定代理
            success = self.bind_account_proxy(account_phone, proxy_url)
            return {
                'success': success,
                'new_proxy': proxy_url if success else None,
                'message': '代理切换成功' if success else '代理切换失败'
            }
        else:
            # 自动切换
            return self.switch_proxy_for_account(account_phone)

    def get_statistics(self):
        """
        获取代理池统计信息

        Returns:
            dict: 包含代理池状态、绑定关系等信息
        """
        pool_status = self.get_pool_status()
        all_bindings = self.get_all_bindings()

        return {
            'pool': pool_status,
            'bindings': all_bindings,
            'summary': {
                'total_proxies': pool_status['total'],
                'active_proxies': pool_status['active'],
                'observation_proxies': pool_status['observation'],
                'total_bindings': len(all_bindings)
            }
        }

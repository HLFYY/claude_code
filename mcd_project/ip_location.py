#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IP 地理位置查询工具
支持多个免费 API，自动重试
"""

import requests
from typing import Optional, Dict
from utils import log_info


class IPLocationQuery:
    """IP 地理位置查询"""

    def __init__(self, timeout: int = 5):
        self.timeout = timeout
        self.apis = [
            self._query_ipapi,
            self._query_ip_api,
            self._query_ipinfo,
        ]

    def query(self, ip: str) -> Optional[str]:
        """
        查询 IP 的城市信息

        Args:
            ip: IP 地址

        Returns:
            城市名称，查询失败返回 None
        """
        for api_func in self.apis:
            try:
                city = api_func(ip)
                if city:
                    return city
            except Exception as e:
                continue

        log_info(f"⚠️  所有 API 查询失败，IP: {ip}")
        return None

    def _query_ipapi(self, ip: str) -> Optional[str]:
        """使用 ipapi.co API"""
        url = f"https://ipapi.co/{ip}/json/"
        response = requests.get(url, timeout=self.timeout)

        if response.status_code == 200:
            data = response.json()
            city = data.get('city')
            if city:
                log_info(f"✓ ipapi.co 查询成功: {ip} -> {city}")
                return city

        return None

    def _query_ip_api(self, ip: str) -> Optional[str]:
        """使用 ip-api.com API"""
        url = f"http://ip-api.com/json/{ip}?lang=zh-CN"
        response = requests.get(url, timeout=self.timeout)

        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                city = data.get('city')
                if city:
                    log_info(f"✓ ip-api.com 查询成功: {ip} -> {city}")
                    return city

        return None

    def _query_ipinfo(self, ip: str) -> Optional[str]:
        """使用 ipinfo.io API"""
        url = f"https://ipinfo.io/{ip}/json"
        response = requests.get(url, timeout=self.timeout)

        if response.status_code == 200:
            data = response.json()
            city = data.get('city')
            if city:
                log_info(f"✓ ipinfo.io 查询成功: {ip} -> {city}")
                return city

        return None


# 单例实例
_ip_query = IPLocationQuery()


def query_ip_city(ip: str) -> Optional[str]:
    """
    便捷函数：查询 IP 的城市信息

    Args:
        ip: IP 地址

    Returns:
        城市名称，查询失败返回 None
    """
    return _ip_query.query(ip)


if __name__ == '__main__':
    # 测试
    test_ips = [
        '61.155.141.105',  # 当前代理 IP
        '8.8.8.8',         # Google DNS
        '114.114.114.114'  # 国内 DNS
    ]

    for ip in test_ips:
        log_info(f"\n查询 IP: {ip}")
        city = query_ip_city(ip)
        log_info(f"结果: {city or '未知'}")
        log_info("-" * 50)

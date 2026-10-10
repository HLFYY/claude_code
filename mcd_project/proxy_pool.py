#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理池管理器

功能：
- 加载代理列表（支持文件/Redis/直接添加）
- 代理健康检查
- 代理状态管理
- 负载统计
- 故障标记
"""

import json
import time
import redis
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any
from config import (
    REDIS_SET_URL,
    PROXY_POOL_CONFIG,
    PROXY_HEALTH_CHECK_CONFIG,
    PROXY_REQUEST_TIMEOUT
)
from ip_location import query_ip_city
from utils import log_info


class ProxyPool:
    """代理池管理器"""

    def __init__(self):
        """初始化代理池"""
        # Redis 连接
        self.redis_client = redis.Redis(
            host=REDIS_SET_URL['host'],
            port=REDIS_SET_URL['port'],
            db=REDIS_SET_URL['db'],
            password=REDIS_SET_URL['password'],
            decode_responses=True
        )

        # Redis Key
        self.pool_key = f"{PROXY_POOL_CONFIG['redis_prefix']}:pool"
        self.stats_key_prefix = f"{PROXY_POOL_CONFIG['redis_prefix']}:stats"
        self.health_cache_prefix = "mcd:proxy_health_cache"

        log_info(f"✓ ProxyPool 初始化完成，Redis Key: {self.pool_key}")

    def add_proxy(self, host: str, port: int, username: str = '', password: str = '',
                  protocol: str = 'http', proxy_id: str = None, city: str = None) -> Dict[str, Any]:
        """
        添加代理到代理池

        Args:
            host: 代理主机
            port: 代理端口
            username: 用户名（可选）
            password: 密码（可选）
            protocol: 协议类型（http/https/socks5）
            proxy_id: 代理ID（可选，自动生成）
            city: 城市名称（可选，自动查询）

        Returns:
            代理信息字典
        """
        # 检查是否已存在相同的 host:port
        all_proxies = self.get_all_proxies()
        for existing_proxy in all_proxies:
            if existing_proxy['host'] == host and existing_proxy['port'] == port:
                log_info(f"⚠️  代理已存在: {existing_proxy['id']} ({host}:{port})，跳过添加")
                return existing_proxy

        # 生成代理ID
        if not proxy_id:
            proxy_id = f"proxy_{int(time.time() * 1000)}"

        # 查询城市信息
        if not city:
            log_info(f"正在查询 IP {host} 的城市信息...")
            city = query_ip_city(host)
            if not city:
                city = "未知"
                log_info(f"⚠️  无法查询到城市信息，使用默认值: 未知")

        # 构建代理信息
        proxy_info = {
            'id': proxy_id,
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'protocol': protocol,
            'city': city,
            'status': PROXY_POOL_CONFIG['status']['active'],
            'bound_accounts': 0,
            'success_count': 0,
            'fail_count': 0,
            'response_time': 0.0,
            'last_check': None,
            'created_at': datetime.now().isoformat()
        }

        # 保存到 Redis
        self.redis_client.hset(
            self.pool_key,
            proxy_id,
            json.dumps(proxy_info, ensure_ascii=False)
        )

        log_info(f"✓ 代理已添加: {proxy_id} ({host}:{port}, {city})")
        return proxy_info

    def get_proxy(self, proxy_id: str) -> Optional[Dict[str, Any]]:
        """
        获取指定代理信息

        Args:
            proxy_id: 代理ID

        Returns:
            代理信息字典，不存在返回None
        """
        proxy_json = self.redis_client.hget(self.pool_key, proxy_id)

        if not proxy_json:
            return None

        return json.loads(proxy_json)

    def get_all_proxies(self, status: str = None) -> List[Dict[str, Any]]:
        """
        获取所有代理

        Args:
            status: 过滤状态（可选）active/slow/observation/failed/disabled

        Returns:
            代理列表
        """
        all_proxies = self.redis_client.hgetall(self.pool_key)

        proxies = []
        for proxy_id, proxy_json in all_proxies.items():
            proxy = json.loads(proxy_json)

            # 状态过滤
            if status and proxy['status'] != status:
                continue

            proxies.append(proxy)

        return proxies

    def get_available_proxy(self, exclude: List[str] = None, city: str = None) -> Optional[Dict[str, Any]]:
        """
        获取可用代理（负载最小优先）

        Args:
            exclude: 排除的代理ID列表
            city: 指定城市（可选），如果指定则优先返回该城市的代理

        Returns:
            代理信息字典，无可用代理返回None
        """
        if exclude is None:
            exclude = []

        # 获取所有active状态的代理
        active_proxies = self.get_all_proxies(status=PROXY_POOL_CONFIG['status']['active'])

        # 过滤排除列表
        available_proxies = [p for p in active_proxies if p['id'] not in exclude]

        if not available_proxies:
            log_info("⚠️  警告: 没有可用代理")
            return None

        # 获取负载上限
        max_accounts = PROXY_POOL_CONFIG['max_accounts_per_proxy']

        # 过滤掉已达上限的代理
        available_proxies = [p for p in available_proxies if p['bound_accounts'] < max_accounts]

        if not available_proxies:
            log_info(f"⚠️  警告: 所有代理已达负载上限 (每个代理最多 {max_accounts} 个账号)")
            return None

        # 如果指定了城市，优先筛选该城市的代理
        if city:
            city_proxies = [p for p in available_proxies if p.get('city') == city]

            if city_proxies:
                log_info(f"✓ 找到 {len(city_proxies)} 个 {city} 的代理，优先使用")
                available_proxies = city_proxies
            else:
                log_info(f"⚠️  未找到 {city} 的代理，使用其他城市的代理")

        # 按绑定账号数排序（负载最小优先）
        available_proxies.sort(key=lambda x: x['bound_accounts'])

        return available_proxies[0]

    def update_proxy(self, proxy_id: str, updates: Dict[str, Any]) -> bool:
        """
        更新代理信息

        Args:
            proxy_id: 代理ID
            updates: 更新字段字典

        Returns:
            是否成功
        """
        proxy = self.get_proxy(proxy_id)

        if not proxy:
            log_info(f"⚠️  代理不存在: {proxy_id}")
            return False

        # 更新字段
        proxy.update(updates)

        # 保存
        self.redis_client.hset(
            self.pool_key,
            proxy_id,
            json.dumps(proxy, ensure_ascii=False)
        )

        return True

    def mark_proxy_success(self, proxy_id: str) -> None:
        """
        标记代理请求成功

        Args:
            proxy_id: 代理ID
        """
        proxy = self.get_proxy(proxy_id)

        if not proxy:
            return

        proxy['success_count'] += 1
        proxy['fail_count'] = 0  # 重置失败计数

        self.update_proxy(proxy_id, proxy)

    def mark_proxy_failed(self, proxy_id: str) -> None:
        """
        标记代理请求失败

        Args:
            proxy_id: 代理ID
        """
        proxy = self.get_proxy(proxy_id)

        if not proxy:
            return

        proxy['fail_count'] += 1

        # 连续失败超过阈值，标记为failed
        if proxy['fail_count'] >= PROXY_POOL_CONFIG['max_fail_count']:
            proxy['status'] = PROXY_POOL_CONFIG['status']['failed']
            log_info(f"❌ 代理 {proxy_id} 连续失败{proxy['fail_count']}次，标记为 failed")

        self.update_proxy(proxy_id, proxy)

    def mark_proxy_observation(self, proxy_id: str) -> None:
        """
        标记代理进入观察期

        Args:
            proxy_id: 代理ID
        """
        self.update_proxy(proxy_id, {
            'status': PROXY_POOL_CONFIG['status']['observation']
        })
        log_info(f"👁️  代理 {proxy_id} 进入观察期")

    def increase_bound_count(self, proxy_id: str) -> None:
        """
        增加绑定账号数

        Args:
            proxy_id: 代理ID
        """
        proxy = self.get_proxy(proxy_id)

        if proxy:
            proxy['bound_accounts'] += 1
            self.update_proxy(proxy_id, proxy)

    def decrease_bound_count(self, proxy_id: str) -> None:
        """
        减少绑定账号数

        Args:
            proxy_id: 代理ID
        """
        proxy = self.get_proxy(proxy_id)

        if proxy and proxy['bound_accounts'] > 0:
            proxy['bound_accounts'] -= 1
            self.update_proxy(proxy_id, proxy)

    def check_proxy_health(self, proxy_id: str, use_cache: bool = True) -> bool:
        """
        检查单个代理健康状态

        Args:
            proxy_id: 代理ID
            use_cache: 是否使用缓存（默认True）

        Returns:
            是否健康
        """
        # 检查缓存
        if use_cache:
            cache_key = f"{self.health_cache_prefix}:{proxy_id}"
            cached = self.redis_client.get(cache_key)

            if cached:
                result = json.loads(cached)
                log_info(f"  📦 使用缓存结果: {proxy_id} -> {result['status']}")
                return result['status'] == 'healthy'

        # 获取代理信息
        proxy = self.get_proxy(proxy_id)

        if not proxy:
            return False

        # 构建代理URL
        proxy_url = self._build_proxy_url(proxy)

        # 测试请求
        test_url = 'https://api.mcd.cn/bff/common/proxy/tid?channelId=96&campaignId=0&exposureId=0&groupId=0&salerId=0'

        try:
            start = time.time()

            response = requests.get(
                test_url,
                proxies={'http': proxy_url, 'https': proxy_url},
                timeout=PROXY_HEALTH_CHECK_CONFIG['health_check_timeout']
            )

            response_time = time.time() - start

            is_healthy = response.status_code == 200

            # 更新代理信息
            updates = {
                'response_time': round(response_time, 2),
                'last_check': datetime.now().isoformat()
            }

            if is_healthy:
                if response_time > PROXY_POOL_CONFIG['slow_threshold']:
                    updates['status'] = PROXY_POOL_CONFIG['status']['slow']
                else:
                    updates['status'] = PROXY_POOL_CONFIG['status']['active']
            else:
                self.mark_proxy_failed(proxy_id)

            self.update_proxy(proxy_id, updates)

            # 写入缓存
            cache_result = {
                'status': 'healthy' if is_healthy else 'failed',
                'check_time': datetime.now().isoformat(),
                'response_time': response_time
            }

            cache_key = f"{self.health_cache_prefix}:{proxy_id}"
            self.redis_client.setex(
                cache_key,
                PROXY_HEALTH_CHECK_CONFIG['health_check_cache_ttl'],
                json.dumps(cache_result)
            )

            status_emoji = "✓" if is_healthy else "✗"
            log_info(f"  {status_emoji} 代理 {proxy_id} 健康检查: {'正常' if is_healthy else '失败'} ({response_time:.2f}s)")

            return is_healthy

        except Exception as e:
            log_info(f"  ✗ 代理 {proxy_id} 健康检查失败: {str(e)}")
            self.mark_proxy_failed(proxy_id)

            # 写入失败缓存
            cache_result = {
                'status': 'failed',
                'check_time': datetime.now().isoformat(),
                'error': str(e)
            }

            cache_key = f"{self.health_cache_prefix}:{proxy_id}"
            self.redis_client.setex(
                cache_key,
                PROXY_HEALTH_CHECK_CONFIG['health_check_cache_ttl'],
                json.dumps(cache_result)
            )

            return False

    def full_health_check(self) -> Dict[str, Any]:
        """
        全量健康检查

        Returns:
            检查结果统计
        """
        log_info("\n" + "="*50)
        log_info("开始全量健康检查")
        log_info("="*50)

        proxies = self.get_all_proxies()

        if not proxies:
            log_info("⚠️  代理池为空")
            return {'total': 0, 'healthy': 0, 'failed': 0}

        total = len(proxies)
        healthy_count = 0
        failed_count = 0

        batch_size = PROXY_HEALTH_CHECK_CONFIG['batch_size']
        batch_delay = PROXY_HEALTH_CHECK_CONFIG['batch_delay']

        # 分批检查
        for i in range(0, total, batch_size):
            batch = proxies[i:i+batch_size]
            batch_num = i // batch_size + 1

            log_info(f"\n批次 {batch_num} ({len(batch)} 个代理):")

            for proxy in batch:
                is_healthy = self.check_proxy_health(proxy['id'], use_cache=False)

                if is_healthy:
                    healthy_count += 1
                else:
                    failed_count += 1

            # 批次间隔
            if i + batch_size < total:
                log_info(f"  ⏳ 等待 {batch_delay} 秒...")
                time.sleep(batch_delay)

        result = {
            'total': total,
            'healthy': healthy_count,
            'failed': failed_count,
            'check_time': datetime.now().isoformat()
        }

        log_info("\n" + "="*50)
        log_info(f"健康检查完成: 总数={total}, 正常={healthy_count}, 失败={failed_count}")
        log_info("="*50 + "\n")

        return result

    def get_proxy_stats(self) -> Dict[str, Any]:
        """
        获取代理池统计信息

        Returns:
            统计信息字典
        """
        proxies = self.get_all_proxies()

        if not proxies:
            return {
                'total': 0,
                'by_status': {},
                'total_bound_accounts': 0,
                'avg_response_time': 0
            }

        # 按状态分组统计
        by_status = {}
        total_bound = 0
        total_response_time = 0
        response_time_count = 0

        for proxy in proxies:
            status = proxy['status']
            by_status[status] = by_status.get(status, 0) + 1

            total_bound += proxy['bound_accounts']

            if proxy['response_time'] > 0:
                total_response_time += proxy['response_time']
                response_time_count += 1

        avg_response_time = 0
        if response_time_count > 0:
            avg_response_time = round(total_response_time / response_time_count, 2)

        return {
            'total': len(proxies),
            'by_status': by_status,
            'total_bound_accounts': total_bound,
            'avg_response_time': avg_response_time
        }

    def get_load_statistics(self) -> Dict[str, Any]:
        """
        获取代理负载统计信息

        Returns:
            负载统计字典，包含：
            - max_accounts_per_proxy: 每个代理最大账号数（来自配置）
            - total_proxies: 总代理数
            - active_proxies: 可用代理数（active + slow）
            - total_capacity: 总容量（所有可用代理的最大账号数之和）
            - total_bound: 已绑定账号总数
            - remaining_capacity: 剩余容量
            - utilization_rate: 使用率（百分比）
            - proxies: 每个代理的详细负载信息
        """
        proxies = self.get_all_proxies()
        max_per_proxy = PROXY_POOL_CONFIG['max_accounts_per_proxy']

        if not proxies:
            return {
                'max_accounts_per_proxy': max_per_proxy,
                'total_proxies': 0,
                'active_proxies': 0,
                'total_capacity': 0,
                'total_bound': 0,
                'remaining_capacity': 0,
                'utilization_rate': 0.0,
                'proxies': []
            }

        # 统计数据
        total_proxies = len(proxies)
        active_proxies = 0
        total_bound = 0
        proxy_details = []

        active_statuses = [
            PROXY_POOL_CONFIG['status']['active'],
            PROXY_POOL_CONFIG['status']['slow']
        ]

        for proxy in proxies:
            bound_count = proxy['bound_accounts']
            total_bound += bound_count

            is_active = proxy['status'] in active_statuses
            if is_active:
                active_proxies += 1

            remaining = max_per_proxy - bound_count if is_active else 0

            proxy_details.append({
                'proxy_id': proxy['id'],
                'host': proxy['host'],
                'port': proxy['port'],
                'city': proxy.get('city', '未知'),
                'status': proxy['status'],
                'is_active': is_active,
                'bound_accounts': bound_count,
                'max_accounts': max_per_proxy,
                'remaining_capacity': remaining,
                'utilization_rate': round(bound_count / max_per_proxy * 100, 1) if is_active else 0.0
            })

        # 总容量 = 可用代理数 × 每个代理最大账号数
        total_capacity = active_proxies * max_per_proxy
        remaining_capacity = total_capacity - total_bound
        utilization_rate = round(total_bound / total_capacity * 100, 1) if total_capacity > 0 else 0.0

        return {
            'max_accounts_per_proxy': max_per_proxy,
            'total_proxies': total_proxies,
            'active_proxies': active_proxies,
            'total_capacity': total_capacity,
            'total_bound': total_bound,
            'remaining_capacity': remaining_capacity,
            'utilization_rate': utilization_rate,
            'proxies': proxy_details
        }

    def update_all_cities(self) -> Dict[str, Any]:
        """
        更新所有代理的城市信息
        如果 Redis 中的城市与实际查询结果不同，则更新

        Returns:
            更新结果统计
        """
        log_info("\n" + "="*50)
        log_info("开始更新代理城市信息")
        log_info("="*50)

        proxies = self.get_all_proxies()

        if not proxies:
            log_info("⚠️  代理池为空")
            return {'total': 0, 'updated': 0, 'skipped': 0, 'failed': 0}

        total = len(proxies)
        updated_count = 0
        skipped_count = 0
        failed_count = 0
        updates = []

        for proxy in proxies:
            proxy_id = proxy['id']
            host = proxy['host']
            old_city = proxy.get('city', '未知')

            log_info(f"\n检查代理: {proxy_id} ({host})")
            log_info(f"  当前城市: {old_city}")

            # 查询真实城市
            new_city = query_ip_city(host)

            if not new_city:
                log_info(f"  ⚠️  查询失败，跳过")
                failed_count += 1
                continue

            # 比较城市是否变化
            if new_city != old_city:
                log_info(f"  🔄 城市变化: {old_city} -> {new_city}")

                # 更新 Redis
                success = self.update_proxy(proxy_id, {'city': new_city})

                if success:
                    updated_count += 1
                    updates.append({
                        'proxy_id': proxy_id,
                        'host': host,
                        'old_city': old_city,
                        'new_city': new_city
                    })
                    log_info(f"  ✓ 更新成功")
                else:
                    failed_count += 1
                    log_info(f"  ✗ 更新失败")
            else:
                log_info(f"  ✓ 城市未变化，跳过")
                skipped_count += 1

            # 避免 API 限流
            time.sleep(0.5)

        result = {
            'total': total,
            'updated': updated_count,
            'skipped': skipped_count,
            'failed': failed_count,
            'updates': updates
        }

        log_info("\n" + "="*50)
        log_info(f"城市信息更新完成:")
        log_info(f"  总数: {total}")
        log_info(f"  更新: {updated_count}")
        log_info(f"  跳过: {skipped_count}")
        log_info(f"  失败: {failed_count}")
        log_info("="*50 + "\n")

        return result

    def _build_proxy_url(self, proxy: Dict[str, Any]) -> str:
        """
        构建代理URL

        Args:
            proxy: 代理信息字典

        Returns:
            代理URL字符串
        """
        protocol = proxy['protocol']
        host = proxy['host']
        port = proxy['port']
        username = proxy.get('username', '')
        password = proxy.get('password', '')

        if username and password:
            return f"{protocol}://{username}:{password}@{host}:{port}"
        else:
            return f"{protocol}://{host}:{port}"

    def get_proxy_url(self, proxy_id: str) -> Optional[str]:
        """
        获取代理URL（外部调用）

        Args:
            proxy_id: 代理ID

        Returns:
            代理URL字符串
        """
        proxy = self.get_proxy(proxy_id)

        if not proxy:
            return None

        return self._build_proxy_url(proxy)

    def delete_proxy(self, proxy_id: str) -> bool:
        """
        删除代理

        Args:
            proxy_id: 代理ID

        Returns:
            是否成功
        """
        result = self.redis_client.hdel(self.pool_key, proxy_id)

        if result:
            log_info(f"✓ 代理已删除: {proxy_id}")
        else:
            log_info(f"⚠️  代理不存在: {proxy_id}")

        return bool(result)

    def clear_all(self) -> int:
        """
        清空代理池（危险操作，仅测试使用）

        Returns:
            删除的代理数量
        """
        proxies = self.get_all_proxies()
        count = len(proxies)

        if count > 0:
            self.redis_client.delete(self.pool_key)
            log_info(f"⚠️  已清空代理池，删除 {count} 个代理")

        return count


if __name__ == '__main__':
    # 测试代码
    log_info("ProxyPool 测试\n")

    pool = ProxyPool()

    # 清空旧数据
    pool.clear_all()

    # 添加测试代理
    log_info("\n1. 添加代理:")
    proxy = pool.add_proxy(
        host='61.155.141.105',
        port=20080,
        username='jygdGVUi',
        password='d5xP8koC0PHz',
        protocol='http',
        proxy_id='proxy_test_001'
    )

    # 获取代理
    log_info("\n2. 获取代理:")
    proxy = pool.get_proxy('proxy_test_001')
    log_info(f"代理信息: {json.dumps(proxy, indent=2, ensure_ascii=False)}")

    # 健康检查
    log_info("\n3. 健康检查:")
    is_healthy = pool.check_proxy_health('proxy_test_001', use_cache=False)
    log_info(f"健康状态: {'✓ 正常' if is_healthy else '✗ 失败'}")

    # 获取统计
    log_info("\n4. 统计信息:")
    stats = pool.get_proxy_stats()
    log_info(f"统计: {json.dumps(stats, indent=2, ensure_ascii=False)}")

    # 获取代理URL
    log_info("\n5. 代理URL:")
    url = pool.get_proxy_url('proxy_test_001')
    log_info(f"URL: {url}")

    log_info("\n✓ 测试完成")

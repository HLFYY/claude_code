#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
账号-代理绑定管理器

功能：
- 为账号分配代理（首次登录时）
- 持久化绑定关系（Redis）
- 绑定关系查询
- 观察期管理
- 代理故障时重新分配
"""

import json
import redis
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from config import (
    REDIS_SET_URL,
    ACCOUNT_PROXY_BINDING_CONFIG,
    PROXY_FAILURE_CONFIG,
    make_redis,
)
from proxy_pool import ProxyPool
from utils import log_info


class AccountProxyBinding:
    """账号-代理绑定管理器"""

    def __init__(self, proxy_pool=None):
        """
        初始化绑定管理器

        Args:
            proxy_pool: ProxyPool 实例（可选，不传则自动创建）
        """
        # Redis 连接（带超时/重试/keepalive）
        self.redis_client = make_redis()

        # ProxyPool 实例（使用传入的或创建新的）
        if proxy_pool is not None:
            self.proxy_pool = proxy_pool
        else:
            from proxy_pool import ProxyPool
            self.proxy_pool = ProxyPool()

        # Redis Key
        self.binding_key_prefix = ACCOUNT_PROXY_BINDING_CONFIG['redis_key']
        self.observation_set_key = 'mcd:accounts_in_observation'

        log_info(f"✓ AccountProxyBinding 初始化完成")

    def get_binding(self, phone: str) -> Optional[Dict[str, Any]]:
        """
        获取账号的绑定信息

        Args:
            phone: 手机号

        Returns:
            绑定信息字典，不存在返回None
        """
        key = f"{self.binding_key_prefix}:{phone}"
        binding_json = self.redis_client.get(key)

        if not binding_json:
            return None

        return json.loads(binding_json)

    def save_binding(self, phone: str, binding: Dict[str, Any]) -> None:
        """
        保存绑定信息

        Args:
            phone: 手机号
            binding: 绑定信息字典
        """
        key = f"{self.binding_key_prefix}:{phone}"

        # 设置TTL（0表示永不过期）
        ttl = ACCOUNT_PROXY_BINDING_CONFIG['binding_ttl']

        if ttl > 0:
            self.redis_client.setex(
                key,
                ttl,
                json.dumps(binding, ensure_ascii=False)
            )
        else:
            self.redis_client.set(
                key,
                json.dumps(binding, ensure_ascii=False)
            )

    def get_or_assign_proxy(self, phone: str, city: str = None) -> Dict[str, Any]:
        """
        获取或分配代理

        Args:
            phone: 手机号
            city: 指定城市（可选），如果指定则优先分配该城市的代理

        Returns:
            代理信息字典
        """
        # 检查是否已有绑定
        binding = self.get_binding(phone)

        if binding:
            proxy_id = binding['proxy_id']
            proxy = self.proxy_pool.get_proxy(proxy_id)

            if proxy:
                log_info(f"✓ 账号 {phone} 已绑定代理 {proxy_id}")
                return proxy

            # 绑定的代理不存在，重新分配
            log_info(f"⚠️  账号 {phone} 绑定的代理 {proxy_id} 不存在，重新分配")

        # 分配新代理（支持按城市筛选）
        proxy = self.proxy_pool.get_available_proxy(city=city)

        if not proxy:
            raise Exception("没有可用代理")

        # 创建绑定
        binding = {
            'phone': phone,
            'proxy_id': proxy['id'],
            'temp_proxy_id': None,
            'bound_at': datetime.now().isoformat(),

            # 观察期状态
            'in_observation': False,
            'observation_start': None,
            'observation_until': None,
            'observation_fail_count': 0,

            # 失败统计
            'total_fail_count': 0,
            'fail_window_start': datetime.now().isoformat(),
            'last_fail_time': None,

            # 请求统计
            'request_count': 0,
            'last_request': None
        }

        self.save_binding(phone, binding)

        # 增加代理绑定计数
        self.proxy_pool.increase_bound_count(proxy['id'])

        log_info(f"✓ 账号 {phone} 分配代理 {proxy['id']} (城市: {proxy.get('city', '未知')})")

        return proxy

    def get_proxy_for_request(self, phone: str) -> Dict[str, Any]:
        """
        获取本次请求应该使用的代理

        逻辑：
        1. 如果有临时代理且在观察期 → 使用临时代理
        2. 否则 → 使用主代理

        Args:
            phone: 手机号

        Returns:
            代理信息字典
        """
        binding = self.get_binding(phone)

        if not binding:
            # 没有绑定，分配新代理
            return self.get_or_assign_proxy(phone)

        # 检查是否在观察期
        if binding.get('in_observation') and binding.get('temp_proxy_id'):
            # 使用临时代理
            proxy_id = binding['temp_proxy_id']
            proxy = self.proxy_pool.get_proxy(proxy_id)

            if proxy:
                return proxy

        # 使用主代理
        proxy_id = binding['proxy_id']
        proxy = self.proxy_pool.get_proxy(proxy_id)

        if proxy:
            return proxy

        # 主代理不存在，重新分配
        log_info(f"⚠️  账号 {phone} 的代理不存在，重新分配")
        return self.get_or_assign_proxy(phone)

    def start_observation(self, phone: str, main_proxy_id: str, temp_proxy_id: str) -> None:
        """
        启动观察期

        Args:
            phone: 手机号
            main_proxy_id: 主代理ID（进入观察）
            temp_proxy_id: 临时代理ID
        """
        binding = self.get_binding(phone)

        if not binding:
            log_info(f"⚠️  账号 {phone} 没有绑定信息")
            return

        now = datetime.now()
        observation_until = now + timedelta(seconds=PROXY_FAILURE_CONFIG['observation_period'])

        binding.update({
            'temp_proxy_id': temp_proxy_id,
            'in_observation': True,
            'observation_start': now.isoformat(),
            'observation_until': observation_until.isoformat(),
            'observation_fail_count': 0,  # 观察期失败计数归零
        })

        self.save_binding(phone, binding)

        # 添加到观察期集合
        self.redis_client.sadd(self.observation_set_key, phone)

        # 标记主代理进入观察状态
        self.proxy_pool.mark_proxy_observation(main_proxy_id)

        log_info(f"👁️  账号 {phone} 进入观察期: {main_proxy_id} → {temp_proxy_id}（临时）")

    def increment_fail_count(self, phone: str) -> int:
        """
        增加失败计数

        Args:
            phone: 手机号

        Returns:
            当前失败次数
        """
        binding = self.get_binding(phone)

        if not binding:
            return 0

        now = datetime.now()

        # 检查失败计数窗口是否过期
        fail_window_start = datetime.fromisoformat(binding['fail_window_start'])
        window_duration = timedelta(seconds=PROXY_FAILURE_CONFIG['failure_window'])

        if now - fail_window_start > window_duration:
            # 窗口过期，重置计数
            binding['total_fail_count'] = 1
            binding['fail_window_start'] = now.isoformat()
        else:
            # 窗口内，累加计数
            binding['total_fail_count'] += 1

        binding['last_fail_time'] = now.isoformat()

        self.save_binding(phone, binding)

        return binding['total_fail_count']

    def increment_observation_fail_count(self, phone: str) -> int:
        """
        增加观察期失败计数

        Args:
            phone: 手机号

        Returns:
            当前观察期失败次数
        """
        binding = self.get_binding(phone)

        if not binding or not binding.get('in_observation'):
            return 0

        observation_fail_count = binding.get('observation_fail_count', 0) + 1
        binding['observation_fail_count'] = observation_fail_count
        binding['last_fail_time'] = datetime.now().isoformat()

        self.save_binding(phone, binding)

        return observation_fail_count

    def permanent_switch(self, phone: str, new_proxy_id: str) -> None:
        """
        永久切换代理

        Args:
            phone: 手机号
            new_proxy_id: 新代理ID
        """
        binding = self.get_binding(phone)

        if not binding:
            log_info(f"⚠️  账号 {phone} 没有绑定信息")
            return

        old_proxy_id = binding['proxy_id']

        # 更新绑定
        binding.update({
            'proxy_id': new_proxy_id,
            'temp_proxy_id': None,
            'in_observation': False,
            'observation_start': None,
            'observation_until': None,
            'observation_fail_count': 0,
            'bound_at': datetime.now().isoformat(),
            'total_fail_count': 0,  # 重置失败计数
            'fail_window_start': datetime.now().isoformat(),
        })

        self.save_binding(phone, binding)

        # 从观察期集合移除
        self.redis_client.srem(self.observation_set_key, phone)

        # 更新代理池统计
        self.proxy_pool.decrease_bound_count(old_proxy_id)
        self.proxy_pool.increase_bound_count(new_proxy_id)

        log_info(f"🔄 账号 {phone} 永久切换: {old_proxy_id} → {new_proxy_id}")

    def revert_to_main_proxy(self, phone: str) -> None:
        """
        回归主代理（观察期结束，主代理恢复）

        Args:
            phone: 手机号
        """
        binding = self.get_binding(phone)

        if not binding:
            return

        main_proxy_id = binding['proxy_id']

        binding.update({
            'temp_proxy_id': None,
            'in_observation': False,
            'observation_start': None,
            'observation_until': None,
            'observation_fail_count': 0,
        })

        self.save_binding(phone, binding)

        # 从观察期集合移除
        self.redis_client.srem(self.observation_set_key, phone)

        log_info(f"↩️  账号 {phone} 回归主代理 {main_proxy_id}")

    def is_in_observation(self, phone: str) -> bool:
        """
        检查账号是否在观察期

        Args:
            phone: 手机号

        Returns:
            是否在观察期
        """
        binding = self.get_binding(phone)

        if not binding:
            return False

        return binding.get('in_observation', False)

    def get_accounts_in_observation(self) -> List[str]:
        """
        获取所有处于观察期的账号

        Returns:
            账号列表
        """
        return list(self.redis_client.smembers(self.observation_set_key))

    def check_observation_status(self, phone: str) -> str:
        """
        检查观察期状态（由定时任务调用）

        Returns:
            action: 'continue' / 'revert' / 'permanent_switch'
        """
        binding = self.get_binding(phone)

        if not binding or not binding.get('in_observation'):
            return 'continue'

        # 检查观察期是否到期
        observation_until = datetime.fromisoformat(binding['observation_until'])

        if datetime.now() < observation_until:
            return 'continue'  # 观察期未结束

        # 观察期结束，进行健康检查
        main_proxy_id = binding['proxy_id']
        is_healthy = self.proxy_pool.check_proxy_health(main_proxy_id, use_cache=False)

        if is_healthy:
            # 主代理恢复，回归主代理
            self.revert_to_main_proxy(phone)
            return 'revert'
        else:
            # 主代理仍失败，永久切换到临时代理
            temp_proxy_id = binding['temp_proxy_id']
            self.permanent_switch(phone, temp_proxy_id)
            return 'permanent_switch'

    def record_request(self, phone: str) -> None:
        """
        记录请求统计

        Args:
            phone: 手机号
        """
        binding = self.get_binding(phone)

        if not binding:
            return

        binding['request_count'] = binding.get('request_count', 0) + 1
        binding['last_request'] = datetime.now().isoformat()

        self.save_binding(phone, binding)

    def get_all_bindings(self) -> List[Dict[str, Any]]:
        """
        获取所有绑定关系

        Returns:
            绑定列表
        """
        # 扫描所有绑定Key
        pattern = f"{self.binding_key_prefix}:*"
        keys = self.redis_client.keys(pattern)

        bindings = []
        for key in keys:
            binding_json = self.redis_client.get(key)
            if binding_json:
                bindings.append(json.loads(binding_json))

        return bindings

    def get_binding_stats(self) -> Dict[str, Any]:
        """
        获取绑定统计信息

        Returns:
            统计信息字典
        """
        bindings = self.get_all_bindings()

        total = len(bindings)
        in_observation = sum(1 for b in bindings if b.get('in_observation'))

        return {
            'total_accounts': total,
            'in_observation': in_observation,
            'normal': total - in_observation
        }


if __name__ == '__main__':
    # 测试代码
    log_info("AccountProxyBinding 测试\n")

    binding_manager = AccountProxyBinding()

    # 测试账号
    test_phone = "13800138000"

    # 1. 获取或分配代理
    log_info("\n1. 获取或分配代理:")
    proxy = binding_manager.get_or_assign_proxy(test_phone)
    log_info(f"分配代理: {proxy['id']}")

    # 2. 获取绑定信息
    log_info("\n2. 获取绑定信息:")
    binding = binding_manager.get_binding(test_phone)
    log_info(f"绑定信息: {json.dumps(binding, indent=2, ensure_ascii=False)}")

    # 3. 增加失败计数
    log_info("\n3. 增加失败计数:")
    for i in range(3):
        fail_count = binding_manager.increment_fail_count(test_phone)
        log_info(f"  失败次数: {fail_count}")

    # 4. 启动观察期
    log_info("\n4. 启动观察期:")
    temp_proxy = binding_manager.proxy_pool.get_available_proxy()
    if temp_proxy:
        binding_manager.start_observation(
            test_phone,
            proxy['id'],
            temp_proxy['id']
        )

    # 5. 获取观察期账号
    log_info("\n5. 观察期账号:")
    obs_accounts = binding_manager.get_accounts_in_observation()
    log_info(f"观察期账号: {obs_accounts}")

    # 6. 增加观察期失败计数
    log_info("\n6. 观察期失败计数:")
    for i in range(3):
        obs_fail_count = binding_manager.increment_observation_fail_count(test_phone)
        log_info(f"  观察期失败次数: {obs_fail_count}")

    # 7. 获取统计
    log_info("\n7. 绑定统计:")
    stats = binding_manager.get_binding_stats()
    log_info(f"统计: {json.dumps(stats, indent=2, ensure_ascii=False)}")

    log_info("\n✓ 测试完成")

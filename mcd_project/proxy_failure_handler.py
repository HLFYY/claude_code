#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理失败处理器（四级容错）

功能：
- 阶段1：首次失败 → 立即重试（同代理）
- 阶段2：连续失败3次 → 临时切换 + 观察期
- 阶段2.5：观察期内连续失败10次 → 立即永久切换
- 阶段3：观察期结束 → 健康检查决定
"""

import time
import logging
from typing import Tuple
from config import PROXY_FAILURE_CONFIG
from proxy_pool import ProxyPool
from account_proxy_binding import AccountProxyBinding
from utils import log_info

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ProxyFailureHandler:
    """代理失败处理器（四级容错）"""

    def __init__(self, proxy_pool=None, binding_manager=None):
        """
        初始化失败处理器

        Args:
            proxy_pool: ProxyPool 实例（可选，不传则自动创建）
            binding_manager: AccountProxyBinding 实例（可选，不传则自动创建）
        """
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
            self.binding_manager = AccountProxyBinding(proxy_pool=self.proxy_pool)

        # 配置参数
        self.observation_threshold = PROXY_FAILURE_CONFIG['observation_threshold']
        self.permanent_switch_threshold = PROXY_FAILURE_CONFIG['permanent_switch_threshold']

        logger.info("✓ ProxyFailureHandler 初始化完成")

    def handle_request_failure(self, phone: str, proxy_id: str, error: str) -> Tuple[bool, str, str, str]:
        """
        处理请求失败

        Args:
            phone: 手机号
            proxy_id: 当前使用的代理ID
            error: 错误信息

        Returns:
            (should_retry, new_proxy_id, action, message)
            - should_retry: 是否应该重试
            - new_proxy_id: 新的代理ID（如果切换）
            - action: 执行的动作类型
            - message: 日志消息
        """
        binding = self.binding_manager.get_binding(phone)

        if not binding:
            logger.error(f"账号 {phone} 没有绑定信息")
            return (False, proxy_id, "error", "账号没有绑定信息")

        # 标记代理失败
        self.proxy_pool.mark_proxy_failed(proxy_id)

        # ===== 优先检查：是否在观察期内 =====
        if self.binding_manager.is_in_observation(phone):
            observation_fail_count = self.binding_manager.increment_observation_fail_count(phone)

            if observation_fail_count >= self.permanent_switch_threshold:
                # 立即永久切换
                temp_proxy_id = binding['temp_proxy_id']
                self.binding_manager.permanent_switch(phone, temp_proxy_id)

                logger.error(
                    f"账号 {phone[-4:]} 观察期内连续失败{observation_fail_count}次，"
                    f"立即永久切换到 {temp_proxy_id}"
                )

                # 标记原代理为彻底失效
                self.proxy_pool.update_proxy(binding['proxy_id'], {'status': 'failed'})

                return (True, temp_proxy_id, "permanent_switch_immediate",
                        "观察期连续失败10次，立即永久切换")
            else:
                # 继续使用临时代理
                logger.warning(
                    f"账号 {phone[-4:]} 观察期内失败 {observation_fail_count}/{self.permanent_switch_threshold} 次，"
                    f"继续使用临时代理 {binding['temp_proxy_id']}"
                )
                return (True, binding['temp_proxy_id'], "continue_temp_proxy",
                        f"观察期内失败{observation_fail_count}次")

        # ===== 阶段1-2：正常失败计数逻辑 =====
        total_fail_count = self.binding_manager.increment_fail_count(phone)

        if total_fail_count == 1:
            # 阶段1：首次失败 → 立即重试
            logger.info(f"账号 {phone[-4:]} 代理 {proxy_id} 首次失败，1秒后重试")
            time.sleep(1)
            return (True, proxy_id, "retry_same_proxy", "首次失败，立即重试")

        elif total_fail_count == 2:
            # 第2次失败 → 继续重试
            logger.warning(f"账号 {phone[-4:]} 代理 {proxy_id} 第2次失败，继续重试")
            time.sleep(1)
            return (True, proxy_id, "retry_same_proxy", "第2次失败，继续重试")

        elif total_fail_count == self.observation_threshold:
            # 阶段2：连续失败3次 → 临时切换
            temp_proxy = self.proxy_pool.get_available_proxy(exclude=[proxy_id])

            if not temp_proxy:
                logger.error(f"账号 {phone[-4:]} 无可用临时代理")
                return (False, proxy_id, "error", "无可用代理")

            # 启动观察期
            self.binding_manager.start_observation(phone, proxy_id, temp_proxy['id'])

            logger.warning(
                f"账号 {phone[-4:]} 代理 {proxy_id} 连续失败{total_fail_count}次，"
                f"临时切换到 {temp_proxy['id']}，观察期{PROXY_FAILURE_CONFIG['observation_period']//60}分钟"
            )

            return (True, temp_proxy['id'], "temp_switch", "连续失败3次，临时切换")

        else:
            # 阶段3：超过3次（异常情况，不应该到这里）→ 永久切换
            new_proxy = self.proxy_pool.get_available_proxy(exclude=[proxy_id])

            if not new_proxy:
                logger.error(f"账号 {phone[-4:]} 无可用代理")
                return (False, proxy_id, "error", "无可用代理")

            self.binding_manager.permanent_switch(phone, new_proxy['id'])

            logger.error(
                f"账号 {phone[-4:]} 代理 {proxy_id} 失败{total_fail_count}次（异常），"
                f"永久切换到 {new_proxy['id']}"
            )

            return (True, new_proxy['id'], "permanent_switch", "异常失败，永久切换")

    def handle_request_success(self, phone: str, proxy_id: str) -> None:
        """
        处理请求成功

        Args:
            phone: 手机号
            proxy_id: 代理ID
        """
        # 标记代理成功
        self.proxy_pool.mark_proxy_success(proxy_id)

        # 记录请求统计
        self.binding_manager.record_request(phone)


if __name__ == '__main__':
    # 测试代码
    log_info("ProxyFailureHandler 测试\n")

    handler = ProxyFailureHandler()

    # 准备测试数据
    test_phone = "13800138001"
    pool = ProxyPool()

    # 添加两个测试代理
    proxy1 = pool.add_proxy(
        host='61.155.141.105',
        port=20080,
        username='jygdGVUi',
        password='d5xP8koC0PHz',
        protocol='http',
        proxy_id='proxy_test_001'
    )

    proxy2 = pool.add_proxy(
        host='61.155.141.106',
        port=20080,
        username='test',
        password='test',
        protocol='http',
        proxy_id='proxy_test_002'
    )

    # 为账号分配代理
    binding_mgr = AccountProxyBinding()
    assigned_proxy = binding_mgr.get_or_assign_proxy(test_phone)

    log_info(f"\n初始代理: {assigned_proxy['id']}")

    # 模拟失败场景
    log_info("\n" + "="*60)
    log_info("测试1：基础失败处理流程（1次→2次→3次）")
    log_info("="*60)

    current_proxy_id = assigned_proxy['id']

    # 第1次失败 - 立即重试
    log_info("\n第1次失败:")
    should_retry, current_proxy_id, action, msg = handler.handle_request_failure(
        test_phone, current_proxy_id, "Connection timeout"
    )
    log_info(f"  动作: {action}")
    log_info(f"  代理: {current_proxy_id}")
    log_info(f"  消息: {msg}")

    # 第2次失败
    log_info("\n第2次失败:")
    should_retry, current_proxy_id, action, msg = handler.handle_request_failure(
        test_phone, current_proxy_id, "Connection timeout"
    )
    log_info(f"  动作: {action}")
    log_info(f"  代理: {current_proxy_id}")
    log_info(f"  消息: {msg}")

    # 第3次失败 - 临时切换
    log_info("\n第3次失败:")
    should_retry, current_proxy_id, action, msg = handler.handle_request_failure(
        test_phone, current_proxy_id, "Connection timeout"
    )
    log_info(f"  动作: {action}")
    log_info(f"  代理: {current_proxy_id}")
    log_info(f"  消息: {msg}")

    # 检查观察期状态
    is_in_obs = binding_mgr.is_in_observation(test_phone)
    log_info(f"\n✓ 是否在观察期: {is_in_obs}")

    if is_in_obs:
        binding_info = binding_mgr.get_binding(test_phone)
        log_info(f"✓ 主代理: {binding_info['proxy_id']}")
        log_info(f"✓ 临时代理: {binding_info['temp_proxy_id']}")

    # 观察期内连续失败
    log_info("\n" + "="*60)
    log_info("测试2：观察期内连续失败10次 → 立即永久切换")
    log_info("="*60)

    for i in range(1, 12):  # 多测试2次，确保触发
        should_retry, current_proxy_id, action, msg = handler.handle_request_failure(
            test_phone, current_proxy_id, "Connection timeout"
        )
        log_info(f"  第{i}次: {action}, 代理={current_proxy_id}")

        if action == "permanent_switch_immediate":
            log_info(f"\n  ✓✓✓ 触发立即永久切换！")
            break

    # 查看最终绑定
    log_info("\n最终绑定:")
    final_binding = binding_mgr.get_binding(test_phone)
    log_info(f"  主代理: {final_binding['proxy_id']}")
    log_info(f"  观察期: {final_binding['in_observation']}")

    log_info("\n✓ 测试完成")

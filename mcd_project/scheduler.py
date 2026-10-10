#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度器
负责执行代理池的定时检查和清理任务
"""
import os.path
import traceback

import schedule
import time
import logging
from datetime import datetime
from proxy_manager import ProxyManager
from config import REDIS_SET_URL, LOG_DIR
from utils import log_info


def full_health_check():
    """全量健康检查任务（12小时一次）"""
    try:
        from proxy_pool import ProxyPool

        pool = ProxyPool()
        result = pool.full_health_check()

        log_info(f"✅ 全量健康检查完成")
        log_info(f"   - 总代理数: {result['total']}")
        log_info(f"   - 健康代理: {result['healthy']}")
        log_info(f"   - 故障代理: {result['failed']}")

    except Exception as e:
        log_info(f"❌ 全量健康检查失败: {str(traceback.format_exc())}")


def observation_check():
    """观察期检查任务（5分钟一次）"""
    try:
        log_info("-" * 60)
        log_info("开始执行观察期检查")

        from account_proxy_binding import AccountProxyBinding
        from proxy_pool import ProxyPool

        pool = ProxyPool()
        binding_mgr = AccountProxyBinding(proxy_pool=pool)

        # 获取所有观察期账号
        observation_accounts = binding_mgr.get_accounts_in_observation()

        if not observation_accounts:
            log_info("✓ 当前没有观察期账号")
            return

        log_info(f"发现 {len(observation_accounts)} 个观察期账号")

        recovered_count = 0
        switched_count = 0

        for phone in observation_accounts:
            try:
                # 检查观察期状态
                status = binding_mgr.check_observation_status(phone)

                if status == 'recovered':
                    log_info(f"   ✓ {phone}: 恢复主代理")
                    recovered_count += 1
                elif status == 'permanent_switch':
                    log_info(f"   → {phone}: 永久切换代理")
                    switched_count += 1
                else:
                    log_info(f"   ⏳ {phone}: 观察期继续")

            except Exception as e:
                log_info(f"   ✗ {phone}: 检查失败 - {str(e)}")

        log_info(f"✅ 观察期检查完成")
        log_info(f"   - 观察期账号数: {len(observation_accounts)}")
        log_info(f"   - 恢复主代理: {recovered_count}")
        log_info(f"   - 永久切换: {switched_count}")

    except Exception as e:
        log_info(f"❌ 观察期检查失败: {str(traceback.format_exc())}")


def cleanup_failure_records():
    """清理过期的失败计数记录（每天一次）"""
    try:
        log_info("-" * 60)
        log_info("开始清理过期失败记录")

        from account_proxy_binding import AccountProxyBinding
        from proxy_pool import ProxyPool
        import time

        pool = ProxyPool()
        binding_mgr = AccountProxyBinding(proxy_pool=pool)

        # 获取所有绑定关系
        all_bindings = binding_mgr.get_all_bindings()

        cleaned_count = 0
        now = time.time()
        failure_window = 10 * 60  # 10分钟

        for binding in all_bindings:
            phone = binding.get('phone')
            fail_window_start = binding.get('fail_window_start')

            if not fail_window_start:
                continue

            # 解析时间戳
            from datetime import datetime
            try:
                fail_time = datetime.fromisoformat(fail_window_start).timestamp()

                # 如果失败窗口已过期（超过10分钟）
                if now - fail_time > failure_window:
                    total_fail_count = binding.get('total_fail_count', 0)

                    if total_fail_count > 0:
                        # 重置失败计数
                        binding['total_fail_count'] = 0
                        binding['fail_window_start'] = datetime.now().isoformat()
                        binding['last_fail_time'] = None
                        binding_mgr.save_binding(phone, binding)

                        log_info(f"   ✓ 清理 {phone} 的失败记录 (原有 {total_fail_count} 次失败)")
                        cleaned_count += 1

            except Exception as e:
                log_info(f"   ✗ 清理 {phone} 失败: {str(e)}")

        log_info(f"✅ 失败记录清理完成")
        log_info(f"   - 检查的账号数: {len(all_bindings)}")
        log_info(f"   - 清理的记录数: {cleaned_count}")

    except Exception as e:
        log_info(f"❌ 失败记录清理失败: {str(traceback.format_exc())}")


def sync_proxy_binding_count():
    """同步代理绑定计数（每3分钟执行）"""
    try:
        from proxy_pool import ProxyPool
        from account_proxy_binding import AccountProxyBinding

        pool = ProxyPool()
        binding_mgr = AccountProxyBinding(proxy_pool=pool)

        # 统计实际绑定数
        all_bindings = binding_mgr.get_all_bindings()
        actual_counts = {}

        for binding in all_bindings:
            proxy_id = binding.get('proxy_id')
            if proxy_id:
                actual_counts[proxy_id] = actual_counts.get(proxy_id, 0) + 1

        # 更新所有代理的 bound_accounts
        all_proxies = pool.get_all_proxies()

        for proxy in all_proxies:
            proxy_id = proxy['id']
            actual_count = actual_counts.get(proxy_id, 0)
            cached_count = proxy.get('bound_accounts', 0)

            # 只有不一致时才更新
            if cached_count != actual_count:
                pool.update_proxy(proxy_id, {'bound_accounts': actual_count})
                log_info(f"✓ 同步代理 {proxy_id} 绑定数: {cached_count} → {actual_count}")

        log_info("✓ 代理绑定计数同步完成")

    except Exception as e:
        log_info(f"❌ 同步代理绑定计数失败: {str(traceback.format_exc())}")


def run_scheduler():
    """运行调度器"""
    log_info("=" * 60)
    log_info("🚀 代理池定时任务调度器启动")
    log_info("=" * 60)
    log_info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_info("")
    log_info("任务配置:")
    log_info("  1. 全量健康检查: 每12小时")
    log_info("  2. 观察期检查:   每5分钟")
    log_info("  3. 失败记录清理: 每天凌晨3点")
    log_info("=" * 60)

    # 配置定时任务
    schedule.every(5).minutes.do(observation_check)
    schedule.every(3).minutes.do(sync_proxy_binding_count)
    schedule.every().day.at("01:00").do(full_health_check)
    schedule.every().day.at("03:00").do(cleanup_failure_records)

    # 启动时立即执行一次观察期检查
    log_info("⏰ 执行启动时的初始检查...")
    observation_check()

    # 主循环
    log_info("\n⏰ 调度器运行中，等待任务触发...")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # 每30秒检查一次待执行任务

    except KeyboardInterrupt:
        log_info("\n")
        log_info("=" * 60)
        log_info("⚠️  收到停止信号，调度器正在关闭...")
        log_info("=" * 60)
    except Exception as e:
        log_info(f"❌ 调度器异常退出: {str(e)}")


if __name__ == '__main__':
    run_scheduler()
    # cleanup_failure_records()


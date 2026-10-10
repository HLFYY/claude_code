#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度器
负责执行代理池的定时检查和清理任务
"""
import os.path

import schedule
import time
import logging
from datetime import datetime
from proxy_manager import ProxyManager
from config import REDIS_SET_URL, LOG_DIR

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'scheduler.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def full_health_check():
    """全量健康检查任务（12小时一次）"""
    try:
        logger.info("=" * 60)
        logger.info("开始执行全量健康检查")
        logger.info("=" * 60)

        proxy_manager = ProxyManager(**REDIS_SET_URL)
        result = proxy_manager.check_all_proxies()

        logger.info(f"✅ 全量健康检查完成")
        logger.info(f"   - 总代理数: {result['total']}")
        logger.info(f"   - 检查成功: {result['success']}")
        logger.info(f"   - 检查失败: {result['failed']}")
        logger.info(f"   - 移除代理: {result['removed']}")

    except Exception as e:
        logger.error(f"❌ 全量健康检查失败: {str(e)}", exc_info=True)


def observation_check():
    """观察期检查任务（5分钟一次）"""
    try:
        logger.info("-" * 60)
        logger.info("开始执行观察期检查")

        proxy_manager = ProxyManager(**REDIS_SET_URL)
        result = proxy_manager.check_observation_proxies()

        logger.info(f"✅ 观察期检查完成")
        logger.info(f"   - 观察期代理数: {result['observation_count']}")
        logger.info(f"   - 检查成功: {result['success']}")
        logger.info(f"   - 检查失败: {result['failed']}")
        logger.info(f"   - 移除代理: {result['removed']}")

    except Exception as e:
        logger.error(f"❌ 观察期检查失败: {str(e)}", exc_info=True)


def cleanup_failure_records():
    """清理过期的失败计数记录（每天一次）"""
    try:
        logger.info("-" * 60)
        logger.info("开始清理过期失败记录")

        proxy_manager = ProxyManager(**REDIS_SET_URL)
        result = proxy_manager.cleanup_old_failure_records()

        logger.info(f"✅ 失败记录清理完成")
        logger.info(f"   - 清理的代理数: {result['cleaned_proxies']}")
        logger.info(f"   - 清理的记录数: {result['total_cleaned']}")

    except Exception as e:
        logger.error(f"❌ 失败记录清理失败: {str(e)}", exc_info=True)


def run_scheduler():
    """运行调度器"""
    logger.info("=" * 60)
    logger.info("🚀 代理池定时任务调度器启动")
    logger.info("=" * 60)
    logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("任务配置:")
    logger.info("  1. 全量健康检查: 每12小时")
    logger.info("  2. 观察期检查:   每5分钟")
    logger.info("  3. 失败记录清理: 每天凌晨3点")
    logger.info("=" * 60)

    # 配置定时任务
    schedule.every(12).hours.do(full_health_check)
    schedule.every(5).minutes.do(observation_check)
    schedule.every().day.at("03:00").do(cleanup_failure_records)

    # 启动时立即执行一次观察期检查
    logger.info("⏰ 执行启动时的初始检查...")
    observation_check()

    # 主循环
    logger.info("\n⏰ 调度器运行中，等待任务触发...")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # 每30秒检查一次待执行任务

    except KeyboardInterrupt:
        logger.info("\n")
        logger.info("=" * 60)
        logger.info("⚠️  收到停止信号，调度器正在关闭...")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"❌ 调度器异常退出: {str(e)}", exc_info=True)


if __name__ == '__main__':
    run_scheduler()

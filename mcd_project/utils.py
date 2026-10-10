#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公共工具函数模块
"""
import time


def log_info(content):
    """
    统一的日志输出函数
    使用 flush=True 确保输出立即写入文件（不受缓冲影响）

    Args:
        content: 日志内容
    """
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")}, {content}', flush=True)

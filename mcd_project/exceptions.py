#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳登录相关异常类
"""


class LoginError(Exception):
    """登录相关基础异常"""
    pass


class NetworkError(LoginError):
    """网络连接问题（可重试）"""
    def __init__(self, message, original_error=None, retry_after=None):
        super().__init__(message)
        self.original_error = original_error
        self.retry_after = retry_after  # 建议重试间隔（秒）


class AuthenticationError(LoginError):
    """认证失败（凭证失效，不可重试）"""
    pass


class ServerError(LoginError):
    """服务端错误（可重试）"""
    def __init__(self, message, status_code=None, retry_after=None):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class ValidationError(LoginError):
    """参数校验错误（不可重试）"""
    pass

"""
统一的运行日志输出：把散落在各处的裸 print() 运行日志（等待提示/重试提示/状态
变化这类"发生在什么时间、哪个平台"很重要的信息）统一成一个固定格式：

    {时间 YYYY-MM-DD HH:MM:SS} {平台} {日志内容}

比如：
    2026-07-30 18:28:00 pkulaw 验证码已发送到 xxx@gmail.com，等待用户输入

跟 core/request_logger.py 是两回事：那个是落盘的结构化请求流水（JSONL文件，
给统计/审计用），这个只是终端输出本身要不要带时间戳/平台前缀的格式问题，不
落盘、不做任何聚合。像"步骤标题""最终结果展示"这类本来就是给人看的报告格式
（表格/分隔线/键值对），不属于这里说的"运行日志"，不需要经过这个函数。
"""
from __future__ import annotations

from datetime import datetime


def log(platform: str, message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} {platform} {message}")

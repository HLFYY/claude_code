"""
通用的采集文档存储：每个平台一个 MongoDB collection（core/mongo_client.py），
对文档的 _id 该长什么样、携带哪些字段没有任何主张——那是平台特定的事情
（比如 wkinfo 把 _id 拼成 "{category}_{docId}"，见
platforms/wkinfo/document_store.py）。这个模块只负责针对平台给定的
collection 做"缓存命中就返回/否则 upsert"的机制本身。
"""
from __future__ import annotations

import time
from datetime import datetime

from .mongo_client import get_collection


def get_cached(platform: str, doc_id: str) -> dict | None:
    return get_collection(platform).find_one({"_id": doc_id})


def save(platform: str, doc_id: str, fields: dict) -> dict:
    """fields 会被平铺到存储文档的顶层字段里，另外再加上 cctime
    （10位unix秒级时间戳）和 crawl_time（人类可读格式）。"""
    now = datetime.now()
    record = dict(fields)
    record["_id"] = doc_id
    record["cctime"] = int(time.time())
    record["crawl_time"] = now.strftime("%Y-%m-%d %H:%M:%S")
    get_collection(platform).replace_one({"_id": doc_id}, record, upsert=True)
    return record

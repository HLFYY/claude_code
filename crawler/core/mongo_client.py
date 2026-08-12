"""
单例共享 MongoDB 连接。同一个库（config.MONGO_DB_NAME）下每个平台一个
collection —— get_collection("wkinfo") 拿 wkinfo 的文档，
get_collection("someotherplatform") 拿另一个平台的，互不混杂。
"""
from __future__ import annotations

import pymongo
from pymongo.collection import Collection
from pymongo.database import Database

from . import config

_client: pymongo.MongoClient | None = None


def get_db() -> Database:
    global _client
    if _client is None:
        _client = pymongo.MongoClient(config.MONGO_URI)
    return _client[config.MONGO_DB_NAME]


def get_collection(platform: str) -> Collection:
    return get_db()[platform]

"""Single shared MongoDB connection, built from config.py."""
from __future__ import annotations

import pymongo
from pymongo.database import Database

import config

_client: pymongo.MongoClient | None = None


def get_db() -> Database:
    global _client
    if _client is None:
        _client = pymongo.MongoClient(config.MONGO_URI)
    return _client[config.MONGO_DB_NAME]

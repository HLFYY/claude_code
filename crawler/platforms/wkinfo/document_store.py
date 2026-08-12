"""
wkinfo 自己的文档 _id 命名规则，包在 core/document_store.py 通用的按平台分
collection 存储之上：_id = "{category}_{docId}"，category 是 indexId 去掉
"law." 前缀（比如 "law.legislation" -> "legislation"）。通用层不关心也不
需要知道这个命名规则——它只是按拿到的 _id 做 upsert。
"""
from __future__ import annotations

from core import document_store as _store

from . import config


def category_from_index_id(index_id: str) -> str:
    """"law.legislation" -> "legislation"（去掉 "law." 前缀）"""
    return index_id.split(".", 1)[-1]


def make_id(index_id: str, doc_id: str) -> str:
    return f"{category_from_index_id(index_id)}_{doc_id}"


def get_cached(index_id: str, doc_id: str) -> dict | None:
    return _store.get_cached(config.PLATFORM, make_id(index_id, doc_id))


def save(index_id: str, doc_id: str, raw_content: dict) -> dict:
    fields = dict(raw_content)
    fields["category"] = category_from_index_id(index_id)
    fields["docId"] = doc_id
    return _store.save(config.PLATFORM, make_id(index_id, doc_id), fields)

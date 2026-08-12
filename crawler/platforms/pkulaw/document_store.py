"""
pkulaw 自己的文档 _id 命名规则，包在 core/document_store.py 通用的按平台分 collection
存储之上：_id = "{category}_{doc_id}"，category/doc_id 都是从文章 URL 路径里来的
（比如 "https://www.pkulaw.com/qikan/5c6347f6bc4c4866bdca50e0aff747f0bdfb.html"
-> category="qikan", doc_id="5c6347f6bc4c4866bdca50e0aff747f0bdfb"）。跟 wkinfo
平台的 document_store.py 是同一个思路。
"""
from __future__ import annotations

import re

from core import document_store as _store

from . import config

_URL_RE = re.compile(r"^https?://www\.pkulaw\.com/([^/]+)/([^/.]+)\.html")


def parse_url(url: str) -> tuple[str, str]:
    """从完整文章 URL 解析出 (category, doc_id)。"""
    m = _URL_RE.match(url)
    if not m:
        raise ValueError(f"document_store.parse_url: 无法从 {url!r} 解析出 category/doc_id")
    return m.group(1), m.group(2)


def build_url(category: str, doc_id: str) -> str:
    return f"{config.WWW_BASE}/{category}/{doc_id}.html"


def make_id(category: str, doc_id: str) -> str:
    return f"{category}_{doc_id}"


def get_cached(category: str, doc_id: str) -> dict | None:
    return _store.get_cached(config.PLATFORM, make_id(category, doc_id))


def save(category: str, doc_id: str, fields: dict) -> dict:
    record = dict(fields)
    record["category"] = category
    record["docId"] = doc_id
    return _store.save(config.PLATFORM, make_id(category, doc_id), record)

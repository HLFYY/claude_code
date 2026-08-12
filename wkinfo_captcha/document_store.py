"""
Collected document storage in MongoDB (separate from Redis -- Redis is live
operational state for the account pool, this is the actual crawled content).

One document per (category, docId), collection config.MONGO_DOCUMENTS_COLLECTION:
    _id: "{category}_{docId}"          e.g. "legislation_MTAxMDA1MDY0MzE="
    ...<every field from the raw /csi/document/{docId}/html response, spread
        at the top level as-is>
    category: "legislation"             (redundant with _id, kept for easy querying)
    docId: "MTAxMDA1MDY0MzE="           (ditto)
    cctime: 1785318300                  10-digit unix seconds
    crawl_time: "2026-07-29 10:05:00"   same instant, human readable

get_cached() is the cache-check: if a document's already stored, callers
should use it instead of spending detail-view quota on a re-fetch.
"""
from __future__ import annotations

import time
from datetime import datetime

from mongo_client import get_db
import config


def category_from_index_id(index_id: str) -> str:
    """"law.legislation" -> "legislation" """
    return index_id.split(".", 1)[-1]


def make_id(index_id: str, doc_id: str) -> str:
    return f"{category_from_index_id(index_id)}_{doc_id}"


def _collection():
    return get_db()[config.MONGO_DOCUMENTS_COLLECTION]


def get_cached(index_id: str, doc_id: str) -> dict | None:
    return _collection().find_one({"_id": make_id(index_id, doc_id)})


def save(index_id: str, doc_id: str, raw_content: dict) -> dict:
    now = datetime.now()
    record = dict(raw_content)
    record["_id"] = make_id(index_id, doc_id)
    record["category"] = category_from_index_id(index_id)
    record["docId"] = doc_id
    record["cctime"] = int(time.time())
    record["crawl_time"] = now.strftime("%Y-%m-%d %H:%M:%S")
    _collection().replace_one({"_id": record["_id"]}, record, upsert=True)
    return record

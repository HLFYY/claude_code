"""
Redis / MongoDB 数据迁移脚本，从 crawler/ 目录直接跑：

    cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
    /Users/houjie/venv/python3-forcrawl/bin/python migrate_data.py <子命令> ...

"源"永远是当前机器（读 core/config.py 解析出来的连接信息——那边是按 hostname
判断本地/线上，不是环境变量；在本地 Mac 上跑就是本地库，在线上服务器上跑就是
线上库，跟其它爬虫代码用的是同一份连接配置）。三个子命令对应两种迁移方式：

  1. 导出文件 -> 上传服务器 -> 导入数据库（两步，分别在源/目标机器上跑）：
       在源机器：  python migrate_data.py export --target both --file /tmp/dump
       （生成 /tmp/dump.redis.jsonl 和/或 /tmp/dump.mongo.jsonl，用 scp/rsync 传到
       目标机器）
       在目标机器：python migrate_data.py import --target both --file /tmp/dump
       （目标机器自己的 core/config.py 环境变量就是这次的写入目标）

  2. 直接迁移（一步，在能同时连到源和目标的机器上跑，不落文件）：
       python migrate_data.py direct --target both \
           --dest-redis-host 1.2.3.4 --dest-redis-password xxx \
           --dest-mongo-uri "mongodb://user:pass@1.2.3.4:27017"

--target 三选一：redis / mongo / both。

Redis 用 DUMP/RESTORE（Redis 自己的序列化格式）逐个 key 迁移，而不是自己按类型
分别处理 string/hash/zset/set/list——这样不管业务代码用了哪种数据结构、以后新加
了什么类型，这个脚本都不需要跟着改，并且能保留每个 key 的 TTL。默认只处理
`config.KEY_PREFIX`（"crawler:"）前缀下的 key，避免动到同一个 Redis 实例上其它
用途的数据（见 core/config.py 里 KEY_PREFIX 的说明）。

MongoDB 默认迁移 config.MONGO_DB_NAME 这个库下的所有 collection（也可以用
--collections 指定其中几个），按 `_id` upsert，可以安全地重复跑。

默认都不覆盖目标那边已经存在的 key/文档（迁移到一个可能已经有数据的目标时，
不会覆盖数据），传 --overwrite 才会覆盖。
"""
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

import pymongo
import redis
from bson import json_util

from core import config as core_config
from core.mongo_client import get_db as local_mongo_db

PROGRESS_EVERY = 1000


def _local_raw_redis() -> redis.Redis:
    """跟 core/redis_client.py 用的是同一份连接配置，但 decode_responses=False——
    DUMP/RESTORE 传的是 Redis 自己的二进制序列化格式，用 decode_responses=True
    的连接会把这段字节按字符串解码，破坏数据。"""
    return redis.Redis(
        host=core_config.REDIS_HOST, port=core_config.REDIS_PORT,
        db=core_config.REDIS_DB, password=core_config.REDIS_PASSWORD,
        decode_responses=False,
    )


def _dest_raw_redis(args) -> redis.Redis:
    if not args.dest_redis_host:
        raise SystemExit("--target 包含 redis 时必须传 --dest-redis-host")
    return redis.Redis(
        host=args.dest_redis_host, port=args.dest_redis_port,
        db=args.dest_redis_db, password=args.dest_redis_password,
        decode_responses=False,
    )


def _dest_mongo_db(args):
    if not args.dest_mongo_uri:
        raise SystemExit("--target 包含 mongo 时必须传 --dest-mongo-uri")
    db_name = args.dest_mongo_db or core_config.MONGO_DB_NAME
    return pymongo.MongoClient(args.dest_mongo_uri)[db_name]


# ─────────────────────────────────────────────
#  Redis：export / import / direct
# ─────────────────────────────────────────────

def _redis_pattern(args) -> bytes:
    pattern = args.pattern or (core_config.KEY_PREFIX + "*")
    return pattern.encode("utf-8")


def redis_export(client: redis.Redis, pattern: bytes, file_path: Path) -> int:
    count = 0
    with open(file_path, "w", encoding="utf-8") as f:
        for key in client.scan_iter(match=pattern, count=500):
            dump = client.dump(key)
            if dump is None:
                continue  # 扫描到 key 之后、真正 DUMP 之前被删了/过期了
            pttl = client.pttl(key)
            ttl_ms = pttl if pttl and pttl > 0 else 0
            record = {
                "key": key.decode("utf-8"),
                "ttl_ms": ttl_ms,
                "dump": base64.b64encode(dump).decode("ascii"),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % PROGRESS_EVERY == 0:
                print(f"  ...已导出 {count} 个 key")
    return count


def redis_import(client: redis.Redis, file_path: Path, overwrite: bool) -> tuple[int, int]:
    imported = skipped = 0
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            key = record["key"].encode("utf-8")
            if not overwrite and client.exists(key):
                skipped += 1
                continue
            dump = base64.b64decode(record["dump"])
            client.restore(key, record["ttl_ms"], dump, replace=overwrite)
            imported += 1
            if imported % PROGRESS_EVERY == 0:
                print(f"  ...已导入 {imported} 个 key")
    return imported, skipped


def redis_direct(src: redis.Redis, dst: redis.Redis, pattern: bytes, overwrite: bool) -> tuple[int, int]:
    migrated = skipped = 0
    for key in src.scan_iter(match=pattern, count=500):
        if not overwrite and dst.exists(key):
            skipped += 1
            continue
        dump = src.dump(key)
        if dump is None:
            continue
        pttl = src.pttl(key)
        ttl_ms = pttl if pttl and pttl > 0 else 0
        dst.restore(key, ttl_ms, dump, replace=overwrite)
        migrated += 1
        if migrated % PROGRESS_EVERY == 0:
            print(f"  ...已迁移 {migrated} 个 key")
    return migrated, skipped


# ─────────────────────────────────────────────
#  MongoDB：export / import / direct
# ─────────────────────────────────────────────

def _mongo_collections(db, collections_arg: str | None) -> list[str]:
    if collections_arg:
        return [c.strip() for c in collections_arg.split(",") if c.strip()]
    return db.list_collection_names()


def mongo_export(db, collections: list[str], file_path: Path) -> dict:
    counts: dict[str, int] = {}
    with open(file_path, "w", encoding="utf-8") as f:
        for coll_name in collections:
            n = 0
            for doc in db[coll_name].find({}):
                line = json_util.dumps({"_collection": coll_name, "doc": doc}, ensure_ascii=False)
                f.write(line + "\n")
                n += 1
                if n % PROGRESS_EVERY == 0:
                    print(f"  ...{coll_name} 已导出 {n} 条")
            counts[coll_name] = n
    return counts


def _upsert_op(doc: dict, overwrite: bool):
    if overwrite:
        return pymongo.ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
    # 目标已经有这条就不动它，只在缺失时插入——跟 Redis 那边默认不覆盖是同一个原则。
    return pymongo.UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True)


def mongo_import(db, file_path: Path, overwrite: bool) -> dict:
    counts: dict[str, int] = {}
    batches: dict[str, list] = {}

    def flush(coll_name: str):
        ops = batches.pop(coll_name, [])
        if not ops:
            return
        db[coll_name].bulk_write(ops, ordered=False)
        counts[coll_name] = counts.get(coll_name, 0) + len(ops)

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json_util.loads(line)
            coll_name = record["_collection"]
            batch = batches.setdefault(coll_name, [])
            batch.append(_upsert_op(record["doc"], overwrite))
            if len(batch) >= PROGRESS_EVERY:
                flush(coll_name)
                print(f"  ...{coll_name} 已导入 {counts.get(coll_name, 0)} 条")
    for coll_name in list(batches):
        flush(coll_name)
    return counts


def mongo_direct(src_db, dst_db, collections: list[str], overwrite: bool) -> dict:
    counts: dict[str, int] = {}
    for coll_name in collections:
        ops = []
        n = 0
        for doc in src_db[coll_name].find({}):
            ops.append(_upsert_op(doc, overwrite))
            if len(ops) >= PROGRESS_EVERY:
                dst_db[coll_name].bulk_write(ops, ordered=False)
                n += len(ops)
                ops = []
                print(f"  ...{coll_name} 已迁移 {n} 条")
        if ops:
            dst_db[coll_name].bulk_write(ops, ordered=False)
            n += len(ops)
        counts[coll_name] = n
    return counts


# ─────────────────────────────────────────────
#  子命令
# ─────────────────────────────────────────────

def cmd_export(args) -> None:
    if args.target in ("redis", "both"):
        file_path = Path(f"{args.file}.redis.jsonl")
        print(f"[redis] 从本机 {core_config.REDIS_HOST}:{core_config.REDIS_PORT}/{core_config.REDIS_DB} "
              f"导出匹配 {args.pattern or core_config.KEY_PREFIX + '*'!r} 的 key 到 {file_path}")
        count = redis_export(_local_raw_redis(), _redis_pattern(args), file_path)
        print(f"[redis] 完成，共导出 {count} 个 key")

    if args.target in ("mongo", "both"):
        file_path = Path(f"{args.file}.mongo.jsonl")
        db = local_mongo_db()
        collections = _mongo_collections(db, args.collections)
        print(f"[mongo] 从本机 {core_config.MONGO_URI}/{core_config.MONGO_DB_NAME} 导出 "
              f"collection {collections} 到 {file_path}")
        counts = mongo_export(db, collections, file_path)
        print(f"[mongo] 完成，各 collection 导出条数: {counts}")


def cmd_import(args) -> None:
    if args.target in ("redis", "both"):
        file_path = Path(f"{args.file}.redis.jsonl")
        if not file_path.exists():
            raise SystemExit(f"找不到 {file_path}，先在源机器上跑 export 并把文件传过来")
        print(f"[redis] 导入 {file_path} 到本机 {core_config.REDIS_HOST}:{core_config.REDIS_PORT}"
              f"/{core_config.REDIS_DB}（overwrite={args.overwrite}）")
        imported, skipped = redis_import(_local_raw_redis(), file_path, args.overwrite)
        print(f"[redis] 完成，导入 {imported} 个 key，跳过已存在 {skipped} 个")

    if args.target in ("mongo", "both"):
        file_path = Path(f"{args.file}.mongo.jsonl")
        if not file_path.exists():
            raise SystemExit(f"找不到 {file_path}，先在源机器上跑 export 并把文件传过来")
        print(f"[mongo] 导入 {file_path} 到本机 {core_config.MONGO_URI}/{core_config.MONGO_DB_NAME}"
              f"（overwrite={args.overwrite}）")
        counts = mongo_import(local_mongo_db(), file_path, args.overwrite)
        print(f"[mongo] 完成，各 collection 导入条数: {counts}")


def cmd_direct(args) -> None:
    if args.target in ("redis", "both"):
        dst = _dest_raw_redis(args)
        print(f"[redis] 从本机 {core_config.REDIS_HOST}:{core_config.REDIS_PORT}/{core_config.REDIS_DB} "
              f"直接迁移到 {args.dest_redis_host}:{args.dest_redis_port}/{args.dest_redis_db}"
              f"（overwrite={args.overwrite}）")
        migrated, skipped = redis_direct(_local_raw_redis(), dst, _redis_pattern(args), args.overwrite)
        print(f"[redis] 完成，迁移 {migrated} 个 key，跳过已存在 {skipped} 个")

    if args.target in ("mongo", "both"):
        src_db = local_mongo_db()
        dst_db = _dest_mongo_db(args)
        collections = _mongo_collections(src_db, args.collections)
        print(f"[mongo] 从本机 {core_config.MONGO_URI}/{core_config.MONGO_DB_NAME} 直接迁移 collection "
              f"{collections} 到 {args.dest_mongo_uri}/{dst_db.name}（overwrite={args.overwrite}）")
        counts = mongo_direct(src_db, dst_db, collections, args.overwrite)
        print(f"[mongo] 完成，各 collection 迁移条数: {counts}")


def sync_local_to_online(target: str = "both", overwrite: bool = False,
                          pattern: str | None = None, collections: str | None = None) -> None:
    """本地 -> core.config.ONLINE_DB 那台线上库，直接同步，不落文件，不用传
    任何连接参数——线上连接信息就是 core/config.py 里 ONLINE_DB 那份，跟
    `direct` 子命令的区别是不用在命令行上敲一堆 --dest-* 参数。这个函数不接
    argparse，是给手动调用用的：

        cd crawler
        /Users/houjie/venv/python3-forcrawl/bin/python3 -c \
            "import migrate_data; migrate_data.sync_local_to_online()"

    只能在本地机器上跑（core.config.ONLINE 为 False 的机器）——如果当前机器
    自己就是线上那台，本地库和 core.config.ONLINE_DB 会是同一个库，同步没有
    意义，直接报错。

    target: "redis" / "mongo" / "both"，默认 both。
    overwrite: 默认 False，线上已经存在的 key/文档不动；传 True 才覆盖，
    跟 export/import/direct 三个子命令是同一个默认值。
    """
    if core_config.ONLINE:
        raise RuntimeError(
            "当前机器已经识别成线上环境了（core.config.ONLINE=True），"
            "sync_local_to_online() 是给本地机器同步到线上用的，不能在线上机器上跑"
        )

    if target in ("redis", "both"):
        dst = redis.Redis(
            host=core_config.ONLINE_DB["redis_host"], port=core_config.ONLINE_DB["redis_port"],
            db=core_config.ONLINE_DB["redis_db"], password=core_config.ONLINE_DB["redis_password"],
            decode_responses=False,
        )
        redis_pattern = (pattern or core_config.KEY_PREFIX + "*").encode("utf-8")
        print(f"[redis] 本地 {core_config.REDIS_HOST}:{core_config.REDIS_PORT}/{core_config.REDIS_DB} -> "
              f"线上 {core_config.ONLINE_DB['redis_host']}:{core_config.ONLINE_DB['redis_port']}"
              f"/{core_config.ONLINE_DB['redis_db']}（overwrite={overwrite}）")
        migrated, skipped = redis_direct(_local_raw_redis(), dst, redis_pattern, overwrite)
        print(f"[redis] 完成，迁移 {migrated} 个 key，跳过已存在 {skipped} 个")

    if target in ("mongo", "both"):
        src_db = local_mongo_db()
        dst_db = pymongo.MongoClient(core_config.ONLINE_DB["mongo_uri"])[core_config.ONLINE_DB["mongo_db"]]
        colls = _mongo_collections(src_db, collections)
        print(f"[mongo] 本地 {core_config.MONGO_URI}/{core_config.MONGO_DB_NAME} -> "
              f"线上 {core_config.ONLINE_DB['mongo_uri']}/{core_config.ONLINE_DB['mongo_db']} "
              f"collection {colls}（overwrite={overwrite}）")
        counts = mongo_direct(src_db, dst_db, colls, overwrite)
        print(f"[mongo] 完成，各 collection 迁移条数: {counts}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)

    def add_target(p):
        p.add_argument("--target", choices=["redis", "mongo", "both"], default="both",
                        help="迁移 redis / mongo / both，默认 both")

    p_export = sub.add_parser("export", help="从本机导出到文件")
    add_target(p_export)
    p_export.add_argument("--file", required=True,
                           help="导出文件的基础路径，实际会写 <file>.redis.jsonl / <file>.mongo.jsonl")
    p_export.add_argument("--pattern", default=None, help="Redis key 匹配模式，默认 core.config.KEY_PREFIX + '*'")
    p_export.add_argument("--collections", default=None, help="逗号分隔的 Mongo collection 名单，默认导出全部")
    p_export.set_defaults(func=cmd_export)

    p_import = sub.add_parser("import", help="把 export 导出的文件导入本机数据库")
    add_target(p_import)
    p_import.add_argument("--file", required=True, help="跟 export 时传的 --file 是同一个基础路径")
    p_import.add_argument("--overwrite", action="store_true", help="目标已存在的 key/文档也覆盖，默认跳过")
    p_import.set_defaults(func=cmd_import)

    p_direct = sub.add_parser("direct", help="从本机直接迁移到指定的目标 Redis/MongoDB，不落文件")
    add_target(p_direct)
    p_direct.add_argument("--pattern", default=None)
    p_direct.add_argument("--collections", default=None)
    p_direct.add_argument("--overwrite", action="store_true", help="目标已存在的 key/文档也覆盖，默认跳过")
    p_direct.add_argument("--dest-redis-host", default=None)
    p_direct.add_argument("--dest-redis-port", type=int, default=6379)
    p_direct.add_argument("--dest-redis-db", type=int, default=0)
    p_direct.add_argument("--dest-redis-password", default=None)
    p_direct.add_argument("--dest-mongo-uri", default=None)
    p_direct.add_argument("--dest-mongo-db", default=None, help="默认跟源用同一个库名（core.config.MONGO_DB_NAME）")
    p_direct.set_defaults(func=cmd_direct)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
    # sync_local_to_online()

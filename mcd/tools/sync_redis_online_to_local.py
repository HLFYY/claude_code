#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把线上 Redis 里 mcd* / mkt* 开头的 key 单向同步到本地 Redis(用于本地测试)。

流程:
  1) 先删本地 mcd* / mkt* 开头的 key;
  2) 再从线上【只读】扫出 mcd* / mkt*,按类型(string/hash/list/set/zset)复制到本地,保留 TTL。

⚠️ 方向写死:线上=源(只读,全程不发任何写命令)  本地=目标(可写)。
   禁止本地同步到线上:本脚本对线上连接只调用读命令;并断言目标必须是 127.0.0.1/localhost,
   源目标 host 不能相同,否则直接退出,杜绝误写线上。

用法: python3 sync_redis_online_to_local.py
"""
import sys
import redis

# ===== 写死配置 =====
ONLINE = dict(host='db.selfmediaai.cn', port=26379, db=0, password='123.456.',
              socket_connect_timeout=15, socket_timeout=30)   # 源:线上(只读)
LOCAL = dict(host='127.0.0.1', port=6379, db=0, password=None,
             socket_connect_timeout=5, socket_timeout=15)      # 目标:本地(可写)
PATTERNS = ['mcd*', 'mkt*']

# ===== 安全护栏:绝不写线上 =====
if LOCAL['host'] not in ('127.0.0.1', 'localhost', '::1'):
    sys.exit(f'[拒绝] 目标(可写)库 host={LOCAL["host"]} 不是本机,已中止,防止误写线上')
if (LOCAL['host'], LOCAL['port'], LOCAL['db']) == (ONLINE['host'], ONLINE['port'], ONLINE['db']):
    sys.exit('[拒绝] 源与目标是同一个库,已中止')


def scan_keys(r, pattern):
    """SCAN 扫出所有匹配 key(非阻塞,分批)。"""
    keys, cursor = [], 0
    while True:
        cursor, batch = r.scan(cursor=cursor, match=pattern, count=500)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def copy_key(src, dst, key):
    """按类型从 src 复制 key 到 dst,保留 TTL。只对 src 发读命令。"""
    ktype = src.type(key)
    ktype = ktype.decode() if isinstance(ktype, (bytes, bytearray)) else ktype
    if ktype == 'string':
        dst.set(key, src.get(key))
    elif ktype == 'hash':
        data = src.hgetall(key)             # {field: value}
        if data:
            pipe = dst.pipeline()
            for f, v in data.items():
                pipe.hset(key, f, v)        # 逐字段 3 参数式,兼容各版本 redis-py
            pipe.execute()
    elif ktype == 'list':
        vals = src.lrange(key, 0, -1)
        if vals:
            dst.rpush(key, *vals)
    elif ktype == 'set':
        vals = src.smembers(key)
        if vals:
            dst.sadd(key, *vals)
    elif ktype == 'zset':
        vals = src.zrange(key, 0, -1, withscores=True)
        if vals:
            dst.zadd(key, {m: s for m, s in vals})
    else:
        return False
    pttl = src.pttl(key)
    if pttl and pttl > 0:
        dst.pexpire(key, pttl)
    return True


def main():
    # decode_responses=False: 原样字节复制,二进制安全
    src = redis.Redis(decode_responses=False, **ONLINE)   # 线上:只读
    dst = redis.Redis(decode_responses=False, **LOCAL)    # 本地:可写
    try:
        src.ping()
    except Exception as e:
        sys.exit(f'[错误] 连不上线上 Redis {ONLINE["host"]}:{ONLINE["port"]}: {e}')
    try:
        dst.ping()
    except Exception as e:
        sys.exit(f'[错误] 连不上本地 Redis {LOCAL["host"]}:{LOCAL["port"]}(先启动本地 redis-server): {e}')

    print(f'源(只读) = {ONLINE["host"]}:{ONLINE["port"]}/{ONLINE["db"]}')
    print(f'目标(写) = {LOCAL["host"]}:{LOCAL["port"]}/{LOCAL["db"]}')

    # 1) 删本地 mcd*/mkt*
    print('\n== 1) 删除本地 mcd*/mkt* ==')
    for pat in PATTERNS:
        ks = scan_keys(dst, pat)
        for i in range(0, len(ks), 500):
            dst.delete(*ks[i:i + 500])
        print(f'  本地 {pat}: 删除 {len(ks)} 个')

    # 2) 线上 -> 本地 复制
    print('\n== 2) 线上 -> 本地 复制 mcd*/mkt* ==')
    total_ok, total_skip = 0, 0
    for pat in PATTERNS:
        ks = scan_keys(src, pat)
        ok = 0
        for k in ks:
            try:
                if copy_key(src, dst, k):
                    ok += 1
                else:
                    total_skip += 1
                    kn = k.decode(errors='replace') if isinstance(k, (bytes, bytearray)) else k
                    print(f'    [跳过未知类型] {kn}')
            except Exception as e:
                total_skip += 1
                kn = k.decode(errors='replace') if isinstance(k, (bytes, bytearray)) else k
                print(f'    [复制失败] {kn}: {e}')
        total_ok += ok
        print(f'  线上 {pat}: 扫到 {len(ks)} 个, 成功复制 {ok} 个')

    print(f'\n✓ 完成: 复制成功 {total_ok} 个, 跳过/失败 {total_skip} 个')


if __name__ == '__main__':
    main()

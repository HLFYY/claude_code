#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
device_pool_redis.py —— 数美设备身份池(Redis 版)
运行环境: /Users/houjie/venv/python3-forcrawl/bin/python

两个核心功能:
  1) 生成/补池(refill): 每 15 分钟查空闲数,低于 DEVICE_POOL_CONFIG['min_free'] 就补足差额。
     造设备要连真机(frida),所以补池进程跑在接着真机的本机;Redis 是共享的,别处只管领用。
  2) 领用(claim): 自动取【未使用且更早生成】的设备(FIFO),标记被哪个账号使用,取用不删除。
     同一账号可多次领(旧设备失效后换新),account 列表累积多个设备。

数据结构:
  - List  free_key              空闲设备 id 队列。RPUSH 入队、LPOP 领用 → 先生成先取。LLEN=空闲数。
  - Hash  info_key(id)          设备全部参数(唯一 id + android_id + props + smid + d + 时间 + status + account)
  - List  account_key(account)  该账号领过的设备 id(支持一账号多设备)
  - Set   index_key             已注册 smid,生成查重避免重复设备

命令:
    device_pool_redis.py refill            # 补一次池
    device_pool_redis.py serve             # 常驻,每 15 分钟补一次
    device_pool_redis.py claim <account>   # 领一个设备(返回 d)
    device_pool_redis.py release <id>      # 标记设备失效(不回池)
    device_pool_redis.py stats             # 空闲/总量
测试接本地 Redis: MCD_REDIS_HOST=127.0.0.1 python device_pool_redis.py ...
"""
import os
import random
import sys
import json
import time
import uuid

from config import make_redis, DEVICE_POOL_CONFIG as C, CODE_DIR

_r = None


def r():
    global _r
    if _r is None:
        _r = make_redis()
    return _r


def count_free():
    return int(r().llen(C['free_key']) or 0)


def store_entry(entry):
    """把一个设备入库:整条 JSON 存进单 Hash 的一个 field(id) + 入空闲队列尾 + 记 smid。"""
    rc = r()
    rc.hset(C['info_key'], entry['id'], json.dumps(entry, ensure_ascii=False))
    rc.rpush(C['free_key'], entry['id'])  # 入队尾 → LPOP 取队头=更早生成
    rc.sadd(C['index_key'], entry['smid'])


def _get_device(did):
    raw = r().hget(C['info_key'], did)
    return json.loads(raw) if raw else {}


def claim(account):
    """领一个未使用且更早生成的设备(FIFO),标记账号,返回设备 dict(含 d)。无空闲返回 None。"""
    did = r().lpop(C['free_key'])
    if not did:
        return None
    dev = _get_device(did)
    dev['status'] = 'assigned'
    dev['account'] = account
    dev['assigned_at'] = int(time.time())
    pipe = r().pipeline()
    pipe.hset(C['info_key'], did, json.dumps(dev, ensure_ascii=False))
    pipe.rpush(C['account_key'].format(account=account), did)
    pipe.execute()
    return dev


def release(did):
    """标记设备失效(被封等),不回空闲池。"""
    rc = r()
    dev = _get_device(did)
    if not dev:
        print("[!] 没找到设备", did); return
    dev['status'] = 'dead'
    rc.hset(C['info_key'], did, json.dumps(dev, ensure_ascii=False))
    print("设备 %s 已标记 dead" % did)


def refill_once():
    """空闲低于 min_free 则补足差额(单轮不超过 build_batch_cap)。
    build_mode(config): 'frida'=真机frida造 / 'local'=本地纯HTTP造(mcd_shumei_d,不要真机)。"""
    free = count_free()
    need = C['min_free'] - free
    if need <= 0:
        print("[refill] 空闲 %d ≥ 最小 %d,无需补" % (free, C['min_free']), flush=True)
        return 0
    need = min(need, C['build_batch_cap'])
    mode = C.get('build_mode', 'frida')
    print("[refill] 空闲 %d < 最小 %d,本轮造 %d 个 (mode=%s)" % (free, C['min_free'], need, mode), flush=True)

    # 延迟 import:local 走 mcd_shumei_d+注册接口(纯HTTP);frida 走 device_builder(真机)。领用端(claim)都不触发。
    if mode == 'local':
        from local_device_gen import register_local
    else:
        from device_builder import gen_profile, register_one

    made = 0
    for i in range(need):
        if mode == 'local':
            res = register_local()                     # 本地造:模板换身份字段→make_box→注册(走新短效代理)
            if not res:
                print("  [%d/%d] ✗ 本地造设备失败,跳过" % (i + 1, need), flush=True); continue
            d, smid = res['d'], res['smid']
            if r().sismember(C['index_key'], smid):
                print("     ⚠ smid 重复,跳过", flush=True); continue
            entry = {
                'id': uuid.uuid4().hex[:12], 'model': res['model'], 'android_id': res['android_id'],
                'token': res['token'], 'smid': smid, 'd': d, 'shortbox': res['shortbox'],
                'source': 'local', 'created': int(time.time()), 'status': 'free', 'account': '',
            }
        else:
            p = gen_profile()
            print("  [%d/%d] model=%s android_id=%s 注册中…" % (i + 1, need, p['_model'], p['android_id']), flush=True)
            d = register_one(p)
            if not d:
                print("     ✗ 注册超时,跳过", flush=True); continue
            smid = d[1:]
            if r().sismember(C['index_key'], smid):
                print("     ⚠ smid 重复,跳过", flush=True); continue
            entry = {
                'id': uuid.uuid4().hex[:12], 'model': p['_model'], 'android_id': p['android_id'],
                'token': p.get('token'), 'props': p['props'], 'smid': smid, 'd': d,
                'source': 'frida', 'created': int(time.time()), 'status': 'free', 'account': '',
            }
        store_entry(entry); made += 1
        print("     ✓ 入库 id=%s d=%s… (source=%s)" % (entry['id'], d[:22], entry['source']), flush=True)
        if mode != 'local':
            time.sleep(random.randint(60, 90))         # 真机造间隔防风控;本地各走不同代理,无需
    print("[refill] 本轮新增 %d,当前空闲 %d" % (made, count_free()), flush=True)
    return made


def serve():
    print("[serve] 设备池守护启动,每 %ds 补池一次" % C['check_interval'], flush=True)
    while True:
        try:
            refill_once()
        except Exception as e:
            print("[serve] refill 异常:", e, flush=True)
        time.sleep(C['check_interval'])


def stats():
    rc = r()
    free = count_free()
    total = rc.scard(C['index_key'])
    print("空闲 %d | 历史总量(smid数) %d | 最小阈值 %d" % (free, total, C['min_free']))


def test_sendcode(phone):
    """本地造一个 B 开头设备 → 发送验证码,验证 d 端到端有效性。
    注册与发码【共用同一个短效代理IP】(同一台设备同一网络,一致性更强)。"""
    from local_device_gen import register_local, new_short_proxy
    import mcd_api
    print("[test] 本地造 B 设备 + 发码验证 phone=%s" % phone, flush=True)

    proxies, pinfo = new_short_proxy()                  # 注册+发码共用这一个出口IP
    if not proxies:
        print("[test] ✗ 短效代理提取失败,重跑 test"); return
    print("[test] 本次代理(注册+发码共用): %s" % pinfo, flush=True)

    res = register_local(proxies=proxies, pinfo=pinfo)  # 注册用该代理
    print(res)
    if not res:
        print("[test] ✗ 造设备失败(该代理可能不可用,重跑 test)"); return
    print("[test] ✓ 造出设备 model=%s\n       d=%s" % (res['model'], res['d']), flush=True)

    mcd_api.set_active_d(res['d'])                       # 注入本地造的 d,build_headers 自动带 d 头
    ok, tid, amsg = mcd_api.activate_token(res['token'], proxies=proxies)      # 同代理
    print("[test] activate_token: ok=%s tid=%s %s" % (ok, tid, amsg), flush=True)
    if ok:
        mcd_api.set_active_tid(tid)
    ok2, msg2 = mcd_api.send_verification_code(phone, res['token'], proxies=proxies)  # 同代理
    print("[test] send_verification_code: ok=%s | %s" % (ok2, msg2), flush=True)
    print("[test] 判据: 已发送/420(滑块)/707053(IP) = d有效; 706070/账号安全问题 = d无效", flush=True)


def main():
    if len(sys.argv) < 2:
        print(__doc__); return 2
    c = sys.argv[1]
    if c == 'refill':
        refill_once()
    elif c == 'serve':
        serve()
    elif c == 'claim':
        dev = claim(sys.argv[2])
        if dev:
            print("账号 %s 领到设备 %s" % (sys.argv[2], dev['id']))
            print("d =", dev['d'])
        else:
            print("[!] 无空闲设备,请先 refill")
    elif c == 'release':
        release(sys.argv[2])
    elif c == 'stats':
        stats()
    elif c == 'test':
        test_sendcode(sys.argv[2] if len(sys.argv) > 2 else '13800138000')
    else:
        print("未知命令:", c); print(__doc__); return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())

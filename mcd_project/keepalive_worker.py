#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""keepalive_worker.py — 账号保活队列消费者（跑在 94，2026-11-25）

为什么改成队列
--------------
原来平台「立即执行」同步 HTTP 调 94 /api/maintenance/keepalive：
  - 前端 axios 全局 20s 超时 → 94 还在跑，前端已经报 500（假失败）
  - 平台写死 limit=15，商户 58 个号只能跑 15 个
现在：
  平台只 RPUSH 队列立刻返回 → 本进程单线程 BLPOP 串行跑全量
  → 结果写 mkt:{env}:{merchant}:keepalive:last（24h），后台弹窗只读那份快照。

链路
----
  平台 POST /admin/accounts/keepalive/run（或每天到点）
    → RPUSH mkt:{env}:platform:keepalive:queue  {task}
    → 本进程 BLPOP（单线程，串行）
    → keepalive.run_keepalive(...)  # 结果由它自己写 :keepalive:last
  平台 GET /admin/accounts/keepalive → 读 :keepalive:last 展示上次统计

★ 任务 JSON 是跨语言契约，与 Go 侧 internal/handler/admin_keepalive.go
  keepAliveQueueTask 一一对应，改字段必须两边同步：
    merchant_id / limit / min_interval_hours / do_launch / source / requested_at

启动（94，独立进程，不重启 mcd-server）：
  systemctl restart mcd-keepalive-worker.service
日志：/home/pi/work/logs/keepalive_worker.log
"""
import json
import os
import time

import keepalive as ka
from config import make_redis

ENV = os.getenv('MCD_REGISTER_ENV', 'prod')
QUEUE_KEY = f'mkt:{ENV}:platform:keepalive:queue'
IDLE_TIMEOUT = 30  # BLPOP 阻塞秒数
RETRY_SLEEP = 5

# socket_timeout 必须 > BLPOP 阻塞时长，否则空闲时每 25s 抛一次超时（dpool/proxy_selfcheck 同款坑）
REDIS = make_redis(socket_timeout=45)


def log(msg):
    print('[%s] %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg), flush=True)


def _write_error_last(merchant_id, message):
    """任务整体失败时也留一份结果，避免弹窗一直显示上一次的成功统计。"""
    payload = {
        'ran_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'merchant_id': merchant_id,
        'stats': {'total': 0, 'alive': 0, 'dead': 0, 'unknown': 0,
                  'launch_ok': 0, 'launch_total': 0},
        'details': [], 'errors': [str(message)[:200]], 'elapsed_ms': 0,
    }
    try:
        REDIS.set(ka.KEEPALIVE_LAST_KEY_TMPL.format(merchant_id=merchant_id or '*'),
                  json.dumps(payload, ensure_ascii=False), ex=ka.KEEPALIVE_LAST_TTL)
    except Exception as e:
        log('写失败结果到 Redis 也失败: %s' % e)


def run_task(task):
    mid = str(task.get('merchant_id') or '').strip()
    if not mid:
        log('任务缺 merchant_id，丢弃: %s' % task)
        return

    # ⚠️ 不能用 `or 20`：显式 0（手动全量、不去重）会被当成没传（2026-11-24 同款坑）
    raw_min = task.get('min_interval_hours')
    min_hours = 20.0 if raw_min is None else float(raw_min)
    try:
        limit = int(task.get('limit') or 0)
    except Exception:
        limit = 0
    do_launch = task.get('do_launch')
    do_launch = True if do_launch is None else bool(do_launch)
    source = str(task.get('source') or '?')

    log('■ 开始 mid=%s source=%s limit=%s min_interval=%s 请求于=%s'
        % (mid, source, limit, min_hours, task.get('requested_at') or '-'))
    t0 = time.time()
    try:
        # only_with_rt=True：只保活带 refreshToken 的号（与平台原默认一致）
        payload = ka.run_keepalive(merchant_id=mid, limit=limit,
                                   min_interval_hours=min_hours,
                                   do_launch=do_launch, only_with_rt=True,
                                   redis_client=REDIS)
    except Exception as e:
        log('■ 执行异常 mid=%s: %s' % (mid, e))
        _write_error_last(mid, e)
        return

    st = payload.get('stats') or {}
    log('■ 完成 mid=%s total=%d alive=%d dead=%d unknown=%d skipped=%d '
        '模拟启动 %d/%d 用时 %dms'
        % (mid, st.get('total', 0), st.get('alive', 0), st.get('dead', 0),
           st.get('unknown', 0), st.get('skipped', 0), st.get('launch_ok', 0),
           st.get('launch_total', 0), int((time.time() - t0) * 1000)))


def main():
    log('keepalive_worker 启动：队列=%s 环境=%s（单线程串行，不设 limit 上限）'
        % (QUEUE_KEY, ENV))
    while True:
        try:
            item = REDIS.blpop(QUEUE_KEY, timeout=IDLE_TIMEOUT)
        except Exception as e:
            # 不细分 redis.exceptions.*：不同 redis-py 版本的异常类不完全一致，
            # worker 是常驻进程，任何 Redis 抖动都统一sleep后重试，绝不退出。
            log('BLPOP 异常: %s，%ds 后重试' % (e, RETRY_SLEEP))
            time.sleep(RETRY_SLEEP)
            continue
        if not item:
            continue
        try:
            task = json.loads(item[1])
        except Exception as e:
            log('任务 JSON 解析失败: %s（%s）' % (str(item[1])[:200], e))
            continue
        try:
            run_task(task)
        except Exception as e:
            log('任务未捕获异常: %s' % e)
            _write_error_last(str(task.get('merchant_id') or ''), e)


if __name__ == '__main__':
    main()

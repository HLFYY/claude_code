#!/usr/bin/env python3
"""常驻 pulse WebSocket 心跳守护（2026-11-28 用户授权，实验性质）。

背景：真机 App 后台靠 wss://ws.mcd.cn/bff/pulse/ws 每 15s 心跳维持在线画像。
现有 pulse 是「会话绑定式」——领号建连、还号后 30s/180s 就断，号在池里
静置时没有任何心跳。本守护给指定的一批号常驻心跳，验证「补常驻心跳」是否
影响掉线率。**这是对照实验，不是已证实的解法**——真机静置数天不掉线，
说明心跳不是 sid 存活的必要条件；能否降低脚本号掉线率仍未知。

设计原则：
  1. 只读凭据、只读短效代理池；不写 Redis 任何业务 key、不改账号状态。
  2. 每个号取一次代理并固定（真机一次打开全程一个出口 IP）；代理为短效池，
     IP 约 3 分钟 TTL，但 WebSocket 连接一旦建立就走同一出口，不主动换。
  3. 每号一个 daemon 线程，15s 发一次 39B gzip HEARTBEAT，断线指数退避重连。
  4. 全程失败静默、只记日志；任何异常绝不影响其他号。
  5. 目标号用 --phones 显式传入，默认空（不传不跑，防止误开全池）。

用法（94 上）：
  python3 pulse_guard.py --merchant 100014 \
      --phones 16620683519,13049292479,19832759510,18033411290
systemd: /etc/systemd/system/mcd-pulse-guard.service
"""
import argparse
import json
import logging
import os
import sys
import threading
import time
import zlib

# 让脚本可从任意 cwd 启动：把本目录加进 sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import make_redis, CREDENTIALS_KEY_TMPL
from mcd_profiles import get_client_profile
from pulse_protocol import (
    PULSE_WS_URL, PULSE_HEARTBEAT, PULSE_INTERVAL, build_pulse_headers,
    pulse_message_type, pulse_proxy_options,
)
from utils import get_short_proxy

CLIENT_PROFILE = get_client_profile()
MAX_BACKOFF = 300          # 重连退避上限 5 分钟
LOG_FILE = '/home/pi/work/logs/mcd_pulse_guard.log'

log = logging.getLogger('pulse_guard')

_stop = threading.Event()


def _load_cred(r, merchant_id, phone):
    """只读凭据：返回 (sid, token, meddy_id, city, province)。"""
    raw = r.hget(CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id), phone)
    if not raw:
        return None
    try:
        c = json.loads(raw)
    except Exception:
        return None
    return (c.get('sid') or '', c.get('token') or '', c.get('meddy_id') or '',
            c.get('city') or '', c.get('province') or '')


def _get_proxy(city, province, merchant_id):
    """只读短效代理池取一个代理 dict；失败返回 None（不写任何绑定）。"""
    try:
        return get_short_proxy(city or None, province=province or None,
                               merchant_id=merchant_id, as_dict=True)
    except Exception as error:
        log.warning('  取代理异常 city=%s: %s' % (city, type(error).__name__))
        return None


def _proxy_kw(proxies):
    """requests proxies dict → websocket.create_connection 的 proxy kw。"""
    url = (proxies or {}).get('http') or (proxies or {}).get('https') or ''
    if not url:
        raise ValueError('缺少 HTTP 代理地址')
    return pulse_proxy_options(url)


def heartbeat_worker(phone, merchant_id, r):
    """单号常驻心跳线程：连上就 15s 一跳，断了指数退避重连，直到进程停。"""
    backoff = 5
    while not _stop.is_set():
        cred = _load_cred(r, merchant_id, phone)
        if not cred or not cred[0]:
            log.warning('%s 无凭据/sid，60s 后重试' % phone)
            _stop.wait(60)
            continue
        sid, _token, meddy, city, province = cred
        proxies = _get_proxy(city, province, merchant_id)
        if not proxies:
            log.warning('%s 无可用代理(city=%s)，30s 后重试' % (phone, city))
            _stop.wait(30)
            continue

        try:
            import websocket
        except ImportError:
            log.error('缺 websocket-client 库，退出')
            _stop.set()
            return

        ws = None
        sent = acknowledged = other = invalid = 0
        reason = 'stopped'
        try:
            headers = build_pulse_headers(sid, meddy, CLIENT_PROFILE)
            kw = dict(header=headers, timeout=20, http_proxy_timeout=20, **_proxy_kw(proxies))
            ws = websocket.create_connection(PULSE_WS_URL, **kw)
            backoff = 5   # 连上即重置退避
            log.info('%s ✅心跳连接建立 city=%s v=%s' % (phone, city, CLIENT_PROFILE.version))
            next_send = time.monotonic() + PULSE_INTERVAL
            while not _stop.is_set():
                now = time.monotonic()
                if now >= next_send:
                    ws.send(PULSE_HEARTBEAT, opcode=websocket.ABNF.OPCODE_BINARY)
                    sent += 1
                    next_send = time.monotonic() + PULSE_INTERVAL
                remaining = next_send - time.monotonic()
                if remaining <= 0:
                    continue
                ws.settimeout(min(1, remaining))
                try:
                    opcode, payload = ws.recv_data(control_frame=True)
                except websocket.WebSocketTimeoutException:
                    continue
                if opcode == websocket.ABNF.OPCODE_CLOSE:
                    reason = 'peer_closed'
                    break
                if opcode in (websocket.ABNF.OPCODE_PING, websocket.ABNF.OPCODE_PONG):
                    continue
                try:
                    if pulse_message_type(payload) == 'HEARTBEAT_RESPONSE':
                        acknowledged += 1
                    else:
                        other += 1
                except (ValueError, OSError, EOFError, zlib.error):
                    invalid += 1
        except Exception as error:
            reason = type(error).__name__
        finally:
            if ws is not None:
                try:
                    ws.close(timeout=3)
                except Exception:
                    pass
            log.info('%s 心跳会话结束 reason=%s sent=%d ack=%d other=%d invalid=%d'
                     % (phone, reason, sent, acknowledged, other, invalid))
        if not _stop.is_set():
            log.info('%s %ds 后重连（退避）' % (phone, backoff))
            _stop.wait(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--merchant', default='100014')
    ap.add_argument('--phones', required=True,
                    help='逗号分隔的手机号；不传不跑（防误开全池）')
    args = ap.parse_args()
    phones = [p.strip() for p in args.phones.split(',') if p.strip()]
    if not phones:
        log.error('--phones 为空，拒绝启动')
        return 1

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])
    r = make_redis()
    log.info('=== pulse_guard 启动 merchant=%s 号数=%d 号=%s ===' % (
        args.merchant, len(phones), ','.join(phones)))
    threads = []
    for p in phones:
        t = threading.Thread(target=heartbeat_worker, args=(p, args.merchant, r),
                             daemon=True, name='pg-%s' % p)
        t.start()
        threads.append(t)
    try:
        while not _stop.is_set():
            _stop.wait(1)
    except KeyboardInterrupt:
        log.info('收到 Ctrl-C，退出')
    finally:
        _stop.set()
        log.info('=== pulse_guard 退出 ===')
    return 0


if __name__ == '__main__':
    sys.exit(main())

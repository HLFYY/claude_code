"""
baostock 拉全市场长周期日线（前复权，含换手率/isST），含已退市股票（减少幸存者偏差）
存 cache_bs_long/，不动 stock_screener 的 ./cache；已存在的跳过，可断点续跑
baostock 会按 IP 拉黑并发/高频请求（10001011 黑名单用户），这里每个 SOCKS5 静态代理(proxy_bs.py)一个进程，
单连接串行 + REQ_DELAY 间隔；遇到黑名单立即停该进程
"""
import os
import pickle
import sys
import time
from pathlib import Path

import pandas as pd

LONG_DIR = Path("cache_bs_long")
START = "2018-01-01"
FIELDS = "date,open,high,low,close,volume,amount,turn,tradestatus,isST"
REQ_DELAY = 0.5


def _path(code):
    return LONG_DIR / f"{code.replace('.', '_')}.pkl"


class Blacklisted(Exception):
    pass


def _use_proxy(proxy):
    import socket
    import socks
    socks.set_default_proxy(socks.SOCKS5, proxy["host"], proxy["port"], username=proxy["user"], password=proxy["pwd"])
    socket.socket = socks.socksocket


def _init():
    import baostock as bs
    import io, contextlib, socket
    socket.setdefaulttimeout(30)  # 连接断开时 baostock 会无限挂起，加超时触发重试
    with contextlib.redirect_stdout(io.StringIO()):
        try:
            bs.logout()
        except Exception:
            pass
        bs.login()


def fetch(code):
    import baostock as bs
    p = _path(code)
    if p.exists():
        return "skip"
    for attempt in range(3):
        try:
            rs = bs.query_history_k_data_plus(code, FIELDS, start_date=START, frequency="d", adjustflag="2")
            if rs.error_code == "10001011" or "黑名单" in (rs.error_msg or ""):
                raise Blacklisted(rs.error_msg)
            if rs.error_code != "0":
                raise RuntimeError(rs.error_msg)
            df = rs.get_data()
            if code.startswith("sz.399") and (df is None or df.empty):
                raise RuntimeError("index empty")
            break
        except Blacklisted:
            raise
        except Exception:
            time.sleep(2)
            _init()
    else:
        return "fail"
    if df is None or df.empty:
        pickle.dump(None, open(p, "wb"))
        return "empty"
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume", "amount", "turn"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.rename(columns={"turn": "turnover"})
    df = df[df["tradestatus"] == "1"].dropna(subset=["close"]).reset_index(drop=True)  # 去掉停牌日
    pickle.dump(df, open(p, "wb"))
    time.sleep(REQ_DELAY)
    return "ok"


def stock_list():
    f = LONG_DIR / "_stock_list.pkl"
    if f.exists():
        return pickle.load(open(f, "rb"))
    import baostock as bs
    _init()
    d = bs.query_stock_basic().get_data()
    d = d[(d["type"] == "1") & d["code"].str.match(r"^(sh\.6|sz\.0|sz\.3)")]
    d = d[["code", "code_name", "ipoDate", "outDate", "status"]].reset_index(drop=True)
    pickle.dump(d, open(f, "wb"))
    return d


def worker(k, proxy, codes):
    _use_proxy(proxy)
    _init()
    stats = {}
    for n, code in enumerate(codes):
        try:
            st = fetch(code)
        except Blacklisted as e:
            print(f"[代理{proxy['port']}] 被拉黑，停止：{e}", flush=True)
            break
        stats[st] = stats.get(st, 0) + 1
        if n % 100 == 0:
            print(f"[代理{proxy['port']}] {n}/{len(codes)} {stats}", flush=True)
    print(f"[代理{proxy['port']}] done {stats}", flush=True)


if __name__ == "__main__":
    from multiprocessing import Process
    from proxy_bs import PROXIES
    LONG_DIR.mkdir(exist_ok=True)
    _use_proxy(PROXIES[0])
    sl = stock_list()
    codes = ["sz.399001"] + [c for c in sl["code"] if not _path(c).exists()]
    print(f"股票 {len(sl)} 只（含退市 {(sl['status'] == '0').sum()}），待拉 {len(codes)}，代理 {len(PROXIES)} 个", flush=True)
    procs = [Process(target=worker, args=(k, px, codes[k::len(PROXIES)])) for k, px in enumerate(PROXIES)]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join()

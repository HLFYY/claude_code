"""
北交所日线（东方财富，前复权，含成交额/换手率）→ cache_bs_long/bj_{代码}.pkl，格式同 baostock 长周期数据，供回测含北交所对比
  - 股票列表：新浪 hs_a 里的 bj 股票（仅现存，无退市股）；ST 按当前名称近似（名称含 ST 则全程视为 ST）
  - 东方财富不是 baostock，可小并发；已存在的文件跳过
用法：python fetch_bj_eastmoney.py
"""
import pickle
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests

from stock_check_xinlang import _sina_get_stock_list_all

OUT = __import__("pathlib").Path("cache_bs_long")
URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"


def fetch(sym_name):
    sym, name = sym_name
    code = sym[2:]
    f = OUT / f"bj_{code}.pkl"
    if f.exists():
        return sym, "skip"
    for attempt in range(3):
        try:
            r = requests.get(URL, params={"secid": f"0.{code}", "fields1": "f1,f2,f3", "klt": 101, "fqt": 1,
                                          "fields2": "f51,f52,f53,f54,f55,f56,f57,f61",
                                          "beg": "20180101", "end": "20500101"}, timeout=15)
            kl = (r.json().get("data") or {}).get("klines") or []
            break
        except Exception:
            time.sleep(2 * (attempt + 1))
    else:
        return sym, "fail"
    if not kl:
        return sym, "empty"
    d = pd.DataFrame([x.split(",") for x in kl],
                     columns=["date", "open", "close", "high", "low", "volume", "amount", "turnover"])
    d["date"] = pd.to_datetime(d["date"])
    for c in ("open", "close", "high", "low", "amount", "turnover"):
        d[c] = d[c].astype(float)
    d["volume"] = d["volume"].astype(int) * 100          # 手 → 股
    d["tradestatus"] = "1"
    d["isST"] = "1" if "ST" in name.upper() else "0"
    d = d[["date", "open", "high", "low", "close", "volume", "amount", "turnover", "tradestatus", "isST"]]
    pickle.dump(d, open(f, "wb"))
    time.sleep(0.2)
    return sym, len(d)


def main():
    bj = [(s["symbol"], s.get("name", "")) for s in _sina_get_stock_list_all() if s["symbol"].startswith("bj")]
    with ThreadPoolExecutor(4) as ex:
        res = list(ex.map(fetch, bj))
    bad = [r for r in res if r[1] in ("fail", "empty")]
    print(f"北交所 {len(bj)} 只，失败/无数据 {len(bad)}：{bad[:10]}")
    pickle.dump(pd.DataFrame({"code": [f"bj.{s[2:]}" for s, _ in bj], "code_name": [n for _, n in bj]}),
                open(OUT / "_stock_list_bj.pkl", "wb"))


if __name__ == "__main__":
    main()

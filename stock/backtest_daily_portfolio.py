"""
日线突破选股（screen_new_high_breakout_sina 的筛选条件）信号提取，供 backtest_long_run.py 使用
  信号日T收盘出信号 → T+1 开盘可买（仅当天，过期作废），A=首日突破(昨日非新高) / B=突破第二日(昨日也新高)
  数据同 backtest_weekly_portfolio（cache_bs_long），信号日为 ST 的不做
"""
import pickle
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
from stock_check_xinlang import _check_breakout_sina, _NEW_HIGH_DAYS

SIG_FILE = bw.SIG_DIR / "_daily_breakout_signals.pkl"


def signals_for_stock(args):
    code, name = args
    _, d = bw.load_stock(code)
    if d is None or len(d) < _NEW_HIGH_DAYS + 8:
        return []
    df = d.rename(columns={"date": "day"})
    is_st = (df["isST"] == "1").values
    c, h = df["close"].values, df["high"].values
    prior_max = pd.Series(h).rolling(100).max().shift(1).values
    out = []
    for i in range(_NEW_HIGH_DAYS + 6, len(df)):
        if np.isnan(prior_max[i]) or c[i] < prior_max[i] or is_st[i]:
            continue
        r = _check_breakout_sina(code, name, df.iloc[i - _NEW_HIGH_DAYS - 10:i + 1], {})
        if not r or np.isnan(r["14日ATR"]) or r["14日ATR"] <= 0:
            continue
        out.append({
            "code": code, "sig_date": df["day"].iloc[i],
            "signal": "B突破第二日" if r["昨日是否新高"] == "是" else "A首日突破",
            "score": r["突破日成交额倍量比"], "vol_ratio": r["突破日成交额倍量比"],
            "breakout_price": r["突破价"], "atr": r["14日ATR"], "daily_ok": "是",
        })
    return out


def extract_signals(codes):
    if SIG_FILE.exists():
        return pickle.load(open(SIG_FILE, "rb"))
    rows = []
    with Pool(8) as pool:
        for k, r in enumerate(pool.imap_unordered(signals_for_stock, codes, chunksize=20)):
            rows.extend(r)
            if k % 1000 == 0:
                print(f"日线选股 {k}/{len(codes)} 信号 {len(rows)}", file=sys.stderr, flush=True)
    sig = pd.DataFrame(rows)
    pickle.dump(sig, open(SIG_FILE, "wb"))
    return sig

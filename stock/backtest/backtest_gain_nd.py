"""
日线"近N日涨幅"上限：近2日（实盘 40%，原 20%）/ 近3日 / 近4日 各阈值对比（其余按实盘方案，合并池 + 日线单独）
信号用实盘选股函数提取的不限涨幅版（cache_bs_long/_daily_breakout_signals_c2-inf_ginf.pkl），近N日涨幅 = 当日收盘 / N+1 个交易日前收盘 - 1（同近2日口径）
用法：python backtest/backtest_gain_nd.py > stock_result/gain_nd.log 2>&1
"""
import multiprocessing as mp
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
CASES = [("近2日", 20), ("近2日", 40)] + [(f"近{n}日", g) for n in (3, 4) for g in (20, 25, 30, 35, 40, 45, 50, 60)] \
        + [("不限", None)]


def run(a):
    (col, g), pool = a
    dy = G["dy"]
    if g is not None:
        dy = dy[dy[{"近2日": "g2", "近3日": "g3", "近4日": "g4"}[col]] < g]
    sig, mode = (pd.concat([G["wk"], dy], ignore_index=True), "mixed") if pool == "合并池" else (dy, "daily")
    e, t = bw.run_portfolio(sig, G["daily"], G["mkt"], priority=PRIORITY, pool_mode=mode, real=REAL)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    td = t[~t["signal"].str.startswith(WEEKLY_TYPES)]
    return {"池": pool, "条件": f"{col}<{g}%" if g else "不限", "日线信号": len(dy), "年化%": p["年化%"], "回撤%": p["最大回撤%"],
            "夏普": p["夏普"], "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "日线笔数": len(td), "日线平均%": round(td["ret"].mean() * 100, 2), **yearly(e)}


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    G["daily"] = daily

    def gain(code, d, n):
        dd = daily.get(code)
        j = None if dd is None else dd["pos"].get(d)
        if j is None or j < n:
            return np.nan
        return (dd["c"][j] / dd["c"][j - n] - 1) * 100
    for n in (2, 3, 4):
        dy[f"g{n}"] = [gain(c, d, n) for c, d in zip(dy["code"], dy["sig_date"])]
    print("近2日口径核对：与选股函数 gain_2d 差 >0.5 个点的占比",
          round((abs(dy["g2"] - dy["gain_2d"]) > 0.5).mean() * 100, 2), "%", flush=True)
    dy["g2"] = dy["gain_2d"]                      # 近2日用选股函数原值（同实盘）
    G["mkt"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    G["wk"] = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    G["dy"] = bw.add_rank_fields(dy, daily).assign(pool="daily")
    cases = [(c, p) for p in ("合并池", "日线单独") for c in CASES]
    with mp.get_context("fork").Pool(5) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    for pn in ("合并池", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    res.to_csv("stock_result/gain_nd_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

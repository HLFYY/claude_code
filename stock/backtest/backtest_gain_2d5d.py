"""
日线 近2日涨幅<30%/40%(实盘) × 近5日涨幅 不限/<30/40/50/60%，其余按现行实盘方案（开盘>6%不买）
信号：实盘选股函数的不限涨幅版（cache_bs_long/_daily_breakout_signals_c2-inf_ginf.pkl）；近5日涨幅 = 当日收盘 / 5 个交易日前收盘 - 1
用法：python backtest/backtest_gain_2d5d.py > stock_result/gain_2d5d.log 2>&1
"""
import multiprocessing as mp
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
CASES = [(g2, g5) for g2 in (40, 30) for g5 in (None, 30, 40, 50, 60)]


def run(a):
    (g2, g5), pool = a
    dy = G["dy"][G["dy"]["gain_2d"] < g2]
    if g5 is not None:
        dy = dy[dy["g5"] < g5]
    sig, mode = (pd.concat([G["wk"], dy], ignore_index=True), "mixed") if pool == "合并池" else (dy, "daily")
    e, t = bw.run_portfolio(sig, G["daily"], G["mkt"], priority=PRIORITY, pool_mode=mode, real=REAL)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    td = t[~t["signal"].str.startswith(WEEKLY_TYPES)]
    name = f"2日<{g2}%" + (f" 且 5日<{g5}%" if g5 else "") + ("（实盘）" if (g2, g5) == (40, None) else "")
    return {"池": pool, "条件": name, "日线信号": len(dy), "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
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

    def g5(code, d):
        dd = daily.get(code)
        j = None if dd is None else dd["pos"].get(d)
        return np.nan if j is None or j < 5 else (dd["c"][j] / dd["c"][j - 5] - 1) * 100
    dy["g5"] = [g5(c, d) for c, d in zip(dy["code"], dy["sig_date"])]
    print("近5日涨幅分布（2日<40% 的信号）：", dy.loc[dy["gain_2d"] < 40, "g5"].describe(percentiles=[.5, .75, .9, .95]).round(1).to_dict(), flush=True)
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
    res.to_csv("stock_result/gain_2d5d_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

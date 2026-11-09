"""
顶分型清仓后：确认日之后连续 N 天收盘在10日线上（没突破顶点）也按10日线规则开仓（None=实盘，必须突破顶点），按现行实盘方案（近2日<40%、开盘>6%不买）
用法：python backtest/backtest_top_wait.py > stock_result/top_wait.log 2>&1
"""
import multiprocessing as mp
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
MAS = (None, 1, 2, 3, 4, 5)


def run(a):
    n, pool = a
    mkt = G["mkt"][n]
    sig, mode = {"合并池": (G["mix"], "mixed"), "周线单独": (G["wk"], "weekly"), "日线单独": (G["dy"], "daily")}[pool]
    e, t = bw.run_portfolio(sig, G["daily"], mkt, priority=PRIORITY, pool_mode=mode, real=REAL)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"池": pool, "顶分清仓后": ("必须突破顶点(实盘)" if n is None else f"连续{n}天在10日线上"), "年化%": p["年化%"], "回撤%": p["最大回撤%"],
            "夏普": p["夏普"], "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "在场%": round((mkt["level"] > 0).mean() * 100, 1), "清仓次数": int(mkt["clear"].sum()),
            "笔数": len(t), "胜率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2), **yearly(e)}


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    G["daily"] = bw.load_daily([c for c, _ in codes])
    G["mkt"] = {n: bw.market_levels_v2(idx, True, "confirm", **MKT_KW, top_wait=n) for n in MAS}
    G["wk"] = bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly")
    G["dy"] = bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")
    G["mix"] = pd.concat([G["wk"], G["dy"]], ignore_index=True)
    cases = [(n, p) for p in ("合并池", "周线单独", "日线单独") for n in MAS]
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    for pn in ("合并池", "周线单独", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    res.to_csv("stock_result/top_wait_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

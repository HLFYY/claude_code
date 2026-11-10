"""
盘中清仓价规则（昨收在5日线下时怎么算），按现行实盘方案：
  实盘   ：max(昨MA5, 昨最低)×0.99
  low_if_below    ：昨收<MA5 时只用 昨最低×0.99
  nearest_if_below：昨收<MA5 时用昨收下方最近的 MA10/20/30 与昨最低取大×0.99（都在上方则只用最低）
  nearest         ：始终用昨收下方最近的 MA5/10/20/30 与昨最低取大×0.99
用法：python backtest/backtest_stop_line.py > stock_result/stop_line.log 2>&1
"""
import multiprocessing as mp
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
RULES = (None, "low_if_below", "nearest_if_below", "nearest")
NAME = {None: "max(MA5,最低)(实盘)", "low_if_below": "线下只用最低", "nearest_if_below": "线下用下方最近均线",
        "nearest": "始终用下方最近均线"}


def run(a):
    rule, pool = a
    sig, mode = {"合并池": (G["mix"], "mixed"), "周线单独": (G["wk"], "weekly"), "日线单独": (G["dy"], "daily")}[pool]
    e, t = bw.run_portfolio(sig, G["daily"], G["mkt"], priority=PRIORITY, pool_mode=mode, real=REAL, stop_rule=rule)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    stop = t[t["reason"].astype(str).str.contains("止损")] if "reason" in t else t.iloc[0:0]
    return {"池": pool, "止损价": NAME[rule], "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "笔数": len(t), "胜率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2),
            "止损笔数": len(stop), **yearly(e)}


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
    G["mkt"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    G["wk"] = bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly")
    G["dy"] = bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")
    G["mix"] = pd.concat([G["wk"], G["dy"]], ignore_index=True)
    cases = [(r, p) for p in ("合并池", "周线单独", "日线单独") for r in RULES]
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    for pn in ("合并池", "周线单独", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    res.to_csv("stock_result/stop_line_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

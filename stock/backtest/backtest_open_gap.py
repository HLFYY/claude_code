"""
开盘涨幅 > X 不买：5%/6%/7%(实盘)/8%/9%/10%/不限(只跳过开盘涨停)，其余按实盘方案（日线近2日涨幅<40%）
用法：python backtest/backtest_open_gap.py > stock_result/open_gap.log 2>&1
"""
import multiprocessing as mp
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
GAPS = (0.05, 0.06, 0.07, 0.08, 0.09, 0.10, None)


def run(a):
    gap, pool = a
    sig, mode = {"合并池": (G["mix"], "mixed"), "周线单独": (G["wk"], "weekly"), "日线单独": (G["dy"], "daily")}[pool]
    e, t = bw.run_portfolio(sig, G["daily"], G["mkt"], priority=PRIORITY, pool_mode=mode, real={**REAL, "max_open_gap": gap})
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"池": pool, "开盘涨幅>不买": f"{gap:.0%}" if gap else "不限(涨停除外)", "年化%": p["年化%"], "回撤%": p["最大回撤%"],
            "夏普": p["夏普"], "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "笔数": len(t), "胜率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2), **yearly(e)}


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)          # 默认近2日涨幅<40%（同实盘）
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    G["daily"] = bw.load_daily([c for c, _ in codes])
    G["mkt"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    G["wk"] = bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly")
    G["dy"] = bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")
    G["mix"] = pd.concat([G["wk"], G["dy"]], ignore_index=True)
    print("日线信号", len(dy), flush=True)
    cases = [(g, p) for p in ("合并池", "周线单独", "日线单独") for g in GAPS]
    with mp.get_context("fork").Pool(5) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    for pn in ("合并池", "周线单独", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    res.to_csv("stock_result/open_gap_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

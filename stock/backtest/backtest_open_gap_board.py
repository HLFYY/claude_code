"""
开盘涨幅上限按板块：max(6%, 涨跌停幅度 × k)，k = 50%/60%/70%（主板 10% 涨停 → 6%/6%/7%；创业板/科创板 20% → 10%/12%/14%）
对照：统一 6%（实盘）/ 7%；其余按现行实盘方案（近2日<40%）
用法：python backtest/backtest_open_gap_board.py > stock_result/open_gap_board.log 2>&1
"""
import multiprocessing as mp
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}


def board_gap(k):
    return lambda code: max(0.06, bw._limit_pct(code) * k)


CASES = {"统一6%(实盘)": 0.06, "统一7%": 0.07, "max(6%,涨停幅度×50%)": 0.5, "max(6%,涨停幅度×60%)": 0.6, "max(6%,涨停幅度×70%)": 0.7}


def run(a):
    name, pool = a
    v = CASES[name]
    gap = board_gap(v) if v >= 0.5 else v
    sig, mode = {"合并池": (G["mix"], "mixed"), "周线单独": (G["wk"], "weekly"), "日线单独": (G["dy"], "daily")}[pool]
    e, t = bw.run_portfolio(sig, G["daily"], G["mkt"], priority=PRIORITY, pool_mode=mode, real={**REAL, "max_open_gap": gap})
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    gem = t["code"].str[3:6].isin(("300", "301", "688", "689"))
    return {"池": pool, "开盘涨幅上限": name, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "笔数": len(t), "创/科笔数": int(gem.sum()), "创/科平均%": round(t.loc[gem, "ret"].mean() * 100, 2),
            "胜率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2), **yearly(e)}


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
    cases = [(n, p) for p in ("合并池", "周线单独", "日线单独") for n in CASES]
    with mp.get_context("fork").Pool(5) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    for pn in ("合并池", "周线单独", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    res.to_csv("stock_result/open_gap_board_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

"""
可开额度（上限−冻结−持仓）> 阈值就开 1 只，钱不够一层用冻结资金补足（单变量，按现行实盘方案，合并池）
用法：python backtest/backtest_top_up.py > stock_result/top_up.log 2>&1
"""
import multiprocessing as mp
import pickle
import sys

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
CASES = [("实盘", {})] + [(f"额度>{x}层就开(冻结补足)", {"top_up": x}) for x in (0.1, 0.2, 0.3, 0.5, 0.7)]


def run(a):
    name, kw = a
    e, t = bw.run_portfolio(G["mix"], G["daily"], G["mkt"], priority=PRIORITY, pool_mode="mixed", real=REAL, **kw)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"方案": name, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), **yearly(e)}


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
    G["mix"] = pd.concat([bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly"),
                          bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")], ignore_index=True)
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, CASES, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    print(res.to_string(index=False))
    res.to_csv("stock_result/top_up_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

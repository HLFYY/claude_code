"""
首日 70% 的涨幅门槛（单变量，按现行实盘方案，合并池；不冻结规则 2.5% + 7 天不变）
  strong=(门槛, 0.7)：开仓信号当天指数涨幅 > 门槛 首日建 70%；带 "ma" = 只对收在 10 日线上的开仓（底分型确认的不算）
用法：python backtest/backtest_strong_lv.py > stock_result/strong_lv.log 2>&1
"""
import multiprocessing as mp
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
CASES = [("实盘 >3%", (0.03, 0.7)), ("实盘 >3% 只限站上10日线", (0.03, 0.7, "ma")),
         (">2.5%", (0.025, 0.7)), (">2.5% 只限站上10日线", (0.025, 0.7, "ma")),
         (">2%", (0.02, 0.7)), (">2% 只限站上10日线", (0.02, 0.7, "ma")),
         (">3.5%", (0.035, 0.7)), (">2.5% 首日100%", (0.025, 1.0)), (">3% 首日100%", (0.03, 1.0))]


def run(a):
    name, st = a
    mkt = bw.market_levels_v2(G["idx"], True, "confirm", **{**MKT_KW, "strong": st})
    lv = mkt["level"].values
    n70 = int(sum(1 for i in range(1, len(lv)) if lv[i - 1] == 0 and lv[i] == st[1]))
    e, t = bw.run_portfolio(G["mix"], G["daily"], mkt, priority=PRIORITY, pool_mode="mixed", real=REAL)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"方案": name, "首日强仓次数": n70, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "笔数": len(t), **yearly(e)}


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    G["idx"] = idx[idx["date"].isin(cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    G["daily"] = bw.load_daily([c for c, _ in codes])
    G["mix"] = pd.concat([bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly"),
                          bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")], ignore_index=True)
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, CASES, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    print(res.to_string(index=False))
    res.to_csv("stock_result/strong_lv_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

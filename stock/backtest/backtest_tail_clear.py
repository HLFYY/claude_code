"""
尾盘走弱判断天数：前2天(实盘)/3/4/5/全部持仓期
用法：python backtest/backtest_tail_clear.py > stock_result/tail_clear.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly


GRIDS = (None, "ma10", "all")


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    mix = pd.concat([wk, dy], ignore_index=True)
    mkts = {fm: bw.market_levels_v2(idx, True, "confirm", tail_clear=fm, **MKT_KW) for fm in GRIDS}
    rows, yrows = [], []
    for pool_name, sig, mode, fms in (("合并池", mix, "mixed", GRIDS),
                                      ("周线单独", wk, "weekly", GRIDS), ("日线单独", dy, "daily", GRIDS)):
        for fm in fms:
            e, t = bw.run_portfolio(sig, daily, mkts[fm], priority=PRIORITY, pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            fri = pd.to_datetime(t["buy_date"]).dt.weekday == 4
            label = f"{pool_name} " + {None: "次日开盘清(实盘)", "ma10": "跌破10日线尾盘清", "all": "所有清仓信号尾盘清"}[fm]
            rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), "胜率%": round((t["ret"] > 0).mean() * 100, 1),
                         "网格卖出份数均值": round(t["grid_sold"].mean(), 2), "网格卖完%": round((t["reason"] == "网格卖完").mean() * 100, 1), "尾盘清笔数": int(t["reason"].str.contains("尾盘降仓").sum()),
                         "周五买入笔数": int(fri.sum()),
                         "周五买入平均%": round(t.loc[fri, "ret"].mean() * 100, 2) if fri.any() else None,
                         "周五买入胜率%": round((t.loc[fri, "ret"] > 0).mean() * 100, 1) if fri.any() else None})
            yrows.append({"方案": label, **yearly(e)})
            print(rows[-1], flush=True)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv("stock_result/tail_clear_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

"""
买入排序对比（其余按已确认的实盘方案：合并池 × 开盘>7%不买 × 实盘约束 × 深证综指，周线参数=当前代码）
用法：python backtest_priority.py > stock_result/priority.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

ORDERS = ["RPS120高→低", "10日涨幅排名强→弱", "辨识度档→RPS120高→低", "辨识度档→10日涨幅排名强→弱", "min(RPS120名次,10日名次)",
          "RPS120(1位小数)高→低,同分10日强→弱", "RPS120(1位小数)高→低,同分10日弱→强",
          "辨识度档→RPS120低→高", "辨识度档→10日涨幅排名弱→强", "周线在前→RPS120高→低"]
import sys
if len(sys.argv) > 1:          # 只跑指定排序：python backtest_priority.py 0 5 6
    ORDERS = [ORDERS[int(k)] for k in sys.argv[1:]]


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
    mkt = bw.market_levels_v2(idx, True, "confirm")
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    mix = pd.concat([wk, dy], ignore_index=True)

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    print("== 信号辨识度档位分布（合并池）==")
    print(mix["tier"].value_counts().sort_index().head(10).to_string())

    rows, yrows = [], []
    for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", wk, "weekly")):
        for od in ORDERS:
            e, t = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority=od, pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            label = f"{pool_name} {od}"
            rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24年化": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "成功率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2)})
            yrows.append({"方案": label, **yearly(e)})
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    if len(sys.argv) > 1:
        return
    res.to_csv("stock_result/priority_summary.csv", index=False, encoding="utf-8-sig")
    yr.to_csv("stock_result/priority_yearly.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

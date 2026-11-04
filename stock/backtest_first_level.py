"""
大盘首日建仓仓位对比：站上10日线 / 底分型确认的第一天建 N 层，满足加仓条件第二天加满（其余按已确认的实盘方案）
用法：python backtest_first_level.py > stock_result/first_level.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

PRIORITY = "RPS120(1位小数)高→低,同分10日弱→强"
LEVELS = [0.4, 0.5, 0.6, 0.7, 0.8, 1.0]
STRONG = [(0.03, 0.7), (0.02, 0.7), (0.03, 1.0)]          # 开仓信号当天指数涨幅 > 阈值则首日用该仓位，否则 4 层
import sys
if len(sys.argv) > 1 and sys.argv[1] == "strong":        # python backtest_first_level.py strong：只跑大涨日加仓对比
    LEVELS = [0.4]


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

    rows, yrows = [], []
    combos = [(lv, None) for lv in LEVELS] + ([(0.4, st) for st in STRONG] if len(sys.argv) > 1 else [])
    for lv, st in combos:
        mkt = bw.market_levels_v2(idx, True, "confirm", first_lv=lv, strong=st)
        for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", wk, "weekly")):
            e, t = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority=PRIORITY, pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            label = f"{pool_name} 首日{int(round(lv * 10))}层" + (f"，指数涨>{st[0]:.0%}则{int(round(st[1] * 10))}层" if st else "")
            rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24年化": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "成功率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2)})
            yrows.append({"方案": label, **yearly(e)})

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    print("== 总览 ==")
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n== 逐年收益% ==")
    print(pd.DataFrame(yrows).to_string(index=False))
    if len(sys.argv) > 1:
        return
    pd.DataFrame(rows).to_csv("stock_result/first_level_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

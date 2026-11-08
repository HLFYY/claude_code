"""
本金 × 份数：80/100/130/160/200 万 × 每份 1/10、1/12、1/15（合并池，实盘约束：整手、费率、滑点）
份数越多每只越小；大盘 40%/70% 仓位时可开 层数 = 仓位 × 份数（向下取整）
用法：python backtest/backtest_capital_layers.py > stock_result/capital_layers.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

CAPS = (800_000, 1_000_000, 1_300_000, 1_600_000, 2_000_000)
import sys
LAYERS = tuple(int(x) for x in sys.argv[1].split(",")) if len(sys.argv) > 1 else (10, 12, 15)


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
    mkt = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    rows, yrows = [], []
    for n in LAYERS:
        bw.LAYER = 1 / n
        for cap in CAPS:
            e, t = bw.run_portfolio(mix, daily, mkt, priority=PRIORITY, pool_mode="mixed", real={**REAL, "capital": cap})
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            label = f"{cap // 10000}万/{n}份"
            rows.append({"方案": label, "本金万": cap // 10000, "份数": n, "每份万": round(cap / n / 10000, 1),
                         "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "期末倍数": round(e.iloc[-1] / e.iloc[0], 2),
                         "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), "胜率%": round((t["ret"] > 0).mean() * 100, 1)})
            yrows.append({"方案": label, **yearly(e)})
            print(rows[-1], flush=True)
    bw.LAYER = 0.10
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 年化%（行=本金，列=份数）==")
    print(res.pivot(index="本金万", columns="份数", values="年化%").to_string())
    print("\n== 回撤%（行=本金，列=份数）==")
    print(res.pivot(index="本金万", columns="份数", values="回撤%").to_string())
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv(f"stock_result/capital_layers_summary{'_' + sys.argv[1].replace(',', '_') if len(sys.argv) > 1 else ''}.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

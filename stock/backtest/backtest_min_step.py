"""
股票池过滤网格步长（买入前一日 ATR14% × 1.2）< 3/4/5/6% 的票：收益 + 股票池够不够用
够用指标：开仓日（大盘允许仓位 > 已用）里，买满允许层数的天数占比、平均缺口层数
用法：python backtest/backtest_min_step.py > stock_result/min_step.log 2>&1
"""
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW


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
    rows = []
    for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", wk, "weekly"), ("日线单独", dy, "daily")):
        for ms in (None, 3, 4, 5, 6):
            e, t = bw.run_portfolio(sig, daily, mkt, priority=PRIORITY, pool_mode=mode, real=REAL, min_step=ms)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            ex = e.attrs["exposure"]
            gap = (ex["allowed"] - ex["used"]).clip(lower=0)
            short = gap > 0.5
            rows.append({"池": pool_name, "步长下限%": ms or "-", "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), "胜率%": round((t["ret"] > 0).mean() * 100, 1),
                         "买不满天数%": round(short.mean() * 100, 1), "平均缺口层": round(gap.mean(), 2),
                         "平均仓位利用%": round((ex["used"] / ex["allowed"].replace(0, np.nan)).mean() * 100, 1)})
            print(rows[-1], flush=True)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(rows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    res.to_csv("stock_result/min_step_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

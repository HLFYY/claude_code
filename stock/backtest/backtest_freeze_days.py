"""
止损/走弱卖出资金冻结规则：不冻结 / 冻结1~4天（每周一全部解冻）/ 冻结到下周一(实盘) /
次日冻结、第3天看次日深证综指收涨且收红则解冻否则冻到下周一
用法：python backtest/backtest_freeze_days.py > stock_result/freeze_days.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

CASES = {
    "冻结到下周一(实盘)": dict(use_freeze=True),
    "不冻结": dict(use_freeze=False),
    "冻结1天+周一解冻": dict(use_freeze=1, freeze_mon=True),
    "冻结2天+周一解冻": dict(use_freeze=2, freeze_mon=True),
    "冻结3天+周一解冻": dict(use_freeze=3, freeze_mon=True),
    "冻结4天+周一解冻": dict(use_freeze=4, freeze_mon=True),
    "次日冻结,综指涨红解冻否则下周一": dict(use_freeze=True, freeze_rule="mkt"),
}


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
    full = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    full["idx_up_red"] = (full["close"] > full["close"].shift(1)) & (full["close"] > full["open"])
    mkt = mkt.merge(full[["date", "idx_up_red"]], on="date", how="left")
    mkt["idx_up_red"] = mkt["idx_up_red"].fillna(False).astype(bool)
    print("综指涨红天数占比", round(mkt["idx_up_red"].mean() * 100, 1), "%", flush=True)
    rows, yrows = [], []
    for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", wk, "weekly"), ("日线单独", dy, "daily")):
        for name, kw in CASES.items():
            e, t = bw.run_portfolio(sig, daily, mkt, priority=PRIORITY, pool_mode=mode, real=REAL, **kw)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            label = f"{pool_name} {name}"
            rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), "胜率%": round((t["ret"] > 0).mean() * 100, 1)})
            yrows.append({"方案": label, **yearly(e)})
            print(rows[-1], flush=True)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv("stock_result/freeze_days_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

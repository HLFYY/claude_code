"""
新开票走弱规则对比（走弱 = 当日高 ≤ 昨高 或 收盘 ≤ 昨收）
  open      买入当天走弱→次日开盘卖；第2天走弱→第2天尾盘卖（当前实盘）
  tail2     买入当天走弱→第2天尾盘卖；第2天走弱→第2天尾盘卖
  day2only  买入当天不判断；第2天走弱→第2天尾盘卖
  both      买入当天和第2天都走弱→第2天尾盘卖
  off       不做走弱判断（只有止损/网格/大盘）
用法：python backtest/backtest_weak_rule.py > stock_result/weak_rule.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW

MODES = [("open", "当前：首日弱→次日开盘卖，次日弱→尾盘卖"), ("tail2", "首日弱→次日尾盘卖，次日弱→尾盘卖"),
         ("day2only", "只看次日，弱→尾盘卖"), ("both", "首日和次日都弱→次日尾盘卖"), ("off", "不做走弱判断")]


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
        for wm, desc in MODES:
            e, t = bw.run_portfolio(sig, daily, mkt, priority=PRIORITY, pool_mode=mode, real=REAL, weak_mode=wm)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            weak_n = t["reason"].str.contains("走弱").sum()
            rows.append({"池": pool_name, "规则": wm, "说明": desc, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), "胜率%": round((t["ret"] > 0).mean() * 100, 1),
                         "走弱卖出笔数": int(weak_n)})
            print(rows[-1], flush=True)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(rows)
    print("\n== 总览 ==")
    print(res.drop(columns="说明").to_string(index=False))
    res.to_csv("stock_result/weak_rule_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

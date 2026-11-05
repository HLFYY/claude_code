"""
大盘底分型确认条件对比（其余按已确认的实盘方案：合并池 × RPS120高→低 × 开盘>7%不买 × 实盘约束 × 深证综指）
  k3c    确认日收盘 >= 第3根收盘（现规则）
  maxc   确认日收盘 >= 三根最高收盘
  maxc_h 确认日收盘 >= 三根最高收盘 且 最高价 > 三根最高价
  k3c_h  确认日收盘 >= 第3根收盘 且 最高价 > 三根最高价
用法：python backtest/backtest_bottom_confirm.py > stock_result/bottom_confirm.log 2>&1
"""
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

RULES = {"k3c": "收盘≥第3根收盘(现)", "maxc": "收盘≥三根最高收盘", "maxc_h": "收盘≥三根最高收盘+最高>三根最高",
         "k3c_h": "收盘≥第3根收盘+最高>三根最高", "k3body": "收盘≥第3根实体顶部", "k3body+wl": "收盘≥第3根实体顶部+弱势线也用实体顶部"}
import sys
if len(sys.argv) > 1:          # 只跑指定规则：python backtest/backtest_bottom_confirm.py k3c k3body k3body+wl
    RULES = {k: RULES[k] for k in sys.argv[1:]}


def fractal_entries(idx, mkt):
    """底分型开仓次数：前一日收盘在10日线下、当日开盘 0→4层；及这些开仓后指数到下一次清仓/站上10日线的涨幅"""
    c = idx["close"].values
    ma10 = pd.Series(c).rolling(10).mean().values
    lv = mkt["level"].values
    rets, wins = [], 0
    for i in range(1, len(lv)):
        if lv[i - 1] == 0 and lv[i] > 0 and c[i - 1] < ma10[i - 1]:
            j = i
            while j + 1 < len(lv) and lv[j + 1] > 0 and c[j] < ma10[j]:
                j += 1          # 底分型持仓段：直到清仓或站上10日线
            r = c[j] / idx["open"].values[i] - 1
            rets.append(r); wins += r > 0
    return len(rets), (wins / len(rets) * 100 if rets else np.nan), (np.mean(rets) * 100 if rets else np.nan)


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
    for rule, desc in RULES.items():
        mkt = bw.market_levels_v2(idx, True, "confirm", confirm_rule=rule.split("+")[0],
                                  weak_line="k3body" if rule.endswith("+wl") else "k3c")
        n_ent, win, avg = fractal_entries(idx, mkt)
        for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", wk, "weekly")):
            e, t = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority="RPS120高→低", pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            label = f"{pool_name} {desc}"
            rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24年化": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "在场%": round((mkt["level"] > 0).mean() * 100, 1), "底分型开仓次数": n_ent,
                         "底分型段指数胜率%": round(win, 1), "底分型段指数均%": round(avg, 2),
                         "笔数": len(t), "成功率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2)})
            yrows.append({"方案": label, **yearly(e)})

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    if len(sys.argv) > 1:
        return
    res.to_csv("stock_result/bottom_confirm_summary.csv", index=False, encoding="utf-8-sig")
    yr.to_csv("stock_result/bottom_confirm_yearly.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

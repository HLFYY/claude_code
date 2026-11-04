"""
日线选股"近2日涨幅"上限对比（其余按已确认的实盘方案：合并池 × RPS120(1位小数)高→低,同分10日弱→强 × 开盘>7%不买 × 实盘约束 × 深证综指）
  20%（现）/ 涨跌幅2倍（主板20%、创业板科创板40%）/ 统一30% / 统一40% / 不限
  信号只提取一次（不限涨幅，带 gain_2d 列），各阈值事后过滤
用法：python backtest_gain2d.py > stock_result/gain2d.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

PRIORITY = "RPS120(1位小数)高→低,同分10日弱→强"


def limit_2x(code):
    return bw._limit_pct(code) * 2 * 100


RULES = {"<20%(现)": lambda c, g: g < 20, "<涨跌幅2倍": lambda c, g: g < limit_2x(c),
         "<30%": lambda c, g: g < 30, "<40%": lambda c, g: g < 40, "不限": lambda c, g: True}


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy_all = bd.extract_signals(codes, 2, None, None)
    dy_all = dy_all[dy_all["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    mkt = bw.market_levels_v2(idx, True, "confirm")
    daily = bw.load_daily([c for c, _ in codes])
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    dy_all = bw.add_rank_fields(dy_all, daily).assign(pool="daily")

    rows, yrows = [], []
    for rule, ok in RULES.items():
        dy = dy_all[[ok(c, g) for c, g in zip(dy_all["code"], dy_all["gain_2d"])]].reset_index(drop=True)
        for pool_name, sig, mode in (("合并池", pd.concat([wk, dy], ignore_index=True), "mixed"), ("日线单独", dy, "daily")):
            e, t = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority=PRIORITY, pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            td = t[~t["signal"].str.startswith(WEEKLY_TYPES)]
            label = f"{pool_name} 近2日涨幅{rule}"
            rows.append({"方案": label, "日线信号数": len(dy), "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24年化": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "成功率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2),
                         "日线笔数": len(td), "日线成功率%": round((td["ret"] > 0).mean() * 100, 1),
                         "日线平均%": round(td["ret"].mean() * 100, 2)})
            yrows.append({"方案": label, **yearly(e)})

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    g = dy_all["gain_2d"]
    print(f"日线信号（不限涨幅）{len(dy_all)} 条，近2日涨幅分布：<20% {(g < 20).sum()}，20~30% {((g >= 20) & (g < 30)).sum()}，"
          f"30~40% {((g >= 30) & (g < 40)).sum()}，≥40% {(g >= 40).sum()}")
    print("\n== 总览 ==")
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n== 逐年收益% ==")
    print(pd.DataFrame(yrows).to_string(index=False))
    pd.DataFrame(rows).to_csv("stock_result/gain2d_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

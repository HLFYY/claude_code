"""
长周期回测（cache_bs_long：2018起 baostock 前复权，含退市股）
实盘方案：合并池（周线 买点1-W2/W3/买点2 + 日线连续≥2日新高）× RPS120(1位小数)高→低,同分10日弱→强 × 开盘涨幅>6%不买 × 实盘约束
大盘：10日线 + 底分型次日确认 + 顶分型次日确认清仓/突破顶点再进场；首日 40%，开仓当天指数涨>3% 首日 70%（MKT_KW）
大盘指数：深证综指（bw.INDEX_CODE）
对照：理想成交、周线单独、日线单独、不冻结、旧大盘规则、无大盘择时、深证成指做大盘、指数持有
用法：python backtest/backtest_long_run.py > stock_result/long_run.log 2>&1
     （选股信号缓存在 cache_bs_long/_weekly_signals.pkl、_daily_breakout_signals_c2-inf.pkl，改了选股条件要删掉重提）
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd

REAL = {"max_open_gap": 0.06}            # 开盘涨幅>6%不买（2026-11-08 由 7% 改）；其余见 bw.REAL_DEFAULT：本金80万整手、实测费率、止损0.2%/尾盘0.1%滑点
WEEKLY_TYPES = ("买点1-W2", "买点1-W3", "买点2")
PRIORITY = "RPS120(1位小数)高→低,同分10日弱→强"
MKT_KW = {"first_lv": 0.4, "strong": (0.025, 0.7), "top_bottom": "defer_low", "strong_nofreeze": (7, 0.025)}   # 同 stock_check_xinlang FIRST_LV / STRONG_UP_* / TOP_BOTTOM_DEFER / STRONG_NOFREEZE_DAYS


def yearly(eq):
    ye = eq.resample("A").last()
    yr = (ye / ye.shift(1).fillna(1) - 1) * 100
    return {str(k.year): round(v, 1) for k, v in yr.items()}


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))          # 深证成指，对照
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    idx_cz = idx_cz[idx_cz["date"].isin(idx["date"])].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    mix = pd.concat([wk, dy], ignore_index=True)
    print(f"区间 {idx['date'].iloc[0].date()} ~ {idx['date'].iloc[-1].date()}（{len(idx)} 交易日），"
          f"周线信号 {len(wk)}，日线(连续≥2日)信号 {len(dy)}，股票 {len(daily)} 只（含退市）")

    mkt_new = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    mkt_old = bw.market_levels(idx, "break_k3c")
    mkt_all = mkt_new.copy(); mkt_all["level"] = 1.0; mkt_all["clear"] = False
    print(f"大盘新规则：在场 {(mkt_new['level'] > 0).mean() * 100:.1f}%，满仓 {(mkt_new['level'] == 1).mean() * 100:.1f}%，"
          f"清仓 {int(mkt_new['clear'].sum())} 次")

    cases = [  # (名称, 信号, 池模式, 大盘, 冻结, 成交)
        ("★实盘方案：合并池", mix, "mixed", mkt_new, True, REAL),
        ("合并池 理想成交", mix, "mixed", mkt_new, True, None),
        ("合并池 不冻结", mix, "mixed", mkt_new, False, REAL),
        ("合并池 旧大盘规则", mix, "mixed", mkt_old, True, REAL),
        ("合并池 无大盘择时", mix, "mixed", mkt_all, True, REAL),
        ("合并池 深证成指做大盘", mix, "mixed", bw.market_levels_v2(idx_cz, True, "confirm", **MKT_KW), True, REAL),
        ("周线单独", wk, "weekly", mkt_new, True, REAL),
        ("日线单独", dy, "daily", mkt_new, True, REAL),
    ]
    rows, yrows, curves = [], [], {}
    for name, sig, mode, m, fz, real in cases:
        e, t = bw.run_portfolio(sig, daily, m, use_freeze=fz, priority=PRIORITY, pool_mode=mode, real=real)
        e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
        p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
        rows.append({"方案": name, "总收益%": p["总收益%"], "年化%": p["年化%"], "最大回撤%": p["最大回撤%"], "夏普": p["夏普"],
                     "18-24年化%": p1["年化%"], "18-24回撤%": p1["最大回撤%"], "24-26年化%": p2["年化%"], "24-26回撤%": p2["最大回撤%"],
                     "笔数": len(t), "个股成功率%": round((t["ret"] > 0).mean() * 100, 1), "个股平均%": round(t["ret"].mean() * 100, 2)})
        yrows.append({"方案": name, **yearly(e)})
        curves[name] = e
        if name.startswith("★"):
            t.to_csv("stock_result/long_实盘方案_trades.csv", index=False, encoding="utf-8-sig")
    bench = idx.set_index("date")["close"]; bench = bench / bench.iloc[0]
    p = bw.perf(bench, "")
    rows.append({"方案": f"{bw.INDEX_NAME}持有", "总收益%": p["总收益%"], "年化%": p["年化%"], "最大回撤%": p["最大回撤%"], "夏普": p["夏普"]})
    yrows.append({"方案": f"{bw.INDEX_NAME}持有", **yearly(bench)})
    curves[bw.INDEX_NAME] = bench

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv("stock_result/long_summary.csv", index=False, encoding="utf-8-sig")
    yr.to_csv("stock_result/long_yearly.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(curves).to_csv("stock_result/long_equity.csv", encoding="utf-8-sig")


if __name__ == "__main__":
    main()

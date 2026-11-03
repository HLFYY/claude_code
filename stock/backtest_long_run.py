"""
长周期回测（cache_bs_long：2018起 baostock 前复权，含退市股）
大盘：10日线 + 底分型次日确认 + 顶分型次日确认清仓/突破顶点再进场（market_levels_v2(True, "confirm")）
方案：周线/日线 × RPS120高→低 / 10日涨幅强→弱；资金冻结：到下周一 / 1天 / 不冻结
对照：旧大盘规则（仅10日线+底分型不确认、无顶分型）、去掉大盘择时、深证成指持有
用法：python backtest_long_run.py（选股信号缓存在 cache_bs_long/_*_signals.pkl，改了选股条件要删掉重提）
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd


def yearly(eq):
    ye = eq.resample("A").last()
    yr = (ye / ye.shift(1).fillna(1) - 1) * 100
    return {str(k.year): round(v, 1) for k, v in yr.items()}


def main():
    codes = bw.stock_codes()
    wk_sig = bw.extract_signals()
    dy_sig = bd.extract_signals(codes)
    start = max(wk_sig["sig_date"].min(), dy_sig["sig_date"].min())
    wk_sig = wk_sig[wk_sig["sig_date"] >= start].reset_index(drop=True)
    dy_sig = dy_sig[dy_sig["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx = idx[idx["date"] >= start].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    wk_sig = bw.add_rank_fields(wk_sig, daily)
    dy_sig = bw.add_rank_fields(dy_sig, daily)
    print(f"区间 {idx['date'].iloc[0].date()} ~ {idx['date'].iloc[-1].date()}（{len(idx)} 交易日），"
          f"周线信号 {len(wk_sig)}，日线信号 {len(dy_sig)}，股票 {len(daily)} 只（含退市）")

    mkt_new = bw.market_levels_v2(idx, True, "confirm")
    mkt_old = bw.market_levels(idx, "break_k3c")
    mkt_all = mkt_new.copy(); mkt_all["level"] = 1.0; mkt_all["clear"] = False
    for nm, m in (("新规则", mkt_new), ("旧规则", mkt_old)):
        print(f"大盘{nm}：在场 {(m['level'] > 0).mean() * 100:.1f}%，满仓 {(m['level'] == 1).mean() * 100:.1f}%，清仓 {int(m['clear'].sum())} 次")

    plans = [
        ("周线_RPS高→低", wk_sig, "RPS120高→低", "weekly"),
        ("周线_10日强→弱", wk_sig, "10日涨幅排名强→弱", "weekly"),
        ("日线_RPS高→低", dy_sig, "RPS120高→低", "daily"),
        ("日线_10日强→弱", dy_sig, "10日涨幅排名强→弱", "daily"),
    ]
    variants = [
        ("新规则+冻结到周一", mkt_new, True),
        ("新规则+冻结1天", mkt_new, 1),
        ("新规则+不冻结", mkt_new, False),
        ("旧规则+冻结到周一", mkt_old, True),
        ("无大盘择时+冻结到周一", mkt_all, True),
    ]
    rows, yrows, curves = [], [], {}
    for pname, sig, pr, mode in plans:
        for vname, m, fz in variants:
            e, t = bw.run_portfolio(sig, daily, m, use_freeze=fz, priority=pr, pool_mode=mode)
            p = bw.perf(e, "")
            rows.append({"方案": pname, "规则": vname, "总收益%": p["总收益%"], "年化%": p["年化%"],
                         "最大回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"], "笔数": len(t),
                         "个股成功率%": round((t["ret"] > 0).mean() * 100, 1),
                         "个股平均%": round(t["ret"].mean() * 100, 2)})
            yrows.append({"方案": pname, "规则": vname, **yearly(e)})
            curves[f"{pname}|{vname}"] = e
            if vname == "新规则+冻结到周一":
                t.to_csv(f"stock_result/long_{pname}_trades.csv", index=False, encoding="utf-8-sig")
    bench = idx.set_index("date")["close"]; bench = bench / bench.iloc[0]
    p = bw.perf(bench, "")
    rows.append({"方案": "深证成指持有", "规则": "", "总收益%": p["总收益%"], "年化%": p["年化%"],
                 "最大回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"]})
    yrows.append({"方案": "深证成指持有", "规则": "", **yearly(bench)})
    curves["深证成指"] = bench

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

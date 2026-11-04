"""
回测加入北交所的对比（其余按已确认的实盘方案：合并池 × RPS120(1位小数)高→低,同分10日弱→强 × 开盘>7%不买 × 实盘约束 × 深证综指）
  北交所数据：fetch_bj_eastmoney.py（东方财富前复权，仅现存股票，2020 年精选层起）；涨跌幅 30%，≥100 股后 1 股递增
  含北交所时，RPS120 / 10日排名的全市场分布也含北交所
用法：python backtest_bj.py > stock_result/bj.log 2>&1（北交所信号缓存 cache_bs_long/_weekly_signals_bj.pkl、_daily_breakout_signals_bj_c2-inf.pkl）
"""
import pickle
import sys
from multiprocessing import Pool

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

PRIORITY = "RPS120(1位小数)高→低,同分10日弱→强"


def _cached(f, fn, args):
    if f.exists():
        return pickle.load(open(f, "rb"))
    rows = []
    with Pool(8) as pool:
        for r in pool.imap_unordered(fn, args, chunksize=5):
            rows.extend(r)
    sig = pd.DataFrame(rows)
    pickle.dump(sig, open(f, "wb"))
    return sig


def main():
    codes = bw.stock_codes()
    bj_codes = [x for x in bw.stock_codes(include_bj=True) if x[0].startswith("bj")]
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    wk_bj = _cached(bw.LONG_DIR / "_weekly_signals_bj.pkl", bw.signals_for_stock, bj_codes)
    wk_bj = wk_bj[wk_bj["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    dy_bj = _cached(bw.LONG_DIR / "_daily_breakout_signals_bj_c2-inf.pkl", bd.signals_for_stock,
                    [(c, n, 2, None) for c, n in bj_codes])
    dy_bj = dy_bj[dy_bj["sig_date"] >= start].reset_index(drop=True)
    print(f"北交所 {len(bj_codes)} 只：周线信号 {len(wk_bj)}，日线信号 {len(dy_bj)}；沪深：周线 {len(wk)}，日线 {len(dy)}")

    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    mkt = bw.market_levels_v2(idx, True, "confirm")
    daily = bw.load_daily([c for c, _ in codes])
    daily_all = {**daily, **bw.load_daily([c for c, _ in bj_codes])}

    base_wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    base_dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    all_wk = bw.add_rank_fields(pd.concat([wk, wk_bj], ignore_index=True), daily_all).assign(pool="weekly")
    all_dy = bw.add_rank_fields(pd.concat([dy, dy_bj], ignore_index=True), daily_all).assign(pool="daily")
    cases = [  # 名称, 信号, 池模式, 行情, real
        ("合并池 不含北交所", pd.concat([base_wk, base_dy], ignore_index=True), "mixed", daily, REAL),
        ("合并池 含北交所", pd.concat([all_wk, all_dy], ignore_index=True), "mixed", daily_all, REAL),
        ("合并池 含北交所(北交所滑点+1%)", pd.concat([all_wk, all_dy], ignore_index=True), "mixed", daily_all, {**REAL, "slip_bj": 0.01}),
        ("周线单独 不含北交所", base_wk, "weekly", daily, REAL),
        ("周线单独 含北交所", all_wk, "weekly", daily_all, REAL),
        ("日线单独 不含北交所", base_dy, "daily", daily, REAL),
        ("日线单独 含北交所", all_dy, "daily", daily_all, REAL),
    ]
    rows, yrows = [], []
    for name, sig, mode, dd, real in cases:
        e, t = bw.run_portfolio(sig, dd, mkt, use_freeze=True, priority=PRIORITY, pool_mode=mode, real=real)
        e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
        p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
        tb = t[t["code"].str.startswith("bj")]
        rows.append({"方案": name, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                     "18-24年化": p1["年化%"], "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                     "笔数": len(t), "成功率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2),
                     "北交所笔数": len(tb), "北交所成功率%": round((tb["ret"] > 0).mean() * 100, 1) if len(tb) else None,
                     "北交所平均%": round(tb["ret"].mean() * 100, 2) if len(tb) else None})
        yrows.append({"方案": name, **yearly(e)})
        if name == "合并池 含北交所":
            tb.to_csv("stock_result/bj_trades.csv", index=False, encoding="utf-8-sig")
        print(f"done {name}", file=sys.stderr, flush=True)

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    print("\n== 总览 ==")
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n== 逐年收益% ==")
    print(pd.DataFrame(yrows).to_string(index=False))
    pd.DataFrame(rows).to_csv("stock_result/bj_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

"""
周线选股参数改动回测（2026-11-05 提议）：其余按实盘方案（合并池 × RPS120(1位)同分10日弱→强 × 开盘>7%不买 × 深证综指 + 首日4/7层）
  旧（实盘）：bp1_vol_ratio 1.6 / bp1_confirm_warm_max 1.5 / bp1_confirm_gain_max 0.15 / min_price 3 / min_market_cap 20 / min_weeks_data 40
  新：        1.5 / 1.8 / 0.20 / 2 / 10 / 26
  拆开看：只改买点1三项 / 只改通用过滤三项
信号缓存：cache_bs_long/_weekly_signals_{组名}.pkl（旧=_weekly_signals.pkl）
用法：python backtest/backtest_weekly_cfg.py > stock_result/weekly_cfg.log 2>&1
"""
import pickle
import sys
from multiprocessing import Pool

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

BP1_OLD = {"bp1_vol_ratio": 1.6, "bp1_confirm_warm_max": 1.5, "bp1_confirm_gain_max": 0.15}
BP1_NEW = {"bp1_vol_ratio": 1.5, "bp1_confirm_warm_max": 1.8, "bp1_confirm_gain_max": 0.20}
FLT_OLD = {"min_price": 3.0, "min_market_cap": 20, "min_weeks_data": 40}
FLT_NEW = {"min_price": 2.0, "min_market_cap": 10, "min_weeks_data": 26}
COMMON = {"hard_hist_spike_min": 0.15, "bp2_confirm_vol_max": 1.5, "hard_consec_green_max": 10, "bp1_gain_min": 0.05,
          "bp1_confirm_shrink": 0.8, "bp1_max_entry_age": 2}
PARAM_SETS = {
    "旧(实盘)": None,
    "新(全部)": {**COMMON, **BP1_NEW, **FLT_NEW},
    "只改买点1": {**COMMON, **BP1_NEW, **FLT_OLD},
    "只改通用过滤": {**COMMON, **BP1_OLD, **FLT_NEW},
}


def _init(cfg):
    bw.ss.CFG.update(cfg)


def extract(name):
    if PARAM_SETS[name] is None:
        return pickle.load(open(bw.LONG_DIR / "_weekly_signals.pkl", "rb"))
    f = bw.LONG_DIR / f"_weekly_signals_cfg1105_{name}.pkl"
    if f.exists():
        return pickle.load(open(f, "rb"))
    rows, codes = [], bw.stock_codes()
    with Pool(8, initializer=_init, initargs=(PARAM_SETS[name],)) as pool:
        for k, r in enumerate(pool.imap_unordered(bw.signals_for_stock, codes, chunksize=20)):
            rows.extend(r)
            if k % 1000 == 0:
                print(f"[{name}] 选股 {k}/{len(codes)} 信号 {len(rows)}", file=sys.stderr, flush=True)
    sig = pd.DataFrame(rows)
    pickle.dump(sig, open(f, "wb"))
    return sig


def main():
    codes = bw.stock_codes()
    wks = {n: extract(n) for n in PARAM_SETS}
    wks = {n: w[w["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True) for n, w in wks.items()}
    start = wks["旧(实盘)"]["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    mkt = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    weeks = pd.period_range(idx["date"].iloc[0], idx["date"].iloc[-1], freq="W-FRI")
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)

    srows = []
    for n, w in wks.items():
        w = w[w["sig_date"] >= start]
        cnt = w.groupby(w["sig_date"].dt.to_period("W-FRI")).size().reindex(weeks, fill_value=0)
        srows.append({"参数": n, "信号数": len(w), "周均": round(cnt.mean(), 1), "中位": cnt.median(),
                      "0只周%": round((cnt == 0).mean() * 100, 1), "<4只周%": round((cnt < 4).mean() * 100, 1),
                      "<10只周%": round((cnt < 10).mean() * 100, 1),
                      **{k: int((w["signal"].str.startswith(k)).sum()) for k in WEEKLY_TYPES}})
    print("== 周线股票池规模（每周信号数）==")
    print(pd.DataFrame(srows).to_string(index=False), flush=True)

    rows, yrows = [], []
    for n, w in wks.items():
        w = bw.add_rank_fields(w[w["sig_date"] >= start].reset_index(drop=True), daily).assign(pool="weekly")
        mix = pd.concat([w, dy], ignore_index=True)
        for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", w, "weekly")):
            e, t = bw.run_portfolio(sig, daily, mkt, priority=PRIORITY, pool_mode=mode, real=REAL)
            ex = e.attrs["exposure"]
            full = ex[ex["allowed"] > 0]
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            label = f"{n} {pool_name}"
            rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "笔数": len(t), "周/日笔数": f"{t['signal'].str.startswith(WEEKLY_TYPES).sum()}/{(~t['signal'].str.startswith(WEEKLY_TYPES)).sum()}",
                         "胜率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2),
                         "层数利用率%": round(full["used"].sum() / full["allowed"].sum() * 100, 1)})
            yrows.append({"方案": label, **yearly(e)})
            print(rows[-1], flush=True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv("stock_result/weekly_cfg_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

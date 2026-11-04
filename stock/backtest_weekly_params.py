"""
周线选股参数对比（其余按已确认的实盘方案：合并池 × RPS120高→低 × 开盘>7%不买 × 实盘约束 × 深证综指）
  原始PDF：bp1_vol_ratio 2 / bp1_confirm_warm_max 1.3 / bp1_confirm_gain_max 0.12 / hard_hist_spike_min 0.2 / bp2_confirm_vol_max 1.3
  当前代码：1.6 / 1.5 / 0.15 / 0.15 / 1.5
  注释口径：同当前代码，hard_hist_spike_min 0.1
留池条件：last=前一日收盘≥突破价（现逻辑）；all=信号后每日收盘都≥突破价
用法：python backtest_weekly_params.py > stock_result/weekly_params.log 2>&1（各组信号缓存 cache_bs_long/_weekly_signals_{组名}.pkl）
"""
import pickle
import sys
from multiprocessing import Pool

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

PARAM_SETS = {
    "原始PDF": {"bp1_vol_ratio": 2.0, "bp1_confirm_warm_max": 1.3, "bp1_confirm_gain_max": 0.12,
              "hard_hist_spike_min": 0.2, "bp2_confirm_vol_max": 1.3},
    "当前代码": {"bp1_vol_ratio": 1.6, "bp1_confirm_warm_max": 1.5, "bp1_confirm_gain_max": 0.15,
              "hard_hist_spike_min": 0.15, "bp2_confirm_vol_max": 1.5},
    "注释口径": {"bp1_vol_ratio": 1.6, "bp1_confirm_warm_max": 1.5, "bp1_confirm_gain_max": 0.15,
              "hard_hist_spike_min": 0.1, "bp2_confirm_vol_max": 1.5},
}


def _init(cfg):
    bw.ss.CFG.update(cfg)


def extract(name):
    f = bw.LONG_DIR / f"_weekly_signals_{name}.pkl"
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
    start = min(w["sig_date"].min() for w in wks.values())
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    mkt = bw.market_levels_v2(idx, True, "confirm")
    dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    weeks = pd.period_range(idx["date"].iloc[0], idx["date"].iloc[-1], freq="W-FRI")

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)

    # 周线股票池规模
    srows = []
    for n, w in wks.items():
        cnt = w.groupby(w["sig_date"].dt.to_period("W-FRI")).size().reindex(weeks, fill_value=0)
        srows.append({"参数": n, "信号数": len(w), "周均": round(cnt.mean(), 1), "中位": cnt.median(),
                      "0只周%": round((cnt == 0).mean() * 100, 1), "<4只周%": round((cnt < 4).mean() * 100, 1),
                      "<10只周%": round((cnt < 10).mean() * 100, 1),
                      **{k: int((w["signal"].str.startswith(k)).sum()) for k in WEEKLY_TYPES}})
    print("== 周线股票池规模（每周信号数）==")
    print(pd.DataFrame(srows).to_string(index=False))

    rows, yrows = [], []
    for n, w in wks.items():
        w = bw.add_rank_fields(w, daily).assign(pool="weekly")
        mix = pd.concat([w, dy], ignore_index=True)
        for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", w, "weekly")):
            for hold in ("last", "all"):
                e, t = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority="RPS120高→低",
                                        pool_mode=mode, real=REAL, hold_rule=hold)
                ex = e.attrs["exposure"]
                full = ex[ex["allowed"] > 0]
                e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
                p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
                label = f"{n} {pool_name} 留池{'前日收盘' if hold == 'last' else '每日收盘'}"
                rows.append({"方案": label, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                             "18-24年化": p1["年化%"], "24-26年化": p2["年化%"], "笔数": len(t),
                             "周/日笔数": f"{t['signal'].str.startswith(WEEKLY_TYPES).sum()}/{(~t['signal'].str.startswith(WEEKLY_TYPES)).sum()}",
                             "成功率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2),
                             "开仓日满额%": round((full["used"] >= full["allowed"] - 1e-6).mean() * 100, 1),
                             "层数利用率%": round(full["used"].sum() / full["allowed"].sum() * 100, 1)})
                yrows.append({"方案": label, **yearly(e)})
                print(f"done {label}", file=sys.stderr, flush=True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv("stock_result/weekly_params_summary.csv", index=False, encoding="utf-8-sig")
    yr.to_csv("stock_result/weekly_params_yearly.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

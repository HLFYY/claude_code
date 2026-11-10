"""
全市场新高股次日表现择时（单变量，按现行实盘方案，合并池）
指标（第 t 日收盘可算，用于 t+1 日开仓决策）：
  第 t-1 日收盘创 100 日收盘新高的股票（非 ST、正常交易、上市满 120 日），第 t 日的表现：
    cc = 收盘/昨收-1（次日涨跌）     oc = 收盘/开盘-1（次日开盘买入当天盈亏，贴近"买入日失败"）
  当日等权平均 → 近 N 日（按新高股数加权）平均
规则：指标 < 阈值 → 次日不开新仓（no_new）或开仓上限压到 X 层（open_cap，只限开仓不卖）
用法：python backtest/backtest_breadth_nh.py > stock_result/breadth_nh.log 2>&1
"""
import multiprocessing as mp
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
NH = 100
OUT = "stock_result/breadth_nh_summary.csv"


def breadth(codes):
    """返回 DataFrame(date, n, cc_sum, oc_sum, cc_win)：t 日 = 昨日新高股今日表现"""
    acc = {}
    for code, _ in codes:
        try:
            d = pickle.load(open(bw.LONG_DIR / f"{code.replace('.', '_')}.pkl", "rb"))
        except Exception:
            continue
        if d is None or len(d) < NH + 25:
            continue
        d = d.sort_values("date").reset_index(drop=True)
        c, o = d["close"].astype(float), d["open"].astype(float)
        ok = (d["tradestatus"].astype(str) == "1") & (d["isST"].astype(str) != "1")
        nh = (c > c.shift(1).rolling(NH).max()) & ok & (pd.Series(np.arange(len(d))) >= 120)
        prev_nh = nh.shift(1, fill_value=False) & ok & (o > 0)
        if not prev_nh.any():
            continue
        cc = (c / c.shift(1) - 1)[prev_nh]
        oc = (c / o - 1)[prev_nh]
        df = pd.DataFrame({"date": d["date"][prev_nh].values, "cc": cc.values, "oc": oc.values})
        acc[code] = df
    a = pd.concat(acc.values(), ignore_index=True)
    g = a.groupby("date")
    return pd.DataFrame({"n": g.size(), "cc_sum": g["cc"].sum(), "oc_sum": g["oc"].sum(),
                         "cc_win": g["cc"].apply(lambda x: (x > 0).sum())}).reset_index()


def signal(b, dates, col, n):
    """t 日收盘后的近 n 日加权平均（%），对齐到 dates 后整体后移 1 天 = 用于当天开盘决策"""
    b = b.set_index("date").reindex(dates).fillna(0)
    v = b[col].rolling(n, min_periods=1).sum() / b["n"].rolling(n, min_periods=1).sum().replace(0, np.nan) * 100
    return v.shift(1).values


def run(a):
    name, rule = a
    mkt = G["mkt"].copy()
    if rule:
        col, n, thr, cap = rule
        v = -G["sig"][(col[1:], n)] if col.startswith("-") else G["sig"][(col, n)]   # "-cc"=反向（指标 > -thr）
        weak = np.nan_to_num(v, nan=99) < thr
        if cap == 0:
            mkt["no_new"] = mkt["no_new"].values | weak
        else:
            mkt["open_cap"] = np.where(weak, cap, np.nan)
        wk_rate = round(weak.mean() * 100, 1)
    else:
        wk_rate = 0
    e, t = bw.run_portfolio(G["mix"], G["daily"], mkt, priority=PRIORITY, pool_mode="mixed", real=REAL)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"方案": name, "弱天%": wk_rate, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), **yearly(e)}


def cases():
    cs = [("实盘", None)]
    for col in ("cc", "oc"):
        for n in (3, 5, 10, 20):
            for thr in ((-1.0, -0.5, 0.0, 0.5) if col == "cc" else (-1.5, -1.0, -0.5, 0.0)):
                cs.append((f"{col} 近{n}日<{thr}% 不开", (col, n, thr, 0)))
    for col, n, thr in (("cc", 5, 0.0), ("cc", 10, 0.0), ("oc", 5, -0.5), ("oc", 10, -0.5)):
        for cap in (0.3, 0.5, 0.7):
            cs.append((f"{col} 近{n}日<{thr}% 开仓上限{int(cap * 10)}层", (col, n, thr, cap)))
    return cs


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    G["daily"] = bw.load_daily([c for c, _ in codes])
    G["mkt"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    G["mix"] = pd.concat([bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly"),
                          bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")], ignore_index=True)
    b = breadth(codes)
    b.to_csv("stock_result/breadth_nh_daily.csv", index=False, encoding="utf-8-sig")
    dates = pd.DatetimeIndex(G["mkt"]["date"])
    G["sig"] = {(col, n): signal(b, dates, col + "_sum", n) for col in ("cc", "oc") for n in (3, 5, 10, 20)}
    # 指标描述：各大回撤期的指标均值
    desc = pd.DataFrame({"date": dates, **{f"{c}{n}": G["sig"][(c, n)] for c, n in G["sig"]}}).set_index("date")
    print("指标分布（%）:\n", desc.describe().round(2).to_string())
    for a, z in (("2018-11-19", "2019-02-15"), ("2019-04-08", "2019-11-21"), ("2020-02-24", "2020-03-10"),
                 ("2021-01-04", "2021-08-11"), ("2025-02-12", "2025-04-29"), ("2025-12-10", "2026-02-11")):
        print(f"回撤期 {a}~{z} 指标均值:", desc.loc[a:z].mean().round(2).to_dict())
    print("全期均值:", desc.mean().round(2).to_dict(), flush=True)
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, cases(), chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    print(res.to_string(index=False))
    res.to_csv(OUT, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

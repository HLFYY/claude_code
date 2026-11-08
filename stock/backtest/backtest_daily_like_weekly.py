"""
日线选股参考周线选股条件，逐条加到现行日线 6 条上（每次只加一条），用当前实盘方案回测（合并池 + 日线单独）
周线条件 → 日线对照：
  通用过滤：股价≥3 / 周均换手≥0.5%（近20日换手和÷4）/ 流通市值≥20亿
  硬过滤  ：本周涨幅≥30%过热 → 近5日涨幅<30% / 连红≥10周 → 连续收红<N日 / 历史单周涨幅≥15%（辨识度）→ 近3年有5日涨幅≥15%
  买点1 W1：倍量≥1.6 + 周涨幅≥5% + 站上20MA且20MA走平向上 → 首日突破涨幅≥5% / 首日成交额≥前日×1.6 / 站上MA20且MA20向上
  买点1 W2：成交额≤W1×1.5、缩量≤0.8 最优 → 第2日成交额≤首日×1.5 / ≤×0.8
用法：python backtest/backtest_daily_like_weekly.py > stock_result/daily_like_weekly.log 2>&1（特征缓存 cache_bs_long/_daily_features_wk.pkl）
"""
import pickle
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

FEAT_FILE = bw.LONG_DIR / "_daily_features_wk.pkl"


def features_for_stock(args):
    code, _ = args
    _, d = bw.load_stock(code)
    if d is None or len(d) < 130:
        return []
    o, h, l, c, a = (d[k].values.astype(float) for k in ("open", "high", "low", "close", "amount"))
    turn, vol = d["turnover"].values.astype(float), d["volume"].values.astype(float)
    st = (d["isST"] == "1").values
    pmax100 = pd.Series(h).rolling(100).max().shift(1).values
    nh = c >= pmax100
    ma20 = pd.Series(c).rolling(20).mean().values
    amt5 = pd.Series(a).rolling(5).mean().shift(1).values
    r5 = np.full(len(c), np.nan)
    r5[5:] = c[5:] / c[:-5] - 1
    spike = pd.Series(r5).rolling(750, min_periods=150).max().values        # 近3年最大5日涨幅
    green = np.zeros(len(c), int)
    for i in range(len(c)):
        green[i] = green[i - 1] + 1 if i and c[i] >= o[i] else (1 if c[i] >= o[i] else 0)
    out = []
    for i in range(106, len(d)):
        if not (nh[i] and nh[i - 1]) or st[i] or c[i - 2] <= 0 or not amt5[i] or amt5[i] <= 0:
            continue
        k = 1
        while k < 30 and i - k >= 100 and nh[i - k]:
            k += 1
        f0 = i - k + 1                                                       # 首日突破
        body = abs(c[i] - o[i])
        t20 = turn[i - 19:i + 1]
        cap = (vol[i - 19:i + 1].mean() / (t20.mean() / 100) * c[i] / 1e8) if t20.mean() > 0 else np.nan
        out.append({
            "code": code, "sig_date": d["date"].iloc[i], "signal": "B突破第二日", "daily_ok": "是",
            "breakout_price": round(c[i], 2),
            "atr": round(np.mean(np.maximum(h[i - 13:i + 1] - l[i - 13:i + 1],
                                            np.maximum(abs(h[i - 13:i + 1] - c[i - 14:i]), abs(l[i - 13:i + 1] - c[i - 14:i])))) / c[i] * 100, 2),
            "score": a[i] / amt5[i], "vol_ratio": a[i] / amt5[i],
            "gain_2d": (c[i] / c[i - 2] - 1) * 100, "pct": (c[i] / c[i - 1] - 1) * 100,
            "body_pct": (c[i] / o[i] - 1) * 100, "mid_ok": c[i] >= (h[i] + l[i]) / 2, "upper_ok": (h[i] - c[i]) < body,
            "consec": k, "price": c[i], "turn_wk": t20.sum() / 4, "cap": cap,
            "gain5": r5[i] * 100, "green": green[i], "spike": spike[i] * 100 if not np.isnan(spike[i]) else np.nan,
            "f_pct": (c[f0] / c[f0 - 1] - 1) * 100, "f_amt": a[f0] / a[f0 - 1] if a[f0 - 1] > 0 else np.nan,
            "d2_amt": a[i] / a[i - 1] if a[i - 1] > 0 else np.nan,
            "ma20_ok": c[i] > ma20[i] and ma20[i] >= ma20[i - 5],
        })
    return out


def extract():
    if FEAT_FILE.exists():
        return pickle.load(open(FEAT_FILE, "rb"))
    rows = []
    with Pool(8) as pool:
        for k, r in enumerate(pool.imap_unordered(features_for_stock, bw.stock_codes(), chunksize=20)):
            rows.extend(r)
            if k % 1000 == 0:
                print(f"特征 {k} 条 {len(rows)}", file=sys.stderr, flush=True)
    f = pd.DataFrame(rows)
    pickle.dump(f, open(FEAT_FILE, "wb"))
    return f


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    f = extract()
    f = f[f["sig_date"] >= start].reset_index(drop=True)
    B = (f["vol_ratio"] >= 1.5) & (f["gain_2d"] < 20) & (f["body_pct"] > 1) & (f["pct"] > 1) & f["mid_ok"] & f["upper_ok"]
    ref = bd.extract_signals(codes, 2, None)
    ref = ref[ref["sig_date"] >= start]
    both = len(f[B].merge(ref[["code", "sig_date"]], on=["code", "sig_date"]))
    print(f"基准 {B.sum()} 条，实盘提取 {len(ref)} 条，重合 {both}", flush=True)

    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    mkt = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    daily = bw.load_daily([c for c, _ in codes])
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    f = bw.add_rank_fields(f, daily).assign(pool="daily")
    gen = (f["price"] >= 3) & (f["turn_wk"] >= 0.5) & (f["cap"] >= 20)
    hard = (f["gain5"] < 30) & (f["green"] < 10) & (f["spike"] >= 15)
    if len(sys.argv) > 1 and sys.argv[1] == "spike":                 # 辨识度阈值稳健性 + 组合
        cases = {"基准(现行6条)": B, **{f"加 辨识度≥{x}%": B & (f["spike"] >= x) for x in (10, 12, 15, 18, 20)},
                 "加 辨识度≥15% + 周均换手≥0.5%": B & (f["spike"] >= 15) & (f["turn_wk"] >= 0.5),
                 "加 辨识度≥15% + 首日涨幅≥5%": B & (f["spike"] >= 15) & (f["f_pct"] >= 5),
                 "加 辨识度≥15% + 连续收红<10日": B & (f["spike"] >= 15) & (f["green"] < 10)}
    else:
        cases = None
    cases = cases or {
        "基准(现行6条)": B,
        "加 股价≥3": B & (f["price"] >= 3),
        "加 周均换手≥0.5%": B & (f["turn_wk"] >= 0.5),
        "加 流通市值≥20亿": B & (f["cap"] >= 20),
        "加 通用过滤三条": B & gen,
        "加 近5日涨幅<30%": B & (f["gain5"] < 30),
        "加 连续收红<10日": B & (f["green"] < 10),
        "加 连续收红<5日": B & (f["green"] < 5),
        "加 辨识度(近3年5日涨幅≥15%)": B & (f["spike"] >= 15),
        "加 辨识度(近3年5日涨幅≥25%)": B & (f["spike"] >= 25),
        "加 硬过滤三条": B & hard,
        "加 首日涨幅≥5%": B & (f["f_pct"] >= 5),
        "加 首日成交额≥前日×1.6": B & (f["f_amt"] >= 1.6),
        "加 站上MA20且MA20向上": B & f["ma20_ok"],
        "加 第2日成交额≤首日×1.5": B & (f["d2_amt"] <= 1.5),
        "加 第2日缩量(≤首日×0.8)": B & (f["d2_amt"] <= 0.8),
        "加 通用+硬过滤全部": B & gen & hard,
    }
    rows, yrows = [], []
    for name, m in cases.items():
        dy = f[m].reset_index(drop=True)
        for pool_name, sig, mode in (("合并池", pd.concat([wk, dy], ignore_index=True), "mixed"), ("日线单独", dy, "daily")):
            e, t = bw.run_portfolio(sig, daily, mkt, priority=PRIORITY, pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            td = t[~t["signal"].str.startswith(WEEKLY_TYPES)]
            rows.append({"池": pool_name, "条件": name, "日线信号": len(dy), "年化%": p["年化%"], "回撤%": p["最大回撤%"],
                         "夏普": p["夏普"], "18-24年化": p1["年化%"], "18-24回撤": p1["最大回撤%"],
                         "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "日线笔数": len(td), "日线胜率%": round((td["ret"] > 0).mean() * 100, 1),
                         "日线平均%": round(td["ret"].mean() * 100, 2)})
            yrows.append({"池": pool_name, "条件": name, **yearly(e)})
        print(f"done {name}", rows[-2]["年化%"], rows[-1]["年化%"], file=sys.stderr, flush=True)

    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    for pn in ("合并池", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    print("\n== 逐年收益%（合并池）==")
    print(yr[yr["池"] == "合并池"].drop(columns="池").to_string(index=False))
    res.to_csv(f"stock_result/daily_like_weekly_summary{'_' + sys.argv[1] if len(sys.argv) > 1 else ''}.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

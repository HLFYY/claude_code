"""
日线选股条件逐项检验（其余按已确认的实盘方案：合并池 × RPS120(1位小数)高→低,同分10日弱→强 × 开盘>7%不买 × 实盘约束 × 深证综指）
做法：放宽提取"100日新高 + 连续≥2日新高"的全部候选，记下各项特征，再按条件组合过滤；基准 = 现行 6 条，应与 bd.extract_signals(codes,2,None) 基本一致
每次只改一项：删条件 / 调阈值 / 加条件，看合并池和日线单独、两段区间是否同向改善
用法：python backtest/backtest_daily_conditions.py > stock_result/daily_conditions.log 2>&1（特征缓存 cache_bs_long/_daily_features.pkl）
"""
import pickle
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, yearly

PRIORITY = "RPS120(1位小数)高→低,同分10日弱→强"
FEAT_FILE = bw.LONG_DIR / "_daily_features.pkl"


def features_for_stock(args):
    code, _ = args
    _, d = bw.load_stock(code)
    if d is None or len(d) < 130:
        return []
    o, h, l, c, a = (d[k].values.astype(float) for k in ("open", "high", "low", "close", "amount"))
    turn, vol = d["turnover"].values.astype(float), d["volume"].values.astype(float)
    st = (d["isST"] == "1").values
    hs = pd.Series(h)
    pmax100 = hs.rolling(100).max().shift(1).values
    pmax250 = hs.rolling(250, min_periods=200).max().shift(1).values
    nh = c >= pmax100
    ma20 = pd.Series(c).rolling(20).mean().values
    ma60 = pd.Series(c).rolling(60).mean().values
    amt5 = pd.Series(a).rolling(5).mean().shift(1).values
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    atr14 = np.concatenate([[np.nan], pd.Series(tr).rolling(14).mean().values])
    turn5 = pd.Series(turn).rolling(5).mean().values
    lim = bw._limit_pct(code)
    out = []
    for i in range(106, len(d)):
        if not (nh[i] and nh[i - 1]) or st[i] or c[i - 2] <= 0 or not amt5[i] or amt5[i] <= 0:
            continue
        k = 1
        while k < 30 and i - k >= 100 and nh[i - k]:
            k += 1
        body = abs(c[i] - o[i])
        cap = (vol[i - 4:i + 1].mean() / (turn[i - 4:i + 1].mean() / 100) * c[i] / 1e8) if turn[i - 4:i + 1].mean() > 0 else np.nan
        out.append({
            "code": code, "sig_date": d["date"].iloc[i], "signal": "B突破第二日", "daily_ok": "是",
            "breakout_price": round(c[i], 2), "atr": round(atr14[i] / c[i] * 100, 2),
            "score": a[i] / amt5[i], "vol_ratio": a[i] / amt5[i],
            "gain_2d": (c[i] / c[i - 2] - 1) * 100, "pct": (c[i] / c[i - 1] - 1) * 100,
            "body_pct": (c[i] / o[i] - 1) * 100, "mid_ok": c[i] >= (h[i] + l[i]) / 2,
            "upper_ok": (h[i] - c[i]) < body, "consec": k,
            "nh250": bool(c[i] >= pmax250[i]) if not np.isnan(pmax250[i]) else False,
            "limit_up": c[i] >= c[i - 1] * (1 + lim) - 0.011,
            "ma20_up": ma20[i] > ma20[i - 5], "above_ma60": c[i] > ma60[i],
            "cap": cap, "turn5": turn5[i], "price": c[i],
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


def base_mask(f, vol=1.5, g2=20, shape=True):
    m = f["vol_ratio"] >= vol
    if g2 is not None:
        m &= f["gain_2d"] < g2
    if shape:
        m &= (f["body_pct"] > 1) & (f["pct"] > 1) & f["mid_ok"] & f["upper_ok"]
    return m


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    f = extract()
    f = f[f["sig_date"] >= start].reset_index(drop=True)
    ref = bd.extract_signals(codes, 2, None)
    ref = ref[ref["sig_date"] >= start]
    b = f[base_mask(f)]
    both = len(b.merge(ref[["code", "sig_date"]], on=["code", "sig_date"]))
    print(f"候选(100日新高+连续≥2日) {len(f)}；基准条件 {len(b)} 条，原提取 {len(ref)} 条，重合 {both}")

    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    mkt = bw.market_levels_v2(idx, True, "confirm")
    daily = bw.load_daily([c for c, _ in codes])
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    f = bw.add_rank_fields(f, daily).assign(pool="daily")
    B = base_mask(f)
    cases = {
        "基准(现行6条)": B,
        "删 K线形态(收红/涨幅>1%/中位/上影)": base_mask(f, shape=False),
        "删 倍量": base_mask(f, vol=0),
        "倍量≥1.2": base_mask(f, vol=1.2),
        "倍量≥2": base_mask(f, vol=2),
        "倍量≥3": base_mask(f, vol=3),
        "加 250日新高": B & f["nh250"],
        "加 突破日非涨停": B & ~f["limit_up"],
        "加 突破日涨幅<7%": B & (f["pct"] < 7),
        "加 突破日涨幅≥3%": B & (f["pct"] >= 3),
        "加 连续新高≤3日": B & (f["consec"] <= 3),
        "加 连续新高≤5日": B & (f["consec"] <= 5),
        "加 MA20向上": B & f["ma20_up"],
        "加 流通市值≥20亿": B & (f["cap"] >= 20),
        "加 流通市值≥50亿": B & (f["cap"] >= 50),
        "加 5日换手≥1%": B & (f["turn5"] >= 1),
        "加 5日换手≤15%": B & (f["turn5"] <= 15),
        "加 股价≥3元": B & (f["price"] >= 3),
        "加 RPS120≥80": B & (f["rps120"] >= 80),
        "加 RPS120≥90": B & (f["rps120"] >= 90),
    }
    rows, yrows = [], []
    for name, m in cases.items():
        dy = f[m].reset_index(drop=True)
        for pool_name, sig, mode in (("合并池", pd.concat([wk, dy], ignore_index=True), "mixed"), ("日线单独", dy, "daily")):
            e, t = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority=PRIORITY, pool_mode=mode, real=REAL)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            td = t[~t["signal"].str.startswith(WEEKLY_TYPES)]
            rows.append({"池": pool_name, "条件": name, "日线信号": len(dy), "年化%": p["年化%"], "回撤%": p["最大回撤%"],
                         "夏普": p["夏普"], "18-24年化": p1["年化%"], "18-24回撤": p1["最大回撤%"],
                         "24-26年化": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                         "日线笔数": len(td), "日线成功率%": round((td["ret"] > 0).mean() * 100, 1),
                         "日线平均%": round(td["ret"].mean() * 100, 2)})
            yrows.append({"池": pool_name, "条件": name, **yearly(e)})
        print(f"done {name}", file=sys.stderr, flush=True)

    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    for pn in ("合并池", "日线单独"):
        print(f"\n== {pn} ==")
        print(res[res["池"] == pn].drop(columns="池").to_string(index=False))
    print("\n== 逐年收益%（合并池）==")
    print(yr[yr["池"] == "合并池"].drop(columns="池").to_string(index=False))
    res.to_csv("stock_result/daily_conditions_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

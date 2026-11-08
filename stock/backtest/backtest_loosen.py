"""
日线 / 周线选股条件逐条放宽（扩大股票池），按实盘方案回测（合并池 × RPS120 × 开盘>7%不买 × 实盘约束 × 深证综指 + 冻结到下周一）
  周线：每次只改 stock_screener.CFG 的一项（重新提取信号，缓存 cache_bs_long/_weekly_signals_loose_{名}.pkl），日线用实盘 6 条
  日线：每次只放宽 6 条中的一条 / 新高窗口 / 连续新高天数（特征缓存 cache_bs_long/_daily_features_loose_w{窗口}.pkl），周线用实盘
用法：
  python backtest/backtest_loosen.py wk     周线单项放宽
  python backtest/backtest_loosen.py dy     日线单项放宽
  python backtest/backtest_loosen.py combo  组合验证（COMBO_WK / COMBO_DY 按单项结果填）
"""
import multiprocessing as mp
import pickle
import sys

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

WK_SETS = {
    "倍量1.6→1.4": {"bp1_vol_ratio": 1.4},
    "W1涨幅5%→3%": {"bp1_gain_min": 0.03},
    "W2放量上限1.5→2.0": {"bp1_confirm_warm_max": 2.0},
    "W2涨幅上限15%→20%": {"bp1_confirm_gain_max": 0.20},
    "入场W3→W4": {"bp1_max_entry_age": 3},
    "买点2反包量能1.5→2.0": {"bp2_confirm_vol_max": 2.0},
    "买点2回踩4→5周": {"bp2_pullback_max": 5},
    "过热30%→40%": {"hard_gain_max": 0.40},
    "连红10→15周": {"hard_consec_green_max": 15},
    "辨识度15%→10%": {"hard_hist_spike_min": 0.10},
    "换手0.5→0.3": {"min_turnover": 0.3},
    "股价3→2": {"min_price": 2.0},
    "市值20→10亿": {"min_market_cap": 10},
    "新高20周→15周": {"ma_new_high": 15},
}
WK_TYPES_EXT = WEEKLY_TYPES + ("买点1-W4",)

G = {}   # fork 共享：daily / mkt / wk0 / dy0


def _init(cfg):
    bw.ss.CFG.update(cfg)


def extract_wk(name):
    return pickle.load(open(bw.SIG_FILE, "rb"))


def extract_wk_cfg(name, cfg):
    f = bw.LONG_DIR / f"_weekly_signals_loose_{name}.pkl"
    if f.exists():
        return pickle.load(open(f, "rb"))
    rows, codes = [], bw.stock_codes()
    with mp.get_context("fork").Pool(8, initializer=_init, initargs=(cfg,)) as pool:
        for r in pool.imap_unordered(bw.signals_for_stock, codes, chunksize=20):
            rows.extend(r)
    sig = pd.DataFrame(rows)
    pickle.dump(sig, open(f, "wb"))
    print(f"[周线 {name}] 信号 {len(sig)}", file=sys.stderr, flush=True)
    return sig


# ─────────── 日线特征（新高窗口 W，连续新高≥1 天都出行）───────────
def feats_for_stock(args):
    code, W = args
    _, d = bw.load_stock(code)
    if d is None or len(d) < W + 30:
        return []
    o, h, l, c, a = (d[k].values.astype(float) for k in ("open", "high", "low", "close", "amount"))
    st = (d["isST"] == "1").values
    nh = c >= pd.Series(h).rolling(W).max().shift(1).values
    amt5 = pd.Series(a).rolling(5).mean().shift(1).values
    out = []
    for i in range(W + 6, len(d)):
        if not nh[i] or st[i] or c[i - 2] <= 0 or not amt5[i] or amt5[i] <= 0:
            continue
        k = 1
        while k < 30 and i - k >= W and nh[i - k]:
            k += 1
        body = abs(c[i] - o[i])
        tr = np.maximum(h[i - 13:i + 1] - l[i - 13:i + 1],
                        np.maximum(abs(h[i - 13:i + 1] - c[i - 14:i]), abs(l[i - 13:i + 1] - c[i - 14:i])))
        out.append({
            "code": code, "sig_date": d["date"].iloc[i], "signal": "B突破第二日", "daily_ok": "是",
            "breakout_price": round(c[i], 2), "atr": round(tr.mean() / c[i] * 100, 2),
            "score": a[i] / amt5[i], "vol_ratio": a[i] / amt5[i],
            "gain_2d": (c[i] / c[i - 2] - 1) * 100, "pct": (c[i] / c[i - 1] - 1) * 100,
            "body_pct": (c[i] / o[i] - 1) * 100, "mid_ok": c[i] >= (h[i] + l[i]) / 2, "upper_ok": (h[i] - c[i]) < body,
            "consec": k,
        })
    return out


def extract_dy(W):
    f = bw.LONG_DIR / f"_daily_features_loose_w{W}.pkl"
    if f.exists():
        return pickle.load(open(f, "rb"))
    rows = []
    with mp.get_context("fork").Pool(8) as pool:
        for r in pool.imap_unordered(feats_for_stock, [(c, W) for c, _ in bw.stock_codes()], chunksize=20):
            rows.extend(r)
    df = pd.DataFrame(rows)
    pickle.dump(df, open(f, "wb"))
    print(f"[日线特征 W{W}] {len(df)}", file=sys.stderr, flush=True)
    return df


def dy_mask(f, vol=1.5, g2=20, body=1, pct=1, mid=True, upper=True, consec=2):
    m = f["consec"] >= consec
    if vol is not None: m &= f["vol_ratio"] >= vol
    if g2 is not None: m &= f["gain_2d"] < g2
    if body is not None: m &= f["body_pct"] > body
    if pct is not None: m &= f["pct"] > pct
    if mid: m &= f["mid_ok"]
    if upper: m &= f["upper_ok"]
    return m


def run_case(args):
    name, wk, dy = args
    daily, mkt = G["daily"], G["mkt"]
    out = []
    for pool_name, sig, mode in (("合并池", pd.concat([wk, dy], ignore_index=True), "mixed"),):
        e, t = bw.run_portfolio(sig, daily, mkt, priority=PRIORITY, pool_mode=mode, real=REAL)
        e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
        p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
        isw = t["signal"].str.startswith(WK_TYPES_EXT)
        out.append(({"方案": name, "周线信号": len(wk), "日线信号": len(dy), "年化%": p["年化%"], "回撤%": p["最大回撤%"],
                     "夏普": p["夏普"], "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"],
                     "24-26回撤": p2["最大回撤%"], "周/日笔数": f"{isw.sum()}/{(~isw).sum()}",
                     "胜率%": round((t["ret"] > 0).mean() * 100, 1), "平均%": round(t["ret"].mean() * 100, 2)},
                    {"方案": name, **yearly(e)}))
    return out


def prep_wk(w, start, types=WEEKLY_TYPES):
    w = w[w["signal"].str.startswith(types) & (w["sig_date"] >= start)].reset_index(drop=True)
    return bw.add_rank_fields(w, G["daily"]).assign(pool="weekly")


def prep_dy(f, m):
    return bw.add_rank_fields(f[m].reset_index(drop=True), G["daily"]).assign(pool="daily")


def main():
    mode = sys.argv[1]
    codes = bw.stock_codes()
    wk_raw = extract_wk(None)
    start = wk_raw[wk_raw["signal"].str.startswith(WEEKLY_TYPES)]["sig_date"].min()
    # 先把要用的信号全部提取好（Pool 8 进程），再加载日线做回测
    if mode == "wk":
        wk_sets = {n: extract_wk_cfg(n, c) for n, c in WK_SETS.items()}
    f100 = extract_dy(100)
    f100 = f100[f100["sig_date"] >= start].reset_index(drop=True)
    if mode == "dy":
        f60, f80, f120 = (extract_dy(w) for w in (60, 80, 120))
        f60, f80, f120 = (x[x["sig_date"] >= start].reset_index(drop=True) for x in (f60, f80, f120))

    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    G["daily"] = bw.load_daily([c for c, _ in codes])
    G["mkt"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    wk0 = prep_wk(wk_raw, start)
    B = dy_mask(f100)
    ref = bd.extract_signals(codes, 2, None)
    ref = ref[ref["sig_date"] >= start]
    print(f"日线基准 {B.sum()} 条，实盘提取 {len(ref)} 条，重合 {len(f100[B].merge(ref[['code', 'sig_date']], on=['code', 'sig_date']))}", flush=True)
    dy0 = prep_dy(f100, B)
    G["wk0"], G["dy0"] = wk0, dy0

    cases = [("基准(实盘)", wk0, dy0)]
    if mode == "wk":
        for n, w in wk_sets.items():
            cases.append((f"周线 {n}", prep_wk(w, start, WK_TYPES_EXT if "W4" in n else WEEKLY_TYPES), dy0))
        cases.append(("周线 池子加买点3", prep_wk(wk_raw, start, WEEKLY_TYPES + ("买点3",)), dy0))
    elif mode == "dy":
        D = {
            "倍量1.5→1.3": dict(vol=1.3), "倍量1.5→1.2": dict(vol=1.2), "倍量1.5→1.0": dict(vol=1.0), "不限量能": dict(vol=None),
            "2日涨幅<20%→<25%": dict(g2=25), "2日涨幅<20%→<30%": dict(g2=30), "不限2日涨幅": dict(g2=None),
            "收红1%→0.5%": dict(body=0.5), "收红1%→0%": dict(body=0), "不要求收红": dict(body=None),
            "涨幅>1%→>0.5%": dict(pct=0.5), "涨幅>1%→>0%": dict(pct=0), "不限涨幅": dict(pct=None),
            "去掉收盘≥中位": dict(mid=False), "去掉上影<实体": dict(upper=False), "去掉K线形态两条": dict(mid=False, upper=False),
            "连续新高2→1天": dict(consec=1),
        }
        for n, kw in D.items():
            cases.append((f"日线 {n}", wk0, prep_dy(f100, dy_mask(f100, **kw))))
        for W, fw in ((60, f60), (80, f80), (120, f120)):
            cases.append((f"日线 新高100→{W}日", wk0, prep_dy(fw, dy_mask(fw))))
    elif mode == "combo":
        cases += COMBO(start, wk_raw, f100)

    with mp.get_context("fork").Pool(4) as pool:
        res = pool.map(run_case, cases, chunksize=1)
    rows = [r for x in res for r, _ in x]
    yrows = [y for x in res for _, y in x]
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res, yr = pd.DataFrame(rows), pd.DataFrame(yrows)
    print("\n== 合并池 总览 ==")
    print(res.to_string(index=False))
    print("\n== 逐年收益% ==")
    print(yr.to_string(index=False))
    res.to_csv(f"stock_result/loosen_{mode}_summary.csv", index=False, encoding="utf-8-sig")


COMBO_WK = {"过热30%→35%": {"hard_gain_max": 0.35}, "过热30%→40%": {"hard_gain_max": 0.40}, "过热30%→50%": {"hard_gain_max": 0.50}}


def COMBO(start, wk_raw, f100):
    """日线近2日涨幅阈值稳健性 + 与周线放宽项组合"""
    lim2 = f100["code"].map(lambda c: bw._limit_pct(c) * 2 * 100)
    out = []
    for g in (22, 25, 27, 30, 33, 35, 40, 50):
        out.append((f"日线 2日涨幅<{g}%", G["wk0"], prep_dy(f100, dy_mask(f100, g2=g))))
    out.append(("日线 2日涨幅不限", G["wk0"], prep_dy(f100, dy_mask(f100, g2=None))))
    out.append(("日线 2日涨幅<涨跌幅2倍", G["wk0"], prep_dy(f100, dy_mask(f100, g2=None) & (f100["gain_2d"] < lim2))))
    for n, cfg in COMBO_WK.items():
        w = prep_wk(extract_wk_cfg(n, cfg), start)
        out.append((f"周线 {n}", w, G["dy0"]))
        for g in (30, None):
            out.append((f"周线 {n} + 日线2日涨幅<{g or '不限'}", w, prep_dy(f100, dy_mask(f100, g2=g))))
    return out


if __name__ == "__main__":
    main()

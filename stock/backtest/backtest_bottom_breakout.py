"""
首轮换池回测：大盘空仓→开仓后 W 个交易日内，候选池 = 底部倍量突破票（排最前）+ 原合并池（排后面）；其他时间不变
底部倍量突破（信号日收盘判断，次日开盘买）：
  底部位置   前一日收盘 ≤ 近250日最高价 × dd（默认 0.7，即回撤≥30%）
  底部区间   前 N 日最高/最低 ≤ rng（默认 N=40、1.35）
  突破       收盘 > 前 N 日最高价（突破价 = 该最高价）
  大涨       涨幅 ≥ 5%（创业板/科创板 ≥ 7%）
  倍量       成交量 ≥ 前5日均量 × vol（默认 2）
  排除       ST、上市不足 250 日、北交所
底部池排序：默认量比高→低
用法：python backtest/backtest_bottom_breakout.py > stock_result/bottom_breakout.log 2>&1
     python backtest/backtest_bottom_breakout.py brk      # 强势/所有开仓 × 第1天/前2天 × 突破看前 40/30/20/10 日最高价
     python backtest/backtest_bottom_breakout.py narrow   # 窄窗口：只开仓第1/2天、只强势开仓（首日70%）
     （底部候选特征缓存 cache_bs_long/_bottom_breakout_feats.pkl，改了特征定义要删掉重提）
"""
import pickle
import sys
from multiprocessing import Pool

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW

FEAT_FILE = bw.LONG_DIR / "_bottom_breakout_feats_v2.pkl"
NS = (10, 20, 30, 40, 60)


def feats_for_stock(code):
    """宽松预筛（涨幅≥5%/7%、突破前10日最高、量比≥1.5、回撤≥20%），记录各变体需要的特征"""
    _, d = bw.load_stock(code)
    if d is None or len(d) < 260:
        return []
    c, h, l, v = (d[k].values.astype(float) for k in ("close", "high", "low", "volume"))
    st = (d["isST"].astype(str) == "1").values
    lim = 0.07 if code[3:6] in ("300", "301", "688", "689") else 0.05
    hs, ls = pd.Series(h), pd.Series(l)
    max250 = hs.rolling(250).max().shift(1).values
    vma5 = pd.Series(v).rolling(5).mean().shift(1).values
    mx = {n: hs.rolling(n).max().shift(1).values for n in NS}
    mn = {n: ls.rolling(n).min().shift(1).values for n in NS}
    out = []
    for i in range(250, len(d)):
        if st[i] or c[i - 1] <= 0 or v[i] <= 0 or not vma5[i] > 0:
            continue
        pct = c[i] / c[i - 1] - 1
        vr = v[i] / vma5[i]
        ddv = c[i - 1] / max250[i]
        if pct < lim or vr < 1.5 or ddv > 0.8 or c[i] <= mx[10][i]:
            continue
        r = {"code": code, "sig_date": d["date"].iloc[i], "signal": "底部倍量突破", "daily_ok": "是",
             "vol_ratio": vr, "pct": pct, "dd": ddv, "atr": bw._atr_pct({"h": h, "l": l, "c": c}, i)}
        for n in NS:
            r[f"brk{n}"] = c[i] > mx[n][i]
            r[f"rng{n}"] = mx[n][i] / mn[n][i]
            r[f"bp{n}"] = mx[n][i]
        out.append(r)
    return out


def load_feats(codes):
    if FEAT_FILE.exists():
        return pickle.load(open(FEAT_FILE, "rb"))
    rows = []
    with Pool(8) as pool:
        for k, r in enumerate(pool.imap_unordered(feats_for_stock, [c for c, _ in codes], chunksize=20)):
            rows.extend(r)
            if k % 1000 == 0:
                print(f"底部特征 {k}/{len(codes)} 候选 {len(rows)}", file=sys.stderr, flush=True)
    f = pd.DataFrame(rows)
    pickle.dump(f, open(FEAT_FILE, "wb"))
    return f


def bottom_sig(feats, dd=0.7, n=40, rng=1.35, vol=2.0, brk=None):
    """n=底部区间天数；brk=突破看前几日最高价（None 同 n）"""
    brk = brk or n
    m = (feats["dd"] <= dd) & feats[f"brk{brk}"] & (feats["vol_ratio"] >= vol)
    if rng is not None:
        m &= feats[f"rng{n}"] <= rng
    s = feats[m].copy()
    s["breakout_price"] = s[f"bp{brk}"]
    s["pool"] = "bottom"
    return s.reset_index(drop=True)


def open_windows(mkt, w, strong_only=False):
    """空仓→开仓的首日起 w 个交易日（返回 (窗口日集合, 开仓首日列表)）；strong_only=只取首日 70% 的强势开仓（指数涨>3%）"""
    lv = mkt["level"].values
    dates = list(mkt["date"])
    starts = [t for t in range(1, len(lv)) if lv[t] > 0 and lv[t - 1] == 0
              and (not strong_only or lv[t] >= MKT_KW["strong"][1] - 1e-9)]
    days = {dates[k] for t in starts for k in range(t, min(t + w, len(dates)))}
    return days, [dates[t] for t in starts]


def main():
    codes = bw.stock_codes()
    wk = bw.extract_signals()
    wk = wk[wk["signal"].str.startswith(WEEKLY_TYPES)].reset_index(drop=True)
    start = wk["sig_date"].min()
    dy = bd.extract_signals(codes, 2, None)
    dy = dy[dy["sig_date"] >= start].reset_index(drop=True)
    idx = pickle.load(open(bw.LONG_DIR / f"{bw.INDEX_CODE.replace('.', '_')}.pkl", "rb"))
    idx_cz = pickle.load(open(bw.LONG_DIR / "sz_399001.pkl", "rb"))
    idx = idx[idx["date"].isin(idx_cz["date"]) & (idx["date"] >= start)].reset_index(drop=True)
    daily = bw.load_daily([c for c, _ in codes])
    wk = bw.add_rank_fields(wk, daily).assign(pool="weekly")
    dy = bw.add_rank_fields(dy, daily).assign(pool="daily")
    mix = pd.concat([wk, dy], ignore_index=True)
    feats = load_feats(codes)
    feats = feats[feats["sig_date"] >= start].reset_index(drop=True)
    mkt = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    dates = list(mkt["date"])
    prev_of = {dates[k]: dates[k - 1] for k in range(1, len(dates))}

    def win_stats(bs, wdays):
        cnt = bs.groupby("sig_date").size()
        n = [cnt.get(prev_of[d], 0) for d in sorted(wdays) if pd.Timestamp(d).weekday() != 4 and d in prev_of]
        return f"窗口买入日 {len(n)}，有底部票 {sum(x > 0 for x in n)} 天，平均 {np.mean(n):.1f} 只/天"

    w5, starts = open_windows(mkt, 5)
    print(f"区间 {dates[0].date()} ~ {dates[-1].date()}，空→开仓 {len(starts)} 次")
    base_bs = bottom_sig(feats)
    print(f"默认底部信号 {len(base_bs)} 条；W=5 {win_stats(base_bs, w5)}")

    narrow = "narrow" in sys.argv[1:]
    brkmode = "brk" in sys.argv[1:]
    _, st_starts = open_windows(mkt, 1, True)
    print(f"强势开仓（首日70%）{len(st_starts)} 次")
    cases = [("A 基线（实盘方案）", None, None, None)]
    add = lambda name, w=5, by=(["vol_ratio"], [False]), strong_only=False, **kw: \
        cases.append((name, (w, strong_only), kw, by))
    if brkmode:      # 底部区间固定 40日≤1.35，只改突破看前 N 日最高价；底部票在前，不够用新高票补
        for strong in (True, False):
            for w in (1, 2):
                for b in (40, 30, 20, 10):
                    tag = "强势开仓" if strong else "(对照)所有开仓"
                    add(f"{tag} {'第1天' if w == 1 else '前2天'} 突破前{b}日高", w=w, strong_only=strong, brk=b)
    elif narrow:
        add("开仓第1天", w=1)
        add("开仓前2天", w=2)
        add("开仓第1天 N=60", w=1, n=60)
        add("强势开仓 第1天", w=1, strong_only=True)
        add("强势开仓 前3天", w=3, strong_only=True)
        add("强势开仓 前5天", w=5, strong_only=True)
        add("强势开仓 前5天 N=60", w=5, strong_only=True, n=60)
    else:
        add("B 底部池在前 W=5（默认）")
        add("  W=3", w=3)
        add("  W=7", w=7)
        add("  回撤≥40%（dd 0.6）", dd=0.6)
        add("  回撤≥20%（dd 0.8）", dd=0.8)
        add("  区间 ≤1.5", rng=1.5)
        add("  不限区间", rng=None)
        add("  N=20", n=20)
        add("  N=60", n=60)
        add("  倍量 1.5", vol=1.5)
        add("  倍量 3", vol=3.0)
        add("  底部池按涨幅高→低", by=(["pct"], [False]))

    rows = []
    for name, w, kw, by in cases:
        if w is None:
            e, t = bw.run_portfolio(mix, daily, mkt, priority=PRIORITY, pool_mode="mixed", real=REAL)
            stat = ""
        else:
            bs = bottom_sig(feats, **kw)
            wd, _ = open_windows(mkt, w[0], w[1])
            e, t = bw.run_portfolio(mix, daily, mkt, priority=PRIORITY, pool_mode="mixed", real=REAL,
                                    front_sig=bs, front_days=wd, front_by=by)
            stat = win_stats(bs, wd)
        e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
        p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
        tb = t[t["signal"] == "底部倍量突破"]
        rows.append({"方案": name, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                     "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
                     "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2),
                     "底部笔数": len(tb), "底部平均%": round(tb["ret"].mean() * 100, 2) if len(tb) else np.nan,
                     "底部胜率%": round((tb["ret"] > 0).mean() * 100, 1) if len(tb) else np.nan, "窗口统计": stat})
        print(rows[-1], flush=True)
        if name.startswith("B") and not narrow and not brkmode:
            t.to_csv("stock_result/bottom_breakout_trades.csv", index=False, encoding="utf-8-sig")

    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(rows)
    print("\n== 总览 ==")
    print(res.drop(columns="窗口统计").to_string(index=False))
    print("\n== 窗口统计 ==")
    print(res[["方案", "窗口统计"]].to_string(index=False))
    res.to_csv(f"stock_result/bottom_breakout{'_brk' if brkmode else '_narrow' if narrow else ''}_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

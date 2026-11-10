"""
临时取消冻结（单变量，按现行实盘方案，合并池）：
  A. 前一日涨停数 > 50 且 跌停数/涨停数 < 0.05~0.3 → 当日不执行冻结（已冻结资金解冻、当天卖出不冻结）
  B. 空仓→开仓且首日 70%、次日满仓的波段：开仓日起 3~7 个交易日不冻结
涨跌停按收盘统计（前复权价，收盘=最高且涨幅≥限幅−0.3% 算涨停，跌停同理）：
  主板 10%、ST 5%、创业板 2020-08-24 起 20%（之前 10%）、科创板 20%；剔除上市前 5 日，不含北交所
用法：python backtest/backtest_ztdt_nofreeze.py > stock_result/ztdt_nofreeze.log 2>&1
"""
import multiprocessing as mp
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
CYB20 = pd.Timestamp("2020-08-24")


def zt_dt(codes):
    zt, dt = [], []
    for code, _ in codes:
        try:
            d = pickle.load(open(bw.LONG_DIR / f"{code.replace('.', '_')}.pkl", "rb"))
        except Exception:
            continue
        if d is None or len(d) < 10:
            continue
        d = d.sort_values("date").reset_index(drop=True)
        c, h, l = d["close"].astype(float), d["high"].astype(float), d["low"].astype(float)
        r = c / c.shift(1) - 1
        b = code[3:6]
        if b in ("688", "689"):
            p = pd.Series(0.2, index=d.index)
        elif b in ("300", "301"):
            p = pd.Series(np.where(d["date"] >= CYB20, 0.2, 0.1), index=d.index)
        else:
            p = pd.Series(0.1, index=d.index)
        p = p.where(d["isST"].astype(str) != "1", 0.05)
        ok = (d["tradestatus"].astype(str) == "1") & (pd.Series(np.arange(len(d))) >= 5)
        zt.append(d["date"][ok & (r >= p - 0.003) & (c >= h - 1e-6)])
        dt.append(d["date"][ok & (r <= -p + 0.003) & (c <= l + 1e-6)])
    z = pd.concat(zt).value_counts().rename("zt")
    t = pd.concat(dt).value_counts().rename("dt")
    return pd.concat([z, t], axis=1).fillna(0).sort_index()


def entry_mask(lv, n, first):
    m = np.zeros(len(lv), bool)
    for i in range(1, len(lv) - 1):
        if lv[i - 1] == 0 and abs(lv[i] - first) < 1e-9 and lv[i + 1] >= 1:
            m[i:i + n] = True
    return m


def run(a):
    name, mask = a
    kw = {"free_mask": mask} if mask is not None else {}
    e, t = bw.run_portfolio(G["mix"], G["daily"], G["mkt"], priority=PRIORITY, pool_mode="mixed", real=REAL, **kw)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"方案": name, "不冻结天数": int(mask.sum()) if mask is not None else 0,
            "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"],
            "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"], "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"],
            "笔数": len(t), "平均%": round(t["ret"].mean() * 100, 2), **yearly(e)}


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
    dates = pd.DatetimeIndex(G["mkt"]["date"])
    zd = zt_dt(codes)
    zd.to_csv("stock_result/zt_dt_daily.csv", encoding="utf-8-sig")
    zd = zd.reindex(dates).fillna(0).shift(1)               # 前一日
    print("涨停数分布:", zd["zt"].describe().round(1).to_dict())
    print(zd.loc["2026-10-20":].astype(int).to_string(), flush=True)
    lv = G["mkt"]["level"].values
    cases = [("实盘", None)]
    for thr in (0.05, 0.1, 0.15, 0.2, 0.25, 0.3):
        m = ((zd["zt"] > 50) & (zd["dt"] / zd["zt"].replace(0, np.nan) < thr)).values & (lv > 0)
        cases.append((f"昨涨停>50 且 跌停/涨停<{thr} 当日不冻结", m))
    n70 = int(entry_mask(lv, 1, 0.7).sum())
    for n in (3, 4, 5, 6, 7):
        cases.append((f"首日70%次日满仓({n70}次) 开仓日起{n}天不冻结", entry_mask(lv, n, 0.7)))
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    print(res.to_string(index=False))
    res.to_csv("stock_result/ztdt_nofreeze_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

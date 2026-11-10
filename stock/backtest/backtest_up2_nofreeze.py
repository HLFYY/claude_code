"""
空仓→开仓、且开仓信号日（前一交易日）深证综指涨幅 > 2% 并收在 10 日线上：开仓日起 N 天不冻结（单变量，合并池）
  full=True 另要求开仓次日满仓（同实盘 STRONG_NOFREEZE 的判断方式）；开盘卖出的钱次日开盘才可用（引擎已按此口径）
  对照：原规则（不加不冻结）、实盘（首日70%次日满仓 6 天）
用法：python backtest/backtest_up2_nofreeze.py [涨幅%列表，如 2.5,3] > stock_result/up2_nofreeze.log 2>&1
"""
import multiprocessing as mp
import sys
import pickle

import numpy as np
import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}


def up_mask(lv, c, n, up=0.02, full=False):
    ma10 = pd.Series(c).rolling(10).mean().values
    m = np.zeros(len(lv), bool)
    ev = 0
    for i in range(2, len(lv)):
        if lv[i - 1] == 0 and lv[i] > 0 and c[i - 1] / c[i - 2] - 1 > up and c[i - 1] > ma10[i - 1]:
            if full and not (i + 1 < len(lv) and lv[i + 1] >= 1):
                m[i] = True                       # 开仓日当天还不知道次日是否满仓（当天也没有可卖的票）
                continue
            ev += 1
            m[i] = True
            m[i + 1:i + n] = lv[i + 1:i + n] > 0
    return m, ev


def run(a):
    name, spec = a
    if spec == "base":
        mkt, kw, ev = G["mkt0"], {}, 0
    elif spec == "live":
        mkt, kw, ev = G["mkt_live"], {}, 8
    else:
        n, full, up = spec if len(spec) == 3 else (*spec, 0.02)
        m, ev = up_mask(G["mkt0"]["level"].values, G["c"], n, up, full)
        mkt, kw = G["mkt0"], {"free_mask": m}
    e, t = bw.run_portfolio(G["mix"], G["daily"], mkt, priority=PRIORITY, pool_mode="mixed", real=REAL, **kw)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"方案": name, "次数": ev, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"],
            "18-24": p1["年化%"], "24-26": p2["年化%"], "笔数": len(t), **yearly(e)}


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
    kw0 = {k: v for k, v in MKT_KW.items() if k != "strong_nofreeze"}
    G["mkt0"] = bw.market_levels_v2(idx, True, "confirm", **kw0)
    G["mkt_live"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW)
    G["c"] = idx["close"].astype(float).values
    G["mix"] = pd.concat([bw.add_rank_fields(wk, G["daily"]).assign(pool="weekly"),
                          bw.add_rank_fields(dy, G["daily"]).assign(pool="daily")], ignore_index=True)
    cases = [("原规则(无不冻结)", "base"), ("实盘:首日70%次日满仓6天", "live")]
    ups = [float(x) / 100 for x in sys.argv[1].split(",")] if len(sys.argv) > 1 else [0.02]
    for up in ups:
        cases += [(f"涨>{up * 100:g}%站上10日线 {n}天", (n, False, up)) for n in range(3, 11)]
    if len(sys.argv) == 1:
        cases += [(f"涨>2%站上10日线+次日满仓 {n}天", (n, True)) for n in range(3, 11)]
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, cases, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    print(res.to_string(index=False))
    res.to_csv("stock_result/up2_nofreeze_summary" + ("_" + sys.argv[1].replace(",", "_") if len(sys.argv) > 1 else "") + ".csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

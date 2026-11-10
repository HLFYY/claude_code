"""
回撤控制参数（单变量，按现行实盘方案，合并池）：
  max_new_day：每天最多新开 N 只
  brk=(L,K[,P])：近 L 个交易日买入日止损/走弱 ≥ K 笔 → 不开新仓（P=触发后再停 P 天）
  dd_cap=(thr,lv)：策略净值回撤 ≥ thr 时开仓上限压到 lv，创新高恢复
  week_stop=N：本周止损/走弱 ≥ N 笔 → 本周不再开新仓
用法：python backtest/backtest_dd_control.py > stock_result/dd_control.log 2>&1
"""
import multiprocessing as mp
import pickle
import sys

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW, yearly

G = {}
CASES = [("实盘", {})]
CASES += [(f"每天最多新开{n}只", {"max_new_day": n}) for n in (2, 3, 5)]
CASES += [(f"熔断 近{L}日失败≥{K}" + (f" 停{P}天" if P else ""), {"brk": (L, K, P) if P else (L, K)})
          for L, K, P in ((5, 4, 0), (5, 6, 0), (10, 6, 0), (10, 8, 0), (10, 10, 0), (5, 4, 5), (10, 8, 5))]
CASES += [(f"净值回撤≥{int(a * 100)}%降到{int(b * 10)}层", {"dd_cap": (a, b)})
          for a, b in ((0.06, 0.5), (0.08, 0.5), (0.10, 0.5), (0.08, 0.7), (0.10, 0.3), (0.12, 0.5))]
CASES += [(f"本周止损/走弱≥{n}笔停开", {"week_stop": n}) for n in (3, 5, 8)]
CASES += [(f"开盘≤止损价×{1 + x:.2f}不买", {"skip_below_stop": x}) for x in (0, 0.01, 0.02, 0.03)]
if len(sys.argv) > 1 and sys.argv[1] == "stop":
    CASES = [CASES[0]] + CASES[-4:]


def run(a):
    name, kw = a
    e, t = bw.run_portfolio(G["mix"], G["daily"], G["mkt"], priority=PRIORITY, pool_mode="mixed", real=REAL, **kw)
    e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
    p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
    return {"方案": name, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"], "卡玛": p["卡玛"],
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
    with mp.get_context("fork").Pool(6) as pool:
        res = pool.map(run, CASES, chunksize=1)
    pd.set_option("display.width", 320); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(res)
    print(res.to_string(index=False))
    res.to_csv(f"stock_result/dd_control_summary{'_' + sys.argv[1] if len(sys.argv) > 1 else ''}.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

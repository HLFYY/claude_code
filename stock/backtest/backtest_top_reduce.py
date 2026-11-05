"""
顶分型处理 + 大盘清仓资金冻结 对比
  当前：顶分型出现后次日收盘 ≤ 第3根收盘（确认）→ 第三日开盘清仓；收盘突破顶点才重新建仓
  尾盘降到 x：确认当天尾盘把持仓降到 x（按浮盈低→高整只卖，当天买的不卖）；之后最多 x 仓，
             收盘突破顶点 → 恢复满仓；跌破10日线 → 次日清仓
  大盘清仓资金冻结：大盘清仓 / 顶分型降仓卖出的钱冻结到下周一
用法：python backtest/backtest_top_reduce.py > stock_result/top_reduce.log 2>&1
"""
import pickle

import pandas as pd

import backtest_weekly_portfolio as bw
import backtest_daily_portfolio as bd
from backtest_long_run import REAL, WEEKLY_TYPES, PRIORITY, MKT_KW


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

    mkts = {"当前：次日开盘清仓": bw.market_levels_v2(idx, True, "confirm", **MKT_KW)}
    for x in (0.7, 0.5, 0.4, 0.3, 0.0):
        mkts[f"尾盘降到{int(x * 10)}层"] = bw.market_levels_v2(idx, True, "confirm", **MKT_KW, top_reduce=x)
    m0 = mkts["当前：次日开盘清仓"]
    print(f"顶分型确认清仓 {int(m0['clear'].sum())} 次（含跌破10日线等）；"
          f"尾盘降仓触发 {int(mkts['尾盘降到4层']['tail_level'].notna().sum())} 次（4层）")
    for k, m in mkts.items():
        print(f"  {k}：在场 {(m['level'] > 0).mean() * 100:.1f}%，平均仓位 {m['level'].mean() * 100:.1f}%，满仓 {(m['level'] == 1).mean() * 100:.1f}%")

    cases = [(k, m, False) for k, m in mkts.items()] + [("当前 + 大盘清仓资金冻结", m0, True)]
    rows = []
    for pool_name, sig, mode in (("合并池", mix, "mixed"), ("周线单独", wk, "weekly"), ("日线单独", dy, "daily")):
        for name, m, fz in cases:
            e, t = bw.run_portfolio(sig, daily, m, priority=PRIORITY, pool_mode=mode, real=REAL, freeze_mkt=fz)
            e1, e2 = e.loc[:"2024-08-08"], e.loc["2024-08-09":]
            p, p1, p2 = bw.perf(e, ""), bw.perf(e1 / e1.iloc[0], ""), bw.perf(e2 / e2.iloc[0], "")
            rows.append({"池": pool_name, "方案": name, "年化%": p["年化%"], "回撤%": p["最大回撤%"], "夏普": p["夏普"],
                         "卡玛": p["卡玛"], "18-24": p1["年化%"], "18-24回撤": p1["最大回撤%"],
                         "24-26": p2["年化%"], "24-26回撤": p2["最大回撤%"], "笔数": len(t),
                         "平均%": round(t["ret"].mean() * 100, 2),
                         "降仓卖出笔数": int((t["reason"] == "顶分型尾盘降仓").sum()) if len(t) else 0})
            print(rows[-1], flush=True)
    pd.set_option("display.width", 300); pd.set_option("display.max_columns", 40)
    pd.set_option("display.unicode.east_asian_width", True)
    res = pd.DataFrame(rows)
    print("\n== 总览 ==")
    print(res.to_string(index=False))
    res.to_csv("stock_result/top_reduce_summary.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

"""
前向跟踪：从 START（2026-11-03）起，按实盘规则模拟交易到最新交易日，算策略收益率，和实际账户对比
  选股：screen_new_high_breakout_sina 的条件（_check_breakout_sina），信号日T收盘出 → T+1 开盘买，仅当天有效
  优先级：10日涨幅排名强→弱；大盘：market_levels_v2(底分型确认, 顶分型confirm)，与 sz_market_action 一致
  个股/资金规则同 backtest_weekly_portfolio.run_portfolio（止损、走弱、网格、止损资金冻结到下周一）
数据：新浪（同实盘选股，复用 cache_sina 缓存；收盘 15:10 后跑数据才完整）。新浪日K不复权，期间遇除权会有偏差
实际账户：stock_result/actual_account.csv（列：日期,总资产），每天收盘后追加一行，程序自动对比
用法：python forward_track.py
"""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

import backtest_weekly_portfolio as bw
from stock_check_xinlang import (_check_breakout_sina, _sina_get_kline, _sina_get_latest_trading_date,
                                 _sina_get_stock_list, _NEW_HIGH_DAYS, _SINA_MAX_WORKERS)

START = "2026-11-03"          # 跟踪起始日（当天开盘起按策略操作）
CAPITAL = 800_000             # 模拟本金，只影响金额显示，收益率与之无关
PRIORITY = "10日涨幅排名强→弱"
ACTUAL_FILE = Path("stock_result/actual_account.csv")


def load_index(datalen: int = 300) -> pd.DataFrame:
    url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    resp = requests.get(url, params={"symbol": "sz399001", "scale": 240, "ma": "no", "datalen": datalen}, timeout=15)
    idx = pd.DataFrame(json.loads(resp.text))
    for col in ["open", "high", "low", "close"]:
        idx[col] = pd.to_numeric(idx[col], errors="coerce")
    now = datetime.now()
    if idx["day"].iloc[-1] == now.strftime("%Y-%m-%d") and (now.hour < 15 or (now.hour == 15 and now.minute < 10)):
        idx = idx.iloc[:-1]   # 当日未收盘不算
    idx["date"] = pd.to_datetime(idx["day"])
    return idx.reset_index(drop=True)


def load_stocks(latest_date):
    stock_list = _sina_get_stock_list()
    dfs = {}
    with ThreadPoolExecutor(max_workers=_SINA_MAX_WORKERS) as ex:
        futs = {ex.submit(_sina_get_kline, s["symbol"], latest_date, 250): s for s in stock_list}
        for fut in as_completed(futs):
            try:
                df = fut.result(timeout=30)
            except Exception:
                df = None
            if df is not None and not df.empty:
                dfs[futs[fut]["symbol"]] = df
    names = {s["symbol"]: s.get("name", "") for s in stock_list}
    return dfs, names


def to_daily_dict(dfs: dict) -> dict:
    daily = {}
    for code, df in dfs.items():
        c = df["close"].values.astype(float)
        daily[code] = {
            "dates": df["day"].values, "pos": {t: k for k, t in enumerate(df["day"])},
            "o": df["open"].values.astype(float), "h": df["high"].values.astype(float),
            "l": df["low"].values.astype(float), "c": c, "ma5": pd.Series(c).rolling(5).mean().values,
        }
    return daily


def daily_signals(dfs: dict, names: dict, sig_days) -> pd.DataFrame:
    rows = []
    for code, df in dfs.items():
        if "ST" in str(names.get(code, "")).upper():
            continue
        pos = {t: k for k, t in enumerate(df["day"])}
        c, h = df["close"].values, df["high"].values
        for d in sig_days:
            i = pos.get(d)
            if i is None or i < _NEW_HIGH_DAYS + 10 or c[i] < h[i - 100:i].max():
                continue
            r = _check_breakout_sina(code, names.get(code, ""), df.iloc[i - _NEW_HIGH_DAYS - 10:i + 1], {})
            if not r or np.isnan(r["14日ATR"]) or r["14日ATR"] <= 0:
                continue
            rows.append({"code": code, "name": names.get(code, ""), "sig_date": d,
                         "signal": "B突破第二日" if r["昨日是否新高"] == "是" else "A首日突破",
                         "score": r["突破日成交额倍量比"], "vol_ratio": r["突破日成交额倍量比"],
                         "breakout_price": r["突破价"], "atr": r["14日ATR"], "daily_ok": "是"})
    return pd.DataFrame(rows)


def main():
    latest_date = _sina_get_latest_trading_date()
    idx = load_index()
    mkt_full = bw.market_levels_v2(idx, True, "confirm")
    start = pd.Timestamp(START)
    if idx["date"].iloc[-1] < start:
        print(f"最新已收盘交易日 {idx['date'].iloc[-1].date()}，还没到跟踪起始日 {START}，收盘后再跑")
        return
    k0 = int(idx.index[idx["date"] >= start][0]) - 1          # 起始日前一交易日：权益=1，信号可在起始日开盘买
    mkt = mkt_full.iloc[k0:].reset_index(drop=True)
    days = list(mkt["date"])
    print(f"跟踪区间 {days[1].date()} ~ {days[-1].date()}（{len(days) - 1} 个交易日），加载行情...")

    dfs, names = load_stocks(latest_date)
    daily = to_daily_dict(dfs)
    sig = daily_signals(dfs, names, days[:-1])                # 最后一天的信号要明天才能买
    if sig.empty:
        sig = pd.DataFrame(columns=["code", "name", "sig_date", "signal", "score", "vol_ratio",
                                    "breakout_price", "atr", "daily_ok"])
    else:
        sig = bw.add_rank_fields(sig, daily)
    eq, tr = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority=PRIORITY, pool_mode="daily")

    bench = idx.set_index("date")["close"].loc[days[0]:]
    out = pd.DataFrame({
        "日期": [d.strftime("%Y-%m-%d") for d in days],
        "大盘目标仓位": [f"{int(v * 100)}%" for v in mkt["level"]],
        "策略总资产": (eq.values * CAPITAL).round(0),
        "策略当日%": (eq.pct_change().fillna(0).values * 100).round(2),
        "策略累计%": ((eq.values - 1) * 100).round(2),
        "深证成指累计%": ((bench.values / bench.values[0] - 1) * 100).round(2),
    }).iloc[1:]

    if ACTUAL_FILE.exists():
        act = pd.read_csv(ACTUAL_FILE, encoding="utf-8-sig")
        if not act.empty:
            act["日期"] = pd.to_datetime(act["日期"]).dt.strftime("%Y-%m-%d")
            base = act["总资产"].iloc[0]
            act["实际累计%"] = ((act["总资产"] / base - 1) * 100).round(2)
            out = out.merge(act[["日期", "总资产", "实际累计%"]].rename(columns={"总资产": "实际总资产"}), on="日期", how="left")
            out["差值(实际-策略)%"] = (out["实际累计%"] - out["策略累计%"]).round(2)

    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
    pd.set_option("display.unicode.east_asian_width", True)
    print(f"\n{'=' * 72}\n  策略前向跟踪（{START} 起，模拟本金 {CAPITAL:,}）\n{'=' * 72}")
    print(out.to_string(index=False))
    last = out.iloc[-1]
    print(f"\n  ▶ 截至 {last['日期']}：策略累计 {last['策略累计%']:+.2f}%，深证成指 {last['深证成指累计%']:+.2f}%"
          + (f"，实际 {last['实际累计%']:+.2f}%" if "实际累计%" in out and pd.notna(last.get("实际累计%")) else ""))

    # 当前持仓：明日止损价 = max(今MA5×0.99, 今最低×0.99)，下一格网格价
    hold = []
    for p in eq.attrs["open_positions"]:
        dd = daily[p["code"]]
        j = len(dd["c"]) - 1
        hold.append({"代码": p["code"], "名称": names.get(p["code"], ""), "信号": p["signal"],
                     "买入日": pd.Timestamp(p["buy_date"]).strftime("%Y-%m-%d"), "买入价": round(p["buy_px"], 2),
                     "剩余仓位": f"{p['left'] * 10:.1f}成", "已卖网格份数": p["grid_sold"],
                     "最新收盘": round(p["last_close"], 2), "浮盈%": round((p["last_close"] / p["buy_px"] - 1) * 100, 2),
                     "明日止损价": round(max(dd["ma5"][j] * 0.99, dd["l"][j] * 0.99), 2),
                     "下一格卖价": round(p["levels"][p["grid_sold"]], 2) if p["grid_sold"] < len(p["levels"]) else "-"})
    print(f"\n  当前模拟持仓 {len(hold)} 只")
    if hold:
        print(pd.DataFrame(hold).to_string(index=False))

    if not tr.empty:
        tr = tr.assign(名称=tr["code"].map(names))
        show = pd.DataFrame({
            "代码": tr["code"], "名称": tr["名称"], "信号": tr["signal"],
            "买入日": pd.to_datetime(tr["buy_date"]).dt.strftime("%Y-%m-%d"), "买入价": tr["buy_px"].round(2),
            "清仓日": pd.to_datetime(tr["exit_date"]).dt.strftime("%Y-%m-%d"), "清仓原因": tr["reason"],
            "网格卖出份数": tr["grid_sold"], "收益%": (tr["ret"] * 100).round(2),
            "盈亏金额": (tr["alloc"] * tr["ret"] * CAPITAL).round(0)})
        print(f"\n  已平仓 {len(show)} 笔，成功率 {(tr['ret'] > 0).mean() * 100:.1f}%，平均 {tr['ret'].mean() * 100:+.2f}%")
        print(show.to_string(index=False))
        show.to_csv("stock_result/forward_trades.csv", index=False, encoding="utf-8-sig")
    out.to_csv("stock_result/forward_equity.csv", index=False, encoding="utf-8-sig")
    print("\n已保存 stock_result/forward_equity.csv / forward_trades.csv")


if __name__ == "__main__":
    main()

"""
前向跟踪：从 START（2026-11-03）起，按实盘规则模拟交易到最新交易日，算策略收益率，和实际账户对比
  候选池（与 stock_check_xinlang.build_buy_pool 一致）：
    - 周线：stock_screener 写入的 weekly_signals.csv（买点1-W2/W3/买点2），信号周的下一周周一~五可买，前收<突破价不买
    - 日线：_check_breakout_sina 连续≥2日新高，信号日次日开盘可买
  优先级：RPS120 高→低（候选股前复权）；大盘：深证综指 market_levels_v2(底分型确认, 顶分型confirm)，与 sz_market_action 一致
  个股/资金规则同 backtest_weekly_portfolio.run_portfolio，按实盘约束（real）：本金 CAPITAL 整手、开盘涨幅>6%不买（11-09 前 7%）、
    T+1、止损/走弱资金冻结到下周一、实测费率、盘中止损/尾盘滑点
数据：新浪（同实盘选股，复用 cache_sina 缓存；收盘 15:10 后跑数据才完整）；全市场用新浪复权因子做前复权（同回测）
实际账户：stock_result/actual_account.csv（列：日期,总资产），每天收盘后追加一行，程序自动对比
持仓操作：stock_result/forward_holdings_{日期}.csv —— 模拟持仓及「下一日盘中清仓价」，每天据此更新券商条件单
         （网格在买入当天按「网格步长」设好后不改）；stock_check_xinlang.main 会把它作为邮件附件
用法：python forward_track.py（stock_check_xinlang.main 会自动调用）
"""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent / "backtest"))   # 回测引擎在 backtest/ 目录
import backtest_weekly_portfolio as bw  # noqa: E402
from stock_check_xinlang import (_check_breakout_sina, _sina_get_kline, _sina_get_latest_trading_date,
                                 _sina_get_stock_list, _sina_qfq_market, _sina_qfq_factors, _qfq_f_at, _next_trading_day,
                                 _sz_market_states, load_weekly_signals, OPEN_GAP_MAX, OPEN_GAP_OLD, OPEN_GAP_FROM, MARKET_INDEX, MARKET_INDEX_NAME,
                                 _NEW_HIGH_DAYS, _SINA_MAX_WORKERS, FIRST_LV, STRONG_UP_PCT, STRONG_UP_LV, TOP_BOTTOM_DEFER,
                                 STRONG_NOFREEZE_DAYS, STRONG_NOFREEZE_UP, STRONG_NOFREEZE_FROM)

START = "2026-11-03"          # 跟踪起始日（当天开盘起按策略操作）
CAPITAL = 800_000             # 模拟本金，只影响金额显示，收益率与之无关
PRIORITY = "RPS120(1位小数)高→低,同分10日弱→强"
REAL = {"capital": CAPITAL, "max_open_gap": OPEN_GAP_MAX,   # 实盘约束，其余用 backtest_weekly_portfolio.REAL_DEFAULT
        "gap_before": (pd.Timestamp(OPEN_GAP_FROM), OPEN_GAP_OLD)}   # OPEN_GAP_FROM 之前的买入按旧的 7%（历史买卖不变）
ACTUAL_FILE = Path("stock_result/actual_account.csv")
# 实盘手动跳过没买的票（模拟同步：当天不买，顺延买下一只）：(日期, 代码, 原因)
MANUAL_SKIP = [
    ("2026-11-06", "sz002811", "郑中设计开盘跌停，实盘跳过，顺延买中原传媒"),
]
HISTORY_FILE = Path("stock_result/历史清仓个股.xlsx")   # 模拟已清仓个股，每天合并新增（已有行不改）
SIG_COLS = ["code", "name", "sig_date", "signal", "score", "vol_ratio", "breakout_price", "atr", "daily_ok", "pool"]


def load_index(datalen: int = 300, symbol: str = MARKET_INDEX) -> pd.DataFrame:
    url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    resp = requests.get(url, params={"symbol": symbol, "scale": 240, "ma": "no", "datalen": datalen}, timeout=15)
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


def weekly_bp_to_qfq(sig: pd.DataFrame) -> None:
    """周线突破价是信号周五 baostock 前复权价（≈当时实际价），换算到当前前复权口径（之后有除权才会变）"""
    m = sig["pool"] == "weekly"
    sig.loc[m, "breakout_price"] = [bp / _qfq_f_at(_sina_qfq_factors(c, refresh=False), d)
                                    for c, d, bp in zip(sig.loc[m, "code"], sig.loc[m, "sig_date"], sig.loc[m, "breakout_price"])]


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
            r = _check_breakout_sina(code, names.get(code, ""), df.iloc[i - _NEW_HIGH_DAYS - 10:i + 1], {}, 2, None)
            if not r or np.isnan(r["14日ATR"]) or r["14日ATR"] <= 0:
                continue
            rows.append({"code": code, "name": names.get(code, ""), "sig_date": d,
                         "signal": f"日线连续{r['连续新高天数']}日新高",
                         "score": r["突破日成交额倍量比"], "vol_ratio": r["突破日成交额倍量比"],
                         "breakout_price": r["突破价"], "atr": r["14日ATR"], "daily_ok": "是", "pool": "daily"})
    return pd.DataFrame(rows, columns=SIG_COLS)


def weekly_signals(dfs: dict, days: list) -> pd.DataFrame:
    """周线信号历史 → 跟踪期内可用的信号（信号周在跟踪起始日前一周到最后一天之间）"""
    w = load_weekly_signals()
    w = w[(w["信号周"] >= days[0] - pd.Timedelta(days=7)) & (w["信号周"] <= days[-1]) & w["代码"].isin(dfs.keys())]
    return pd.DataFrame({
        "code": w["代码"], "name": w["名称"], "sig_date": w["信号周"], "signal": w["信号类型"], "score": 0.0,
        "vol_ratio": 0.0, "breakout_price": w["突破价"].astype(float), "atr": w["ATR%"].astype(float),
        "daily_ok": "是", "pool": "weekly"}, columns=SIG_COLS).reset_index(drop=True)


def _raw_px(raw_daily, code, day, adj_px, daily):
    """前复权价换回当天实际成交价（与券商成交价对照用）"""
    if code not in raw_daily:
        return adj_px
    j = daily[code]["pos"].get(day)
    return adj_px if j is None else adj_px * raw_daily[code]["c"][j] / daily[code]["c"][j]


def update_history(tr, names, daily, raw_daily):
    """已清仓个股写入 HISTORY_FILE：按（代码, 买入日）去重，已有的行保持不动，只追加新清仓的"""
    cols = ["代码", "名称", "股票池", "信号", "信号日", "RPS120", "10日涨幅排名", "买入日", "买入价", "买入股数",
            "网格步长%", "网格价差", "网格卖出份数", "网格卖出股数", "规则退出股数", "清仓日", "清仓价", "清仓原因",
            "持有天数", "期间最高涨幅%", "收益%", "盈亏金额"]
    rows = []
    for r in tr.itertuples():
        dd = daily[r.code]
        jb, je = dd["pos"].get(r.buy_date), dd["pos"].get(r.exit_date)
        buy_raw = round(_raw_px(raw_daily, r.code, r.buy_date, r.buy_px, daily), 2)
        total = int(round(r.shares0 * CAPITAL))
        rule_sh = int(round(r.left_frac * total))
        rows.append({"代码": r.code, "名称": names.get(r.code, ""), "股票池": {"weekly": "周线", "daily": "日线"}.get(r.pool, r.pool),
                     "信号": r.signal, "信号日": pd.Timestamp(r.sig_date).strftime("%Y-%m-%d"),
                     "RPS120": r.rps120_r1, "10日涨幅排名": None if pd.isna(r.gain10_rank) else int(r.gain10_rank),
                     "买入日": pd.Timestamp(r.buy_date).strftime("%Y-%m-%d"), "买入价": buy_raw, "买入股数": total,
                     "网格步长%": round(r.step * 100, 2), "网格价差": round(buy_raw * r.step, 2),
                     "网格卖出份数": r.grid_sold, "网格卖出股数": total - rule_sh, "规则退出股数": rule_sh,
                     "清仓日": pd.Timestamp(r.exit_date).strftime("%Y-%m-%d"),
                     "清仓价": round(_raw_px(raw_daily, r.code, r.exit_date, r.exit_px, daily), 2) if rule_sh else None,
                     "清仓原因": r.reason,
                     "持有天数": None if jb is None or je is None else je - jb + 1,
                     "期间最高涨幅%": None if jb is None or je is None else round((dd["h"][jb:je + 1].max() / r.buy_px - 1) * 100, 2),
                     "收益%": round(r.ret * 100, 2), "盈亏金额": round(r.alloc * r.ret * CAPITAL)})
    new = pd.DataFrame(rows, columns=cols)
    if HISTORY_FILE.exists():
        old = pd.read_excel(HISTORY_FILE, dtype={"代码": str})
        key = set(zip(old["代码"], old["买入日"].astype(str)))
        new = new[[(c, b) not in key for c, b in zip(new["代码"], new["买入日"])]]
        out = pd.concat([old, new], ignore_index=True)
    else:
        out = new
    out = out.sort_values(["清仓日", "买入日", "代码"], kind="stable").reset_index(drop=True)
    out.to_excel(HISTORY_FILE, index=False)
    print(f"  历史清仓个股 {len(out)} 笔（本次新增 {len(new)}）→ {HISTORY_FILE}")
    return len(new)


def main(market=None, market_qfq=None) -> dict:
    """
    market：stock_check_xinlang._sina_load_market() 的结果（不复权），传入则复用已加载行情
    market_qfq：stock_check_xinlang._sina_qfq_market(market) 的结果（前复权），不传则在这里算
    返回 {"holdings_file": 持仓操作csv或None, "summary": 邮件正文几行,
          "sim": {equity, held, held_layers, traded_week, next_level} 供 build_buy_pool 标注}
    """
    latest_date = market[0] if market else _sina_get_latest_trading_date()
    idx = load_index()
    mkt_full = bw.market_levels_v2(idx, True, "confirm", first_lv=FIRST_LV, strong=(STRONG_UP_PCT, STRONG_UP_LV),
                                   top_bottom="defer_low" if TOP_BOTTOM_DEFER else None)
    start = pd.Timestamp(START)
    if idx["date"].iloc[-1] < start:
        msg = f"最新已收盘交易日 {idx['date'].iloc[-1].date()}，还没到跟踪起始日 {START}，收盘后再跑"
        print(msg)
        return {"holdings_file": None, "summary": [f"▶ 前向跟踪：{msg}"], "sim": None}
    k0 = int(idx.index[idx["date"] >= start][0]) - 1          # 起始日前一交易日：权益=1，信号可在起始日开盘买
    mkt = mkt_full.iloc[k0:].reset_index(drop=True)
    # 开仓信号日涨 > STRONG_NOFREEZE_UP 且站上10日线的不冻结窗口（STRONG_NOFREEZE_FROM 起，之前的按冻结执行不变）
    idx_c = idx["close"].astype(float).values
    nf_all = bw.strong_nofreeze_mask(mkt_full["level"].values, idx_c, STRONG_NOFREEZE_DAYS, STRONG_NOFREEZE_UP) if STRONG_NOFREEZE_DAYS else None
    if nf_all is not None:
        mkt["no_freeze"] = nf_all[k0:] & (mkt["date"] >= pd.Timestamp(STRONG_NOFREEZE_FROM)).values
    days = list(mkt["date"])
    print(f"跟踪区间 {days[1].date()} ~ {days[-1].date()}（{len(days) - 1} 个交易日），加载行情...")

    if market:
        dfs = {k: v for k, v in market[2].items() if v is not None and not v.empty}
        names = {s["symbol"]: s.get("name", "") for s in market[1]}
    else:
        dfs, names = load_stocks(latest_date)
    if market_qfq is None:
        market_qfq = _sina_qfq_market((latest_date, [], dfs))
    dfq = {k: v for k, v in market_qfq[2].items() if v is not None and not v.empty}
    daily = to_daily_dict(dfq)                                 # 全市场前复权：选股/RPS/10日排名/止损同回测
    sig = pd.concat([daily_signals(dfq, names, days[:-1]),   # 最后一天的日线信号要明天才能买
                     weekly_signals(dfq, days)], ignore_index=True)
    if not sig.empty:
        weekly_bp_to_qfq(sig)
        sig = bw.add_rank_fields(sig, daily)
        print(f"候选信号 {len(sig)} 条（周线 {(sig['pool'] == 'weekly').sum()} / 日线 {(sig['pool'] == 'daily').sum()}）")
    else:
        sig["rps120_r1"] = sig["gain10_rank"] = pd.Series(dtype=float)
    raw_daily = to_daily_dict({c: dfs[c] for c in set(sig["code"])}) if not sig.empty else {}
    eq, tr = bw.run_portfolio(sig, daily, mkt, use_freeze=True, priority=PRIORITY, pool_mode="mixed", real=REAL,
                              skip_buy={(pd.Timestamp(d), c) for d, c, _ in MANUAL_SKIP})

    bench = idx.set_index("date")["close"].loc[days[0]:]
    try:   # 对照：深证成指
        b2 = load_index(60, "sz399001").set_index("date")["close"].reindex(bench.index).ffill()
    except Exception:
        b2 = pd.Series(np.nan, index=bench.index)
    out = pd.DataFrame({
        "日期": [d.strftime("%Y-%m-%d") for d in days],
        "大盘目标仓位": [f"{int(v * 100)}%" for v in mkt["level"]],
        "策略总资产": (eq.values * CAPITAL).round(0),
        "策略当日%": (eq.pct_change().fillna(0).values * 100).round(2),
        "策略累计%": ((eq.values - 1) * 100).round(2),
        f"{MARKET_INDEX_NAME}累计%": ((bench.values / bench.values[0] - 1) * 100).round(2),
        "深证成指累计%": ((b2.values / b2.values[0] - 1) * 100).round(2),
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
    print(f"\n  ▶ 截至 {last['日期']}：策略累计 {last['策略累计%']:+.2f}%，{MARKET_INDEX_NAME} {last[f'{MARKET_INDEX_NAME}累计%']:+.2f}%"
          f"，深证成指 {last['深证成指累计%']:+.2f}%"
          + (f"，实际 {last['实际累计%']:+.2f}%" if "实际累计%" in out and pd.notna(last.get("实际累计%")) else ""))

    # 当前持仓：下一日盘中清仓价 = max(今MA5×0.99, 今最低×0.99)，每天更新；网格按买入时步长设好不改
    hold = []
    for p in eq.attrs["open_positions"]:
        dd = daily[p["code"]]
        j = np.searchsorted(dd["dates"], np.datetime64(days[-1]), side="right") - 1   # 跟踪截止日那根K线
        shares0 = p["shares0"] * CAPITAL
        buy_raw = round(_raw_px(raw_daily, p["code"], p["buy_date"], p["buy_px"], daily), 2)
        step = round((p["levels"][0] / p["buy_px"] - 1) * 100, 2)
        gap = round(buy_raw * step / 100, 2)                 # 券商网格"按价格"：每格价差 = 买入价 × 步长
        n_held = int(((dd["dates"] >= np.datetime64(p["buy_date"])) & (dd["dates"] <= np.datetime64(days[-1]))).sum())
        tail_chk = n_held + 1 <= bw.WEAK_DAYS                # 下一交易日仍在走弱判断期（买入后第 2~3 个交易日）
        hold.append({"代码": p["code"], "名称": names.get(p["code"], ""),
                     "买入日": pd.Timestamp(p["buy_date"]).strftime("%Y-%m-%d"), "买入价": buy_raw,
                     "剩余股数": int(round(p["left"] * shares0)),
                     "网格步长%": step, "网格价差": gap,
                     "已卖网格份数": p["grid_sold"],
                     "剩余网格卖出价": "/".join(f"{buy_raw + k * gap:.2f}" for k in range(p["grid_sold"] + 1, bw.GRID_PARTS + 1)),
                     "下一日盘中清仓价": round(max(dd["ma5"][j] * 0.99, dd["l"][j] * 0.99), 2),
                     "下一日操作": f"开盘卖出（{p['pending'].split('→')[0]}，T+1今天卖不了）" if p["pending"]
                                   else (f"按清仓价挂盘中清仓条件单 + 14:55 走弱则尾盘卖（持有第{n_held + 1}天）" if tail_chk
                                         else "按清仓价挂盘中清仓条件单"),
                     "最新收盘": round(p["last_close"], 2),
                     "浮盈%": round((p["last_close"] / p["buy_px"] - 1) * 100, 2)})
    hold = pd.DataFrame(hold, columns=["代码", "名称", "买入日", "买入价", "剩余股数", "网格步长%", "网格价差", "已卖网格份数", "剩余网格卖出价",
                                       "下一日盘中清仓价", "下一日操作", "最新收盘", "浮盈%"])
    hold_file = f"stock_result/forward_holdings_{days[-1].strftime('%Y%m%d')}.csv"
    hold.to_csv(hold_file, index=False, encoding="utf-8-sig")
    print(f"\n  当前模拟持仓 {len(hold)} 只（{hold_file}）")
    if not hold.empty:
        print(hold.to_string(index=False))

    sell_open = hold[hold["下一日操作"].str.startswith("开盘卖出")]
    summary = [f"▶ 策略前向跟踪（{START} 起）：累计 {last['策略累计%']:+.2f}%，{MARKET_INDEX_NAME} {last[f'{MARKET_INDEX_NAME}累计%']:+.2f}%"
               f"，深证成指 {last['深证成指累计%']:+.2f}%"
               + (f"，实际 {last['实际累计%']:+.2f}%" if "实际累计%" in out and pd.notna(last.get("实际累计%")) else ""),
               f"▶ 模拟持仓 {len(hold)} 只" + (f"（其中 {len(sell_open)} 只明日开盘卖出）" if len(sell_open) else "") + f"：见附件 {Path(hold_file).name}，按「下一日盘中清仓价」更新条件单"]
    if len(sell_open):
        summary.append("▶ 明日开盘卖出（买入当天触发止损/走弱，T+1）：" + "，".join(sell_open["名称"]))
    tail_watch = hold[hold["下一日操作"].str.contains("尾盘卖")]
    if len(tail_watch):
        summary.append("▶ 明日 14:55 看走弱（不创新高或不收涨→尾盘卖）：" + "，".join(tail_watch["名称"]))
    today_exit = tr[pd.to_datetime(tr["exit_date"]) == days[-1]] if not tr.empty else tr
    if len(today_exit):
        summary.append("▶ 今日模拟平仓：" + "，".join(
            f"{names.get(r.code, r.code)}({r.reason} {r.ret * 100:+.2f}%)" for r in today_exit.itertuples()))

    if not tr.empty:
        tr = tr.assign(名称=tr["code"].map(names))
        show = pd.DataFrame({
            "代码": tr["code"], "名称": tr["名称"], "信号": tr["signal"],
            "买入日": pd.to_datetime(tr["buy_date"]).dt.strftime("%Y-%m-%d"), "买入价": [round(_raw_px(raw_daily, r.code, r.buy_date, r.buy_px, daily), 2) for r in tr.itertuples()],
            "清仓日": pd.to_datetime(tr["exit_date"]).dt.strftime("%Y-%m-%d"), "清仓原因": tr["reason"],
            "网格卖出份数": tr["grid_sold"], "收益%": (tr["ret"] * 100).round(2),
            "盈亏金额": (tr["alloc"] * tr["ret"] * CAPITAL).round(0)})
        print(f"\n  已平仓 {len(show)} 笔，成功率 {(tr['ret'] > 0).mean() * 100:.1f}%，平均 {tr['ret'].mean() * 100:+.2f}%")
        print(show.to_string(index=False))
        show.to_csv("stock_result/forward_trades.csv", index=False, encoding="utf-8-sig")
        try:
            update_history(tr, names, daily, raw_daily)
        except Exception as e:
            print(f"  [历史清仓个股] 写入失败：{type(e).__name__}: {e}")
    out.to_csv("stock_result/forward_equity.csv", index=False, encoding="utf-8-sig")
    print(f"\n已保存 stock_result/forward_equity.csv / forward_trades.csv / {Path(hold_file).name}")

    # 给股票池用的模拟账户状态
    next_day = pd.Timestamp(_next_trading_day(days[-1]))
    eq_last = float(eq.iloc[-1])
    ops = eq.attrs["open_positions"]
    layer = 0.1 * eq_last                                      # 1 层 = 前一交易日收盘总资产（含冻结）× 10%，同回测
    keep = [p for p in ops if not p["pending"]]                # 明天开盘要卖的（T+1 顺延）不算持仓
    sell = [p for p in ops if p["pending"]]
    held = [p["code"] for p in keep]
    held_layers = sum(p["left"] * p["shares0"] * p["buy_px"] for p in keep) / layer        # 按成本，同回测开仓额度
    same_week = next_day.to_period("W-FRI") == pd.Timestamp(days[-1]).to_period("W-FRI")
    traded_week = sorted(set(tr.loc[pd.to_datetime(tr["buy_date"]).dt.to_period("W-FRI") == next_day.to_period("W-FRI"), "code"])
                         ) if same_week and not tr.empty else []
    traded_week = sorted(set(traded_week) | {p["code"] for p in sell                         # 本周买的、明天开盘卖掉的，本周不再买
                                             if pd.Timestamp(p["buy_date"]).to_period("W-FRI") == next_day.to_period("W-FRI")})
    st = _sz_market_states(idx[["day", "open", "high", "low", "close"]].reset_index(drop=True)).iloc[-1]
    next_level = int(st["下一日仓位"].rstrip("%")) / 100
    next_nf = False                                            # 明天是否在不冻结窗口内（明天只用到今天及以前的收盘）
    if STRONG_NOFREEZE_DAYS and next_day >= pd.Timestamp(STRONG_NOFREEZE_FROM):
        next_nf = bool(bw.strong_nofreeze_mask(np.append(mkt_full["level"].values, next_level), np.append(idx_c, np.nan),
                                               STRONG_NOFREEZE_DAYS, STRONG_NOFREEZE_UP)[-1])
    # 明天开盘的资金（同 run_portfolio 开仓前的顺序：解冻 → 网格款到账 → 开盘卖出 → 开新仓）
    cash, frozen = eq.attrs["cash"] + eq.attrs["pending_grid"], eq.attrs["frozen"]
    cash_now, frozen_now = cash, frozen                       # 今日收盘的现金（含网格待到账）/ 冻结
    held_all = sum(p["left"] * p["shares0"] * p["buy_px"] for p in ops) / layer          # 今日收盘全部持仓（含明早卖出的）
    if not same_week or next_nf:                               # 新的一周 / 不冻结窗口：冻结资金解冻
        cash, frozen = cash + frozen, 0.0
    sell_cash = sell_frozen = 0.0                              # 开盘卖出的钱（按最新收盘估）：赶不上同一场集合竞价买入，不冻结的也要后天开盘才能用
    for p in sell:
        v = p["left"] * p["shares0"] * p["last_close"]
        if p["pending"].startswith("大盘") or next_nf:
            sell_cash += v
        else:
            sell_frozen += v
    frozen = frozen + sell_frozen
    cap_n = max(0, int(np.floor(next_level * 10 - held_layers + 1e-6)))   # 持仓 + 1 层 ≤ 目标仓位
    cash_n, c = 0, cash
    while c >= layer * 0.9 * (1 - 1e-6):                        # 现金 ≥ 0.9 层才买（买不满一层按剩余现金买）
        cash_n += 1; c -= min(layer, c)
    sim = {"equity": eq_last * CAPITAL, "held": held, "held_layers": held_layers, "traded_week": traded_week,
           "next_level": next_level, "next_no_freeze": next_nf,
           "cash_layers": cash / layer, "frozen_layers": frozen / layer, "sell_layers": (sell_cash + sell_frozen) / layer,
           "held_all_layers": held_all, "cash_now_layers": cash_now / layer, "frozen_now_layers": frozen_now / layer,
           "sell_cash_layers": sell_cash / layer, "sell_frozen_layers": sell_frozen / layer,
           "n_sell": len(sell), "cap_n": cap_n, "cash_n": cash_n, "max_new": min(cap_n, cash_n), "new_week": not same_week}
    return {"holdings_file": hold_file, "summary": summary, "sim": sim,
            "history_file": str(HISTORY_FILE) if HISTORY_FILE.exists() else None}


if __name__ == "__main__":
    main()

"""
组合回测引擎（库，入口见 backtest_long_run.py）
数据：cache_bs_long/（fetch_daily_long_bs.py 拉取：baostock 前复权日线，2018起，含退市股、换手率、isST）
  - 周线由日线合成；深证成指 cache_bs_long/sz_399001.pkl
  - 周线选股直接调用 stock_screener 的 check_buy_point_1 / check_buy_point_2 / _check_daily_w1，过滤同 process_one
  - 日线选股（screen_new_high_breakout_sina 的条件）见 backtest_daily_portfolio.py

大盘仓位（深证成指，只看收盘）：
  market_levels_v2(bottom_confirm=True, top_mode="confirm")  ← 实盘采用，与 stock_check_xinlang.sz_market_action 逐日一致
  market_levels(fractal_mode)                                 ← 旧规则，仅作对照
个股：每只固定1层(10%权益)
  - 网格：买价 × (1 + k × ATR%×1.2)，k=1..4，各卖1/4
  - 止损：max(昨MA5×0.99, 昨低×0.99)，盘中触及清仓（开盘已破按开盘价）
  - 新开票走弱：买入当天和下一交易日，尾盘需 当日高>昨高 且 收盘>昨收，否则尾盘清
  - 一字涨停买不进；周五不开新仓
  - 周线池：上周五信号本周一~四可买，前一日收盘 < 突破价视为走坏不买；日线池：前一交易日信号仅当日开盘可买
资金冻结：网格卖出资金次日开盘可用；止损/走弱清仓资金冻结到下周一（use_freeze 可改为 N 天/不冻结）；大盘清仓资金不冻结
同日顺序：大盘清仓 → 个股开盘止损/网格跳空 → 开新仓 → 盘中止损(先) → 网格 → 尾盘走弱
"""
import contextlib
import io
import pickle
import sys
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

with contextlib.redirect_stdout(io.StringIO()):
    import stock_screener as ss

LONG_DIR = Path("cache_bs_long")
SIG_DIR = LONG_DIR                    # 选股信号缓存（删掉即重新提取）
SIG_FILE = SIG_DIR / "_weekly_signals.pkl"
INDEX_CODE = "sz.399001"
WANT_SIGNALS = ("买点1-W2", "买点1-W3", "买点2")
LAYER = 0.10
GRID_MULT, GRID_PARTS = 1.2, 4
BUY_FEE, SELL_FEE = 0.0003, 0.0008   # 佣金万2.5 + 卖出印花税万5（2023-08 前为千1，这里统一取近年值）
WEEK_LOOKBACK = 156                   # 同 stock_screener start_date=156周


# ─────────────────────────── 选股信号 ───────────────────────────

def to_weekly(d: pd.DataFrame) -> pd.DataFrame:
    """日线合成周线（周五为界，date 取该周最后一个交易日，同 baostock 周线口径）"""
    wk = d.groupby(d["date"].dt.to_period("W-FRI"))
    return pd.DataFrame({
        "date": wk["date"].last(), "open": wk["open"].first(), "high": wk["high"].max(),
        "low": wk["low"].min(), "close": wk["close"].last(), "volume": wk["volume"].sum(),
        "amount": wk["amount"].sum(), "turnover": wk["turnover"].sum(),
    }).reset_index(drop=True)


def load_stock(code: str):
    """返回 (周线, 前复权日线)。code 形如 sh.600000"""
    try:
        d = pickle.load(open(LONG_DIR / f"{code.replace('.', '_')}.pkl", "rb"))
    except Exception:
        return None, None
    if d is None or d.empty:
        return None, None
    d = d.sort_values("date").reset_index(drop=True)
    return to_weekly(d), d


def _passes_filters(df: pd.DataFrame) -> bool:
    """同 stock_screener.process_one 的通用过滤 + 硬过滤"""
    CFG = ss.CFG
    if len(df) < CFG["min_weeks_data"] or df["close"].iloc[-1] < CFG["min_price"]:
        return False
    recent_turn = df["turnover"].iloc[-4:].mean()
    if pd.isna(recent_turn) or recent_turn < CFG["min_turnover"]:
        return False
    recent = df[(df["turnover"] > 0) & (df["volume"] > 0)].iloc[-4:]
    if not recent.empty:
        float_cap = recent["volume"].mean() / (recent["turnover"].mean() / 100) * df["close"].iloc[-1] / 1e8
        if float_cap < CFG["min_market_cap"]:
            return False
    c, o, h, a = (df[k].values for k in ("close", "open", "high", "amount"))
    last = len(df) - 1
    if c[last - 1] > 0 and (c[last] - c[last - 1]) / c[last - 1] >= CFG["hard_gain_max"]:
        return False
    body, upper = abs(c[last] - o[last]), h[last] - max(c[last], o[last])
    if a[last - 1] > 0 and a[last] > a[last - 1] and body > 0 and upper >= body:
        return False
    if ss._count_consec_green(c, o, last) >= CFG["hard_consec_green_max"]:
        return False
    pct = np.where(c[:-1] > 0, (c[1:] - c[:-1]) / c[:-1], 0)
    return pct.max() >= CFG["hard_hist_spike_min"]


def signals_for_stock(args):
    code, name = args
    w, d = load_stock(code)
    if w is None or len(w) < ss.CFG["min_weeks_data"]:
        return []
    st_days = set(d.loc[d["isST"] == "1", "date"])
    out = []
    for i in range(ss.CFG["min_weeks_data"] - 1, len(w)):
        wd = w.iloc[max(0, i - WEEK_LOOKBACK + 1):i + 1].reset_index(drop=True)
        sig_day = wd["date"].iloc[-1]
        if sig_day in st_days:              # 信号当日是ST不做
            continue
        if not _passes_filters(wd):
            continue
        r = ss.check_buy_point_1(wd)
        if r is None:
            r = ss.check_buy_point_2(wd)
        if r is None or not r["signal"].startswith(WANT_SIGNALS):
            continue
        hist = d[d["date"] <= sig_day]
        if len(hist) < 20:
            continue
        daily_ok = "是"
        if r["signal"].startswith("买点1"):
            fails = ss._check_daily_w1(hist.tail(140), r["breakout_date"], r["gain_w1"])
            daily_ok = "是" if not fails else "/".join(fails)
        atr = ss._calc_atr(hist.tail(30), 14)
        if np.isnan(atr) or atr <= 0:
            continue
        out.append({
            "code": code, "sig_date": sig_day, "signal": r["signal"], "score": r["score"],
            "breakout_price": r["breakout_price"], "vol_ratio": r["vol_ratio"],
            "atr": atr, "daily_ok": daily_ok,
        })
    return out


def stock_codes():
    """baostock 全部A股（含退市）及名称"""
    names = pickle.load(open(LONG_DIR / "_stock_list.pkl", "rb")).set_index("code")["code_name"].to_dict()
    return [(c, names.get(c, "")) for c in sorted(names)]


def extract_signals():
    if SIG_FILE.exists():
        return pickle.load(open(SIG_FILE, "rb"))
    rows = []
    codes = stock_codes()
    with Pool(8) as pool:
        for k, r in enumerate(pool.imap_unordered(signals_for_stock, codes, chunksize=20)):
            rows.extend(r)
            if k % 500 == 0:
                print(f"选股 {k}/{len(codes)} 信号 {len(rows)}", file=sys.stderr, flush=True)
    sig = pd.DataFrame(rows)
    pickle.dump(sig, open(SIG_FILE, "wb"))
    return sig


# ─────────────────────────── 大盘仓位 ───────────────────────────

FRACTAL_MODE = "break_k3c"   # 10日线下底分型的弱势定义，见 market_levels


def market_levels(idx: pd.DataFrame, fractal_mode: str = FRACTAL_MODE) -> pd.DataFrame:
    """
    返回每个交易日 开盘时 的目标仓位 level 与是否清仓日（均由前一日收盘决定）
    底分型：中间K线最高、最低都低于左右两根，第3根收盘确认（不做包含处理）
    fractal_mode：
      off       不用底分型（10日线下一律铁律清仓）
      break     10日线下底分型建仓后，收盘跌破分型最低点(第2根低点)才清仓
      break_k3  跌破分型最低点 或 收盘跌破第3根(确认K)最低点 → 清仓
      break_down 跌破分型最低点 或 收盘下跌 → 清仓
      break_k3c 跌破分型最低点 或 收盘跌破第3根(确认K)收盘价 → 清仓
    底分型仓位：确认次日4层，之后收盘上涨 → 再次日10层；收盘重新站上10日线后回到普通规则
    """
    c, h, l = idx["close"].values, idx["high"].values, idx["low"].values
    ma10 = pd.Series(c).rolling(10).mean().values
    n = len(idx)
    level_open = np.zeros(n)          # 当天开盘执行的目标仓位
    clear_open = np.zeros(n, bool)    # 当天开盘清仓
    fractal = np.zeros(n, bool)
    in_frac = np.zeros(n, bool)       # 当天收盘时处于"10日线下底分型持仓"状态
    lv, frac_mode, frac_low, k3_low, k3_close = 0.0, False, 0.0, 0.0, 0.0
    for t in range(n - 1):
        if np.isnan(ma10[t]):
            continue
        if t >= 2 and l[t - 1] < l[t - 2] and l[t - 1] < l[t] and h[t - 1] < h[t - 2] and h[t - 1] < h[t]:
            fractal[t] = True
        if c[t] >= ma10[t]:
            frac_mode = False
            if lv == 0:
                lv = 0.4
            elif lv == 0.4:
                lv = 1.0
        elif fractal_mode != "off" and fractal[t]:
            # 10日线下出现底分型：不受铁律覆盖，开始/延续底分型仓位
            frac_mode, frac_low, k3_low, k3_close = True, l[t - 1], l[t], c[t]
            if lv == 0:
                lv = 0.4
            elif lv == 0.4 and c[t] > c[t - 1]:
                lv = 1.0
        elif frac_mode and lv > 0:
            weak = c[t] < frac_low
            if fractal_mode == "break_k3":
                weak |= c[t] < k3_low
            elif fractal_mode == "break_k3c":
                weak |= c[t] < k3_close
            elif fractal_mode == "break_down":
                weak |= c[t] < c[t - 1]
            if weak:
                clear_open[t + 1] = True
                lv, frac_mode = 0.0, False
            elif lv == 0.4 and c[t] > c[t - 1]:
                lv = 1.0
        else:                          # 铁律
            if lv > 0:
                clear_open[t + 1] = True
            lv, frac_mode = 0.0, False
        in_frac[t] = frac_mode
        level_open[t + 1] = lv
    return pd.DataFrame({"date": idx["date"], "level": level_open, "clear": clear_open,
                         "fractal": fractal, "below": c < ma10, "in_frac": in_frac})


def market_levels_v2(idx: pd.DataFrame, bottom_confirm=True, top_mode="off") -> pd.DataFrame:
    """
    10日线规则 + 分型确认版（分型：3根K线，不做包含；第3根收盘确认）
      底分型（10日线下）：bottom_confirm=True 时，第3根确认后次日收盘 >= 第3根收盘 → 再次日开盘4层；
          4层当天收盘仍 >= 第3根收盘 → 再次日10层；之后收盘 < 第3根收盘 → 次日开盘清仓
          bottom_confirm=False 时同 market_levels(break_k3c)：确认次日即4层，收涨加满
      顶分型（持仓时出现）top_mode：
          off          不处理
          immediate    次日开盘清仓，之后照常按10日线规则（线上次日即可重新4层）
          cross        次日开盘清仓，需重新"跌破再站上10日线"或新底分型才再开仓
          confirm      次日收盘 > 第3根收盘 → 顶分型不生效不清仓；否则第三日开盘清仓，收盘突破分型顶点(中间K最高)才失效
          reclaim_k3   次日开盘清仓，收盘 > 第3根收盘 → 失效，按10日线规则重新开仓
          reclaim_high 次日开盘清仓，收盘 > 分型顶点(中间K最高) → 失效，按10日线规则重新开仓
      顶分型失效前若收盘跌破10日线，按铁律空仓，之后正常按"站上10日线/底分型"重新触发
    """
    c, h, l = idx["close"].values, idx["high"].values, idx["low"].values
    ma10 = pd.Series(c).rolling(10).mean().values
    n = len(idx)
    level_open, clear_open = np.zeros(n), np.zeros(n, bool)
    bottom_f, top_f = np.zeros(n, bool), np.zeros(n, bool)
    lv, frac_mode, k3_close = 0.0, False, 0.0
    pending = None          # 已出底分型、等待次日收盘确认的弱势线
    top_pending = None      # confirm 模式：等待次日确认的顶分型 (第3根收盘, 顶点)
    block = None            # 顶分型清仓后的失效线：收盘突破才允许重新开仓
    need_cross = False      # cross 模式：等待重新触发

    for t in range(n - 1):
        if np.isnan(ma10[t]):
            continue
        bottom = t >= 2 and l[t - 1] < l[t - 2] and l[t - 1] < l[t] and h[t - 1] < h[t - 2] and h[t - 1] < h[t]
        top = t >= 2 and h[t - 1] > h[t - 2] and h[t - 1] > h[t] and l[t - 1] > l[t - 2] and l[t - 1] > l[t]
        bottom_f[t], top_f[t] = bottom, top
        above = c[t] >= ma10[t]

        def clear_all():
            nonlocal lv, frac_mode, pending
            if lv > 0:
                clear_open[t + 1] = True
            lv, frac_mode, pending = 0.0, False, None

        # 顶分型：confirm 模式的次日确认
        if top_pending is not None:
            ref, hi = top_pending
            top_pending = None
            if c[t] <= ref and lv > 0:
                clear_all(); block = hi
                continue
        # 顶分型：新出现（持仓或等待底分型确认时）
        if top_mode != "off" and top and (lv > 0 or pending is not None):
            if top_mode == "confirm":
                pending = None
                if lv > 0:
                    top_pending = (c[t], h[t - 1])
            else:
                clear_all()
                need_cross = top_mode == "cross"
                block = {"reclaim_k3": c[t], "reclaim_high": h[t - 1]}.get(top_mode)
                continue
        # 顶分型失效线
        if block is not None:
            if not above:
                block = None              # 已跌破10日线：转铁律/底分型逻辑
            elif c[t] > block:
                block = None              # 顶分型失效，下面按10日线规则重新开仓
            else:
                level_open[t + 1] = lv
                continue

        if pending is not None and lv == 0:
            # 底分型确认日：收盘不破第3根收盘 → 次日4层
            if c[t] >= pending:
                lv, frac_mode, k3_close, need_cross = 0.4, True, pending, False
            pending = None
            if bottom and not above and lv == 0:
                pending = c[t]
        elif above and not need_cross and not frac_mode:
            lv = 0.4 if lv == 0 else 1.0
        elif above and frac_mode:
            frac_mode = False             # 站上10日线，回到10日线规则
            lv = 0.4 if lv == 0 else 1.0
        elif frac_mode and lv > 0:
            if c[t] < k3_close:
                clear_all()
            elif lv == 0.4 and (c[t] >= k3_close if bottom_confirm else c[t] > c[t - 1]):
                lv = 1.0
            if bottom and frac_mode:
                k3_close = c[t]           # 新底分型，弱势线更新
        elif bottom and (not above or need_cross):
            if bottom_confirm:
                if not above:
                    clear_all()           # 线下：先按铁律清（若有仓），等确认再开
                pending = c[t]
            else:                         # 同 market_levels(break_k3c)：不清，直接进入底分型持仓
                lv = 0.4 if lv == 0 else (1.0 if c[t] > c[t - 1] else lv)
                frac_mode, k3_close, need_cross = True, c[t], False
        elif not above:
            clear_all()
            need_cross = False            # 已跌破，下一次站上即重新触发
        level_open[t + 1] = lv
    return pd.DataFrame({"date": idx["date"], "level": level_open, "clear": clear_open,
                         "fractal": bottom_f, "top": top_f, "below": c < ma10})


# ─────────────────────────── 组合模拟 ───────────────────────────

class Pos:
    __slots__ = ("code", "sig", "buy_i", "buy_px", "shares0", "left", "levels", "sold", "cost")


PRIORITIES = {
    "评分高→低(默认)": (["score", "vol_ratio"], [False, False]),
    "RPS120高→低": (["rps120"], [False]),
    "RPS120低→高": (["rps120"], [True]),
    "10日涨幅排名强→弱": (["gain10_rank"], [True]),
    "10日涨幅排名弱→强": (["gain10_rank"], [False]),
}


def add_rank_fields(sig: pd.DataFrame, daily: dict) -> pd.DataFrame:
    """信号日全市场 RPS120 百分位 / 10日涨幅名次，算法同 stock_screener._calc_rps_and_rank（按个股自身交易日回看）"""
    g120, g10 = {}, {}
    for code, dd in daily.items():
        c = pd.Series(dd["c"], index=pd.to_datetime(dd["dates"]))
        g120[code] = c / c.shift(120) - 1
        g10[code] = c / c.shift(10) - 1
    p120, p10 = pd.DataFrame(g120), pd.DataFrame(g10)   # 停牌日为 NaN，不参与当日排名
    sd = sorted(sig["sig_date"].unique())
    p120, p10 = p120.reindex(sd), p10.reindex(sd)
    rps = p120.rank(axis=1, pct=True, method="min") * 100 - 100 / p120.notna().sum(axis=1).values[:, None]
    rank10 = p10.rank(axis=1, ascending=False, method="min")
    sig = sig.copy()
    sig["rps120"] = [rps.at[d, c] if c in rps.columns else np.nan for d, c in zip(sig["sig_date"], sig["code"])]
    sig["gain10_rank"] = [rank10.at[d, c] if c in rank10.columns else np.nan for d, c in zip(sig["sig_date"], sig["code"])]
    return sig


def run_portfolio(sig: pd.DataFrame, daily: dict, mkt: pd.DataFrame, use_freeze=True, priority="评分高→低(默认)",
                  pool_mode="weekly"):
    """pool_mode: weekly=上周五信号本周一~四可买；daily=前一交易日信号仅当日开盘可买"""
    dates = list(mkt["date"])
    level = mkt["level"].values
    clear = mkt["clear"].values
    week_id = pd.Series(dates).dt.to_period("W-FRI").values

    # 选股池：信号周 → 下一周可用，按优先级排序（缺值排最后）
    sig = sig.copy()
    sig["wk"] = sig["sig_date"].dt.to_period("W-FRI") if pool_mode == "weekly" else sig["sig_date"]
    by, asc = PRIORITIES[priority]
    pools = {wk: g.sort_values(by, ascending=asc, na_position="last") for wk, g in sig.groupby("wk")}

    cash = 1.0
    pending_grid = 0.0                  # 次日可用
    frozen = 0.0                        # 冻结中的资金合计（use_freeze=True 下周一可用；=N 卖出后第N+1个交易日开盘可用）
    frozen_q = []                       # use_freeze=N 时的 [(可用日序号, 金额)]
    freeze_days = use_freeze if type(use_freeze) is int else None
    positions: list = []
    trades, equity_curve = [], []
    traded_this_pool = set()

    def px(code, i, field):
        dd = daily[code]
        j = dd["pos"].get(dates[i])
        return None if j is None else dd[field][j]

    def prev_row(code, i):
        """该股在 i 日之前最近一个交易日的行号"""
        dd = daily[code]
        j = dd["pos"].get(dates[i])
        return (j - 1) if j is not None else None

    def close_pos(p, i, price, reason, bucket):
        nonlocal cash, frozen, pending_grid
        val = p.left * p.shares0 * price * (1 - SELL_FEE)
        p.cost += val
        if bucket == "frozen":
            frozen += val
            if freeze_days is not None:
                frozen_q.append((i + freeze_days + 1, val))
        else:
            cash += val
        trades.append({"code": p.code, "signal": p.sig["signal"], "sig_date": p.sig["sig_date"],
                       "buy_date": dates[p.buy_i], "exit_date": dates[i], "reason": reason,
                       "ret": p.cost / (p.shares0 * p.buy_px * (1 + BUY_FEE)) - 1, "grid_sold": p.sold,
                       "daily_ok": p.sig["daily_ok"], "alloc": p.shares0 * p.buy_px * (1 + BUY_FEE),
                       "buy_px": p.buy_px})
        p.left = 0

    def grid_sell(p, price):
        nonlocal pending_grid
        q = 1.0 / GRID_PARTS
        val = q * p.shares0 * price * (1 - SELL_FEE)
        p.cost += val
        pending_grid += val
        p.left -= q
        p.sold += 1

    for i, day in enumerate(dates):
        if i == 0:
            equity_curve.append(1.0)
            continue
        new_week = week_id[i] != week_id[i - 1]
        if freeze_days is not None:
            ready = sum(v for k, v in frozen_q if k <= i)
            frozen_q = [(k, v) for k, v in frozen_q if k > i]
            cash += ready; frozen -= ready
        elif new_week:
            cash += frozen; frozen = 0.0
        if new_week:
            traded_this_pool = set()
        cash += pending_grid; pending_grid = 0.0
        if use_freeze is False:
            cash += frozen; frozen = 0.0

        # 1. 大盘铁律清仓
        if clear[i]:
            for p in positions:
                o = px(p.code, i, "o")
                if o is None:  # 停牌：按最近收盘清（近似）
                    dd = daily[p.code]; j = np.searchsorted(dd["dates"], np.datetime64(day)) - 1
                    o = dd["c"][j]
                close_pos(p, i, o, "大盘清仓", "cash")
            positions = []

        # 2. 持仓开盘处理
        for p in positions:
            j = daily[p.code]["pos"].get(day)
            if j is None:
                continue
            dd = daily[p.code]
            stop = max(dd["ma5"][j - 1] * 0.99, dd["l"][j - 1] * 0.99)
            if dd["o"][j] <= stop:
                close_pos(p, i, dd["o"][j], "止损(开盘)", "frozen"); continue
            while p.sold < GRID_PARTS and dd["o"][j] >= p.levels[p.sold]:
                grid_sell(p, dd["o"][j])
            if p.left <= 1e-9:
                close_pos(p, i, 0, "网格卖完", "cash")
        positions = [p for p in positions if p.left > 1e-9]

        # 3. 开新仓
        wd = pd.Timestamp(day).weekday()
        if level[i] > 0 and not clear[i] and wd != 4:
            pool = pools.get(week_id[i] - 1 if pool_mode == "weekly" else dates[i - 1])
            eq_prev = equity_curve[-1]
            layer_amt = LAYER * eq_prev
            held = {p.code for p in positions}
            used_layers = sum(p.left * p.shares0 * p.buy_px for p in positions) / layer_amt if positions else 0
            if pool is not None:
                for _, s in pool.iterrows():
                    if used_layers + 1 > level[i] * 10 + 1e-6 or cash < layer_amt * (1 - 1e-6):
                        break
                    code = s["code"]
                    if code in held or code in traded_this_pool or code not in daily:
                        continue
                    dd = daily[code]
                    j = dd["pos"].get(day)
                    if j is None or j < 6:
                        continue
                    if dd["c"][j - 1] < s["breakout_price"]:   # 周中走坏，不符合条件
                        continue
                    lim = 0.2 if code[3:6] in ("300", "301", "688", "689") else 0.1
                    if dd["h"][j] == dd["l"][j] and dd["o"][j] >= dd["c"][j - 1] * (1 + lim) - 0.011:
                        continue
                    p = Pos()
                    p.code, p.sig, p.buy_i, p.buy_px = code, s, i, dd["o"][j]
                    p.shares0 = layer_amt / (p.buy_px * (1 + BUY_FEE))
                    p.left, p.sold, p.cost = 1.0, 0, 0.0
                    step = s["atr"] * GRID_MULT / 100
                    p.levels = [p.buy_px * (1 + k * step) for k in range(1, GRID_PARTS + 1)]
                    cash -= layer_amt
                    positions.append(p)
                    held.add(code); traded_this_pool.add(code)
                    used_layers += 1

        # 4. 盘中 + 尾盘
        for p in positions:
            dd = daily[p.code]
            j = dd["pos"].get(day)
            if j is None:
                continue
            stop = max(dd["ma5"][j - 1] * 0.99, dd["l"][j - 1] * 0.99)
            if dd["l"][j] <= stop:
                close_pos(p, i, min(stop, dd["o"][j]), "止损", "frozen"); continue
            while p.sold < GRID_PARTS and dd["h"][j] >= p.levels[p.sold]:
                grid_sell(p, p.levels[p.sold])
            if p.left <= 1e-9:
                close_pos(p, i, 0, "网格卖完", "cash"); continue
            held_days = sum(1 for k in range(p.buy_i, i + 1) if dd["pos"].get(dates[k]) is not None)
            if held_days <= 2 and not (dd["h"][j] > dd["h"][j - 1] and dd["c"][j] > dd["c"][j - 1]):
                close_pos(p, i, dd["c"][j], "走弱尾盘", "frozen")
        positions = [p for p in positions if p.left > 1e-9]

        # 5. 估值
        mv = 0.0
        for p in positions:
            dd = daily[p.code]
            k = np.searchsorted(dd["dates"], np.datetime64(day), side="right") - 1
            mv += p.left * p.shares0 * dd["c"][k]
        equity_curve.append(cash + pending_grid + frozen + mv)

    eq = pd.Series(equity_curve, index=pd.to_datetime(dates))
    # 期末未平仓持仓（前向跟踪用）：剩余份额按最新收盘估值
    eq.attrs["open_positions"] = [{
        "code": p.code, "signal": p.sig["signal"], "sig_date": p.sig["sig_date"], "buy_date": dates[p.buy_i],
        "buy_px": p.buy_px, "left": p.left, "grid_sold": p.sold, "levels": p.levels,
        "alloc": p.shares0 * p.buy_px * (1 + BUY_FEE),
        "last_close": daily[p.code]["c"][np.searchsorted(daily[p.code]["dates"], np.datetime64(dates[-1]), side="right") - 1],
    } for p in positions]
    return eq, pd.DataFrame(trades)


def load_daily(codes):
    daily = {}
    for code in codes:
        _, d = load_stock(code)
        if d is None or d.empty:
            continue
        c = d["close"].values.astype(float)
        daily[code] = {
            "dates": d["date"].values, "pos": {t: k for k, t in enumerate(d["date"])},
            "o": d["open"].values.astype(float), "h": d["high"].values.astype(float),
            "l": d["low"].values.astype(float), "c": c,
            "ma5": pd.Series(c).rolling(5).mean().values,
        }
    return daily


def perf(eq: pd.Series, label: str):
    ret = eq.pct_change().dropna()
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = eq.iloc[-1] ** (1 / years) - 1
    dd = (eq / eq.cummax() - 1).min()
    sharpe = ret.mean() / ret.std() * np.sqrt(244) if ret.std() > 0 else np.nan
    return {"策略": label, "总收益%": round((eq.iloc[-1] - 1) * 100, 1), "年化%": round(cagr * 100, 1),
            "最大回撤%": round(dd * 100, 1), "夏普": round(sharpe, 2),
            "卡玛": round(cagr / -dd, 2) if dd < 0 else np.nan}

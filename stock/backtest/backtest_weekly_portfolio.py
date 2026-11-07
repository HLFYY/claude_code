"""
组合回测引擎（库，入口见 backtest_long_run.py）
数据：cache_bs_long/（fetch_daily_long_bs.py 拉取：baostock 前复权日线，2018起，含退市股、换手率、isST）
  - 周线由日线合成；大盘指数 深证综指 cache_bs_long/sz_399106.pkl（新浪拉取；深证成指 sz_399001.pkl 作对照）
  - 周线选股直接调用 stock_screener 的 check_buy_point_1 / check_buy_point_2 / _check_daily_w1，过滤同 process_one
  - 日线选股（screen_new_high_breakout_sina 的条件）见 backtest_daily_portfolio.py

大盘仓位（深证综指，只看收盘）：
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
import os as _os
import sys as _sys
_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))   # stock/ 根目录（stock_screener、stock_check_xinlang 在这）
if _ROOT not in _sys.path:
    _sys.path.insert(0, _ROOT)
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
INDEX_CODE = "sz.399106"           # 深证综指；对照用深证成指 "sz.399001"
INDEX_NAME = "深证综指"
WANT_SIGNALS = ("买点1-W2", "买点1-W3", "买点2", "买点3")
BP3_VOL_RATIO = 3.0                   # 买点3：买点1-W1 中突破周成交额 ≥ 上周 × 3（手动分组规则），下周一~四可买
LAYER = 0.10
GRID_MULT, GRID_PARTS = 1.2, 4
GRID_ROUND = "floor"   # 网格每份股数取整：floor 2600→600/600/600/800；ceil→700/700/700/500
WEAK_DAYS = 3        # 走弱判断天数（含买入日）：买入日弱→次日开盘卖，第 2~3 天弱→尾盘卖
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
        if r is not None and r["signal"].startswith("买点1-W1") and r["vol_ratio"] >= BP3_VOL_RATIO:
            r = {**r, "signal": "买点3"}
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


def stock_codes(include_bj=False):
    """baostock 全部A股（含退市）及名称；include_bj=True 时加上北交所（东方财富，见 fetch_bj_eastmoney.py，仅现存股票）"""
    names = pickle.load(open(LONG_DIR / "_stock_list.pkl", "rb")).set_index("code")["code_name"].to_dict()
    codes = [(c, names.get(c, "")) for c in sorted(names)]
    if include_bj and (LONG_DIR / "_stock_list_bj.pkl").exists():
        bj = pickle.load(open(LONG_DIR / "_stock_list_bj.pkl", "rb"))
        codes += list(zip(bj["code"], bj["code_name"]))
    return codes


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


def market_levels_v2(idx: pd.DataFrame, bottom_confirm=True, top_mode="off", confirm_rule="k3c",
                     weak_line="k3c", first_lv=0.4, strong=None, top_reduce=None, tail_clear=None, watch_ma=None,
                     top_no_new=False, top_need_high=None, top_min_days=None) -> pd.DataFrame:
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
      confirm_rule：底分型确认日（第3根之后一日）的开仓条件，弱势线仍为第3根收盘
          k3c     收盘 >= 第3根收盘
          maxc    收盘 >= 三根K线最高收盘
          maxc_h  收盘 >= 三根最高收盘 且 最高价 > 三根最高价
          k3c_h   收盘 >= 第3根收盘 且 最高价 > 三根最高价
          k3body  收盘 >= 第3根实体顶部 max(开,收)
      first_lv：首日建仓仓位（站上10日线 / 底分型确认的第一天），默认 0.4；满足加仓条件后加满 1.0
      strong=(涨幅阈值, 仓位)：开仓信号当天指数涨幅 > 阈值时首日改用该仓位，如 (0.03, 0.7)
      top_reduce：仅 top_mode=confirm。None=确认后第三日开盘清仓（实盘）；数值 x=确认当天尾盘降到 x 仓（0=尾盘清仓），
                  之后保持 ≤x，收盘突破顶点 → 按10日线规则恢复满仓，跌破10日线 → 清仓（输出 tail_level 列）
      tail_clear：清仓信号当天尾盘就清（可卖的尾盘卖，当天买的次日开盘卖）：None=全部次日开盘清（实盘）/
                  "ma10"=只有收盘跌破10日线这一种 / "all"=所有清仓信号（10日线、底分型弱势线、顶分型确认）
      watch_ma：如 8。10日线规则持仓中收盘跌破10日线、但收盘 ≥ watch_ma 日线 → 次日不清、不开新仓（观察）；
                次日收盘回到10日线上 → 恢复正常，否则次日尾盘清（输出 no_new 列）。None=不观察（实盘）
      top_no_new：顶分型出现后的确认日不开新仓（默认照常开）
      top_need_high：顶分型中间K最高须是近 N 日最高才算（"在顶部"），None=不限（实盘）
      top_min_days：本轮开仓已持有 ≥ N 天才看顶分型，None=不限（实盘）
      weak_line：底分型持仓后的弱势线（收盘跌破清仓、收盘站上加满）k3c=第3根收盘；k3body=第3根实体顶部
    """
    c, h, l, o = idx["close"].values, idx["high"].values, idx["low"].values, idx["open"].values
    body_top = np.maximum(o, c)
    wl = body_top if weak_line == "k3body" else c          # 弱势线取值
    ma10 = pd.Series(c).rolling(10).mean().values
    n = len(idx)
    level_open, clear_open = np.zeros(n), np.zeros(n, bool)
    tail_lv = np.full(n, np.nan)
    maw = pd.Series(c).rolling(watch_ma).mean().values if watch_ma else None
    no_new = np.zeros(n, bool)
    watching = False        # watch_ma：跌破10日线未破 watch_ma 线的观察日
    bottom_f, top_f = np.zeros(n, bool), np.zeros(n, bool)
    lv, frac_mode, k3_close = 0.0, False, 0.0
    pending = None          # 已出底分型、等待次日收盘确认的弱势线
    top_pending = None      # confirm 模式：等待次日确认的顶分型 (第3根收盘, 顶点)
    block = None            # 顶分型清仓后的失效线：收盘突破才允许重新开仓
    need_cross = False      # cross 模式：等待重新触发

    def entry(t):
        if strong is not None and t > 0 and c[t] / c[t - 1] - 1 > strong[0]:
            return strong[1]
        return first_lv

    for t in range(n - 1):
        if np.isnan(ma10[t]):
            continue
        bottom = t >= 2 and l[t - 1] < l[t - 2] and l[t - 1] < l[t] and h[t - 1] < h[t - 2] and h[t - 1] < h[t]
        top = t >= 2 and h[t - 1] > h[t - 2] and h[t - 1] > h[t] and l[t - 1] > l[t - 2] and l[t - 1] > l[t]
        bottom_f[t], top_f[t] = bottom, top
        above = c[t] >= ma10[t]

        def clear_all(why="other"):
            nonlocal lv, frac_mode, pending
            if lv > 0:
                clear_open[t + 1] = True
                if tail_clear == "all" or (tail_clear == "ma10" and why == "ma10"):
                    tail_lv[t] = 0.0
            lv, frac_mode, pending = 0.0, False, None

        # watch_ma 观察日收盘：回到10日线上 → 正常；否则尾盘清
        if watching:
            watching = False
            if not above and lv > 0:
                clear_all("watch")
                tail_lv[t] = 0.0
                level_open[t + 1] = 0.0
                need_cross = False
                continue
        # 顶分型：confirm 模式的次日确认
        if top_pending is not None:
            ref, hi = top_pending
            top_pending = None
            if c[t] <= ref and lv > 0:
                if top_reduce is None:
                    clear_all(); block = hi
                    continue
                if lv > top_reduce:
                    tail_lv[t] = top_reduce
                if top_reduce == 0:
                    clear_open[t + 1] = True      # 当天买的（T+1 尾盘卖不了）次日开盘清
                lv, frac_mode, pending, block = min(lv, top_reduce), False, None, hi
                level_open[t + 1] = lv
                continue
        # 顶分型：新出现（持仓或等待底分型确认时）
        if top and top_need_high and h[t - 1] < h[max(0, t - top_need_high + 1):t + 1].max():
            top = False                   # 中间K不是近 N 日最高：不在顶部，不算
        if top and top_min_days:
            k = t
            while k >= 0 and level_open[k] > 0:
                k -= 1
            if t - k < top_min_days:
                top = False               # 刚开仓没几天：不算
        if top_mode != "off" and top and (lv > 0 or pending is not None):
            if top_mode == "confirm":
                pending = None
                if lv > 0:
                    top_pending = (c[t], h[t - 1])
                    if top_no_new:
                        no_new[t + 1] = True      # 确认日不开新仓
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
            k3c, mc, mh, kb, kw = pending
            ok = c[t] >= {"maxc": mc, "maxc_h": mc, "k3body": kb}.get(confirm_rule, k3c)
            if confirm_rule.endswith("_h"):
                ok = ok and h[t] > mh
            if ok:
                lv, frac_mode, k3_close, need_cross = entry(t), True, kw, False
            pending = None
            if bottom and not above and lv == 0:
                pending = (c[t], c[t - 2:t + 1].max(), h[t - 2:t + 1].max(), body_top[t], wl[t])
        elif above and not need_cross and not frac_mode:
            lv = entry(t) if lv == 0 else 1.0
        elif above and frac_mode:
            frac_mode = False             # 站上10日线，回到10日线规则
            lv = entry(t) if lv == 0 else 1.0
        elif frac_mode and lv > 0:
            if c[t] < k3_close:
                clear_all()
            elif 0 < lv < 1.0 and (c[t] >= k3_close if bottom_confirm else c[t] > c[t - 1]):
                lv = 1.0
            if bottom and frac_mode:
                k3_close = wl[t]          # 新底分型，弱势线更新
        elif bottom and (not above or need_cross):
            if bottom_confirm:
                if not above:
                    clear_all("ma10")     # 线下：先按铁律清（若有仓），等确认再开
                pending = (c[t], c[t - 2:t + 1].max(), h[t - 2:t + 1].max(), body_top[t], wl[t])
            else:                         # 同 market_levels(break_k3c)：不清，直接进入底分型持仓
                lv = entry(t) if lv == 0 else (1.0 if c[t] > c[t - 1] else lv)
                frac_mode, k3_close, need_cross = True, c[t], False
        elif not above and watch_ma and lv > 0 and c[t] >= maw[t]:
            watching = True               # 跌破10日线但没破 watch_ma 线：次日观察，不清不开新仓
            no_new[t + 1] = True
        elif not above:
            clear_all("ma10")
            need_cross = False            # 已跌破，下一次站上即重新触发
        level_open[t + 1] = lv
    return pd.DataFrame({"date": idx["date"], "level": level_open, "clear": clear_open, "tail_level": tail_lv, "no_new": no_new,
                         "fractal": bottom_f, "top": top_f, "below": c < ma10})


# ─────────────────────────── 组合模拟 ───────────────────────────

class Pos:
    __slots__ = ("code", "sig", "buy_i", "buy_px", "shares0", "left", "levels", "sold", "cost", "pending", "alloc", "weak1")


PRIORITIES = {
    "评分高→低(默认)": (["score", "vol_ratio"], [False, False]),
    "RPS120高→低": (["rps120"], [False]),
    "RPS120低→高": (["rps120"], [True]),
    "10日涨幅排名强→弱": (["gain10_rank"], [True]),
    "10日涨幅排名弱→强": (["gain10_rank"], [False]),
    "辨识度档→RPS120高→低": (["tier", "rps120"], [True, False]),
    "辨识度档→10日涨幅排名强→弱": (["tier", "gain10_rank"], [True, True]),
    "min(RPS120名次,10日名次)": (["combo", "combo_src", "combo_other"], [True, True, True]),
    "RPS120(1位小数)高→低,同分10日强→弱": (["rps120_r1", "gain10_rank"], [False, True]),
    "RPS120(1位小数)高→低,同分10日弱→强": (["rps120_r1", "gain10_rank"], [False, False]),
    "辨识度档→RPS120低→高": (["tier", "rps120"], [True, True]),
    "辨识度档→10日涨幅排名弱→强": (["tier", "gain10_rank"], [True, False]),
    "周线在前→RPS120高→低": (["pool", "rps120_r1", "gain10_rank"], [False, False, False]),   # "weekly" > "daily"
}
TIER_MAX = 20     # 辨识度第k档：RPS120 ≥ 100-5k 或 10日涨幅名次 ≤ 100k，取满足的最小档


def rank_tables(daily: dict, dates=None) -> dict:
    """全市场 RPS120 百分位 / 120日涨幅名次 / 10日涨幅名次（行=日期，列=代码），算法同 stock_screener._calc_rps_and_rank"""
    g120, g10 = {}, {}
    for code, dd in daily.items():
        c = pd.Series(dd["c"], index=pd.to_datetime(dd["dates"]))
        g120[code] = c / c.shift(120) - 1
        g10[code] = c / c.shift(10) - 1
    p120, p10 = pd.DataFrame(g120), pd.DataFrame(g10)   # 停牌日为 NaN，不参与当日排名
    if dates is not None:
        p120, p10 = p120.reindex(dates), p10.reindex(dates)
    return {"rps120": p120.rank(axis=1, pct=True, method="min") * 100 - 100 / p120.notna().sum(axis=1).values[:, None],
            "rps120_rank": p120.rank(axis=1, ascending=False, method="min"),
            "gain10_rank": p10.rank(axis=1, ascending=False, method="min")}


def fill_rank_cols(sig: pd.DataFrame, tables: dict, on) -> pd.DataFrame:
    """按 on（每行的取值日期）从 rank_tables 取排名字段，并派生 rps120_r1 / 辨识度档 / combo"""
    sig = sig.copy()
    for k, t in tables.items():
        sig[k] = [t.at[d, c] if c in t.columns and d in t.index else np.nan for d, c in zip(on, sig["code"])]
    sig["rps120_r1"] = sig["rps120"].round(1)          # 实盘股票池显示/排序口径
    # 辨识度档位
    k_rps = np.ceil((100 - sig["rps120"]) / 5 - 1e-9).clip(lower=1)
    k_r10 = np.ceil(sig["gain10_rank"] / 100).clip(lower=1)
    sig["tier"] = pd.concat([k_rps, k_r10], axis=1).min(axis=1).clip(upper=TIER_MAX)
    # min(RPS120名次, 10日名次)：同分时来自 RPS120 的在前（combo_src=0），再按另一个名次
    r1, r2 = sig["rps120_rank"], sig["gain10_rank"]
    from_rps = r1.notna() & ((r1 <= r2) | r2.isna())
    sig["combo"] = np.where(from_rps, r1, r2)
    sig["combo_src"] = np.where(from_rps, 0, 1)
    sig["combo_other"] = np.where(from_rps, r2, r1)
    return sig


def add_rank_fields(sig: pd.DataFrame, daily: dict) -> pd.DataFrame:
    """信号日的排名字段"""
    return fill_rank_cols(sig, rank_tables(daily, sorted(sig["sig_date"].unique())), sig["sig_date"])


# 实盘约束模式默认参数：本金、单边滑点、佣金(含过户费，按财通证券对账单实测买卖都约万1.35、无最低5元)、
# 开盘最大涨幅（None=只跳过开盘涨停；0.06=开盘涨幅>6%就不买）
# 滑点按成交方式分开：集合竞价（开盘买、开盘卖）和网格限价单按挂单价成交不加；盘中止损条件单 slip_stop；尾盘走弱 slip_close
REAL_DEFAULT = {"capital": 800_000, "slip_auction": 0.0, "slip_stop": 0.002, "slip_close": 0.001,
                "fee_rate": 0.000135, "min_fee": 0.0, "max_open_gap": None, "slip_bj": 0.0}   # slip_bj：北交所买入/非网格卖出额外滑点


def _atr_pct(dd: dict, j: int, n: int = 14) -> float:
    """第 j 根收盘时的 ATR(n)%，同 stock_screener._calc_atr（保留 2 位）"""
    if j < n:
        return float("nan")
    h, l, c = dd["h"][j - n + 1:j + 1], dd["l"][j - n + 1:j + 1], dd["c"][j - n:j]
    tr = np.maximum(h - l, np.maximum(np.abs(h - c), np.abs(l - c)))
    return round(tr.mean() / dd["c"][j] * 100, 2)


def _limit_pct(code: str) -> float:
    if code.startswith("bj"):
        return 0.3
    return 0.2 if code[3:6] in ("300", "301", "688", "689") else 0.1


def run_portfolio(sig: pd.DataFrame, daily: dict, mkt: pd.DataFrame, use_freeze=True, priority="评分高→低(默认)",
                  pool_mode="weekly", t1=True, real=None, hold_rule="last", rank_daily=None,
                  grid_atr="prev", grid_geo=False, front_sig=None, front_days=None, front_by=(["vol_ratio"], [False]),
                  weak_mode="open", freeze_mkt=False, buy_day_stop="on", buy_day_exempt=None, exempt_day2=False, min_step=None, friday="all", weak_days=WEAK_DAYS, open_down=None, skip_buy=None, weak_cond="and", mon_open=None):
    """
    buy_day_exempt：买入当天涨幅（收盘/昨收-1）> 该值时，当天触发的止损/走弱都不算（不在次日开盘卖）；None=不豁免（实盘）
    friday：周五开新仓 all=正常开（实盘，2026-11-05 起）/ no=不开（之前的规则）/ daily_only=只开日线票（周线票已是上周五信号的第5天）
    min_step：网格步长%（买入前一日 ATR14% × 1.2）低于该值的票不买（如 4 = 4%）；None 不过滤
    exempt_day2：True 时第2天走弱也用同一涨幅豁免（当天涨幅 > buy_day_exempt 不尾盘卖）
    buy_day_stop：买入当天盘中触及止损价（max(昨MA5,昨低)×0.99）的处理（T+1，都是次日开盘卖）
          on          触及就次日开盘卖（实盘）
          off         买入当天不看止损，只看走弱
          open_above  开盘价 ≥ 止损价时才看（开盘就在止损价下方买入的，当天不算止损）
    freeze_mkt：大盘清仓 / 顶分型尾盘降仓卖出的资金也冻结到下周一（默认不冻结）
    mkt 带 tail_level 列（market_levels_v2 的 top_reduce）时：当天尾盘把持仓市值降到 总资产×tail_level，按浮盈低→高整只卖出（当天买的 T+1 不卖）
    weak_cond：不算走弱的条件：and=创新高且收涨（实盘，少一项就走弱）/ or=两项满足一项即可 / close=只看收涨 / high=只看创新高
    mon_open：每周第一个交易日开盘卖出的资金：None=冻结到下周一（实盘）/ "same"=上周触发的（顺延开盘卖）当天可用 /
              "next"=上周触发的次日可用 / "all_same"=周一所有开盘卖出（含开盘止损）当天可用
    skip_buy：{(日期, 代码)} 实盘手动跳过没买的票（前向跟踪同步实盘用），当天不买、顺延下一只
    open_down：新开票开盘大跌就跳过、顺延买下一只：None=不跳过（实盘）/ "limit"=开盘跌停 / 0.09 等=开盘跌幅 > 9%
    weak_days：走弱判断覆盖持有前几天（含买入日，None=整个持仓期），实盘 3（2026-11-05 由 2 改 3）
    weak_mode：新开票走弱（当日高>昨高 且 收盘>昨收，少一项=走弱）的处理
          open      买入当天走弱→次日开盘卖；第2天走弱→第2天尾盘卖（实盘）
          tail2     买入当天走弱→第2天尾盘卖（不再看第2天）；第2天走弱→第2天尾盘卖
          day2only  买入当天不判断；第2天走弱→第2天尾盘卖
          both      买入当天和第2天都走弱→第2天尾盘卖
          off       不做走弱判断
    front_sig/front_days：前置池。dates 属于 front_days 的买入日，把前一交易日的 front_sig 信号按 front_by 排序后放在候选池最前面
                          （如：空→开仓后 5 个交易日内底部倍量突破票优先，原候选池排在后面），同一只只算一次
    pool_mode: weekly=上周五信号本周一~四可买；daily=前一交易日信号仅当日开盘可买；
               mixed=sig 带 pool 列（"weekly"/"daily"），两类按各自有效期合成当天候选池，统一按优先级排序（同一只只算一次）
    real: None=理想成交；dict（见 REAL_DEFAULT）=实盘约束：
      - 按本金整手买入（100股，科创板≥200股），买不起一手跳过；网格每份按整手卖，最后一份卖剩余
      - 开盘即涨停（不只一字）买不到，跳过；一字跌停卖不出，顺延到下一交易日开盘
      - 滑点（买入价上浮、卖出价下浮）：集合竞价 slip_auction、盘中止损 slip_stop、尾盘 slip_close，网格限价不加；佣金 fee_rate（最低 min_fee 元）；印花税 2023-08-28 前千1、之后万5
      - max_open_gap：开盘涨幅超过该值不买（None 时只跳过开盘涨停）
    rank_daily: None=排名字段用信号日的值（周线票整周用上周五的）；rank_tables(daily) 的结果=每天按前一交易日收盘重算（同实盘股票池）
    grid_geo: 网格价位 False=按价格（等差，各格都相对买入价：买入价×(1+k×步长)）；True=按比例（下一格=上一格×(1+步长)，买入价×(1+步长)^k）
    grid_atr: 网格步长用的 ATR14%：prev=买入前一交易日（同实盘）；signal=信号日（周线票是信号周五）
    hold_rule: 周线票留池条件。last=前一日收盘 ≥ 突破价；all=信号日之后每日收盘都 ≥ 突破价（跌破一次本周剔除）
    """
    dates = list(mkt["date"])
    level = mkt["level"].values
    clear = mkt["clear"].values
    no_new = mkt["no_new"].values if "no_new" in mkt else np.zeros(len(mkt), bool)
    tail_level = mkt["tail_level"].values if "tail_level" in mkt else None
    mkt_bucket = "frozen" if freeze_mkt else "cash"
    week_id = pd.Series(dates).dt.to_period("W-FRI").values
    if real is not None:
        real = {**REAL_DEFAULT, **real}
        cap = float(real["capital"])
        slip_auc, slip_stop, slip_close = real["slip_auction"], real["slip_stop"], real["slip_close"]
        fee_rate, min_fee, max_gap = real["fee_rate"], real["min_fee"] / cap, real["max_open_gap"]
        stamp_cut = np.datetime64("2023-08-28")

    def buy_fee(amt):
        return max(min_fee, amt * fee_rate) if real else amt * BUY_FEE

    def sell_net(amt, i):
        if not real:
            return amt * (1 - SELL_FEE)
        stamp = 0.001 if np.datetime64(dates[i]) < stamp_cut else 0.0005
        return amt - max(min_fee, amt * fee_rate) - amt * stamp

    def locked_down(code, j):
        """一字跌停：全天卖不出"""
        dd = daily[code]
        return real is not None and dd["h"][j] == dd["l"][j] and dd["c"][j] <= dd["c"][j - 1] * (1 - _limit_pct(code)) + 0.011

    # 选股池：信号周 → 下一周可用，按优先级排序（缺值排最后）
    sig = sig.copy()
    by, asc = PRIORITIES[priority]
    if pool_mode == "mixed":
        is_wk = sig["pool"] == "weekly"
        wk_pools = {k: g for k, g in sig[is_wk].groupby(sig.loc[is_wk, "sig_date"].dt.to_period("W-FRI"))}
        dy_pools = {k: g for k, g in sig[~is_wk].groupby("sig_date")}

        def raw_pool(i):
            parts = [x for x in (wk_pools.get(week_id[i] - 1), dy_pools.get(dates[i - 1])) if x is not None]
            return pd.concat(parts) if parts else None
    else:
        sig["wk"] = sig["sig_date"].dt.to_period("W-FRI") if pool_mode == "weekly" else sig["sig_date"]
        pools = dict(list(sig.groupby("wk")))

        def raw_pool(i):
            return pools.get(week_id[i] - 1 if pool_mode == "weekly" else dates[i - 1])

    def get_pool(i):
        pool = raw_pool(i)
        if pool is None:
            return None
        if rank_daily is not None:
            pool = fill_rank_cols(pool, rank_daily, [pd.Timestamp(dates[i - 1])] * len(pool))
        return pool.sort_values(by, ascending=asc, na_position="last").drop_duplicates("code", keep="first")

    if front_sig is not None:
        front_pools = {k: g.sort_values(front_by[0], ascending=front_by[1]) for k, g in front_sig.groupby("sig_date")}
        front_days_set = set(pd.to_datetime(list(front_days)))
        _get_pool_base = get_pool

        def get_pool(i):
            base = _get_pool_base(i)
            fp = front_pools.get(dates[i - 1]) if pd.Timestamp(dates[i]) in front_days_set else None
            if fp is None:
                return base
            return pd.concat([fp] + ([base] if base is not None else [])).drop_duplicates("code", keep="first")

    cash = 1.0
    pending_grid = 0.0                  # 次日可用
    frozen = 0.0                        # 冻结中的资金合计（use_freeze=True 下周一可用；=N 卖出后第N+1个交易日开盘可用）
    frozen_q = []                       # use_freeze=N 时的 [(可用日序号, 金额)]
    freeze_days = use_freeze if type(use_freeze) is int else None
    positions: list = []
    trades, equity_curve = [], []
    traded_this_pool = set()
    expo = []                           # 开仓日：(日期, 开仓后占用层数, 允许层数)

    def px(code, i, field):
        dd = daily[code]
        j = dd["pos"].get(dates[i])
        return None if j is None else dd[field][j]

    def is_weak(dd, j):
        up_h, up_c = dd["h"][j] > dd["h"][j - 1], dd["c"][j] > dd["c"][j - 1]
        return not {"and": up_h and up_c, "or": up_h or up_c, "close": up_c, "high": up_h}[weak_cond]

    def close_pos(p, i, price, reason, bucket, slip=0.0):
        nonlocal cash, frozen, pending_grid
        left0 = p.left                                   # 规则退出（非网格）的剩余份额
        if real:
            price *= 1 - slip - (real["slip_bj"] if p.code.startswith("bj") else 0)
        val = sell_net(p.left * p.shares0 * price, i) if p.left > 1e-12 else 0.0
        p.cost += val
        if bucket == "next":
            pending_grid += val
        elif bucket == "frozen":
            frozen += val
            if freeze_days is not None:
                frozen_q.append((i + freeze_days + 1, val))
        else:
            cash += val
        trades.append({"code": p.code, "signal": p.sig["signal"], "sig_date": p.sig["sig_date"],
                       "buy_date": dates[p.buy_i], "exit_date": dates[i], "reason": reason,
                       "ret": p.cost / p.alloc - 1, "grid_sold": p.sold,
                       "daily_ok": p.sig["daily_ok"], "alloc": p.alloc, "buy_px": p.buy_px,
                       "exit_px": price, "left_frac": left0, "shares0": p.shares0, "step": p.levels[0] / p.buy_px - 1,
                       "pool": p.sig.get("pool"), "rps120_r1": p.sig.get("rps120_r1"), "gain10_rank": p.sig.get("gain10_rank")})
        p.left = 0

    def grid_sell(p, price, i, slip=0.0):
        nonlocal pending_grid
        if real:
            price *= 1 - slip
            total = round(p.shares0 * cap)
            left_sh = round(p.left * total)
            per = total / GRID_PARTS / 100                 # 每份几手：floor=向下取整（实盘，余数归最后一份）/ ceil=向上 / round=四舍五入
            per = int({"floor": np.floor, "ceil": np.ceil, "round": np.round}[GRID_ROUND](per)) * 100
            q_sh = left_sh if p.sold == GRID_PARTS - 1 else min(left_sh, max(100, per))
            q = q_sh / total
        else:
            q = 1.0 / GRID_PARTS
        val = sell_net(q * p.shares0 * price, i)
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

        # 1. 大盘铁律清仓（实盘约束下：停牌/一字跌停卖不出，顺延到下一交易日开盘）
        if clear[i]:
            for p in positions:
                j = daily[p.code]["pos"].get(day)
                if real and (j is None or locked_down(p.code, j)):
                    p.pending = p.pending or "大盘清仓(顺延)"
                    continue
                o = px(p.code, i, "o")
                if o is None:  # 停牌：按最近收盘清（近似）
                    dd = daily[p.code]; j = np.searchsorted(dd["dates"], np.datetime64(day)) - 1
                    o = dd["c"][j]
                close_pos(p, i, o, "大盘清仓", mkt_bucket, slip_auc if real else 0)
            positions = [p for p in positions if p.left > 1e-9]

        # 2. 持仓开盘处理
        for p in positions:
            j = daily[p.code]["pos"].get(day)
            if j is None:
                continue
            dd = daily[p.code]
            if p.pending:                  # T+1 / 顺延：该卖未卖的，今天开盘卖
                if locked_down(p.code, j):
                    continue
                bk = mkt_bucket if p.pending.startswith("大盘") else "frozen"
                if new_week and bk == "frozen" and mon_open:
                    bk = "next" if mon_open == "next" else "cash"     # 上周触发、本周一开盘卖：算上周的钱
                close_pos(p, i, dd["o"][j], p.pending, bk, slip_auc if real else 0); continue
            stop = max(dd["ma5"][j - 1] * 0.99, dd["l"][j - 1] * 0.99)
            if dd["o"][j] <= stop:
                if locked_down(p.code, j):
                    p.pending = "止损(跌停顺延)"; continue
                close_pos(p, i, dd["o"][j], "止损(开盘)", "cash" if new_week and mon_open == "all_same" else "frozen",
                          slip_auc if real else 0); continue
            while p.sold < GRID_PARTS and dd["o"][j] >= p.levels[p.sold]:
                grid_sell(p, dd["o"][j], i, slip_auc if real else 0)
            if p.left <= 1e-9:
                close_pos(p, i, 0, "网格卖完", "cash")
        positions = [p for p in positions if p.left > 1e-9]

        # 3. 开新仓
        wd = pd.Timestamp(day).weekday()
        if level[i] > 0 and not clear[i] and not no_new[i] and (wd != 4 or friday != "no"):
            pool = get_pool(i)
            if wd == 4 and friday == "daily_only" and pool is not None and "pool" in pool:
                pool = pool[pool["pool"] == "daily"]
            eq_prev = equity_curve[-1]
            layer_amt = LAYER * eq_prev
            held = {p.code for p in positions}
            used_layers = sum(p.left * p.shares0 * p.buy_px for p in positions) / layer_amt if positions else 0
            if pool is not None:
                for _, s in pool.iterrows():
                    if used_layers + 1 > level[i] * 10 + 1e-6 or cash < layer_amt * (1 - 1e-6) * (0.9 if real else 1):
                        break
                    code = s["code"]
                    if code in held or code in traded_this_pool or code not in daily:
                        continue
                    if skip_buy and (pd.Timestamp(day), code) in skip_buy:
                        continue
                    dd = daily[code]
                    j = dd["pos"].get(day)
                    if j is None or j < 6:
                        continue
                    if dd["c"][j - 1] < s["breakout_price"]:   # 周中走坏，不符合条件
                        continue
                    if hold_rule == "all" and s.get("pool", "weekly") == "weekly":
                        k = dd["pos"].get(s["sig_date"])
                        if k is not None and k + 1 < j and dd["c"][k + 1:j].min() < s["breakout_price"]:
                            continue
                    if min_step is not None:
                        a_ = _atr_pct(dd, j - 1)
                        if not a_ > 0:
                            a_ = s["atr"]
                        if a_ * GRID_MULT < min_step:
                            continue
                    lim_up = dd["c"][j - 1] * (1 + _limit_pct(code)) - 0.011
                    if real:
                        if dd["o"][j] >= lim_up:                 # 开盘即涨停，集合竞价买不到
                            continue
                        if max_gap is not None and dd["o"][j] > dd["c"][j - 1] * (1 + max_gap):
                            continue                             # 开盘涨幅过大不追
                    if open_down is not None:
                        lim_dn = dd["c"][j - 1] * (1 - _limit_pct(code)) + 0.011 if open_down == "limit" else dd["c"][j - 1] * (1 - open_down)
                        if dd["o"][j] <= lim_dn if open_down == "limit" else dd["o"][j] < lim_dn:
                            continue                             # 开盘大跌不买，顺延下一只
                    elif dd["h"][j] == dd["l"][j] and dd["o"][j] >= lim_up:
                        continue
                    buy_px = dd["o"][j] * (1 + slip_auc + (real["slip_bj"] if code.startswith("bj") else 0)) if real else dd["o"][j]
                    if real:
                        budget = min(layer_amt, cash) * cap
                        sh = budget / (buy_px * (1 + fee_rate))
                        if code[3:6] in ("688", "689"):
                            sh = int(sh) if sh >= 200 else 0
                        elif code.startswith("bj"):                # 北交所：≥100股，之后1股递增
                            sh = int(sh) if sh >= 100 else 0
                        else:
                            sh = int(sh // 100 * 100)
                        if sh == 0:                              # 买不起一手
                            continue
                        shares0 = sh / cap
                    else:
                        shares0 = layer_amt / (buy_px * (1 + BUY_FEE))
                    p = Pos()
                    p.code, p.sig, p.buy_i, p.buy_px = code, s, i, buy_px
                    p.shares0 = shares0
                    p.alloc = shares0 * buy_px + buy_fee(shares0 * buy_px)
                    p.left, p.sold, p.cost, p.pending = 1.0, 0, 0.0, None
                    atr = _atr_pct(dd, j - 1) if grid_atr == "prev" else s["atr"]
                    if not atr > 0:
                        atr = s["atr"]
                    step = atr * GRID_MULT / 100
                    p.levels = [buy_px * ((1 + step) ** k if grid_geo else 1 + k * step) for k in range(1, GRID_PARTS + 1)]
                    cash -= p.alloc
                    positions.append(p)
                    held.add(code); traded_this_pool.add(code)
                    used_layers += 1
            expo.append((day, used_layers, level[i] * 10))

        # 4. 盘中 + 尾盘
        for p in positions:
            dd = daily[p.code]
            j = dd["pos"].get(day)
            if j is None or p.pending:
                continue
            stop = max(dd["ma5"][j - 1] * 0.99, dd["l"][j - 1] * 0.99)
            if t1 and i == p.buy_i:        # T+1：买入当天不卖，触及止损/走弱则次日开盘卖，网格不成交
                p.weak1 = is_weak(dd, j)
                if buy_day_exempt is not None and dd["c"][j] / dd["c"][j - 1] - 1 > buy_day_exempt:
                    p.weak1 = False
                    continue
                if dd["l"][j] <= stop and (buy_day_stop == "on" or (buy_day_stop == "open_above" and dd["o"][j] >= stop)):
                    p.pending = "买入日止损→次日开盘"
                elif p.weak1 and weak_mode == "open":
                    p.pending = "买入日走弱→次日开盘"
                continue
            if dd["l"][j] <= stop:
                if locked_down(p.code, j):
                    p.pending = "止损(跌停顺延)"; continue
                close_pos(p, i, min(stop, dd["o"][j]), "止损", "frozen", slip_stop if real else 0); continue
            while p.sold < GRID_PARTS and dd["h"][j] >= p.levels[p.sold]:
                grid_sell(p, p.levels[p.sold], i)
            if p.left <= 1e-9:
                close_pos(p, i, 0, "网格卖完", "cash"); continue
            held_days = sum(1 for k in range(p.buy_i, i + 1) if dd["pos"].get(dates[k]) is not None)
            weak = is_weak(dd, j)
            w1 = getattr(p, "weak1", False)
            sell_weak = {"open": weak, "day2only": weak, "tail2": weak or w1, "both": weak and w1, "off": False}[weak_mode]
            if sell_weak and exempt_day2 and buy_day_exempt is not None and dd["c"][j] / dd["c"][j - 1] - 1 > buy_day_exempt:
                sell_weak = False
            if (weak_days is None or held_days <= weak_days) and sell_weak:
                if locked_down(p.code, j):
                    p.pending = "走弱(跌停顺延)"; continue
                close_pos(p, i, dd["c"][j], "走弱尾盘", "frozen", slip_close if real else 0)
        positions = [p for p in positions if p.left > 1e-9]

        # 4b. 顶分型确认当天尾盘降仓
        if tail_level is not None and not np.isnan(tail_level[i]) and positions:
            vals = {}
            for p in positions:
                dd = daily[p.code]
                k = np.searchsorted(dd["dates"], np.datetime64(day), side="right") - 1
                vals[id(p)] = p.left * p.shares0 * dd["c"][k]
            mv = sum(vals.values())
            target = tail_level[i] * (cash + pending_grid + frozen + mv)
            cand = []
            for p in positions:
                j = daily[p.code]["pos"].get(day)
                if j is None or p.buy_i == i or p.pending or locked_down(p.code, j):
                    continue
                cand.append((daily[p.code]["c"][j] / p.buy_px, j, p))
            for _, j, p in sorted(cand, key=lambda x: x[0]):
                if mv <= target + 1e-9:
                    break
                mv -= vals[id(p)]
                close_pos(p, i, daily[p.code]["c"][j], "大盘尾盘降仓" if tail_level[i] > 0 else "大盘尾盘清仓(尾盘降仓)", mkt_bucket, slip_close if real else 0)
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
    eq.attrs["exposure"] = pd.DataFrame(expo, columns=["date", "used", "allowed"])
    eq.attrs["open_positions"] = [{
        "code": p.code, "signal": p.sig["signal"], "sig_date": p.sig["sig_date"], "buy_date": dates[p.buy_i],
        "buy_px": p.buy_px, "left": p.left, "grid_sold": p.sold, "levels": p.levels,
        "alloc": p.alloc, "pending": p.pending, "shares0": p.shares0,
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

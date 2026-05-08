#!/usr/bin/env python3
"""
A股选股程序：周线倍量突破逻辑
===============================
买点1：近期出现倍量突破N周新高 + 随后缩量确认（最强信号，大资金进场+筹码锁定）
买点2：历史有过倍量突破 → 经过回踩洗盘 → 当前重新启动（筹码更干净）

数据源：baostock（免费，无需购买）
缓存：./cache/ 目录，半天内不重复下载

新增输出字段：
  - rps120: 120日相对价格强度（RPS），该股120日涨幅在全市场的百分位排名(0~100)
  - gain10_rank: 近10日涨幅在全市场的排名名次（第1名最强）
"""

import os
import sys
import time
import pickle
import warnings
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
#  参数配置
# ─────────────────────────────────────────────
CFG = {
    # ── 数据 ──
    "start_date":       "2023-01-01",
    "cache_dir":        "./cache",
    # 周线新鲜度：以"下一个周五 15:00 收盘"为界，收盘前不重拉（见 _weekly_is_fresh）
    "max_workers":      8,
    "request_delay":    0.05,

    # ── 均线（周） ──
    "ma_long":          20,   # 20周线：判断方向 + 新高基准
    "ma_short":         5,    # 5周线：买点2回踩支撑

    # ── 买点1：放量突破确认型 ──
    # 力度周（W1）条件
    "bp1_vol_ratio":         2.0,   # 成交额 ≥ 上周×2（倍量）
    "bp1_gain_min":          0.05,  # 周涨幅 ≥ 5%
    # 确认周（W2）条件
    "bp1_confirm_shrink":    0.8,   # 缩量：成交额 ≤ W1×0.8 → 优先做
    "bp1_confirm_warm_max":  1.3,   # 温和放量上限：≤ W1×1.3 → 可做排后
                                    # > W1×1.3 明显放量 → 不做
    "bp1_confirm_gain_max":  0.12,  # W2 涨幅 < 12%
    "bp1_max_entry_age":     2,     # W1 最多2周前（W2最优/W3次优/W4+不做）

    # ── 买点2：回踩反包型 ──
    "bp2_pre_green_min":     2,     # 回踩前连续收红 ≥ 2 周
    "bp2_pullback_max":      4,     # 回踩最大持续周数（1-4周）
    "bp2_near_double_weeks": 4,     # 近N周内必须有一周倍量
    "bp2_confirm_vol_max":   1.3,   # 反包周量能上限（明显放量则不做）

    # ── 硬过滤（一票否决）──
    "hard_gain_max":         0.30,  # 本周涨幅 ≥ 30% 过热
    "hard_consec_green_max": 7,     # 连红 ≥ 7 周
    "hard_hist_spike_min":   0.20,  # 历史须有单周涨幅 ≥ 20%（辨识度验证）

    # ── 通用过滤 ──
    "min_price":        3.0,
    "min_turnover":     0.5,   # 最低周换手率%
    "min_market_cap":   50,    # 最低流通市值估算（亿）
    "min_weeks_data":   56,    # 最少历史周数
    "exclude_st":       True,
    "min_score":        -1,

    # ── RPS / 涨幅排名 ──
    "rps_days":         120,   # RPS 计算天数
    "gain_rank_days":   10,    # 短期涨幅排名天数
}

# ─────────────────────────────────────────────
#  全局 baostock session
#  baostock 使用自定义 socket 协议，不支持并发，
#  用全局锁串行化所有网络请求，分析逻辑仍可并行。
# ─────────────────────────────────────────────
import threading
import baostock as _bs_global

_bs_lock    = threading.Lock()   # 所有网络请求串行化
_bs_logged  = False

_no_new_count        = 0              # 连续无新数据的拉取次数
_first_new_logged    = False          # 是否已打印第一条拉取日期
_stop_scan           = threading.Event()  # 触发后 process_one 直接跳过
_last_known_date     = None           # 最近一次缓存/拉取的数据日期

_daily_fail_reasons  = {}             # 日线确认失败原因计数 {"原因": N}
_daily_fail_lock     = threading.Lock()

# ─────────────────────────────────────────────
#  全市场涨幅数据（用于 RPS / 涨幅排名）
# ─────────────────────────────────────────────
# 结构：{ code: {"gain120": float, "gain10": float} }
_market_gains: dict = {}
_market_gains_lock = threading.Lock()


def _ensure_login():
    global _bs_logged
    if not _bs_logged:
        _bs_global.login()
        _bs_logged = True


# ─────────────────────────────────────────────
#  数据获取（带缓存）
# ─────────────────────────────────────────────

def _cache_path(code: str) -> str:
    os.makedirs(CFG["cache_dir"], exist_ok=True)
    safe = code.replace(".", "_")
    return os.path.join(CFG["cache_dir"], f"{safe}.pkl")


def _cache_path_daily(code: str) -> str:
    os.makedirs(CFG["cache_dir"], exist_ok=True)
    safe = code.replace(".", "_")
    return os.path.join(CFG["cache_dir"], f"{safe}_d.pkl")


def _weekly_is_fresh(df: pd.DataFrame) -> bool:
    """
    判断周线缓存是否仍然有效。
    逻辑：最新一根周线的收盘日（周五）加 7 天 = 下一根周线的收盘日，
    下一根收盘日 15:00 之前数据不会有更新，视为新鲜。
    例：上周五 Apr 10 的数据，在本周五 Apr 17 15:00 之前都是最新。
    """
    last_date  = df["date"].iloc[-1]                          # pandas Timestamp
    next_close = (last_date + timedelta(days=7)).to_pydatetime().replace(
        hour=15, minute=0, second=0, microsecond=0
    )
    return datetime.now() < next_close


def _parse_raw(df: pd.DataFrame) -> pd.DataFrame:
    """统一处理 baostock 返回的原始 DataFrame"""
    df = df.rename(columns={"turn": "turnover"})
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 成交额：优先用接口 amount 字段，否则用 volume×close 估算
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["amount"] = df.get("amount", pd.Series(dtype=float)).fillna(df["volume"] * df["close"])
    return df.dropna(subset=["close", "volume"]).reset_index(drop=True)


def _bs_fetch(code: str, start_date: str) -> Optional[pd.DataFrame]:
    """调用 baostock 接口（必须在 _bs_lock 内调用）"""
    try:
        _ensure_login()
        time.sleep(CFG["request_delay"])
        rs = _bs_global.query_history_k_data_plus(
            code,
            "date,open,high,low,close,volume,amount,turn",
            start_date=start_date,
            frequency="w",
            adjustflag="2",
        )
        df = rs.get_data()
        return _parse_raw(df) if (df is not None and not df.empty) else None
    except Exception:
        return None


def fetch_weekly(code: str) -> Optional[pd.DataFrame]:
    """
    获取周线数据，支持增量更新：
      - 无缓存        → 全量下载
      - 有缓存且已是本周最新  → 直接返回缓存
      - 有缓存但有新周数据    → 只补拉缺少的部分，追加后保存
    """
    path = _cache_path(code)

    # ── 读缓存 ──
    cached: Optional[pd.DataFrame] = None
    if os.path.exists(path):
        with open(path, "rb") as f:
            cached = pickle.load(f)
        # 旧缓存没有 amount 列，用 volume×close 估算补全
        if cached is not None and "amount" not in cached.columns:
            cached["amount"] = cached["volume"] * cached["close"]

    if cached is not None and not cached.empty:
        if _weekly_is_fresh(cached):
            return cached                  # 下一根周线尚未收盘，直接返回

    # 已确认无新数据，跳过网络请求直接用缓存
    if _stop_scan.is_set():
        return cached

    # ── 需要网络请求（串行） ──
    with _bs_lock:
        # 双重检查（另一线程可能刚更新过同一股票）
        if os.path.exists(path):
            with open(path, "rb") as f:
                cached = pickle.load(f)
            if cached is not None and not cached.empty:
                if _weekly_is_fresh(cached):
                    return cached

        global _no_new_count, _first_new_logged, _last_known_date

        if cached is not None and not cached.empty:
            # ── 增量模式：只拉最后日期之后的数据 ──
            last_date  = cached["date"].iloc[-1]
            start      = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            new_df     = _bs_fetch(code, start)

            if new_df is not None and not new_df.empty:
                merged = (
                    pd.concat([cached, new_df], ignore_index=True)
                    .drop_duplicates("date")
                    .sort_values("date")
                    .reset_index(drop=True)
                )
                with open(path, "wb") as f:
                    pickle.dump(merged, f)
                _no_new_count = 0
                _last_known_date = merged["date"].iloc[-1].date()
                if not _first_new_logged:
                    _first_new_logged = True
                    tqdm.write(f"拉取后数据日期: {_last_known_date}")
                return merged
            else:
                # 无新数据（本周未收盘或停牌）
                _last_known_date = cached["date"].iloc[-1].date()
                _no_new_count += 1
                if _no_new_count >= 10 and not _stop_scan.is_set():
                    _stop_scan.set()
                    tqdm.write(f"无新数据，停止扫描，接口数据时间:{_last_known_date}")
                return cached
        else:
            # ── 全量模式：首次下载 ──
            df = _bs_fetch(code, CFG["start_date"])
            if df is None:
                return None
            with open(path, "wb") as f:
                pickle.dump(df, f)
            _no_new_count = 0
            _last_known_date = df["date"].iloc[-1].date()
            if not _first_new_logged:
                _first_new_logged = True
                tqdm.write(f"拉取后数据日期: {_last_known_date}")
            return df


def fetch_daily_recent(code: str, n: int = 30) -> Optional[pd.DataFrame]:
    """
    获取最近 n 个交易日的日线数据，用于计算 ATR(14)。
    用文件修改时间判断新鲜度，避免节假日/周末因数据日期落后而误判为过期。
    """
    path = _cache_path_daily(code)

    if _daily_cache_is_fresh(path):
        with open(path, "rb") as f:
            cached = pickle.load(f)
        if cached is not None and not cached.empty:
            return cached

    start = (datetime.now() - timedelta(days=n * 2)).strftime("%Y-%m-%d")  # 留足节假日余量
    with _bs_lock:
        # 双重检查
        if _daily_cache_is_fresh(path):
            with open(path, "rb") as f:
                cached = pickle.load(f)
            if cached is not None and not cached.empty:
                return cached
        try:
            _ensure_login()
            time.sleep(CFG["request_delay"])
            rs = _bs_global.query_history_k_data_plus(
                code,
                "date,open,high,low,close,volume,turn",
                start_date=start,
                frequency="d",
                adjustflag="2",
            )
            df = rs.get_data()
            if df is None or df.empty:
                return None
            df = _parse_raw(df).tail(n).reset_index(drop=True)
            with open(path, "wb") as f:
                pickle.dump(df, f)
            return df
        except Exception:
            return None


def get_stock_list() -> pd.DataFrame:
    """获取全部A股列表（sh.6xxx / sz.0xxx / sz.3xxx）"""
    _ensure_login()
    rs = _bs_global.query_stock_basic()
    df = rs.get_data()

    df = df[(df["type"] == "1") & (df["status"] == "1")]
    df = df[df["code"].str.match(r"^(sh\.6|sz\.0|sz\.3)")]

    if CFG["exclude_st"]:
        df = df[~df["code_name"].str.contains(r"\*?ST", regex=True, na=False)]

    return df[["code", "code_name"]].rename(columns={"code_name": "name"}).reset_index(drop=True)


# ─────────────────────────────────────────────
#  ATR 计算（日线，标准 14 日）
# ─────────────────────────────────────────────

def _calc_atr(df: pd.DataFrame, n: int = 14) -> float:
    """返回 ATR%（ATR(n日) / 当前收盘价 × 100），方便跨股票比较"""
    if len(df) < n + 1:
        return float("nan")
    c = df["close"].values
    h = df["high"].values
    l = df["low"].values
    tr = np.maximum(h[1:] - l[1:],
         np.maximum(np.abs(h[1:] - c[:-1]),
                    np.abs(l[1:] - c[:-1])))
    atr = tr[-n:].mean()
    return round(atr / c[-1] * 100, 2)


# ─────────────────────────────────────────────
#  辅助函数
# ─────────────────────────────────────────────

def _count_consec_green(closes: np.ndarray, opens: np.ndarray, end_idx: int) -> int:
    """从 end_idx 往前数连续收红（close≥open）周数"""
    count = 0
    for i in range(end_idx, -1, -1):
        if closes[i] >= opens[i]:
            count += 1
        else:
            break
    return count


def _ma(closes: np.ndarray, idx: int, window: int) -> float:
    """idx 处的 window 周移动平均（不含 idx 本周，用于判断突破）"""
    if idx < window:
        return float("nan")
    return float(closes[idx - window: idx].mean())


# ─────────────────────────────────────────────
#  买点1 日线确认（力度周内部）
# ─────────────────────────────────────────────

def _check_daily_w1(daily: pd.DataFrame, w1_date, gain_w1: float) -> list:
    """
    检查力度周（W1）内的日线质量。
    返回失败原因列表，空列表表示全部通过。
    1. 涨放量跌缩量：上涨日均成交额 > 下跌日均成交额（买方主导）
    2. 无大幅回吐：无单日跌幅 ≥ W1周涨幅×50%（相对前收）的阴线
    3. 无大阴线：振幅 > W1周涨幅×30% 且最低价跌幅 > 3% 的阴线
    数据不足时（W1 超出近期日线范围）直接放行，返回空列表。
    """
    w1_ts   = pd.Timestamp(w1_date)
    w_start = w1_ts - timedelta(days=6)
    week_df = daily[(daily["date"] >= w_start) & (daily["date"] <= w1_ts)].copy()

    if len(week_df) < 3:
        return []   # 数据不足，放行

    closes  = week_df["close"].values
    opens   = week_df["open"].values
    highs   = week_df["high"].values
    lows    = week_df["low"].values
    amounts = week_df["amount"].values if "amount" in week_df.columns \
              else (week_df["volume"].values * closes)

    pullback_limit = gain_w1 * 0.5
    big_red_limit  = gain_w1 * 0.3

    reasons = []

    # ── 检查1：涨放量跌缩量 ──
    up_mask   = closes >= opens
    down_mask = closes < opens
    up_amt   = amounts[up_mask].mean()   if up_mask.any()   else 0
    down_amt = amounts[down_mask].mean() if down_mask.any() else 0
    if down_mask.any() and up_mask.any() and up_amt <= down_amt:
        reasons.append("涨放量跌缩量")

    # ── 检查2：无大幅回吐 ──
    for i in range(1, len(week_df)):
        if closes[i - 1] <= 0:
            continue
        daily_chg = (closes[i] - closes[i - 1]) / closes[i - 1]
        if daily_chg <= -pullback_limit and closes[i] < opens[i]:
            reasons.append("大幅回吐")
            break

    # ── 检查3：无大阴线（振幅 > W1周涨幅×30% 且最低价跌幅 > 3%）──
    for i in range(len(week_df)):
        candle_rng = highs[i] - lows[i]
        ref_price  = max(closes[i], 0.01)
        prev_close = closes[i - 1] if i > 0 else opens[i]
        low_drop   = (prev_close - lows[i]) / max(prev_close, 0.01)
        if (closes[i] < opens[i]
                and candle_rng / ref_price > big_red_limit
                and low_drop > 0.03):
            reasons.append("大阴线")
            break

    return reasons


# ─────────────────────────────────────────────
#  买点1：放量突破确认型（优先级最高）
# ─────────────────────────────────────────────

def check_buy_point_1(df: pd.DataFrame) -> Optional[dict]:
    """
    力度周 W1：20周新高 + 收盘站上20MA + 20MA走平或向上 + 周涨幅≥5% + 成交额≥上周×2
    确认周 W2：收盘>W1 + 收红 + 涨幅<12% + 成交额≤W1×1.3（>1.3不做）
    入场窗口：
      age=0 → W1就是本周，下周一入场（W2最优，W2未知故标"待W2"）
      age=1 → W1上周，本周=W2已收盘可验证（最优）
      age=2 → W1两周前，本周=W3（次优）
      age≥3 → 不做
    日线确认由 process_one 在命中后单独拉取并调用 _check_daily_w1。
    """
    MA_L    = CFG["ma_long"]
    last    = len(df) - 1
    closes  = df["close"].values
    opens   = df["open"].values
    amounts = df["amount"].values
    MAX_AGE = CFG["bp1_max_entry_age"]   # 2

    for age in range(0, MAX_AGE + 1):   # age=0 表示 W1 = 本周
        w1 = last - age
        if w1 < MA_L + 2:
            continue

        # ── W1 条件 ──
        # 1. 20周新高（收盘 > 前20周最高收盘）
        prior_20_high = closes[w1 - MA_L: w1].max()
        if closes[w1] <= prior_20_high:
            continue

        # 2. 20周线走平或向上（容差0.1%）
        ma20_w1   = closes[w1 - MA_L: w1].mean()
        ma20_prev = closes[w1 - MA_L - 1: w1 - 1].mean()
        if ma20_w1 < ma20_prev * 0.999:
            continue

        # 3. 收盘站上20周线
        if closes[w1] <= ma20_w1:
            continue

        # 4. 周涨幅 ≥ 5%
        if closes[w1 - 1] <= 0:
            continue
        gain_w1 = (closes[w1] - closes[w1 - 1]) / closes[w1 - 1]
        if gain_w1 < CFG["bp1_gain_min"]:
            continue

        # 5. 成交额 ≥ 上周×2（倍量）
        if amounts[w1 - 1] <= 0:
            continue
        w1_vol_ratio = amounts[w1] / amounts[w1 - 1]
        if w1_vol_ratio < CFG["bp1_vol_ratio"]:
            continue

        # ── age=0：W1 = 本周，W2 尚未发生，直接输出待入场信号 ──
        if age == 0:
            score = round(30 + min(w1_vol_ratio * 3, 12), 1)  # 最优优先级
            return {
                "signal":         "买点1-W1(下周一入场)",
                "score":          score,
                "breakout_date":  df["date"].iloc[w1].strftime("%Y-%m-%d"),
                "breakout_price": round(closes[w1], 2),
                "current_price":  round(closes[last], 2),
                "chg_since":      f"{(closes[last]/closes[w1]-1)*100:+.1f}%",
                "vol_ratio":      round(w1_vol_ratio, 2),
                "w2_vol_ratio":   None,
                "weeks_ago":      0,
                "gain_w1":        round(gain_w1, 4),   # 供日线确认使用
                "daily_ok":       "是",    # 由 process_one 在拉日线后覆盖
            }

        # ── age≥1：W2 已收盘，验证确认周条件 ──
        w2 = w1 + 1
        if w2 > last:
            continue

        # W2 收盘 > W1 收盘
        if closes[w2] <= closes[w1]:
            continue

        # W2 收红
        if closes[w2] < opens[w2]:
            continue

        # W2 涨幅 < 12%
        gain_w2 = (closes[w2] - closes[w1]) / closes[w1]
        if gain_w2 >= CFG["bp1_confirm_gain_max"]:
            continue

        # W2 量能三档判断
        if amounts[w1] <= 0:
            continue
        w2_vol_ratio = amounts[w2] / amounts[w1]
        if w2_vol_ratio > CFG["bp1_confirm_warm_max"]:   # >1.3 明显放量，不做
            continue

        is_shrink = w2_vol_ratio <= CFG["bp1_confirm_shrink"]  # ≤0.8 缩量，优先

        # ── 评分 ──
        vol_score   = 20 if is_shrink else 10
        age_score   = 20 if age == 1 else 10
        ratio_score = min(w1_vol_ratio * 3, 12)
        score = round(vol_score + age_score + ratio_score, 1)

        entry_week   = age + 1                        # age=1→W2已过/W3入场, age=2→W3已过/W4…
        confirm_type = "缩量" if is_shrink else "温和"

        return {
            "signal":         f"买点1-W{entry_week}({confirm_type})",
            "score":          score,
            "breakout_date":  df["date"].iloc[w1].strftime("%Y-%m-%d"),
            "breakout_price": round(closes[w1], 2),
            "current_price":  round(closes[last], 2),
            "chg_since":      f"{(closes[last]/closes[w1]-1)*100:+.1f}%",
            "vol_ratio":      round(w1_vol_ratio, 2),
            "w2_vol_ratio":   round(w2_vol_ratio, 2),
            "weeks_ago":      age,
            "gain_w1":        round(gain_w1, 4),   # 供日线确认使用
            "daily_ok":       True,   # 由 process_one 在拉日线后覆盖
        }

    return None


# ─────────────────────────────────────────────
#  买点2：回踩反包型（优先级次之）
# ─────────────────────────────────────────────

def check_buy_point_2(df: pd.DataFrame) -> Optional[dict]:
    """
    当前周：反包突破20周新高 + 收红 + 量能不明显放量（≤突破周×1.3）
    突破周：前面5周至少有1周突破之前的20周新高，突破周之前必须连续收红≥1周
    回踩周：当前周和突破周之间，至少1周，收盘价低于当前周和突破周收盘价
    回踩1-2周：每周成交额 < 突破周成交额，且周收盘价 ≥ 当时5MA
    回踩3-4周：每周成交额 < 突破周成交额×1.3，且周收盘价 ≥ 当时20MA
    5周线走平或向上
    近4周（不含当前）内必须有一周倍量（成交额≥上周×2）
    """
    is_test = False
    MA_L = CFG["ma_long"]   # 20
    MA_S = CFG["ma_short"]  # 5
    last = len(df) - 1
    if last < MA_L + 6:
        return None
    if is_test:
        print('111')
    closes  = df["close"].values
    opens   = df["open"].values
    amounts = df["amount"].values

    # ── 当前周：反包突破20周新高 ──
    prior_20_high_cur = closes[last - MA_L: last].max()
    if closes[last] <= prior_20_high_cur:
        return None

    # 当前周收红
    if closes[last] < opens[last]:
        return None
    if is_test:
        print(222)
    # ── 5周线走平或向上 ──
    if last >= MA_S + 1:
        ma5_now  = closes[last - MA_S + 1: last + 1].mean()
        ma5_prev = closes[last - MA_S: last].mean()
        if ma5_now < ma5_prev * 0.998:
            return None
    if is_test:
        print(333)

    # ── 寻找突破周：当前周前5周内，找最近一周突破当时20周新高的周 ──
    breakout_idx = None
    search_start = max(MA_L + 1, last - 5)
    for i in range(last - 1, search_start - 1, -1):
        prior_20_high_i = closes[i - MA_L: i].max()
        if closes[i] > prior_20_high_i:
            breakout_idx = i
            break

    if breakout_idx is None:
        return None

    # 当前周量能不明显放量（≤ 突破周×1.3）
    breakout_amt = amounts[breakout_idx]
    if breakout_amt > 0 and amounts[last] > breakout_amt * CFG["bp2_confirm_vol_max"]:
        return None
    if is_test:
        print(444)
    # ── 突破周之前连续收红 ≥ 1 周 ──
    pre_green = _count_consec_green(closes, opens, breakout_idx - 1)
    if pre_green < 1:
        return None

    # ── 回踩周：突破周和当前周之间的所有周 ──
    pullback_indices = list(range(breakout_idx + 1, last))  # 不含突破周和当前周
    pullback_len = len(pullback_indices)

    # # 至少1周，最多4周
    if not (1 <= pullback_len <= CFG["bp2_pullback_max"]):
        return None
    if is_test:
        print(555)
    # 回踩周收盘价必须同时低于突破周和当前周收盘价
    breakout_close = closes[breakout_idx]
    current_close  = closes[last]
    for i in pullback_indices:
        if closes[i] >= breakout_close or closes[i] >= current_close:
            return None

    if is_test:
        print(666)
    # ── 回踩质量检查 ──
    breakout_amt = amounts[breakout_idx]

    if pullback_len <= 2:
        # 短回踩：成交额 < 突破周成交额，收盘价 ≥ 当时5MA
        for i in pullback_indices:
            if breakout_amt > 0 and amounts[i] >= breakout_amt:
                return None
            if i < MA_S:
                continue
            ma5_i = closes[i - MA_S + 1: i + 1].mean()
            if closes[i] < ma5_i * 0.98:
                return None
    else:
        # 长回踩（3-4周）：成交额 < 突破周成交额×1.3，收盘价 ≥ 当时20MA
        for i in pullback_indices:
            if breakout_amt > 0 and amounts[i] >= breakout_amt * 1.3:
                return None
            if i < MA_L:
                continue
            ma20_i = closes[i - MA_L: i].mean()
            if closes[i] < ma20_i * 0.98:
                return None
    if is_test:
        print(777)
    # ── 评分 ──
    is_shrink_now = (amounts[last - 1] > 0
                     and amounts[last] <= amounts[last - 1] * 0.8)
    vol_score     = 15 if is_shrink_now else 5
    green_score   = min(pre_green, 4) * 2

    pullback_low = min(closes[i] for i in pullback_indices)
    pullback_pct = (breakout_close - pullback_low) / breakout_close if breakout_close > 0 else 0

    return {
        "signal":         "买点2",
        "score":          5,
        "breakout_date":  df["date"].iloc[breakout_idx].strftime("%Y-%m-%d"),
        "breakout_price": round(breakout_close, 2),
        "pullback_low":   round(pullback_low, 2),
        "pullback_pct":   f"{pullback_pct*100:.1f}%",
        "current_price":  round(current_close, 2),
        "chg_from_low":   f"{(current_close/pullback_low-1)*100:+.1f}%",
        "vol_ratio":      round(amounts[last] / max(amounts[last - 1], 1), 2),
        "consol_weeks":   pullback_len,
        "weeks_ago":      pullback_len + 1,
    }


# ─────────────────────────────────────────────
#  全市场涨幅收集（供 RPS 和涨幅排名计算）
# ─────────────────────────────────────────────

def _daily_cache_is_fresh(path: str) -> bool:
    """
    判断日线缓存是否仍然有效：
    用文件修改时间判断——当天15:00之前写入的缓存视为最新。
    （交易日15:00收盘，收盘前数据不变；非交易日同理，
      只要今天还没收盘就不需要重拉。）
    """
    if not os.path.exists(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    today_close = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
    # 文件是今天15:00之后写入 → 已包含今日收盘数据，绝对新鲜
    if mtime >= today_close:
        return True
    # 文件是今天15:00之前写入，但今天还未到15:00 → 今天尚未收盘，仍然新鲜
    if datetime.now() < today_close and mtime.date() == datetime.now().date():
        return True
    # 今天已过15:00但文件是今天15:00前写的 → 缺今日数据，需重拉
    # 今天已过15:00但文件是昨天或更早写的 → 同上
    return False


def _fetch_daily_for_rps(code: str) -> None:
    """
    为全市场 RPS 计算补拉日线数据。
    用文件修改时间判断新鲜度，避免因节假日导致数据日期落后而误判为过期。
    """
    path = _cache_path_daily(code)
    need_rows = CFG["rps_days"] + 15   # 留节假日余量

    if _daily_cache_is_fresh(path):
        # 还需确认行数足够（首次拉取可能 n 较小）
        try:
            with open(path, "rb") as f:
                cached = pickle.load(f)
            if cached is not None and len(cached) >= need_rows:
                return   # 新鲜且够用，跳过
        except Exception:
            pass

    # ── 需要拉取 ──
    fetch_daily_recent(code, n=need_rows)   # 内部已处理缓存写入


def collect_market_gains(rows: list):
    """
    1. 多线程为全市场补拉日线（有新鲜缓存则跳过，无缓存则拉取）
    2. 读取所有日线缓存，计算 gain120 / gain10，存入全局 _market_gains
    """
    global _market_gains
    days120 = CFG["rps_days"]       # 120
    days10  = CFG["gain_rank_days"] # 10

    # ── Step1：并行补拉日线（主扫描用的8线程，这里共享同一 baostock 锁）──
    print("补拉全市场日线数据（RPS计算用）...")
    with ThreadPoolExecutor(max_workers=CFG["max_workers"]) as executor:
        futs = {executor.submit(_fetch_daily_for_rps, code): code for code, _ in rows}
        with tqdm(total=len(rows), desc="日线补拉", unit="只") as pbar:
            for fut in as_completed(futs):
                pbar.update(1)
                try:
                    fut.result(timeout=30)
                except Exception:
                    pass

    # ── Step2：读取缓存，计算涨幅 ──
    gains = {}
    for code, _ in rows:
        path = _cache_path_daily(code)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "rb") as f:
                daily = pickle.load(f)
            if daily is None or len(daily) < days10 + 1:
                continue
            closes = daily["close"].values
            # ── gain120 ──
            if len(closes) >= days120 + 1:
                g120 = (closes[-1] - closes[-(days120 + 1)]) / closes[-(days120 + 1)]
            else:
                # 数据不足 120 日则用全部可用数据
                g120 = (closes[-1] - closes[0]) / closes[0] if closes[0] > 0 else float("nan")
            # ── gain10 ──
            g10 = (closes[-1] - closes[-(days10 + 1)]) / closes[-(days10 + 1)]
            gains[code] = {"gain120": g120, "gain10": g10}
        except Exception:
            continue

    _market_gains = gains


def _calc_rps_and_rank(code: str) -> tuple:
    """
    根据全市场数据计算：
      rps120    : 该股 120 日涨幅的全市场百分位（0~100，保留1位小数），越高越强
      gain10_rank: 该股 10 日涨幅在全市场的名次（整数，第1名最强）
    返回 (rps120, gain10_rank)，数据不足时返回 (nan, nan)。
    """
    if not _market_gains or code not in _market_gains:
        return float("nan"), float("nan")

    all_g120 = [v["gain120"] for v in _market_gains.values()
                if not np.isnan(v["gain120"])]
    all_g10  = [v["gain10"]  for v in _market_gains.values()
                if not np.isnan(v["gain10"])]

    target_g120 = _market_gains[code]["gain120"]
    target_g10  = _market_gains[code]["gain10"]

    if np.isnan(target_g120) or not all_g120:
        rps120 = float("nan")
    else:
        # 百分位：比该股涨幅低的股票占比（×100）
        rps120 = round(sum(1 for g in all_g120 if g < target_g120) / len(all_g120) * 100, 1)

    if np.isnan(target_g10) or not all_g10:
        gain10_rank = float("nan")
    else:
        # 排名：比该股涨幅高的股票数 + 1（第1名最强）
        gain10_rank = sum(1 for g in all_g10 if g > target_g10) + 1

    return rps120, gain10_rank


# ─────────────────────────────────────────────
#  单只股票处理
# ─────────────────────────────────────────────

def process_one(row: tuple) -> Optional[dict]:
    code, name = row
    df = fetch_weekly(code)
    if df is None or len(df) < CFG["min_weeks_data"]:
        return None
    if df["close"].iloc[-1] < CFG["min_price"]:
        return None
    # 换手率过滤：取最近4周均值，过低说明无人关注
    min_turn = CFG.get("min_turnover", 0)
    if min_turn > 0 and "turnover" in df.columns:
        recent_turn = df["turnover"].iloc[-4:].mean()
        if pd.isna(recent_turn) or recent_turn < min_turn:
            return None

    # 总市值过滤：用换手率估算流通市值作为代理（流通市值 ≤ 总市值，安全下界）
    # 公式：流通股本 = 成交量(股) / 换手率%，流通市值 = 流通股本 × 收盘价
    min_cap = CFG.get("min_market_cap", 0)
    if min_cap > 0 and "turnover" in df.columns:
        recent = df[(df["turnover"] > 0) & (df["volume"] > 0)].iloc[-4:]
        if not recent.empty:
            avg_vol   = recent["volume"].mean()
            avg_turn  = recent["turnover"].mean()
            cur_close = df["close"].iloc[-1]
            float_cap = avg_vol / (avg_turn / 100) * cur_close / 1e8   # 亿元
            if float_cap < min_cap:
                return None

    # ── 硬过滤（一票否决，先于信号检测）──
    closes  = df["close"].values
    opens   = df["open"].values
    highs   = df["high"].values
    lows    = df["low"].values
    amounts = df["amount"].values
    last    = len(df) - 1

    # 1. 本周涨幅 ≥ 30%（过热）
    if last >= 1 and closes[last - 1] > 0:
        if (closes[last] - closes[last - 1]) / closes[last - 1] >= CFG["hard_gain_max"]:
            return None

    # 2. 放量 + 上影 ≥ 实体（冲高回落）
    body     = abs(closes[last] - opens[last])
    upper_sh = highs[last] - max(closes[last], opens[last])
    if (last >= 1 and amounts[last - 1] > 0
            and amounts[last] > amounts[last - 1]
            and body > 0 and upper_sh >= body):
        return None

    # 3. 连红 ≥ 7 周
    if _count_consec_green(closes, opens, last) >= CFG["hard_consec_green_max"]:
        return None

    # 4. 历史须有单周涨幅 ≥ 20%（辨识度验证：没有过暴涨记录的票弹性不够）
    pct_changes = np.where(closes[:-1] > 0,
                           (closes[1:] - closes[:-1]) / closes[:-1], 0)
    if pct_changes.max() < CFG["hard_hist_spike_min"]:
        return None

    # 先用周线判断买点1（不带日线）
    result = check_buy_point_1(df)
    daily = None
    if result is not None:
        # 只对命中买点1的股票拉日线做力度周确认
        daily = fetch_daily_recent(code, n=max(CFG["rps_days"] + 10, 30))
        if daily is not None:
            fail_reasons = _check_daily_w1(daily, result["breakout_date"], result["gain_w1"])
            daily_ok = len(fail_reasons) == 0
            result["daily_ok"] = "是" if daily_ok else "/".join(fail_reasons)
            if not daily_ok:
                with _daily_fail_lock:
                    for r in fail_reasons:
                        _daily_fail_reasons[r] = _daily_fail_reasons.get(r, 0) + 1

    if result is None:
        result = check_buy_point_2(df)

    if result is None:
        return None
    if result.get("score", 0) < CFG["min_score"]:
        return None

    result["code"]    = code
    result["name"]    = name
    # ATR 复用已拉取的日线，否则补拉（买点2）
    if daily is None:
        daily = fetch_daily_recent(code, n=max(CFG["rps_days"] + 10, 30))
    result["atr_pct"] = _calc_atr(daily, 14) if daily is not None else float("nan")
    result["atr_limit"] = round(result["atr_pct"] * 1.2, 2)

    # ── RPS120 和 10日涨幅排名在 main() 中统一计算，这里预置占位 ──
    result["rps120"]     = float("nan")
    result["gain10_rank"] = float("nan")

    return result


# ─────────────────────────────────────────────
#  输出
# ─────────────────────────────────────────────

def print_results(results: list):
    if not results:
        print("\n未找到符合条件的股票。")
        return

    bp1 = sorted([r for r in results if r["signal"].startswith("买点1")], key=lambda x: -x["score"])
    bp2 = sorted([r for r in results if r["signal"].startswith("买点2")], key=lambda x: -x["score"])

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*72}")
    print(f"  A股选股结果  {now}  买点1:{len(bp1)}只  买点2:{len(bp2)}只")
    print(f"{'='*72}")

    if bp1:
        print(f"\n{'─'*72}")
        print(f"  ★ 买点1：放量突破确认型  共 {len(bp1)} 只  （W2缩量最优→W2温和→W3）")
        print(f"{'─'*72}")
        for r in bp1:
            daily_val  = r.get("daily_ok", "是")
            daily_pass = (daily_val == "是")
            daily_flag = "" if daily_pass else f"  ⚠ 日线未达标({daily_val})"
            rps_str    = f"{r['rps120']:.1f}" if not np.isnan(r.get("rps120", float("nan"))) else "--"
            rank_str   = str(int(r["gain10_rank"])) if not np.isnan(r.get("gain10_rank", float("nan"))) else "--"
            print(f"\n  【{r['code']}  {r['name']}】  {r['signal']}  评分:{r['score']}{daily_flag}")
            print(f"    力度周: {r['breakout_date']}  突破价:{r['breakout_price']}  "
                  f"当前:{r['current_price']}({r['chg_since']})")
            print(f"    W1倍量比:{r['vol_ratio']}x  W2量能比:{r.get('w2_vol_ratio','--')}x  "
                  f"距今:{r['weeks_ago']}周  ATR:{r.get('atr_pct','--')}%")
            print(f"    RPS120:{rps_str}  10日涨幅排名:第{rank_str}名")

    if bp2:
        print(f"\n{'─'*72}")
        print(f"  ☆ 买点2：回踩反包型  共 {len(bp2)} 只")
        print(f"{'─'*72}")
        for r in bp2:
            rps_str  = f"{r['rps120']:.1f}" if not np.isnan(r.get("rps120", float("nan"))) else "--"
            rank_str = str(int(r["gain10_rank"])) if not np.isnan(r.get("gain10_rank", float("nan"))) else "--"
            print(f"\n  【{r['code']}  {r['name']}】  {r['signal']}  评分:{r['score']}")
            print(f"    前高: {r['breakout_price']}  回踩底({r['breakout_date']}): {r['pullback_low']}"
                  f"  回踩幅:{r['pullback_pct']}")
            print(f"    当前价:{r['current_price']}  距低点:{r['chg_from_low']}  "
                  f"反包量比:{r['vol_ratio']}x  回踩:{r['consol_weeks']}周  ATR:{r.get('atr_pct','--')}%")
            print(f"    RPS120:{rps_str}  10日涨幅排名:第{rank_str}名")

    print(f"\n{'='*72}")


_CN_COLUMNS = {
    "code":           "股票代码",
    "name":           "股票名称",
    "signal":         "信号类型",
    "score":          "评分",
    "breakout_date":  "突破/底部日期",
    "breakout_price": "突破价/前高",
    "current_price":  "当前价",
    "chg_since":      "突破后涨幅",
    "vol_ratio":      "成交额倍量比",
    "w2_vol_ratio":   "W2量能比(vs W1)",
    "weeks_ago":      "距今(周)",
    "pullback_low":   "回踩低点",
    "pullback_pct":   "回踩幅度",
    "chg_from_low":   "距低点涨幅",
    "consol_weeks":   "回踩周数",
    "atr_pct":        "ATR%",
    "atr_limit":       "网格步长",
    "daily_ok":       "日线确认",
    "rps120":         "RPS120(百分位)",
    "gain10_rank":    "10日涨幅排名",
}


def save_results(results: list, total_stocks: int):
    if not results:
        return
    df = pd.DataFrame(results)

    # 格式化 gain10_rank（纯整数，nan→空）
    if "gain10_rank" in df.columns:
        df["gain10_rank"] = df["gain10_rank"].apply(
            lambda v: int(v) if not (isinstance(v, float) and np.isnan(v)) else ""
        )

    if "rps120" in df.columns:
        df["rps120"] = df["rps120"].apply(
            lambda v: f"{v:.1f}" if not (isinstance(v, float) and np.isnan(v)) else "--"
        )

    df = df.rename(columns=_CN_COLUMNS)
    fname = f"result_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False)
    print(f"结果已保存: {fname}")


# ─────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────

def main():
    print("获取A股列表...")
    stock_list = get_stock_list()
    rows = list(zip(stock_list["code"], stock_list["name"]))
    total = len(rows)

    # 统计缓存命中情况（帮助用户了解增量效果）
    cache_dir = CFG["cache_dir"]
    cached_count = 0
    fresh_count  = 0
    first_cache_date = None
    if os.path.exists(cache_dir):
        for code, _ in rows:
            p = _cache_path(code)
            if os.path.exists(p):
                cached_count += 1
                try:
                    with open(p, "rb") as f:
                        df = pickle.load(f)
                    if first_cache_date is None:
                        first_cache_date = df["date"].iloc[-1].date()
                    if _weekly_is_fresh(df):
                        fresh_count += 1
                except Exception:
                    pass
    need_update = cached_count - fresh_count
    no_cache    = total - cached_count
    if first_cache_date is not None:
        print(f"缓存数据日期: {first_cache_date}")
    print(f"共 {total} 只股票  |  缓存已是最新:{fresh_count}  需增量更新:{need_update}  无缓存(首次):{no_cache}")
    print(f"开始扫描...\n")

    results = []
    errors  = 0

    with ThreadPoolExecutor(max_workers=CFG["max_workers"]) as executor:
        futures = {executor.submit(process_one, r): r for r in rows}
        with tqdm(total=total, desc="扫描", unit="只",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, 命中:{postfix}]") as pbar:
            for future in as_completed(futures):
                pbar.update(1)
                try:
                    res = future.result(timeout=30)
                    if res:
                        results.append(res)
                        pbar.set_postfix_str(str(len(results)))
                except Exception:
                    errors += 1

    print(f"\n扫描完成：命中 {len(results)} 只，失败/跳过 {errors} 只")
    if _daily_fail_reasons:
        total_fail = sum(_daily_fail_reasons.values())
        print(f"日线未通过 {total_fail} 个，原因汇总：{_daily_fail_reasons}")
    if _stop_scan.is_set() and results:
        print(f"⚠️  注意：接口无最新数据（数据截止 {_last_known_date}），以下结果基于历史缓存，请勿直接操作！")

    # ── 计算全市场涨幅数据：先补拉全市场日线，再计算 RPS / 排名 ──
    collect_market_gains(rows)
    mkt_total = len(_market_gains)
    print(f"全市场参与计算股票数: {mkt_total} 只")
    if results:
        for r in results:
            rps120, gain10_rank = _calc_rps_and_rank(r["code"])
            r["rps120"]      = rps120
            r["gain10_rank"] = gain10_rank

    print_results(results)
    save_results(results, total)


# ─────────────────────────────────────────────
#  回测工具
# ─────────────────────────────────────────────

def backtest_single(code: str, as_of_date: str, action: int = 2) -> Optional[dict]:
    """
    回测：以 as_of_date 所在周作为"当前周"，判断该股票是否满足买点1或买点2。
    code: baostock格式，如 'sz.300657'
    as_of_date: 'YYYY-MM-DD'，会自动对齐到所在周的周五
    action: 1=买点1, 2=买点2
    """
    df = fetch_weekly(code)
    if df is None or df.empty:
        print(f"[{code}] 无数据")
        return None

    target_dt = pd.Timestamp(as_of_date)
    mask = df["date"] <= target_dt
    if not mask.any():
        print(f"[{code}] {as_of_date} 早于数据起始日期 {df['date'].iloc[0].date()}")
        return None

    last = int(mask[::-1].idxmax())
    actual_week_date = df["date"].iloc[last].strftime("%Y-%m-%d")
    print(f"[{code}] 回测日期={as_of_date}，对齐到周线={actual_week_date}，索引={last}，共{len(df)}周数据")

    df_slice = df.iloc[: last + 1].reset_index(drop=True)

    if action == 1:
        result = check_buy_point_1(df_slice)
        if result:
            print(f"[{code}] ✅ 买点1命中  信号={result['signal']}  "
                  f"突破日={result['breakout_date']}  突破价={result['breakout_price']}  "
                  f"W1倍量={result['vol_ratio']}x  评分={result['score']}")
        else:
            print(f"[{code}] ❌ 买点1未命中")
    elif action == 2:
        result = check_buy_point_2(df_slice)
        if result:
            print(f"[{code}] ✅ 买点2命中  突破周={result['breakout_date']}  "
                  f"突破价={result['breakout_price']}  回踩低={result['pullback_low']}  "
                  f"回踩幅={result['pullback_pct']}  回踩周数={result['consol_weeks']}")
        else:
            print(f"[{code}] ❌ 买点2未命中")
    else:
        print(f"action 参数错误，只支持 1 或 2")
        return None

    return result


def backtest_batch(tests: list, action: int = 2):
    """
    批量回测
    tests: [(code, date), ...]  如 [("sz.300657", "2024-03-15"), ...]
    action: 1=买点1, 2=买点2
    """
    print(f"\n{'='*60}")
    print(f"  买点{action} 批量回测  共 {len(tests)} 条")
    print(f"{'='*60}")
    hit, miss = 0, 0
    for code, date in tests:
        print(f"\n─ {code}  {date} ─")
        result = backtest_single(code, date, action=action)
        if result:
            hit += 1
        else:
            miss += 1
    print(f"\n{'='*60}")
    print(f"  命中 {hit} 条  未命中 {miss} 条  命中率 {hit/(hit+miss)*100:.1f}%")
    print(f"{'='*60}")


if __name__ == "__main__":
    # 周五5点半后出当前周数据
    main()

    # 单只回测-是否满足买点2
    # backtest_single("sh.688629", "2026-02-16")
    # backtest_single("sz.002001", "2026-02-28")

    # 批量回测
    # backtest_batch([
    #     ("sz.300657", "2024-03-15"),
    #     ("sh.600519", "2023-11-10"),
    #     ("sz.002436", "2024-01-05"),
    # ])
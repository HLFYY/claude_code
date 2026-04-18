#!/usr/bin/env python3
"""
A股选股程序：周线倍量突破逻辑
===============================
买点1：近期出现倍量突破N周新高 + 随后缩量确认（最强信号，大资金进场+筹码锁定）
买点2：历史有过倍量突破 → 经过回踩洗盘 → 当前重新启动（筹码更干净）

数据源：baostock（免费，无需购买）
缓存：./cache/ 目录，半天内不重复下载
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
    "start_date":             "2023-01-01",
    "cache_dir":              "./cache",
    # 周线新鲜度：以"下一个周五 15:00 收盘"为界，收盘前不重拉（见 _weekly_is_fresh）
    "max_workers":            8,            # 线程数（网络请求已串行，此值影响分析并行度）
    "request_delay":          0.05,        # 串行请求间隔（秒）

    # ── 倍量突破 ──
    "vol_avg_weeks":          10,           # 均量窗口（周）
    "vol_ratio":              2.5,          # 倍量阈值（突破周量 ≥ 均量 × X）
    "new_high_weeks":         52,           # N周新高（78=1.5年）

    # ── 买点1：缩量确认 ──
    "confirm_weeks":          1,            # 突破后需缩量的周数
    "shrink_ratio":           0.7,          # 后续每周量 ≤ 突破周量 × X
    "bp1_entry_buffer":       2,            # 确认完成后还允许等几周入场（bp1_max_age = confirm_weeks + buffer）
    "bp1_confirmed_only":     False,         # True=只要已完成缩量确认；False=待确认也收录

    # ── 买点2：回踩重启 ──
    "bp2_breakout_min_age":   6,
    "bp2_breakout_max_age":   52,
    "pullback_min":           0.15,         # 回踩最小幅度（从突破后高点下跌≥X%）
    "pullback_max":           0.40,         # 回踩最大幅度
    "consolidation_min_weeks": 4,           # 盘整至少N周（原4，调大=洗盘更充分）
    "restart_vol_ratio":      2.0,          # 近4周量 ≥ 盘整均量 × X
    "bp2_max_recovery_ratio": 0.75,         # 当前价在[低点→回踩前高点]区间的恢复比例上限（0.618=黄金分割，0.75=四分之三）

    # ── 通用过滤 ──
    "min_price":              3.0,          # 最低股价（原3，调高过滤低价垃圾股）
    "min_turnover":           0.5,          # 最低周换手率%（过滤流动性差的冷门股）
    "min_market_cap":         80,          # 最低总市值（亿元，用流通市值估算；0=不过滤）
    "min_weeks_data":         78,           # 最少数据周数（原60，调高确保有足够历史）
    "exclude_st":             True,
    "min_score":              5,
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
    return df.dropna(subset=["close", "volume"]).reset_index(drop=True)


def _bs_fetch(code: str, start_date: str) -> Optional[pd.DataFrame]:
    """调用 baostock 接口（必须在 _bs_lock 内调用）"""
    try:
        _ensure_login()
        time.sleep(CFG["request_delay"])
        rs = _bs_global.query_history_k_data_plus(
            code,
            "date,open,high,low,close,volume,turn",
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
    缓存 1 天内视为最新（日线当天只需拉一次）。
    """
    path = _cache_path_daily(code)

    if os.path.exists(path):
        with open(path, "rb") as f:
            cached = pickle.load(f)
        if cached is not None and not cached.empty:
            if (datetime.now() - cached["date"].iloc[-1]).days <= 1:
                return cached

    start = (datetime.now() - timedelta(days=n * 2)).strftime("%Y-%m-%d")  # 留足节假日余量
    with _bs_lock:
        # 双重检查
        if os.path.exists(path):
            with open(path, "rb") as f:
                cached = pickle.load(f)
            if cached is not None and not cached.empty:
                if (datetime.now() - cached["date"].iloc[-1]).days <= 1:
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
#  核心算法：找所有倍量突破点
# ─────────────────────────────────────────────

def _find_volume_breakouts(df: pd.DataFrame) -> list:
    N_HIGH  = CFG["new_high_weeks"]
    N_VOL   = CFG["vol_avg_weeks"]
    RATIO   = CFG["vol_ratio"]
    min_idx = N_HIGH + N_VOL

    closes  = df["close"].values
    volumes = df["volume"].values
    breakouts = []

    for i in range(min_idx, len(df)):
        prior_high = closes[i - N_HIGH: i].max()
        if closes[i] <= prior_high:
            continue

        avg_vol = volumes[i - N_VOL: i].mean()
        if avg_vol == 0:
            continue
        vol_r = volumes[i] / avg_vol
        if vol_r < RATIO:
            continue

        breakouts.append({
            "idx":       i,
            "date":      df["date"].iloc[i],
            "close":     closes[i],
            "prior_high": prior_high,
            "vol":       volumes[i],
            "avg_vol":   avg_vol,
            "vol_ratio": round(vol_r, 2),
        })

    breakouts.reverse()   # 最新在前
    return breakouts


# ─────────────────────────────────────────────
#  买点1
# ─────────────────────────────────────────────

def check_buy_point_1(df: pd.DataFrame) -> Optional[dict]:
    """倍量突破新高 + 缩量确认"""
    CONFIRM = CFG["confirm_weeks"]
    SHRINK  = CFG["shrink_ratio"]
    MAX_AGE = CONFIRM + CFG.get("bp1_entry_buffer", 1)   # 确认需 CONFIRM 周，之后再给 buffer 周入场
    last_idx = len(df) - 1

    for bp in _find_volume_breakouts(df):
        age = last_idx - bp["idx"]
        if age > MAX_AGE:
            break

        # 价格须站稳（允许 -5%）
        cur = df["close"].iloc[-1]
        if cur < bp["close"] * 0.95:
            continue

        # 缩量确认
        after = df["volume"].values[bp["idx"]+1 : bp["idx"]+1+CONFIRM]
        if len(after) < CONFIRM:
            confirm_ok   = False
            shrink_score = None
        else:
            confirm_ok   = all(v <= bp["vol"] * SHRINK for v in after)
            shrink_score = round(float(after.mean()) / bp["vol"], 2)

        # 只要已确认模式：待确认直接跳过
        if CFG.get("bp1_confirmed_only", False) and not confirm_ok:
            continue

        # 评分：倍量比越大越好，越新越好，已确认加分
        freshness = max(0, MAX_AGE - age) / MAX_AGE   # 0~1，越新越高
        score = round(bp["vol_ratio"] * 8 + freshness * 20 + (10 if confirm_ok else 0), 1)
        return {
            "signal":         "买点1" + ("✓" if confirm_ok else "（待确认）"),
            "score":          score,
            "breakout_date":  bp["date"].strftime("%Y-%m-%d"),
            "breakout_price": round(bp["close"], 2),
            "current_price":  round(cur, 2),
            "chg_since":      f"{(cur/bp['close']-1)*100:+.1f}%",
            "vol_ratio":      bp["vol_ratio"],
            "shrink_score":   shrink_score,
            "weeks_ago":      age,
            "confirm_weeks":  len(after),
        }

    return None


# ─────────────────────────────────────────────
#  买点2
# ─────────────────────────────────────────────

def check_buy_point_2(df: pd.DataFrame) -> Optional[dict]:
    """历史倍量突破 → 回踩洗盘 → 重新启动"""
    MIN_AGE    = CFG["bp2_breakout_min_age"]
    MAX_AGE    = CFG["bp2_breakout_max_age"]
    PB_MIN     = CFG["pullback_min"]
    PB_MAX     = CFG["pullback_max"]
    CONSOL_MIN = CFG["consolidation_min_weeks"]
    RESTART_R  = CFG["restart_vol_ratio"]
    last_idx   = len(df) - 1

    closes  = df["close"].values
    volumes = df["volume"].values

    for bp in _find_volume_breakouts(df):
        age = last_idx - bp["idx"]
        if age < MIN_AGE:
            continue
        if age > MAX_AGE:
            break

        after = df.iloc[bp["idx"]+1:]
        if len(after) < CONSOL_MIN:
            continue

        # 突破后最低点（回踩低点）
        low_rel  = after["low"].values.argmin()
        low_px   = after["low"].values[low_rel]
        low_abs  = bp["idx"] + 1 + low_rel

        # 基于突破后最高点计算回踩幅度
        high_after = after["high"].values[:low_rel+1].max()
        pullback   = (high_after - low_px) / high_after
        if not (PB_MIN <= pullback <= PB_MAX):
            continue

        # 盘整期（低点到现在）
        consol_len = last_idx - low_abs
        if consol_len < CONSOL_MIN:
            continue

        # 重启信号：近4周量能 vs 盘整期均量
        recent_n   = min(4, consol_len)
        rv         = volumes[last_idx - recent_n + 1: last_idx + 1]
        cv         = volumes[low_abs: last_idx - recent_n + 1]
        if len(cv) == 0:
            continue
        vol_r = round(float(rv.mean()) / float(cv.mean()), 2)
        if vol_r < RESTART_R:
            continue

        # 价格须高于盘整中枢
        cur = closes[last_idx]
        mid = low_px * (1 + pullback * 0.5)
        if cur < mid:
            continue

        # 恢复比例不能太高（已经跑完的不要）
        # recovery_ratio = 当前价在 [低点, 回踩前高点] 区间的位置，0=低点，1=高点
        chg_from_low  = (cur - low_px) / low_px
        recovery      = (cur - low_px) / (high_after - low_px) if high_after > low_px else 1.0
        if recovery > CFG.get("bp2_max_recovery_ratio", 0.75):
            continue

        # 评分：重启量比 + 原始倍量 + 回踩幅度适中（太小太大都减分）
        pb_score  = 20 - abs(pullback - 0.25) * 40   # 回踩25%附近最优
        age_decay = max(0, 1 - (age - MIN_AGE) / (MAX_AGE - MIN_AGE))
        score     = round(vol_r * 10 + bp["vol_ratio"] * 5 + pb_score + age_decay * 10, 1)
        return {
            "signal":         "买点2",
            "score":          score,
            "breakout_date":  bp["date"].strftime("%Y-%m-%d"),
            "breakout_price": round(bp["close"], 2),
            "pullback_low":   round(low_px, 2),
            "pullback_pct":   f"{pullback*100:.1f}%",
            "current_price":  round(cur, 2),
            "chg_from_low":   f"{(cur/low_px-1)*100:+.1f}%",
            "vol_ratio_bp":   bp["vol_ratio"],
            "vol_restart_r":  vol_r,
            "consol_weeks":   consol_len,
            "weeks_ago":      age,
        }

    return None


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

    result = check_buy_point_1(df) or check_buy_point_2(df)
    if result is None:
        return None
    if result.get("score", 0) < CFG["min_score"]:
        return None

    # 突破时间过滤：仅保留最近半年（约26周）内的突破
    if result.get("weeks_ago", 999) > 26:
        return None

    result["code"]    = code
    result["name"]    = name
    daily = fetch_daily_recent(code, n=30)
    result["atr_pct"] = _calc_atr(daily, 14) if daily is not None else float("nan")
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
        print(f"  ★ 买点1：倍量突破新高 + 缩量确认  共 {len(bp1)} 只")
        print(f"{'─'*72}")
        for r in bp1:
            print(f"\n  【{r['code']}  {r['name']}】  {r['signal']}  评分:{r['score']}")
            print(f"    突破: {r['breakout_date']}  突破价:{r['breakout_price']}  "
                  f"当前:{r['current_price']}({r['chg_since']})")
            print(f"    倍量比:{r['vol_ratio']}x  "
                  f"缩量比:{r.get('shrink_score','--')}  "
                  f"距今:{r['weeks_ago']}周  已确认:{r['confirm_weeks']}周")

    if bp2:
        print(f"\n{'─'*72}")
        print(f"  ☆ 买点2：历史突破 → 回踩洗盘 → 重新启动  共 {len(bp2)} 只")
        print(f"{'─'*72}")
        for r in bp2:
            print(f"\n  【{r['code']}  {r['name']}】  {r['signal']}  评分:{r['score']}")
            print(f"    原始突破: {r['breakout_date']}  突破价:{r['breakout_price']}")
            print(f"    回踩低点: {r['pullback_low']}  回踩幅:{r['pullback_pct']}")
            print(f"    当前价:   {r['current_price']}  (距低点{r['chg_from_low']})")
            print(f"    启动量比: {r['vol_restart_r']}x  盘整:{r['consol_weeks']}周  原倍量:{r['vol_ratio_bp']}x")

    print(f"\n{'='*72}")


_CN_COLUMNS = {
    "code":           "股票代码",
    "name":           "股票名称",
    "signal":         "信号类型",
    "score":          "评分",
    "breakout_date":  "突破日期",
    "breakout_price": "突破价",
    "current_price":  "当前价",
    "chg_since":      "突破后涨幅",
    "vol_ratio":      "倍量比",
    "shrink_score":   "缩量比",
    "weeks_ago":      "突破距今(周)",
    "confirm_weeks":  "缩量确认(周)",
    "pullback_low":   "回踩低点",
    "pullback_pct":   "回踩幅度",
    "chg_from_low":   "距低点涨幅",
    "vol_ratio_bp":   "原始倍量比",
    "vol_restart_r":  "启动量比",
    "consol_weeks":   "盘整周数",
    "atr_pct":        "ATR%",
}


def save_results(results: list):
    if not results:
        return
    df = pd.DataFrame(results)
    df = df.rename(columns=_CN_COLUMNS)
    fname = f"result_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        f.write(f"数据更新时间,{_last_known_date}\n")
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
    if _stop_scan.is_set() and results:
        print(f"⚠️  注意：接口无最新数据（数据截止 {_last_known_date}），以下结果基于历史缓存，请勿直接操作！")
    print_results(results)
    save_results(results)


if __name__ == "__main__":
    main()
    # daily = fetch_daily_recent('sz.300497', n=30)
    # atr_pct = _calc_atr(daily, 14) if daily is not None else float("nan")
    # print(atr_pct)
    # print(atr_pct*1.2)
import json
import os
import pickle
import socket
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from stock_screener import fetch_daily_recent, _calc_atr, WEEKLY_SIGNALS_FILE, WEEKLY_POOL_SIGNALS


def _log(msg: str):
    """流程日志：前面带时间（服务器定时任务排查用）"""
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_weekly_kdata(code: str):
    headers = {"Referer": "https://finance.sina.com.cn"}

    # 1. 实时数据
    url_rt = f"https://hq.sinajs.cn/list={code}"
    resp = requests.get(url_rt, headers=headers, timeout=5)
    resp.encoding = "gbk"
    fields = resp.text.split('"')[1].split(",")

    today_open  = float(fields[1])
    last_close  = float(fields[2])
    current_p   = float(fields[3])
    today_high  = float(fields[4])
    today_low   = float(fields[5])
    today_vol   = float(fields[8])   # ✅ 成交量（手）
    today_amt   = float(fields[9])   # ✅ 成交额（元）
    today_date  = fields[30]

    # print(f"=== 实时 ===")
    # print(f"  当前价={current_p}, 今日成交量={today_vol}手, 今日成交额={today_amt/1e8:.4f}亿")

    # 2. 获取近40日日线
    url_hist = (
        f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
        f"/CN_MarketData.getKLineData?symbol={code}&scale=240&ma=no&datalen=40"
    )
    resp2 = requests.get(url_hist, headers=headers, timeout=5)
    daily = resp2.json()

    # print(f"\n=== 日线原始字段示例（最新一条）===")
    # print(daily[-1])

    today      = datetime.strptime(today_date, "%Y-%m-%d")
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday - timedelta(days=1)

    # 3. 按周分组
    week_map = {}
    for bar in daily:
        d = datetime.strptime(bar["day"], "%Y-%m-%d")
        if d >= today:  # ✅ 排除今日及以后
            continue
        wk_key = (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")
        week_map.setdefault(wk_key, []).append(bar)

    def bar_amount(bar):
        # 新浪日线 volume 单位是【股】，成交额 = 成交量(股) * 均价 ≈ volume * close
        return float(bar["volume"]) * float(bar["close"])

    def week_summary(bars):
        return {
            "open":   float(bars[0]["open"]),
            "close":  float(bars[-1]["close"]),
            "high":   max(float(b["high"]) for b in bars),
            "low":    min(float(b["low"])  for b in bars),
            "amount": sum(bar_amount(b) for b in bars),
        }

    # 4. 本周数据
    this_wk_key = this_monday.strftime("%Y-%m-%d")
    hist_bars   = week_map.get(this_wk_key, [])

    if hist_bars:
        week_open = float(hist_bars[0]["open"])
        week_high = max([float(b["high"]) for b in hist_bars] + [today_high])
        week_low  = min([float(b["low"])  for b in hist_bars] + [today_low])
        # ✅ 历史日线成交额（股*收盘估算）+ 今日实时成交额（直接用fields[9]）
        hist_amt  = sum(bar_amount(b) for b in hist_bars)
    else:
        week_open = today_open
        week_high = today_high
        week_low  = today_low
        hist_amt  = 0

    week_amt = hist_amt + today_amt  # ✅ 今日直接用实时成交额字段

    this_week = {
        "open":   week_open,
        "close":  current_p,
        "high":   week_high,
        "low":    week_low,
        "amount": week_amt,
    }

    # print(f"\n=== 本周成交额构成 ===")
    # print(f"  历史日线累计（估算）= {hist_amt/1e8:.4f}亿")
    # print(f"  今日实时成交额       = {today_amt/1e8:.4f}亿")
    # print(f"  本周合计             = {week_amt/1e8:.4f}亿")

    # 5. 上周数据
    last_wk_key = last_monday.strftime("%Y-%m-%d")
    last_bars   = week_map.get(last_wk_key, [])
    if not last_bars:
        last_bars = [
            b for b in daily
            if last_monday <= datetime.strptime(b["day"], "%Y-%m-%d") <= last_sunday
        ]
    last_week = week_summary(last_bars) if last_bars else {
        "open": 0, "close": last_close, "high": 0, "low": 0, "amount": 0
    }

    # print(f"\n=== 上周成交额 ===")
    # print(f"  上周合计（估算）= {last_week['amount']/1e8:.4f}亿")
    # print(f"  上周收盘        = {last_week['close']}")

    # 6. 实时5周线 = 当前价 + 前4周收盘 均值
    sorted_wk_keys  = sorted(week_map.keys())
    past_wk_keys    = [w for w in sorted_wk_keys if w != this_wk_key]
    recent_4_keys   = past_wk_keys[-4:]
    recent_4_prices = [float(week_map[w][-1]["close"]) for w in recent_4_keys]
    ma5_realtime    = (current_p + sum(recent_4_prices)) / 5

    prev_5_keys   = past_wk_keys[-5:]
    prev_5_prices = [float(week_map[w][-1]["close"]) for w in prev_5_keys]
    ma5_last_week = sum(prev_5_prices) / len(prev_5_prices) if prev_5_prices else ma5_realtime

    return {
        "this_week":       this_week,
        "last_week":       last_week,
        "last_week_close": last_week["close"],
        "ma5_realtime":    round(ma5_realtime, 3),
        "ma5_last_week":   round(ma5_last_week, 3),
    }

def get_reason(o, c, h, l, last_week_close, this_amt, last_amt, ma5):
    """
    :param o: 开盘价
    :param c: 收盘价
    :param h: 最高价
    :param l: 最低价
    :param last_week_close: 上周收盘价
    :param this_amt: 本周交易额
    :param last_amt: 上周交易额
    :param ma5: 实时5周线
    :return:
    """
    # print((o, c, h, l, last_week_close, this_amt, last_amt, ma5))
    is_green = c < o
    # todo 节假日导致前后两周天数不一样，做归一化处理
    # last_amt = last_amt / 3 * 4
    is_expand = this_amt > last_amt
    gain = (c - last_week_close) / last_week_close * 100
    mid = (h + l) / 2
    upper_shadow = h - c
    body = abs(c - o)
    shrink_ratio = this_amt / last_amt if last_amt else 1

    print(f"  本周：开={o}  收={c}  高={h}  低={l}")
    print(f"  上周收盘：{last_week_close}")
    print(f"  涨跌幅：{gain:+.2f}%   {'📈 放量' if is_expand else '📉 缩量'}（本周/上周={shrink_ratio:.2f}）")
    print(f"  本周成交额={this_amt / 1e8:.2f}亿   上周成交额={last_amt / 1e8:.2f}亿")
    print(f"  实时5周线={ma5}   {'收盘在5周线上方 ✅' if c > ma5 else '收盘在5周线下方 ❌'}")
    print(f"{'=' * 58}")

    reason = ""

    if is_expand:
        print("→ 【零级】放量，检查4条淘汰规则")
        if is_green:
            reason = "放量收绿"
        elif gain <= 1:
            reason = f"放量滞涨（涨幅{gain:.2f}%≤1%）"
        elif c < mid:
            reason = f"放量冲高回落（收盘{c}<中位{mid:.3f}）"
        elif upper_shadow >= body:
            reason = f"放量长上影（上影{upper_shadow:.3f}≥实体{body:.3f}）"
    else:
        print("→ 【一级】缩量，检查收绿情况")
        if is_green:
            daily = fetch_daily_recent(f'{code[:2]}.{code[2:]}', n=30)
            atr_pct = _calc_atr(daily, 14) if daily is not None else float("nan")
            print("   缩量且收绿，检查3个留仓条件")
            reasons = []
            if shrink_ratio > 0.8:
                reasons.append(f"缩量不明显（本周/上周={shrink_ratio:.2f}>0.8）")
            if c <= ma5:
                reasons.append(f"收盘{c}未在5周线{ma5}上方")
            if gain < -atr_pct:
                reasons.append(f"跌幅{gain:.2f}%超{atr_pct}%")
            reason = "；".join(reasons)
    return reason

def check_stock(code: str):
    data            = get_weekly_kdata(code)
    this_week       = data["this_week"]
    last_week       = data["last_week"]
    last_week_close = data["last_week_close"]
    ma5             = data["ma5_realtime"]

    o        = this_week["open"]
    c        = this_week["close"]
    h        = this_week["high"]
    l        = this_week["low"]
    this_amt = this_week["amount"]
    last_amt = last_week["amount"]
    print(f"\n{'=' * 58}")
    print(f"  代码：{code}   检查时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 58}")
    reason = get_reason(o, c, h, l, last_week_close, this_amt, last_amt, ma5)

    _print_result(code, reason)
    return reason == ""

def _print_result(code, reason):
    action = "保留" if not reason else "清仓"
    print(f"\n  {'✅ 保留' if action == '保留' else '❌ 清仓'}")
    print(f"  代码：{code}")
    # if action == '保留':
    daily = fetch_daily_recent(f'{code[:2]}.{code[2:]}', n=30)
    atr_pct = _calc_atr(daily, 14) if daily is not None else float("nan")
    print(f"  最新步长：{atr_pct * 1.2}")
    # print(f"  结果：{action}")
    print(f"  原因：{reason if reason else '—'}")



# ─────────────────────────────────────────────
#  100日新高 + 倍量突破 选股（新浪数据源）
# ─────────────────────────────────────────────
#  选股条件：
#   1. 日线收盘新高（近100交易日，不含当日），且连续≥2日新高（今天、昨天都新高），且近2日涨幅 < 40%（信号日 2026-11-06 前为 20%）
#   2. 日线收红（收盘价 > 开盘价 * 1.01），且剔除 收绿/涨幅≤1%/收在中位以下/上影≥实体
#   3. 突破日成交额 > 前一日往前推5天成交额均值 * 1.5
#
#  字段计算全部对齐 stock_screener.py：
#   - 14日ATR：_calc_atr()，即 ATR(14) / 当前收盘价 * 100（%）
#   - 网格步长：ATR% * 1.2
#   - RPS120：120日涨幅 (close[-1]-close[-121])/close[-121] 的全市场百分位排名
#   - 10日涨幅排名：10日涨幅 (close[-1]-close[-11])/close[-11] 的全市场名次（第1名最强）

_SINA_CACHE_DIR = Path("cache_sina")
_SINA_CACHE_DIR.mkdir(exist_ok=True)

_SINA_MAX_WORKERS = 10
#
_RPS_DAYS = 120
_GAIN_RANK_DAYS = 10
_NEW_HIGH_DAYS = 130
# 日线"近2日涨幅"上限：2026-11-08 回测由 20% 放宽到 40%（合并池年化 25.8%→28.7%，回撤 −15.6%→−17.3%，见 backtest/backtest_long_run.py）
# 按信号日切换：信号日 < MAX_GAIN_2D_FROM 仍用旧值，保证前向跟踪里已执行的历史买卖不变（11-06 信号 = 11-09 起买入用新规则）
MAX_GAIN_2D_OLD, MAX_GAIN_2D = 20, 40
MAX_GAIN_2D_FROM = "2026-11-06"


_SINA_MIN_STOCKS = 4000   # A股约5500只，少于这个数视为列表没拉全


def _sina_get_stock_list():
    """沪深A股列表（排除北交所 bj：回测 baostock 和周线选股都不含北交所，涨跌幅/门槛也不同）"""
    return [s for s in _sina_get_stock_list_all() if not s["symbol"].startswith("bj")]


def _sina_get_stock_list_all():
    """获取全部A股列表（新浪 hs_a 节点，含北交所），缓存1天；某页失败会重试，拉不全不写缓存（退回旧缓存或报错）"""
    cache_file = _SINA_CACHE_DIR / "stock_list_sina.pkl"
    old = None
    if cache_file.exists():
        with open(cache_file, "rb") as f:
            old = pickle.load(f)
        if old is None or len(old) < _SINA_MIN_STOCKS:
            old = None   # 之前缓存过不完整的列表，作废
        elif datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime) < timedelta(days=1):
            return old

    _log("正在获取股票列表...")
    base = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    num = 100
    # 先取总数算页数：新浪偶尔会对中间页返回空/不足一页，不能以"不足一页"判断结束
    try:
        total = int(json.loads(requests.get(base + "Market_Center.getHQNodeStockCount",
                                            params={"node": "hs_a"}, timeout=15).text))
    except Exception as e:
        total = 0
        _log(f"获取股票总数失败：{type(e).__name__}: {e}")
    pages = -(-total // num) if total else 80
    by_symbol = {}
    for page in range(1, pages + 1):
        expect = min(num, total - (page - 1) * num) if total else num
        params = {"page": page, "num": num, "sort": "symbol", "asc": 1,
                  "node": "hs_a", "symbol": "", "_s_r_a": "page"}
        data, err = None, ""
        for attempt in range(5):
            try:
                resp = requests.get(base + "Market_Center.getHQNodeData", params=params, timeout=15)
                if resp.status_code == 200:
                    data = json.loads(resp.text) or []
                    if len(data) >= expect:
                        break
                    err = f"只返回{len(data)}只（应{expect}只）"
                else:
                    err = f"HTTP {resp.status_code}"
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
            time.sleep(2 * (attempt + 1))
        if not data or len(data) < expect:
            _log(f"股票列表第{page}/{pages}页不完整（重试5次）：{err}")
        for s in data or []:
            by_symbol[s["symbol"]] = s
        if not total and not data:
            break
        time.sleep(1)
    all_stocks = list(by_symbol.values())

    _log(f"共获取 {len(all_stocks)} 只股票" + (f"（总数 {total}）" if total else ""))
    need = int(max(_SINA_MIN_STOCKS, total * 0.98))
    if len(all_stocks) < need:
        if old is not None:
            _log(f"⚠ 股票列表不完整（{len(all_stocks)}只），沿用上次缓存（{len(old)}只）")
            return old
        raise RuntimeError(f"股票列表获取不完整，仅{len(all_stocks)}只（至少需{need}只），未写缓存")
    with open(cache_file, "wb") as f:
        pickle.dump(all_stocks, f)
    return all_stocks


def _sina_get_kline(symbol: str, latest_date, datalen: int = 250):
    """获取日线K线（新浪财经），symbol 形如 'sh600000'/'sz000001'（新浪列表接口自带前缀），按最新交易日做缓存新鲜度判断"""
    cache_file = _SINA_CACHE_DIR / f"kline_sina_{symbol}.pkl"

    if cache_file.exists():
        with open(cache_file, "rb") as f:
            cached = pickle.load(f)
        if cached is not None and not cached.empty:
            cache_date = pd.to_datetime(cached["day"].iloc[-1]).date()
            if cache_date >= latest_date:
                return cached

    url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    params = {"symbol": symbol, "scale": 240, "ma": "no", "datalen": datalen}
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data = json.loads(resp.text)
        if not data:
            return None

        df = pd.DataFrame(data)
        df["day"] = pd.to_datetime(df["day"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["amount"] = df["close"] * df["volume"]  # 新浪volume单位为股
        df = df.sort_values("day").reset_index(drop=True)

        latest_data_date = df["day"].iloc[-1].date()
        now = datetime.now()
        should_cache = True
        if latest_data_date == now.date() and (now.hour < 15 or (now.hour == 15 and now.minute < 10)):
            should_cache = False
        if should_cache:
            with open(cache_file, "wb") as f:
                pickle.dump(df, f)
        return df
    except Exception as e:
        _log(f"[ERROR] {symbol}: {type(e).__name__}: {e}")
        return None


def _to_sina_symbol(code: str) -> str:
    """裸代码/已带前缀均可，统一转成新浪需要的 sh/sz/bj 前缀格式"""
    code = code.strip().lower()
    if code[:2] in ("sh", "sz", "bj"):
        return code
    if code.startswith("6"):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    if code.startswith(("4", "8", "9")):
        return f"bj{code}"
    return f"sz{code}"


def _sina_qfq_factors(symbol: str, refresh: bool = True):
    """
    新浪前复权因子（finance.sina.com.cn/realstock/company/{symbol}/qfq.js），返回按日期升序的 DataFrame[d, f]；
    前复权价 = 不复权价 / f，f 从 d 当天起生效到下一个 d（已用 baostock 前复权核对，误差<0.01）。
    缓存 cache_sina/qfq_{symbol}.pkl：refresh=True 时当天只拉一次；refresh=False 有缓存就用（老区间不受新除权影响）。
    拉取失败返回旧缓存或 None（None 表示不复权）
    """
    cache_file = _SINA_CACHE_DIR / f"qfq_{symbol}.pkl"
    cached = None
    if cache_file.exists():
        with open(cache_file, "rb") as f:
            cached = pickle.load(f)
        fresh = datetime.fromtimestamp(cache_file.stat().st_mtime).date() == datetime.now().date()
        if not refresh or fresh:
            return cached
    try:
        resp = requests.get(f"https://finance.sina.com.cn/realstock/company/{symbol}/qfq.js",
                            headers={"Referer": "https://finance.sina.com.cn"}, timeout=10)
        text = resp.text
        data = json.loads(text[text.index("{"):text.rindex("}") + 1])["data"]
        fac = pd.DataFrame(data)
        if fac.empty:
            fac = pd.DataFrame({"d": [pd.Timestamp("1990-01-01")], "f": [1.0]})
        else:
            fac["d"] = pd.to_datetime(fac["d"])
            fac["f"] = fac["f"].astype(float)
        fac = fac.sort_values("d").reset_index(drop=True)
    except Exception as e:
        _log(f"[复权因子] {symbol} 获取失败：{type(e).__name__}: {e}")
        return cached
    with open(cache_file, "wb") as f:
        pickle.dump(fac, f)
    return fac


def _sina_qfq_adjust(df: pd.DataFrame, fac) -> pd.DataFrame:
    """新浪不复权日K → 前复权（open/high/low/close ÷ 因子），fac 为 None 时原样返回"""
    if fac is None or fac.empty:
        return df
    f = pd.merge_asof(df[["day"]].sort_values("day"), fac.rename(columns={"d": "day"}),
                      on="day", direction="backward")["f"].fillna(fac["f"].iloc[0]).values
    out = df.sort_values("day").reset_index(drop=True).copy()
    for col in ("open", "high", "low", "close"):
        out[col] = out[col] / f
    return out


def _qfq_f_at(fac, day) -> float:
    """day 当天生效的复权因子（前复权价 = 不复权价 / f）"""
    if fac is None or fac.empty:
        return 1.0
    f = fac.loc[fac["d"] <= pd.Timestamp(day), "f"]
    return float(f.iloc[-1]) if len(f) else float(fac["f"].iloc[0])


QFQ_STALE_DAYS = 30        # 周五顺带刷新超过这么多天的复权因子缓存（兜底漏掉的除权）
QFQ_STALE_MAX = 1500       # 每次兜底刷新上限（新浪 qfq.js 实测 10 并发约 0.55 秒/只）
QFQ_MISSING_MAX = 800      # 日常运行最多补拉多少只无缓存的；首次请先跑 python stock_check_xinlang.py qfq_init
QFQ_GAP_MAX = 1500         # 有漏跑的交易日时，每次最多补刷新多少只（本地偶尔跑一次时，分几次补齐）
QFQ_CHECKED_FILE = _SINA_CACHE_DIR / "qfq_checked.txt"   # 复权因子缓存已覆盖到哪个交易日（含当天除权）


def _prev_trading_day(d):
    import chinese_calendar as cc
    d = pd.Timestamp(d).date() - timedelta(days=1)
    while d.weekday() >= 5 or not cc.is_workday(d):
        d -= timedelta(days=1)
    return d


def _sina_ex_rights_today(df_all: dict, latest_date):
    """
    当天除权除息的股票：新浪实时行情的"昨收"在除权日是除权后价格，与日K里前一日收盘（不复权）不一致即为除权。
    一次请求查 500 只，全市场几秒。返回 (除权代码集合, 是否完整检测了 latest_date)：
    盘中运行时实时行情是今天、日K最新是昨天，对不上，算没检测
    """
    codes = [k for k, d in df_all.items()
             if d is not None and len(d) >= 2 and pd.Timestamp(d["day"].iloc[-1]).date() == latest_date]
    ex, ok, n_match = set(), True, 0
    for i in range(0, len(codes), 500):
        try:
            resp = requests.get("https://hq.sinajs.cn/list=" + ",".join(codes[i:i + 500]),
                                headers={"Referer": "https://finance.sina.com.cn"}, timeout=15)
            resp.encoding = "gbk"
        except Exception as e:
            _log(f"[除权检测] 实时行情获取失败：{type(e).__name__}: {e}")
            ok = False
            continue
        for line in resp.text.splitlines():
            if not line.startswith("var hq_str_") or '"' not in line:
                continue
            code, fields = line[11:line.index("=")], line.split('"')[1].split(",")
            if len(fields) < 31 or fields[30] != str(latest_date) or code not in df_all:
                continue
            n_match += 1
            pre, prev = float(fields[2] or 0), float(df_all[code]["close"].iloc[-2])
            if pre > 0 and abs(pre - prev) > 0.0051:
                ex.add(code)
    return ex, ok and n_match > len(codes) * 0.5


def _sina_qfq_market(market, missing_max=QFQ_MISSING_MAX, stale=None):
    """
    全市场新浪日K → 前复权（与回测 baostock 前复权口径一致），返回同 _sina_load_market 的 (最新交易日, 股票列表, {symbol: 前复权df})。
    复权因子只在除权时变：
      - 当天除权的重拉；无缓存的补拉（最多 missing_max 只）；
      - 漏跑补齐：QFQ_CHECKED_FILE 记录缓存已覆盖到哪个交易日，若早于前一交易日（中间有没跑的日子，本地偶尔跑时常见），
        缓存日期早于前一交易日的都重拉（最旧优先，每次最多 QFQ_GAP_MAX 只，补齐前不前移覆盖日）；
      - stale=True（默认周五）时顺带刷新超过 QFQ_STALE_DAYS 天的缓存（最多 QFQ_STALE_MAX 只），兜底。
    最新一天因子为 1，前复权后的最新收盘 = 实际收盘
    """
    latest_date, stock_list, df_all = market
    if stale is None:
        stale = datetime.now().weekday() == 4
    ex, ex_ok = _sina_ex_rights_today(df_all, latest_date)
    codes = [k for k, d in df_all.items() if d is not None and not d.empty]
    now = time.time()

    def mtime(code):
        f = _SINA_CACHE_DIR / f"qfq_{code}.pkl"
        return f.stat().st_mtime if f.exists() else None

    mt = {c: mtime(c) for c in codes}
    missing = [c for c in codes if mt[c] is None]
    old = sorted([c for c in codes if mt[c] is not None and (now - mt[c]) / 86400 > QFQ_STALE_DAYS], key=lambda c: mt[c])
    # 漏跑补齐：缓存覆盖日 < 前一交易日 → 缓存日期早于前一交易日的都要重拉
    prev_td = _prev_trading_day(latest_date)
    try:
        checked = datetime.strptime(QFQ_CHECKED_FILE.read_text().strip(), "%Y-%m-%d").date()
    except Exception:
        checked = None
    gap = [] if checked is not None and checked >= prev_td else sorted(
        [c for c in codes if mt[c] is not None and datetime.fromtimestamp(mt[c]).date() < prev_td], key=lambda c: mt[c])
    refresh = (set(ex) | set(missing[:missing_max]) | set(gap[:QFQ_GAP_MAX])
               | (set(old[:QFQ_STALE_MAX]) if stale else set()))
    # 今天已拉过的直接用缓存（_sina_qfq_factors 当天只拉一次），日志里单独标出
    done_today = {c for c in refresh if mt.get(c) and datetime.fromtimestamp(mt[c]).date() == datetime.now().date()}
    _log(f"[前复权] 当天除权 {len(ex)} 只" + ("" if ex_ok else "（盘中/行情不全，今天未检测）")
         + f"，无缓存 {len(missing)} 只"
         + (f"（本次只补拉 {missing_max} 只，请跑 qfq_init 补全）" if len(missing) > missing_max else "")
         + (f"，上次覆盖到 {checked or '无记录'}，漏跑补刷新 {len(gap)} 只"
            + (f"（本次 {QFQ_GAP_MAX} 只，下次继续）" if len(gap) > QFQ_GAP_MAX else "") if gap else "")
         + (f"，超过{QFQ_STALE_DAYS}天的缓存 {len(old)} 只" + ("（本次刷新）" if stale else "") if old else "")
         + f"，需拉取 {len(refresh) - len(done_today)} 只"
         + (f"（另 {len(done_today)} 只今天已拉过，用缓存）" if done_today else ""))
    with ThreadPoolExecutor(max_workers=_SINA_MAX_WORKERS * 2) as ex_:
        facs = dict(zip(codes, ex_.map(lambda c: _sina_qfq_factors(c, refresh=c in refresh), codes)))
    df_adj = {k: (_sina_qfq_adjust(d, facs.get(k)) if d is not None and not d.empty else d) for k, d in df_all.items()}
    # 覆盖日前移：漏跑的都补齐了才前移；今天检测完整则到 latest_date，否则到前一交易日（今天的除权等下次检测）
    unfixed = [c for c in gap if datetime.fromtimestamp(mtime(c)).date() < prev_td]   # 拉取失败、仍是旧缓存
    if not unfixed and len(missing) <= missing_max:
        new_checked = latest_date if ex_ok else prev_td
        if checked is None or new_checked > checked:
            QFQ_CHECKED_FILE.write_text(str(new_checked))
    n_adj = sum(1 for f in facs.values() if f is not None and not (f["f"] == 1).all())
    _log(f"[前复权] 完成，有复权因子的 {n_adj} 只，无因子按不复权 {sum(1 for f in facs.values() if f is None)} 只")
    return latest_date, stock_list, df_adj


def qfq_init():
    """首次全量拉取全市场复权因子缓存（约 50 分钟），之后日常只拉当天除权的"""
    latest_date, stock_list, df_all = _sina_load_market()
    _sina_qfq_market((latest_date, stock_list, df_all), missing_max=10 ** 6, stale=True)


def calc_buy_price_sina(code: str, date, datalen: int = 250) -> float:
    """
    计算指定股票在指定日期的买入参考价 / 次日盘中清仓价：max(该日MA5 * 0.99, 该日最低价 * 0.99)，按前复权价计算
    code: 股票代码，裸代码('600000')或带前缀('sh600000')均可
    date: 目标日期，字符串('2025-07-01')或 date/datetime
    """
    symbol = _to_sina_symbol(code)
    target_date = pd.to_datetime(date).date()

    df = _sina_get_kline(symbol, target_date, datalen)
    if df is None or df.empty:
        raise ValueError(f"未获取到 {symbol} 的K线数据")

    df = _sina_qfq_adjust(df.sort_values("day").reset_index(drop=True), _sina_qfq_factors(symbol))
    idx = df.index[df["day"].dt.date == target_date]
    if len(idx) == 0:
        raise ValueError(f"{symbol} 在 {target_date} 没有K线数据（非交易日或超出数据范围）")
    i = idx[0]

    if i < 4:
        raise ValueError(f"{symbol} 在 {target_date} 之前的数据不足5天，无法计算MA5")

    ma5 = df["close"].iloc[i - 4:i + 1].mean()
    low = df["low"].iloc[i]
    print(low)
    return round(max(ma5 * 0.99, low * 0.99), 2)


def _sina_get_latest_trading_date():
    """最新已收盘的交易日（15:10前用前一天，周末回退到周五）"""
    import chinese_calendar as cc
    now = datetime.now()
    d = now.date()
    if d.weekday() >= 5:
        d -= timedelta(days=d.weekday() - 4)
    elif now.hour < 15 or (now.hour == 15 and now.minute < 10):
        d -= timedelta(days=1)
    while not cc.is_workday(d):
        d -= timedelta(days=1)
    return d


def _collect_market_gains_sina(df_all: dict) -> dict:
    """全市场涨幅数据，用于 RPS120 / 10日涨幅排名，公式与 stock_screener.py 一致"""
    gains = {}
    for code, df in df_all.items():
        if df is None or len(df) < _GAIN_RANK_DAYS + 1:
            continue
        closes = df["close"].values
        if len(closes) >= _RPS_DAYS + 1:
            g120 = (closes[-1] - closes[-(_RPS_DAYS + 1)]) / closes[-(_RPS_DAYS + 1)]
        else:
            g120 = (closes[-1] - closes[0]) / closes[0] if closes[0] > 0 else float("nan")
        g10 = (closes[-1] - closes[-(_GAIN_RANK_DAYS + 1)]) / closes[-(_GAIN_RANK_DAYS + 1)]
        gains[code] = {"gain120": g120, "gain10": g10}
    return gains


def _calc_rps_and_rank_sina(code: str, market_gains: dict) -> tuple:
    """同 stock_screener.py 的 _calc_rps_and_rank：RPS120 为百分位，10日涨幅排名为名次（第1名最强）"""
    if not market_gains or code not in market_gains:
        return float("nan"), float("nan")

    all_g120 = [v["gain120"] for v in market_gains.values() if not np.isnan(v["gain120"])]
    all_g10 = [v["gain10"] for v in market_gains.values() if not np.isnan(v["gain10"])]
    target_g120 = market_gains[code]["gain120"]
    target_g10 = market_gains[code]["gain10"]

    if np.isnan(target_g120) or not all_g120:
        rps120 = float("nan")
    else:
        rps120 = round(sum(1 for g in all_g120 if g < target_g120) / len(all_g120) * 100, 1)

    if np.isnan(target_g10) or not all_g10:
        gain10_rank = float("nan")
    else:
        gain10_rank = sum(1 for g in all_g10 if g > target_g10) + 1

    return rps120, gain10_rank


def _check_breakout_sina(code: str, name: str, df: pd.DataFrame, market_gains: dict,
                         min_consec: int = 1, max_consec=2, max_gain_2d="auto") -> dict:
    """
    检查单只股票是否满足 100日新高+收红+倍量 条件，满足则返回结果字典，否则 None
    min_consec / max_consec：连续新高天数（含当日）的下限 / 上限（None=不限），默认 1~2 即"首日突破或突破第二日"
    max_gain_2d：近2日涨幅上限%（>= 剔除），None=不限（回测按阈值对比用）；
                "auto"=实盘：信号日 >= MAX_GAIN_2D_FROM 用 MAX_GAIN_2D(40)，之前用 MAX_GAIN_2D_OLD(20)
    """
    if df is None or len(df) < _NEW_HIGH_DAYS + 7:
        return None

    latest = df.iloc[-1]
    latest_open = float(latest["open"])
    latest_close = float(latest["close"])
    latest_amount = float(latest["amount"])
    latest_day = latest["day"]
    prev_close = float(df["close"].iloc[-2])
    pct_change = (latest_close - prev_close) / prev_close * 100 if prev_close > 0 else float("nan")

    # 条件1：近100交易日新高（不含当日）
    prior_100_high = df["high"].iloc[-101:-1].max()
    if latest_close < prior_100_high:
        return None

    # 条件1.1：连续新高天数在 [min_consec, max_consec] 内（默认最多2日：只要首日突破/突破第二日）
    def _is_new_high(i):
        return df["close"].iloc[i] >= df["high"].iloc[i - 100:i].max()

    n = len(df)
    yesterday_new_high = _is_new_high(n - 2)
    look = max(min_consec, max_consec + 1) if max_consec is not None else 30   # 不设上限时数出实际天数（最多30）
    consec = 1
    while consec < look and n - 1 - consec - 100 >= 0 and _is_new_high(n - 1 - consec):
        consec += 1
    if consec < min_consec or (max_consec is not None and consec > max_consec):
        return None

    # 条件1.2：近2日涨幅 < 40%（含当日，对比3个交易日前收盘；信号日 2026-11-06 前为 20%），过滤已大幅拉升的
    if max_gain_2d == "auto":
        max_gain_2d = MAX_GAIN_2D if pd.Timestamp(latest_day) >= pd.Timestamp(MAX_GAIN_2D_FROM) else MAX_GAIN_2D_OLD
    close_2d_ago = float(df["close"].iloc[-3])
    if close_2d_ago <= 0:
        return None
    gain_2d = (latest_close - close_2d_ago) / close_2d_ago * 100
    if max_gain_2d is not None and gain_2d >= max_gain_2d:
        return None

    # 条件2：收红
    if latest_close <= latest_open * 1.01:
        return None

    # 条件2.1：K线形态淘汰（同 get_reason 放量4条），满足任一剔除
    latest_high = float(latest["high"])
    latest_low = float(latest["low"])
    if latest_close < latest_open:  # 收绿
        return None
    if not pct_change > 1:  # 滞涨（涨幅≤1%）
        return None
    if latest_close < (latest_high + latest_low) / 2:  # 冲高回落（收在中位以下）
        return None
    if latest_high - latest_close >= abs(latest_close - latest_open):  # 长上影（上影≥实体）
        return None

    # 条件3：成交额 > 前一日往前推5天均值 * 1.5（即前5个交易日：索引 -6 到 -2）
    avg_amount_5d = df["amount"].iloc[-6:-1].mean()
    if not avg_amount_5d or avg_amount_5d <= 0:
        return None
    amount_ratio = latest_amount / avg_amount_5d
    if amount_ratio < 1.5:
        return None

    atr_pct = _calc_atr(df, 14)
    grid_step = round(atr_pct * 1.2, 2) if not np.isnan(atr_pct) else float("nan")
    rps120, gain10_rank = _calc_rps_and_rank_sina(code, market_gains)

    return {
        "股票代码": code,
        "股票名称": name,
        "突破日期": pd.to_datetime(latest_day).strftime("%Y-%m-%d"),
        "突破价": round(latest_close, 2),
        "当前价": round(latest_close, 2),
        "突破日涨幅(%)": round(pct_change, 2),
        "近2日涨幅(%)": round(gain_2d, 2),
        "昨日是否新高": "是" if yesterday_new_high else "否",
        "连续新高天数": consec if consec < look else f"≥{look}",
        "突破日成交额倍量比": round(amount_ratio, 2),
        "14日ATR": atr_pct,
        "网格步长": grid_step,
        "RPS120(百分位)": rps120,
        "10日涨幅排名": gain10_rank,
    }


def _sina_load_market(datalen: int = 250, max_workers: int = _SINA_MAX_WORKERS):
    """加载全市场新浪日K（走 cache_sina 缓存），返回 (最新交易日, 股票列表, {symbol: df})"""
    latest_date = _sina_get_latest_trading_date()
    stock_list = _sina_get_stock_list()
    _log(f"最新交易日: {latest_date}，共 {len(stock_list)} 只股票，开始拉取日线...")
    df_all = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {executor.submit(_sina_get_kline, s["symbol"], latest_date, datalen): s for s in stock_list}
        for fut in as_completed(futs):
            try:
                df_all[futs[fut]["symbol"]] = fut.result(timeout=30)
            except Exception:
                df_all[futs[fut]["symbol"]] = None
    miss = [k for k, v in df_all.items() if v is None or v.empty]
    _log(f"日线获取成功 {len(df_all) - len(miss)} 只，失败/无数据 {len(miss)} 只"
         + ("（失败过多，可能被新浪限流，选股可能不完整）" if len(miss) > len(df_all) * 0.05 else ""))
    return latest_date, stock_list, df_all


def screen_new_high_breakout_sina(datalen: int = 250, max_workers: int = _SINA_MAX_WORKERS,
                                  min_consec: int = 2, max_consec=None, market=None) -> pd.DataFrame:
    """
    选股：100日新高 + 倍量突破，以最新交易日为突破日，全部满足才入选
    筛选条件：
      1. 新高：收盘价 >= 前100个交易日（不含当日）最高价
      2. 新高天数：连续新高天数在 [min_consec, max_consec] 内，默认至少连续2日（T+1回测下只突破1日的拖后腿）
      3. 近2日涨幅 < 40%：当日收盘对比3个交易日前收盘，>=40%剔除（信号日 2026-11-06 前为 20%，见 MAX_GAIN_2D_FROM）
      4. 收红：收盘价 > 开盘价 * 1.01
      5. K线形态，满足任一剔除：
         - 收绿（收盘 < 开盘）
         - 滞涨（涨幅 <= 1%，相对昨日收盘）
         - 冲高回落（收盘 < (最高+最低)/2）
         - 长上影（上影 最高-收盘 >= 实体 |收盘-开盘|）
      6. 倍量：当日成交额 >= 前5个交易日成交额均值 * 1.5（成交额=收盘价*成交量估算）
    参数：
      datalen: 每只股票拉取的日K线条数，需 >= 137（100日新高+ATR等计算余量）
      max_workers: 拉取K线的并发线程数
      market: 已加载的 _sina_load_market() 结果，传入则不重复加载
    数据源：新浪财经（hq.sinajs.cn 实时 / money.finance.sina.com.cn 日K线），与 stock_check_xinlang 同源
    字段计算：14日ATR / 网格步长 / RPS120 / 10日涨幅排名，公式对齐 stock_screener.py
    输出：stock_result/result_sina_{时间戳}.csv（中文列名，utf-8-sig），按RPS120高→低；这是日线信号，实际买入看 build_buy_pool 的合并股票池
      额外字段：突破日涨幅(%) / 近2日涨幅(%) / 昨日是否新高 / 连续新高天数
    """
    latest_date, stock_list, df_all = market or _sina_load_market(datalen, max_workers)

    market_gains = _collect_market_gains_sina(df_all)

    results = []
    for s in stock_list:
        code = s["symbol"]
        name = s.get("name", "")
        r = _check_breakout_sina(code, name, df_all.get(code), market_gains, min_consec, max_consec)
        if r:
            results.append(r)

    if not results:
        _log("无符合条件股票")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("RPS120(百分位)", ascending=False, na_position="last").reset_index(drop=True)

    os.makedirs("stock_result", exist_ok=True)
    fname = f"stock_result/result_sina_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False)
    _log(f"结果已保存: {fname}（共 {len(df)} 只）")
    df.attrs["fname"] = fname
    return df


# ─────────────────────────────────────────────
#  合并股票池：本周有效的周线信号 + 当日日线信号，按 RPS120 高→低，次日集合竞价买入
# ─────────────────────────────────────────────
OPEN_GAP_MAX = 0.07          # 开盘涨幅 > 7% 不买：集合竞价挂单价 = 前收 × 1.07（不超过涨停价）
SHARES_GAP = 0.04            # 1层参考股数按 前收 × 1.04 估算（按挂单上限 1.07 算会少买太多；开盘高于 4% 时实际金额略超 1 层）
POOL_CAPITAL = 800_000       # 没有模拟账户信息时，按此本金算 1 层参考股数


def _next_trading_day(d):
    import chinese_calendar as cc
    d = pd.Timestamp(d).date() + timedelta(days=1)
    while d.weekday() >= 5 or not cc.is_workday(d):
        d += timedelta(days=1)
    return d


def _limit_pct_sina(code: str) -> float:
    return 0.2 if code[2:5] in ("300", "301", "688", "689") else 0.1


def _rps120_qfq(code: str, df: pd.DataFrame, all_g120: np.ndarray) -> float:
    """候选股（df 已前复权）120日涨幅在全市场（前复权）分布里的百分位；与回测/前向跟踪口径一致"""
    c = df["close"].values
    if len(c) < _RPS_DAYS + 1 or not len(all_g120):
        return float("nan")
    g = c[-1] / c[-(_RPS_DAYS + 1)] - 1
    return round((all_g120 < g).mean() * 100, 1)


def load_weekly_signals() -> pd.DataFrame:
    """stock_screener 写入的周线信号历史（只留进股票池的类型）"""
    if not os.path.exists(WEEKLY_SIGNALS_FILE):
        return pd.DataFrame(columns=["信号周", "代码", "名称", "信号类型", "突破价", "ATR%", "网格步长%"])
    w = pd.read_csv(WEEKLY_SIGNALS_FILE, encoding="utf-8-sig", dtype={"代码": str})
    w["信号周"] = pd.to_datetime(w["信号周"])
    return w[w["信号类型"].str.startswith(WEEKLY_POOL_SIGNALS)].reset_index(drop=True)


def build_buy_pool(market, daily_sig: pd.DataFrame, sim: dict = None) -> pd.DataFrame:
    """
    生成下一交易日的合并股票池 stock_result/pool_{日期}.csv：
      - 周线：上一周的买点1-W2/W3/买点2（下一周周一~周五有效），前收 < 突破价 的剔除
      - 日线：当日 screen_new_high_breakout_sina 的信号（连续≥2日新高），只在下一交易日有效
      - 统一按 RPS120（前复权）高→低给买入顺序，同分 10日涨幅弱→强；开盘买入价上限 = 前收×1.07（不超过涨停价）
      - RPS120 / 10日涨幅排名的取值日：周线票固定用信号周五收盘（整周不变），日线票用当日收盘；与回测/前向跟踪一致
      - sim（forward_track 结果）：标注模拟持仓中/本周已买卖，并给出 1 层参考股数与可新开只数
    """
    latest_date, stock_list, df_all = market
    names = {s["symbol"]: s.get("name", "") for s in stock_list}
    next_day = _next_trading_day(latest_date)
    nd = pd.Timestamp(next_day)

    cands = []
    wk = load_weekly_signals()
    wk = wk[wk["信号周"].dt.to_period("W-FRI") == nd.to_period("W-FRI") - 1]
    for r in wk.to_dict("records"):
        cands.append({"代码": r["代码"], "来源": "周线", "信号": r["信号类型"], "信号日": r["信号周"].strftime("%Y-%m-%d"),
                      "突破价": r["突破价"], "网格步长%": r["网格步长%"]})
    if daily_sig is not None and not daily_sig.empty:
        for r in daily_sig.to_dict("records"):
            cands.append({"代码": r["股票代码"], "来源": "日线", "信号": f"连续{r['连续新高天数']}日新高",
                          "信号日": r["突破日期"], "突破价": r["突破价"], "网格步长%": r["网格步长"]})

    snaps = {}

    def snapshot(asof):
        """asof 日收盘时的全市场涨幅分布（周线票用信号周五）"""
        if asof not in snaps:
            sub = df_all if asof == pd.Timestamp(latest_date) else \
                {k: d[d["day"] <= asof] for k, d in df_all.items() if d is not None}
            g = _collect_market_gains_sina(sub)
            snaps[asof] = (sub, g, np.array([v["gain120"] for v in g.values() if not np.isnan(v["gain120"])]),
                           np.array([v["gain10"] for v in g.values() if not np.isnan(v["gain10"])]))
        return snaps[asof]
    layer_amt = (sim or {}).get("equity", POOL_CAPITAL) * 0.1
    held, traded = set((sim or {}).get("held", [])), set((sim or {}).get("traded_week", []))
    rows = []
    for c in cands:
        code, df = c["代码"], df_all.get(c["代码"])
        if df is None or df.empty or "ST" in names.get(code, "").upper():
            continue
        if pd.Timestamp(df["day"].iloc[-1]).date() != latest_date:
            continue                                         # 当日停牌
        prev = float(df["close"].iloc[-1])                  # 前复权最新收盘 = 实际收盘
        if c["来源"] == "周线":
            # 突破价是信号周五跑 baostock 时的前复权价（≈当时实际价），之后若除权，换算到当前前复权口径再比
            bp = float(c["突破价"]) / _qfq_f_at(_sina_qfq_factors(code, refresh=False), c["信号日"])
            if prev < bp:
                continue                                     # 周中走坏，跌破突破价
        cap = min(round(prev * (1 + OPEN_GAP_MAX), 2), round(prev * (1 + _limit_pct_sina(code)), 2))
        sh = layer_amt / min(round(prev * (1 + SHARES_GAP), 2), cap)
        sh = (int(sh) if sh >= 200 else 0) if code[2:5] in ("688", "689") else int(sh // 100 * 100)
        note = ("模拟持仓中，不买" if code in held else "本周已买卖，不买" if code in traded
                else "1层买不起一手" if sh == 0 else "")
        asof = pd.Timestamp(c["信号日"]) if c["来源"] == "周线" else pd.Timestamp(latest_date)
        sub, gains, all_g120, all_g10 = snapshot(asof)
        g10 = gains.get(code, {}).get("gain10", np.nan)
        atr = _calc_atr(df, 14)                              # 网格步长 = 买入前一交易日（最新收盘）ATR14% × 1.2
        c = {**c, "网格步长%": round(atr * 1.2, 2) if not np.isnan(atr) else c["网格步长%"]}
        rows.append({**c, "名称": names.get(code, ""), "RPS120": _rps120_qfq(code, sub[code], all_g120),
                     "10日涨幅排名": int((all_g10 > g10).sum() + 1) if not np.isnan(g10) else np.nan,
                     "前收": round(prev, 2), "开盘买入价上限": cap, "1层参考股数": sh, "备注": note})
    pool = pd.DataFrame(rows, columns=["代码", "名称", "来源", "信号", "信号日", "RPS120", "10日涨幅排名", "前收",
                                       "开盘买入价上限", "突破价", "网格步长%", "1层参考股数", "备注"])
    if not pool.empty:
        # RPS120 高→低；同分（一位小数）时 10日涨幅弱→强（排名数字大的在前），见 backtest_priority.py
        pool = pool.sort_values(["RPS120", "10日涨幅排名"], ascending=[False, False], na_position="last")
        src = pool.groupby("代码")["来源"].agg(lambda x: "+".join(sorted(set(x), reverse=True)))
        pool = pool.drop_duplicates("代码", keep="first").assign(来源=lambda d: d["代码"].map(src)).reset_index(drop=True)
    pool.insert(0, "买入顺序", range(1, len(pool) + 1))

    fname = f"stock_result/pool_{next_day.strftime('%Y%m%d')}.csv"
    pool.to_csv(fname, index=False, encoding="utf-8-sig")
    can_buy = pool[pool["备注"] == ""]
    _log(f"股票池已保存: {fname}（{next_day} 开盘候选 {len(can_buy)} 只：周线 {can_buy['来源'].str.contains('周线').sum()} / "
         f"日线 {can_buy['来源'].str.contains('日线').sum()}）")
    summary = [f"▶ {next_day} 开盘股票池 {len(can_buy)} 只（附件 {os.path.basename(fname)}）：按「买入顺序」集合竞价，"
               f"挂「开盘买入价上限」(前收×1.07)，开盘涨幅>7%买不到即跳过；每只1层（参考股数见附件）"]
    if sim and sim.get("next_level") is not None:
        free = max(0, int(round(sim["next_level"] * 10 - sim.get("held_layers", 0))))
        summary.append(f"▶ 明日大盘目标仓位 {int(sim['next_level'] * 100)}%，模拟持仓约 {sim.get('held_layers', 0):.1f} 层"
                       f" → 最多新开 {free} 只（止损/走弱资金冻结到下周一，以实际可用资金为准）")
    pool.attrs["fname"], pool.attrs["summary"] = fname, summary
    return pool


def _weekly_signals_ready(latest_date) -> bool:
    """本周（latest_date 所在周）的周线信号是否已生成"""
    w = load_weekly_signals() if os.path.exists(WEEKLY_SIGNALS_FILE) else None
    if w is None or w.empty:
        return False
    return (w["信号周"].dt.to_period("W-FRI") == pd.Timestamp(latest_date).to_period("W-FRI")).any()


def run_weekly_screener_if_needed(latest_date) -> list:
    """
    周线选股每周只跑一次：下一交易日的股票池要用"上一周"的周线信号，缺了才跑 stock_screener（本地走代理/服务器直连）。
      - 正常：周五 19:00 那次运行时跑（baostock 17:30 后才有本周周线），周一~四 15:30 直接复用 weekly_signals.csv
      - 周四跑（下一交易日周五）：用的仍是上一周信号，已存在，直接复用
      - 周五休市等：周四 15:30 跑时 baostock 还没本周数据 → 提示并跳过，下一次运行（下周一）再补跑上周的
    """
    nxt = pd.Timestamp(_next_trading_day(latest_date))
    need_week = nxt.to_period("W-FRI") - 1                   # 需要的信号周
    if _weekly_signals_ready(need_week.end_time.date()):
        return []
    now = datetime.now()
    if need_week == pd.Timestamp(latest_date).to_period("W-FRI") and latest_date == now.date() \
            and (now.hour < 17 or (now.hour == 17 and now.minute < 30)):
        msg = "⚠ 本周周线信号还没生成：baostock 17:30 后才有本周周线，请 17:30 后再运行（否则下周股票池缺周线信号，下次运行会补跑）"
        _log(msg)
        return [msg]
    _log(f"开始跑周线选股 stock_screener（信号周 {need_week}）...")
    import stock_screener
    stock_screener.main()
    if not _weekly_signals_ready(need_week.end_time.date()):
        msg = f"⚠ 周线选股已运行，但没有得到 {need_week} 的周线信号（baostock 数据可能未更新）"
        _log(msg)
        return [msg]
    return []


# ─────────────────────────────────────────────
#  深证综指(399106) 10日线 + 底/顶分型 仓位操作（只看收盘，不看盘中）
#  指数选择：深证综指与博主一致，长周期回测（合并池）年化21.6%/夏普1.40，优于深证成指 18.4%/1.24
# ─────────────────────────────────────────────
#  分型：相邻3根K线，不做包含处理，第3根收盘确认。底分型=中间K最高、最低都低于左右；顶分型=反之
#  10日线：收盘站上 → 次日开盘4层；次日收盘仍在线上 → 第三日开盘满仓
#          收盘跌破 → 当天停止新开/加仓/补仓，次交易日集合竞价清空，转为空仓
#  底分型：第3根收盘在10日线下才按底分型处理；第3根收盘价为弱势线，收盘低于它算跌破
#          第二日收盘未跌破 → 第三日开盘4层；第三日收盘未跌破 → 第四日开盘满仓；第二日就跌破 → 作废
#          持仓期间在10日线下：不受10日线清仓约束，收盘跌破弱势线 → 次日开盘清仓；收盘站上10日线后按10日线规则
#  顶分型：有持仓时出现即处理；第3根收盘价为参考，收盘高于它算顶分失效
#          第二日收盘失效 → 不处理；未失效 → 第三日开盘清仓
#          清仓后：收盘突破分型顶点(中间K最高) → 按10日线规则重新进场；先跌破10日线 → 空仓，等站上10日线或新底分型
#  只给大盘仓位建议，不影响选股输出
#  回测见 backtest_weekly_portfolio.market_levels_v2(bottom_confirm=True, top_mode="confirm")

MARKET_INDEX, MARKET_INDEX_NAME = "sz399106", "深证综指"   # 大盘仓位用的指数（新浪代码）


FIRST_LV = 0.4                  # 首日建仓仓位（站上10日线 / 底分型确认）
STRONG_UP_PCT, STRONG_UP_LV = 0.03, 0.7   # 开仓信号当天指数涨幅 > 3% 时首日改建 70%（回测见 backtest_first_level.py strong）


def _pct(lv: float) -> str:
    return f"{int(round(lv * 100))}%"


def _pos_op(cur: float, nxt: float) -> str:
    """当前仓位 → 下一日开盘仓位 的直白操作（每只1层=10%）"""
    if nxt == cur:
        return "空仓不动" if cur == 0 else f"持有{_pct(cur)}不动"
    if nxt == 0:
        return f"集合竞价全部清仓（{_pct(cur)}→0%）"
    if cur == 0:
        return f"开盘买入{int(round(nxt * 10))}只，建到{_pct(nxt)}"
    return f"开盘加仓{int(round((nxt - cur) * 10))}只，{_pct(cur)}→{_pct(nxt)}"


def _watch(lv, frac_mode, k3_close, pending, top_pending, block, ma10_now) -> str:
    """下一交易日收盘要看的条件，决定再下一日的操作"""
    if top_pending is not None:
        ref, _ = top_pending
        return f"收盘≤{ref:.2f}→后天开盘清仓；>{ref:.2f}→顶分型失效继续持有"
    if block is not None:
        return f"收盘>{block:.2f}(顶点)且在10日线上→后天开盘建40%（当天涨>3%则70%）；否则继续空仓"
    if pending is not None:
        return f"收盘≥{pending:.2f}→后天开盘建40%（当天涨>3%则70%）；<{pending:.2f}→底分型作废"
    if frac_mode and lv > 0:
        if lv < 1:
            return f"收盘≥{k3_close:.2f}→后天加满100%；<{k3_close:.2f}→后天开盘清仓"
        return f"收盘<{k3_close:.2f}→后天开盘清仓；站上10日线(今≈{ma10_now:.2f})→转10日线规则"
    if lv > 0:
        if lv < 1:
            return f"收盘仍在10日线上(今≈{ma10_now:.2f})→后天加满100%；跌破→后天开盘清仓"
        return f"收盘跌破10日线(今≈{ma10_now:.2f})→后天开盘清仓；否则满仓持有"
    return f"收盘站上10日线(今≈{ma10_now:.2f})→后天开盘建40%（当天涨>3%则70%）；10日线下出底分型→等次日确认"


def _sz_market_states(df: pd.DataFrame) -> pd.DataFrame:
    """逐日推演仓位状态，df 需含 day/open/high/low/close（只含已收盘K线），逻辑与 market_levels_v2 一致"""
    c, h, l = df["close"].values, df["high"].values, df["low"].values
    ma10 = pd.Series(c).rolling(10).mean().values
    rows = []

    def entry(t):
        """首日建仓仓位及说明：当天涨幅 > STRONG_UP_PCT 用 STRONG_UP_LV，否则 FIRST_LV"""
        g = c[t] / c[t - 1] - 1 if t > 0 else 0
        if g > STRONG_UP_PCT:
            return STRONG_UP_LV, f"建{_pct(STRONG_UP_LV)}（当天涨{g:.1%}>{STRONG_UP_PCT:.0%}）"
        return FIRST_LV, f"建{_pct(FIRST_LV)}"

    lv, frac_mode, k3_close = 0.0, False, 0.0
    pending = None          # 底分型待确认的弱势线
    top_pending = None      # 顶分型待确认 (第3根收盘, 顶点)
    block = None            # 顶分型清仓后需突破的顶点
    for t in range(len(df)):
        bottom = t >= 2 and l[t - 1] < l[t - 2] and l[t - 1] < l[t] and h[t - 1] < h[t - 2] and h[t - 1] < h[t]
        top = t >= 2 and h[t - 1] > h[t - 2] and h[t - 1] > h[t] and l[t - 1] > l[t - 2] and l[t - 1] > l[t]
        prev_lv, notes, action = lv, [], ""
        if np.isnan(ma10[t]):
            action = "—"
        else:
            above = c[t] >= ma10[t]
            done = False
            if top_pending is not None:
                ref, hi = top_pending
                top_pending = None
                if c[t] <= ref and lv > 0:
                    lv, frac_mode, pending, block = 0.0, False, None, hi
                    action = f"次日集合竞价清仓（顶分型确认：收盘≤第3根收盘{ref:.2f}），收盘突破顶点{hi:.2f}才重新进场"
                    done = True
                elif c[t] > ref and lv > 0:
                    notes.append(f"顶分型失效（收盘>{ref:.2f}），不处理")
            if not done and top and (lv > 0 or pending is not None):
                if pending is not None:
                    notes.append(f"顶分型出现，待确认的底分型(弱势线{pending:.2f})作废")
                pending = None
                if lv > 0:
                    top_pending = (c[t], h[t - 1])
                    notes.append(f"顶分型出现：次日收盘≤{c[t]:.2f}则第三日开盘清仓")
            if not done and block is not None:
                if not above:
                    block = None
                elif c[t] > block:
                    notes.append(f"收盘突破顶点{block:.2f}，顶分型失效")
                    block = None
                else:
                    action = f"空仓等待（未突破顶分型顶点{block:.2f}）"
                    done = True
            if not done:
                if pending is not None and lv == 0:
                    if c[t] >= pending:
                        (lv, txt), frac_mode, k3_close = entry(t), True, pending
                        action = f"底分型确认（收盘≥弱势线{pending:.2f}），次日开盘{txt}"
                    else:
                        action = f"底分型作废（收盘<弱势线{pending:.2f}），空仓"
                    pending = None
                    if bottom and not above and lv == 0:
                        pending = c[t]
                        action += f"；新底分型，弱势线{c[t]:.2f}，待次日确认"
                elif above and not frac_mode:
                    if lv == 0:
                        lv, txt = entry(t)
                        action = f"次日开盘{txt}（收盘站上10日线）"
                    elif lv < 1:
                        lv, action = 1.0, "次日开盘加满100%（仍在10日线上）"
                    else:
                        action = "满仓持有（10日线上）"
                elif above and frac_mode:
                    frac_mode = False
                    if lv == 0:
                        lv, txt = entry(t)
                        action = f"次日开盘{txt}（站上10日线）"
                    else:
                        lv, action = 1.0, "站上10日线，改按10日线规则，次日加满100%"
                elif frac_mode and lv > 0:
                    if c[t] < k3_close:
                        lv, frac_mode = 0.0, False
                        action = f"次日集合竞价清仓（底分型跌破：收盘<弱势线{k3_close:.2f}）"
                    elif lv < 1:
                        lv, action = 1.0, f"次日开盘加满100%（底分型未跌破弱势线{k3_close:.2f}）"
                    else:
                        action = f"满仓持有（底分型持仓，弱势线{k3_close:.2f}）"
                    if bottom and frac_mode:
                        k3_close = c[t]
                        action += f"；新底分型，弱势线更新为{k3_close:.2f}"
                elif bottom and not above:
                    pre = "当天停止新开/加仓/补仓，次日集合竞价清仓（跌破10日线）；" if lv > 0 else ""
                    lv, frac_mode, pending = 0.0, False, c[t]
                    action = pre + f"10日线下底分型（弱势线{c[t]:.2f}），次日收盘不跌破则第三日开盘建40%（确认当天涨>3%则70%）"
                elif not above:
                    action = "当天停止新开/加仓/补仓，次日集合竞价清仓（收盘跌破10日线）" if lv > 0 else "空仓观望（10日线下）"
                    lv, frac_mode = 0.0, False
        rows.append({
            "日期": pd.to_datetime(df["day"].iloc[t]).strftime("%Y-%m-%d"),
            "收盘": round(c[t], 2), "MA10": round(ma10[t], 2) if not np.isnan(ma10[t]) else np.nan,
            "位置": "" if np.isnan(ma10[t]) else ("线上" if c[t] >= ma10[t] else "线下"),
            "分型": "底分型" if bottom else ("顶分型" if top else ""),
            "当前仓位": _pct(prev_lv), "下一日仓位": _pct(lv), "下一日操作": _pos_op(prev_lv, lv),
            "原因": "；".join(notes + [action]) if notes else action,
            "下一日收盘关注": _watch(lv, frac_mode, k3_close, pending, top_pending, block,
                              ma10[t] if not np.isnan(ma10[t]) else c[t]),
        })
    return pd.DataFrame(rows)


def sz_market_action(show_days: int = 15, datalen: int = 120) -> pd.DataFrame:
    """输出大盘指数（MARKET_INDEX，深证综指）近 show_days 日的分型与仓位操作；盘中运行时当日未收盘K线不参与判定，单独按实时价预估"""
    url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    resp = requests.get(url, params={"symbol": MARKET_INDEX, "scale": 240, "ma": "no", "datalen": datalen}, timeout=15)
    df = pd.DataFrame(json.loads(resp.text))
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    now = datetime.now()
    closed = now.hour > 15 or (now.hour == 15 and now.minute >= 5)
    today = now.strftime("%Y-%m-%d")
    if df["day"].iloc[-1] == today and not closed:
        df = df.iloc[:-1]   # 当日未收盘，不参与判定

    states = _sz_market_states(df.reset_index(drop=True))
    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 80)
    pd.set_option("display.unicode.east_asian_width", True)
    _log(f"{MARKET_INDEX_NAME}数据截至 {states['日期'].iloc[-1]} 收盘")
    print(f"{'=' * 72}\n  {MARKET_INDEX_NAME} 10日线 + 分型 仓位操作（截至 {states['日期'].iloc[-1]} 收盘）\n{'=' * 72}")
    # 表格不放"下一日收盘关注"（太宽终端会折行错乱），只在下面最新一日的汇总里给出
    print(states.drop(columns="下一日收盘关注").tail(show_days).to_string(index=False))
    last = states.iloc[-1]
    summary = [
        f"▶ {last['日期']} 收盘 {last['收盘']}（MA10 {last['MA10']}，{last['位置']}）{('，' + last['分型']) if last['分型'] else ''}",
        f"▶ 当前仓位：{last['当前仓位']}",
        f"▶ 下一交易日开盘：{last['下一日操作']}  →  仓位 {last['下一日仓位']}",
        f"▶ 原因：{last['原因']}",
        f"▶ 下一交易日收盘关注：{last['下一日收盘关注']}",
    ]
    import chinese_calendar as cc
    if closed and df["day"].iloc[-1] != today and now.weekday() < 5 and cc.is_workday(now.date()):
        # 已过收盘但新浪日K还没有今天这根：上面是截至昨天的结论，不是今天的收盘信号
        summary.insert(0, f"⚠ 今天（{today}）的收盘数据新浪还没更新，以下是截至 {last['日期']} 的结论，"
                          f"不是今天的收盘信号，请 15:15 后再跑")
        _log(summary[0])
    print("\n" + "\n".join("  " + x for x in summary))
    states.attrs["summary"] = summary

    # 盘中：按实时价当作收盘预估今天收盘后的操作
    if not closed and df["day"].iloc[-1] != today:
        fields = requests.get(f"https://hq.sinajs.cn/list={MARKET_INDEX}",
                              headers={"Referer": "https://finance.sina.com.cn"}, timeout=5).text.split('"')[1].split(",")
        if fields[30] == today:
            rt = pd.DataFrame([{"day": today, "open": float(fields[1]), "high": float(fields[4]),
                                "low": float(fields[5]), "close": float(fields[3])}])
            est = _sz_market_states(pd.concat([df, rt], ignore_index=True)).iloc[-1]
            print(f"\n  ◇ 盘中预估（{fields[31]}，若今天按当前价 {est['收盘']} 收盘，MA10 {est['MA10']}，{est['位置']}{('，' + est['分型']) if est['分型'] else ''}）")
            print(f"    明天开盘：{est['下一日操作']} → 仓位 {est['下一日仓位']}；原因：{est['原因']}")
            states.attrs["summary"] += [
                f"◇ 盘中预估（{fields[31]}，若今天按当前价 {est['收盘']} 收盘）：明天开盘 {est['下一日操作']} → 仓位 {est['下一日仓位']}"]
    return states


def weekly_screener_mail_part(latest_date) -> tuple:
    """
    每周最后一个交易日（下一交易日是新的一周，通常周五）发邮件时，附上 stock_screener 的周线选股结果，
    正文标明运行日期、周线数据日期、命中数、拉取失败数。返回 (正文行列表, 附件路径或 None)。
    """
    nxt = pd.Timestamp(_next_trading_day(latest_date))
    if nxt.to_period("W-FRI") == pd.Timestamp(latest_date).to_period("W-FRI"):
        return [], None
    import json
    from stock_screener import WEEKLY_RUN_INFO
    try:
        with open(WEEKLY_RUN_INFO, encoding="utf-8") as f:
            info = json.load(f)
    except Exception:
        return ["⚠ 没找到周线选股运行记录（stock_result/weekly_run_info.json），本周周线结果未附上"], None
    week = pd.Timestamp(latest_date).to_period("W-FRI")
    if not info.get("data_date") or pd.Timestamp(info["data_date"]).to_period("W-FRI") != week:
        return [f"⚠ 周线选股结果的数据日期 {info.get('data_date') or '未知'} 不是本周（{latest_date}），未附上，请检查周线选股"], None
    n_pool = 0
    try:
        w = pd.read_csv(WEEKLY_SIGNALS_FILE, encoding="utf-8-sig")
        w = w[pd.to_datetime(w["信号周"]).dt.to_period("W-FRI") == week]
        n_pool = int(w["信号类型"].astype(str).str.startswith(WEEKLY_POOL_SIGNALS).sum())
    except Exception:
        pass
    f = info.get("result_file")
    lines = [f"▶ 周线选股：运行于 {info['run_time']}，周线数据日期 {info['data_date']}；"
             f"全市场 {info['total']} 只（最新 {info['fresh']} / 落后(停牌等) {info['lag']} / 无数据 {len(info.get('missing', []))}），"
             f"命中 {info['hits']} 只，其中进股票池信号(W2/W3/买点2) {n_pool} 只"
             + (f"（附件 {os.path.basename(f)}）" if f else "")]
    bad = info.get("scan_failed") or info.get("missing")
    if bad:
        lines.append(f"  ⚠ 有 {len(bad)} 只周线拉取失败未参与扫描：{', '.join(bad[:15])}{' ...' if len(bad) > 15 else ''}，"
                     f"可手动重跑 python stock_screener.py 补齐后再跑 main")
    return lines, (f if f and os.path.exists(f) else None)


def send_mail_advice(summary: list, file_dirs: list):
    """发送操作建议邮件：标题=操作建议，正文=大盘下一日建议，附件=股票池csv"""
    import yagmail
    receivers = ["1558109546@qq.com"]
    title = "操作建议"
    content = "\n".join(summary)
    yag = yagmail.SMTP(user='dailyfreshhlfyy@yeah.net', password='315128abc', host='smtp.yeah.net', port=465)
    res = yag.send(to=receivers, subject=title, contents=content, attachments=file_dirs)
    _log(f"邮件已发送：{receivers}  附件：{file_dirs or '无'}  {res or ''}")

SINA_WAIT_MAX_MIN = 90     # main 开始时等新浪日K出今天这根，最多等这么久（分钟），每 5 分钟查一次


def _wait_sina_daily_ready() -> bool:
    """
    今天是交易日且已过 15:10 时，等新浪日K（指数 + 两只个股）出现今天这根再往下跑；
    新浪收盘后更新日K有延迟（实测 15:20 仍未更新），没更新就跑会把昨天的数据当今天用。超时返回 False
    """
    import chinese_calendar as cc
    deadline = time.time() + SINA_WAIT_MAX_MIN * 60
    while True:
        latest = _sina_get_latest_trading_date()
        today = latest.strftime("%Y-%m-%d")
        if latest != datetime.now().date():
            return True                          # 收盘前/非交易日：按前一交易日跑，不用等
        url = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        ok = 0
        for sym in (MARKET_INDEX, "sh600000", "sz000001"):
            try:
                data = json.loads(requests.get(url, params={"symbol": sym, "scale": 240, "ma": "no", "datalen": 1},
                                               timeout=15).text)
                ok += bool(data) and data[-1]["day"] == today
            except Exception:
                pass
        if ok == 3:
            return True
        if time.time() >= deadline:
            return False
        _log(f"新浪日K还没有 {today} 的数据（{ok}/3 已更新），5 分钟后再查")
        time.sleep(300)


def main():
    """
    每日流程（收盘后运行；每周最后一个交易日请在 17:30 后运行，会自动跑周线选股）：
      大盘仓位建议 → [周末]周线选股 → 日线选股(连续≥2日新高) → 前向跟踪(模拟持仓/清仓价) → 合并股票池 → 发邮件
    """
    _log(f"开始运行（主机 {socket.gethostname()}）")
    if not _wait_sina_daily_ready():
        msg = f"⚠ 等了 {SINA_WAIT_MAX_MIN} 分钟新浪日K仍没有今天的数据，本次不出结果，请稍后手动运行 python stock_check_xinlang.py main"
        _log(msg)
        if "houjie-" not in socket.gethostname():
            send_mail_advice([msg], [])
        return
    # 深证综指 10日线+底/顶分型 仓位操作；只是仓位建议，出错也不影响下面选股
    try:
        summary = sz_market_action().attrs["summary"]
    except Exception as e:
        summary = [f"[大盘仓位] 获取失败：{type(e).__name__}: {e}"]
        _log(summary[0])
    market, market_raw, daily_sig = None, None, None
    weekly_file = None
    try:
        market_raw = _sina_load_market()
        summary += run_weekly_screener_if_needed(market_raw[0])
        try:
            wk_lines, weekly_file = weekly_screener_mail_part(market_raw[0])
            summary += wk_lines
        except Exception as e:
            _log(f"[周线结果附件] 失败：{type(e).__name__}: {e}")
        market = _sina_qfq_market(market_raw)            # 日线选股/RPS/10日排名/股票池都用前复权，同回测
        daily_sig = screen_new_high_breakout_sina(market=market)
    except Exception as e:
        err = f"[选股] 失败：{type(e).__name__}: {e}"
        _log(err)
        summary = summary + ["", err]
    # 策略前向跟踪：模拟持仓 + 下一日盘中清仓价（复用上面加载的行情）
    hold_file, sim, hist_file = None, None, None
    try:
        import forward_track
        ft = forward_track.main(market=market_raw, market_qfq=market)
        hold_file, sim, hist_file = ft["holdings_file"], ft.get("sim"), ft.get("history_file")
        summary = summary + [""] + ft["summary"]
    except Exception as e:
        err = f"[前向跟踪] 失败：{type(e).__name__}: {e}"
        _log(err)
        summary = summary + ["", err]
    # 合并股票池（下一交易日开盘买入）
    pool_file = None
    if market is not None:
        try:
            pool = build_buy_pool(market, daily_sig, sim)
            pool_file = pool.attrs["fname"]
            summary = summary + [""] + pool.attrs["summary"]
        except Exception as e:
            err = f"[股票池] 失败：{type(e).__name__}: {e}"
            _log(err)
            summary = summary + ["", err]
    # 服务器定时任务跑时发邮件（本地不发）
    if "houjie-" not in socket.gethostname():
        send_mail_advice(summary, [f for f in (pool_file, hold_file, hist_file, weekly_file) if f])
    _log("运行结束")


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'main':
        main()
    elif len(sys.argv) == 2 and sys.argv[1] == 'qfq_init':
        qfq_init()
    else:
        # main()
        # 计算指定日期价格-max(MA5×0.99, 最低价×0.99),每天计算一次持仓个股
        for code in ['sz301045']:
            res = calc_buy_price_sina(code, date=time.strftime("%Y-%m-%d"))
            print(code, res)

        # 新浪的数据成交额是计算的，有偏差；东方财富的是直接取的，准确些
        # check_stock("sh603256")
        # 260430
        # for code in ['sh600791', 'sh603256', 'sz300438', 'sz002240', 'sz002730', 'sz300657', 'sz002436', 'sh600773', 'sh688667', 'sh688081', 'sz002810', 'sz002947']:
        # 2605
        # for code in ['sh600234', 'sh600330', 'sh603268', 'sh688661', 'sh600510', 'sz002859', 'sz301196', 'sh600791', 'sz002240', 'sz002730', 'sh600773']:
        # for code in ['sz300769', 'sh600105', 'sz301176', 'sz300861', 'sz002785', 'sz300201', 'sz300668', 'sh600330', 'sh603268', 'sh688661', 'sh600791', 'sz002730']:
        # for code in ['sz002730', 'sh688268', 'sz300161', 'sz300502', 'sz002832', 'sz003018', 'sz300821', 'sh603520', 'sh688390', 'sh688548', 'sh603268', 'sh600791']:
        # for code in ['sz002273', 'sh688328', 'sh688383', 'sh603269', 'sh688700', 'sz300001', 'sz300776', 'sh688300', 'sz003018', 'sh688390', 'sh600791']:
        # 2606
        # for code in ['sz300679', 'sz002281', 'sz300757', 'sh601016', 'sz300825', 'sz300502', 'sh600869', 'sh688328', 'sh688300']:
        # for code in 'sh600869,sz300825,sz300700,sz301568,sz300234,sz300398,sh603663,sh600226,sz300706,sh688096,sh688432,sh688126,sh688757,sz301188'.split(','):
        # codes = 'sz300825,sz300700,sz300234,sh603663,sh600226,sz301188,sh603256,sz301389,sz002125,sh688379,sz300835,sz300567,sz300398,sz000608,sz301128'.split(','):
        # codes = 'sz301188,sh603256,sz300835,sz300398,sz301369,sz300236,sz301045,sz301182,sh688045,sz300689,sz300985,sh688172,sh688551,sh688362'
        # 2607
        # codes = 'sz301369,sz300236,sz300985,sh688362,sz002056,sz300666,sz300234,sz300041,sz300263,sh603738'
        # 260814
        # codes = 'sz002821,sz300759,sh600521,sz301376,sz300985,sz301045,sz301188,sz300398,sz300567,sh603663'
        # codes = 'sz300759,sz300985,sz301045,sz300398,sz300567,sh603663,sz002768,sz301568,sz300706,sh688757,sz301196'
        # codes = 'sz300985,sz301045,sh603663,sz002768,sz301196,sh600226,sh603256,sz002388,sz300684,sh603151,sh603979,sz301188'
        # for code in codes.split(',')[::-1]:
        #     check_stock(code)
        # reason = get_reason(34.77, 37.55, 40.45, 34.32, 34.91, 2345310250.4, 2031046330.03, 31.178)
        # print(reason)

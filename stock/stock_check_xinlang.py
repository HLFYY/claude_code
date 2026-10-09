import json
import os
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from stock_screener import fetch_daily_recent, _calc_atr


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
#   1. 日线收盘新高（近100交易日，不含当日）
#   2. 日线收红（收盘价 > 开盘价 * 1.01）
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


def _sina_get_stock_list():
    """获取全部A股列表（新浪财经），缓存1天"""
    cache_file = _SINA_CACHE_DIR / "stock_list_sina.pkl"
    if cache_file.exists():
        cache_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
        if datetime.now() - cache_time < timedelta(days=1):
            with open(cache_file, "rb") as f:
                return pickle.load(f)

    print("正在获取股票列表...")
    all_stocks = []
    url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    page = 1
    while True:
        params = {
            "page": page, "num": 80, "sort": "symbol", "asc": 1,
            "node": "hs_a", "symbol": "", "_s_r_a": "page",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                break
            data = json.loads(resp.text)
            if not data:
                break
            all_stocks.extend(data)
            if len(data) < 80:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"获取股票列表出错: {e}")
            break

    with open(cache_file, "wb") as f:
        pickle.dump(all_stocks, f)
    print(f"共获取 {len(all_stocks)} 只股票")
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
        print(f"[ERROR] {symbol}: {type(e).__name__}: {e}")
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


def calc_buy_price_sina(code: str, date, datalen: int = 250) -> float:
    """
    计算指定股票在指定日期的买入参考价：max(该日MA5 * 0.99, 该日最低价 * 0.99)
    code: 股票代码，裸代码('600000')或带前缀('sh600000')均可
    date: 目标日期，字符串('2025-07-01')或 date/datetime
    """
    symbol = _to_sina_symbol(code)
    target_date = pd.to_datetime(date).date()

    df = _sina_get_kline(symbol, target_date, datalen)
    if df is None or df.empty:
        raise ValueError(f"未获取到 {symbol} 的K线数据")

    df = df.sort_values("day").reset_index(drop=True)
    idx = df.index[df["day"].dt.date == target_date]
    if len(idx) == 0:
        raise ValueError(f"{symbol} 在 {target_date} 没有K线数据（非交易日或超出数据范围）")
    i = idx[0]

    if i < 4:
        raise ValueError(f"{symbol} 在 {target_date} 之前的数据不足5天，无法计算MA5")

    ma5 = df["close"].iloc[i - 4:i + 1].mean()
    low = df["low"].iloc[i]
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


def _check_breakout_sina(code: str, name: str, df: pd.DataFrame, market_gains: dict) -> dict:
    """检查单只股票是否满足 100日新高+收红+倍量 条件，满足则返回结果字典，否则 None"""
    if df is None or len(df) < _NEW_HIGH_DAYS + 7:
        return None

    latest = df.iloc[-1]
    latest_open = float(latest["open"])
    latest_close = float(latest["close"])
    latest_amount = float(latest["amount"])
    latest_day = latest["day"]

    # 条件1：近100交易日新高（不含当日）
    prior_100_high = df["high"].iloc[-101:-1].max()
    if latest_close < prior_100_high:
        return None

    # 条件2：收红
    if latest_close <= latest_open * 1.01:
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
        "突破日成交额倍量比": round(amount_ratio, 2),
        "14日ATR": atr_pct,
        "网格步长": grid_step,
        "RPS120(百分位)": rps120,
        "10日涨幅排名": gain10_rank,
    }


def screen_new_high_breakout_sina(datalen: int = 250, max_workers: int = _SINA_MAX_WORKERS) -> pd.DataFrame:
    """
    选股：日线收盘新高(近100交易日) + 日线收红(收盘>开盘*1.01) + 成交额>前5日均值*1.5倍
    数据源：新浪财经（hq.sinajs.cn 实时 / money.finance.sina.com.cn 日K线），与 stock_check_xinlang 同源
    字段计算：14日ATR / 网格步长 / RPS120 / 10日涨幅排名，公式对齐 stock_screener.py
    输出：stock_result/result_sina_{时间戳}.csv（中文列名，utf-8-sig）
    """
    latest_date = _sina_get_latest_trading_date()
    stock_list = _sina_get_stock_list()
    print(f"最新交易日: {latest_date}，共 {len(stock_list)} 只股票，开始拉取日线...")

    df_all = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {
            executor.submit(_sina_get_kline, s["symbol"], latest_date, datalen): s
            for s in stock_list
        }
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                df_all[s["symbol"]] = fut.result(timeout=30)
            except Exception:
                df_all[s["symbol"]] = None

    market_gains = _collect_market_gains_sina(df_all)

    results = []
    for s in stock_list:
        code = s["symbol"]
        name = s.get("name", "")
        r = _check_breakout_sina(code, name, df_all.get(code), market_gains)
        if r:
            results.append(r)

    if not results:
        print("无符合条件股票")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("突破日成交额倍量比", ascending=False).reset_index(drop=True)

    os.makedirs("stock_result", exist_ok=True)
    fname = f"stock_result/result_sina_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False)
    print(f"结果已保存: {fname}（共 {len(df)} 只）")
    return df


if __name__ == "__main__":
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
    # 100日线新高
    screen_new_high_breakout_sina()
    # 计算指定日期价格-max(MA5×0.99, 最低价×0.99),每天计算一次持仓个股
    for code in []:
        res = calc_buy_price_sina(code, date=time.strftime("%Y-%m-%d"))
        print(code, res)
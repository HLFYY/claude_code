import requests
from datetime import datetime, timedelta


def get_secid(code: str) -> str:
    """sz→0, sh→1"""
    if code.startswith("sz"):
        return f"0.{code[2:]}"
    elif code.startswith("sh"):
        return f"1.{code[2:]}"
    raise ValueError(f"未知市场前缀：{code}")


def get_realtime_eastmoney(code: str) -> dict:
    """东财实时行情"""
    secid = get_secid(code)
    url = (
        f"https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={secid}&fields=f43,f44,f45,f46,f47,f48,f57,f58,f60"
    )
    resp = requests.get(url, timeout=5)
    d = resp.json()["data"]
    # f43=当前价 f44=最高 f45=最低 f46=开盘 f47=成交量(手) f48=成交额(元)
    # f60=昨收
    factor = d.get("f59", 100)  # 小数位数因子，一般是100
    return {
        "current_p":  d["f43"] / factor,
        "today_high": d["f44"] / factor,
        "today_low":  d["f45"] / factor,
        "today_open": d["f46"] / factor,
        "today_amt":  d["f48"],           # 成交额（元），直接精确值
        "last_close": d["f60"] / factor,  # 昨收（非上周收盘）
    }


def get_daily_klines_eastmoney(code: str, count: int = 40) -> list:
    """
    东财日线K线，直接返回 amount 字段，无需估算
    返回列表：[{"day", "open", "close", "high", "low", "volume", "amount"}, ...]
    """
    secid = get_secid(code)
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={secid}"
        f"&fields1=f1,f2,f3,f4,f5,f6"
        f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
        f"&klt=101&fqt=0&lmt={count}&end=20991231"
    )
    resp = requests.get(url, timeout=5)
    raw = resp.json()["data"]["klines"]

    result = []
    for item in raw:
        # 格式: "日期,开,收,高,低,成交量(手),成交额(元),振幅,涨跌幅,涨跌额,换手率"
        parts = item.split(",")
        result.append({
            "day":    parts[0],
            "open":   float(parts[1]),
            "close":  float(parts[2]),
            "high":   float(parts[3]),
            "low":    float(parts[4]),
            "volume": float(parts[5]),
            "amount": float(parts[6]),  # ✅ 直接精确成交额（元）
        })
    return result


def get_weekly_kdata(code: str):
    # 1. 实时数据
    rt = get_realtime_eastmoney(code)
    current_p   = rt["current_p"]
    today_high  = rt["today_high"]
    today_low   = rt["today_low"]
    today_open  = rt["today_open"]
    today_amt   = rt["today_amt"]   # ✅ 精确今日成交额

    # 2. 历史日线（含精确 amount）
    daily = get_daily_klines_eastmoney(code, count=40)

    today_date  = daily[-1]["day"] if daily else datetime.now().strftime("%Y-%m-%d")
    # 判断最后一条是否是今天（盘中时最后一条可能是昨天）
    today       = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    last_bar_dt = datetime.strptime(daily[-1]["day"], "%Y-%m-%d")
    last_bar_is_today = (last_bar_dt.date() == today.date())

    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday - timedelta(days=1)

    # 3. 按周分组
    week_map = {}
    for bar in daily:
        d = datetime.strptime(bar["day"], "%Y-%m-%d")
        # 盘中时最后一条是昨天，不需要排除；收盘后最后一条是今天，排除避免重复
        if last_bar_is_today and bar["day"] == daily[-1]["day"]:
            continue
        wk_key = (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")
        week_map.setdefault(wk_key, []).append(bar)

    def week_summary(bars):
        return {
            "open":   bars[0]["open"],
            "close":  bars[-1]["close"],
            "high":   max(b["high"] for b in bars),
            "low":    min(b["low"]  for b in bars),
            "amount": sum(b["amount"] for b in bars),  # ✅ 精确
        }

    # 4. 本周数据
    this_wk_key = this_monday.strftime("%Y-%m-%d")
    hist_bars   = week_map.get(this_wk_key, [])

    if hist_bars:
        week_open = hist_bars[0]["open"]
        week_high = max([b["high"] for b in hist_bars] + [today_high])
        week_low  = min([b["low"]  for b in hist_bars] + [today_low])
        hist_amt  = sum(b["amount"] for b in hist_bars)  # ✅ 精确
    else:
        week_open = today_open
        week_high = today_high
        week_low  = today_low
        hist_amt  = 0

    week_amt = hist_amt + today_amt  # ✅ 历史精确 + 今日实时精确

    this_week = {
        "open":   week_open,
        "close":  current_p,
        "high":   week_high,
        "low":    week_low,
        "amount": week_amt,
    }

    # 5. 上周数据
    last_wk_key = last_monday.strftime("%Y-%m-%d")
    last_bars   = week_map.get(last_wk_key, [])
    if not last_bars:
        last_bars = [
            b for b in daily
            if last_monday <= datetime.strptime(b["day"], "%Y-%m-%d") <= last_sunday
        ]
    last_week = week_summary(last_bars) if last_bars else {
        "open": 0, "close": rt["last_close"], "high": 0, "low": 0, "amount": 0
    }

    # 6. 实时5周线 = 当前价 + 前4周收盘 均值
    sorted_wk_keys  = sorted(week_map.keys())
    past_wk_keys    = [w for w in sorted_wk_keys if w != this_wk_key]
    recent_4_keys   = past_wk_keys[-4:]
    recent_4_prices = [week_map[w][-1]["close"] for w in recent_4_keys]
    ma5_realtime    = (current_p + sum(recent_4_prices)) / 5

    return {
        "this_week":       this_week,
        "last_week":       last_week,
        "last_week_close": last_week["close"],
        "ma5_realtime":    round(ma5_realtime, 3),
    }


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

    is_green     = c < o
    is_expand    = this_amt > last_amt
    gain         = (c - last_week_close) / last_week_close * 100
    mid          = (h + l) / 2
    upper_shadow = h - c
    body         = abs(c - o)
    shrink_ratio = this_amt / last_amt if last_amt else 1

    print(f"\n{'='*58}")
    print(f"  代码：{code}   检查时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*58}")
    print(f"  本周：开={o}  收={c}  高={h}  低={l}")
    print(f"  上周收盘：{last_week_close}")
    print(f"  涨跌幅：{gain:+.2f}%   {'📈 放量' if is_expand else '📉 缩量'}（本周/上周={shrink_ratio:.2f}）")
    print(f"  本周成交额={this_amt/1e8:.2f}亿   上周成交额={last_amt/1e8:.2f}亿")
    print(f"  实时5周线={ma5}   {'收盘在5周线上方 ✅' if c > ma5 else '收盘在5周线下方 ❌'}")
    print(f"{'='*58}")

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
            print("   缩量且收绿，检查3个留仓条件")
            reasons = []
            if shrink_ratio > 0.8:
                reasons.append(f"缩量不明显（本周/上周={shrink_ratio:.2f}>0.8）")
            if c <= ma5:
                reasons.append(f"收盘{c}未在5周线{ma5}上方")
            if gain < -5:
                reasons.append(f"跌幅{gain:.2f}%超5%")
            reason = "；".join(reasons)

    _print_result(code, reason)
    return reason == ""


def _print_result(code, reason):
    action = "保留" if not reason else "清仓"
    print(f"\n  {'✅ 保留' if action == '保留' else '❌ 清仓'}")
    print(f"  代码：{code}")
    print(f"  原因：{reason if reason else '—'}")


if __name__ == "__main__":
    for code in ['sz300438', 'sh603256', 'sh603906', 'sz002240', 'sh688667', '', '', '', '', '', '', '', '']:
        check_stock(code)
import requests
from datetime import datetime, timedelta

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
    codes = 'sz301369,sz300236,sz300985,sh688362,sz002056,sz300666,sz300234,sz300041,sz300263,sh603738'
    for code in codes.split(',')[::-1]:
        check_stock(code)
    # reason = get_reason(34.77, 37.55, 40.45, 34.32, 34.91, 2345310250.4, 2031046330.03, 31.178)
    # print(reason)
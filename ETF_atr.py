#!/usr/bin/env python3
"""
场内ETF ATR 采集与计算
=======================
用法：
    python3 etf_atr.py <ETF代码> <ATR天数>

示例：
    python3 etf_atr.py 588000 14

数据源：akshare（东方财富接口，免费，无需token）
"""

import sys
import argparse
import numpy as np
import pandas as pd

try:
    import akshare as ak
except ImportError:
    print("请先安装 akshare: pip install akshare --break-system-packages")
    sys.exit(1)


def fetch_etf_daily(code: str, need_days: int) -> pd.DataFrame:
    """
    获取ETF日线数据。
    need_days: ATR计算所需的最小天数，实际拉取会多留一些余量
               （应对节假日/停牌导致的数据缺口）。
    """
    buffer_days = max(need_days * 3, need_days + 30)  # 留足余量
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=buffer_days * 2)).strftime("%Y%m%d")
    end_date = pd.Timestamp.now().strftime("%Y%m%d")

    df = ak.fund_etf_hist_em(
        symbol=code,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",   # 前复权，和历史价格连续性保持一致
    )

    if df is None or df.empty:
        raise ValueError(f"未获取到ETF代码 {code} 的行情数据，请确认代码是否正确")

    df = df.rename(columns={
        "日期": "date", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount",
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def calc_atr(df: pd.DataFrame, n: int = 14, method: str = "wilder") -> dict:
    """
    计算ATR(N)。
    method:
      "sma"    —— 简单移动平均：最近N根TR的算术平均
      "wilder" —— 威尔德平滑（标准ATR算法，业界更常用）：
                   第一个ATR值 = 前N根TR的简单平均
                   之后每根：ATR_today = (ATR_prev*(N-1) + TR_today) / N
    返回最新一期的 ATR 绝对值 和 ATR%（相对当前收盘价的百分比）。
    """
    if len(df) < n + 1:
        raise ValueError(f"数据不足：需要至少 {n + 1} 根K线，实际只有 {len(df)} 根")

    close = df["close"].values
    high = df["high"].values
    low = df["low"].values

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]  # 第一根没有前收盘，用当天收盘代替（不影响TR计算，因为high-low已经足够）

    tr = np.maximum(
        high - low,
        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close))
    )
    tr = tr[1:]  # 去掉第一根（没有真实的前收盘数据）

    if method == "sma":
        atr_series = pd.Series(tr).rolling(window=n).mean()
    else:  # wilder 威尔德平滑
        atr_series = pd.Series(tr).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()

    latest_atr = round(float(atr_series.iloc[-1]), 4)
    latest_close = round(float(close[-1]), 4)
    atr_pct = round(latest_atr / latest_close * 100, 2) if latest_close > 0 else float("nan")

    return {
        "code": None,          # 由调用方补充
        "latest_date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "latest_close": latest_close,
        "atr": latest_atr,
        "atr_pct": atr_pct,
        "atr_n": n,
        "method": method,
    }


def get_etf_atr(code: str, n: int = 14, method: str = "sma") -> dict:
    """对外主接口：传入ETF代码和ATR天数，返回最新一期ATR结果"""
    df = fetch_etf_daily(code, need_days=n)
    result = calc_atr(df, n=n, method=method)
    result["code"] = code
    return result


def main():
    ap = argparse.ArgumentParser(description="场内ETF ATR 计算工具")
    ap.add_argument("code", help="ETF代码，如 588000")
    ap.add_argument("days", type=int, nargs="?", default=14, help="ATR天数，默认14")
    ap.add_argument("--method", choices=["sma", "wilder"], default="wilder",
                     help="计算方式：sma=简单移动平均，wilder=威尔德平滑（默认）")
    args = ap.parse_args()

    try:
        result = get_etf_atr(args.code, n=args.days, method=args.method)
    except Exception as e:
        print(f"计算失败：{e}")
        sys.exit(1)

    print(f"\nETF代码: {result['code']}")
    print(f"最新交易日: {result['latest_date']}")
    print(f"最新收盘价: {result['latest_close']}")
    print(f"ATR({result['atr_n']}, {result['method']}): {result['atr']}")
    print(f"ATR%（相对收盘价）: {result['atr_pct']}%\n")


if __name__ == "__main__":
    for code in ['159326', '159667', '512100', '563300', '588000', '588200', '515880', '159732', '516510', '159852']:
        result = get_etf_atr(code, n=14, method='sma')
        # result1 = get_etf_atr('588000', n=14, method='wilder')
        print(result)
        # print(result1)

"""反向验证：全市场新高股次日表现"太强"时不开新仓（指标同 backtest_breadth_nh.py）
用法：python backtest/backtest_breadth_nh_rev.py > stock_result/breadth_nh_rev.log 2>&1"""
import backtest_breadth_nh as m


def cases():
    cs = [("实盘", None)]
    for col in ("cc", "oc"):
        for n in (3, 5, 10):
            for thr in (0.5, 1.0, 1.5):
                cs.append((f"{col} 近{n}日>{thr}% 不开", ("-" + col, n, -thr, 0)))
    return cs


if __name__ == "__main__":
    m.cases = cases
    m.OUT = "stock_result/breadth_nh_rev_summary.csv"
    m.main()

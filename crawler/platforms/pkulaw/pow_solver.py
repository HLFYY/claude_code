"""
腾讯 Turing 验证码提交时带的工作量证明题，来自 cap_union_prehandle 响应里的
comm_captcha_cfg.pow_cfg = {"prefix": "...#", "md5": "目标哈希"}。

已用真实抓包数据验证过算法（见 reverse-records/请求链路.md）：
在 dy-ele.js 里定位到 refreshWorkload()，本质就是本地爆破整数 N，
直到 md5(prefix + str(N)) == 目标md5，pow_answer = prefix + str(N)。
跟 tdc.js/JSVMP 完全无关，纯 Python 实现，不需要任何 JS。
"""
from __future__ import annotations

import hashlib
import time


def solve(prefix: str, target_md5: str, max_n: int = 2_000_000) -> tuple[str, int, int]:
    """返回 (pow_answer, nonce, calc_time_ms)。爆破失败(超过max_n)抛异常。"""
    start = time.monotonic()
    for n in range(max_n):
        candidate = f"{prefix}{n}"
        if hashlib.md5(candidate.encode()).hexdigest() == target_md5:
            calc_time_ms = int((time.monotonic() - start) * 1000)
            return f"{prefix}{n}", n, calc_time_ms
    raise RuntimeError(f"pow_solver: no nonce found under {max_n} for prefix={prefix!r} target={target_md5!r}")


if __name__ == "__main__":
    answer, nonce, ms = solve("c03144eec4874e11#", "558cafa46f92f9613d595e0a197139eb")
    print(f"answer={answer} nonce={nonce} calc_time_ms={ms}")
    assert answer == "c03144eec4874e11#149864"
    print("OK: matches known real capture")

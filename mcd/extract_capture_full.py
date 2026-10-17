#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 .mitm 全量提取 mcd.cn + fengkongcloud(数美) 的请求/响应，输出可读 txt，
并在末尾汇总每条 verifyCode/sms/send 的关键头(d/token/tid/st/nonce/biz/cookie)+响应码，便于 diff。

用法: /Users/houjie/venv/python3-forcrawl/bin/python extract_capture_full.py <xxx.mitm> [输出.txt]
"""
import sys
from mitmproxy import io
from mitmproxy.exceptions import FlowReadException

MITM = sys.argv[1]
OUT = sys.argv[2] if len(sys.argv) > 2 else MITM.rsplit('.', 1)[0] + '_full.txt'
HOST_KEEP = ('mcd.cn', 'fengkongcloud')
KEY_HEADERS = ['d', 'token', 'mcdtoken', 'tid', 'st', 'nonce', 'sv', 'biz_scenario', 'biz_from',
               'authorization', 'x-hmac-digest', 'x-mcd-sec-verify', 'cookie', 'x-b3-traceid',
               'br_interactive_uuid']


def body_text(content, headers, limit=2500):
    if not content:
        return '(空)'
    ctype = headers.get('content-type', '')
    if any(t in ctype for t in ('image', 'octet-stream', 'font')):
        return f'(二进制 {len(content)} 字节, content-type={ctype})'
    try:
        s = content.decode('utf-8')
    except Exception:
        return f'(非utf8 {len(content)} 字节)'
    return s if len(s) <= limit else s[:limit] + f'... (截断,共{len(s)}字符)'


def main():
    sends = []
    n = 0
    with open(MITM, 'rb') as f, open(OUT, 'w', encoding='utf-8') as out:
        try:
            for flow in io.FlowReader(f).stream():
                if not hasattr(flow, 'request'):
                    continue
                url = flow.request.pretty_url
                if not any(h in url for h in HOST_KEEP):
                    continue
                n += 1
                req, resp = flow.request, flow.response
                out.write('=' * 100 + '\n')
                out.write(f"[{n}] {req.timestamp_start:.3f} {req.method} {url}\n")
                out.write('-' * 100 + '\n请求头:\n')
                for k, v in req.headers.items():
                    out.write(f"  {k}: {v}\n")
                out.write('请求体:\n  ' + body_text(req.content, req.headers).replace('\n', '\n  ') + '\n')
                if resp:
                    out.write('-' * 100 + f'\n响应状态码: {resp.status_code}\n响应体:\n  ')
                    out.write(body_text(resp.content, resp.headers).replace('\n', '\n  ') + '\n')
                out.write('=' * 100 + '\n\n')

                # 收集 sms/send 用于汇总
                if 'verifyCode/sms/send' in url:
                    hd = {k: req.headers.get(k, '') for k in KEY_HEADERS}
                    rbody = body_text(resp.content, resp.headers, 300) if resp else ''
                    sends.append((n, req.timestamp_start, hd, resp.status_code if resp else '-', rbody))
        except FlowReadException as e:
            out.write(f"读取中断: {e}\n")

        # 汇总 diff 区
        out.write('\n\n' + '#' * 100 + '\n# verifyCode/sms/send 汇总（逐次对比 d/token/tid/st/nonce/biz/cookie）\n' + '#' * 100 + '\n')
        for idx, ts, hd, sc, rbody in sends:
            out.write(f"\n--- 第{len(sends) and [s[0] for s in sends].index(idx)+1}次 send  [flow {idx}]  ts={ts:.3f}  http={sc} ---\n")
            for k in KEY_HEADERS:
                if hd.get(k):
                    out.write(f"  {k}: {hd[k]}\n")
            out.write(f"  响应: {rbody}\n")

    print(f"✅ 提取完成: {OUT}")
    print(f"   共 {n} 条 mcd/数美 请求，其中 sms/send {len(sends)} 条")
    for idx, ts, hd, sc, rbody in sends:
        print(f"   send[flow {idx}] http={sc} d={'有' if hd.get('d') else '无'} token={hd.get('token','')[:12]} tid={hd.get('tid','')} -> {rbody[:60]}")


if __name__ == '__main__':
    main()

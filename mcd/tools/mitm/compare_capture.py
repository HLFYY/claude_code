#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
compare_capture.py —— 把 mitm 抓到的真实请求(.req 文件)和 mcd_api.build_headers 的构造逐头对比,
                      找出我们和真机不一致的地方(头缺失/多余/值不同/签名头差异)。

用法:
  python compare_capture.py /tmp/mcd_cap               # 对比目录下所有 api2/api.mcd.cn 的 .req
  python compare_capture.py /tmp/mcd_cap/0061_xxx.req  # 对比单个

只对比请求头与 body 存在性; body 的字段级(加密手机号等)需人工看。
"""
import os, sys, re, json
sys.path.insert(0, '/Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd_project')
import mcd_api

# 真机签名头集合(authorization 里 SignedHeaders),用于标注哪些头改了会导致签名失效
SIGNED = {'ct', 'language', 'p', 'sid', 'sv', 'token', 'v', 'x-mcd-gw-v'}
# 只关心这些头的值对比(其余如 nonce/st/traceid/authorization 每次变,不比值)
CMP_KEYS = ['ct', 'language', 'p', 'sv', 'v', 'x-mcd-gw-v', 'user-agent',
            'biz_scenario', 'biz_from', 'mcdtoken', 'x-mcd-sec-verify', 'content-type']

def parse_req(path):
    txt = open(path, encoding='utf-8', errors='replace').read()
    lines = txt.splitlines()
    # 跳过开头的 # 注释行（如 # captured_at: ...）
    i = 0
    while i < len(lines) and lines[i].startswith('#'):
        i += 1
    first = lines[i] if i < len(lines) else ''
    m = re.match(r'(\w+)\s+(\S+)', first)
    method, url = (m.group(1), m.group(2)) if m else ('?', '?')
    headers = {}
    i += 1
    while i < len(lines) and lines[i].strip() != '':
        if ':' in lines[i]:
            k, v = lines[i].split(':', 1)
            headers[k.strip().lower()] = v.strip()
        i += 1
    body = '\n'.join(lines[i+1:]) if i+1 < len(lines) else ''
    return method, url, headers, body

def mcd_headers_for(method, path, body):
    # 用一个占位 token 构造我们的头(只比结构/固定值, token/签名每次不同)
    biz_s, biz_f = ('500', '1011') if 'verifyCode' in path or 'login/mobile' in path else ('102', '1006')
    return mcd_api.build_headers('TOKEN_PLACEHOLDER', sid='', body=body or None,
                                 method=method, path=path, biz_scenario=biz_s, biz_from=biz_f)

def compare_one(path):
    method, url, real, body = parse_req(path)
    pu = re.sub(r'^https?://[^/]+', '', url).split('?')[0]
    host = re.match(r'https?://([^/]+)', url)
    host = host.group(1) if host else ''
    if 'mcd.cn' not in host or '/bff/' not in pu:
        return  # 只比 bff 接口
    ours = {k.lower(): str(v) for k, v in mcd_headers_for(method, pu, body).items()}
    print(f"\n===== {method} {host}{pu} =====")
    diffs = []
    for k in CMP_KEYS:
        rv, ov = real.get(k), ours.get(k)
        if rv is None and ov is None:
            continue
        if rv != ov:
            tag = ' [签名头!]' if k in SIGNED else ''
            diffs.append(f"  ✗ {k}{tag}: 真机={rv!r}  我们={ov!r}")
    # 真机有我们没有的头(排除每次变化的)
    skip = {'authorization', 'x-hmac-digest', 'st', 'nonce', 'x-b3-traceid', 'x-b3-spanid',
            'br_interactive_uuid', 'host', 'accept-encoding', 'content-length', 'routingid',
            'meddyid', 'language', 'tid', 'd'}
    extra = [h for h in real if h not in ours and h not in skip]
    missing = [h for h in ours if h not in real and h not in skip]
    if diffs:
        print("值不同:"); [print(d) for d in diffs]
    if extra:
        print("  真机有/我们无:", extra, "→", {h: real[h][:40] for h in extra})
    if missing:
        print("  我们有/真机无:", missing)
    if not (diffs or extra or missing):
        print("  ✓ 头一致")
    if body:
        print("  body:", body[:120])

def main():
    target = sys.argv[1] if len(sys.argv) > 1 else '/tmp/mcd_cap'
    if os.path.isdir(target):
        for f in sorted(os.listdir(target)):
            if f.endswith('.req'):
                compare_one(os.path.join(target, f))
    else:
        compare_one(target)

if __name__ == '__main__':
    main()

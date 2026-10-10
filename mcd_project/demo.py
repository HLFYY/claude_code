from urllib.parse import quote
import requests
from proxy_pool import ProxyPool
from mcd_api import build_headers, API_BASE


def add_proxy():
    print("=" * 80)
    print("添加代理到代理池")
    print("=" * 80)

    # 初始化代理池
    pool = ProxyPool()

    # 代理列表
    proxies = [
        {
            'host': '61.155.141.105',
            'port': 20080,
            'username': 'jygdGVUi',
            'password': 'd5xP8koC0PHz',
            'protocol': 'http',
            'proxy_id': 'mcd_proxy_001'
        },
        {
            'host': '61.155.141.105',
            'port': 20081,
            'username': 'jygdGVUi',
            'password': 'd5xP8koC0PHz',
            'protocol': 'http',
            'proxy_id': 'mcd_proxy_002'
        },
        {
            'host': '61.155.141.105',
            'port': 20082,
            'username': 'jygdGVUi',
            'password': 'd5xP8koC0PHz',
            'protocol': 'http',
            'proxy_id': 'mcd_proxy_003'
        }
    ]

    print(f"\n准备添加 {len(proxies)} 个代理...\n")

    for proxy in proxies:
        result = pool.add_proxy(
            host=proxy['host'],
            port=proxy['port'],
            username=proxy['username'],
            password=proxy['password'],
            protocol=proxy['protocol'],
            proxy_id=proxy['proxy_id']
        )

    # 显示代理池状态
    print("\n" + "=" * 80)
    print("代理池当前状态")
    print("=" * 80)

    all_proxies = pool.get_all_proxies()
    print(f"\n总计: {len(all_proxies)} 个代理\n")

    for p in all_proxies:
        print(f"  [{p['id']}]")
        print(f"    - 地址: {p['host']}:{p['port']}")
        print(f"    - 状态: {p['status']}")
        print(f"    - 绑定账号: {p['bound_accounts']}")
        print(f"    - 成功/失败: {p['success_count']}/{p['fail_count']}")
        print()

    print("=" * 80)
    print("✅ 完成")
    print("=" * 80)


if __name__ == '__main__':
    headers = {
        'Host': 'api.mcd.cn',
        'biz_from': '1003',
        'biz_scenario': '200',
        'br_interactive_uuid': 'b782407b-b8e4-4720-ab57-5aa3a74c900e',
        'user-agent': 'mcdonald_Android/7.0.41.0 (Android)',
        'v': '7.0.41.0',
        'ct': '102',
        'p': '102',
        'token': 'ac05229f37f344e582a330cef7a580d1',
        'sid': '667563e750f06c6b2f53c5621afc57d2_',
        'language': 'cn',
        'x-b3-traceid': 'D571D93A7EC94592A4113FA92DFDFD08',
        'x-b3-spanid': '2DBBCFBCEDB34A65',
        'routingid': '53',
        'st': '1790258594',
        'nonce': '1790258594662477851',
        'tid': '00003TuN',
        'meddyid': 'MEDDY189945313626490653',
        'mcdtoken': 'ac05229f37f344e582a330cef7a580d1',
        'd': 'BaBgFpegFWhM61GKd7F4HAmvLP3sDrUqmYrHkqbENk6PpuCGTjZPElWlL194YaZNYSQVsZfq3Xow+bms61Z+qqQ==',
        'authorization': 'hmac-auth-v1#HJ7YLqOY06F61FPEhF7H#w63rPL/RAsOlgb+i4hvUMoSD1cfRagyiz8CoJA14Y2I=#hmac-sha256#Thu, 24 Sep 2026 14:03:14 GMT#ct;language;p;sid;sv;token;v;x-mcd-gw-v',
        'sv': 'v4',
        'x-hmac-digest': 'HaZ2dJQa+Mwfwupi5FQS4eF6niZRs02Lhke4LEek10U=',
        'x-mcd-gw-v': '1',
        'content-type': 'application/json; charset=UTF-8',
    }

    body = '{"beCode":"","beType":0,"canReverse":false,"cartType":"1","changeAddress":1,"channelCode":"03","daypartCode":"6","orderMode":"0","orderType":1,"pickupTimeType":"","pinId":"","storeCode":"1450745","storeName":"","supportGroupMealPromotion":false}'

    params = None

    path = '/bff/cart/carts/empty'

    # 构建请求头
    headers_par = build_headers(
        headers['token'],
        sid=headers['sid'],
        body=body,
        method='PUT',
        path=path,
        query_params=params
    )
    headers_par['Host'] = 'api.mcd.cn'


    # 发送请求
    url = API_BASE + path
    print(url)
    response = requests.put(url, headers=headers_par, params=params, data=body, timeout=10)
    # response = requests.get('https://api.mcd.cn/bff/store/stores', headers=headers, params=params)

    print(response.text)
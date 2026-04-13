import time
import requests
import execjs

if __name__ == '__main__':
    # ts = str(int(time.time())*1000)
    # print(ts)
    # js_file = './demo16.js'
    # comp = execjs.compile(open(js_file).read())
    # sign = comp.call('generateM', ts)
    # print
    import requests

    cookies = {
        'gfkadpd': '1522,33666',
        's_v_web_id': 'verify_mn56imqx_ZyggwjJx_ep2k_4agD_9f6L_Mggxms8CLvfi',
        'ttwid': '1%7CUNwcdP7hrvGE4tomt6EkcOW2fvwDUGSo0wn9_WJBAHI%7C1774403447%7Cd93cf35820f1eaaaabba2b81eb5649e60fbdaf98627e22ab67c1b18bfa05fbe8',
    }

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://haohuo.jinritemai.com',
        'Referer': 'https://haohuo.jinritemai.com/ecommerce/trade/detail/index.html?id=3688079129325994068&origin_type=604',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    data = {
        'origin_type': '604'
    }

    response = requests.post('https://haohuo.jinritemai.com/ecom/product/detail/h5/sc/', headers=headers,
                             cookies=cookies, data=data)
    print(res)
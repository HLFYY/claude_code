import base64
import hashlib
import hmac

import requests


def generate_authorization(headers, date, accesskey, secret_key):
    """生成 authorization 签名"""
    # 按照固定顺序拼接参与签名的请求头
    header_keys = ["ct", "language", "p", "sid", "sv", "token", "v", "x-mcd-gw-v"]

    # 消息格式待验证（可能有多种方式）
    parts = [f"{k}={headers[k]}" for k in header_keys]
    message = "&".join(parts)
    # 或者：message = f"accesskey={accesskey}&date={date}&" + "&".join(parts)

    signature = hmac.new(
        secret_key.encode(),
        message.encode(),
        hashlib.sha256
    ).digest()

    sig_b64 = base64.b64encode(signature).decode()

    # 组装完整的 authorization
    return f"hmac-auth-v1#{accesskey}#{sig_b64}#hmac-sha256#{date}#{'ct;language;p;sid;sv;token;v;x-mcd-gw-v'}"


def generate_digest(body, secret_key):
    """生成 x-hmac-digest 签名"""
    signature = hmac.new(
        secret_key.encode(),
        body.encode('utf-8'),
        hashlib.sha256
    ).digest()

    return base64.b64encode(signature).decode()

def menu():
    import requests

    headers = {
        'Host': 'api2.mcd.cn',
        'biz_scenario': '102',
        'biz_from': '1006',
        # 'br_interactive_uuid': '9813dd30-d881-4ffd-b337-d82ea13e5bf2',
        'user-agent': 'mcdonald_Android/7.0.41.0 (Android)',
        'v': '7.0.41.0',
        'ct': '102',
        'p': '102',
        'token': 'f0f2d9b33e604e1997f7069d2f3c37a1',
        'sid': '',
        'language': 'cn',
        # 'x-b3-traceid': '43743D1D3E904D4BBA1B671C82077AD3',
        # 'x-b3-spanid': '42F98B1274F14099',
        'routingid': '',
        # 'st': '1789366216',
        # 'nonce': '1789366216241817279',
        # 'tid': '00003TuN',
        'meddyid': '',
        'mcdtoken': 'f0f2d9b33e604e1997f7069d2f3c37a1',
        # 'd': 'BJYYc2W5mqxc6qsaeq35EuW9SstVVn/MNq8Lyg+DMFFomovLHoSbVViH1XilKVkaFyHvUjb3i8mU0EunlieM0kQ==',
        'authorization': 'hmac-auth-v1#HJ7YLqOY06F61FPEhF7H#hR+JQN0FwcJxpckXfe5p5wXm9Uc9VSbBP/t5+WOXTrU=#hmac-sha256#Mon, 14 Sep 2026 06:10:16 GMT#ct;language;p;sid;sv;token;v;x-mcd-gw-v',
        'sv': 'v4',
        'x-hmac-digest': 'UpiBsa1pBaUKD+QfXUM6sC73wuZaR34SZzChjfuZIHQ=',
        'x-mcd-gw-v': '1',
        'if-modified-since': 'Mon, 14 Sep 2026 03:00:15 GMT',
    }

    params = (
        ('storeCode', '1450688'),
        ('orderType', '2'),
        ('beCode', '145068802'),
        ('beType', '2'),
        ('orderMode', '0'),
        ('pinId', ''),
        ('dayPartCode', '8'),
    )

    response = requests.get('https://api2.mcd.cn/bff/spc/menu', headers=headers, params=params)
    # response = requests.get('https://api2.mcd.cn/bff/spc/menu?storeCode=1450688&orderType=2&beCode=145068802&beType=2&orderMode=0&pinId=&dayPartCode=8', headers=headers)
    print(response.text)

def menu_login():
    import requests

    headers = {
        'Host': 'api.mcd.cn',
        'biz_scenario': '102',
        'biz_from': '1009',
        # 'br_interactive_uuid': '4c6c4c08-c363-4d91-9a38-d2369ab888ef',
        'user-agent': 'mcdonald_Android/7.0.41.0 (Android)',
        'v': '7.0.41.0',
        'ct': '102',
        'p': '102',
        'token': '749adb7088124d29af47b74f52bed1b5',
        'sid': 'de03f6c78eda9303d83025f3de2f90fb_',
        'language': 'cn',
        # 'x-b3-traceid': 'FCCCF3B3255343B58158F43B8640516D',
        # 'x-b3-spanid': 'D0F8E4C3262C493D',
        # 'routingid': '57',
        # 'st': '1789698400',
        # 'nonce': '1789698400560797984',
        # 'tid': '00003TuN',
        # 'meddyid': 'MEDDY163321681473498257',
        # 'mcdtoken': '749adb7088124d29af47b74f52bed1b5',
        # 'd': 'BsoknQy1ULU2YF8NM1i20unJWG/5f/p9bjD5Aw3IGUOEFjlfk6onIMwI7YNnGdheX7KTzmIRlg1qhTeWpQ9oSIQ==',
        'authorization': 'hmac-auth-v1#HJ7YLqOY06F61FPEhF7H#5hIQZr95FxsYfAa6BxBG1Syd1dG4iWkJKdt9j9Qge6w=#hmac-sha256#Fri, 18 Sep 2026 02:26:40 GMT#ct;language;p;sid;sv;token;v;x-mcd-gw-v',
        'sv': 'v4',
        'x-hmac-digest': 'UpiBsa1pBaUKD+QfXUM6sC73wuZaR34SZzChjfuZIHQ=',
        'x-mcd-gw-v': '1',
    }

    params = (
        ('storeCode', '1450688'),
        ('orderType', '2'),
        ('beCode', '145068802'),
        ('beType', '2'),
        ('orderMode', '0'),
        ('pinId', ''),
        ('dayPartCode', '1'),
    )

    response = requests.get('https://api.mcd.cn/bff/spc/menu', headers=headers, params=params)

    # response = requests.get('https://api.mcd.cn/bff/spc/menu?storeCode=1450688&orderType=2&beCode=145068802&beType=2&orderMode=0&pinId=&dayPartCode=1', headers=headers)
    print(response.text)

def add_cart():
    import requests

    headers = {
        'Host': 'api.mcd.cn',
        'biz_scenario': '102',
        'biz_from': '1007',
        # 'br_interactive_uuid': '6c370c8d-6051-4625-8c9d-c881ed62ad7b',
        'user-agent': 'mcdonald_Android/7.0.41.0 (Android)',
        'v': '7.0.41.0',
        'ct': '102',
        'p': '102',
        'token': '749adb7088124d29af47b74f52bed1b5',
        'sid': 'de03f6c78eda9303d83025f3de2f90fb_',
        'language': 'cn',
        # 'x-b3-traceid': '3543AE3918C14550A4D8BECEB2E1DEBA',
        # 'x-b3-spanid': '3C7EE750710E46D5',
        # 'routingid': '57',
        # 'st': '1789698183',
        # 'nonce': '1789698183471260207',
        # 'tid': '00003TuN',
        # 'meddyid': 'MEDDY163321681473498257',
        # 'mcdtoken': '749adb7088124d29af47b74f52bed1b5',
        # 'd': 'BsoknQy1ULU2YF8NM1i20unJWG/5f/p9bjD5Aw3IGUOEFjlfk6onIMwI7YNnGdheX7KTzmIRlg1qhTeWpQ9oSIQ==',
        # 'authorization': 'hmac-auth-v1#HJ7YLqOY06F61FPEhF7H#p6TiSxl7IDXdrsELfZVkK3+oApuiDMuArXOlcjme3Z8=#hmac-sha256#Fri, 18 Sep 2026 02:42:07 GMT#ct;language;p;sid;sv;token;v;x-mcd-gw-v',
        'authorization': 'hmac-auth-v1#HJ7YLqOY06F61FPEhF7H#+Nm6vyYJtf7HGbuu8zJ8IAqMM8SVx6NlY4V2zrlmLUM=#hmac-sha256#Fri, 18 Sep 2026 03:00:12 GMT#ct;language;p;sid;sv;token;v;x-mcd-gw-v',
        'sv': 'v4',
        'x-hmac-digest': 'Q/02KzzMH9pldZqohG+Rd1bi0XZBPCAr9jfvreqx3XM=',
        'x-mcd-gw-v': '1',
        'content-type': 'application/json; charset=UTF-8',
    }
    data = '{"beCode":"145068802","beType":2,"cartType":"1","channelCode":"03","dataSource":1,"daypartCode":"8","hasCustomized":false,"maxPurchaseQuantity":999,"orderMode":"0","orderType":2,"pickupTimeType":"","pinId":"","products":[{"animationId":"","cardId":"","code":"9900016011","comboItems":[{"animationId":"","comboProducts":[{"animationId":"","code":"516014","quantity":1}],"round":"4"},{"animationId":"","comboProducts":[{"animationId":"","code":"516013","quantity":1}],"round":"3"},{"animationId":"","comboProducts":[{"animationId":"","code":"521824","quantity":1}],"round":"2"},{"animationId":"","comboProducts":[{"animationId":"","code":"516020","quantity":1}],"round":"1"},{"animationId":"","comboProducts":[{"animationId":"","code":"901406","modification":{"values":[{"code":"102201","key":"0-1","quantity":1},{"code":"120512","key":"0-1","quantity":1}]},"quantity":1}],"round":"5"},{"animationId":"","comboProducts":[{"animationId":"","code":"901440","modification":{"values":[{"code":"100136","key":"0-1","quantity":1},{"code":"120512","key":"0-1","quantity":1}]},"quantity":1}],"round":"6"},{"animationId":"","comboProducts":[{"animationId":"","code":"903050","modification":{"values":[{"code":"200002","key":"0-1","quantity":1}]},"quantity":1},{"animationId":"","code":"903050","modification":{"values":[{"code":"200002","key":"0-1","quantity":1}]},"quantity":1}],"round":"7"}],"couponCode":"","couponId":"","gmAssistServiceCode":"","id":"1-9900016011","image":"https://menu-img.mcd.cn/pcm/prod/menu/ProductPool/product/MS_9900016011_600.png","name":"\u5821\u5821\u56E2\u5706\u53CC\u4EBA\u6876","quantity":1,"sequence":-1,"type":"7"}],"storeCode":"1450688","storeName":"","supportGroupMealPromotion":false}'
    # headers['x-hmac-digest'] = generate_digest(data, 'HJ7YLqOY06F61FPEhF7H')
    response = requests.put('https://api.mcd.cn/bff/cart/carts', headers=headers, data=data.encode('utf-8'))
    print(response.text)

if __name__ == '__main__':
    # menu()
    # menu_login()
    add_cart()
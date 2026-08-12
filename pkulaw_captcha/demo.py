import json

import requests
from lxml import etree

import eo_challenge

cookies = {
    'path': '/',
    '__tst_status': '538622729#',
    'EO_Bot_Ssid': '3036676096',
    'referer': 'https://www.pkulaw.com/law?way=listCrumbs',
    'pkulaw_v6_sessionid': 'v3sdxpcytaqmcu2dzhlnom5g',
    'Hm_lvt_8266968662c086f34b2a3e2ae9014bf8': '1785301988',
    'HMACCOUNT': 'A3EF5C87BEDDE6DE',
    'cookieUUID': 'cookieUUID_1785301988148',
    'xCloseNew': '30',
    'WEIXIN_APP_LOGIN_KEY': 'cb0321d9-0a15-4ee1-8da2-685468399f50',
    'CookieId': '73cfb5cec5e411275210e3f04fc0221e',
    'SUB': '04c364e0-ae99-4431-8209-36fbd0dce091',
    'preferred_username': 'email202607291659268327',
    'loginType': 'email',
    'session_state': '6170fe69-74fa-4af0-8306-487c3c524c72',
    'UserAuthAssetAid': '',
    'authormes': '4b6f2649495ec2fab0f7ed04de16db4d3f12edc66c78ad0b31df38baf97b4875d1e76ed39f0cfe46bdfb',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_penalty': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_case': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_law': 'false',
    'isTip_topSub': 'true',
    'div_display': 'none',
    'xClose': '29',
    'userislogincookie': 'always',
    'LoginAccount': 'email202607291659268327',
    'Hm_lpvt_8266968662c086f34b2a3e2ae9014bf8': '1785320989',
}

cookies = {
  "CookieId": "73cfb5cec5e411275210e3f04fc0221e",
  "SUB": "04c364e0-ae99-4431-8209-36fbd0dce091",
  "UserAuthAssetAid": "",
  "WEIXIN_APP_LOGIN_KEY": "13710d94-4ee4-4d0d-9415-c066a837cb32",
  "loginType": "email",
  "preferred_username": "email202607291659268327",
  "session_state": "87b4ce0f-54f4-402c-99ba-85c317916339",
  "route": "e9570c5e52a53da8",
  "AUTH_SESSION_ID": "87b4ce0f-54f4-402c-99ba-85c317916339.keycloak-deployment-564b695c67-h5tj9",
  "AUTH_SESSION_ID_LEGACY": "87b4ce0f-54f4-402c-99ba-85c317916339.keycloak-deployment-564b695c67-h5tj9",
  "KEYCLOAK_IDENTITY": "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJkNzg3MGJjNi0yMDY3LTQ3MjAtYWNmNC04MjRhZTIzMWFiZDAifQ.eyJleHAiOjE3ODc5MTU3MDcsImlhdCI6MTc4NTMyMzcwNywianRpIjoiZTkxMjI1ZjMtYTUwMS00YmU2LTgxN2EtNmNkYzJkZDI2NzA0IiwiaXNzIjoiaHR0cHM6Ly9jYXMucGt1bGF3LmNvbS9hdXRoL3JlYWxtcy9mYWJhbyIsInN1YiI6IjA0YzM2NGUwLWFlOTktNDQzMS04MjA5LTM2ZmJkMGRjZTA5MSIsInR5cCI6IlNlcmlhbGl6ZWQtSUQiLCJzZXNzaW9uX3N0YXRlIjoiODdiNGNlMGYtNTRmNC00MDJjLTk5YmEtODVjMzE3OTE2MzM5Iiwic3RhdGVfY2hlY2tlciI6ImM4SU1tMW1mb2RHVk9yV3QzLWJMdmMtLUhNcldHTlFZdzJ5c2U0cWNibmcifQ.oQwOSDwuew0xNJ2rGtxAOsiQba4kDS7TCEVs2nbZSP8",
  "KEYCLOAK_IDENTITY_LEGACY": "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJkNzg3MGJjNi0yMDY3LTQ3MjAtYWNmNC04MjRhZTIzMWFiZDAifQ.eyJleHAiOjE3ODc5MTU3MDcsImlhdCI6MTc4NTMyMzcwNywianRpIjoiZTkxMjI1ZjMtYTUwMS00YmU2LTgxN2EtNmNkYzJkZDI2NzA0IiwiaXNzIjoiaHR0cHM6Ly9jYXMucGt1bGF3LmNvbS9hdXRoL3JlYWxtcy9mYWJhbyIsInN1YiI6IjA0YzM2NGUwLWFlOTktNDQzMS04MjA5LTM2ZmJkMGRjZTA5MSIsInR5cCI6IlNlcmlhbGl6ZWQtSUQiLCJzZXNzaW9uX3N0YXRlIjoiODdiNGNlMGYtNTRmNC00MDJjLTk5YmEtODVjMzE3OTE2MzM5Iiwic3RhdGVfY2hlY2tlciI6ImM4SU1tMW1mb2RHVk9yV3QzLWJMdmMtLUhNcldHTlFZdzJ5c2U0cWNibmcifQ.oQwOSDwuew0xNJ2rGtxAOsiQba4kDS7TCEVs2nbZSP8",
  "KEYCLOAK_LOCALE": "zh-CN",
  "KEYCLOAK_REMEMBER_ME": "username:1558109546%40qq.com",
  "KEYCLOAK_SESSION": "fabao/04c364e0-ae99-4431-8209-36fbd0dce091/87b4ce0f-54f4-402c-99ba-85c317916339",
  "KEYCLOAK_SESSION_LEGACY": "fabao/04c364e0-ae99-4431-8209-36fbd0dce091/87b4ce0f-54f4-402c-99ba-85c317916339",
  "LoginAccount": "email202607291659268327",
  "authormes": "4b6f2649495ec2fab0f7ed04de16db4d3f12edc66c78ad0b31df38baf97b4875d1e76ed39f0cfe46bdfb",
  "dcf6577e-33a3-4a19-a2f0-656853d9c027_case": "false",
  "pkulaw_v6_sessionid": "sfiop1v0ykh3ql4wv5524ypt",
  "userislogincookie": "always",
# '__tst_status': '538622729#', 'EO_Bot_Ssid': '93519872'
}

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
}
session = requests.Session()
session.headers.update(headers)
session.cookies.update(cookies)
article_url = 'https://www.pkulaw.com/qikan/5c6347f6bc4c4866bdca50e0aff747f0bdfb.html'
response = eo_challenge.get_with_challenge_retry(session, article_url, headers=headers, timeout=15)
if eo_challenge.is_challenge_page(response):
    raise SystemExit(f"还是拿到 EdgeOne 挑战页，重试 {2} 次后仍未通过，可能触发了更严格的风控")

html_res = etree.HTML(response.text)
if not html_res.xpath('//*[@class="content"]'):
    print(response.text)
content = html_res.xpath('//*[@class="content"]')[0]
content_html = etree.tostring(content, encoding='unicode', method='html')
print(content_html)
with open('/Users/houjie/Downloads/pkulaw_detail.json', 'w') as f:
    f.write(json.dumps({'content': content_html}, ensure_ascii=False))
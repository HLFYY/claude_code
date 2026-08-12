import re
import time

import requests
import json


headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Appversion": "1.0.0",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/json;charset=UTF-8",
    # "Identification": "_b08f49408a3b11f1aac5af90dee1f103",
    "Origin": "https://law.wkinfo.com.cn",
    "Pragma": "no-cache",
    # "Referer": "https://law.wkinfo.com.cn/legislation/list?simple=%E6%B0%91%E6%B3%95%E5%85%B8",
    "UCV": "1",
    "UID": "1000984047",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    # "b3": "172f575dd376d88455a629c7f33dc7c2-04ed7088826eeb7a-1",
    "module;": "",
    # "traceparent": "00-172f575dd376d88455a629c7f33dc7c2-04ed7088826eeb7a-01",
    # "uber-trace-id": "172f575dd376d88455a629c7f33dc7c2:04ed7088826eeb7a:0:1"
}
cookies = {
    "UM_distinctid": "19fa6f3836c1121-0ed11f6aacc4fb8-19525631-16a7f0-19fa6f3836d18b3",
    "cna": "fa6a589f23ec48b28188ec58da05188b",
    "Hm_lvt_fecce484974a74c6d10f421b6d3bd395": "1785212439",
    "HMACCOUNT": "A3EF5C87BEDDE6DE",
    "identification": "%22_b08f49408a3b11f1aac5af90dee1f103%22",
    "loginType": "%22id%22",
    "username": "hj1558109546%40gmail.com",
    "check": "valid",
    "column-maxsize": "100",
    "contentBgcolor": "%22detail-white%22",
    "autologin": "true",
    "userInfoV5": "%7B%22username%22%3A%22hj1558109546%40gmail.com%22%2C%22password%22%3A%220x02000000229527637665b78659079b9fcff6debdea0227007ff5eb8833757bde3321164701bbfca5c8771fd1cb2b4087aaf8371f%22%7D",
    "cinfo": "%7B%22ex%22%3A1787830112379%2C%22t%22%3Afalse%2C%22v%22%3A%221.0%22%7D",
    "acw_tc": "b65ca39017852908702195550eb875cc5f47156a682786670b09da230ecd5a",
    "connect.sid": "s%3Acc0Ga5PSVEdDt0k5stRWwRPt2KJWuAk3.8aiHyGq8VQ%2BtpAdParebDLhWGZFIabIvK7bSEt%2BUKho",
    "userInfo": "%7B%22id%22%3A%221000984047%22%2C%22username%22%3A%22hj1558109546%40gmail.com%22%2C%22password%22%3A%220x02000000229527637665b78659079b9fcff6debdea0227007ff5eb8833757bde3321164701bbfca5c8771fd1cb2b4087aaf8371f%22%2C%22userType%22%3A%22normal%22%2C%22email%22%3A%22hj1558109546%40gmail.com%22%2C%22userLang%22%3A%22cn%22%2C%22userPageSize%22%3A25%2C%22isSend%22%3Atrue%2C%22sendLang%22%3A%22cn%22%2C%22recieveEmails%22%3A%5B%5D%2C%22groupName%22%3A%22law%22%2C%22libraryCode%22%3A%22law%2Ctaa%2Chr%2CHKBold%22%2C%22licences%22%3A1%2C%22telephone%22%3A%2217717295039%22%2C%22conf%22%3A%22%22%2C%22productVersion%22%3A%22boldV5%22%7D",
    "loginin": "true",
    "loginId": "fb6687ce700e4b6a9fa5f0ee7c350a5f",
    "Hm_lpvt_fecce484974a74c6d10f421b6d3bd395": "1785290898",
    "CNZZDATA1261306096": "462721143-1785212439-https%253A%252F%252Fwww.wkinfo.com.cn%252F%7C1785212687",
    "userConfig": "%7B%22moduleList%22%3A%5B%22case%22%5D%2C%22userStaffType%22%3A1%2C%22isIpUser%22%3A0%2C%22parentId%22%3A0%2C%22sourceSiteUrl%22%3Anull%2C%22sourceSiteName%22%3Anull%2C%22clientSource%22%3A%22%E8%87%AA%E4%B8%BB%E6%B3%A8%E5%86%8C%22%2C%22trial%22%3Atrue%2C%22expire%22%3Afalse%2C%22caseView%22%3Afalse%2C%22advancedlSearchExpires%22%3A2%2C%22proximateLatestExpires%22%3A48%2C%22proximateSearch%22%3Afalse%7D",
    "doc_type": "%22procuratorialCase%22"
}

cate = 'law.legislation'  # 法律法规
word = '刑法'
sort_list = [
    {
        "sortKey": "important",
        "sortDirection": "ASC"
    },
    {
        "sortKey": "score",
        "sortDirection": "DESC"
    },
    {
        "sortKey": "promulgatingDate",
        "sortDirection": "DESC"
    }
]
# cate = 'law.case'  # 裁判文书
# word = '法学生'
# sort_list = [
#     {
#         "sortKey": "score",
#         "sortDirection": "DESC"
#     },
#     {
#         "sortKey": "judgmentDate",
#         "sortDirection": "DESC"
#     }
# ]
cate = 'law.procuratorialCase'  # 检察文书
word = '刑法'
sort_list = [
    {
        "sortKey": "procuratorialDate",
        "sortDirection": "DESC"
    }
]

def detail(docId):
    url = f"https://law.wkinfo.com.cn/csi/document/{docId}/html"
    params = {
        "indexId": cate,
        "searchId": "",
        "print": "false",
        "fromType": "",
        "useBalance": "true",
        "module": ""
    }
    response = requests.get(url, headers=headers, cookies=cookies, params=params)
    return response

def notes(docId):
    # 包含了法律法规下内容里的法条释义和法条提示
    url = f'https://law.wkinfo.com.cn/csi/document/{docId}/notes'
    response = requests.get(url, headers=headers, cookies=cookies)
    return response


for i in range(1):
    url = "https://law.wkinfo.com.cn/csi/search"
    data = {
        "query": {
            "queryString": f"simple:(({word}))",
            "filterDates": [],
            "filterQueries": []
        },
        "searchScope": {
            "treeNodeIds": []
        },
        "relatedIndexQueries": [],
        "sortOrderList": sort_list,
        "pageInfo": {
            "limit": 100,
            "offset": i*100
        },
        "chargingInfo": {
            "useBalance": True
        },
        "otherOptions": {
            "requireLanguage": "cn",
            "relatedIndexEnabled": True,
            "groupEnabled": False,
            "smartEnabled": True,
            "buy": False,
            "summaryLengthLimit": 100,
            "synonymEnabled": True,
            "advanced": False,
            "isHideBigLib": 0,
            "relatedIndexFetchRows": 5,
            "proximateCourtID": "",
            "module": "",
            "correctEnabled": True,
            "mappingEnabled": True,
            "webSearchEnabled": True,
            "defaultSearch": False,
            "rankKeyword": ""
        },
        "indexId": cate
    }
    data = json.dumps(data, separators=(',', ':'))
    response = requests.post(url, headers=headers, cookies=cookies, data=data)

    # print(response.text)
    # print(response)
    try:
        documentList = response.json()["documentList"]
    except:
        print(response.text)
        break
    datas = documentList[:3] + documentList[-3:]
    for data in datas:
        docId = data['docId']
        title = re.sub(r'<[^>]+>', '', data['title'], 1000, re.S)
        print(docId, title)
        content = detail(docId)
        print(content.text)
        with open(f'/Users/houjie/Downloads/{cate}_detail.json', 'w') as f:
            f.write(content.text)
        time.sleep(2)
        # if cate in ['law.legislation']:
        #     notes = notes(docId)
        #     with open(f'/Users/houjie/Downloads/{cate}_notes.json', 'w') as f:
        #         f.write(notes.text)
        #     print(notes.text[:500])
        #     print(notes.text[-500:])
        print('=='*10, datas.index(data))
        time.sleep(3)
        break
    print(i, len(documentList))
    time.sleep(3)


''
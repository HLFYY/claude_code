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
    "identification": "%22_adafd6608a4311f1a9148f6478c591f9%22",
    "UM_distinctid": "19fa728673395c-0ce9c5b86230ff8-19525631-16a7f0-19fa72867341f17",
    "cna": "a69984055bc9487a9fcfc85af20786c0",
    "connect.sid": "s%3AEmeQu_ptQxCY__cgbuUY4JRxQ4PnqZYP.tuGmJvh8i%2BLVx7s0J%2BgCpFv%2BGAJOsjx6K0Bt5W5QT5g",
    "Hm_lvt_fecce484974a74c6d10f421b6d3bd395": "1785215871,1785228594",
    "HMACCOUNT": "69149A2C219F91AD",
    "acw_tc": "b65ca39017852331182988232e1248f14b486e96aeb671caf5e96168c139ce",
    "check": "valid",
    "loginType": "%22id%22",
    "autologin": "true",
    "username": "1565655612%40qq.com",
    "loginId": "208968013e184c98b34a5a5bbdfc23c2",
    "userInfo": "%7B%22id%22%3A%221000984267%22%2C%22username%22%3A%221565655612%40qq.com%22%2C%22password%22%3A%220x02000000cb50e5d47e7683783094f60f9a679bb659e8711155f6cd54c234c19a63f714c52c45da8b7e4515ad687be0358ae5a06a%22%2C%22email%22%3A%221565655612%40qq.com%22%2C%22userLang%22%3A%22cn%22%2C%22userPageSize%22%3A25%2C%22isSend%22%3Atrue%2C%22sendLang%22%3A%22cn-en%22%2C%22recieveEmails%22%3A%5B%5D%2C%22userType%22%3A%22normal%22%2C%22groupName%22%3A%22law%22%2C%22libraryCode%22%3A%22law%2Ctaa%2Chr%2CHKBold%22%2C%22licences%22%3A1%2C%22telephone%22%3A%2213032214030%22%2C%22conf%22%3A%22%7B%5C%22legislationViewType%5C%22%3A%5C%22list%5C%22%2C%5C%22legislationDetailType%5C%22%3A%5C%22complex%5C%22%2C%5C%22lawExpressViewType%5C%22%3A%5C%22list%5C%22%2C%5C%22mLegislationViewType%5C%22%3A%5C%22list%5C%22%2C%5C%22mlegislationDetailType%5C%22%3A%5C%22complex%5C%22%2C%5C%22mLawExpressViewType%5C%22%3A%5C%22list%5C%22%7D%22%2C%22wechatSiteInfo%22%3Anull%2C%22isVerify%22%3Afalse%2C%22oaUserInfo%22%3Anull%7D",
    "Hm_lpvt_fecce484974a74c6d10f421b6d3bd395": "1785233487",
    "CNZZDATA1261306096": "648474484-1785215871-%7C1785233487",
    "userInfoV5": "%7B%22username%22%3A%221565655612%40qq.com%22%2C%22password%22%3A%220x02000000cb50e5d47e7683783094f60f9a679bb659e8711155f6cd54c234c19a63f714c52c45da8b7e4515ad687be0358ae5a06a%22%7D",
    "cinfo": "%7B%22ex%22%3A1787825487077%2C%22t%22%3Afalse%2C%22v%22%3A%221.0%22%7D",
    "userConfig": "%7B%22moduleList%22%3A%5B%22case%22%5D%2C%22userStaffType%22%3A1%2C%22isIpUser%22%3A0%2C%22parentId%22%3A0%2C%22sourceSiteUrl%22%3Anull%2C%22sourceSiteName%22%3Anull%2C%22clientSource%22%3A%22%E8%87%AA%E4%B8%BB%E6%B3%A8%E5%86%8C%22%2C%22trial%22%3Atrue%2C%22expire%22%3Afalse%2C%22caseView%22%3Afalse%2C%22advancedlSearchExpires%22%3A2%2C%22proximateLatestExpires%22%3A48%2C%22proximateSearch%22%3Afalse%7D",
    "doc_type": "%22case%22"
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
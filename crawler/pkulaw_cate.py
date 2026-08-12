import time
import urllib

import requests
from lxml import etree

from platforms.pkulaw import detail_client

cookies = {
    'referer': 'https://www.pkulaw.com/pal/a3ecfd5d734f711dec40c3f62a95960681632407d599d03bbdfb.html?way=listView',
    'pkulaw_v6_sessionid': 'v3sdxpcytaqmcu2dzhlnom5g',
    'Hm_lvt_8266968662c086f34b2a3e2ae9014bf8': '1785301988',
    'HMACCOUNT': 'A3EF5C87BEDDE6DE',
    'cookieUUID': 'cookieUUID_1785301988148',
    'UserAuthAssetAid': '',
    'xClose': '29',
    'xCloseNew': '6',
    'WEIXIN_APP_LOGIN_KEY': '62421be0-1ce3-4808-9d8a-d839aa4a4ba7',
    'CookieId': '73cfb5cec5e411275210e3f04fc0221e',
    'SUB': '04c364e0-ae99-4431-8209-36fbd0dce091',
    'preferred_username': 'email202607291659268327',
    'loginType': 'email',
    'session_state': 'f4fb7514-e8cf-4a7c-adb8-54bcaf8ff14e',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_case': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_law': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_reference': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_english': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_journal': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_procuratorate': 'false',
    'dcf6577e-33a3-4a19-a2f0-656853d9c027_penalty': 'false',
    'userislogincookie': 'always',
    'LoginAccount': 'email202607291659268327',
    'authormes': '4b6f2649495ec2fab0f7ed04de16db4d3f12edc66c78ad0b31df38baf97b4875d1e76ed39f0cfe46bdfb',
    'isTip_topSub': 'true',
    'Hm_lpvt_8266968662c086f34b2a3e2ae9014bf8': '1785982178',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://www.pkulaw.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.pkulaw.com/case?way=topGuid',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

api_url = 'https://www.pkulaw.com/case/search/RecordSearch'


def cate_crawl(raw_data, page_data):
    raw_data.update(page_data)
    response = requests.post(api_url, headers=headers, cookies=cookies, data=raw_data)
    return response

def case():
    """
类目:0, 参照级别：指导性案例, 页码:0, 数量:100
类目:1, 参照级别：公报案例, 页码:0, 数量:100
类目:2, 参照级别：典型案例, 页码:0, 数量:100
类目:3, 参照级别：参阅案例, 页码:0, 数量:100
类目:4, 参照级别：经典案例, 页码:0, 数量:100
类目:5, 参照级别：法宝推荐, 页码:0, 数量:0
类目:6, 参照级别：普通案例, 页码:0, 数量:0
类目:7, 参照级别：应用案例, 页码:0, 数量:0
类目:8, 参照级别：评析案例, 页码:0, 数量:100
类目:9, 参照级别：优秀案例, 页码:0, 数量:100
类目:10, 参照级别：参考案例, 页码:0, 数量:100"""
    for i in [19]:
        # for CaseGrade in range(100):
        for CaseGrade in [4]:
            data = [
                ('Menu', 'case'),
                ('Keywords', ''),
                ('SearchKeywordType', 'Fulltext'),
                ('MatchType', 'Exact'),
                ('RangeType', 'Piece'),
                ('Library', 'pfnl'),
                ('ClassFlag', 'pfnl'),
                ('GroupLibraries', ''),
                ('QueryOnClick', 'False'),
                ('AfterSearch', 'False'),
                ('ComplexSearch', 'False'),
                ('PreviousLib', 'pfnl'),
                ('pdfStr', ''),
                ('pdfTitle', ''),
                ('IsSynonymSearch', 'false'),
                ('RequestFrom', ''),
                ('LastLibForChangeColumn', 'pfnl'),
                ('IsSearchProvision', 'False'),
                ('IsCustomSortSearch', 'False'),
                ('CustomSortExpression', ''),
                ('IsAdv', 'False'),
                ('ClassCodeKey', ',,,,,,,,,,,,,'),
                ('Aggs.CategoryIntegration', ''),
                ('Aggs.CaseGrade', '01'),
                ('Aggs.CaseClass', ''),
                ('Aggs.SubjectClassSpecialType', ''),
                ('Aggs.CourtGrade', ''),
                ('Aggs.LastInstanceCourt', ''),
                ('Aggs.TrialStep', ''),
                ('Aggs.DocumentAttr', ''),
                ('Aggs.LastInstanceDate', ''),
                ('Aggs.TrialStepCount', ''),
                ('Aggs.PenaltyCodes', ''),
                ('Aggs.WordNum', ''),
                ('Aggs.NoPublicReason', ''),
                ('Aggs.AnocsInfoList', ''),
                ('GroupByIndex', '3'),
                ('OrderByIndex', ''),
                ('ShowType', 'Default'),
                ('GroupValue', ''),
                ('TitleKeywords', ''),
                ('FullTextKeywords', ''),
                ('Pager.PageIndex', '0'),
                ('Pager.PageIndex', '0'),
                ('RecordShowType', 'List'),
                ('Pager.PageSize', '100'),
                ('QueryBase64Request', ''),
                ('VerifyCodeResult', ''),
                ('isEng', 'chinese'),
                ('OldPageIndex', ''),
                ('newPageIndex', ''),
                ('IsShowListSummary', ''),
                ('X-Requested-With', 'XMLHttpRequest'),
            ]
            page_data = {
                'Pager.PageIndex': str(i),
                'OldPageIndex': '' if i == 0 else str(i - 1),
                'Pager.PageSize': '100',
                'Aggs.CaseGrade': f'0{CaseGrade+1}' if CaseGrade < 9 else str(CaseGrade + 1),
            }
            response = cate_crawl(dict(data), page_data)
            html_str = etree.HTML(response.text)
            cate = html_str.xpath('//*[@class="search-condition-wrap"]//*[@class="crumb-select-item"]//text()')
            cate_str = ''.join([x1.strip() for x1 in cate if x1.strip()])
            list_items = html_str.xpath('//*[@class="list-wrap"]//*[@class="item"][.//input]|//*[@class="list-wrap"]//li[.//*[@title]]')
            print(f'类目:{CaseGrade}, {cate_str}, 页码:{i}, 数量:{len(list_items)}')
            if not list_items:
                print(html_str)
                continue
            for item in [list_items[0]]:
                url = item.xpath(f'.//h4/a[contains(@href, ".html")]/@href')[0]
                url = urllib.parse.urljoin(api_url, url)
                res = detail_client.view_detail_pooled_by_url(url, True)
                print(url, res['contentHtml'][:120].replace('\n', ' '))
            print(list_items[-1].xpath('.//text()'))
            if len(list_items) < 100:
                break
            time.sleep(1)

if __name__ == '__main__':
    case()
# data = [
#       ('Menu', 'reference'),
#       ('Keywords', ''),
#       ('SearchKeywordType', 'Title'),
#       ('MatchType', 'Exact'),
#       ('RangeType', 'Piece'),
#       ('Library', 'specialtopic'),
#       ('ClassFlag', 'specialtopic'),
#       ('GroupLibraries', ''),
#       ('QueryOnClick', 'False'),
#       ('AfterSearch', 'False'),
#       ('ComplexSearch', 'False'),
#       ('PreviousLib', 'specialtopic'),
#       ('pdfStr', ''),
#       ('pdfTitle', ''),
#       ('IsSynonymSearch', 'false'),
#       ('RequestFrom', ''),
#       ('LastLibForChangeColumn', 'specialtopic'),
#       ('IsSearchProvision', 'False'),
#       ('IsCustomSortSearch', 'False'),
#       ('CustomSortExpression', ''),
#       ('IsAdv', 'False'),
#       ('ClassCodeKey', ''),
#       ('Aggs.Subject', '025'),
#       ('Aggs.Category', ''),
#       ('Aggs.AuthorUnAnalyzed', ''),
#       ('Aggs.PublishDate', ''),
#       ('GroupByIndex', '0'),
#       ('OrderByIndex', ''),
#       ('ShowType', 'Default'),
#       ('GroupValue', ''),
#       ('TitleKeywords', ''),
#       ('FullTextKeywords', ''),
#       ('Pager.PageIndex', '0'),
#       ('Pager.PageIndex', '0'),
#       ('RecordShowType', 'List'),
#       ('Pager.PageSize', '100'),
#       ('QueryBase64Request', ''),
#       ('VerifyCodeResult', ''),
#       ('isEng', 'chinese'),
#       ('OldPageIndex', ''),
#       ('newPageIndex', ''),
#       ('IsShowListSummary', ''),
#       ('X-Requested-With', 'XMLHttpRequest'),
#     ]

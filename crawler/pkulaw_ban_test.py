import copy
import json
import re
import time
import urllib

from lxml import etree

from core.redis_client import get_client
from platforms.pkulaw import detail_client


def tb_share():
    import requests

    headers = {
        'Host': 'e.tb.cn',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en,zh-CN;q=0.9,zh;q=0.8',
        'priority': 'u=0, i',
    }

    # response = requests.get('https://e.tb.cn/h.8Uyd54nU3aaQX1e?tk=var2gCN92zR', headers=headers)
    response = requests.get('https://e.tb.cn/h.8eYnr1Mn4SfrJmc?tk=PqwSgEq6Z3M', headers=headers)
    print(response.text)
    goods_url = re.findall(r"var.?url.?=.?'(http.+?)';", response.text, re.S)
    print(goods_url)
    gid = re.findall(r"[\?\&]id=(\d+)", goods_url[0], re.S)
    print(gid)

import requests

cookies = {
    'pkulaw_v6_sessionid': 'ytkmn1gk4cf3o2dqwdlnkerc',
    'Hm_lvt_8266968662c086f34b2a3e2ae9014bf8': '1786079472',
    'HMACCOUNT': '3F7E7E784C3C4826',
    'cookieUUID': 'cookieUUID_1786079471805',
    'xCloseNew': '11',
    'WEIXIN_APP_LOGIN_KEY': 'a0bf8e73-ed00-4089-a0ca-880f45b01262',
    'CookieId': '8d5cf7d873ce843a8f923c5932cf5498',
    'SUB': 'fbf5b1e3-9826-4263-bc73-555041193e96',
    'preferred_username': 'email202607291854100885',
    'loginType': 'password',
    'session_state': '957af341-d15e-44c0-bad1-636a1c113e74',
    'UserAuthAssetAid': '',
    'authormes': '70c21c73e382fb72153cf8ffd8d15adc6545f3352b451ce8e241cd875fe63aafdc889e5a35b40a2fbdfb',
    '422d4def-0229-4c65-97c2-cfd89c676841_reference': 'false',
    '422d4def-0229-4c65-97c2-cfd89c676841_case': 'false',
    'isTip_topSub': 'true',
    '422d4def-0229-4c65-97c2-cfd89c676841_journal': 'false',
    '422d4def-0229-4c65-97c2-cfd89c676841_law': 'false',
    'chlOrderMemery': '0',
    '422d4def-0229-4c65-97c2-cfd89c676841_english': 'false',
    'userislogincookie': 'always',
    'LoginAccount': 'email202607291854100885',
    'fmtOrderMemery': '1',
    '422d4def-0229-4c65-97c2-cfd89c676841_procuratorate': 'false',
    'referer': 'https://www.pkulaw.com/law?way=topGuid',
    'Hm_lpvt_8266968662c086f34b2a3e2ae9014bf8': '1786346812',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://www.pkulaw.com',
    'Pragma': 'no-cache',
    'Referer': 'https://www.pkulaw.com/procuratorate?way=topGuid',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

def pkulaw_检查文书():
    data = {
        'library': 'procuratoratedoc',
        'Aggs': '{"Category":"","DocumentAttr":"","SutraCase":"","CourtGrade":"","LastInstanceCourt":"","LastInstanceDate":""}',
        'QueryBase64Request': '',
        'keyword': '',
        'advDic': '',
        'SearchInResult': '',
        'ClassFlag': 'procuratoratedoc',
        'ExtCondition': '',
        'KeywordType': 'Title',
        'MatchType': 'Exact'
    }

    response = requests.post('https://www.pkulaw.com/Aggregate/ClusterResult', headers=headers, cookies=cookies, data=data)
    cate_from = 'Category'
    cate1_len = 3
    cate_from = 'LastInstanceCourt'
    cate1_len = 2
    cate1, cate2 = [], []
    for data in response.json():
        if data['AggName'] != cate_from:
            continue
        for cate in data['Data']:
            if cate['pId'] == '-1' or len(cate['id']) == cate1_len:
                cate1.append([cate['name'], cate['id']])
            elif len(cate['pId']) == cate1_len:
                cate2.append(cate['id'])
    print(cate1)
    print(cate2)
    result = parse_cates(response.json())
    print(result)
    # count_list = []
    # for data in response.json():
    #     if data['AggName'] != cate_from:
    #         continue
    #     for cate in data['Data']:
    #         if cate['pId'] in cate2:
    #             count = int(re.findall(r'\((\d+)\)', cate['name'], re.S)[0])
    #             count_list.append(min(count, 4000))
    #
    # print(sum(count_list), count_list)

def pkulaw_行政执法():
    data = {
        'library': 'apy',
        'Aggs': '{"LawEnforceType":"","Category":"","PunishmentTarget":"","PunishmentTypeNew":"","EnforcementLevel":"","DepartCode":"","LawRegional":"","PunishmentDate":"","PunishmentAmount2":""}',
        'QueryBase64Request': '',
        'keyword': '',
        'advDic': '',
        'SearchInResult': '',
        'ClassFlag': 'apy',
        'ExtCondition': '',
        'KeywordType': 'Title',
        'MatchType': 'Exact'
    }
    response = requests.post('https://www.pkulaw.com/Aggregate/ClusterResult', headers=headers, cookies=cookies, data=data)
    cate_from = 'LawRegional'
    cate1_len = 2
    cate1, cate2 = [], []
    for data in response.json():
        if data['AggName'] != cate_from:
            continue
        for cate in data['Data']:
            if cate['pId'] == '-1' or len(cate['id']) == cate1_len:
                cate1.append([cate['name'], cate['id']])
            elif len(cate['pId']) == cate1_len:
                cate2.append(cate['id'])
    print(cate1)
    print(cate2)
    result = parse_cates(response.json())
    print(result)
    # count_list = []
    # for data in response.json():
    #     if data['AggName'] != cate_from:
    #         continue
    #     for cate in data['Data']:
    #         if cate['pId'] in cate2:
    #             count = int(re.findall(r'\((\d+)\)', cate['name'], re.S)[0])
    #             count_list.append(min(count, 4000))

    # print(sum(count_list), count_list)

def parse_cate_num(name):
    return int(re.findall(r'\((\d+)\)', name, re.S)[0])

def parse_cates(section, type_pass=None):
    result = []
    for section in section:
        type_name = section["AggName"]
        if type_name == type_pass:
            continue
        data = section.get("Data") or []
        parent_ids = {str(item["pId"]) for item in data}
        leaves = [item for item in data if str(item["id"]) not in parent_ids]
        more_cate = []
        more_num = 0
        total_num = 0
        for leaf in leaves:
            leaf_count = parse_cate_num(leaf['name'])
            total_num += leaf_count
            if leaf_count > 4000:
                more_cate.append(leaf)
                more_num += leaf_count - 4000
        tmp = {
            "type_name": type_name,
            "leaves": leaves,
            "leave_count": len(leaves),
            "more_cate": more_cate,
            "more_cate_count": len(more_cate),
            "total_num": total_num,
            "more_num": more_num,
            "crawl_num": total_num - more_num,
        }
        result.append(tmp)
    return result

def req_cate_arts(cate1, cate2, show_type, params):
    # cate1 菜单 一级栏目
    # cate2 二级栏目
    # OrderByIndex 0-发布时间倒序 1-发布时间顺序
    data = [
        ('Menu', cate1),
        ('Keywords', ''),
        ('SearchKeywordType', show_type),
        ('MatchType', 'Exact'),
        ('RangeType', 'Piece'),
        ('Library', cate2),
        ('ClassFlag', cate2),
        ('GroupLibraries', ''),
        ('QueryOnClick', 'False'),
        ('AfterSearch', 'False'),
        ('ComplexSearch', 'False'),
        ('PreviousLib', cate2),
        ('pdfStr', ''),
        ('pdfTitle', ''),
        ('IsSynonymSearch', 'true'),
        ('RequestFrom', ''),
        ('LastLibForChangeColumn', cate2),
        ('IsSearchProvision', 'False'),
        ('IsCustomSortSearch', 'False'),
        ('CustomSortExpression', ''),
        ('IsAdv', 'False'),
        ('ClassCodeKey', ''),
        # ('Aggs.SubjectWord', ''),
        # ('Aggs.Provinces', ''),
        # ('Aggs.NCID', ''),
        # ('Aggs.SubmitDate', '2022'),
        ('GroupByIndex', '0'),
        ('OrderByIndex', '0'),
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
    data = dict(data)
    data.update(params)
    response = requests.post(f'https://www.pkulaw.com/{cate1}/search/RecordSearch', headers=headers, data=data, proxies={'http': None, 'https': None})
    return response

def pick_best(lst, threshold):
    # 需要lst里total_num最大的那些元素 或 大于等于threshold的那些元素
    candidates = [x for x in lst if x["total_num"] >= threshold]
    if not candidates:
        max_total = max(x["total_num"] for x in lst)
        candidates = [x for x in lst if x["total_num"] == max_total]
    return candidates

def cate_all_reqs(cate1, cate2, show_type):
    data = {
        'Menu': cate1,
        'Keywords': '',
        'SearchKeywordType': show_type,
        'MatchType': 'Exact',
        'RangeType': 'Piece',
        'Library': cate2,
        'ClassFlag': cate2,
        'GroupLibraries': '',
        'IsSynonymSearch': 'true',
        'LastLibForChangeColumn': '',
        'ClassCodeKey': '',
        'IsClink': '',
        'IsAdv': 'False',
        'GroupValue': '',
        'QueryBase64Request': '',
        'RecordShowType': 'List',
        'FirstQueryKeywords': '',
        'FirstQueryKeywordType': show_type,
        'X-Requested-With': 'XMLHttpRequest'
    }
    response = requests.post('https://www.pkulaw.com/law/search/ClassSearch', headers=headers, cookies=cookies, data=data)
    html_res = etree.HTML(response.text)
    aggs_keys = [agg_name.replace("Aggs.", "") for agg_name in html_res.xpath('//*[@class="clearFilterItems"]//*[contains(@name, "Aggs")]/@name')]
    print(f'类目类型数:{len(aggs_keys)}, {aggs_keys}')

    aggs = {aggs_key: "" for aggs_key in aggs_keys}
    pdata = {
        'library': cate2,
        'Aggs': json.dumps(aggs),
        'QueryBase64Request': '',
        'keyword': '',
        'advDic': '',
        'SearchInResult': '',
        'ClassFlag': cate2,
        'ExtCondition': '',
        'KeywordType': show_type,
        'MatchType': 'Exact'
    }

    response = requests.post('https://www.pkulaw.com/Aggregate/ClusterResult', headers=headers, data=pdata)
    result = parse_cates(response.json())
    # 最大数据量、最少需要采集下级类目的文章数、最少需要采集下级类目的类目数、最少需要采集文章列表的类目数
    cur_cate = max(result, key=lambda x: (x["total_num"], -x["more_num"], -x["more_cate_count"], -x["leave_count"]))
    type_name = cur_cate["type_name"]
    req_cates = []
    req_more = []
    for cur_cate in cur_cate["leaves"]:
        cate_id = cur_cate["id"]
        cate_name = cur_cate["name"]
        cur_count = parse_cate_num(cate_name)
        if cur_count <= 4000:
            req_cate = dict(
                aggs={type_name: cate_id},
                aggs_count=1,
                cate_cur=cur_cate,
                cur_count=cur_count
            )
            req_cates.append(req_cate)
        else:
            aggs_data = copy.deepcopy(aggs)
            aggs_data[type_name] = cate_id
            pdata['Aggs'] = json.dumps(aggs_data)
            print(pdata)
            response2 = requests.post('https://www.pkulaw.com/Aggregate/ClusterResult', headers=headers, data=pdata)
            result_2 = parse_cates(response2.json(), type_pass=type_name)
            print(result_2)
            resul_parse2 = pick_best(result_2, parse_cate_num(cate_name))
            cur_cate2 = max(resul_parse2, key=lambda x: (-x["more_num"], -x["more_cate_count"], -x["leave_count"]))
            print(cur_cate2)
            type_name2 = cur_cate2["type_name"]
            for cur_cate2 in cur_cate2["leaves"]:
                cate2_id = cur_cate2["id"]
                cate2_name = cur_cate2["name"]
                cur_count2 = parse_cate_num(cate2_name)
                req_cate2 = dict(
                    aggs={type_name: cate_id, type_name2: cate2_id},
                    aggs_count=2,
                    cate1=cur_cate,
                    cur_cate=cur_cate2,
                    cur_count=cur_count2
                )
                req_cates.append(req_cate2)
                if cur_count2 > 4000:
                    req_more.append(req_cate2)
    return req_cates, aggs_keys

def cate_art_all():
    # 1、获取栏目下的类目分类
    cate1, cate2 = 'law', 'news'
    show_type = 'Title'  # FullText-全文， Title-标题
    fname = f'pkulaw_{cate1}_{cate2}_all.txt'
    with open(fname) as f:
        req_cates = json.loads(f.read().strip())
    aggs_keys = ['SubjectWord', 'Provinces', 'NCID', 'SubmitDate']
    # req_cates, aggs_keys = cate_all_reqs(cate1, cate2, show_type)
    # with open(fname, 'w') as f:
    #     f.write(json.dumps(req_cates))
    print(req_cates)
    print(len(req_cates))
    api_url = 'https://www.pkulaw.com/case/search/RecordSearch'
    r = get_client()

    for req_cate in req_cates:
        print(req_cate)
        total_count = req_cate["cur_count"]
        cate_cur = req_cate["cate_cur"]
        aggs = req_cate["aggs"]
        cate_over_key = f'pkulaw_cate_over_{cate1}_{cate2}'
        cate_key = ",".join([f"{key}:{aggs[key]}" for key in sorted(aggs.keys())])
        if r.sismember(cate_over_key, cate_key):
            print(f'已完成类目抓取:{req_cate}')
            continue
        total_page = total_count // 100 + 3
        url_set = set()
        for i in range(min(40, total_page)):
            if i < 20:
                page = i
                OrderByIndex = 0
            else:
                page = i - 20
                OrderByIndex = '1'
            page_data = {
                'Pager.PageIndex': str(page),
                'OldPageIndex': '' if page == 0 else str(page - 1),
                'Pager.PageSize': '100',
                'OrderByIndex': OrderByIndex
            }
            for aggs_key in aggs_keys:
                page_data[f'Aggs.{aggs_key}'] = aggs.get(aggs_key, "")
            response = req_cate_arts(cate1, cate2, show_type, page_data)
            with open('cate_news_all.txt', 'w') as f:
                f.write(response.text)
            html_str = etree.HTML(response.text)
            cate = html_str.xpath('//*[@class="search-condition-wrap"]//*[@class="crumb-select-item"]//text()')
            cate_str = ''.join([x1.strip() for x1 in cate if x1.strip()])
            list_items = html_str.xpath('//*[@class="list-wrap"]//*[@class="item"][.//input]|//*[@class="list-wrap"]//li[.//*[@title]]')
            if not list_items:
                print('异常', response.text)
                return
            for item in list_items:
                url = item.xpath(f'.//h4/a[contains(@href, ".html")]/@href')[0]
                url = urllib.parse.urljoin(api_url, url)
                r.sadd(f'pkulaw_detail_task_{cate1}_{cate2}', url)
                url_set.add(url)
                res = detail_client.view_detail_pooled_by_url(url, is_login=False)
                # print(res)
                print(url, True if res else False)
                break

            print(f'类目:{cate_str}, 页码:{i}, 排序:{OrderByIndex}, 数量:{len(list_items)}, 总数:{len(url_set)}')
            if len(url_set) >= total_count:
                print(f'类目抓取完成:{req_cate}')
                r.sadd(cate_over_key, cate_key)
            # print(list_items[-1].xpath('.//text()'))
            if len(list_items) < 100:
                break
            time.sleep(2)


if __name__ == '__main__':
    # pkulaw_行政执法()
    # pkulaw_检查文书()
    cate_art_all()
#     datas = """刑事 (9873)
# 民事 (31011)
# 行政 (6454)
# 执行 (1086)
# 国家赔偿 (237)
#     """
#     count_list = []
#     count_raw = []
#     for cc in re.findall(r'\((\d+)\)', datas, re.S):
#         cc = int(cc)
#         if cc > 4000:
#             print(cc)
#         count_list.append(min(cc, 4000))
#         count_raw.append(cc)
#     print(count_list)
#     print(sum(count_list))
#     print(sum(count_raw))


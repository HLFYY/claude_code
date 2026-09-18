import copy
import hashlib
import json
import os
import random
import re
import sys
import time
import traceback
import urllib
from urllib.parse import quote

from lxml import etree

from core.logger import log
from core.proxi_ip import get_proxy_dict
from core.redis_client import get_client
from platforms.pkulaw import detail_client
from platforms.pkulaw.detail_client import cate_account_sesion

plat_name = 'pkulaw'

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

def req_cate_arts(cate1, cate2, show_type, params, is_login=False):
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
        ('IsSynonymSearch', 'False'),
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
    # proxy_url = 'http://127.0.0.1:11153'
    # proxies = {
    #     "http": proxy_url,
    #     "https": proxy_url,
    # }
    now = time.time()
    if is_login:
        proxies = {'http': None, 'https': None}
        identifier, session = cate_account_sesion('cate', 'list')
        log(plat_name, f'acc:{identifier}')
    else:
        # session会优先读取环境变量的代理，次优先级才是session.proxies设置的代理，故禁止 session 读取任何环境变量代理配置
        session = requests.Session()
        session.trust_env = False
        proxies = get_proxy_dict()
    # print(f'获取代理，准备请求:{time.time() - now}')
    response = session.post(f'https://www.pkulaw.com/{cate1}/search/RecordSearch', headers=headers, data=data, proxies=proxies, timeout=15)
    # print(f'请求完成:{time.time() - now}')
    return response

def pick_best(lst, threshold):
    # 需要lst里total_num最大的那些元素 或 大于等于threshold的那些元素
    candidates = [x for x in lst if x["total_num"] >= threshold]
    if not candidates:
        max_total = max(x["total_num"] for x in lst)
        candidates = [x for x in lst if x["total_num"] == max_total]
    return candidates

def cate_all_reqs(cate1, cate2, show_type):
    cate_aggs = {
        'specialtopic': {
            "Subject":"",
            "Category":"",
            "AuthorUnAnalyzed":"",
            "PublishDate":""
        }

    }
    if cate2 in cate_aggs:
        aggs_keys = cate2
    else:
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
        response = requests.post('https://www.pkulaw.com/law/search/ClassSearch', headers=headers, cookies=cookies, data=data, proxies=get_proxy_dict())
        html_res = etree.HTML(response.text)
        aggs_names = html_res.xpath('//*[@class="classtitle"]/text()')
        aggs_raws = html_res.xpath('//*[@class="clearFilterItems"]//*[contains(@name, "Aggs")]/@name')
        aggs_keys = {aggs_raw.replace("Aggs.", ""): aggs_names[aggs_raws.index(aggs_raw)] for aggs_raw in aggs_raws}
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

    response = requests.post('https://www.pkulaw.com/Aggregate/ClusterResult', headers=headers, data=pdata, proxies=get_proxy_dict())
    result = parse_cates(response.json())
    # 最大数据量、最少需要采集下级类目的文章数、最少需要采集下级类目的类目数、最少需要采集文章列表的类目数
    cur_cate = max(result, key=lambda x: (x["total_num"], -x["more_num"], -x["more_cate_count"], -x["leave_count"]))
    print([[x["type_name"], x["total_num"], -x["more_num"], -x["more_cate_count"], -x["leave_count"]] for x in result])
    log(plat_name, f'数量最多且溢出数量最少的一级筛选:{aggs_keys[cur_cate["type_name"]]}, 文章数量:{cur_cate["total_num"]}, 类目数量:{cur_cate["leave_count"]}, 溢出类目:{cur_cate["more_cate_count"]}, 溢出数量:{cur_cate["more_num"]}')
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
                cur_cate=cur_cate,
                cur_count=cur_count
            )
            req_cates.append(req_cate)
        else:
            aggs_data = copy.deepcopy(aggs)
            aggs_data[type_name] = cate_id
            pdata['Aggs'] = json.dumps(aggs_data)
            response2 = requests.post('https://www.pkulaw.com/Aggregate/ClusterResult', headers=headers, data=pdata, proxies=get_proxy_dict())
            result_2 = parse_cates(response2.json(), type_pass=type_name)
            resul_parse2 = pick_best(result_2, parse_cate_num(cate_name))
            cur_cate2 = max(resul_parse2, key=lambda x: (-x["more_num"], -x["more_cate_count"], -x["leave_count"]))
            print([[x["type_name"], x["total_num"], -x["more_num"], -x["more_cate_count"], -x["leave_count"]] for x in result_2])
            log(plat_name, f'数量最多且溢出数量最少的二级筛选:{cate_name}, {aggs_keys[cur_cate2["type_name"]]}, 文章数量:{cur_cate2["total_num"]}, 类目数量:{cur_cate2["leave_count"]}, 溢出类目:{cur_cate2["more_cate_count"]}, 溢出数量:{cur_cate2["more_num"]}')
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
    return req_cates, aggs_keys, req_more

def run(tname, tcates):
    chan_name = plat_name + f':{tname}'
    login_map = {
        'case_pfnl': True,
    }
    # 1、获取栏目下的类目分类, FullText-全文， Title-标题
    cate1, cate2, show_type = tcates
    page_max_num = 100
    is_login = login_map.get(f'{cate1}_{cate2}', False)
    cate_key = f'pkulaw_{cate1}_{cate2}'
    fname = f'{cate_key}_all.txt'
    if os.path.exists(fname):
        with open(fname) as f:
            req_datas = json.loads(f.read().strip())
        req_cates = req_datas['req_cates']
        aggs_keys = req_datas['aggs_keys']
        req_more = req_datas['req_more']
    else:
        req_cates, aggs_keys, req_more = cate_all_reqs(cate1, cate2, show_type)
        with open(fname, 'w') as f:
            f.write(json.dumps({'req_cates': req_cates, 'aggs_keys': aggs_keys, 'req_more': req_more}))
        # print(req_cates)
    r = get_client()
    cate_over_key = f'{cate_key}_cate_over'
    cate_failed_key = f'{cate_key}_cate_failed'
    detail_task_key = f'{cate_key}_detail_task'
    total_can_crawl, total_num = 0, 0
    for req_cate in req_cates:
        total_can_crawl += min(4000, req_cate['cur_count'])
        total_num += req_cate['cur_count']
    log(chan_name, f'is_login:{is_login}, 类目总数:{len(req_cates)}, 类目已完成:{r.hlen(cate_over_key)}, 溢出类目:{len(req_more)}, 文章总数:{total_num}, 可采集文章数:{total_can_crawl}')
    if len(req_cates) == r.hlen(cate_over_key):
        cache_arts = r.scard(detail_task_key)
        log(chan_name, f'已完成, cate1:{cate1}, cate2:{cate2}, 采集文章数:{cache_arts}')
        return
    api_url = 'https://www.pkulaw.com/case/search/RecordSearch'
    err_count = 0
    for req_cate in req_cates:
        cate_index = req_cates.index(req_cate)
        total_count = req_cate["cur_count"]
        cur_cate = req_cate["cur_cate"]
        aggs = req_cate["aggs"]
        cate_key = ",".join([f"{key}:{aggs[key]}" for key in sorted(aggs.keys())])
        over_data = r.hget(cate_over_key, cate_key)
        if over_data:
            r.hdel(cate_failed_key, cate_key)
            over_data = json.loads(over_data)
            # log(chan_name, f'【{cate_index}】已完成类目抓取:{aggs}, over_data:{over_data}')
            # if over_data["crawl_rate"] == 0:
            #     print('删除失败的', over_data)
            #     r.hdel(cate_over_key, cate_key)
            continue
        failed_data = r.hget(cate_failed_key, cate_key)
        if failed_data:
            failed_data = json.loads(failed_data)
            if failed_data['crawl_count'] >= 1900:
                log(chan_name, f'抓取失败先过滤:{failed_data}')
                continue
        total_page = total_count // page_max_num + 5
        url_set = set()
        last_total = 0
        last_simple = 0
        log(chan_name, f'【{cate_index}】类目抓取开始:{cur_cate}')
        max_page = 44
        for i in range(min(max_page, total_page)):
            if i < max_page // 2:
                page = i
                OrderByIndex = '0'
            else:
                page = i - max_page // 2
                OrderByIndex = '1'
            page_data = {
                'Pager.PageIndex': str(page),
                'OldPageIndex': '' if page == 0 else str(page - 1),
                'Pager.PageSize': str(page_max_num),
                'OrderByIndex': OrderByIndex
            }
            for aggs_key in aggs_keys:
                page_data[f'Aggs.{aggs_key}'] = aggs.get(aggs_key, "")
            for _ in range(2):
                try:
                    if not is_login:
                        need_login = is_login
                    else:
                        need_login = True if total_count > 25 else False
                    response = req_cate_arts(cate1, cate2, show_type, page_data, need_login)
                    break
                except:
                    print(traceback.format_exc())
                    response = ''
                    time.sleep(3)
            if not response:
                err_count += 1
                if err_count > 10:
                    log(chan_name, f'连续失败:{err_count}次退出')
                    return
                continue
            # with open('cate_news_all.txt', 'w') as f:
            #     f.write(response.text)
            try:
                html_str = etree.HTML(response.text)
                cate = html_str.xpath('//*[@class="search-condition-wrap"]//*[@class="crumb-select-item"]//text()')
                cate_str = '|'.join([x1.strip() for x1 in cate if x1.strip()]).replace('：|', '：')
                list_items = html_str.xpath('//*[@class="list-wrap"]//*[@class="item"][.//input]|//*[@class="list-wrap"]//li[.//*[@title]]')
                if not list_items:
                    print('异常无文章数据')
                    # with open(f'{plat_name}_arts_err.txt', 'w') as f:
                    #     f.write(response.text)
                    err_count += 1
                    if err_count > 10:
                        log(chan_name, f'连续失败:{err_count}次退出')
                        return
                err_jiexi = 0
                for item in list_items:
                    url = item.xpath(f'.//h4/a[contains(@href, ".html")]/@href')[0]
                    info = item.xpath(f'.//*[@class="info"]/*[@class="text"]/text()')
                    # if list_items.index(item) == 0:
                    #     print(info)
                    url = urllib.parse.urljoin(api_url, url)
                    r.sadd(detail_task_key, url)
                    url_set.add(url)
                    # if list_items.index(item) == 0:
                    #     res = detail_client.view_detail_pooled_by_url(url, is_login=False)
                    #     # print(res)
                    #     print(url, True if res else False)
            except Exception as e:
                log(tname, f'解析异常:{e}')
                cate_str = ''
                list_items = []

            log(chan_name, f'【{cate_index}】类目:{cate_str}, i:{i}, page:{page}, 排序:{OrderByIndex}, 数量:{len(list_items)}, 总数:{len(url_set)}')
            # print(list_items[-1].xpath('.//text()'))
            if last_total == len(url_set) or len(list_items) < page_max_num:
                last_simple += 1
                if last_simple > 2 or len(url_set) >= min(total_count * 0.99, total_count - 10) or len(list_items) < 15:
                    break
            else:
                last_simple = 0
            last_total = len(url_set)
            # time.sleep(1)
        crawl_rate = round(len(url_set) / total_count, 2) * 100
        tmp = {'page': i, 'crawl_count': len(url_set), 'total_count': total_count, 'crawl_rate': crawl_rate}
        cache_arts = r.scard(detail_task_key)
        # if len(url_set) > 0 and (len(url_set) >= total_count * 0.95 or len(url_set) >= total_count - 5 or len(url_set) >= 3900):
        if len(url_set) >= total_count * 0.95 or len(url_set) >= total_count - 5 or len(url_set) >= 3900:
            log(chan_name, f'【{cate_index}】类目抓取完成:{cur_cate["name"]}, crawl_rate:{crawl_rate}, cache_arts:{cache_arts}')
            r.hset(cate_over_key, cate_key, json.dumps(tmp, ensure_ascii=False))
            r.hdel(cate_failed_key, cate_key)
        elif total_count < 20 and len(url_set) > 0:
            log(chan_name, f'【{cate_index}】类目抓取完成(少数量):{cur_cate["name"]}, crawl_rate:{crawl_rate}, cache_arts:{cache_arts}')
            r.hset(cate_over_key, cate_key, json.dumps(tmp, ensure_ascii=False))
            r.hdel(cate_failed_key, cate_key)
        else:
            log(chan_name, f'【{cate_index}】类目抓取失败:{cur_cate}, crawl_rate:{crawl_rate}, cache_arts:{cache_arts}')
            r.hset(cate_failed_key, cate_key, json.dumps(tmp, ensure_ascii=False))


if __name__ == '__main__':
    # pkulaw_行政执法()
    # pkulaw_检查文书()
    # 法律动态、案例报道 无cookie+代理ip能拿到全部文章url
    # 法律法规: 无cookie+普通账号 能抓取大部分 很多接口采集拿不到页面显示的数量，比如十几个接口只能拿到几条，可能需要付费账号访问才能看到部分文章

    tasks = {
        '法律动态': [['law', 'news', 'Title']],
        '合同范本': [['law', 'contract', 'Title']],
        '法律文书': [['law', 'fmt', 'Title']],
        # '中央法规': [['law', 'chl', 'Title']],
        # '地方法规': [['law', 'lar', 'Title']],
        # '立法资料': [['law', 'protocol', 'Title']],
        # '中外条约': [['law', 'eagn', 'Title']],
        # '外国法规': [['law', 'iel', 'Title']],

        '案例报道': [['case', 'pal', 'Fulltext']],
        # '司法案例': [['case', 'pfnl', 'Fulltext']],
        # '裁判规则': [['case', 'payz', 'Fulltext']],
        '专题参考': [['', 'specialtopic', 'Title']],
    }
    if len(sys.argv) == 2:
        tname = sys.argv[1]
    else:
        tname = ''
    for name, cates_list in tasks.items():
        if tname and name != tname:
            continue
        for cates in cates_list:
            run(name, cates)
            # page_data = {'Pager.PageIndex': '0', 'OldPageIndex': '', 'Pager.PageSize': 100, 'OrderByIndex': '0', 'Aggs.CategoryIntegration': '', 'Aggs.CaseGrade': '0302', 'Aggs.CaseClass': '', 'Aggs.SubjectClassSpecialType': '', 'Aggs.CourtGrade': '', 'Aggs.LastInstanceCourt': '', 'Aggs.TrialStep': '', 'Aggs.DocumentAttr': '', 'Aggs.LastInstanceDate': '', 'Aggs.TrialStepCount': '', 'Aggs.PenaltyCodes': '', 'Aggs.WordNum': '', 'Aggs.NoPublicReason': '', 'Aggs.AnocsInfoList': ''}
            # res = req_cate_arts(cates[0], cates[1], cates[2], page_data, True)
            # print(res)
            # break
    # print(get_proxy_dict())
    # 4bc9ae92f2ca7f76c7b31acc580f9ea0
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


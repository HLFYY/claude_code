from core.proxy_pool import add_proxy, list_proxies
from  core import request_logger
from  core import account_registry
from core.quota_tracker import remaining

if __name__ == '__main__':
    # for i in range(3):
    #     res = add_proxy('byjsnode133.vpsnb.net', f'1200{i+1}', '20250617hxj', 'z62aq50hfxaw')
    #     print(res)
    # print(list_proxies())
    plat = 'pkulaw'
    accs = account_registry.list_all_accounts(plat)
    for acc in accs:
        rs = remaining(plat, acc['email'], 'cate', 'list', 80000)
        print(acc['email'], rs)



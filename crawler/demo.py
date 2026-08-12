from core.proxy_pool import add_proxy, list_proxies
from  core import request_logger
from  core import account_registry

if __name__ == '__main__':
    # for i in range(3):
    #     res = add_proxy('byjsnode133.vpsnb.net', f'1200{i+1}', '20250617hxj', 'z62aq50hfxaw')
    #     print(res)
    # print(list_proxies())
    print(account_registry.list_all_accounts("pkulaw"))


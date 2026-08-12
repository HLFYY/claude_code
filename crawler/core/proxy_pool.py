"""
固定代理池，账号绑定按平台维度分别管理。

为什么账号一旦分配到代理就终身绑定：登录 session 的 cookie 会在账号整个
生命周期内被大量请求复用，session 中途换 IP 既是明显的风控红旗，实际上也
有搞断 WAF cookie 握手的风险。所以绑定只在注册时发生一次，之后这个账号
每次登录/搜索/详情请求都自动复用同一个代理（各平台自己的登录代码会去查
这个绑定关系）。

Redis 数据结构，全部按 `platform` 分区，这样以后同一批物理代理可以被多个
平台共用，而各平台的绑定账号数、上限都是独立统计的：

  proxy:{proxy_id}                    Hash —— host/port/username/password
                                       （不按平台分区：代理资源本身是共享的，
                                       只有它的"绑定关系"是按平台分区的）
  proxies_by_load:{platform}          ZSET —— score=绑定账号数，member=proxy_id。
                                       挑"负载最少且没超上限"的代理只需要一次
                                       ZRANGEBYSCORE 调用。
  account_proxy:{platform}:{email}    String —— 这个账号在这个平台上绑定的
                                       proxy_id 是哪个。

负载计数只在 unbind_account() 里减少，账号过期时不会有任何东西自动调用它——
account_registry.sweep(platform) 才是那个周期性（每天/每几天一次）的任务，
负责释放那些已经不是 ACTIVE 状态的账号占用的槽位，这样"绑定账号数"反映的是
*有效*账号，而不是历史上注册过的全部账号。要记得定期跑这个任务，否则代理池
会显得比实际更"满"。

代理池为空或所有代理都满了的时候，这里没有"退化成直连"的兜底逻辑——
pick_for_new_account() 会直接返回 None，调用方应该直接报错失败，而不是
悄悄地在没有固定 IP 的情况下继续注册账号，那样会让这个模块的设计目的
形同虚设。
"""
from __future__ import annotations

from .redis_client import get_client, k


def _find_by_host_port(host: str, port: int) -> str | None:
    for proxy_id in list_proxy_ids():
        proxy = get_proxy(proxy_id)
        if proxy and proxy.get("host") == host and str(proxy.get("port")) == str(port):
            return proxy_id
    return None


def add_proxy(host: str, port: int, username: str | None = None, password: str | None = None) -> str:
    """登记一个代理资源，按 host+port 去重：已经存在同一个 host+port 就直接复用那个
    proxy_id（顺便刷新 username/password，以防同一个端口后来换了账密），不存在就用
    一个自增计数器（`proxy_id_seq`）分配一个新 proxy_id 再登记——不需要调用方自己
    起名字。这一步本身不会把代理绑定到任何平台/账号，返回这次生效的 proxy_id。"""
    r = get_client()
    proxy_id = _find_by_host_port(host, port) or str(r.incr(k("proxy_id_seq")))
    r.hmset(k("proxy", proxy_id), {
        "host": host, "port": port,
        "username": username or "", "password": password or "",
    })
    return proxy_id


def remove_proxy(proxy_id: str) -> None:
    get_client().delete(k("proxy", proxy_id))


def get_proxy(proxy_id: str) -> dict | None:
    data = get_client().hgetall(k("proxy", proxy_id))
    return data or None


def list_proxy_ids() -> list[str]:
    r = get_client()
    prefix = k("proxy", "")
    return [key[len(prefix):] for key in r.keys(prefix + "*")]


def list_proxies() -> list[dict]:
    """返回池子里每个代理的完整信息（host/port/username/password 都在，外加
    proxy_id 本身），查代理池现在有什么用这个，只要 ID 列表用 list_proxy_ids()。"""
    proxies = []
    for proxy_id in list_proxy_ids():
        proxy = get_proxy(proxy_id)
        if proxy:
            proxies.append({"proxy_id": proxy_id, **proxy})
    return proxies


def requests_proxies(proxy_id: str) -> dict:
    """返回 {"http": "...", "https": "..."}，可以直接传给 requests 的 `proxies=` 参数。"""
    proxy = get_proxy(proxy_id)
    if not proxy:
        raise KeyError(f"unknown proxy: {proxy_id}")
    auth = f"{proxy['username']}:{proxy['password']}@" if proxy.get("username") else ""
    url = f"http://{auth}{proxy['host']}:{proxy['port']}"
    return {"http": url, "https": url}


def _load_key(platform: str) -> str:
    return k("proxies_by_load", platform)


def _account_key(platform: str, email: str) -> str:
    return k("account_proxy", platform, email)


def _ensure_ranked(platform: str) -> None:
    """让每个已知代理都参与这个平台的负载排名，即使它在这个平台上还从没被
    绑定过（初始 score 为 0）。"""
    r = get_client()
    load_key = _load_key(platform)
    for proxy_id in list_proxy_ids():
        if r.zscore(load_key, proxy_id) is None:
            r.zadd(load_key, {proxy_id: 0})


def pick_for_new_account(platform: str, max_accounts_per_ip: int) -> str | None:
    """返回这个平台上负载最少且没超上限的代理，如果代理池为空或全都满了则
    返回 None。`max_accounts_per_ip` 是平台策略，由调用方提供（来自
    platforms/<name>/config.py）——这个模块对这个数值没有任何主张。"""
    _ensure_ranked(platform)
    r = get_client()
    candidates = r.zrangebyscore(_load_key(platform), "-inf", max_accounts_per_ip - 1, start=0, num=1)
    return candidates[0] if candidates else None


def bind_account(platform: str, email: str, proxy_id: str) -> None:
    r = get_client()
    r.set(_account_key(platform, email), proxy_id)
    r.zincrby(_load_key(platform), 1, proxy_id)


def get_account_proxy_id(platform: str, email: str) -> str | None:
    return get_client().get(_account_key(platform, email))


def unbind_account(platform: str, email: str) -> None:
    """释放这个账号占用的槽位，比如 account_registry.sweep() 发现它已经
    不是 ACTIVE 状态时。"""
    r = get_client()
    proxy_id = get_account_proxy_id(platform, email)
    if proxy_id:
        r.zincrby(_load_key(platform), -1, proxy_id)
        r.delete(_account_key(platform, email))


def load_report(platform: str) -> list[dict]:
    """返回 [{"proxy_id":..., "bound_accounts": n}, ...]，方便查看代理池的
    负载是否均衡。"bound_accounts" 只有在最近跑过
    account_registry.sweep(platform) 的前提下才准确反映真实情况。"""
    r = get_client()
    pairs = r.zrange(_load_key(platform), 0, -1, withscores=True)
    return [{"proxy_id": pid, "bound_accounts": int(score)} for pid, score in pairs]




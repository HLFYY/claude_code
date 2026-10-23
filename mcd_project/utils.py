#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""公共工具函数模块"""
import json
import math
import random
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
from urllib.parse import urlsplit, parse_qs

import requests

import config


def log_info(content):
    """
    统一的日志输出函数
    使用 flush=True 确保输出立即写入文件（不受缓冲影响）

    Args:
        content: 日志内容
    """
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")}, {content}', flush=True)


# ==================== 神龙短效代理 ====================
# 配置(KEYS/URL/支持城市/经纬度/池)全部在 config.py。
# 短效代理提一次用~3分钟(实测上海176s/大连181s),区别于手动 add_proxy 的长效代理。

_key_rr = 0      # key 轮换游标(进程内)
_redis = None    # Redis 单例(懒加载)


def _pick_key(index=None):
    """选一个 key/sign。index 指定用该下标(不动游标);否则按游标轮换。"""
    global _key_rr
    keys = config.SHENLONG_KEYS
    if index is not None:
        return keys[index % len(keys)]
    kc = keys[_key_rr % len(keys)]
    _key_rr += 1
    return kc


def _get_redis():
    """懒加载复用 config.make_redis()(单例)。"""
    global _redis
    if _redis is None:
        _redis = config.make_redis()
    return _redis


# ==================== 出口公网 IP 探测 ====================
# 把本机/服务器的公网出口 IP 查出来,用于加进代理白名单(站大爷/神龙都按 IP 放行)。
# 我们网络有多个出口 → 多站点各查一次汇总;proxies={'http':None,'https':None} 强制直连(不走任何代理)。
_IP_QUERY_SITES = [
    'https://api.ipify.org', 'http://icanhazip.com', 'http://ifconfig.me/ip',
    'http://ipinfo.io/ip', 'http://ipecho.net/plain', 'http://ip114.top/',
]
_IPV4_RE = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')


def get_egress_ip(timeout: int = 6, want_all: bool = False):
    """
    查当前网络【出口公网 IP】(直连,不走代理)。

    Args:
        timeout: 单站点超时(秒)。
        want_all: True=返回探测到的所有不同出口 IP(去重 list,多出口场景);
                  False(默认)=返回出现次数最多的那个(str)或 None。
    """
    from collections import Counter
    seen: List[str] = []
    for url in _IP_QUERY_SITES:
        try:
            r = requests.get(url, proxies={'http': None, 'https': None}, timeout=timeout)
            m = _IPV4_RE.search(r.text or '')
            if m:
                seen.append(m.group(0))
                log_info(f'  出口IP {url} -> {m.group(0)}')
        except Exception as e:
            log_info(f'  出口IP探测失败 {url}: {str(e)[:50]}')
    if not seen:
        return [] if want_all else None
    if want_all:
        return list(dict.fromkeys(seen))          # 按出现顺序去重
    return Counter(seen).most_common(1)[0][0]      # 最多数票那个


# ==================== 凭证 key 迁移(mcd:credentials -> mkt:prod:<商户>:credentials) ====================
def city_to_province(city: Optional[str]) -> str:
    """城市 -> 省份简称(config.CITY_PROVINCE);未命中或空返回 ''。"""
    if not city:
        return ''
    return config.CITY_PROVINCE.get(city.strip(), '')


def migrate_credentials(merchant_id: Optional[str] = None, old_key: str = 'mcd:credentials',
                        overwrite: bool = False) -> int:
    """
    把旧凭证 Hash(old_key)里【有 sid(已登录)】的数据同步到按商户隔离的新 key,并补齐 province/city:
      - 无 city → province 和 city 都置空字符串;
      - 有 city → 缺 province 时按 city 补全(city_to_province),再写入。
    additive:默认只补新 key 没有的手机号,不动旧 key;overwrite=True 覆盖同号。返回同步条数。
    """
    new_key = config.CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id or config.MERCHANT_ID_DEFAULT)
    if new_key == old_key:
        return 0
    r = _get_redis()
    if not r.exists(old_key):
        log_info(f'旧凭证 {old_key} 不存在,无需迁移')
        return 0
    old = r.hgetall(old_key)
    n = 0
    for phone, val in old.items():
        try:
            cred = json.loads(val)
        except Exception:
            continue
        if not cred.get('sid'):          # 只同步已登录(有 sid)的
            continue
        city = (cred.get('city') or '').strip()
        if city:
            cred['province'] = (cred.get('province') or '').strip() or city_to_province(city)
            cred['city'] = city
        else:
            cred['province'] = ''
            cred['city'] = ''
        if overwrite or not r.hexists(new_key, phone):
            r.hset(new_key, phone, json.dumps(cred, ensure_ascii=False))
            n += 1
    log_info(f'✓ 凭证迁移 {old_key} -> {new_key}: 同步 {n} 个有sid账号(overwrite={overwrite},旧key保留)')
    return n


# ==================== 代理源配置(Redis Hash: mkt:prod:<商户>:proxy) ====================
def _proxy_cfg_key(merchant_id: Optional[str] = None) -> str:
    mid = merchant_id or config.MERCHANT_ID_DEFAULT
    return config.PROXY_CONFIG_KEY_TMPL.format(merchant_id=mid)


def seed_proxy_sources(merchant_id: Optional[str] = None, overwrite: bool = False) -> List[str]:
    """
    把 config.PROXY_SOURCES_DEFAULT 写进 Redis Hash(mkt:prod:<商户>:proxy)。
    overwrite=False 只补缺失的 field(不覆盖线上已改配置);True 全量覆盖。返回写入的源名。
    """
    key = _proxy_cfg_key(merchant_id)
    try:
        r = _get_redis()
    except Exception as e:
        log_info(f'⚠️  seed 代理源失败(Redis不可用): {e}')
        return []
    written = []
    for name, cfg in config.PROXY_SOURCES_DEFAULT.items():
        try:
            if overwrite or not r.hexists(key, name):
                r.hset(key, name, json.dumps(cfg, ensure_ascii=False))
                written.append(name)
        except Exception as e:
            log_info(f'⚠️  seed 源 {name} 失败: {e}')
    if written:
        log_info(f'✓ 代理源配置写入 {key}: {written}')
    return written


def load_proxy_source(source: Optional[str] = None, merchant_id: Optional[str] = None) -> Optional[Dict]:
    """读某代理源配置 dict。先 Redis Hash,缺失回退 config.PROXY_SOURCES_DEFAULT。未知源返回 None。"""
    source = source or config.PROXY_SOURCE_DEFAULT
    key = _proxy_cfg_key(merchant_id)
    cfg = None
    try:
        raw = _get_redis().hget(key, source)
        if raw:
            cfg = json.loads(raw)
    except Exception as e:
        log_info(f'⚠️  读代理源配置失败 {key}/{source}: {e}(回退默认)')
    if not cfg:
        cfg = config.PROXY_SOURCES_DEFAULT.get(source)
        if cfg is None:
            log_info(f'❌ 未知代理源 source={source}')
            return None
        cfg = dict(cfg)
    return cfg


# ==================== 站大爷(zdtps)短效代理 ====================
def _zdtps_conn(cfg: Dict) -> Dict:
    """
    把 Redis 里 zdtps 配置规整成统一连接信息,兼容两种 schema:
      ① 用户存的: {url(含完整示例query), params:{host,path,inst_id,akey}, whitelist, format}
      ② 简写: {extract_url, inst_id, akey, whitelist_url, params:{dedup,timespan,type}}
    返回 {extract_url(无query), inst_id, akey, defaults(提取默认query), whitelist_base}。
    """
    p = cfg.get('params') or {}
    url = cfg.get('url') or ''
    sp = urlsplit(url) if url else None
    q = {k: v[0] for k, v in parse_qs(sp.query).items()} if (sp and sp.query) else {}

    inst_id = p.get('inst_id') or cfg.get('inst_id') or q.get('inst_id')
    akey = p.get('akey') or cfg.get('akey') or q.get('akey')

    if cfg.get('extract_url'):
        extract_url = cfg['extract_url']
    elif p.get('host') and p.get('path'):
        extract_url = f"http://{p['host']}{p['path']}"
    elif sp:
        extract_url = f"{sp.scheme}://{sp.netloc}{sp.path}"
    else:
        extract_url = ''

    # 提取默认 query(dedup/timespan/type/count):先取 url 里的,再被简写 params 里的同名键覆盖
    defaults: Dict = {}
    for k in ('dedup', 'timespan', 'type', 'count'):
        if k in q:
            defaults[k] = q[k]
    for k in ('dedup', 'timespan', 'type', 'count'):
        if k in p and k not in ('host', 'path', 'inst_id', 'akey'):
            defaults[k] = p[k]

    wl = cfg.get('whitelist_url') or cfg.get('whitelist') or 'http://www.zdopen.com/ShortEdgeProxy'
    wl_base = (wl[:wl.index('/ShortEdgeProxy')] + '/ShortEdgeProxy') if '/ShortEdgeProxy' in wl else wl.rstrip('/')
    return {'extract_url': extract_url, 'inst_id': inst_id, 'akey': akey,
            'defaults': defaults, 'whitelist_base': wl_base}


def _zdtps_parse(text: str) -> List[str]:
    """解析 GetIP 响应 -> ['ip:port',...]。type=2 为 JSON(ip 在 data.proxy_list);非 JSON 按文本每行兜底。"""
    try:
        data = json.loads(text)
    except Exception:
        return [ln.strip() for ln in (text or '').splitlines() if ':' in ln and ln.strip()]
    if str(data.get('code')) != '10001':
        log_info(f'❌ 站大爷提取失败 code={data.get("code")} msg={data.get("msg")}')
        return []
    rows = (data.get('data') or {}).get('proxy_list') or []
    out = []
    for it in rows:
        if isinstance(it, dict) and it.get('ip') and it.get('port'):
            out.append(f'{it["ip"]}:{it["port"]}')
    return out


def _zdtps_extract(cfg: Dict, province: Optional[str] = None, city: Optional[str] = None,
                   count: int = 1, timeout: int = 12) -> List[str]:
    """站大爷提 count 个。adr 先 "省,市";无结果退"仅省"(同省任意);仍无则不限地区。"""
    conn = _zdtps_conn(cfg)
    if not (conn['inst_id'] and conn['akey'] and conn['extract_url']):
        log_info(f'❌ 站大爷配置缺失(inst_id/akey/url): {conn}')
        return []
    base = {'inst_id': conn['inst_id'], 'akey': conn['akey']}
    base.update(conn['defaults'])
    base['count'] = count
    base.setdefault('dedup', 0)
    base.setdefault('timespan', 3)   # 存活等级 1-5,默认3(与神龙短效~3min一致)
    base.setdefault('type', 2)     # 2=JSON(ip 在 data.proxy_list);=1 时按文本每行兜底解析
    extract_url = conn['extract_url']
    adrs: List[Optional[str]] = []
    if province and city:
        adrs.append(f'{province},{city}')
    if province:
        adrs.append(province)
    if not adrs:
        adrs.append(None)
    for adr in adrs:
        params = dict(base)
        if adr:
            params['adr'] = adr
        try:
            r = requests.get(extract_url, params=params,
                             proxies={'http': None, 'https': None}, timeout=timeout)
            ips = _zdtps_parse(r.text or '')
        except Exception as e:
            log_info(f'❌ 站大爷提取异常 adr={adr}: {str(e)[:60]}')
            ips = []
        if ips:
            log_info(f'✓ 站大爷提取成功 adr={adr or "不限"} count={count} -> {len(ips)} 个')
            return ips
        log_info(f'⚠️  站大爷 adr={adr or "不限"} 无结果,放宽地区重试')
    return []


# ---- 站大爷白名单管理(www.zdopen.com/ShortEdgeProxy) ----
# 统一用 Get/Add/DelWhiteIp(逐个增删,不碰已有); 不用 SetIpWhiteList(整表覆盖会清掉服务器等已有IP)。
# ipList=逗号分隔; AddWhiteIp 不带 ipList = 绑定调用方当前出口IP。实测 code=="10001" 为成功。
def _zdtps_wl(action: str, merchant_id: Optional[str], extra: Optional[Dict] = None):
    """调用白名单接口,返回 (data_dict 或 None, conn)。"""
    cfg = load_proxy_source('zdtps', merchant_id)
    if not cfg:
        return None, None
    conn = _zdtps_conn(cfg)
    if not (conn['inst_id'] and conn['akey']):
        log_info(f'❌ 站大爷白名单配置缺失 inst_id/akey')
        return None, conn
    params = {'inst_id': conn['inst_id'], 'akey': conn['akey']}
    params.update(extra or {})
    try:
        r = requests.get(conn['whitelist_base'] + '/' + action + '/', params=params,
                         proxies={'http': None, 'https': None}, timeout=10)
        return r.json(), conn
    except Exception as e:
        log_info(f'❌ 白名单 {action} 异常: {str(e)[:60]}')
        return None, conn


def zdtps_whitelist_list(merchant_id: Optional[str] = None) -> Optional[List[str]]:
    """查当前白名单 IP 列表,失败返回 None。"""
    data, _ = _zdtps_wl('GetIpWhiteList', merchant_id)
    if not data:
        return None
    if str(data.get('code')) != '10001':
        log_info(f'❌ 查白名单失败 code={data.get("code")} msg={data.get("msg")}')
        return None
    return data.get('list') or []


def zdtps_whitelist_add(ip=None, merchant_id: Optional[str] = None):
    """加白名单。ip=None → 绑定【调用方当前出口 IP】(本机测试最常用);传 ip(str/逗号分隔/list)则加指定 IP。返回 (ok, msg)。"""
    extra = {}
    if ip:
        extra['ipList'] = ip if isinstance(ip, str) else ','.join(ip)
    data, _ = _zdtps_wl('AddWhiteIp', merchant_id, extra)
    if not data:
        return False, '请求失败(见日志)'
    ok = str(data.get('code')) == '10001'
    log_info(f'{"✓" if ok else "❌"} 加白名单 ip={ip or "本机出口"}: {data.get("msg")}')
    return ok, data.get('msg')


def ensure_egress_whitelisted(merchant_id: Optional[str] = None) -> List[str]:
    """探测本机出口公网 IP(多出口) → 先查 zdtps 白名单,只加【不在里面】的 IP。返回在白名单内的出口 IP。"""
    ips = get_egress_ip(want_all=True) or []
    if not ips:
        log_info('⚠️  未探测到出口IP,跳过白名单自动添加')
        return []
    current = zdtps_whitelist_list(merchant_id)   # None=查询失败
    if current is None:
        missing = ips                              # 查不到(接口异常),判断不了就全加,保证可用
    else:
        missing = [ip for ip in ips if ip not in current]
    if not missing:
        log_info(f'✓ 出口IP已在白名单,无需添加: {ips}')
        return ips
    ok, msg = zdtps_whitelist_add(ip=missing, merchant_id=merchant_id)
    log_info(f'{"✓" if ok else "❌"} 白名单新增 {missing}: {msg}')
    if ok:
        return ips
    return [ip for ip in ips if current and ip in current]  # 新增失败,返回已在白名单的那部分


def zdtps_whitelist_del(ip, merchant_id: Optional[str] = None):
    """删白名单 IP(必填,str/逗号分隔/list)。返回 (ok, msg)。"""
    ipstr = ip if isinstance(ip, str) else ','.join(ip)
    data, _ = _zdtps_wl('DelWhiteIp', merchant_id, {'ipList': ipstr})
    if not data:
        return False, '请求失败(见日志)'
    ok = str(data.get('code')) == '10001'
    log_info(f'{"✓" if ok else "❌"} 删白名单 ip={ipstr}: {data.get("msg")}')
    return ok, data.get('msg')


def _extract_for_source(src_cfg: Dict, province: Optional[str], city: Optional[str], count: int) -> List[str]:
    """按代理源类型提取 count 个 ['ip:port',...](统一调度入口)。"""
    t = (src_cfg or {}).get('type')
    if t == 'zdtps':
        return _zdtps_extract(src_cfg, province=province, city=city, count=count)
    if t == 'shenlong':
        area_city = nearest_supported_city(city) if city else random.choice(list(config.SHENLONG_SUPPORTED))
        return _extract_batch(area_city, count)   # 神龙 key 轮换批量重试(key/sign 取自 config.SHENLONG_KEYS)
    log_info(f'❌ 不支持的代理源类型 type={t}')
    return []


def _fmt_proxy(proxy: Optional[str], as_dict: bool):
    """ip:port -> 字符串 或 requests 可用的 {'http':..,'https':..}。"""
    if not proxy:
        return None
    if as_dict:
        url = f'http://{proxy}'
        return {'http': url, 'https': url}
    return proxy


def _haversine(a, b):
    """两点(lng,lat)球面距离(km)。"""
    lng1, lat1 = a
    lng2, lat2 = b
    radius = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(h))


def nearest_supported_city(city: str) -> str:
    """
    目标城市若不是神龙支持节点,返回地理上最近的支持城市;本身支持则原样返回。
    目标城市经纬度未知时,回退到随机支持城市。
    """
    if city in config.SHENLONG_SUPPORTED:
        return city
    geo = config.CITY_GEO.get(city)
    if not geo:
        fallback = random.choice(list(config.SHENLONG_SUPPORTED))
        log_info(f'⚠️  未知城市经纬度 {city}，回退随机支持城市 {fallback}')
        return fallback
    best, best_d = None, float('inf')
    for c in config.SHENLONG_SUPPORTED:
        g = config.CITY_GEO.get(c)
        if not g:
            continue
        d = _haversine(geo, g)
        if d < best_d:
            best, best_d = c, d
    log_info(f'📍 {city} 非神龙节点，取最近支持城市 {best} (~{best_d:.0f}km)')
    return best


def _city_to_area(city: str) -> str:
    """城市 -> area 编码。支持城市查表;否则当作已是编码直接用。"""
    return config.SHENLONG_SUPPORTED.get(city, city)


def extract_short_proxy_by_city(
    city: str,
    count: int = 1,
    timeout: int = 10,
    extra_params: Optional[Dict] = None,
    key_index: Optional[int] = None,
) -> List[str]:
    """
    从神龙短效代理【按城市】提取代理 IP，返回 ['ip:port', ...]，失败返回 []。

    ⚠️ IP 存活时间：实测单个短效 IP 存活约 3 分钟(上海 176s / 大连 181s)，用满 ~2.5 分钟
        建议重提。该时长由神龙套餐决定，链接无独立时长参数(mr=去重, pattern=格式)。

    Args:
        city: 支持城市名(如 '上海')或 6 位国标行政区划编码(如 '310000')。
        count: 本次提取数量。
        timeout: 请求超时(秒)。
        extra_params: 透传给接口的额外参数，覆盖默认值(如 {'life': 5})。
        key_index: 指定用第几个 key(None=按游标轮换)。key/sign 取自 config.SHENLONG_KEYS。

    Returns:
        ['ip:port', ...]；失败或无结果返回 []。
    """
    area = _city_to_area(city)
    params = dict(config.SHENLONG_EXTRACT_PARAMS)
    params.update(_pick_key(key_index))    # 轮换注入 key + sign
    params['area'] = area
    params['count'] = count
    if extra_params:
        params.update(extra_params)

    try:
        resp = requests.get(config.SHENLONG_EXTRACT_URL, params=params,
                            proxies={'http': None, 'https': None}, timeout=timeout)
        text = (resp.text or '').strip()
    except Exception as e:
        log_info(f'❌ 神龙提取请求异常 city={city} area={area}: {e}')
        return []

    # 神龙无论成功失败都可能返回 JSON(错误固定 JSON，pattern=json 成功也是 JSON)。
    data = None
    try:
        data = json.loads(text)
    except Exception:
        data = None

    if isinstance(data, dict):
        code = data.get('code')
        rows = data.get('data')
        if code in (0, 200, '0', '200') and isinstance(rows, list):
            result = []
            for item in rows:
                if isinstance(item, dict):
                    ip, port = item.get('ip'), item.get('port')
                    if ip and port:
                        result.append(f'{ip}:{port}')
                elif isinstance(item, str) and ':' in item:
                    result.append(item.strip())
            log_info(f'✓ 神龙提取成功 city={city} area={area} count={count} -> {len(result)} 个')
            return result
        log_info(f'❌ 神龙提取失败 city={city} area={area}: code={code} msg={data.get("msg")}')
        return []

    # txt 成功：每行一个 ip:port
    result = [ln.strip() for ln in text.splitlines() if ':' in ln and ln.strip()]
    if result:
        log_info(f'✓ 神龙提取成功 city={city} area={area} count={count} -> {len(result)} 个')
    else:
        log_info(f'⚠️  神龙提取无结果 city={city} area={area}: {text[:120]}')
    return result


def _extract_batch(city: str, batch: int) -> List[str]:
    """提 batch 个:一次 count=batch 提取;失败则轮换 key 顺延重试。"""
    global _key_rr
    n = len(config.SHENLONG_KEYS)
    start = _key_rr
    _key_rr += 1
    for off in range(n):
        idx = (start + off) % n
        ips = extract_short_proxy_by_city(city, count=batch, key_index=idx)
        if ips:
            return ips
        log_info(f'  key[{idx}] 提取失败，尝试下一个 key')
    return []


# ==================== 短效代理用量统计(源+省+市+日期) ====================
# 取代理时累计:req=使用(请求)次数(每次取用,含复用);proxy=实际消耗(从上游新提取)的代理数(复用不计)。
# 存:Hash mkt:prod:<商户>:proxy:stat:<源>:<省>:<市>,field=<日期>:req / <日期>:proxy。
STAT_TTL = 100 * 86400   # 统计 key 保留 ~100 天(每次写刷新)


def _stat_key(merchant_id: Optional[str], source: str, province: str, city: str) -> str:
    mid = merchant_id or config.MERCHANT_ID_DEFAULT
    return f'mkt:prod:{mid}:proxy:stat:{source}:{province or "_"}:{city or "_"}'


def stat_incr(source: str, province: str, city: str, merchant_id: Optional[str] = None,
              req: int = 0, proxy: int = 0, day: Optional[str] = None) -> None:
    """累计短效代理用量(失败不影响取代理)。req=请求数, proxy=消耗代理数。"""
    if not (req or proxy):
        return
    day = day or time.strftime('%Y-%m-%d')
    key = _stat_key(merchant_id, source, province, city)
    try:
        r = _get_redis()
        pipe = r.pipeline()
        if req:
            pipe.hincrby(key, f'{day}:req', req)
        if proxy:
            pipe.hincrby(key, f'{day}:proxy', proxy)
        pipe.expire(key, STAT_TTL)
        pipe.execute()
    except Exception as e:
        log_info(f'⚠️  写代理统计失败 {key}: {e}')


def query_proxy_stats(merchant_id: Optional[str] = None, days: int = 7, date: Optional[str] = None,
                      source: Optional[str] = None, province: Optional[str] = None,
                      city: Optional[str] = None) -> Dict:
    """
    查短效代理用量。默认近 days 天;传 date(YYYY-MM-DD)只查某天。可按 source/province/city 过滤。
    返回 {tree:{源:{省:{市:{日期:{req,proxy}}}}}, totals:{req,proxy}, dates:[...]}。
    """
    mid = merchant_id or config.MERCHANT_ID_DEFAULT
    if date:
        allow = {date}
    else:
        days = max(1, int(days or 7))
        today = datetime.now()
        allow = {(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)}
    prefix = f'mkt:prod:{mid}:proxy:stat:'
    tree: Dict = {}
    totals = {'req': 0, 'proxy': 0}
    try:
        r = _get_redis()
    except Exception as e:
        log_info(f'⚠️  查代理统计 Redis 不可用: {e}')
        return {'tree': {}, 'totals': totals, 'dates': sorted(allow)}
    keys, cursor = [], 0
    while True:
        cursor, batch = r.scan(cursor=cursor, match=prefix + '*', count=500)
        keys.extend(batch)
        if cursor == 0:
            break
    for key in keys:
        parts = key[len(prefix):].split(':')
        if len(parts) != 3:
            continue
        s, p, c = parts
        p = '' if p == '_' else p
        c = '' if c == '_' else c
        if source and s != source:
            continue
        if province and p != province:
            continue
        if city and c != city:
            continue
        for field, val in (r.hgetall(key) or {}).items():
            try:
                d, kind = field.rsplit(':', 1)
            except ValueError:
                continue
            if d not in allow or kind not in ('req', 'proxy'):
                continue
            try:
                n = int(val)
            except (TypeError, ValueError):
                continue
            node = tree.setdefault(s, {}).setdefault(p, {}).setdefault(c, {}).setdefault(d, {'req': 0, 'proxy': 0})
            node[kind] += n
            totals[kind] += n
    return {'tree': tree, 'totals': totals, 'dates': sorted(allow)}


def get_short_proxy(
    city: Optional[str] = None,
    province: Optional[str] = None,
    source: Optional[str] = None,
    merchant_id: Optional[str] = None,
    count: int = 1,
    force_new: bool = False,
    as_dict: bool = False,
    ttl: Optional[int] = None,
    expire: Optional[int] = None,
) -> Optional[Union[str, Dict[str, str]]]:
    """
    取【一个】短效代理。核心：Redis 池 + 多进程原子轮巡。

    流程：
      1) city 不传 -> 从 config.SHENLONG_SUPPORTED 随机一个支持城市；
         city 不是神龙节点 -> 自动取最近支持城市(nearest_supported_city，如 深圳->珠海)；
      2) force_new=False 且池有缓存：用 RPOPLPUSH 从该城市 Redis 池【原子轮巡】取一个
         (多进程安全：每次调用轮流拿池里下一个，不重复提取、不冲突；不管 count 多少都走这条)；
      3) 池空(TTL 到期自动清)或 force_new：一次提 count 个入池(整池 TTL=ttl)，再轮巡取一个；
         提新时用 NX 锁防多进程惊群重复提取。
      Redis 不可用则降级：直接提 1 个返回。

    Args:
        city: 城市名/国标编码；None=随机支持城市(仅神龙用)。
        province: 省份(zdtps 用,作 adr "省,市";取不到退仅省)。
        source: 代理源名,默认 config.PROXY_SOURCE_DEFAULT='zdtps';'shenlong'=走神龙。
        merchant_id: 商户ID(默认 100001),决定代理配置 Hash key 与池 key 的隔离。
        count: 池空需提新时，一次提取入池的数量(默认 1)；池里有缓存且非 force_new 时此参数不生效(直接轮巡)。
        force_new: True=清空该城市池并重提。
        as_dict: True 返回 {'http':'http://ip:port','https':'http://ip:port'}(requests 直接用)；
                 False(默认)返回 'ip:port'。
        expire: 代理过期时间秒(默认 config.SHORT_PROXY_CONFIG['expire']=180)。
        ttl: 池存活秒数;默认 = expire-20。显式传则覆盖。

    Returns:
        'ip:port' / {'http','https'} / None。

    示例:
        get_short_proxy()                      # 随机城市，轮巡复用
        get_short_proxy('上海')                # 指定城市
        get_short_proxy('深圳')                # 自动落到最近支持城市(珠海)
        get_short_proxy('上海', as_dict=True)  # requests 可直接 proxies=
        get_short_proxy('上海', force_new=True)# 清池重提
    """
    source = source or config.PROXY_SOURCE_DEFAULT
    merchant_id = merchant_id or config.MERCHANT_ID_DEFAULT
    src_cfg = load_proxy_source(source, merchant_id)
    if not src_cfg:
        return None

    cfg = config.SHORT_PROXY_CONFIG
    # 过期时间(默认 config 180),实际池 TTL = 过期时间 - 20
    expire = cfg.get('expire', 180) if expire is None else expire
    ttl = (expire - 20) if ttl is None else ttl

    # 1) 定地域(作池 key 的一部分):神龙按城市落到最近支持节点;zdtps 按 省/市。
    if src_cfg.get('type') == 'shenlong':
        if not city:
            city = random.choice(list(config.SHENLONG_SUPPORTED))
            log_info(f'🎲 未指定城市，随机选 {city}')
        loc = nearest_supported_city(city)
    else:
        loc = (province or '') + (f'_{city}' if city else '') or 'any'
    area_city = loc  # 下方日志/池沿用此名
    # 池 key 按 商户+源+地域 隔离,不同源/地域互不串
    rk = f'mkt:prod:{merchant_id}:proxy:pool:{source}:{loc}'

    # 统计维度:神龙(省=按落点城市推导, 市=落点城市);zdtps 按传入省/市
    if src_cfg.get('type') == 'shenlong':
        st_prov, st_city = city_to_province(loc), loc
    else:
        st_prov, st_city = (province or ''), (city or '')

    def _stat_use(req=0, proxy=0):
        stat_incr(source, st_prov, st_city, merchant_id=merchant_id, req=req, proxy=proxy)

    try:
        r = _get_redis()
    except Exception as e:
        log_info(f'⚠️  Redis 不可用，降级直接提取 city={area_city}: {e}')
        r = None

    # 2) 轮巡复用(RPOPLPUSH 原子轮转，多进程安全)
    if r is not None and not force_new:
        try:
            p = r.rpoplpush(rk, rk)
        except Exception as e:
            log_info(f'⚠️  读 Redis 池失败，改为提新 city={area_city}: {e}')
            p = None
        if p:
            log_info(f'♻️  轮巡复用 city={area_city} -> {p}')
            _stat_use(req=1)
            return _fmt_proxy(p, as_dict)

    # 3) 提新入池(NX 锁防多进程惊群)
    lock_key = rk + ':lock'
    got_lock = False
    if r is not None:
        try:
            got_lock = bool(r.set(lock_key, '1', nx=True, ex=20))
        except Exception:
            got_lock = False
        if not got_lock and not force_new:
            # 别的进程正在补池，稍等后再轮巡一次
            time.sleep(1.5)
            try:
                p = r.rpoplpush(rk, rk)
            except Exception:
                p = None
            if p:
                log_info(f'♻️  (他进程已补池)轮巡复用 city={area_city} -> {p}')
                _stat_use(req=1)
                return _fmt_proxy(p, as_dict)

    try:
        ips = _extract_for_source(src_cfg, province, city, count)
        if not ips and src_cfg.get('type') == 'zdtps':
            # 自愈:提取失败多半是换了网络出口、当前IP不在白名单(判断不了具体原因就直接)
            #       取当前网络IP+加该源白名单一次,再重试一次(office↔home 换网没重启也能恢复)。
            log_info(f'⚠️  {source} 提取失败,尝试自动加白名单后重试…')
            try:
                if ensure_egress_whitelisted(merchant_id):
                    ips = _extract_for_source(src_cfg, province, city, count)
            except Exception as e:
                log_info(f'自愈加白名单异常: {e}')
        if not ips:
            log_info(f'❌ 提取短效代理失败 source={source} loc={area_city}')
            return None
        _stat_use(req=1, proxy=len(ips))   # 本次取用1 + 新消耗 len(ips) 个短效代理
        if r is not None:
            try:
                pipe = r.pipeline()
                if force_new:
                    pipe.delete(rk)
                pipe.rpush(rk, *ips)
                pipe.expire(rk, ttl)
                pipe.execute()
                p = r.rpoplpush(rk, rk)
                log_info(f'🆕 提取 {len(ips)} 个入池 city={area_city} (TTL={ttl}s)，取用 {p}')
                return _fmt_proxy(p or ips[0], as_dict)
            except Exception as e:
                log_info(f'⚠️  写 Redis 池失败(不影响返回) city={area_city}: {e}')
        return _fmt_proxy(ips[0], as_dict)
    finally:
        if got_lock and r is not None:
            try:
                r.delete(lock_key)
            except Exception:
                pass


if __name__ == '__main__':
    # # 本地自测：先把 config.SHENLONG_KEYS 换成真实值再跑。
    # log_info('--- 随机城市 ---')
    # log_info(f'随机: {get_short_proxy()}')
    # log_info('--- 轮巡复用(上海连取4次，应在池内IP间轮转) ---')
    # for _ in range(4):
    #     log_info(f'上海: {get_short_proxy("上海")}')
    # log_info('--- as_dict ---')
    # log_info(f'{get_short_proxy("上海", as_dict=True)}')
    # log_info('--- 不支持城市->最近支持城市 ---')
    # log_info(f'深圳: {get_short_proxy("深圳")}')
    # log_info('--- force_new ---')
    # log_info(f'强制新: {get_short_proxy("上海", force_new=True)}')
    ips = get_egress_ip()
    print(ips)
    print(zdtps_whitelist_add(ips, config.MERCHANT_ID_DEFAULT))

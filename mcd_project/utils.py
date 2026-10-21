#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""公共工具函数模块"""
import json
import math
import random
import time
from typing import Dict, List, Optional, Union

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


def get_short_proxy(
    city: Optional[str] = None,
    count: int = 1,
    force_new: bool = False,
    as_dict: bool = False,
    ttl: Optional[int] = None,
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
        city: 城市名/国标编码；None=随机支持城市。
        count: 池空需提新时，一次提取入池的数量(默认 1)；池里有缓存且非 force_new 时此参数不生效(直接轮巡)。
        force_new: True=清空该城市池并重提。
        as_dict: True 返回 {'http':'http://ip:port','https':'http://ip:port'}(requests 直接用)；
                 False(默认)返回 'ip:port'。
        ttl: 池存活秒数(默认 config.SHORT_PROXY_CONFIG['ttl']=150)。

    Returns:
        'ip:port' / {'http','https'} / None。

    示例:
        get_short_proxy()                      # 随机城市，轮巡复用
        get_short_proxy('上海')                # 指定城市
        get_short_proxy('深圳')                # 自动落到最近支持城市(珠海)
        get_short_proxy('上海', as_dict=True)  # requests 可直接 proxies=
        get_short_proxy('上海', force_new=True)# 清池重提
    """
    cfg = config.SHORT_PROXY_CONFIG
    ttl = cfg['ttl'] if ttl is None else ttl

    # 1) 定城市
    if not city:
        city = random.choice(list(config.SHENLONG_SUPPORTED))
        log_info(f'🎲 未指定城市，随机选 {city}')
    area_city = nearest_supported_city(city)
    rk = cfg['pool_key'].format(city=area_city)

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
                return _fmt_proxy(p, as_dict)

    try:
        ips = _extract_batch(area_city, count)
        if not ips:
            log_info(f'❌ 提取短效代理失败 city={area_city}')
            return None
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
    # 本地自测：先把 config.SHENLONG_KEYS 换成真实值再跑。
    log_info('--- 随机城市 ---')
    log_info(f'随机: {get_short_proxy()}')
    log_info('--- 轮巡复用(上海连取4次，应在池内IP间轮转) ---')
    for _ in range(4):
        log_info(f'上海: {get_short_proxy("上海")}')
    log_info('--- as_dict ---')
    log_info(f'{get_short_proxy("上海", as_dict=True)}')
    log_info('--- 不支持城市->最近支持城市 ---')
    log_info(f'深圳: {get_short_proxy("深圳")}')
    log_info('--- force_new ---')
    log_info(f'强制新: {get_short_proxy("上海", force_new=True)}')

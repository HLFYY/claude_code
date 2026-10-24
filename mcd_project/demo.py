import json
import re
import sys
from datetime import datetime
from urllib.parse import quote
import requests
import redis
import openpyxl
from proxy_pool import ProxyPool
from mcd_api import build_headers, API_BASE
from config import REDIS_SET_URL, CREDENTIALS_KEY_TMPL, MERCHANT_ID_DEFAULT, CITY_PROVINCE

# env -> mcgold_flow 请求的服务端域名（与 run_api_client.API_BASE 对应）
API_BASE_BY_ENV = {
    'local': 'http://127.0.0.1:5001',
    'online': 'http://dns.xme.fan:5001',
}

# 账号轮流分配的主要城市（依次取，第 i 个账号用第 i 个城市，循环）。省份由 CITY_PROVINCE 推。
MAJOR_CITIES = [
    '上海', '北京', '天津', '重庆', '广州', '深圳', '成都', '杭州', '武汉', '西安',
    '南京', '苏州', '郑州', '长沙', '青岛', '沈阳', '大连', '宁波', '厦门', '福州',
]

# 累计被服务端判 invalid 的次数（Redis Hash：field=手机号，value=次数）。
# 单次 invalid 不代表账号废了（可能是城市/代理/设备当时的组合问题），要连吃够
# VIP_INVALID_CLEAR_AT 次才认为"当前 token + 省市这套组合"确实走不通，那时才清空重来。
VIP_INVALID_COUNT_KEY_TMPL = 'mkt:prod:{merchant_id}:vip_invalid'
VIP_INVALID_CLEAR_AT = 3


def _redis_conn(env):
    """按 env 返回 (redis客户端, 连接信息dict)。'local'=本机 / 'online'=线上。"""
    if env == 'online':
        conf = dict(host=REDIS_SET_URL['host'], port=REDIS_SET_URL['port'],
                    db=REDIS_SET_URL['db'], password=REDIS_SET_URL['password'])
    elif env == 'local':
        conf = dict(host='127.0.0.1', port=6379, db=0, password=None)
    else:
        raise ValueError(f"env 只能是 'local' 或 'online'，收到: {env!r}")
    rc = redis.Redis(decode_responses=True, socket_connect_timeout=15,
                     socket_timeout=25, **conf)
    return rc, conf


def write_credential(phone, sid, meddy_id, env='local', merchant_id=None):
    """写入账号登录凭证到 Redis（只写 token/sid/meddy_id 3 个字段）。

    token 置空，交给 app 的 ensure_token 流程（生成→激活→refresh）补全。

    Args:
        phone: 手机号（作为 Redis Hash 的 field）
        sid: 登录会话 SID
        meddy_id: MeddyId
        env: 'local'=本机 Redis(127.0.0.1:6379) / 'online'=线上(db.selfmediaai.cn)
        merchant_id: 商户ID，默认 100001（决定凭证 key）

    Returns:
        写入后回读的 JSON 字符串
    """
    merchant_id = merchant_id or MERCHANT_ID_DEFAULT
    rc, conf = _redis_conn(env)
    key = CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id)
    cred = {'token': '', 'sid': sid, 'meddy_id': meddy_id}
    rc.hset(key, phone, json.dumps(cred, ensure_ascii=False))

    back = rc.hget(key, phone)
    print(f"✓ [{env}] 写入 {conf['host']}:{conf['port']}/{conf['db']} {key} field={phone}")
    print(f"  {back}")
    return back


def sync_credential_to_online(phone, merchant_id=None):
    """把某手机号的凭证从【本地】Redis 原样同步到【线上】Redis。

    Args:
        phone: 手机号（Redis Hash 的 field）
        merchant_id: 商户ID，默认 100001（决定凭证 key）

    Returns:
        同步写入线上的 JSON 字符串；本地没有该凭证时返回 None（不写线上）。
    """
    merchant_id = merchant_id or MERCHANT_ID_DEFAULT
    key = CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id)

    local_rc, _ = _redis_conn('local')
    val = local_rc.hget(key, phone)
    if not val:
        print(f"✗ 本地 Redis 无凭证 {key} field={phone}，不同步")
        return None

    online_rc, oconf = _redis_conn('online')
    online_rc.hset(key, phone, val)                  # 原样写入，不改任何字段

    back = online_rc.hget(key, phone)
    print(f"✓ 已同步到线上 {oconf['host']}:{oconf['port']}/{oconf['db']} {key} field={phone}")
    print(f"  {back}")
    return back


def _read_accounts_from_excel(excel_path):
    """读取 Excel，返回 [(phone, sid, meddy_id), ...]。
    按表头 phone/sid/meddyId(或 meddy_id/meddy) 定位列(忽略大小写)；找不到表头则用前 3 列。"""
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    header = [str(c).strip().lower() if c is not None else '' for c in rows[0]]

    def find(names):
        for i, h in enumerate(header):
            if h in names:
                return i
        return None
    i_phone = find({'phone', '手机号'})
    i_sid = find({'sid'})
    i_meddy = find({'meddyid', 'meddy_id', 'meddy'})
    if i_phone is None or i_sid is None or i_meddy is None:
        i_phone, i_sid, i_meddy = 0, 1, 2          # 无可识别表头→用前3列
        data_rows = rows                            # 没表头，整表都是数据
    else:
        data_rows = rows[1:]                        # 跳过表头行

    out = []
    for r in data_rows:
        if not r or len(r) <= max(i_phone, i_sid, i_meddy):
            continue
        phone = r[i_phone]
        sid = r[i_sid]
        meddy = r[i_meddy]
        if not phone or not sid:                    # 手机号/sid 必须有
            continue
        out.append((str(phone).strip(), str(sid).strip(),
                    str(meddy).strip() if meddy is not None else ''))
    return out


def run_mcgold_from_excel(excel_path='/Users/houjie/Desktop/mcd-sid数据.xlsx',
                          env='local', merchant_id=None, cities=None,
                          sku_pick='last', pause_on_success=False):
    """批量跑麦金卡开通：读 Excel → 逐账号按凭证状态决定动作 → 调 mcgold_flow。

    不再区分 mode，统一逻辑（按该账号在 Redis 凭证里的状态分支）：

      | 凭证状态                                    | 动作                              | 请求省市来源          |
      |--------------------------------------------|-----------------------------------|----------------------|
      | 不存在                                       | 写入凭证(token 空/sid/meddyId)后测试 | cities 依次取         |
      | vip_status == 'valid'（能开/已开）           | **跳过，不测试**                    | —                    |
      | vip_status == 'invalid' 且累计次数 < 3        | 凭证原样（**沿用已有 token + 省市**） | 凭证里的 province/city |
      | vip_status == 'invalid' 且累计次数 >= 3       | 清空 token，换 cities 的下一个城市    | cities 依次取         |
      | vip_status == 'error'（没测出来）             | **保留凭证原样**，直接重测            | 凭证里的 province/city |
      | vip_status 不存在/为空                        | **不改 Redis**，直接测试              | 凭证里的 province/city |

    invalid 计数（Redis Hash `mkt:prod:{merchant_id}:vip_invalid`，field=手机号）：
    - 每次跑完最终 vip_status == 'invalid' → 次数 +1；
    - 最终 vip_status == 'valid' → **删掉该账号的计数**（归零，重新开始）；
    - error / 跳过 / 无状态 → 不动计数。
    计数 >= VIP_INVALID_CLEAR_AT(3) 时清空 token + 换城市，同时把计数归零，
    等于开启新一轮累计（否则第 4、5 次会连续立刻清空）。

    其它约定：
    - 套餐自动取服务端返回列表的**最后一个**（当前是麦金卡365天 ¥118，skuId=12536），
      不再交互式选择；传 sku_pick='first' 可改取第一个。
    - 跑完**不删除** Redis 里的账号凭证。
    - vip_status **以 worker 回写为准**，三态口径：valid=能开/已开，invalid=服务端
      明确拒绝(6000232 或 message 含「异常」)，error=本次没测出来(代理/网络/未知码)。
      demo 只在 worker 没回写（凭证里仍为空）时兜底，且兜底只认「真开通成功」。
    - env 同时决定：Redis 走本地/线上，mcgold_flow 请求本地(127.0.0.1:5001)/线上(dns.xme.fan:5001)。

    Args:
        excel_path: Excel 路径(列: phone/sid/meddyId)
        env: 'local' / 'online'
        merchant_id: 商户ID，默认 100001
        cities: 城市列表(轮取)，默认 MAJOR_CITIES
        sku_pick: 'last'(默认，取最后一个套餐) / 'first'
        pause_on_success: 成功后是否暂停等回车（批量跑建议 False）
    """
    import run_api_client as rc_mod   # 延迟导入：导入时会打印 API_BASE

    merchant_id = merchant_id or MERCHANT_ID_DEFAULT
    cities = cities or MAJOR_CITIES
    base = API_BASE_BY_ENV.get(env)
    if base is None:
        raise ValueError(f"env 只能是 'local' 或 'online'，收到: {env!r}")
    rc_mod.API_BASE = base                           # 让 mcgold_flow 走对应域名

    rconn, conf = _redis_conn(env)
    key = CREDENTIALS_KEY_TMPL.format(merchant_id=merchant_id)
    accounts = _read_accounts_from_excel(excel_path)
    # 只有「新账号」和「失败重试」才消耗城市位，跳过/保留的账号不占位
    city_seq = {'n': 0}

    def _next_city():
        """从 cities 依次取（循环），返回 (city, province)。"""
        c = cities[city_seq['n'] % len(cities)]
        city_seq['n'] += 1
        return c, CITY_PROVINCE.get(c, c)

    def _load(phone):
        """读凭证，返回 dict；不存在或解析失败返回 None。"""
        raw = rconn.hget(key, phone)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _save(phone, row):
        rconn.hset(key, phone, json.dumps(row, ensure_ascii=False))

    def _set_vip(phone, status):
        """兜底回写 vip_status/vip_time（仅在 worker 没写时用），保留其余字段。"""
        row = _load(phone) or {}
        row['vip_status'] = status
        row['vip_time'] = datetime.now().isoformat()
        _save(phone, row)

    # ---- invalid 累计计数（独立 Hash，不塞进凭证 JSON，免得和 worker 的写入互相覆盖）----
    cnt_key = VIP_INVALID_COUNT_KEY_TMPL.format(merchant_id=merchant_id)

    def _invalid_cnt(phone):
        """读该账号已累计的 invalid 次数；没有/读不到都算 0。"""
        try:
            return int(rconn.hget(cnt_key, phone) or 0)
        except Exception:
            return 0

    def _bump_invalid(phone):
        """invalid 次数 +1，返回累计后的值。"""
        try:
            return int(rconn.hincrby(cnt_key, phone, 1))
        except Exception:
            return -1

    def _reset_invalid(phone):
        """清零该账号的 invalid 计数（成功时用）。"""
        try:
            rconn.hdel(cnt_key, phone)
        except Exception:
            pass

    print(f"[batch] env={env} Redis={conf['host']}:{conf['port']} 域名={base} "
          f"key={key} 账号数={len(accounts)} sku={sku_pick}")

    def _open_status(phone):
        """查该账号麦金卡开通最终状态，返回 (status, msg, orderId, payId)。"""
        try:
            st = rc_mod.api_call('/api/member/open_status', params={'phone': phone})
            d = st.get('data', {}) if st.get('success') else {}
            return d.get('status'), d.get('msg', ''), d.get('orderId', ''), d.get('payId', '')
        except Exception as e:
            return None, str(e), '', ''

    # mcgold_flow 内部用 input() 选套餐 —— 临时给模块挂一个自动应答，只影响该模块
    _orig_input = getattr(rc_mod, 'input', None)

    def _auto_pick(prompt=''):
        m = re.search(r'1-(\d+)', str(prompt))
        if not m:
            return ''
        return str(int(m.group(1))) if sku_pick == 'last' else '1'

    rc_mod.input = _auto_pick

    # SEND_LOCATION 是 run_api_client 的模块级开关。它关着时 _extra_params() 不返回省市，
    # demo 这边算好的城市就传不出去了 —— /api/member/open 的 body 里没有 city，
    # server 入队 payload 也是 city=None，worker 提代理只能 adr=不限(池里 city=any)。
    # 批量跑必须打开，跑完恢复。
    _orig_send_loc = getattr(rc_mod, 'SEND_LOCATION', False)
    rc_mod.SEND_LOCATION = True

    results = []   # [{phone, city, province, result, detail}]
    try:
        for idx, (phone, sid, meddy) in enumerate(accounts, 1):
            # ---------- 1. 读凭证，决定分支 ----------
            cur = _load(phone)
            if cur is None:
                # ① 账号不存在 → 写入凭证(token 空，由服务端 ensure_token 补全)
                # 城市一并写进凭证：worker 按 city 提代理，不写就只能提 adr=不限(any)
                city, province = _next_city()
                _save(phone, {'token': '', 'sid': sid, 'meddy_id': meddy,
                              'province': province, 'city': city,
                              'vip_status': '', 'vip_time': ''})
                _reset_invalid(phone)      # 凭证是新建的，顺手清掉可能残留的旧计数
                print("\n" + "=" * 60)
                print(f"[{idx}/{len(accounts)}] phone={phone} 城市={province}/{city} "
                      f"[新账号] 已写入凭证(空 token)")
            else:
                vs = (cur.get('vip_status') or '').strip().lower()
                if vs == 'valid':
                    # ② 已成功过 → 不测试
                    print(f"\n[{idx}/{len(accounts)}] phone={phone} [已成功 vip_status=valid] 跳过")
                    results.append({'phone': phone,
                                    'city': cur.get('city', ''), 'province': cur.get('province', ''),
                                    'result': '跳过', 'detail': 'vip_status=valid 已开通'})
                    continue
                if vs == 'invalid':
                    n_inv = _invalid_cnt(phone)
                    if n_inv >= VIP_INVALID_CLEAR_AT:
                        # ③a 连吃够 3 次 invalid → 当前 token+省市这套组合确实走不通，
                        #     清 token、换下一个城市，并把计数归零重新开始累计。
                        #     （城市直接写进凭证：worker 读凭证就能按城市提代理，
                        #      不必依赖请求参数透传）
                        city, province = _next_city()
                        cur['token'] = ''
                        cur['province'] = province
                        cur['city'] = city
                        _save(phone, cur)
                        _reset_invalid(phone)
                        print("\n" + "=" * 60)
                        print(f"[{idx}/{len(accounts)}] phone={phone} 城市={province}/{city} "
                              f"[invalid 已 {n_inv} 次] 已清空 token、换城市，计数归零")
                    else:
                        # ③b 次数还不够 → 不动凭证，沿用已有 token + 省市重测一次
                        city = cur.get('city') or ''
                        province = cur.get('province') or (CITY_PROVINCE.get(city, city) if city else '')
                        if not city:
                            city, province = _next_city()   # 凭证里连城市都没有，兜底轮取
                        tag = f'invalid 仅 {n_inv}/{VIP_INVALID_CLEAR_AT} 次'
                        print("\n" + "=" * 60)
                        print(f"[{idx}/{len(accounts)}] phone={phone} 城市={province or '-'}/{city or '-'} "
                              f"[{tag}] 沿用凭证 token 与省市")
                else:
                    # ④ 'error'（上次没测出来）或 vip_status 缺失 → 凭证原样不动，直接重测
                    city = cur.get('city') or ''
                    province = cur.get('province') or (CITY_PROVINCE.get(city, city) if city else '')
                    if not city:
                        city, province = _next_city()   # 凭证里连城市都没有，兜底轮取
                    tag = '状态未定 vip_status=error' if vs == 'error' else '无 vip_status'
                    print("\n" + "=" * 60)
                    print(f"[{idx}/{len(accounts)}] phone={phone} 城市={province or '-'}/{city or '-'} "
                          f"[{tag}] 不改 Redis，沿用凭证省市")

            # ---------- 2. 调 mcgold_flow：喂该手机号(触发服务端 ensure_token)，省市走 _extra_params ----------
            rc_mod._CURRENT_PROVINCE = province
            rc_mod._CURRENT_CITY = city

            def _auto_login_info(_phone=phone, _prov=province, _city=city):
                """替代交互式 get_login_info：直接用该手机号查状态(触发 ensure_token)，不手输。"""
                rc_mod._CURRENT_PROVINCE = _prov
                rc_mod._CURRENT_CITY = _city
                res = rc_mod.api_call('/api/login/status',
                                      params={'phone': _phone, **rc_mod._extra_params()})
                d = res.get('data', {}) if res.get('success') else {}
                if not d.get('is_logged_in'):
                    raise RuntimeError(f"{_phone} 未登录/凭证无效: {res}")
                return _phone, d.get('token'), d.get('sid'), d.get('meddy_id')

            rc_mod.get_login_info = _auto_login_info
            flow_err = ''                                # 记录本地这侧的异常原因，供后面自愈
            try:
                rc_mod.mcgold_flow()
            except SystemExit as e:                      # mcgold_flow 内失败会 sys.exit，拦下继续下一个
                flow_err = f'exit({e.code})'
                print(f"  ✗ mcgold_flow 退出(code={e.code})，继续下一个")
            except Exception as e:
                flow_err = str(e)
                print(f"  ✗ mcgold_flow 异常: {e}，继续下一个")

            # ---------- 3. 以服务端开通状态为准记录本次结果 ----------
            status, msg, order_id, pay_id = _open_status(phone)
            if status == 'done':
                print(f"  ✅ 开通成功 phone={phone} 城市={province}/{city} "
                      f"orderId={order_id} payId={pay_id}")
                results.append({'phone': phone, 'city': city, 'province': province,
                                'result': '成功', 'detail': f'orderId={order_id} payId={pay_id}'})
                if pause_on_success:
                    input(f"  >>> 账号 {phone} 开通成功，按回车继续下一个账号...")
            else:
                print(f"  ✗ 开通失败 phone={phone} 城市={province}/{city} status={status} {msg}")
                results.append({'phone': phone, 'city': city, 'province': province,
                                'result': '失败', 'detail': f'status={status} {msg}'})
                # 注意：不删除 Redis 凭证（账号与 sid 保留，供下次重试）

            # ---------- 4. vip_status 以 worker 写回的为准 ----------
            # worker 判的是「账号能否开麦金卡」（success/420 → valid，6000232 或 message
            # 含「异常」 → invalid，其余 → error）。这里只在 worker 没写时兜底，且兜底
            # 严格只认「真开通成功」：open_status 非 done 时原因不可辨（可能是 420 没过滑
            # 块，也可能是代理断了），一律记 error，不下 invalid 的结论。
            after = _load(phone) or {}
            if not (after.get('vip_status') or '').strip():
                verdict = 'valid' if status == 'done' else 'error'
                _set_vip(phone, verdict)
                extra = '' if verdict == 'valid' else '（open_status 非 done，原因不可辨，下轮重测）'
                print(f"  [兜底] worker 未回写 vip_status，记为 {verdict}{extra}")
                after = _load(phone) or {}          # 重新读，拿最终结论

            # 按最终 vip_status 维护 invalid 计数：invalid 累加，valid 清零，error 不动
            final_vs = (after.get('vip_status') or '').strip().lower()
            if final_vs == 'valid':
                _reset_invalid(phone)
                print(f"  [计数] vip_status=valid，已清空无效计数")
            elif final_vs == 'invalid':
                n_new = _bump_invalid(phone)
                if n_new < 0:
                    print("  [计数] 写 Redis 失败，本次 invalid 未累加")
                else:
                    print(f"  [计数] invalid 累计 {n_new}/{VIP_INVALID_CLEAR_AT} 次"
                          + (" → 下次会清空 token + 换城市" if n_new >= VIP_INVALID_CLEAR_AT else ""))

            # 本地这侧就登录失败（凭证不可用）→ 清 token 让服务端重新 ensure_token，
            # 否则下一轮 error 分支会保留旧 token、反复卡在未登录
            if '未登录' in flow_err:
                row = _load(phone) or {}
                if row.get('token'):
                    row['token'] = ''
                    _save(phone, row)
                    print("  [自愈] 凭证未登录，已清空 token 待服务端重新生成")
    finally:
        # 恢复模块本来的 input / SEND_LOCATION，避免影响其它调用
        try:
            rc_mod.SEND_LOCATION = _orig_send_loc
        except Exception:
            pass
        if _orig_input is None:
            try:
                del rc_mod.input
            except AttributeError:
                pass
        else:
            rc_mod.input = _orig_input

    # ===== 结果汇总 =====
    ok = sum(1 for r in results if r['result'] == '成功')
    fail = sum(1 for r in results if r['result'] == '失败')
    skip = sum(1 for r in results if r['result'] == '跳过')
    print("\n" + "=" * 60)
    print(f"[结果汇总] 共 {len(results)}：成功 {ok} / 失败 {fail} / 跳过 {skip}")
    for r in results:
        print(f"  {r['result']}  {r['phone']}  {r['province']}/{r['city']}  {r['detail']}")
    return results


def add_proxy():
    print("=" * 80)
    print("添加代理到代理池")
    print("=" * 80)

    # 初始化代理池
    pool = ProxyPool()

    # 代理列表
    proxies = [
        {
            'host': '61.155.141.105',
            'port': 20080,
            'username': 'jygdGVUi',
            'password': 'd5xP8koC0PHz',
            'protocol': 'http',
            'proxy_id': 'mcd_proxy_001'
        },
        {
            'host': '61.155.141.105',
            'port': 20081,
            'username': 'jygdGVUi',
            'password': 'd5xP8koC0PHz',
            'protocol': 'http',
            'proxy_id': 'mcd_proxy_002'
        },
        {
            'host': '61.155.141.105',
            'port': 20082,
            'username': 'jygdGVUi',
            'password': 'd5xP8koC0PHz',
            'protocol': 'http',
            'proxy_id': 'mcd_proxy_003'
        }
    ]

    print(f"\n准备添加 {len(proxies)} 个代理...\n")

    for proxy in proxies:
        result = pool.add_proxy(
            host=proxy['host'],
            port=proxy['port'],
            username=proxy['username'],
            password=proxy['password'],
            protocol=proxy['protocol'],
            proxy_id=proxy['proxy_id']
        )

    # 显示代理池状态
    print("\n" + "=" * 80)
    print("代理池当前状态")
    print("=" * 80)

    all_proxies = pool.get_all_proxies()
    print(f"\n总计: {len(all_proxies)} 个代理\n")

    for p in all_proxies:
        print(f"  [{p['id']}]")
        print(f"    - 地址: {p['host']}:{p['port']}")
        print(f"    - 状态: {p['status']}")
        print(f"    - 绑定账号: {p['bound_accounts']}")
        print(f"    - 成功/失败: {p['success_count']}/{p['fail_count']}")
        print()

    print("=" * 80)
    print("✅ 完成")
    print("=" * 80)


if __name__ == '__main__':
    # 单条写入凭证:
    # write_credential('19338807286', '0innBrouP0...', 'MEDDY196678424740382833', env='local')

    # sync_credential_to_online('15977852477')
    # 批量: 读 Excel → 按凭证状态分支(新/成功跳过/失败重试/无状态) → 逐账号跑麦金卡开通
    # 用法: python demo.py <local|online> [last|first]   默认 local last
    #   last  = 套餐自动取最后一个（麦金卡365天 ¥118, skuId=12536）
    #   first = 套餐自动取第一个
    env = sys.argv[1] if len(sys.argv) > 1 else 'local'
    sku_pick = sys.argv[2] if len(sys.argv) > 2 else 'last'
    run_mcgold_from_excel(env=env, sku_pick=sku_pick)
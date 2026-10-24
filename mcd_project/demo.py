import json
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
                          env='local', merchant_id=None, cities=None, mode='add'):
    """批量跑麦金卡开通：读 Excel → 逐账号按 mode 过滤 → 调 mcgold_flow。

    两种模式(mode)：
      - 'add'  新增：只处理【没有 token】的账号；写入凭证(token 空，由服务端 ensure_token
               补全)后再跑 mcgold_flow。已有 token 的跳过。
      - 'check' 检查：只处理【已有 token】的账号；不写凭证，直接跑 mcgold_flow。无 token 的跳过。

    城市分配：凭证里带 province/city 就用凭证里的；没有则从 cities(MAJOR_CITIES) 依次轮取，
             省份由 CITY_PROVINCE 推。

    其它：
    - env 同时决定：Redis 走本地/线上，mcgold_flow 请求本地(127.0.0.1:5001)/线上(dns.xme.fan:5001)。
    - mcgold_flow 内选套餐仍需手动输入(逐账号一次)。
    - 开通成功：打印成功并暂停等回车；失败且 env=='online' 删除该账号线上凭证(本地不删)。
    - 结尾汇总每个账号的下单结果(成功/失败/跳过 + 账号 + 城市)。

    Args:
        excel_path: Excel 路径(列: phone/sid/meddyId)
        env: 'local' / 'online'
        merchant_id: 商户ID，默认 100001
        cities: 城市列表(轮取)，默认 MAJOR_CITIES
        mode: 'add'(新增,只看无token) / 'check'(检查,只看有token)
    """
    if mode not in ('add', 'check'):
        raise ValueError(f"mode 只能是 'add' 或 'check'，收到: {mode!r}")
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
    print(f"[batch] mode={mode} env={env} Redis={conf['host']}:{conf['port']} 域名={base} "
          f"key={key} 账号数={len(accounts)}")

    def _open_status(phone):
        """查该账号麦金卡开通最终状态，返回 (status, msg, orderId, payId)。"""
        try:
            st = rc_mod.api_call('/api/member/open_status', params={'phone': phone})
            d = st.get('data', {}) if st.get('success') else {}
            return d.get('status'), d.get('msg', ''), d.get('orderId', ''), d.get('payId', '')
        except Exception as e:
            return None, str(e), '', ''

    results = []   # [{phone, city, province, result, detail}]
    for idx, (phone, sid, meddy) in enumerate(accounts, 1):
        if idx < 0:
            print(f'手动跳过{idx}')
            continue
        # 1. 读凭证
        raw = rconn.hget(key, phone)
        cur = {}
        if raw:
            try:
                cur = json.loads(raw)
            except Exception:
                cur = {}
        has_token = bool(cur.get('token'))

        # 城市：凭证里带 province/city 就用凭证的；没有则从 cities 依次轮取
        if cur.get('city'):
            city = cur.get('city')
            province = cur.get('province') or CITY_PROVINCE.get(city, city)
        else:
            city = cities[(idx - 1) % len(cities)]
            province = CITY_PROVINCE.get(city, city)

        print("\n" + "=" * 60)
        print(f"[{idx}/{len(accounts)}] phone={phone} 城市={province}/{city} has_token={has_token}")

        # 2. 按 mode 过滤 + 决定是否写入
        if mode == 'add':
            if has_token:                            # 新增模式：有 token 的跳过
                print(f"  [新增] 已有 token，跳过")
                results.append({'phone': phone, 'city': city, 'province': province,
                                'result': '跳过', 'detail': '已有 token(新增模式)'})
                continue
            write_credential(phone, sid, meddy, env=env, merchant_id=merchant_id)  # 写空 token
        else:  # mode == 'check'
            if not has_token:                        # 检查模式：无 token 的跳过
                print(f"  [检查] 无 token，跳过")
                results.append({'phone': phone, 'city': city, 'province': province,
                                'result': '跳过', 'detail': '无 token(检查模式)'})
                continue
            # 检查模式不写凭证，直接用现有的

        # 3. 调 mcgold_flow：喂该手机号(触发服务端 ensure_token)，省市走 _extra_params
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
        try:
            rc_mod.mcgold_flow()
        except SystemExit as e:                      # mcgold_flow 内失败会 sys.exit，拦下继续下一个
            print(f"  ✗ mcgold_flow 退出(code={e.code})，继续下一个")
        except Exception as e:
            print(f"  ✗ mcgold_flow 异常: {e}，继续下一个")

        # 4. 以服务端开通状态为准判定下单结果
        status, msg, order_id, pay_id = _open_status(phone)
        if status == 'done':
            print(f"  ✅ 开通成功 phone={phone} 城市={province}/{city} "
                  f"orderId={order_id} payId={pay_id}")
            results.append({'phone': phone, 'city': city, 'province': province,
                            'result': '成功', 'detail': f'orderId={order_id} payId={pay_id}'})
            # 成功：暂停，等我手动确认(处理支付/记录)后再继续下一个
            input(f"  >>> 账号 {phone} 开通成功，按回车继续下一个账号...")
        else:
            print(f"  ✗ 开通失败 phone={phone} 城市={province}/{city} status={status} {msg}")
            results.append({'phone': phone, 'city': city, 'province': province,
                            'result': '失败', 'detail': f'status={status} {msg}'})
            # 失败且走线上 Redis：删掉该账号凭证(本地测试不删)
            if env == 'online' and mode == 'add':
                rconn.hdel(key, phone)
                print(f"  🗑 线上失败，已删除凭证 {key} field={phone}")

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
    # 批量: 读 Excel → 按 mode 过滤 → 逐账号跑麦金卡开通(选套餐需手输)
    # 用法: python demo.py <local|online> <add|check>   默认 local add
    import sys
    env = sys.argv[1] if len(sys.argv) > 1 else 'local'
    mode = sys.argv[2] if len(sys.argv) > 2 else 'add'
    run_mcgold_from_excel(env=env, mode=mode)
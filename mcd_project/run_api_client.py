#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳下单流程测试脚本 - API 客户端版本
通过 Flask API 接口调用，而不是直接调用 mcd_api
"""

import sys
import json
import os
import requests
from datetime import datetime

from config import DATA_DIR

# API 配置
API_BASE = 'http://127.0.0.1:5001'

# 默认配置
DEFAULT_PHONE = '17717295039'
DEFAULT_LATITUDE = 31.026543
DEFAULT_LONGITUDE = 121.379931
STORE_FILE = os.path.join(DATA_DIR, 'save_selected_store.json')
ORDER_GOODS_FILE = os.path.join(DATA_DIR, 'save_order_data.json')
PAY_INFO_FILE = os.path.join(DATA_DIR, 'save_payment_info.json')
PAY_MONEY_FILE = os.path.join(DATA_DIR, 'save_payment_money.json')


def api_call(endpoint, method='GET', data=None, params=None):
    """通用 API 调用"""
    url = f"{API_BASE}{endpoint}"

    try:
        if method == 'POST':
            response = requests.post(url, json=data, timeout=30)
        else:
            response = requests.get(url, params=params, timeout=30)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ API 请求失败: {str(e)}")
        return {'success': False, 'message': f'API 请求失败: {str(e)}'}


def get_login_info():
    """获取登录信息"""
    phone = input(f"请输入手机号 (回车使用默认 {DEFAULT_PHONE}): ").strip()
    if not phone:
        phone = DEFAULT_PHONE

    print(f"\n使用手机号: {phone}")
    print("正在检查登录状态...")

    # 检查登录状态
    result = api_call('/api/login/status', params={'phone': phone})
    if result.get('success') and result.get('data', {}).get('is_logged_in'):
        print(f"✅ 已登录")
        print(result)
        data = result['data']
        token = data.get('token')
        sid = data.get('sid')
        meddy_id = data.get('meddy_id')

        user_info = data.get('user_info', {})
        if user_info:
            print(f"   用户: {user_info.get('name', '')}")
            print(f"   积分: {user_info.get('points', '0')}")
        return phone, token, sid, meddy_id

    # 需要登录
    print(f"⚠️  未登录，开始登录流程...")

    # 发送验证码
    result = api_call('/api/login/send_code', method='POST', data={'phone': phone})

    if not result.get('success'):
        print(f"❌ {result.get('message', '发送验证码失败')}")
        sys.exit(1)

    token = result.get('data', {}).get('token')
    reused = result.get('data', {}).get('reused', False)

    if reused:
        print(f"✅ 验证码已发送（使用已有设备）")
    else:
        print(f"✅ 验证码已发送")

    # 输入验证码
    code = input("请输入验证码: ").strip()
    if not code:
        print("❌ 验证码不能为空")
        sys.exit(1)

    # 验证登录
    result = api_call('/api/login/verify', method='POST', data={
        'phone': phone,
        'verify_code': code,
        'token': token
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '登录失败')}")
        sys.exit(1)

    print(f"✅ 登录成功")
    data = result['data']
    token = data.get('token')
    sid = data.get('sid')
    meddy_id = data.get('meddy_id')

    print(f"   Token: {token[:20]}...")
    print(f"   SID: {sid[:20]}...")
    print(f"   MeddyID: {meddy_id}")

    return phone, token, sid, meddy_id


def store_flow():
    """店铺选择流程"""
    print("=" * 60)
    print("麦当劳店铺选择流程")
    print("=" * 60)

    # 获取登录信息
    phone, token, sid, meddy_id = get_login_info()

    # 选择入口
    print("\n请选择店铺获取方式:")
    print("1. 附近店铺 (使用默认经纬度)")
    print("2. 搜索店铺")
    choice = input("请输入选项 (1/2): ").strip()

    stores = []

    if choice == '1':
        # 1.1 附近店铺
        print(f"\n正在获取附近店铺... (经纬度: {DEFAULT_LATITUDE}, {DEFAULT_LONGITUDE})")
        result = api_call('/api/mcd/get_nearby_stores', params={
            'phone': phone,
            'latitude': DEFAULT_LATITUDE,
            'longitude': DEFAULT_LONGITUDE,
            'show_type': 2,
            'order_type': 1
        })

        if not result.get('success'):
            print(f"❌ {result.get('message', '获取附近店铺失败')}")
            sys.exit(1)

        stores = result.get('data', {}).get('stores', [])
        print(f"✅ {result.get('message', '获取成功')}")

    elif choice == '2':
        # 1.2 搜索店铺
        keyword = input("\n请输入搜索关键词: ").strip()
        if not keyword:
            print("❌ 搜索关键词不能为空")
            sys.exit(1)

        print("\n请选择搜索范围:")
        print("1. 当前城市")
        print("2. 全部城市")
        search_choice = input("请输入选项 (1/2): ").strip()

        if search_choice == '1':
            # 1.2.1 当前城市搜索
            print(f"\n正在获取当前城市信息... (经纬度: {DEFAULT_LATITUDE}, {DEFAULT_LONGITUDE})")
            result = api_call('/api/mcd/get_city_by_location', params={
                'phone': phone,
                'latitude': DEFAULT_LATITUDE,
                'longitude': DEFAULT_LONGITUDE
            })

            if not result.get('success'):
                print(f"❌ {result.get('message', '获取城市信息失败')}")
                sys.exit(1)

            city_data = result.get('data', {}).get('city', {})
            city_code = city_data.get('code', '')
            city_name = city_data.get('name', '')
            print(f"✅ 当前城市: {city_name} ({city_code})")

            print(f"\n正在搜索店铺...")
            result = api_call('/api/mcd/search_stores', params={
                'phone': phone,
                'city_code': city_code,
                'keyword': keyword
            })

            if not result.get('success'):
                print(f"❌ {result.get('message', '搜索失败')}")
                sys.exit(1)

            stores = result.get('data', {}).get('stores', [])
            print(f"✅ {result.get('message', '搜索成功')}")

        elif search_choice == '2':
            # 1.2.2 全部城市
            print("\n正在获取所有城市...")
            result = api_call('/api/mcd/get_all_cities', params={'phone': phone})

            if not result.get('success'):
                print(f"❌ {result.get('message', '获取城市列表失败')}")
                sys.exit(1)

            cities_data = result.get('data', {})

            # 展示城市列表
            city_groups = [{"initial": '热门城市', "cities": cities_data.get('hotCities', [])}] + cities_data.get('groups', [])
            all_cities = []

            print(f"\n✅ {result.get('message', '获取成功')}")
            print("\n城市列表:")
            idx = 1
            for group in city_groups:
                cities = group.get('cities', [])
                for city in cities:
                    print(f"{idx}. {city.get('name', '')} ({city.get('code', '')})")
                    all_cities.append(city)
                    idx += 1

            # 选择城市
            city_idx = input(f"\n请选择城市 (1-{len(all_cities)}): ").strip()
            try:
                city_idx = int(city_idx) - 1
                if city_idx < 0 or city_idx >= len(all_cities):
                    print("❌ 无效的选择")
                    sys.exit(1)
            except ValueError:
                print("❌ 请输入数字")
                sys.exit(1)

            selected_city = all_cities[city_idx]
            city_code = selected_city.get('code', '')
            city_name = selected_city.get('name', '')
            print(f"\n已选择城市: {city_name} ({city_code})")

            print(f"\n正在搜索店铺...")
            result = api_call('/api/mcd/search_stores', params={
                'phone': phone,
                'city_code': city_code,
                'keyword': keyword
            })

            if not result.get('success'):
                print(f"❌ {result.get('message', '搜索失败')}")
                sys.exit(1)

            stores = result.get('data', {}).get('stores', [])
            print(f"✅ {result.get('message', '搜索成功')}")

        else:
            print("❌ 无效的选择")
            sys.exit(1)

    else:
        print("❌ 无效的选择")
        sys.exit(1)

    # 展示店铺列表
    if not stores:
        print("\n❌ 没有找到店铺")
        sys.exit(1)

    print(f"\n找到 {len(stores)} 家店铺:")
    for i, store in enumerate(stores, 1):
        store_name = store.get('name', '')
        store_code = store.get('code', '')
        distance = store.get('distanceText', 0)
        print(f"{i}. {store_name} ({store_code}) - {distance}")

    # 选择店铺
    store_idx = input(f"\n请选择店铺 (1-{len(stores)}): ").strip()
    try:
        store_idx = int(store_idx) - 1
        if store_idx < 0 or store_idx >= len(stores):
            print("❌ 无效的选择")
            sys.exit(1)
    except ValueError:
        print("❌ 请输入数字")
        sys.exit(1)

    selected_store = stores[store_idx]

    # 保存店铺信息
    store_info = {
        'storeCode': selected_store.get('code', ''),
        'storeName': selected_store.get('name', ''),
        'beCode': selected_store.get('beCode', ''),
        'latitude': selected_store.get('latitude', DEFAULT_LATITUDE),
        'longitude': selected_store.get('longitude', DEFAULT_LONGITUDE),
        'phone': phone
    }

    with open(STORE_FILE, 'w', encoding='utf-8') as f:
        json.dump(store_info, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 已选择店铺: {selected_store.get('name', '')}")
    print(f"✅ 店铺信息已保存到: {STORE_FILE}")
    print("=" * 60)


def order_flow():
    """下单流程 - 支持多商品添加"""
    print("=" * 60)
    print("麦当劳下单流程")
    print("=" * 60)

    # 1. 验证登录状态
    print("\n正在验证登录状态...")
    phone, token, sid, meddy_id = get_login_info()
    print("✅ 登录验证通过")

    # 2. 读取店铺信息
    if not os.path.exists(STORE_FILE):
        print(f"\n❌ 未找到店铺信息文件: {STORE_FILE}")
        print("请先运行: python run_api_client.py store")
        sys.exit(1)

    with open(STORE_FILE, 'r', encoding='utf-8') as f:
        store_info = json.load(f)

    store_code = store_info.get('storeCode', '')
    store_name = store_info.get('storeName', '')
    be_code = store_info.get('beCode', '')
    latitude = store_info.get('latitude', DEFAULT_LATITUDE)
    longitude = store_info.get('longitude', DEFAULT_LONGITUDE)

    print(f"\n已加载店铺: {store_name} ({store_code})")

    # 3. 获取店铺菜单
    print(f"\n[步骤 1] 正在获取店铺菜单...")
    result = api_call('/api/mcd/get_store_menu', params={
        'phone': phone,
        'store_code': store_code,
        'be_code': be_code,
        'order_type': 1
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '获取菜单失败')}")
        sys.exit(1)

    print(f"✅ {result.get('message', '获取成功')}")

    menu_data = result.get('data', {})
    menus = menu_data.get('menu', [])
    if not menus:
        print("❌ 菜单为空")
        sys.exit(1)

    product_name_key = 'productName'
    product_code_key = 'productCode'
    product_img_key = 'productImage'

    # 记录已添加商品数量
    added_products = {}
    cart_data = None
    is_first_loop = True

    # 循环添加商品
    while True:
        # 非首次循环询问是否继续
        if not is_first_loop:
            continue_add = input("\n是否继续添加商品? (y/n): ").strip().lower()
            if continue_add != 'y':
                break

        # 展示菜单分类
        print(f"\n菜单分类 (共 {len(menus)} 个):")
        for i, category in enumerate(menus, 1):
            category_name = category.get('categoryName', '').replace('\n', ' ')

            # 统计商品数
            total_products = 0
            direct_products = category.get('productList', [])
            sub_categories = category.get('categories', [])

            total_products += len(direct_products)
            for sub_cat in sub_categories:
                total_products += len(sub_cat.get('productList', []))

            if total_products > 0:
                print(f"{i}. {category_name} (共{total_products}个商品)")
            else:
                print(f"{i}. {category_name} (无商品)")

        # 选择分类
        category_idx = input(f"\n请选择分类 (1-{len(menus)}): ").strip()
        try:
            category_idx = int(category_idx) - 1
            if category_idx < 0 or category_idx >= len(menus):
                print("❌ 无效的选择")
                continue
        except ValueError:
            print("❌ 请输入数字")
            continue

        selected_category = menus[category_idx]
        category_name = selected_category.get('categoryName', '').replace('\n', ' ')

        # 收集所有商品
        all_products = []

        # 直接商品
        direct_products = selected_category.get('productList', [])
        for product in direct_products:
            all_products.append({
                'product': product,
                'sub_category_name': None
            })

        # 小类中的商品
        sub_categories = selected_category.get('categories', [])
        for sub_cat in sub_categories:
            sub_cat_name = sub_cat.get('categoryName', '').replace('\n', ' ')
            sub_products = sub_cat.get('productList', [])
            for product in sub_products:
                all_products.append({
                    'product': product,
                    'sub_category_name': sub_cat_name
                })

        if not all_products:
            print(f"❌ {category_name} 分类下没有商品")
            continue

        # 展示商品列表
        print(f"\n{category_name} - 商品列表:")
        for i, item in enumerate(all_products, 1):
            product = item['product']
            sub_cat_name = item['sub_category_name']
            pname = product.get(product_name_key, '')
            pcode = product.get(product_code_key, '')
            current_qty = added_products.get(pcode, 0)
            limit_qty = product.get('limitQuantity', 0)

            # 构建显示名称
            if sub_cat_name:
                display_name = f"{sub_cat_name}-{pname}"
            else:
                display_name = pname

            # 显示限购信息
            if limit_qty > 0:
                remaining = limit_qty - current_qty
                if remaining <= 0:
                    print(f"{i}. {display_name} ({pcode}) [已达限购]")
                else:
                    print(f"{i}. {display_name} ({pcode}) [还可购{remaining}件]")
            else:
                if current_qty > 0:
                    print(f"{i}. {display_name} ({pcode}) [已添加{current_qty}件]")
                else:
                    print(f"{i}. {display_name} ({pcode})")

        # 选择商品
        product_idx = input(f"\n请选择商品 (1-{len(all_products)}): ").strip()
        try:
            product_idx = int(product_idx) - 1
            if product_idx < 0 or product_idx >= len(all_products):
                print("❌ 无效的选择")
                continue
        except ValueError:
            print("❌ 请输入数字")
            continue

        selected_product = all_products[product_idx]['product']
        product_code = selected_product.get(product_code_key, '')
        product_name = selected_product.get(product_name_key, '')
        product_image = selected_product.get(product_img_key, '')
        limit_qty = selected_product.get('limitQuantity', 0)
        current_qty = added_products.get(product_code, 0)

        # 检查限购
        if limit_qty > 0 and current_qty >= limit_qty:
            print(f"❌ 该商品限购{limit_qty}件，已达上限")
            continue

        # 4. 获取商品详情
        print(f"\n正在获取商品详情...")
        result = api_call('/api/mcd/get_product_detail', params={
            'phone': phone,
            'product_code': product_code,
            'store_code': store_code,
            'be_code': be_code,
            'order_type': 1
        })

        if not result.get('success'):
            print(f"❌ {result.get('message', '获取商品详情失败')}")
            continue

        print(f"✅ {result.get('message', '获取成功')}")
        detail_data = result.get('data', {}).get('product', {})

        # 处理产品组
        actual_product_code = product_code
        actual_product_name = product_name
        actual_product_image = product_image
        modification = None

        if product_code.startswith('G'):
            products = detail_data.get('products', [])
            if products:
                first_sku = products[0]
                actual_product_code = first_sku.get('code', product_code)
                actual_product_name = first_sku.get('name', product_name)
                actual_product_image = first_sku.get('image', product_image)

                # 提取默认modification
                from mcd_api import extract_default_modification
                modification = extract_default_modification(first_sku)

                print(f"  检测到产品组，自动选择默认规格: {actual_product_name} ({actual_product_code})")
        else:
            from mcd_api import extract_default_modification
            modification = extract_default_modification(detail_data)

        detail_name = detail_data.get('name', actual_product_name)
        origin_price = detail_data.get('price', 0) / 100
        real_price = origin_price
        real_bt = ''
        if detail_data.get('rightInfo'):
            real_price = detail_data['rightInfo']['price'] / 100
            real_bt = f"({detail_data['rightInfo']['buttonText']})"

        print(f"\n商品信息:")
        print(f"  名称: {detail_name}")
        print(f"  原价: ¥{origin_price:.2f}")
        if real_bt:
            print(f"  最低价: ¥{real_price:.2f} {real_bt}")

        # 5. 首次循环清空购物车
        if is_first_loop:
            print(f"\n正在清空购物车...")
            result = api_call('/api/mcd/clear_cart', method='POST', data={
                'phone': phone,
                'store_code': store_code,
                'be_code': be_code,
                'order_type': 1,
                'store_name': store_name
            })
            if result.get('success'):
                cart_result = result.get('data', {})
                print(f"✅ {result.get('message', '清空成功')}, 购物车中商品数: {len(cart_result.get('products', []))}")
            else:
                print(f"⚠️  {result.get('message', '清空失败')} (继续流程)")

        # 6. 加入购物车
        print(f"\n正在添加商品到购物车...")
        result = api_call('/api/mcd/add_to_cart', method='POST', data={
            'phone': phone,
            'store_code': store_code,
            'be_code': be_code,
            'product_code': actual_product_code,
            'product_name': actual_product_name,
            'product_image': actual_product_image,
            'modification': modification,
            'suggestion_embedding': '',
            'quantity': 1,
            'order_type': 1,
            'store_name': store_name
        })

        if not result.get('success'):
            print(f"❌ {result.get('message', '添加失败')}")
            continue

        # 更新已添加商品记录
        added_products[actual_product_code] = added_products.get(actual_product_code, 0) + 1

        cart_data = result.get('data', {})
        cart_detail = cart_data.get('cartDetail', {})
        cart_products = cart_detail.get('products', [])
        print(f"✅ {result.get('message', '添加成功')}, 购物车中商品数: {len(cart_products)}")

        is_first_loop = False

    # 循环结束后，展示购物车信息
    if cart_data:
        cart_detail = cart_data.get('cartDetail', {})
        cart_products = cart_detail.get('products', [])
        print(f"\n购物车商品数: {len(cart_products)}")
        print("购物车商品列表:")
        for idx, product in enumerate(cart_products, 1):
            pname = product.get('name', '')
            qty = product.get('quantity', 0)
            print(f"  {idx}. {pname} x{qty}")
    else:
        print("\n❌ 购物车为空")
        sys.exit(0)

    # 7. 获取订单验证信息
    print(f"\n正在获取订单验证信息...")
    result = api_call('/api/mcd/get_order_validation_info', params={
        'phone': phone,
        'store_code': store_code,
        'be_code': be_code,
        'order_type': 1,
        'cart_type': 1
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '获取订单验证信息失败')}")
        sys.exit(1)

    print(f"✅ {result.get('message', '获取成功')}")

    validation_data = result.get('data', {})

    # 展示商品列表
    validation_info = validation_data.get('validation', {})
    product_status_list = validation_info.get('productStatusList', [])
    if product_status_list:
        all_product_name = [pdata['name'] for pdata in product_status_list if pdata.get('name', '')]
        print("\n订单商品列表:")
        for idx, pname in enumerate(all_product_name, 1):
            print(f"  {idx}. {pname}")

    # 展示就餐方式
    confirm_info = validation_data.get('confirmInfo', {})
    store_pickup_info = confirm_info.get('storePickUpInfo', {})
    eat_type_options = store_pickup_info.get('eatTypeOptions', [])

    print("\n就餐方式:")
    for option in eat_type_options:
        sub_text = option.get('subText', '')
        print(f"  - {sub_text}")

    # 展示总价
    product_price_info = confirm_info.get('productPriceInfo', {})
    real_total_amount = product_price_info.get('realTotalAmount', 0)
    total_amount = product_price_info.get('totalAmount', 0)

    real_total_float = float(real_total_amount) if real_total_amount else 0
    total_float = float(total_amount) if total_amount else 0

    if real_total_float != total_float:
        discount = total_float - real_total_float
        print(f"\n原价: ¥{total_float}")
        print(f"实付: ¥{real_total_float} (优惠 ¥{discount})")
    else:
        print(f"\n总价: ¥{real_total_float}")

    # 确认是否继续
    confirm = input("\n是否继续? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ 已取消")
        sys.exit(0)

    # 8. 获取门店信息
    print(f"\n正在获取门店信息...")
    result = api_call('/api/mcd/get_nearest_store_info', params={
        'phone': phone,
        'store_code': store_code,
        'latitude': latitude,
        'longitude': longitude,
        'be_code': be_code
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '获取门店信息失败')}")
        sys.exit(1)

    print(f"✅ {result.get('message', '获取成功')}")
    store_data = result.get('data', {})
    store_text = store_data.get('text', '')
    nearest_store_info = store_data.get('nearestStoreInfo', {})
    store_name_confirm = nearest_store_info.get('storeName', '')

    print(f"\n门店名称: {store_name_confirm}")
    print(f"提示: {store_text}")

    # 确认门店
    confirm = input("\n是否确认门店? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ 已取消")
        sys.exit(0)

    # 9. 保存订单数据
    print("\n正在保存订单数据...")

    cart_product_list = product_price_info.get('cartProductList', [])

    if not cart_product_list:
        print("❌ 订单验证信息中未获取到商品数据")
        sys.exit(1)

    # 提取会员卡信息
    right_card_info = product_price_info.get('rightCardInfo', {})
    card_list = right_card_info.get('cardList', [])

    menu_card_list = []
    for card in card_list:
        menu_card_list.append({
            'menuCardType': 0,
            'menuMembershipCode': card.get('menuMembershipCode', ''),
            'menuMembershipSpecId': card.get('menuMembershipSpecId', ''),
            'productCode': card.get('productCode', '')
        })

    order_data = {
        'phone': phone,
        'storeCode': store_code,
        'storeName': store_name,
        'beCode': be_code,
        'cartItems': cart_product_list,
        'menuCardList': menu_card_list,
        'beType': '1',
        'orderType': '1',
        'eatTypeCode': 'eat-in',
        'tablewareCode': 'no',
        'pinId': '',
        'latitude': latitude,
        'longitude': longitude,
        'realTotalAmount': str(real_total_amount)
    }

    with open(ORDER_GOODS_FILE, 'w', encoding='utf-8') as f:
        json.dump(order_data, f, ensure_ascii=False, indent=2)

    print(f"✅ 订单数据已保存到: {ORDER_GOODS_FILE}")
    print(f"   商品数: {len(cart_product_list)}")
    print(f"   订单总金额: ¥{real_total_amount}")
    print("\n提示: 运行 'python run_api_client.py payment' 提交订单并支付")
    print("=" * 60)


def payment_flow(arg2=''):
    """支付流程"""
    print("=" * 60)
    print("麦当劳提交订单并支付流程")
    print("=" * 60)

    # 1. 验证登录状态
    print("\n正在验证登录状态...")
    phone, token, sid, meddy_id = get_login_info()
    print("✅ 登录验证通过")

    # 如果使用已有订单
    if arg2:
        if arg2 == 'old':
            if not os.path.exists(PAY_INFO_FILE):
                print(f"\n❌ 未找到支付信息文件: {PAY_INFO_FILE}")
                print("请先运行: python run_api_client.py payment (不带 old 参数)")
                sys.exit(1)

            with open(PAY_INFO_FILE, 'r', encoding='utf-8') as f:
                payment_info = json.load(f)

            order_id = payment_info.get('orderId', '')
            pay_id = payment_info.get('payId', '')
        else:
            result = api_call('/api/mcd/get_order_list', params={
                'phone': phone,
                'cursor': '',
                'page_size': 10
            })

            if not result.get('success'):
                print(f"❌ {result.get('message', '获取订单列表失败')}")
                sys.exit(1)
            else:
                print(f"✅ {result.get('message', '获取成功')}")
                order_list_data = result.get('data', {})
                order_list = order_list_data.get('list', [])
                order_data = order_list[0]['omsOrder']
                order_id = order_data['orderId']
                pay_id = order_data['payId']

        print(f"\n✅ 已加载已有支付信息")
        print(f"   订单ID: {order_id}")
        print(f"   支付ID: {pay_id}")

        # 查询订单详情
        print(f"\n正在查询订单详情...")
        result = api_call('/api/mcd/get_order_detail', params={
            'phone': phone,
            'order_id': order_id
        })

        if not result.get('success'):
            print(f"❌ {result.get('message', '获取订单详情失败')}")
            sys.exit(1)

        print(f"✅ {result.get('message', '获取成功')}")
        order_detail = result.get('data', {})

        order_status_code = order_detail.get('orderStatusCode', '')
        order_status = order_detail.get('orderStatus', '')
        mp_order_status_code = order_detail.get('mpOrderStatusCode', '')

        print(f"\n订单状态:")
        print(f"  状态码: {order_status_code}")
        print(f"  状态: {order_status}")
        print(f"  MP状态码: {mp_order_status_code}")

        if order_status_code == '1' and mp_order_status_code == '10':
            print(f"\n✅ 订单状态为待支付，继续支付流程")
        else:
            if order_status_code == '7' and mp_order_status_code == '60':
                print(f"\n❌ 订单已取消，无法继续支付")
            elif order_status_code == '6' and mp_order_status_code == '40':
                pickup_code = order_detail["pickupCode"]
                print(f"\n✅ 订单状态为已完成，获取取餐码-{pickup_code}")
            else:
                print(f"\n⚠️  订单不是待支付状态，无法继续支付")
                print(f"   订单状态: {order_status}")

            sys.exit(1)

    else:
        # 正常流程：提交订单
        if not os.path.exists(ORDER_GOODS_FILE):
            print(f"\n❌ 未找到订单商品文件: {ORDER_GOODS_FILE}")
            print("请先运行: python run_api_client.py order")
            sys.exit(1)

        with open(ORDER_GOODS_FILE, 'r', encoding='utf-8') as f:
            order_data = json.load(f)

        store_code = order_data.get('storeCode', '')
        cart_items = order_data.get('cartItems', [])
        menu_card_list = order_data.get('menuCardList', [])
        real_total_amount = order_data.get('realTotalAmount', '0')
        be_type = order_data.get('beType', '1')
        order_type = order_data.get('orderType', '1')
        eat_type_code = order_data.get('eatTypeCode', 'eat-in')
        tableware_code = order_data.get('tablewareCode', 'no')
        pin_id = order_data.get('pinId', '')
        latitude = order_data.get('latitude', DEFAULT_LATITUDE)
        longitude = order_data.get('longitude', DEFAULT_LONGITUDE)

        print(f"\n✅ 已加载订单商品数据")
        print(f"   门店: {order_data.get('storeName', '')}")
        print(f"   商品数: {len(cart_items)}")
        print(f"   会员卡数: {len(menu_card_list)}")
        print(f"   订单总金额: ¥{real_total_amount}")

        # 提交订单
        print(f"\n[步骤 1] 正在提交订单...")
        result = api_call('/api/mcd/submit_order', method='POST', data={
            'phone': phone,
            'store_code': store_code,
            'cart_items': cart_items,
            'menu_card_list': menu_card_list,
            'real_total_amount': real_total_amount,
            'be_type': be_type,
            'order_type': order_type,
            'eat_type_code': eat_type_code,
            'tableware_code': tableware_code,
            'pin_id': pin_id,
            'latitude': latitude,
            'longitude': longitude
        })

        if not result.get('success'):
            print(f"❌ {result.get('message', '提交订单失败')}")
            sys.exit(1)

        print(f"✅ {result.get('message', '提交成功')}")

        order_result = result.get('data', {})
        order_id = order_result.get('orderId', '')
        pay_id = order_result.get('payId', '')

        if not order_id or not pay_id:
            print("❌ 订单提交成功但未返回订单ID或支付ID")
            sys.exit(1)

        print(f"\n订单ID: {order_id}")
        print(f"支付ID: {pay_id}")

        # 保存支付信息
        payment_info = {
            'orderId': order_id,
            'payId': pay_id,
            'realTotalAmount': real_total_amount,
            'storeName': order_data.get('storeName', ''),
            'timestamp': datetime.now().isoformat()
        }

        with open(PAY_INFO_FILE, 'w', encoding='utf-8') as f:
            json.dump(payment_info, f, ensure_ascii=False, indent=2)

        print(f"✅ 支付信息已保存到: {PAY_INFO_FILE}")

    # 获取支付渠道
    print(f"\n[步骤 2] 正在获取支付渠道...")
    result = api_call('/api/mcd/get_payment_channels', method='POST', data={
        'phone': phone,
        'order_id': order_id,
        'pay_id': pay_id,
        'mcd_id': meddy_id
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '获取支付渠道失败')}")
        sys.exit(1)

    print(f"✅ {result.get('message', '获取成功')}")

    channels_data = result.get('data', {})
    channel_infos = channels_data.get('channelInfos', [])
    if not channel_infos:
        print("❌ 没有可用的支付渠道")
        sys.exit(1)

    print(f"\n可用支付渠道 (共 {len(channel_infos)} 个):")
    for i, ch in enumerate(channel_infos, 1):
        name = ch.get('name', '')
        code = ch.get('code', '')
        free_pay_name = ch.get('freePayName', '')
        print(f"{i}. {name} ({code}) - {free_pay_name}")

    # 选择支付渠道
    channel_idx = input(f"\n请选择支付渠道 (1-{len(channel_infos)}): ").strip()
    try:
        channel_idx = int(channel_idx) - 1
        if channel_idx < 0 or channel_idx >= len(channel_infos):
            print("❌ 无效的选择")
            sys.exit(1)
    except ValueError:
        print("❌ 请输入数字")
        sys.exit(1)

    selected_channel = channel_infos[channel_idx]
    channel_code = selected_channel.get('code', '')
    channel_name = selected_channel.get('name', '')

    print(f"\n已选择: {channel_name} ({channel_code})")

    # 创建支付
    print(f"\n[步骤 3] 正在创建支付订单...")
    result = api_call('/api/mcd/create_payment', method='POST', data={
        'phone': phone,
        'pay_id': pay_id,
        'pay_channel': channel_code
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '创建支付失败')}")
        sys.exit(1)

    print(f"✅ {result.get('message', '创建成功')}")

    payment_data = result.get('data', {})

    print(f"\n支付信息:")
    print(f"  订单ID: {payment_data.get('orderId', '')}")
    print(f"  支付ID: {payment_data.get('payId', '')}")
    print(f"  支付状态: {payment_data.get('payStatus', '')}")
    print(f"  支付渠道: {payment_data.get('channelPayDestination', '')}")

    channel_pay_data = payment_data.get('channelPayData', '')
    if channel_pay_data:
        print(f"\n支付数据已生成 (长度: {len(channel_pay_data)} 字符)")

        try:
            pay_data_json = json.loads(channel_pay_data)

            method = pay_data_json.get('method', '')
            biz_content_str = pay_data_json.get('biz_content', '')

            if biz_content_str:
                biz_content = json.loads(biz_content_str)
                out_trade_no = biz_content.get('out_trade_no', '')
                total_amount = biz_content.get('total_amount', '')
                subject = biz_content.get('subject', '')

                print(f"\n支付信息:")
                print(f"  支付方式: {method}")
                print(f"  商户: {subject}")
                print(f"  金额: ¥{total_amount}")
                print(f"  交易号: {out_trade_no}")

            # 保存支付字符串
            with open(PAY_MONEY_FILE, 'w', encoding='utf-8') as f:
                f.write(channel_pay_data)

            view_data = input("\n是否查看完整支付数据? (y/n): ").strip().lower()

            if view_data == 'y':
                print(f"\n完整支付数据:")
                print(json.dumps(pay_data_json, ensure_ascii=False, indent=2))

        except json.JSONDecodeError:
            print("⚠️  无法解析支付数据")
        except Exception as e:
            print(f"⚠️  处理支付数据时出错: {str(e)}")

    # 查询订单列表
    print(f"\n[步骤 4] 正在查询账号订单列表...")
    result = api_call('/api/mcd/get_order_list', params={
        'phone': phone,
        'cursor': '',
        'page_size': 10
    })

    if not result.get('success'):
        print(f"❌ {result.get('message', '获取订单列表失败')}")
    else:
        print(f"✅ {result.get('message', '获取成功')}")

        order_list_data = result.get('data', {})
        order_list = order_list_data.get('list', [])
        has_next = order_list_data.get('hasNext', False)

        if order_list:
            print(f"\n最近 {len(order_list)} 个订单:")
            print("-" * 80)
            for idx, order_item in enumerate(order_list, 1):
                oms_order = order_item.get('omsOrder', {})
                order_id_display = oms_order.get('orderId', '')
                create_time = oms_order.get('createTime', '')
                order_status = oms_order.get('orderStatus', '')
                store_name = oms_order.get('storeName', '')
                real_total = oms_order.get('realTotalAmount', '')

                print(f"{idx}. 订单ID: {order_id_display}")
                print(f"   时间: {create_time}")
                print(f"   状态: {order_status}")
                print(f"   门店: {store_name}")
                print(f"   金额: ¥{real_total}")
                print("-" * 80)

            if has_next:
                print("提示: 还有更多订单，可以使用分页查询")
        else:
            print("\n暂无订单记录")

    print("\n流程结束")
    print("=" * 60)


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python run_api_client.py store   - 选择店铺")
        print("  python run_api_client.py order   - 下单流程")
        print("  python run_api_client.py payment - 支付流程（提交订单并保存支付信息）")
        print("  python run_api_client.py payment old - 使用已保存的订单ID和支付ID")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == 'store':
        store_flow()
    elif mode == 'order':
        order_flow()
    elif mode == 'payment':
        arg2 = sys.argv[2] if len(sys.argv) > 2 else ''
        payment_flow(arg2=arg2)
    else:
        print(f"❌ 未知模式: {mode}")
        print("支持的模式: store, order, payment")
        sys.exit(1)


if __name__ == '__main__':
    main()

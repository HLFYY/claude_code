#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳下单流程测试脚本
"""

import sys
import json
import os
from datetime import datetime

from login_manager import LoginManager
from mcd_api import (
    get_nearby_stores, search_stores, get_all_cities, get_city_by_location,
    get_store_menu, get_product_detail, clear_cart, add_to_cart,
    get_order_validation_info, get_order_promotion_info, get_nearest_store_info,
    submit_order, get_payment_channels, create_payment
)
from config import DATA_DIR

# 默认配置
DEFAULT_PHONE = '17717295039'
DEFAULT_LATITUDE = 31.026543
DEFAULT_LONGITUDE = 121.379931
STORE_FILE = os.path.join(DATA_DIR, 'save_selected_store.json')
ORDER_GOODS_FILE = os.path.join(DATA_DIR, 'save_order_data.json')
PAY_INFO_FILE = os.path.join(DATA_DIR, 'save_payment_info.json')
PAY_MONEY_FILE = os.path.join(DATA_DIR, 'save_payment_money.json')


def get_login_info():
    """获取登录信息"""
    phone = input(f"请输入手机号 (回车使用默认 {DEFAULT_PHONE}): ").strip()
    if not phone:
        phone = DEFAULT_PHONE

    print(f"\n使用手机号: {phone}")
    print("正在获取登录信息...")

    login_manager = LoginManager(phone)
    token, sid, meddy_id = login_manager.ensure_login(auto_relogin=True)

    if not token or not sid:
        print("❌ 登录失败")
        sys.exit(1)

    print(f"✅ 登录成功")
    print(f"   Token: {token[:20]}...")
    print(f"   SID: {sid[:20]}...")
    print(f"   MeddyID: {meddy_id}")

    return token, sid, meddy_id


def store_flow():
    """店铺选择流程"""
    print("=" * 60)
    print("麦当劳店铺选择流程")
    print("=" * 60)

    # 获取登录信息
    token, sid, meddy_id = get_login_info()

    # 选择入口
    print("\n请选择店铺获取方式:")
    print("1. 附近店铺 (使用默认经纬度)")
    print("2. 搜索店铺")
    choice = input("请输入选项 (1/2): ").strip()

    stores = []

    if choice == '1':
        # 1.1 附近店铺
        print(f"\n正在获取附近店铺... (经纬度: {DEFAULT_LATITUDE}, {DEFAULT_LONGITUDE})")
        success, stores, msg = get_nearby_stores(token, sid, DEFAULT_LATITUDE, DEFAULT_LONGITUDE)

        if not success or not stores:
            print(f"❌ {msg}")
            sys.exit(1)

        print(f"✅ {msg}")

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
            success, city_data, msg = get_city_by_location(token, sid, DEFAULT_LATITUDE, DEFAULT_LONGITUDE)

            if not success:
                print(f"❌ {msg}")
                sys.exit(1)

            city_code = city_data.get('code', '')
            city_name = city_data.get('name', '')
            print(f"✅ 当前城市: {city_name} ({city_code})")

            print(f"\n正在搜索店铺...")
            success, stores_data, msg = search_stores(token, sid, city_code, keyword)

            if not success:
                print(f"❌ {msg}")
                sys.exit(1)

            stores = stores_data.get('stores', [])
            print(f"✅ {msg}")

        elif search_choice == '2':
            # 1.2.2 全部城市
            print("\n正在获取所有城市...")
            success, cities_data, msg = get_all_cities(token, sid)

            if not success:
                print(f"❌ {msg}")
                sys.exit(1)

            # 展示城市列表
            city_groups = [{"initial": '热门城市', "cities": cities_data['hotCities']}] + cities_data.get('groups', [])
            all_cities = []

            print(f"\n✅ {msg}")
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
            success, stores_data, msg = search_stores(token, sid, city_code, keyword)

            if not success:
                print(f"❌ {msg}")
                sys.exit(1)

            stores = stores_data.get('stores', [])
            print(f"✅ {msg}")

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
        'token': token,
        'sid': sid,
        'meddyId': meddy_id
    }

    with open(STORE_FILE, 'w', encoding='utf-8') as f:
        json.dump(store_info, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 已选择店铺: {selected_store.get('storeName', '')}")
    print(f"✅ 店铺信息已保存到: {STORE_FILE}")
    print("=" * 60)


def order_flow():
    """下单流程 - 支持多商品添加"""
    print("=" * 60)
    print("麦当劳下单流程")
    print("=" * 60)

    # 1. 验证登录状态并获取最新的 token/sid
    print("\n正在验证登录状态...")
    token, sid, meddy_id = get_login_info()
    print("✅ 登录验证通过")

    # 2. 读取店铺信息（只读取店铺相关字段，不覆盖 token/sid）
    if not os.path.exists(STORE_FILE):
        print(f"\n❌ 未找到店铺信息文件: {STORE_FILE}")
        print("请先运行: python run.py store")
        sys.exit(1)

    with open(STORE_FILE, 'r', encoding='utf-8') as f:
        store_info = json.load(f)

    # 只读取店铺信息，保留新获取的 token/sid/meddy_id
    store_code = store_info.get('storeCode', '')
    store_name = store_info.get('storeName', '')
    be_code = store_info.get('beCode', '')
    latitude = store_info.get('latitude', DEFAULT_LATITUDE)
    longitude = store_info.get('longitude', DEFAULT_LONGITUDE)

    print(f"\n已加载店铺: {store_name} ({store_code})")

    # 2. 获取店铺菜单
    print(f"\n[步骤 1] 正在获取店铺菜单...")
    success, menu_data, msg = get_store_menu(token, sid, store_code, be_code, order_type=1)

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")

    menus = menu_data.get('menu', [])
    if not menus:
        print("❌ 菜单为空")
        sys.exit(1)

    product_name_key = 'productName'
    product_code_key = 'productCode'
    product_img_key = 'productImage'

    # 记录已添加商品数量（用于限购检查）
    added_products = {}  # {product_code: quantity}
    cart_data = None
    is_first_loop = True

    # 循环添加商品
    while True:
        # 非首次循环询问是否继续
        if not is_first_loop:
            continue_add = input("\n是否继续添加商品? (y/n): ").strip().lower()
            if continue_add != 'y':
                break

        # 3. 展示菜单分类和商品数（统计嵌套结构中的所有商品）
        print(f"\n菜单分类 (共 {len(menus)} 个):")
        for i, category in enumerate(menus, 1):
            category_name = category.get('categoryName', '').replace('\n', ' ')

            # 统计商品数：可能有直接商品或嵌套小类
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

        # 收集所有商品（包括嵌套小类中的商品）
        all_products = []

        # 直接商品
        direct_products = selected_category.get('productList', [])
        for product in direct_products:
            all_products.append({
                'product': product,
                'sub_category_name': None  # 无小类
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

        # 展示该分类下所有商品（格式：小类名称-商品名称）
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

            # 动态计算剩余限购数
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

        # 检查是否已达限购
        if limit_qty > 0 and current_qty >= limit_qty:
            print(f"❌ 该商品限购{limit_qty}件，已达上限")
            continue

        # 4. 获取商品详情，展示标题和价格
        print(f"\n正在获取商品详情...")
        success, detail_data, msg = get_product_detail(token, sid, product_code, store_code, be_code, order_type=1)

        if not success:
            print(f"❌ {msg}")
            continue

        print(f"✅ {msg}")
        detail_data = detail_data.get('product', {})

        # 处理产品组 (G开头的code)
        # 如果是产品组，需要选择具体的SKU
        actual_product_code = product_code
        actual_product_name = product_name
        actual_product_image = product_image
        modification = None
        suggestion_embedding = ''

        if product_code.startswith('G'):
            # 这是产品组，需要获取具体的SKU列表
            products = detail_data.get('products', [])
            if products:
                # 默认选择第一个SKU（通常是默认规格）
                first_sku = products[0]
                actual_product_code = first_sku.get('code', product_code)
                actual_product_name = first_sku.get('name', product_name)
                actual_product_image = first_sku.get('image', product_image)

                # 提取默认的modification
                from mcd_api import extract_default_modification
                modification = extract_default_modification(first_sku)

                print(f"  检测到产品组，自动选择默认规格: {actual_product_name} ({actual_product_code})")
            else:
                print(f"⚠️  产品组 {product_code} 没有可用的SKU")
        else:
            # 单品，提取默认modification（如果有）
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
            success, clear_result, msg = clear_cart(token, sid, store_code, be_code, order_type=1, store_name=store_name)
            if success:
                print(f"✅ {msg}, 购物车中商品数: {len(clear_result.get('products', []))}")
            else:
                print(f"⚠️  {msg} (继续流程)")

        # 6. 加入购物车
        print(f"\n正在添加商品到购物车...")
        success, cart_data, msg = add_to_cart(
            token, sid, store_code, be_code,
            product_code=actual_product_code,
            product_name=actual_product_name,
            product_image=actual_product_image,
            modification=modification,
            suggestion_embedding=suggestion_embedding,
            quantity=1,
            order_type=1,
            store_name=store_name
        )

        if not success:
            print(f"❌ {msg}")
            continue

        # 更新已添加商品记录（使用实际的SKU code）
        added_products[actual_product_code] = added_products.get(actual_product_code, 0) + 1

        cart_detail = cart_data.get('cartDetail', {})
        cart_products = cart_detail.get('products', [])
        print(f"✅ {msg}, 购物车中商品数: {len(cart_products)}")

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
    success, validation_data, msg = get_order_validation_info(
        token, sid, store_code,
        be_code=be_code,
        order_type=1,
        cart_type=1
    )

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")

    # 展示第一个商品标题
    validation_info = validation_data.get('validation', {})
    product_status_list = validation_info.get('productStatusList', [])
    if product_status_list:
        all_product_name = [pdata['name'] for pdata in product_status_list if pdata.get('name', '')]
        print(f"\n商品: {','.join(all_product_name)}")
        print("订单商品列表:")
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

    # 展示总价（使用实际支付价格）
    product_price_info = confirm_info.get('productPriceInfo', {})
    real_total_amount = product_price_info.get('realTotalAmount', 0)
    total_amount = product_price_info.get('totalAmount', 0)

    # 转换为浮点数进行计算
    real_total_float = float(real_total_amount) if real_total_amount else 0
    total_float = float(total_amount) if total_amount else 0

    # 显示价格信息
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
    success, store_data, msg = get_nearest_store_info(
        token, sid, store_code,
        latitude, longitude,
        be_code=be_code
    )

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")
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

    # 9. 保存订单数据（使用订单验证接口返回的完整商品数据）
    print("\n正在保存订单数据...")

    # 从验证信息中提取完整的商品数据
    cart_product_list = product_price_info.get('cartProductList', [])

    if not cart_product_list:
        print("❌ 订单验证信息中未获取到商品数据")
        sys.exit(1)

    # 提取 menuCardList（会员卡信息）
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

    # 直接使用订单验证接口返回的 cartProductList
    order_data = {
        'token': token,
        'sid': sid,
        'meddyId': meddy_id,
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
    print("\n提示: 运行 'python run.py payment' 提交订单并支付")
    print("=" * 60)


def payment_flow(use_existing=False):
    """支付流程（自动提交订单并支付）

    Args:
        use_existing: 是否使用已有的订单ID和支付ID（跳过提交订单步骤）
    """
    print("=" * 60)
    print("麦当劳提交订单并支付流程")
    print("=" * 60)

    # 1. 验证登录状态并获取最新的 token/sid
    print("\n正在验证登录状态...")
    token, sid, meddy_id = get_login_info()
    print("✅ 登录验证通过")

    # 定义支付信息文件路径

    # 如果使用已有订单，直接读取订单信息
    if use_existing:
        if not os.path.exists(PAY_INFO_FILE):
            print(f"\n❌ 未找到支付信息文件: {PAY_INFO_FILE}")
            print("请先运行: python run.py payment (不带 old 参数)")
            sys.exit(1)

        with open(PAY_INFO_FILE, 'r', encoding='utf-8') as f:
            payment_info = json.load(f)

        order_id = payment_info.get('orderId', '')
        pay_id = payment_info.get('payId', '')

        print(f"\n✅ 已加载已有支付信息")
        print(f"   订单ID: {order_id}")
        print(f"   支付ID: {pay_id}")

    else:
        # 正常流程：提交订单
        # 2. 检查并读取订单数据（只读取订单相关字段，不覆盖 token/sid）
        if not os.path.exists(ORDER_GOODS_FILE):
            print(f"\n❌ 未找到订单商品文件: {ORDER_GOODS_FILE}")
            print("请先运行: python run.py order")
            sys.exit(1)

        with open(ORDER_GOODS_FILE, 'r', encoding='utf-8') as f:
            order_data = json.load(f)

        # 只读取订单信息，保留新获取的 token/sid/meddy_id
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

        # 2. 提交订单
        print(f"\n[步骤 1] 正在提交订单...")
        success, order_result, msg = submit_order(
            token=token,
            sid=sid,
            store_code=store_code,
            cart_items=cart_items,
            menu_card_list=menu_card_list,
            real_total_amount=real_total_amount,
            be_type=be_type,
            order_type=order_type,
            eat_type_code=eat_type_code,
            tableware_code=tableware_code,
            pin_id=pin_id,
            latitude=latitude,
            longitude=longitude
        )

        if not success:
            print(f"❌ {msg}")
            sys.exit(1)

        print(f"✅ {msg}")

        # 获取订单ID和支付ID
        order_id = order_result.get('orderId', '')
        pay_id = order_result.get('payId', '')

        if not order_id or not pay_id:
            print("❌ 订单提交成功但未返回订单ID或支付ID")
            print(f"返回数据: {order_result}")
            sys.exit(1)

        print(f"\n订单ID: {order_id}")
        print(f"支付ID: {pay_id}")

        # 保存支付信息到文件，供后续使用
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

    # 步骤1: 获取支付渠道
    print(f"\n[步骤 2] 正在获取支付渠道...")
    success, channels_data, msg = get_payment_channels(token, sid, order_id, pay_id, meddy_id)
    print(channels_data)
    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")

    # 展示支付渠道
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

    # 步骤2: 预支付
    print(f"\n[步骤 3] 正在创建支付订单...")
    success, payment_data, msg = create_payment(token, sid, pay_id, pay_channel=channel_code)
    print(payment_data)

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")

    # 展示支付信息
    print(f"\n支付信息:")
    print(f"  订单ID: {payment_data.get('orderId', '')}")
    print(f"  支付ID: {payment_data.get('payId', '')}")
    print(f"  支付状态: {payment_data.get('payStatus', '')}")
    print(f"  支付渠道: {payment_data.get('channelPayDestination', '')}")

    channel_pay_data = payment_data.get('channelPayData', '')
    if channel_pay_data:
        print(f"\n支付数据已生成 (长度: {len(channel_pay_data)} 字符)")

        # 解析支付数据
        try:
            pay_data_json = json.loads(channel_pay_data)

            # 提取关键信息
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

            # 保存支付字符串到文件
            with open(PAY_MONEY_FILE, 'w', encoding='utf-8') as f:
                f.write(channel_pay_data)

            print(f"\n✅ 支付字符串已保存到: {PAY_MONEY_FILE}")
            print(f"\n⚠️  重要说明:")
            print("支付宝 APP 支付需要在移动端调用支付宝 SDK，不能通过链接或二维码完成。")
            print("\n有以下几种支付方式:")
            print("\n1. 【推荐】在麦当劳官方 APP 中完成支付")
            print("   - 这是麦当劳设计的正常支付流程")
            print("   - APP 会调用支付宝 SDK 并传入支付字符串")

            print("\n2. 如需测试支付接口，可以:")
            print("   - 开发移动端应用并集成支付宝 SDK")
            print("   - 调用 AlipaySDK.payV2(paymentString, fromScheme)")
            print(f"   - 支付字符串已保存在: {PAY_MONEY_FILE}")

            print("\n3. 查看完整支付数据 (用于调试)")
            view_data = input("\n是否查看完整支付数据? (y/n): ").strip().lower()

            if view_data == 'y':
                print(f"\n完整支付数据:")
                print(json.dumps(pay_data_json, ensure_ascii=False, indent=2))

        except json.JSONDecodeError:
            print("⚠️  无法解析支付数据，显示原始内容:")
            print(channel_pay_data)
        except Exception as e:
            print(f"⚠️  处理支付数据时出错: {str(e)}")
            print(f"\n原始支付数据:")
            print(channel_pay_data)

    print("\n流程结束")
    print("=" * 60)


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python run.py store   - 选择店铺")
        print("  python run.py order   - 下单流程")
        print("  python run.py payment - 支付流程（提交订单并保存支付信息）")
        print("  python run.py payment old - 使用已保存的订单ID和支付ID")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == 'store':
        store_flow()
    elif mode == 'order':
        order_flow()
    elif mode == 'payment':
        # 检查是否有 old 参数
        use_existing = len(sys.argv) > 2 and sys.argv[2] == 'old'
        payment_flow(use_existing=use_existing)
    else:
        print(f"❌ 未知模式: {mode}")
        print("支持的模式: store, order, payment")
        sys.exit(1)


if __name__ == '__main__':
    main()

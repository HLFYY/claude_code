#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳下单流程测试脚本
"""

import sys
import json
import os
from login_manager import LoginManager
from mcd_api import (
    get_nearby_stores, search_stores, get_all_cities, get_city_by_location,
    get_store_menu, get_product_detail, clear_cart, add_to_cart,
    get_order_validation_info, get_order_promotion_info, get_nearest_store_info,
    submit_order, get_payment_channels, create_payment
)

# 默认配置
DEFAULT_PHONE = '17717295039'
DEFAULT_LATITUDE = 31.026543
DEFAULT_LONGITUDE = 121.379931
STORE_FILE = 'selected_store.json'
ORDER_DATA_FILE = 'order_data.json'


def get_login_info():
    """获取登录信息"""
    phone = input(f"请输入手机号 (回车使用默认 {DEFAULT_PHONE}): ").strip()
    if not phone:
        phone = DEFAULT_PHONE

    print(f"\n使用手机号: {phone}")
    print("正在获取登录信息...")

    login_manager = LoginManager(phone)
    token, sid, meddy_id = login_manager.ensure_login()

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
    """下单流程"""
    print("=" * 60)
    print("麦当劳下单流程")
    print("=" * 60)

    # 1. 读取店铺信息
    if not os.path.exists(STORE_FILE):
        print(f"\n❌ 未找到店铺信息文件: {STORE_FILE}")
        print("请先运行: python run.py store")
        sys.exit(1)

    with open(STORE_FILE, 'r', encoding='utf-8') as f:
        store_info = json.load(f)

    token = store_info.get('token', '')
    sid = store_info.get('sid', '')
    meddy_id = store_info.get('meddyId', '')
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

    # 展示菜单分类
    print(f"\n菜单分类 (共 {len(menus)} 个):")
    for i, category in enumerate(menus, 1):
        category_name = category.get('categoryName', '')
        products = category.get('productList', [])

        if not products:
            print(f"{i}. {category_name} (无商品)")
            continue

        first_product = products[0].get(product_name_key, '')
        last_product = products[-1].get(product_name_key, '') if len(products) > 1 else first_product

        print(f"{i}. {category_name} (共{len(products)}个商品)")
        print(f"   首个商品: {first_product}")
        if len(products) > 1:
            print(f"   末个商品: {last_product}")

    # 选择分类
    category_idx = input(f"\n请选择分类 (1-{len(menus)}): ").strip()
    try:
        category_idx = int(category_idx) - 1
        if category_idx < 0 or category_idx >= len(menus):
            print("❌ 无效的选择")
            sys.exit(1)
    except ValueError:
        print("❌ 请输入数字")
        sys.exit(1)

    selected_category = menus[category_idx]
    products = selected_category.get('productList', [])

    if not products:
        print("❌ 该分类下没有商品")
        sys.exit(1)

    # 展示商品列表
    print(f"\n{selected_category.get('categoryName', '')} - 商品列表:")
    for i, product in enumerate(products, 1):
        product_name = product.get(product_name_key, '')
        product_code = product.get(product_code_key, '')
        print(f"{i}. {product_name} ({product_code})")

    # 选择商品
    product_idx = input(f"\n请选择商品 (1-{len(products)}): ").strip()
    try:
        product_idx = int(product_idx) - 1
        if product_idx < 0 or product_idx >= len(products):
            print("❌ 无效的选择")
            sys.exit(1)
    except ValueError:
        print("❌ 请输入数字")
        sys.exit(1)

    selected_product = products[product_idx]
    product_code = selected_product.get(product_code_key, '')
    product_name = selected_product.get(product_name_key, '')
    product_image = selected_product.get(product_img_key, '')

    # 3. 获取商品详情
    print(f"\n[步骤 2] 正在获取商品详情...")
    success, detail_data, msg = get_product_detail(token, sid, product_code, store_code, be_code, order_type=1)

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")
    print(detail_data)
    detail_data = detail_data.get('product', {})
    detail_name = detail_data.get('name', product_name)
    origin_price = str(detail_data.get('price', 0) / 100)
    real_price = origin_price
    real_bt = ''
    if detail_data.get('rightInfo'):
        real_price = detail_data['rightInfo']['price'] / 100
        real_bt = f"({detail_data['rightInfo']['buttonText']})"

    print(f"\n商品信息:")
    print(f"  名称: {detail_name}")
    print(f"  原价: ¥{origin_price}")
    print(f"  最低价: ¥{real_price} {real_bt}")

    # 确认加入购物车
    confirm = input("\n是否加入购物车? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ 已取消")
        sys.exit(0)

    # 4. 清空购物车并添加商品
    print(f"\n[步骤 3] 正在清空购物车...")
    success, cart_data, msg = clear_cart(token, sid, store_code, be_code, order_type=1, store_name=store_name)

    if success:
        print(f"✅ {msg}, 购物车中商品数:{len(cart_data['products'])}")
    else:
        print(f"⚠️  {msg} (继续流程)")

    print(f"\n[步骤 4] 正在添加商品到购物车...")
    success, cart_data, msg = add_to_cart(
        token, sid, store_code, be_code,
        product_code=product_code,
        product_name=product_name,
        product_image=product_image,
        quantity=1,
        order_type=1,
        store_name=store_name
    )

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    # 获取购物车商品列表（用于后续订单）
    cart_detail = cart_data.get('cartDetail', {})
    cart_products = cart_detail.get('products', [])

    print(f"✅ {msg}, 购物车中商品数:{len(cart_products)}, {cart_products[0].get('name', '')}")

    # 5. 获取订单验证信息
    print(f"\n[步骤 5] 正在获取订单验证信息...")
    success, validation_data, msg = get_order_validation_info(
        token, sid, store_code,
        order_type=1,
        be_code=be_code
    )

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")

    caer_product_name = validation_data["validation"]["productStatusList"][0]['name']

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
    total_yuan = product_price_info.get('totalAmount', 0)
    print(f"\n商品: {caer_product_name}")
    print(f"\n总价: ¥{total_yuan}")

    # 获取订单验证信息（包含完整商品数据和自动匹配的优惠券）
    print(f"\n[步骤 6] 正在获取订单验证信息...")

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
    # 从验证信息中提取完整的商品数据（包含所有25个必需字段）
    validation_confirm_info = validation_data.get('confirmInfo', {})
    validation_price_info = validation_confirm_info.get('productPriceInfo', {})
    cart_product_list = validation_price_info.get('cartProductList', [])

    if not cart_product_list:
        print("❌ 订单验证信息中未获取到商品数据")
        sys.exit(1)

    print(f"   商品数量: {len(cart_product_list)}")

    # 显示每个商品的优惠券信息
    for idx, item in enumerate(cart_product_list, 1):
        product_name = item.get('productName', '')
        coupon_list = item.get('couponList', [])
        print(f"   [{idx}] {product_name}")
        if coupon_list:
            for coupon in coupon_list:
                coupon_name = coupon.get('couponName', '')
                discount = coupon.get('couponFaceValue', 0) / 100
                print(f"       💰 优惠: {coupon_name} (减免 ¥{discount:.2f})")

    validation_total = validation_price_info.get('totalAmount', '0')
    print(f"   订单总金额: ¥{validation_total}")

    # 确认是否继续
    confirm = input("\n是否继续提交订单? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ 已取消")
        sys.exit(0)

    # 6. 获取最近门店信息
    print(f"\n[步骤 7] 正在获取门店信息...")
    success, store_data, msg = get_nearest_store_info(
        token, sid, store_code,
        latitude, longitude,
        # DEFAULT_LATITUDE, DEFAULT_LONGITUDE,
        be_code=be_code
    )

    if not success:
        print(f"❌ {msg}")
        sys.exit(1)

    print(f"✅ {msg}")
    store_text = store_data.get('text', '')
    store_name_confirm = store_data.get('nearestStoreInfo', '').get('storeName')
    print(f"\n门店名称: {store_name_confirm}")
    print(f"\n提醒: {store_text}")

    # 确认门店
    confirm = input("\n是否确认门店? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ 已取消")
        sys.exit(0)

    # 7. 保存订单数据（使用订单验证接口返回的完整商品数据）
    print("\n[步骤 8] 保存订单数据...")

    # 提取 menuCardList（会员卡信息）
    right_card_info = validation_confirm_info.get('productPriceInfo', {}).get('rightCardInfo', {})
    card_list = right_card_info.get('cardList', [])

    # 为每个卡片添加 menuCardType 字段
    menu_card_list = []
    for card in card_list:
        menu_card_list.append({
            'menuCardType': 0,
            'menuMembershipCode': card.get('menuMembershipCode', ''),
            'menuMembershipSpecId': card.get('menuMembershipSpecId', ''),
            'productCode': card.get('productCode', '')
        })

    # 计算实际总金额（totalAmount 已经是字符串格式的实际价格）
    real_total_amount = validation_price_info.get('totalAmount', '0')

    # 直接使用订单验证接口返回的 cartProductList，包含所有25个必需字段
    order_data = {
        'token': token,
        'sid': sid,
        'meddyId': meddy_id,
        'storeCode': store_code,
        'storeName': store_name,
        'beCode': be_code,
        'cartItems': cart_product_list,  # 直接使用验证接口返回的完整数据
        'menuCardList': menu_card_list,  # 会员卡信息
        'beType': '1',
        'orderType': '1',
        'eatTypeCode': 'eat-in',
        'tablewareCode': 'no',
        'pinId': '',
        'latitude': latitude,
        'longitude': longitude,
        'realTotalAmount': real_total_amount  # 实际订单总金额
    }

    with open(ORDER_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(order_data, f, ensure_ascii=False, indent=2)

    print(f"✅ 订单数据已保存到: {ORDER_DATA_FILE}")
    print(f"   商品数: {len(cart_product_list)}")
    print(f"   会员卡数: {len(menu_card_list)}")
    print(f"   订单总金额: ¥{real_total_amount}")
    print(f"   包含完整字段: couponList, suggestionEmbedding, trackingInfo 等")
    print("\n提示: 运行 'python run.py payment' 提交订单并支付")
    print("=" * 60)


def payment_flow():
    """支付流程（自动提交订单并支付）"""
    print("=" * 60)
    print("麦当劳提交订单并支付流程")
    print("=" * 60)

    # 1. 检查并读取订单数据
    if not os.path.exists(ORDER_DATA_FILE):
        print(f"\n❌ 未找到订单数据文件: {ORDER_DATA_FILE}")
        print("请先运行: python run.py order")
        sys.exit(1)

    with open(ORDER_DATA_FILE, 'r', encoding='utf-8') as f:
        order_data = json.load(f)

    token = order_data.get('token', '')
    sid = order_data.get('sid', '')
    meddy_id = order_data.get('meddyId', '')
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

    print(f"\n✅ 已加载订单数据")
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

    # 步骤1: 获取支付渠道
    print(f"\n[步骤 1] 正在获取支付渠道...")
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
    print(f"\n[步骤 2] 正在创建支付订单...")
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
        print("💡 提示: 将 channelPayData 传递给对应的支付 SDK 完成支付")

    print("\n流程结束")
    print("=" * 60)


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python run.py store   - 选择店铺")
        print("  python run.py order   - 下单流程")
        print("  python run.py payment - 支付流程")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == 'store':
        store_flow()
    elif mode == 'order':
        order_flow()
    elif mode == 'payment':
        payment_flow()
    else:
        print(f"❌ 未知模式: {mode}")
        print("支持的模式: store, order, payment")
        sys.exit(1)


if __name__ == '__main__':
    main()

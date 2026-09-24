#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
登录管理器测试脚本
"""

from login_manager import LoginManager


# 默认手机号
DEFAULT_PHONE = "16752934813"


def get_phone_input(prompt_text="请输入手机号"):
    """
    获取手机号输入，支持默认值

    Args:
        prompt_text: 提示文本

    Returns:
        手机号
    """
    phone = input(f"{prompt_text} (回车使用默认 {DEFAULT_PHONE}): ").strip()
    if not phone:
        phone = DEFAULT_PHONE
        print(f"使用默认手机号: {phone}")
    elif len(phone) != 11:
        print(f"❌ 手机号格式错误，使用默认手机号: {DEFAULT_PHONE}")
        phone = DEFAULT_PHONE
    return phone


def test_basic_usage():
    """测试基本用法"""
    print("\n" + "=" * 80)
    print("测试 1: 基本用法")
    print("=" * 80)

    # 获取手机号
    phone = get_phone_input()

    # 创建登录管理器
    manager = LoginManager(phone=phone)

    # 检查登录状态（不自动重新登录）
    token, sid, meddy_id = manager.ensure_login(auto_relogin=False)

    if token:
        print("\n✅ 测试通过：登录状态有效")
    else:
        print("\n⚠️  登录状态失效，需要重新登录")


def test_manual_login():
    """测试手动登录流程"""
    print("\n" + "=" * 80)
    print("测试 2: 手动登录流程")
    print("=" * 80)
    manager = LoginManager(phone=get_phone_input())

    # 步骤1: 发送验证码
    success, message = manager.send_verify_code()
    if not success:
        print(f"\n❌ 发送验证码失败: {message}")
        return

    print(f"\n✅ {message}")

    # 步骤2: 输入验证码并登录
    verify_code = input("\n请输入6位验证码: ").strip()
    success, message = manager.do_login(verify_code)

    if success:
        print(f"\n✅ {message}")
        token, sid, meddy_id = manager.get_credentials()
        print(f"\nToken:   {token}")
        print(f"SID:     {sid}")
        print(f"MeddyId: {meddy_id}")
    else:
        print(f"\n❌ {message}")


def test_auto_relogin():
    """测试自动重新登录"""
    print("\n" + "=" * 80)
    print("测试 3: 自动重新登录")
    print("=" * 80)

    manager = LoginManager(phone=get_phone_input())

    # 自动重新登录（如果凭证失效会提示输入验证码）
    token, sid, meddy_id = manager.ensure_login(auto_relogin=True)

    if token:
        print("\n✅ 登录成功")
    else:
        print("\n❌ 登录失败")


def test_multi_account():
    """测试多账号管理"""
    print("\n" + "=" * 80)
    print("测试 4: 多账号管理")
    print("=" * 80)

    # 账号1
    print("\n--- 账号 1 ---")
    manager1 = LoginManager(phone=get_phone_input())
    token1, sid1, meddy_id1 = manager1.ensure_login(auto_relogin=False)
    print(f"账号1状态: {'有效' if token1 else '失效'}")

    # 账号2（示例，实际需要真实手机号）
    print("\n--- 账号 2 ---")
    manager2 = LoginManager(phone=get_phone_input())
    token2, sid2, meddy_id2 = manager2.ensure_login(auto_relogin=False)
    print(f"账号2状态: {'有效' if token2 else '失效'}")


def test_check_status_only():
    """测试仅检查登录状态"""
    print("\n" + "=" * 80)
    print("测试 5: 仅检查登录状态")
    print("=" * 80)

    manager = LoginManager(phone=get_phone_input())

    # 加载凭证
    token, sid, meddy_id = manager.load_credentials()

    if token and sid:
        # 检查状态
        is_valid, user_info = manager.check_login_status()

        if is_valid:
            print("\n✅ 登录状态有效")
            print(f"手机号: {user_info['phone']}")
            print(f"昵称: {user_info['name']}")
            print(f"积分: {user_info['points']}")
        else:
            print("\n❌ 登录状态已失效")
    else:
        print("\n❌ 未找到已保存的凭证")


def main():
    """主菜单"""
    print("\n" + "=" * 80)
    print("登录管理器测试")
    print("=" * 80)
    print("\n选择测试项:")
    print("1. 基本用法（检查登录状态）")
    print("2. 手动登录流程")
    print("3. 自动重新登录")
    print("4. 多账号管理")
    print("5. 仅检查登录状态")
    print("0. 退出")

    choice = input("\n请选择 (0-5): ").strip()

    if choice == "1":
        test_basic_usage()
    elif choice == "2":
        test_manual_login()
    elif choice == "3":
        test_auto_relogin()
    elif choice == "4":
        test_multi_account()
    elif choice == "5":
        test_check_status_only()
    elif choice == "0":
        print("\n退出")
        return
    else:
        print("\n无效选择")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n❌ 异常: {e}")
        import traceback
        traceback.print_exc()

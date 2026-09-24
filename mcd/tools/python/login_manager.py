#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
麦当劳登录状态管理
- 保存/加载登录凭证
- 验证登录状态
- 自动重新登录
"""

import json
import os
import requests
from datetime import datetime
from mcdonald_login import (
    generate_token,
    activate_token,
    send_verification_code,
    login,
    build_headers
)

# 凭证保存路径
CREDENTIALS_FILE = '/Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd/credentials.json'


def save_credentials(token, sid, meddy_id):
    """
    保存登录凭证到文件

    Args:
        token: 设备 Token
        sid: 会话 SID
        meddy_id: MeddyId
    """
    data = {
        'token': token,
        'sid': sid,
        'meddy_id': meddy_id,
        'saved_at': datetime.now().isoformat()
    }

    with open(CREDENTIALS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✓ 凭证已保存到 {CREDENTIALS_FILE}")


def load_credentials():
    """
    从文件加载登录凭证

    Returns:
        (token, sid, meddy_id) 或 (None, None, None)
    """
    if not os.path.exists(CREDENTIALS_FILE):
        return None, None, None

    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        token = data.get('token')
        sid = data.get('sid')
        meddy_id = data.get('meddy_id')
        saved_at = data.get('saved_at')

        print(f"✓ 找到已保存的凭证（保存时间: {saved_at}）")
        return token, sid, meddy_id
    except Exception as e:
        print(f"✗ 加载凭证失败: {e}")
        return None, None, None


def check_login_status(token, sid):
    """
    检查登录状态是否有效

    使用获取账号信息接口测试: GET /bff/member/user/portal/info?scene=1

    Args:
        token: 设备 Token
        sid: 会话 SID

    Returns:
        (is_valid, user_info)
    """
    try:
        path = '/bff/member/user/portal/info'
        params = {'scene': '1'}

        # 构建请求头
        headers = build_headers(token, sid=sid, body=None, method='GET', path=path, query_params=params)
        headers['Host'] = 'api2.mcd.cn'

        # 发送请求
        url = 'https://api2.mcd.cn' + path
        response = requests.get(url, headers=headers, params=params, timeout=10)
        print(url)
        print(params)
        print(headers)
        print(response.status_code)
        print(response.text)
        result = response.json()

        if result.get('success'):
            data = result.get('data', {})
            user_info = {
                'phone': data.get('phoneNumber', ''),
                'name': data.get('fullName', ''),
                'points': data.get('availablePoints', '0'),
                'meddy_id': data.get('meddyId', '')
            }
            return True, user_info
        else:
            print(f"  API 返回错误: {result.get('message', '未知错误')}")
            return False, None

    except Exception as e:
        print(f"✗ 检查登录状态异常: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def ensure_login(phone=None):
    """
    确保已登录状态

    1. 尝试加载已保存的凭证
    2. 验证凭证是否有效
    3. 如果无效，执行登录流程

    Args:
        phone: 手机号（如果需要重新登录）

    Returns:
        (token, sid, meddy_id) 或 None
    """
    print("=" * 80)
    print("检查登录状态")
    print("=" * 80)

    # 1. 尝试加载已保存的凭证
    token, sid, meddy_id = load_credentials()

    if token and sid:
        # 2. 验证凭证是否有效
        print("\n验证登录状态...")
        is_valid, user_info = check_login_status(token, sid)

        if is_valid:
            print("\n✅ 登录状态有效")
            print("-" * 80)
            print(f"手机号: {user_info['phone']}")
            print(f"昵称: {user_info['name']}")
            print(f"积分: {user_info['points']}")
            print(f"MeddyId: {user_info['meddy_id']}")
            print("-" * 80)
            return token, sid, meddy_id
        else:
            print("\n✗ 登录状态已失效，需要重新登录")
    else:
        print("\n✗ 未找到已保存的凭证")

    # 3. 执行登录流程
    if not phone:
        phone = input("\n请输入手机号 (11位): ").strip()
        if len(phone) != 11:
            print("❌ 手机号格式错误")
            return None, None, None

    print("\n" + "=" * 80)
    print("开始登录流程")
    print("=" * 80)

    # 生成 Token
    print("\n[1/4] 生成 Token...")
    token = generate_token()
    print(f"✓ Token: {token}")

    # 激活 Token
    print("\n[2/4] 激活 Token...")
    success, tid, message = activate_token(token)
    if not success:
        print(f"❌ 激活失败: {message}")
        return None, None, None
    print(f"✓ TID: {tid}")

    # 发送验证码
    print(f"\n[3/4] 发送验证码到 {phone}...")
    success, message = send_verification_code(phone, token)
    if not success:
        print(f"❌ 发送失败: {message}")
        return None, None, None
    print(f"✅ 验证码已发送")

    # 等待验证码并登录
    print("\n[4/4] 等待验证码...")
    verify_code = input("请输入6位验证码: ").strip()

    if len(verify_code) != 6:
        print("❌ 验证码格式错误")
        return None, None, None

    print(f"\n正在登录...")
    success, sid, meddy_id, message = login(phone, verify_code, token)

    if success:
        print(f"\n✅ 登录成功！")
        print(f"  Token:    {token}")
        print(f"  SID:      {sid}")
        print(f"  MeddyId:  {meddy_id}")

        # 保存凭证
        save_credentials(token, sid, meddy_id)

        return token, sid, meddy_id
    else:
        print(f"\n❌ 登录失败: {message}")
        return None, None, None


def main():
    """测试登录状态管理"""
    result = ensure_login(phone="16752934813")

    if result and result[0]:
        token, sid, meddy_id = result
        print("\n" + "=" * 80)
        print("登录凭证可用")
        print("=" * 80)
        print(f"Token:    {token}")
        print(f"SID:      {sid}")
        print(f"MeddyId:  {meddy_id}")
    else:
        print("\n❌ 登录失败")


if __name__ == '__main__':
    result = ensure_login(phone="16752934813")

    # try:
    #     main()
    # except KeyboardInterrupt:
    #     print("\n\n用户中断")
    # except Exception as e:
    #     print(f"\n❌ 异常: {e}")
    #     import traceback
    #     traceback.print_exc()

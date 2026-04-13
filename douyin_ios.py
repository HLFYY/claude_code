#!/usr/bin/env python3
"""
pip install facebook-wda tidevice

首次使用新设备安装 WDA（只需一次，需 Mac + 数据线）：
  tidevice xctest -B com.facebook.WebDriverAgentRunner.xctrunner

用法:
  私信: python douyin_ios.py --action dm --target <抖音号> --message <消息>
  评论: python douyin_ios.py --action comment --target <抖音号> --video <视频URL> --message <回复>
"""
import re
import sys
import json
import subprocess
import time
import wda

DOUYIN_BUNDLE = "com.ss.iphone.ugc.Aweme"
WDA_BUNDLE    = "com.facebook.WebDriverAgentRunner.xctrunner"
WDA_PORT      = 8100


def log_info(content):
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")}, {content}', flush=True)


def get_connected_device() -> str:
    """通过 tidevice list 获取第一个连接的 iOS 设备 UDID"""
    result = subprocess.run(
        ["tidevice", "list", "--json"], capture_output=True, text=True
    )
    devices = json.loads(result.stdout or "[]")
    if not devices:
        raise RuntimeError("未找到 iOS 设备，请检查数据线连接并在手机上点击「信任此电脑」")
    if len(devices) > 1:
        log_info(f"检测到多台设备，使用第一台: {devices[0].get('SerialNumber')}")
    return devices[0].get("SerialNumber") or devices[0].get("Identifier")


def connect_device(udid: str) -> wda.Client:
    """连接 WDA，未启动则自动拉起"""
    url = f"http://localhost:{WDA_PORT}"
    # 先检查是否已在运行
    try:
        c = wda.Client(url)
        c.status()
        log_info("WDA 已在运行")
        return c
    except Exception:
        pass

    log_info("启动 WDA（首次约需 20s）...")
    subprocess.Popen(
        ["tidevice", "-u", udid, "xctest", "-B", WDA_BUNDLE, "--port", str(WDA_PORT)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    for _ in range(40):
        time.sleep(1)
        try:
            c = wda.Client(url)
            c.status()
            log_info("WDA 启动成功")
            return c
        except Exception:
            pass
    raise RuntimeError(
        "WDA 启动超时，请手动运行：\n"
        f"  tidevice -u {udid} xctest -B {WDA_BUNDLE}"
    )


def el_exists(el, timeout=3) -> bool:
    """检查元素是否存在，兼容不同版本 wda 的返回值"""
    try:
        result = el.wait(timeout=timeout)
        return result is not False
    except Exception:
        return False


def dismiss_popups(c: wda.Client):
    for label in ["关闭", "允许", "暂不"]:
        el = c(label=label)
        if el.exists:
            el.tap()
            time.sleep(0.5)
            break


def wait_tap(c: wda.Client, timeout=10, **kwargs):
    el = c(**kwargs)
    assert el_exists(el, timeout), f"元素未找到: {kwargs}"
    el.tap()
    time.sleep(1)


def open_douyin_home(c: wda.Client):
    app = c.app_current()
    if app.get("bundleId") == DOUYIN_BUNDLE:
        log_info("抖音已在前台，回到首页 ...")
        for _ in range(5):
            if c(label="首页").exists:
                break
            # iOS 没有 back 键，左滑返回
            c.swipe(0.1, 0.5, 0.9, 0.5)
            time.sleep(0.8)
        time.sleep(1)
    else:
        log_info("启动抖音 ...")
        c.app_launch(DOUYIN_BUNDLE)
        time.sleep(4)
        dismiss_popups(c)


def action_dm(c: wda.Client, target: str, message: str):
    """搜索用户并发送私信"""
    open_douyin_home(c)

    # 打开搜索页
    log_info("打开搜索页 ...")
    search_entry = c(label="搜索")
    if not search_entry.exists:
        search_entry = c(name="search")
    assert el_exists(search_entry, 5), "未找到搜索入口"
    search_entry.tap()
    time.sleep(2)

    # 输入抖音号
    log_info(f"搜索 {target} ...")
    box = c(className="XCUIElementTypeTextField")
    if not box.exists:
        box = c(className="XCUIElementTypeSearchField")
    assert el_exists(box, 5), "未找到搜索输入框"
    box.tap()
    time.sleep(0.3)
    box.set_text(target)
    time.sleep(0.8)
    wait_tap(c, timeout=5, label="搜索")
    time.sleep(3)

    # 切换用户 tab
    log_info("切换到用户标签 ...")
    wait_tap(c, timeout=8, label="用户")
    time.sleep(3)

    # 点击目标用户
    log_info("点击用户 ...")
    user = c(label=target)
    if el_exists(user, 8):
        user.tap()
    else:
        log_info(f"未精确找到 {target}，点击第一条结果 ...")
        cells = c(className="XCUIElementTypeCell")
        assert el_exists(cells, 5), "用户列表为空"
        cells[0].tap()
    time.sleep(3)
    dismiss_popups(c)

    # 点「...」→「发私信」
    log_info("打开更多菜单 ...")
    more = c(label="更多")
    if not more.exists:
        more = c(name="ic_profile_more")
    assert el_exists(more, 5), "未找到「...」更多按钮"
    more.tap()
    time.sleep(1.5)
    wait_tap(c, timeout=5, label="发私信")
    time.sleep(2)

    # 输入并发送
    log_info(f"输入消息: {message}")
    inp = c(className="XCUIElementTypeTextField")
    if not inp.exists:
        inp = c(className="XCUIElementTypeTextView")
    assert el_exists(inp, 5), "未找到消息输入框"
    inp.tap()
    time.sleep(0.3)
    inp.set_text(message)
    time.sleep(0.5)
    wait_tap(c, timeout=5, label="发送")
    log_info("✓ 私信发送成功！")


def action_comment(c: wda.Client, video_url: str, target: str,
                   message: str = "", max_scrolls: int = 50) -> bool:
    """打开视频，查找 target 评论，有 message 则回复"""
    match = re.search(r'modal_id=(\d+)', video_url)
    assert match, f"无法从 URL 提取 modal_id: {video_url}"
    video_id = match.group(1)

    # Deep link 打开视频
    log_info(f"打开视频 {video_id} ...")
    c.open_url(f"snssdk1128://aweme/detail/{video_id}")
    time.sleep(4)
    dismiss_popups(c)

    # 打开评论区
    log_info("打开评论区 ...")
    comment_btn = c(label="评论")
    if not comment_btn.exists:
        comment_btn = c(labelContains="条评论")
    if not el_exists(comment_btn, 5):
        comment_btn = c(label="说点什么")
    assert el_exists(comment_btn, 3), "未找到评论入口"
    comment_btn.tap()
    time.sleep(2)

    # 构建用户名匹配条件
    cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9_]', '', target)
    use_exact = (cleaned == target)

    log_info(f"开始查找评论用户 {target}（最多滚动 {max_scrolls} 屏）...")
    for i in range(max_scrolls):
        if use_exact:
            el = c(label=target, className="XCUIElementTypeStaticText")
        else:
            # wda 不支持 labelContains，用 xpath contains
            el = c.xpath(f'//XCUIElementTypeStaticText[contains(@label, "{cleaned}")]')

        if el_exists(el, 1):
            log_info(f"✓ 在第 {i + 1} 屏找到用户 {target} 的评论！")
            if not message:
                return True

            # 找「回复」按钮（iOS 评论列表中通常显示在评论行右侧或下方）
            reply_btn = c(label="回复", className="XCUIElementTypeButton")
            if not el_exists(reply_btn, 2):
                # 评论块未完全露出，再上滑一点
                c.swipe(0.5, 0.7, 0.5, 0.45)
                time.sleep(1)
            assert el_exists(reply_btn, 3), "未找到「回复」按钮"
            log_info("点击「回复」按钮 ...")
            reply_btn.tap()
            time.sleep(1.5)

            inp = c(className="XCUIElementTypeTextField")
            if not inp.exists:
                inp = c(className="XCUIElementTypeTextView")
            assert el_exists(inp, 5), "未找到回复输入框"
            inp.tap()
            time.sleep(0.3)
            inp.set_text(message)
            time.sleep(0.5)
            wait_tap(c, timeout=5, label="发送")
            log_info("✓ 回复发送成功！")
            return True

        # 上滑加载更多评论
        c.swipe(0.5, 0.8, 0.5, 0.2)
        time.sleep(1.5)

    log_info(f"✗ 滚动 {max_scrolls} 屏后未找到用户 {target} 的评论")
    return False


def main(target: str, action: str = "dm", message: str = "你好", video_url: str = ""):
    udid = get_connected_device()
    log_info(f"连接设备 {udid} ...")
    c = connect_device(udid)

    if action == "dm":
        action_dm(c, target, message)
    elif action == "comment":
        assert video_url, "action=comment 时必须传入 video_url"
        found = action_comment(c, video_url, target, message)
        sys.exit(0 if found else 1)
    else:
        raise ValueError(f"不支持的 action: {action}，可选值: dm / comment")


if __name__ == "__main__":
    main(
        target='星撞地球',
        action='comment',
        message='哈哈',
        video_url='https://www.douyin.com/jingxuan?modal_id=7619091479268511030',
    )
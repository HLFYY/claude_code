#!/usr/bin/env python3
"""
pip install uiautomator2==3.5.0
抖音自动化脚本
测试抖音APP版本(260401最新版和一个老版)：38.2.0 | 29.8.0 | 华为38.3
测试手机型号: MI6 红米-Note4X Google手机-Pixel2 华为P30
手机要求：
    手机打开usb调试模式，通过usb链接电脑并信任该电脑
    抖音登录好账号
    手机无锁屏密码-息屏状态下会自动唤醒并解锁。如果有密码就无法解锁
如果最多翻50次，差不多100评论以内的能翻到底，要翻3-4分钟
"""
import re
import sys
import subprocess
import time
import uiautomator2 as u2

DOUYIN_PKG = "com.ss.android.ugc.aweme"


def get_connected_device():
    """通过 adb devices 获取第一个在线设备的序列号"""
    result = subprocess.run(
        ["adb", "devices"], capture_output=True, text=True
    )
    devices = []
    for line in result.stdout.splitlines()[1:]:
        line = line.strip()
        if line.endswith("device") and not line.startswith("*"):
            serial = line.split()[0]
            devices.append(serial)
    if not devices:
        raise RuntimeError("未找到已连接的 ADB 设备，请检查设备连接和 USB 调试是否开启")
    if len(devices) > 1:
        log_info(f"检测到多台设备: {devices}，自动使用第一台: {devices[0]}")
    return devices[0]


def ensure_initialized(serial: str):
    """检查 u2.jar 是否已推送到设备，未推送则执行初始化。"""
    result = subprocess.run(
        ["adb", "-s", serial, "shell", "ls", "/data/local/tmp/u2.jar"],
        capture_output=True, text=True
    )
    if "u2.jar" in result.stdout:
        log_info("u2.jar 已就绪，跳过初始化")
        return
    log_info("检测到设备未初始化，正在推送 u2.jar（首次约需 30s）...")
    subprocess.run(
        [sys.executable, "-m", "uiautomator2", "init", "--serial", serial],
        check=True
    )
    log_info("初始化完成")


def connect_device(serial: str) -> u2.Device:
    """连接设备并验证通信正常"""
    d = u2.connect(serial)
    try:
        _ = d.info
    except Exception as e:
        raise RuntimeError(
            f"设备 {serial} 连接失败：{e}\n"
            "请确认：\n"
            "  1. 手机已开启 USB 调试\n"
            "  2. 已在手机弹窗点击「允许 USB 调试」"
        )
    log_info(f"设备 {serial} 已就绪")
    return d


def ensure_screen_on(serial: str):
    """息屏时唤醒屏幕并解除无密码锁屏。
    必须在 connect_device / u2.connect 之前调用，
    否则 UIAutomator JAR 启动时会把屏幕唤醒，导致检测失效。
    """
    out = subprocess.run(
        ["adb", "-s", serial, "shell", "dumpsys", "power"],
        capture_output=True, text=True, timeout=10
    ).stdout
    if "mWakefulness=Awake" in out or "Display Power: state=ON" in out:
        return
    log_info("屏幕已息屏，正在唤醒 ...")
    subprocess.run(["adb", "-s", serial, "shell", "input", "keyevent", "26"],
                   capture_output=True)
    time.sleep(1)
    # 优先：dismiss-keyguard 直接解除锁屏，不依赖分辨率（Android 6+）
    r = subprocess.run(["adb", "-s", serial, "shell", "wm", "dismiss-keyguard"],
                       capture_output=True, text=True)
    if r.returncode != 0 or "error" in r.stderr.lower():
        # 兜底：动态获取屏幕尺寸，按比例从 80% 上滑到 20%
        size_str = subprocess.run(
            ["adb", "-s", serial, "shell", "wm", "size"],
            capture_output=True, text=True
        ).stdout
        m = re.search(r'(\d+)x(\d+)', size_str)
        if m:
            w, h = int(m.group(1)), int(m.group(2))
            subprocess.run(
                ["adb", "-s", serial, "shell", "input", "swipe",
                 str(w // 2), str(int(h * 0.8)), str(w // 2), str(int(h * 0.2))],
                capture_output=True
            )
    time.sleep(0.8)
    log_info("屏幕已唤醒")


def dismiss_popups(d):
    """关闭可能出现的弹窗/广告，一次 XPath 查询匹配所有候选文字"""
    words = ["关闭", "允许", "暂不", "以后再说", "不再提醒", "仅使用期间允许", "跳过", "我知道了", "同意", "取消"]
    conditions = " or ".join(f'@text="{w}"' for w in words)
    btn = d.xpath(f'//*[{conditions}]')
    if btn.wait(timeout=2):
        btn.click()
        time.sleep(0.5)


def wait_click(d, timeout=10, **kwargs):
    el = d(**kwargs)
    assert el.wait(timeout=timeout), f"元素未找到: {kwargs}"
    el.click()
    time.sleep(1)


def log_info(content):
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")}, {content}', flush=True)


def open_douyin_home(d):
    """启动抖音并回到首页"""
    current_pkg = d.app_current().get("package", "")
    page_xpath = '//*[@text="首页" or @text="推荐"]'
    w, h = d.window_size()
    if current_pkg == DOUYIN_PKG:
        log_info("抖音已在前台，回到首页 ...")
        for _ in range(7):
            if d.xpath(page_xpath).wait(timeout=2):
                # d.click(int(w * 0.05), int(h * 0.95))
                d.xpath(page_xpath).click()
                break
            d.press("back")
        time.sleep(1)
    elif DOUYIN_PKG in (d.app_list_running() or []):
        log_info("抖音在后台，拉起并回到首页 ...")
        d.app_start(DOUYIN_PKG)
        time.sleep(2)
        dismiss_popups(d)
        for _ in range(7):
            if d.xpath(page_xpath).wait(timeout=2):
                d.xpath(page_xpath).click()
                # d.click(int(w * 0.05), int(h * 0.95))
                break
            d.press("back")
        time.sleep(1)
    else:
        log_info("启动抖音 ...")
        d.app_start(DOUYIN_PKG)
        time.sleep(4)
        dismiss_popups(d)


def action_dm(d, target: str, message: str):
    """搜索用户并发送私信"""
    # ── 2. 打开搜索页 ────────────────────────────────────────────
    log_info("打开搜索页 ...")
    if d(description="搜索").exists(timeout=5):
        d(description="搜索").click()
    else:
        d.xpath('//android.widget.ImageView[@clickable="true"]').last.click()
    time.sleep(2)

    # ── 3. 输入抖音号并搜索 ──────────────────────────────────────
    log_info(f"搜索 {target} ...")
    SEARCH_BOX_RES = "com.ss.android.ugc.aweme:id/et_search_kw"
    search_box = d(resourceId=SEARCH_BOX_RES)
    if not search_box.exists(timeout=3):
        search_box_x = d.xpath('//android.widget.EditText[contains(@resource-id,"search")]')
        assert search_box_x.wait(timeout=3), "未找到搜索输入框"
        search_box_x.click()
        time.sleep(0.5)
        search_box = d(resourceId=SEARCH_BOX_RES)

    search_box.click()
    time.sleep(0.3)
    search_box.clear_text()
    search_box.set_text(target)
    time.sleep(0.8)
    wait_click(d, timeout=5, text="搜索")
    time.sleep(3)

    # ── 4. 切换到「用户」标签 ────────────────────────────────────
    log_info("切换到用户标签 ...")
    wait_click(d, timeout=8, text="用户")
    time.sleep(4)

    # ── 5. 点击目标用户 ──────────────────────────────────────────
    log_info("点击用户 ...")
    user = d.xpath(f'//*[contains(@content-desc, "{target}")]')
    if user.wait(timeout=8):
        user.click()
    else:
        log_info(f"未找到抖音号 {target}，点击搜索结果第一个用户 ...")
        first_user = d.xpath(
            '//androidx.recyclerview.widget.RecyclerView'
            '/android.widget.FrameLayout[@index="0"]'
        )
        assert first_user.wait(timeout=2), "用户列表为空，未找到任何用户结果"
        first_user.click()
        time.sleep(3)
        uu1 = d.xpath(f'//*[contains(@text, "{target}")]')
        assert uu1.wait(timeout=5), "非目标用户"
    time.sleep(3)
    dismiss_popups(d)

    # ── 6. 点「...」→「发私信」 ──────────────────────────────────
    log_info("打开更多菜单 ...")
    more_btn = d(description="更多")
    if not more_btn.exists(timeout=3):
        more_btn = d.xpath(
            '//android.widget.ImageView[@clickable="true"][last()]'
            '| //android.widget.ImageButton[@clickable="true"][last()]'
        )
    assert more_btn.exists if hasattr(more_btn, 'exists') else more_btn.wait(timeout=5), \
        "未找到「...」更多按钮"
    more_btn.click() if hasattr(more_btn, 'click') else more_btn.click()
    time.sleep(1.5)

    log_info("点击发私信 ...")
    wait_click(d, timeout=5, text="发私信")
    time.sleep(2)

    # ── 7. 输入消息并发送 ────────────────────────────────────────
    log_info(f"输入消息: {message}")
    input_bar = d(text="发送消息")
    if not input_bar.exists(timeout=3):
        input_bar = d(className="android.widget.EditText")
    assert input_bar.exists(timeout=5), "未找到消息输入框"
    input_bar.click()
    time.sleep(0.5)
    d(className="android.widget.EditText").set_text(message)
    time.sleep(0.5)

    log_info("发送 ...")
    wait_click(d, timeout=5, description="发送")
    log_info("✓ 私信发送成功！")

def click_bottom_tab(d, desc):
    log_info(f"点击「{desc}」标签 ...")
    me_btn = d(text=desc)
    if me_btn.exists(timeout=3):
        me_btn.click()
    else:
        w, h = d.window_size()
        w_rate = {
            '我': 0.94,
            '消息': 0.7,
            '朋友': 0.3,
            '首页': 0.05,
        }
        d.click(int(w * w_rate[desc]), int(h * 0.97))
    time.sleep(2)

def action_comment(d, video_url: str, target: str, message: str = "", max_scrolls: int = 50) -> bool:
    """打开指定视频，滚动评论列表查找 target 用户的评论，找到后回复 message。
    message 为空时只检查是否存在，不回复。
    返回 True 表示找到，False 表示未找到。
    """
    # ── 1. 提取 video_id ─────────────────────────────────────────
    match = re.search(r'modal_id=(\d+)', video_url)
    assert match, f"无法从 URL 提取 modal_id，请检查 URL 格式: {video_url}"
    video_id = match.group(1)

    # 先进入消息tab的页面，再打开目标视频,然后返回,则目标视频一定是历史记录的第一个视频，
    # 如果在首页tab里打开目标视频，然后返回，则目标视频不一定是历史记录的第一个，因为返回的页面可能是视频，也可能不是
    click_bottom_tab(d, '消息')

    # ── 2. Deep Link 打开视频 ────────────────────────────────────
    log_info(f"打开视频 {video_id} ...")
    d.shell(f'am start -a android.intent.action.VIEW -d "snssdk1128://aweme/detail/{video_id}"')
    time.sleep(5)
    dismiss_popups(d)

    d.press("back")
    time.sleep(5)
    his_num=1

    # ── 新增：通过「我」→ 三横杠 → 观看历史 进入刚才的视频 ──────────
    # 1. 点击底部「我」标签
    click_bottom_tab(d, '我')

    # 2&3. 点击「观看历史」：部分机型在「我」页直接有入口，否则先点三横杠再找
    log_info("查找「观看历史」入口 ...")
    his_xpath = '//*[@text="观看历史"]'
    his_btn = d.xpath(his_xpath)
    if not his_btn.wait(timeout=3):
        log_info("直接入口未找到，点击三横杠菜单 ...")
        # more_btn = d(description="更多")
        # if more_btn.exists(timeout=3):
        #     more_btn.click()
        # else:
        w, h = d.window_size()
        d.click(int(w * 0.95), int(h * 0.07))

        time.sleep(1.5)
        his_btn = d.xpath(his_xpath)
        assert his_btn.wait(timeout=5), "未找到「观看历史」入口"
    log_info("点击「观看历史」...")
    his_btn.click()
    time.sleep(2)

    # 4. 点击观看历史第一个视频
    # 部分机型视频格子有 content-desc 可直接 xpath；
    # 另一些机型整个列表是单一 ViewPager 元素，需通过坐标点击宫格第一格（3列，点左列中心）
    log_info(f"点击观看历史第{his_num}个视频 ...")
    first_video = d.xpath(
        '(//android.view.ViewGroup[@content-desc and string-length(@content-desc)>0'
        ' and @focusable="true"])[%s]'%his_num
    )
    if first_video.wait(timeout=3):
        log_info(f"第{his_num}个视频 content-desc: {first_video.info.get('contentDescription', '')}")
        first_video.click()
    else:
        log_info(f"列表为单一容器，通过坐标点击第{his_num}格 ...")
        w, h = d.window_size()
        tab2 = d.xpath('(//*[@class="androidx.appcompat.app.ActionBar$Tab"])[%s]'%his_num)
        tab_bottom = int(h * 0.17)  # 默认估算
        if tab2.wait(timeout=3):
            b = tab2.info.get('bounds', {})
            tab_bottom = b.get('bottom', tab_bottom)
        if his_num == 1:
            d.click(w // 6, tab_bottom + h // 5)  # x=第1列中心，y=tab下沿+1/6屏幕高
        else:
            d.click(w // 2, tab_bottom + h // 5)  # x=第2列中心，y=tab下沿+1/6屏幕高
    time.sleep(3)
    dismiss_popups(d)

    # ── 3. 点击评论按钮 ──────────────────────────────────────────
    log_info("打开评论区 ...")
    comment_btn = d.xpath('//*[contains(@content-desc,"评论") or contains(@text,"说点什么")]')
    assert comment_btn.wait(timeout=5), "未找到评论入口（评论按钮 / 说点什么输入栏）"
    comment_btn.click()
    time.sleep(2)

    # ── 4. 滚动评论列表查找 target ───────────────────────────────
    # 构建用户名 XPath：含 emoji/特殊符号时剔除后用 contains，否则精确匹配
    cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9_]', '', target)
    name_cond = f'@text="{target}"' if cleaned == target else f'contains(@text, "{cleaned}")'

    log_info(f"开始查找用户名 {target}:{name_cond}（最多滚动 {max_scrolls} 屏）...")
    for i in range(max_scrolls):
        username_el = d.xpath(f'//android.widget.TextView[{name_cond}]')
        if username_el.wait(timeout=1):
            log_info(f"✓ 在第 {i + 1} 屏找到用户 {target} 的评论！")
            if not message:
                return True

            # ── 5. 在同一评论块内点「回复」按钮 ─────────────────────
            # 用户名节点 → 上两级到评论容器 → 内部找 text="回复" 按钮
            # 结构: View(nv1) > ViewGroup(l-x) > TextView(title/username)
            reply_btn = d.xpath(
                f'//android.widget.TextView[{name_cond}]'
                f'/../..//android.widget.TextView[@text="回复"]'
            )
            if not reply_btn.wait(timeout=2):
                # 用户名刚滚入屏幕底部，回复按钮还在屏幕外，再上滑一点
                d.swipe_ext("up", scale=0.5)
                time.sleep(1)
            assert reply_btn.wait(timeout=3), "未找到该评论的「回复」按钮"
            log_info("点击「回复」按钮 ...")
            reply_btn.click()
            time.sleep(1.5)

            # ── 6. 在弹出的回复输入框输入内容并发送 ─────────────────
            # 回复输入框 hint 通常为「回复 @xxx」或「说点什么」
            reply_box = d(className="android.widget.EditText")
            if not reply_box.exists(timeout=3):
                # EditText 未出现时点击「回复」文字触发输入框展开
                d.xpath('//*[contains(@text,"回复")]').click()
                time.sleep(0.5)
            assert reply_box.exists(timeout=5), "未找到回复输入框"
            reply_box.click()
            time.sleep(0.3)
            reply_box.set_text(message)
            time.sleep(0.5)

            log_info(f"发送回复: {message}")
            # 评论回复的发送按钮是 Button text="发送"，不是 ImageView description="发送"
            wait_click(d, timeout=5, text="发送")
            log_info("✓ 回复发送成功！")
            return True

        # 上滑加载更多评论
        d.swipe_ext("up", scale=0.7)
        time.sleep(1.5)

    log_info(f"✗ 滚动 {max_scrolls} 屏后未找到用户 {target} 的评论")
    return False


def dyauto_main(target: str, action: str = "dm", message: str = "你好", video_url: str = ""):
    '''

    :param target: 目标用户-私信时传抖音号，评论时传抖音昵称
    :param action: dm-私信 comment-回复视频下的评论
    :param message: 需要发送的内容
    :param video_url: 评论是目标视频url
    :return:
    '''
    device = get_connected_device()
    ensure_screen_on(device)        # ← 必须在 connect_device 前，u2 连接会唤醒屏幕
    ensure_initialized(device)
    log_info(f"连接设备 {device} ...")
    d = connect_device(device)
    d.implicitly_wait(10)
    open_douyin_home(d)
    if action == "dm":
        action_dm(d, target, message)
    elif action == "comment":
        assert video_url, "action=comment 时必须传入 video_url"
        found = action_comment(d, video_url, target, message)
    else:
        raise ValueError(f"不支持的 action: {action}，可选值: dm / comment")


if __name__ == "__main__":
    # 私信
    # dyauto_main(
    #     target='2066349444',
    #     action='dm',
    #     message='你好',
    # )
    # 回复视频下的评论
    dyauto_main(
        # target='星撞地球',
        # target='hlfyy131402',
        # video_url='https://www.douyin.com/jingxuan?modal_id=7619091479268511030',
        target='江西蛋托厂',
        video_url='https://www.douyin.com/jingxuan?modal_id=7598116691310021942',
        action='comment',
        message='+1',
    )

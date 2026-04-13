#!/usr/bin/env python3
"""
用 DrissionPage 接管已有 Chrome 浏览器的各个 Profile。

Chrome 架构限制：
  同一 user-data-dir 只能运行一个 Chrome 进程，
  多个 Profile 窗口共享该进程（同一调试端口）。

接管方案：
  方案A - 单进程多Profile（推荐）：
    关闭当前 Chrome → DrissionPage 重启并注入调试端口 →
    通过 CDP 打开 Profile 1 的独立窗口 → 用 tab 区分两个 Profile

  方案B - 独立进程（完全隔离）：
    Default  → 原始 user-data-dir + port 9223
    Profile1 → 独立 user-data-dir + port 9224（复制 Profile 目录）
"""

import os
import shutil
import time
import psutil
from DrissionPage import ChromiumPage, ChromiumOptions

# ── 常量配置 ──────────────────────────────────────────────
CHROME_BIN  = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
USER_DATA   = "/Users/houjie/Library/Application Support/Google/Chrome"
PORT_EXIST  = 9222    # 已运行的带调试端口的 Chrome（PID 3641）
PORT_DEFAULT = 9223   # 接管 Default profile 用的端口
PORT_P1      = 9224   # 接管 Profile 1 用的端口（方案B）


# ── 工具函数 ──────────────────────────────────────────────

def kill_chrome_by_userdata(user_data_dir: str):
    """关闭使用指定 user-data-dir 的 Chrome 主进程"""
    killed = []
    for proc in psutil.process_iter(["pid", "exe", "cmdline"]):
        try:
            exe = proc.info.get("exe") or ""
            if "Google Chrome" not in exe and "Chromium" not in exe:
                continue
            cmdline = proc.info.get("cmdline") or []
            # 跳过子进程
            if any(f"--type=" in a for a in cmdline):
                continue
            # 提取 user-data-dir（无参数则是默认目录）
            udd = next(
                (a.split("=", 1)[1] for a in cmdline if a.startswith("--user-data-dir=")),
                USER_DATA,
            )
            if os.path.normpath(udd) == os.path.normpath(user_data_dir):
                proc.terminate()
                killed.append(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if killed:
        time.sleep(1.5)
        print(f"  已关闭 Chrome 进程: {killed}")
    return killed


def make_options(user_data_dir: str, profile_dir: str, port: int) -> ChromiumOptions:
    """构造 ChromiumOptions"""
    co = ChromiumOptions()
    co.set_browser_path(CHROME_BIN)
    co.set_user_data_path(user_data_dir)
    co.set_argument(f"--profile-directory={profile_dir}")
    co.set_local_port(port)
    co.set_argument("--no-first-run")
    co.set_argument("--no-default-browser-check")
    return co


# ── 方案0：连接已有调试端口（最简单，适用于已开启 --remote-debugging-port 的 Chrome）──

def connect_existing(port: int = PORT_EXIST) -> ChromiumPage:
    """
    直接连接到已运行的 Chrome（需已开启 --remote-debugging-port）。
    适用于：PID 3641（port 9222，user-data-dir=tmp/chrome-mcp）
    """
    print(f"\n[方案0] 连接已有 Chrome（port={port}）...")
    co = ChromiumOptions().set_address(f"127.0.0.1:{port}")
    page = ChromiumPage(co)
    print(f"  已连接，当前页面: {page.title} | {page.url}")
    return page


# ── 方案A：单进程接管，Default + Profile 1 共用一个调试端口 ──────────────────────

def takeover_single_process() -> dict:
    """
    关闭当前 Chrome（PID 2456），以 Default profile 重启并开启调试端口。
    然后通过 CDP 打开 Profile 1 的新窗口，两个 Profile 在同一进程里。

    返回: {"default": tab_default, "profile1": tab_profile1}
    """
    print("\n[方案A] 单进程接管 Default + Profile 1 ...")

    # 1. 关闭现有 Chrome
    kill_chrome_by_userdata(USER_DATA)

    # 2. 以 Default profile 启动（DrissionPage 会自动注入调试端口）
    co = make_options(USER_DATA, "Default", PORT_DEFAULT)
    print(f"  启动 Default profile（port={PORT_DEFAULT}）...")
    page = ChromiumPage(co)
    tab_default = page.get_tab()   # 当前标签即 Default profile 的窗口
    print(f"  Default 已就绪: {tab_default.title}")

    # 3. 打开 Profile 1 的新窗口
    #    Chrome 命令行参数方式：向已运行的 Chrome 进程发送 --profile-directory 并打开新窗口
    #    DrissionPage 提供 new_tab() 但无法直接切换 profile，
    #    需要通过 CDP 的 Target.createBrowserContext 或直接调用系统命令让 Chrome 打开 Profile 1 窗口
    import subprocess
    subprocess.Popen([
        CHROME_BIN,
        f"--user-data-dir={USER_DATA}",
        "--profile-directory=Profile 1",
        "--new-window",
        "about:blank",
    ])
    time.sleep(2)

    # 4. 枚举所有 tab，找到 Profile 1 新打开的窗口
    tabs = page.get_tabs()
    tab_p1 = None
    for tab in tabs:
        # Profile 1 的新窗口一般是 about:blank 或 new tab
        if tab.tab_id != tab_default.tab_id:
            tab_p1 = tab
            break

    if tab_p1:
        print(f"  Profile 1 已就绪: tab_id={tab_p1.tab_id}")
    else:
        print("  警告：未找到 Profile 1 的 tab，所有 tab：", [t.tab_id for t in tabs])

    return {"default": tab_default, "profile1": tab_p1, "page": page}


# ── 方案B：独立进程，每个 Profile 完全隔离 ──────────────────────────────────────

def prepare_profile1_userdata(src_profile_dir: str = "Profile 1") -> str:
    """
    将 Profile 1 的数据复制到独立目录，用于启动独立 Chrome 进程。
    返回新的 user-data-dir 路径。
    """
    src = os.path.join(USER_DATA, src_profile_dir)
    dst_root = os.path.join(os.path.dirname(USER_DATA), "chrome-profile1-standalone")
    dst = os.path.join(dst_root, "Default")   # 新目录里作为 Default profile

    if not os.path.exists(src):
        raise FileNotFoundError(f"源 Profile 目录不存在: {src}")

    print(f"  复制 Profile 1 数据: {src} → {dst}")
    if os.path.exists(dst_root):
        shutil.rmtree(dst_root)
    os.makedirs(dst_root, exist_ok=True)
    shutil.copytree(src, dst)
    return dst_root


def takeover_independent() -> dict:
    """
    方案B：完全独立的两个 Chrome 进程。
      - Default  → USER_DATA          + port 9223
      - Profile 1 → standalone目录    + port 9224

    返回: {"default": page_default, "profile1": page_p1}
    """
    print("\n[方案B] 独立进程接管 Default + Profile 1 ...")

    # ── Default profile ──
    print(f"  [Default] 关闭现有并以 port={PORT_DEFAULT} 重启...")
    kill_chrome_by_userdata(USER_DATA)
    co_d = make_options(USER_DATA, "Default", PORT_DEFAULT)
    page_default = ChromiumPage(co_d)
    print(f"  [Default] 已就绪: {page_default.title} | port={PORT_DEFAULT}")

    # ── Profile 1（独立 user-data-dir）──
    try:
        standalone_dir = prepare_profile1_userdata("Profile 1")
    except FileNotFoundError as e:
        print(f"  [Profile 1] {e}，跳过")
        return {"default": page_default, "profile1": None}

    print(f"  [Profile 1] 以 port={PORT_P1} 独立启动...")
    co_p1 = make_options(standalone_dir, "Default", PORT_P1)
    page_p1 = ChromiumPage(co_p1)
    print(f"  [Profile 1] 已就绪: {page_p1.title} | port={PORT_P1}")

    return {"default": page_default, "profile1": page_p1}


# ── 示例：接管后做一些操作 ────────────────────────────────────────────────────

def demo_operations(pages: dict):
    """演示：分别对两个 Profile 进行操作"""
    print("\n[演示] 对各 Profile 进行操作...")

    tab_d = pages.get("default")
    tab_p1 = pages.get("profile1")

    if tab_d:
        tab_d.get("https://www.baidu.com")
        print(f"  Default  → 当前页: {tab_d.title}")

    if tab_p1:
        tab_p1.get("https://www.bing.com")
        print(f"  Profile 1 → 当前页: {tab_p1.title}")


# ── 主入口 ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "help"

    if mode == "0":
        # 连接已有调试端口（port 9222，无需关闭 Chrome）
        page = connect_existing(PORT_EXIST)
        demo_operations({"default": page})

    elif mode == "A":
        # 单进程接管（共享调试端口，两个 Profile 在同一 Chrome 进程）
        pages = takeover_single_process()
        demo_operations(pages)

    elif mode == "B":
        # 独立进程接管（每个 Profile 有独立进程和端口）
        pages = takeover_independent()
        demo_operations(pages)

    else:
        print("""
用法: python3 takeover_chrome.py <模式>

  模式说明:
    0   连接已开启调试端口的 Chrome（port 9222，不关闭现有浏览器）
    A   单进程接管：关闭现有 Chrome，Default + Profile 1 共用 port 9223
    B   独立进程：Default(port 9223) 和 Profile 1(port 9224) 完全隔离

  当前 Chrome 状态:
    PID 2456  → Default(hj1558109546@gmail.com) + Profile 1(未登录)  无调试端口
    PID 3641  → port 9222  user-data-dir=tmp/chrome-mcp

  建议:
    - 只需控制已有带端口的 Chrome     → 模式 0
    - 同时控制 Default + Profile 1    → 模式 A（推荐，不破坏数据）
    - 需要两个完全独立的 CDP 连接      → 模式 B（会复制 Profile 1 数据）
""")

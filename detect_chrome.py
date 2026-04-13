#!/usr/bin/env python3
"""
检测当前运行的所有 Chrome 浏览器实例，获取：
  - 用户数据目录（user-data-dir）
  - 系统用户
  - 主进程 PID
  - 监听端口
  - 所有已激活的 Profile（含 Guest）
"""

import json
import os
import re
import sys

try:
    import psutil
except ImportError:
    print("缺少依赖，请先运行: pip install psutil")
    sys.exit(1)

# 只匹配真正的 Chrome 可执行文件路径，避免 crashpad_handler 等误判
CHROME_BINARY_PATTERNS = [
    r".*/Google Chrome\.app/Contents/MacOS/Google Chrome$",
    r".*/Chromium\.app/Contents/MacOS/Chromium$",
    r".*/chromium-browser$",
    r".*/google-chrome(-stable|-beta|-unstable)?$",
    # Windows
    r".*\\chrome\.exe$",
]

# 子进程类型标志，有这些 flag 的跳过
SKIP_TYPE_FLAGS = {
    "--type=renderer", "--type=gpu-process", "--type=utility",
    "--type=zygote", "--type=crashpad-handler", "--type=broker",
    "--type=ppapi", "--type=ppapi-broker", "--type=nacl-loader",
    "--type=sandbox-ipc", "--type=network",
}

# 各平台 Chrome 默认用户数据目录
DEFAULT_USER_DATA_DIR = {
    "darwin": os.path.expanduser("~/Library/Application Support/Google/Chrome"),
    "linux":  os.path.expanduser("~/.config/google-chrome"),
    "win32":  os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\User Data"),
}.get(sys.platform, "")


def is_chrome_main(exe: str, cmdline: list) -> bool:
    """判断是否为 Chrome 主进程（真实可执行文件，非子进程）"""
    if not exe:
        return False
    exe_norm = exe.replace("\\", "/")
    if not any(re.match(p, exe_norm) for p in CHROME_BINARY_PATTERNS):
        return False
    if any(flag in cmdline for flag in SKIP_TYPE_FLAGS):
        return False
    return True


def get_listening_ports(pid: int) -> list:
    """获取指定 PID 的所有 TCP 监听端口"""
    ports = []
    try:
        proc = psutil.Process(pid)
        for conn in proc.net_connections(kind="tcp"):
            if conn.status == psutil.CONN_LISTEN:
                ports.append(conn.laddr.port)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return ports


def get_tree_ports(root_pid: int) -> list:
    """递归收集主进程及其子进程的所有 TCP 监听端口"""
    ports = set(get_listening_ports(root_pid))
    try:
        for child in psutil.Process(root_pid).children(recursive=True):
            for p in get_listening_ports(child.pid):
                ports.add(p)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return sorted(ports)


def read_chrome_profiles(user_data_dir: str) -> list:
    """
    读取 Chrome Local State 文件，返回所有已创建的 Profile 信息列表。
    每项包含: profile_dir, display_name, is_guest, gaia_name(Google账号名)
    """
    local_state_path = os.path.join(user_data_dir, "Local State")
    if not os.path.isfile(local_state_path):
        return []
    try:
        with open(local_state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return []

    profiles = []
    info_cache = state.get("profile", {}).get("info_cache", {})
    for dir_name, meta in info_cache.items():
        profiles.append({
            "profile_dir": dir_name,
            "display_name": meta.get("name", dir_name),
            "gaia_name": meta.get("user_name", ""),          # Google 账号邮箱
            "is_guest": meta.get("is_using_default_name", False) and dir_name == "Guest Profile",
            "avatar": meta.get("last_downloaded_gaia_picture_url_with_size", ""),
        })

    # 检查是否有 Guest Profile 目录（即使不在 info_cache 里）
    guest_dir = os.path.join(user_data_dir, "Guest Profile")
    known_dirs = {p["profile_dir"] for p in profiles}
    if os.path.isdir(guest_dir) and "Guest Profile" not in known_dirs:
        profiles.append({
            "profile_dir": "Guest Profile",
            "display_name": "访客",
            "gaia_name": "",
            "is_guest": True,
            "avatar": "",
        })

    return profiles


def collect_chrome_instances() -> list:
    """扫描所有进程，按 (username, user_data_dir) 分组，返回每个 Chrome 实例信息"""
    groups = {}

    for proc in psutil.process_iter(["pid", "name", "exe", "cmdline", "username"]):
        try:
            exe = proc.info.get("exe") or ""
            cmdline = proc.info.get("cmdline") or []
            if not is_chrome_main(exe, cmdline):
                continue

            username = proc.info.get("username") or "unknown"
            debug_port = None
            user_data_dir = DEFAULT_USER_DATA_DIR

            for arg in cmdline:
                m = re.match(r"--user-data-dir=(.+)", arg)
                if m:
                    user_data_dir = m.group(1).strip('"').strip("'")
                m = re.match(r"--remote-debugging-port=(\d+)", arg)
                if m:
                    debug_port = int(m.group(1))

            key = (username, user_data_dir)
            if key not in groups:
                groups[key] = {
                    "username": username,
                    "user_data_dir": user_data_dir,
                    "debug_port": debug_port,
                    "main_pid": proc.info["pid"],
                    "all_pids": [proc.info["pid"]],
                }
            else:
                groups[key]["all_pids"].append(proc.info["pid"])
                if debug_port and not groups[key]["debug_port"]:
                    groups[key]["debug_port"] = debug_port

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    results = []
    for inst in groups.values():
        # 收集监听端口
        listening_ports = set()
        for pid in inst["all_pids"]:
            for port in get_tree_ports(pid):
                listening_ports.add(port)
        if inst["debug_port"]:
            listening_ports.add(inst["debug_port"])
        inst["listening_ports"] = sorted(listening_ports)

        # 读取 Profile 信息
        inst["profiles"] = read_chrome_profiles(inst["user_data_dir"])
        results.append(inst)

    return results


def main():
    instances = collect_chrome_instances()

    if not instances:
        print("未检测到正在运行的 Chrome 浏览器实例。")
        return

    print(f"共检测到 {len(instances)} 个 Chrome 实例\n")
    sep = "=" * 65

    for i, inst in enumerate(instances, 1):
        ports_str = (
            ", ".join(str(p) for p in inst["listening_ports"])
            if inst["listening_ports"] else "无（普通启动，未监听 TCP 端口）"
        )
        debug_str = str(inst["debug_port"]) if inst["debug_port"] else "未开启"

        print(sep)
        print(f"[实例 {i}]")
        print(f"  系统用户     : {inst['username']}")
        print(f"  用户数据目录 : {inst['user_data_dir']}")
        print(f"  主进程 PID   : {inst['main_pid']}")
        print(f"  所有 PID     : {inst['all_pids']}")
        print(f"  监听端口     : {ports_str}")
        print(f"  调试端口     : {debug_str}")

        profiles = inst["profiles"]
        if profiles:
            print(f"  已有 Profile : {len(profiles)} 个")
            for p in profiles:
                tag = "[访客]" if p["is_guest"] else ""
                account = f"  ({p['gaia_name']})" if p["gaia_name"] else "  (未登录)"
                print(f"    - {p['profile_dir']:20s} {p['display_name']:15s}{account} {tag}")
        else:
            print("  已有 Profile : 无法读取（可能权限不足）")

    print(sep)
    print()
    print("注意: Chrome 多个窗口/Profile 共享同一主进程，若需通过 CDP 控制，")
    print("      须以 --remote-debugging-port=<端口> 重启 Chrome。")


if __name__ == "__main__":
    main()

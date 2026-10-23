#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
remote_run.py — Mac/Linux 侧远程执行 Windows 部署脚本（走 SSH）

设计原则
--------
1. 密码绝不落盘：只从环境变量 MCD_PASS 或交互式 getpass 读取，
   不写进命令行参数（避免进 shell history）、不写日志、不写配置文件。
2. 幂等可重跑：远端目录存在则复用，脚本自身也支持 -SkipPause 重入。
3. 全程回显：远端 stdout/stderr 实时打印，失败时给出定位提示。

用法
----
    # 1) 先探端口，确认网络可达
    python3 remote_run.py --probe --host 1.2.3.4

    # 2) 只上传不执行
    MCD_PASS='***' python3 remote_run.py --host 1.2.3.4 --port 22 \
        --user Administrator --upload-only

    # 3) 上传并执行完整部署（无人值守）
    MCD_PASS='***' python3 remote_run.py --host 1.2.3.4 --port 22 \
        --user Administrator

    # 4) 只跑一条命令
    MCD_PASS='***' python3 remote_run.py --host 1.2.3.4 --port 22 \
        --user Administrator --exec "powershell -NoProfile -Command \"$PSVersionTable\"""

依赖
----
    pip install paramiko
"""

import argparse
import getpass
import os
import posixpath
import socket
import sys
import time

try:
    import paramiko
except ImportError:
    sys.exit("缺少 paramiko，请先安装：pip install paramiko")

# Windows 侧落地目录（用正斜杠，Windows OpenSSH 的 sftp-server 接受盘符路径）
DEFAULT_REMOTE_DIR = "C:/mcd_deploy"

# 上传白名单：只传部署真正需要的文件
UPLOAD_FILES = [
    "setup_windows.ps1",
    "setup.bat",
    "README.md",
]

# 常见端口探测顺序
PROBE_PORTS = [22, 2222, 2022, 5985, 3389]


# --------------------------------------------------------------------------
# 端口探测
# --------------------------------------------------------------------------
def probe(host: str, ports=None, timeout: float = 3.0) -> dict:
    """TCP 连通性探测，返回 {port: bool}"""
    ports = ports or PROBE_PORTS
    result = {}
    for p in ports:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect((host, p))
            result[p] = True
        except Exception:
            result[p] = False
        finally:
            s.close()
    return result


def print_probe(host: str, result: dict) -> None:
    print(f"=== 端口探测 {host} ===")
    for p, ok in result.items():
        tag = {
            22: "SSH（部署首选）",
            2222: "SSH 备用端口",
            2022: "SSH 备用端口",
            5985: "WinRM",
            3389: "RDP（无法承载自动化）",
        }.get(p, "")
        print(f"  {p:<6} {'开放' if ok else '关闭':<4}  {tag}")

    ssh_open = any(result.get(p) for p in (22, 2222, 2022))
    if not ssh_open:
        print("\n[!] 没有可用的 SSH 端口，无法远程部署。")
        print("    请在目标机的 RDP 会话里执行 deploy/enable_ssh.ps1（或 README 里的一行命令），")
        print("    并确认云厂商安全组 / 防火墙放行了该端口。")
        return
    print("\n[OK] SSH 端口可用，可以继续。")


# --------------------------------------------------------------------------
# SSH 连接
# --------------------------------------------------------------------------
def connect(host: str, port: int, user: str, password: str,
            timeout: float = 15.0):
    client = paramiko.SSHClient()
    # 首次连接自动接受主机密钥；生产环境建议换成已知 host key 校验
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=user,
        password=password,
        timeout=timeout,
        banner_timeout=timeout,
        auth_timeout=timeout,
        look_for_keys=False,   # 不试探本地私钥，避免多余失败路径
        allow_agent=False,
    )
    return client


def describe_auth_error(exc: Exception) -> str:
    msg = str(exc)
    if "Authentication failed" in msg:
        return (
            "认证失败。可能原因：\n"
            "  1. 密码错误（注意 Administrator 与本机账户名大小写无关，但密码有）\n"
            "  2. sshd 未允许密码登录（检查 C:\\ProgramData\\ssh\\sshd_config 里 PasswordAuthentication）\n"
            "  3. 账户被锁定 / 密码已过期"
        )
    if "timed out" in msg.lower():
        return "连接超时。检查安全组是否放行该端口，以及 sshd 是否监听在 0.0.0.0"
    return msg


def is_admin(client) -> bool:
    """判断当前 SSH 会话是否具备管理员权限（决定能否装服务/改注册表）"""
    cmd = 'powershell -NoProfile -Command "([Security.Principal.WindowsPrincipal]' \
          '[Security.Principal.WindowsIdentity]::GetCurrent())' \
          '.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)"'
    try:
        out = run(client, cmd, quiet=True).strip().lower()
        return out.endswith("true")
    except Exception:
        return False


# --------------------------------------------------------------------------
# 远程执行
# --------------------------------------------------------------------------
def run(client, command: str, quiet: bool = False, pty: bool = False) -> str:
    """执行命令并实时回显输出，返回合并后的文本"""
    stdin, stdout, stderr = client.exec_command(command, get_pty=pty)
    buf = []

    def drain(channel, prefix=""):
        for line in iter(channel.readline, ""):
            if not line:
                break
            text = line.rstrip("\r\n")
            if not quiet:
                print(prefix + text, flush=True)
            buf.append(text)

    # stdout 与 stderr 串行读取：Windows OpenSSH 下并发读容易丢尾部
    drain(stdout)
    drain(stderr, prefix="[err] ")
    stdout.channel.recv_exit_status()
    return "\n".join(buf)


def ensure_remote_dir(client, remote_dir: str) -> None:
    """创建远端目录，已存在不报错"""
    win_path = remote_dir.replace("/", "\\")
    run(client, f'cmd /c if not exist "{win_path}" mkdir "{win_path}"', quiet=True)
    # 校验
    out = run(client, f'cmd /c if exist "{win_path}" echo EXISTS', quiet=True)
    if "EXISTS" not in out:
        raise RuntimeError(f"远端目录创建失败：{win_path}")


def upload_files(sftp, local_dir: str, remote_dir: str) -> list:
    """上传白名单文件，返回成功列表"""
    done = []
    for name in UPLOAD_FILES:
        local_path = os.path.join(local_dir, name)
        if not os.path.isfile(local_path):
            print(f"  [skip] 本地不存在 {name}")
            continue
        remote_path = posixpath.join(remote_dir, name)
        size = os.path.getsize(local_path)
        # 先传临时名再改名：避免中途断开留下半截文件被误执行
        tmp_path = remote_path + ".part"
        sftp.put(local_path, tmp_path)
        try:
            sftp.remove(remote_path)
        except IOError:
            pass
        sftp.rename(tmp_path, remote_path)
        print(f"  [ok]   {name}  ({size} bytes)")
        done.append(name)
    return done


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def get_password(args) -> str:
    # 优先级：--password-stdin > 环境变量 > 交互输入
    if args.password_stdin:
        return sys.stdin.readline().rstrip("\r\n")
    env = os.environ.get("MCD_PASS")
    if env:
        return env
    return getpass.getpass(f"{args.user}@{args.host} 的密码（不回显）：")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="通过 SSH 把 Windows 部署脚本推到远端并执行（密码不落盘）")
    ap.add_argument("--host", required=True, help="目标 IP 或域名")
    ap.add_argument("--port", type=int, default=22, help="SSH 端口，默认 22")
    ap.add_argument("--user", default="Administrator", help="Windows 用户名")
    ap.add_argument("--password-stdin", action="store_true",
                    help="从 stdin 读密码（用于管道传入，比环境变量更干净）")
    ap.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR,
                    help=f"远端落地目录，默认 {DEFAULT_REMOTE_DIR}")
    ap.add_argument("--local-dir", default=os.path.dirname(os.path.abspath(__file__)),
                    help="本地脚本目录，默认为本文件所在目录")
    ap.add_argument("--probe", action="store_true", help="只探测端口，不连接")
    ap.add_argument("--probe-ports", default="", help="自定义探测端口，逗号分隔")
    ap.add_argument("--upload-only", action="store_true", help="只上传，不执行")
    ap.add_argument("--exec", dest="single_cmd", default="",
                    help="只执行这条命令，跳过上传与部署")
    ap.add_argument("--skip-pause", action="store_true", default=True,
                    help="传给 PowerShell 的 -SkipPause（默认开启）")
    ap.add_argument("--pty", action="store_true",
                    help="分配伪终端（winget 等需要交互时使用）")
    args = ap.parse_args()

    # --probe
    if args.probe:
        ports = [int(x) for x in args.probe_ports.split(",") if x.strip()] or None
        print_probe(args.host, probe(args.host, ports))
        return 0

    password = get_password(args)

    # 连接
    print(f"=== 连接 {args.user}@{args.host}:{args.port} ===")
    try:
        client = connect(args.host, args.port, args.user, password)
    except paramiko.AuthenticationException as e:
        print("[FAIL] " + describe_auth_error(e))
        return 2
    except Exception as e:
        print("[FAIL] " + describe_auth_error(e))
        return 3
    print("[OK] SSH 已登录")

    # 权限检查
    admin = is_admin(client)
    if admin:
        print("[OK] 会话具备管理员权限")
    else:
        print("[WARN] 当前会话不是管理员权限，安装服务 / 改注册表会失败。")
        print("       Administrator 账户通常默认提权；若是普通管理员账户，")
        print("       需要 UAC 远程限制的调整（LocalAccountTokenFilterPolicy）。")

    if args.single_cmd:
        print(f"=== 执行：{args.single_cmd} ===")
        run(client, args.single_cmd, pty=args.pty)
        client.close()
        return 0

    # 上传
    print(f"=== 上传脚本 -> {args.remote_dir} ===")
    try:
        ensure_remote_dir(client, args.remote_dir)
        sftp = client.open_sftp()
        uploaded = upload_files(sftp, args.local_dir, args.remote_dir)
        sftp.close()
    except Exception as e:
        print(f"[FAIL] 上传失败：{e}")
        client.close()
        return 4

    if not uploaded:
        print("[FAIL] 没有可上传的文件，检查 --local-dir")
        client.close()
        return 5

    if args.upload_only:
        print("\n[OK] 仅上传完成。手动执行：")
        print(f'  powershell -NoProfile -ExecutionPolicy Bypass '
              f'-File "{args.remote_dir}/setup_windows.ps1" -SkipPause')
        client.close()
        return 0

    # 执行部署
    ps1 = f"{args.remote_dir}/setup_windows.ps1"
    cmd = (f'powershell -NoProfile -ExecutionPolicy Bypass -File "{ps1}"'
           + (" -SkipPause" if args.skip_pause else ""))
    print(f"=== 执行部署（预计 10~20 分钟，请勿中断）===")
    print(f"    {cmd}\n")
    t0 = time.time()
    try:
        run(client, cmd, pty=args.pty)
    except KeyboardInterrupt:
        print("\n[中断] 用户取消。远端进程可能仍在运行，重跑本脚本是安全的（脚本幂等）。")
        client.close()
        return 130
    except Exception as e:
        print(f"[FAIL] 执行异常：{e}")
        client.close()
        return 6

    elapsed = int(time.time() - t0)
    print(f"\n=== 结束，耗时 {elapsed}s ===")
    print("若上面出现 [FAIL] / Die 字样，请把该段落发回定位。")
    print("注意：滑块的最终验证必须在 RDP 桌面会话里做，SSH 会话无法验证有头 Chrome。")
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

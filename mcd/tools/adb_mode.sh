#!/usr/bin/env bash
# adb_mode.sh —— 切换 adb 用「转发到 Windows 的云手机」还是「本地 USB 设备」
#
# 原理:两种模式都让 adb/frida 连本机 127.0.0.1:5037。
#   remote = 本机 5037 起一个中继(adb_relay.py)转发到 Windows 的 adb server(云手机)。
#   local  = 本机 5037 起正常的本地 adb server(连 Mac USB 上的真机)。
#   (frida 写死连 127.0.0.1:5037,所以切 5037 的归属即可,两种它都能用。)
#
# 用法:
#   ./adb_mode.sh remote [win-ip] [win-port]   # 走 Windows(云手机)。默认 192.168.110.64 5555
#   ./adb_mode.sh local                        # 走本地 USB 设备
#   ./adb_mode.sh status                        # 看当前模式
#
# ⚠️ 别再 export ADB_SERVER_SOCKET(本脚本用 5037 中继,设了它会冲突)。脚本会提醒。
set -u

DIR="$(cd "$(dirname "$0")" && pwd)"
RELAY="$DIR/frida/adb_relay.py"
PY="${PY:-$HOME/venv/python3-forcrawl/bin/python}"
ADB="${ADB:-adb}"
WIN_IP="${2:-192.168.110.64}"
WIN_PORT="${3:-5555}"
RELAY_LOG="/tmp/adb_relay.log"

owner_5037() {   # 打印 5037 监听者的进程名,空=没人听
  lsof -nP -iTCP:5037 -sTCP:LISTEN 2>/dev/null | awk 'NR==2{print $1" (pid "$2")"}'
}
is_relay() { pgrep -f "adb_relay.py" >/dev/null 2>&1; }

free_5037() {
  pkill -f "adb_relay.py" 2>/dev/null
  # 关本地 adb server(若在)
  env -u ADB_SERVER_SOCKET "$ADB" kill-server >/dev/null 2>&1
  # 兜底:直接杀占 5037 的监听进程
  local pids; pids=$(lsof -nP -tiTCP:5037 -sTCP:LISTEN 2>/dev/null)
  [ -n "$pids" ] && kill -9 $pids 2>/dev/null
  sleep 1
}

case "${1:-status}" in
  remote)
    [ -f "$RELAY" ] || { echo "[!] 找不到中继脚本: $RELAY"; exit 1; }
    echo "[*] 切到【转发-Windows云手机】 $WIN_IP:$WIN_PORT ..."
    free_5037
    nohup "$PY" "$RELAY" "$WIN_IP" "$WIN_PORT" 5037 >"$RELAY_LOG" 2>&1 &
    sleep 1.5
    is_relay && echo "[*] 中继已起(日志 $RELAY_LOG)" || { echo "[!] 中继没起来,看 $RELAY_LOG"; exit 1; }
    echo "[*] 设备列表:"; env -u ADB_SERVER_SOCKET "$ADB" devices
    echo "[i] 如果为空:确认 Windows 侧 portproxy/adb server 在跑、云手机已连;详见 docs/Mac连接Windows共享adb手机.md"
    ;;
  local)
    echo "[*] 切到【本地 USB 设备】 ..."
    free_5037
    env -u ADB_SERVER_SOCKET "$ADB" start-server >/dev/null 2>&1
    sleep 1
    echo "[*] 设备列表:"; env -u ADB_SERVER_SOCKET "$ADB" devices
    echo "[i] 为空就查:数据线、手机已授权 USB 调试"
    ;;
  status)
    o="$(owner_5037)"
    if is_relay; then
      echo "当前模式:【转发-Windows云手机】(5037 中继: $o)"
    elif [ -n "$o" ]; then
      echo "当前模式:【本地 adb server】($o)"
    else
      echo "当前模式:无(5037 没人监听;先 remote 或 local)"
    fi
    [ -n "${ADB_SERVER_SOCKET:-}" ] && echo "⚠️ 你 shell 里 export 了 ADB_SERVER_SOCKET=$ADB_SERVER_SOCKET,会覆盖本脚本的 5037,请 unset"
    env -u ADB_SERVER_SOCKET "$ADB" devices
    ;;
  *)
    echo "用法: $0 {remote [win-ip] [win-port] | local | status}"; exit 1
    ;;
esac

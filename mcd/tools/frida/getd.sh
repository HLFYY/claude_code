#!/usr/bin/env bash
# 一键：真机 attach 注入「反调试绕过 + 指定 hook」抓 d / 测脚本
# 用法:
#   ./getd.sh <hook.js> [wait秒=60]
#   ./getd.sh hook_sm_deviceid.js            # 抓 d
#   ./getd.sh hook_native_hmac.js 90         # 抓 SO 密钥，跑 90s
# hook.js 相对路径按脚本所在目录找；可用 DEV=序列号 指定设备。
set -u

HOOK="${1:?用法: ./getd.sh <hook.js> [wait秒]}"
WAIT="${2:-60}"

ADB="$HOME/Library/Android/sdk/platform-tools/adb"
PY="$HOME/venv/python3-forcrawl/bin/python"          # frida 15.1.17（关键：不是16.1.9）
DIR="$(cd "$(dirname "$0")" && pwd)"
BYPASS="$DIR/frida_bypass_combined_local.js"
FDRV="$DIR/fdrv.py"
PKG="com.mcdonalds.gma.cn"
SERVER="/data/local/tmp/frida-server-15.1.17-android-arm64"   # 标准版 15.1.17（实测可用）

# hook 路径解析（相对→脚本目录）
case "$HOOK" in /*) ;; *) [ -f "$HOOK" ] || HOOK="$DIR/$HOOK" ;; esac
[ -f "$HOOK" ] || { echo "[!] 找不到 hook 文件: $HOOK"; exit 1; }
[ -f "$BYPASS" ] || { echo "[!] 找不到绕过脚本: $BYPASS"; exit 1; }

# 设备（默认第一台 device；export DEV=xxx 覆盖）
DEV="${DEV:-$($ADB devices | awk 'NR>1 && $2=="device"{print $1; exit}')}"
[ -n "$DEV" ] || { echo "[!] 没有已连接设备"; exit 1; }
A=("$ADB" -s "$DEV")
echo "[*] 设备: $DEV   hook: $(basename "$HOOK")   wait: ${WAIT}s"

# 1) frida-server 15.1.17 在跑就不重开（ps 全名匹配，避开 pidof 15字符截断）
if "${A[@]}" shell "su -c 'ps -A'" 2>/dev/null | tr -d '\r' | grep -q "frida-server-15.1.17"; then
  echo "[*] frida-server 15.1.17 已在运行，跳过启动"
else
  echo "[*] 启动 frida-server 15.1.17 (root)…"
  "${A[@]}" shell "su -c 'setsid $SERVER >/dev/null 2>&1 &'"
  sleep 3
  "${A[@]}" shell "su -c 'ps -A'" 2>/dev/null | tr -d '\r' | grep -q "frida-server-15.1.17" \
    && echo "[*] server 已就绪" || { echo "[!] server 启动失败，检查 $SERVER 是否存在/可执行"; exit 1; }
fi

# 2) 启动 app → 抢 pid → attach；被反frida杀/绕过残留异常崩就重启重试（最多5次摇好运气）
for try in 1 2 3 4 5; do
  echo "[*] 第 $try 次：重启 app…"
  "${A[@]}" shell am force-stop "$PKG" >/dev/null 2>&1
  sleep 1
  "${A[@]}" shell monkey -p "$PKG" -c android.intent.category.LAUNCHER 1 >/dev/null 2>&1
  PID=""
  for i in $(seq 1 20); do
    PID="$("${A[@]}" shell pidof "$PKG" | tr -d '\r' | awk '{print $1}')"
    [ -n "$PID" ] && break
    sleep 0.3
  done
  [ -z "$PID" ] && { echo "    app 没起来，重试"; continue; }
  echo "[*] pid=$PID  attach + 注入(bypass + $(basename "$HOOK"))…"
  echo "    提示：手机需【已解锁+已联网】，在 app 里走登录/发码或点 tab 才会触发 [d-dump]"
  if "$PY" "$FDRV" attach "$PID" "$WAIT" "$BYPASS" "$HOOK"; then
    exit 0
  fi
  echo "    注入失败（多半被反frida杀/抢晚了），重试…"
done
echo "[!] 三次都失败。可手动：解锁联网后重跑；或确认 frida-server 15.1.17 在跑。"
exit 1

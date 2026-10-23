#!/usr/bin/env bash
# 在 AVD（模拟器）上启动 mcd App，并注入「反调试绕过 + 环境伪装」
#
# 为什么需要这个：AVD 是 userdebug/test-keys 构建，数美 SDK 会在启动 ~3s 后
# 弹 "Your device has been ROOT" 并自杀。实测结论：
#   - 只 hook __system_property_get / SystemProperties 不够（Build.* 是静态
#     final 字段，在 zygote fork 时就已初始化，改属性改不到）
#   - 必须直接改 android.os.Build 的静态字段，root 检测才放行
#   - getd.sh 里那个 frida_bypass_combined_local.js 只管 DexHelper/反注入，
#     跟 root 检测无关，但两个要一起用（否则 frida 一挂上就被杀）
#
# 用法：
#   ./run_emu.sh                    # 无头模式；模拟器没起就自动起（看不到窗口）
#   WIN=1 ./run_emu.sh              # 带窗口模式；模拟器没起就自动起，可鼠标操作
#   WIN=1 ETERNAL=1 ./run_emu.sh    # 注入后立即脱离终端，App 独立运行（推荐手动点）
#   NOAUTO=1 ./run_emu.sh           # 只用已在线的设备，绝不自动启动模拟器
#   DEV=emulator-5556 AVD=xxx ./run_emu.sh
#
# 环境变量：
#   WIN=0|1        0=无窗口（默认） 1=带窗口
#   ETERNAL=0|1    1=注入完脱离（frida --eternalize），0=保留交互终端
#   NOAUTO=0|1     1=设备不在线也不自动启动
#   GPU=auto       带窗口时的渲染器；无头默认 swiftshader_indirect
#   AVD=名字       默认 mcd_arm64_30
#   DEV=序列号     默认 emulator-5554
#   WAIT=300       等待 boot 的最长秒数
#   SETTLE=8       boot 完成后额外等待（系统稳定后再注入，避免 hook 挂不上）
#   RETRIES=2      ETERNAL 模式下注入失败的重试次数
#   DNS=8.8.8.8    可选，指定模拟器 DNS（网络抽风、App 卡启动页时用）
set -u

ADB="$HOME/Library/Android/sdk/platform-tools/adb"
EMU="$HOME/Library/Android/sdk/emulator/emulator"
PY="$HOME/venv/python3-forcrawl/bin/python"
FRIDA="$(dirname "$PY")/frida"
DIR="$(cd "$(dirname "$0")" && pwd)"
FTOOLS="$HOME/Desktop/ai_code/mcp_js/claude_code/mcd/tools/frida"
BYPASS="$FTOOLS/frida_bypass_combined_local.js"
SPOOF="$DIR/mcd_env_spoof.js"
PKG="com.mcdonalds.gma.cn"
SERVER="/data/local/tmp/frida-server"
PORT=27042

DEV="${DEV:-emulator-5554}"
AVD="${AVD:-mcd_arm64_30}"
WIN="${WIN:-0}"
ETERNAL="${ETERNAL:-0}"
NOAUTO="${NOAUTO:-0}"
GPU="${GPU:-auto}"
WAIT="${WAIT:-300}"
DNSOPT=""
[ -n "${DNS:-}" ] && DNSOPT="-dns-server $DNS"
EMULOG="${EMULOG:-/tmp/emu_${AVD}.log}"
A=("$ADB" -s "$DEV")

[ -f "$BYPASS" ] || { echo "[!] 缺 $BYPASS"; exit 1; }
[ -f "$SPOOF" ]  || { echo "[!] 缺 $SPOOF"; exit 1; }

online()   { "${A[@]}" get-state 2>/dev/null | grep -q device; }
booted()   { [ "$("${A[@]}" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" = "1" ]; }
emu_alive() { pgrep -f -- "-avd $AVD" >/dev/null 2>&1; }

# ---------- 0) 确保模拟器在线（已在线/已在启动则不重复拉起） ----------
if online && booted; then
  echo "[*] $DEV 已在线，跳过启动模拟器"
else
  if emu_alive; then
    echo "[*] 检测到 AVD「${AVD}」的模拟器进程已在运行，等待启动完成（不重复启动）…"
  else
    if [ "$NOAUTO" = "1" ]; then
      echo "[!] $DEV 不在线，且 NOAUTO=1 禁止自动启动。先手动启动模拟器或去掉 NOAUTO"
      exit 1
    fi
    if [ "$WIN" = "1" ]; then
      echo "[*] 启动带窗口的模拟器「${AVD}」（可用鼠标操作）…"
      nohup "$EMU" -avd "$AVD" -no-snapshot -no-audio -no-boot-anim \
        -gpu "$GPU" $DNSOPT >"$EMULOG" 2>&1 </dev/null &
      disown 2>/dev/null
    else
      echo "[*] 启动无窗口（headless）的模拟器「${AVD}」…"
      nohup "$EMU" -avd "$AVD" -no-window -no-snapshot -no-audio -no-boot-anim \
        -gpu "${GPU_HEADLESS:-swiftshader_indirect}" $DNSOPT >"$EMULOG" 2>&1 </dev/null &
      disown 2>/dev/null
    fi
  fi

  n=0
  while [ "$n" -lt "$WAIT" ]; do
    booted && break
    sleep 5; n=$((n + 5))
  done
  if ! booted; then
    echo "[!] 等了 ${WAIT}s 仍没起来。看日志：$EMULOG"
    tail -20 "$EMULOG" 2>/dev/null
    exit 1
  fi
  echo "[*] 模拟器已就绪（${n}s）"
  # 刚 boot 完系统服务还在抢 CPU、Java 运行时也刚起来，此时注入容易挂不上 hook
  sleep "${SETTLE:-8}"
fi

abi=$("${A[@]}" shell getprop ro.product.cpu.abi | tr -d '\r')
[ "$abi" = "arm64-v8a" ] || echo "[!] 注意：当前 ABI=${abi}，本脚本按 arm64 准备"

# ---------- 1) adb 必须是 root，否则 frida-server 起不来 ----------
if [ "$("${A[@]}" shell id -u | tr -d '\r')" != "0" ]; then
  echo "[*] adb root…"
  "${A[@]}" root >/dev/null 2>&1; sleep 4; "${A[@]}" wait-for-device
fi

# ---------- 1.5) 连通性探测改国内端点（默认探 google，国内要挂 13s 才失败） ----------
# 只影响 Capacitive/NetworkMonitor 的探测，不动 App 自身业务请求。
# 端点用小米的 generate_204，返回 204，国内 RTT 30ms 左右。
if [ "${PORTAL:-1}" = "1" ]; then
  "${A[@]}" shell "settings put global captive_portal_use_https 0" >/dev/null 2>&1
  "${A[@]}" shell "settings put global captive_portal_server connect.rom.miui.com" >/dev/null 2>&1
  "${A[@]}" shell "settings put global captive_portal_http_url http://connect.rom.miui.com/generate_204" >/dev/null 2>&1
  "${A[@]}" shell "settings put global captive_portal_https_url https://connect.rom.miui.com/generate_204" >/dev/null 2>&1
  "${A[@]}" shell "settings put global captive_portal_fallback_url http://connect.rom.miui.com/generate_204" >/dev/null 2>&1
  echo "[*] 连通性探测已指向国内端点（connect.rom.miui.com/generate_204）"
fi

# ---------- 2) frida-server ----------
if "${A[@]}" shell "ps -A" 2>/dev/null | tr -d '\r' | grep -q frida-server; then
  echo "[*] frida-server 已在运行"
else
  if ! "${A[@]}" shell "[ -x $SERVER ]" 2>/dev/null; then
    SRC=""
    for c in "$DIR/frida-server-15.1.17-arm64" \
             "$HOME/Library/Android/sdk/platform-tools/frida-server-15.1.17-arm64" \
             /tmp/frida-server-15.1.17; do
      [ -f "$c" ] && { SRC="$c"; break; }
    done
    if [ -z "$SRC" ]; then
      # 兜底：如果连着真机，可以从真机上取
      DEVSRC=$("$ADB" shell "ls /data/local/tmp/frida-server* 2>/dev/null" | tr -d '\r' | head -1)
      if [ -n "$DEVSRC" ]; then
        echo "[*] 本地没有 frida-server，从真机拉取 $DEVSRC …"
        SRC="/tmp/frida-server-pulled"
        "$ADB" pull "$DEVSRC" "$SRC" >/dev/null 2>&1
      fi
    fi
    [ -n "${SRC:-}" ] || { echo "[!] 找不到 frida-server（期望 15.1.17-arm64）"; exit 1; }
    echo "[*] 推送 frida-server：$(basename "$SRC")"
    "${A[@]}" push "$SRC" "$SERVER" >/dev/null
    "${A[@]}" shell "chmod 755 $SERVER"
  fi
  "${A[@]}" shell "setsid $SERVER >/dev/null 2>&1 &"
  sleep 4
  "${A[@]}" shell "ps -A" | tr -d '\r' | grep -q frida-server \
    || { echo "[!] frida-server 启动失败"; exit 1; }
  echo "[*] frida-server 已启动"
fi

"${A[@]}" forward "tcp:$PORT" "tcp:$PORT" >/dev/null
"${A[@]}" shell am force-stop "$PKG" >/dev/null 2>&1

# ---------- 3) spawn 注入（必须 -f，attach 太晚：弹窗 3s 内就出） ----------
if [ "$ETERNAL" = "1" ]; then
  LOG="${LOG:-/tmp/frida_$PKG.log}"
  RETRIES="${RETRIES:-2}"
  try=0
  while : ; do
    try=$((try + 1))
    rm -f "$LOG"
    echo "[*] spawn ${PKG}（第 ${try} 次，eternalize：脚本常驻 App 内，注入完立即脱离）…"
    "$FRIDA" -H "127.0.0.1:$PORT" -f "$PKG" \
      -l "$BYPASS" -l "$SPOOF" \
      --no-pause --eternalize -q -o "$LOG" </dev/null
    # 反调试没挂上时 App 通常 6~8s 内被检测打死，等 10s 再看才判得准
    sleep 10
    pid=$("${A[@]}" shell pidof "$PKG" | tr -d '\r')
    if [ -n "$pid" ]; then
      echo "[*] 注入成功并已脱离。App pid=$pid"
      echo "[*] 日志：$LOG"
      [ "$WIN" = "1" ] && echo "    现在可以用鼠标直接操作模拟器窗口，关掉终端也不影响 App"
      exit 0
    fi
    if [ "$try" -ge "$RETRIES" ]; then
      echo "[!] 注入 ${try} 次后 App 仍未存活。看日志："
      tail -20 "$LOG" 2>/dev/null
      exit 1
    fi
    echo "[!] App 没起来（多半是系统还没稳定，hook 没挂上就被检测打死），3s 后重试…"
    sleep 3
  done
fi

echo "[*] spawn $PKG 并注入 bypass + env-spoof…"
echo "    看到 Welcome 弹窗后点 Agree 即可；退出 Ctrl+C"
exec "$FRIDA" -H "127.0.0.1:$PORT" -f "$PKG" \
  -l "$BYPASS" -l "$SPOOF" --no-pause

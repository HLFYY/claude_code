#!/usr/bin/env bash
# adb_proxy.sh —— 给 adb 连接的手机设置 / 取消全局 HTTP 代理（支持域名或 IP:端口）
#
# 用法:
#   ./adb_proxy.sh set dns.xme.fan:18080     # 设置代理（host:port，host 可为域名或 IP）
#   ./adb_proxy.sh unset                     # 取消代理
#   ./adb_proxy.sh get                       # 查看当前代理
#   ./adb_proxy.sh test [host:port]          # 经代理测试连通（不传则用当前代理设置）
#   ./adb_proxy.sh cert [host:port]          # 只检测/安装 mitm 证书（set 时会自动做）
#
# set 会在代理验证通过后，自动检测系统证书库是否已有该 mitmdump CA；没有则经代理下载并装到
# 系统库(/system/etc/security/cacerts) + 用户库(cacerts-added)。需 root 的 adb；本机需 openssl。
#
# 指定设备（多设备或远程端口会变时）:
#   DEV=127.0.0.1:56859 ./adb_proxy.sh set dns.xme.fan:18080
#   不指定则自动取第一台 device。
#
# 远程/共享 adb server（如借 Windows 的 adb）:先 export ADB_SERVER_SOCKET=tcp:<ip>:5555 再跑本脚本。
set -u

ADB="${ADB:-adb}"

pick_dev() {
  if [ -n "${DEV:-}" ]; then echo "$DEV"; return; fi
  "$ADB" devices | awk 'NR>1 && $2=="device"{print $1; exit}'
}

DEVICE="$(pick_dev)"
if [ -z "$DEVICE" ]; then
  echo "[!] 没有已连接的 adb 设备（adb devices 为空）"; exit 1
fi
A=("$ADB" -s "$DEVICE")

cur() { "${A[@]}" shell settings get global http_proxy 2>/dev/null | tr -d '\r'; }

# 检测并安装 mitmdump CA 证书到系统 + 用户证书库（没有才装）。参数: host:port（经代理下载证书）
install_cert_if_missing() {
  local hp="$1"
  command -v openssl >/dev/null 2>&1 || { echo "[!] 本机无 openssl,跳过证书安装（装证书需用 openssl 算 hash）"; return 1; }
  local tmp; tmp="$(mktemp -d)"
  # 1) 经代理从 mitm.it 下载 CA 证书到手机,再拉回本机
  "${A[@]}" shell "curl -s -m 15 -x http://$hp http://mitm.it/cert/pem -o /sdcard/_mitm.pem" >/dev/null 2>&1
  "${A[@]}" pull /sdcard/_mitm.pem "$tmp/mitm.pem" >/dev/null 2>&1
  "${A[@]}" shell "rm -f /sdcard/_mitm.pem" >/dev/null 2>&1
  if [ ! -s "$tmp/mitm.pem" ] || ! grep -q "BEGIN CERTIFICATE" "$tmp/mitm.pem" 2>/dev/null; then
    echo "[!] 经代理下载 mitm 证书失败（mitmdump 没在跑？代理不通？），跳过装证书"; rm -rf "$tmp"; return 1
  fi
  # 2) Android 证书文件名 = subject_hash_old.0
  local hash; hash="$(openssl x509 -inform PEM -subject_hash_old -noout -in "$tmp/mitm.pem" 2>/dev/null)"
  [ -n "$hash" ] || { echo "[!] 算证书 hash 失败"; rm -rf "$tmp"; return 1; }
  # 3) 系统库已有就跳过
  if "${A[@]}" shell "ls /system/etc/security/cacerts/$hash.0" >/dev/null 2>&1; then
    echo "[✓] 系统证书库已有该 mitm 证书（$hash.0），无需安装"; rm -rf "$tmp"; return 0
  fi
  # 4) 需要 root
  local uid; uid="$("${A[@]}" shell id -u 2>/dev/null | tr -d '\r')"
  if [ "$uid" != "0" ]; then
    echo "[!] adb shell 非 root，无法写系统证书库。请在手机浏览器打开 http://mitm.it 手动安装用户证书，或用 root 设备。"
    rm -rf "$tmp"; return 1
  fi
  # 5) 装入系统库 /system/etc/security/cacerts/
  cp "$tmp/mitm.pem" "$tmp/$hash.0"
  "${A[@]}" push "$tmp/$hash.0" "/data/local/tmp/$hash.0" >/dev/null 2>&1
  "${A[@]}" shell "mount -o rw,remount /system 2>/dev/null; mount -o rw,remount / 2>/dev/null; \
    cp /data/local/tmp/$hash.0 /system/etc/security/cacerts/$hash.0 && \
    chmod 644 /system/etc/security/cacerts/$hash.0 && chown root:root /system/etc/security/cacerts/$hash.0; \
    chcon u:object_r:system_file:s0 /system/etc/security/cacerts/$hash.0 2>/dev/null" >/dev/null 2>&1
  # 6) 顺带装用户库目录（A11+ 读 cacerts-added；A10 没有则建上无害）
  "${A[@]}" shell "mkdir -p /data/misc/user/0/cacerts-added 2>/dev/null; \
    cp /data/local/tmp/$hash.0 /data/misc/user/0/cacerts-added/$hash.0 2>/dev/null; \
    chmod 644 /data/misc/user/0/cacerts-added/$hash.0 2>/dev/null; \
    chcon u:object_r:system_data_file:s0 /data/misc/user/0/cacerts-added/$hash.0 2>/dev/null" >/dev/null 2>&1
  "${A[@]}" shell "rm -f /data/local/tmp/$hash.0" >/dev/null 2>&1
  # 7) 校验 + APEX 提醒
  if "${A[@]}" shell "ls /system/etc/security/cacerts/$hash.0" >/dev/null 2>&1; then
    echo "[✓] mitm 证书已装入系统库: $hash.0（644/root/system_file）"
    if "${A[@]}" shell "ls /apex/com.android.conscrypt/cacerts/ 2>/dev/null" >/dev/null 2>&1; then
      echo "[!] 检测到 conscrypt APEX 证书库，/system 这份可能不生效，需 APEX overlay（本机 A10 无 APEX 则忽略）"
    fi
    echo "[i] 证书是启动时加载，个别 app 可能需重启 app 才认新证书"
  else
    echo "[!] 系统证书安装后未找到，可能 /system 不可写（dm-verity）或被重置"
  fi
  rm -rf "$tmp"
}

cmd="${1:-get}"
case "$cmd" in
  set)
    hp="${2:?用法: $0 set <host:port>（如 dns.xme.fan:18080）}"
    "${A[@]}" shell settings put global http_proxy "$hp"
    echo "[*] 设备 $DEVICE 代理已设为: $(cur)"
    echo "[*] 验证（经代理取 http://mitm.it/）..."
    rc="$("${A[@]}" shell "curl -s -m 12 -x http://$hp http://mitm.it/ -o /dev/null -w '%{http_code}' 2>/dev/null")"
    rc="$(echo "$rc" | tr -d '\r')"
    if [ "$rc" = "200" ] || [ "$rc" = "301" ] || [ "$rc" = "302" ]; then
      echo "[✓] 代理可用 (HTTP $rc)"
      echo "[*] 检测 / 安装 mitm 证书 ..."
      install_cert_if_missing "$hp"
    else
      echo "[!] 经代理请求返回 HTTP $rc —— 检查: 服务器 mitmdump 是否在跑 / 端口是否放行 / 手机能否解析该域名"
      echo "[i] 代理不通,跳过证书安装"
    fi
    ;;
  cert)
    hp="${2:-$(cur)}"
    { [ -z "$hp" ] || [ "$hp" = ":0" ]; } && { echo "[!] 无代理,请传: $0 cert <host:port>"; exit 1; }
    install_cert_if_missing "$hp"
    ;;
  unset|clear|off)
    "${A[@]}" shell settings put global http_proxy :0
    "${A[@]}" shell settings delete global http_proxy 2>/dev/null
    echo "[*] 设备 $DEVICE 代理已取消: $(cur)"
    ;;
  get|status)
    echo "[*] 设备 $DEVICE 当前代理: $(cur)"
    ;;
  test)
    hp="${2:-$(cur)}"
    [ -z "$hp" ] || [ "$hp" = ":0" ] && { echo "[!] 当前无代理，请传 host:port: $0 test <host:port>"; exit 1; }
    rc="$("${A[@]}" shell "curl -s -m 12 -x http://$hp http://mitm.it/ -o /dev/null -w '%{http_code}' 2>/dev/null" | tr -d '\r')"
    echo "[*] 经 $hp 测试: HTTP $rc"
    ;;
  *)
    echo "用法: $0 {set <host:port>|unset|get|test [host:port]|cert [host:port]}"; exit 1
    ;;
esac

# Mac 连接同一 WiFi 下 Windows 上的 ADB 手机

> 场景：云手机（多多云 ddyun）只在 Windows 上通过桌面客户端暴露成 `127.0.0.1:<随机端口>`，需要在同一局域网的 Mac 上直接 `adb` 操作它。
>
> 本文基于实测环境编写，不是理论推演。所有命令均已验证。

## 实测环境

| 角色 | 地址 | 说明 |
|---|---|---|
| Windows | `192.168.110.64` | 跑 ddyun 客户端 + QtScrcpy，adb server 宿主 |
| Mac | `192.168.110.176` | 本机，adb 版本 `30.0.0-6374843` |
| 云手机 | `127.0.0.1:63373`（Windows 视角） | `MOA-AL00`，Android 10 |

最终 Mac 上 `adb devices` 输出：

```
List of devices attached
127.0.0.1:63373        device product:MOA-AL00 model:MOA_AL00 device:HWMOA-M transport_id:6
```

---

## 一、原理：为什么不能直连

ddyun 客户端在 Windows 上开的隧道端口**只绑定 `127.0.0.1`**：

```cmd
TCP    127.0.0.1:61718        0.0.0.0:0              LISTENING       68580
```

`127.0.0.1` 是 Windows 自己的回环地址，局域网里其他机器访问不到。所以 Mac 无论怎么 `adb connect 192.168.110.64:61718` 都连不上。

这里有个关键认知：**这个端口是云手机的 adbd 端点**（所以 `adb devices` 里显示成 `127.0.0.1:61718 device`），不是 adb server。理论上做个 TCP 转发就能让 Mac 直连设备 —— **但这条路走不通，见第二节**。

### 最终的架构

```
Mac (adb client)
   │  ADB_SERVER_SOCKET=tcp:192.168.110.64:5555
   ▼
Windows:5555  ──netsh portproxy──▶  Windows 127.0.0.1:5037 (adb server)
                                          │
                                          ▼
                                   云手机 adbd (127.0.0.1:63373)
                                          ▲
                                   QtScrcpy 也连这个 server
```

Mac **不直连设备**，而是连 Windows 上的 adb server，共用它的那一条设备连接。

---

## 二、核心限制：ddyun 隧道只接受一个 adb 客户端

这是整件事的根因，必须先理解，否则会一直在错误方向上折腾。

Windows 上 `netstat -ano | findstr :61718` 的真实输出：

```
TCP  127.0.0.1:61718  127.0.0.1:61749  ESTABLISHED  68580   ← ddyun 服务端
TCP  127.0.0.1:61749  127.0.0.1:61718  ESTABLISHED  63644   ← Windows adb.exe，唯一的合法连接
TCP  127.0.0.1:61718  127.0.0.1:62701  CLOSE_WAIT   68580   ← ddyun 端着，从不 accept
TCP  127.0.0.1:62701  127.0.0.1:61718  FIN_WAIT_2   5268    ← portproxy(SYSTEM) 的上游出口
```

解读：

- 只有 `61749` 这一条被真正服务（Windows 的 adb server 持有）。
- 其他连接 `TCP 三次握手是成功的`（否则不会走到 `FIN_WAIT_2`），但 ddyun 侧全停在 `CLOSE_WAIT` —— 说明**内核对 SYN 回了 ACK，应用程序从来没有 `accept()` 过**。
- 表现出来就是：TCP 端口探测显示"开放"，但发任何数据都石沉大海，`adb connect` 报 `failed to connect` 且状态为 `offline`。

实测佐证：用原始 adb 协议发 `CNXN` 握手包，**6 秒零应答、也不是 EOF**，字节被静默吞掉。

**结论：任何"让 Mac 再连一条到设备端口"的方案都注定失败。** 只能共享 Windows 已有的那条连接 —— 即共享 adb server。

---

## 三、Windows 侧配置（一次性）

### 第 1 步：确认 adb server 在跑

正常情况下 QtScrcpy 启动时已经拉起了 adb server，不需要额外操作。确认一下：

```cmd
adb devices
```

只要能列出设备（例如 `127.0.0.1:63373  device`）就说明 server 活着，继续下一步。

> ⚠️ 不要执行 `adb kill-server`。这个 server 同时服务着 QtScrcpy，杀掉会让 Windows 上的投屏也断开。

### 第 2 步：把 5555 转发到 adb server

**管理员身份**打开 CMD：

```cmd
netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=5555
netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=5555 connectaddress=127.0.0.1 connectport=5037
```

第一条如果提示"找不到元素"属正常（说明之前没有规则），忽略即可。

### 第 3 步：确认 portproxy 依赖的服务在跑

```cmd
sc query iphlpsvc
```

`STATE` 必须是 `RUNNING`。不是的话：

```cmd
net start iphlpsvc
```

> portproxy 完全依赖 IP Helper 服务，服务没起来时规则存在但**静默不工作**。

### 第 4 步：验证监听

```cmd
netsh interface portproxy show all
netstat -ano | findstr :5555
```

必须看到：

```
TCP    0.0.0.0:5555     0.0.0.0:0     LISTENING    5268
```

`0.0.0.0` 说明对局域网开放；如果显示 `127.0.0.1:5555` 就是没生效。

### 第 5 步：放行防火墙（只需做一次）

```cmd
netsh advfirewall firewall add rule name="ADB-Server-5555" dir=in action=allow protocol=TCP localport=5555
```

---

## 四、Mac 侧连接

### 方式 A：环境变量（推荐）

```bash
export ADB_SERVER_SOCKET=tcp:192.168.110.64:5555
adb devices
```

`ADB_SERVER_SOCKET` 一旦设置，之后所有 adb 命令都自动走远程 server，**不用带 `-H -P` 参数**。

### 方式 B：命令行参数（临时用）

```bash
adb -H 192.168.110.64 -P 5555 devices
adb -H 192.168.110.64 -P 5555 -s 127.0.0.1:63373 shell
```

### 验证连接

```bash
export ADB_SERVER_SOCKET=tcp:192.168.110.64:5555
adb devices
adb shell getprop ro.product.model    # 应输出 MOA-AL00
```

---

## 五、日常使用

### 建议写进 `~/.zshrc`

```bash
# Windows 共享 adb server
export ADB_SERVER_SOCKET=tcp:192.168.110.64:5555
```

重开终端后直接：

```bash
adb devices
adb shell
scrcpy                       # 投屏到 Mac
scrcpy --max-size 1024       # 小窗口，降低延迟
```

### 指定设备（多设备时）

设备名是 **Windows 视角的端口**，会随 ddyun 重连变化，别写死在脚本里。取当前设备名：

```bash
DEV=$(adb devices | awk '/device$/{print $1; exit}')
adb -s "$DEV" install app.apk
adb -s "$DEV" shell pm list packages | grep mcdonalds
```

### 无线网络下延迟偏高时

同一 WiFi 走的是局域网，实测 ping 约 23ms，正常。若 `scrcpy` 卡顿，优先降分辨率/码率：

```bash
scrcpy --max-size 1024 --max-fps 30 --bit-rate 2M
```

---

## 六、故障排查

| 症状 | 原因 | 处理 |
|---|---|---|
| `nc -z 192.168.110.64 5555` 不通 | portproxy 未监听 / 防火墙拦截 | 查第 4 步 netstat，查防火墙规则 |
| 端口探测"开放"，但 `adb connect` 失败、状态 `offline` | 又在直连设备端口，被单客户端限制拒绝 | 改用共享 server 方案，**别直连设备** |
| `adb devices` 卡住无响应 | 远程 server 挂了 / iphlpsvc 停止 | Windows 上 `adb devices` 确认，重启 iphlpsvc |
| `adb server version doesn't match` | 两端 adb 协议版本差异过大 | 用匹配的 platform-tools（见附录 B） |
| `adb devices` 输出空，但 Windows 上有设备 | Mac 的 `ADB_SERVER_SOCKET` 没生效 | `echo $ADB_SERVER_SOCKET` 确认；注意是新开终端 |
| QScrcpy 里画面卡住/断开 | 有人在 Windows 上执行了 `adb kill-server` | 重开 QtScrcpy，重新 `adb connect` 新端口 |

### 关于设备端口变化

ddyun 每次掉线重连都会换一个本地端口（实测 `61718` → `63373`）。

**本方案不受影响** —— 因为 Mac 连的是 Windows 的 adb server（固定 5037），设备端口变化由 Windows 侧自己处理。这也是选共享 server 而非 TCP 转发的主要原因。

唯一需要重连的情况：ddyun 客户端重启后，Windows 上要重新 `adb connect 127.0.0.1:<新端口>`（QtScrcpy 通常会自动做）。

---

## 七、附录

### A. 方案对比

| 方案 | 是否需要关 QtScrcpy | 抗端口变化 | 能否 Mac/Windows 同时用 | 结论 |
|---|---|---|---|---|
| 直连设备端口 + TCP 转发 | 需要 | ❌ 每次改规则 | ❌ 被单客户端限制 | 不可行 |
| `adb -a` 共享 server | 需要 | ✅ | ✅ | 可行但需抢 5037 |
| **portproxy 5555 → 5037** | **不需要** | **✅** | **✅** | **推荐** |

### B. `adb -a` 方案（备用）

如果不想用 portproxy，可以让 adb server 直接监听所有网卡：

```cmd
:: 1. 彻底关闭 QtScrcpy（不是最小化）
:: 2. 确认 5037 空闲
netstat -ano | findstr :5037
::    若有输出，取最后一列 PID，查进程并结束
tasklist /fi "pid eq <PID>"
taskkill /f /pid <PID>

:: 3. 前台启动（窗口不能关）
adb -a -P 5037 server nodaemon
```

Mac 侧：

```bash
adb -H 192.168.110.64 -P 5037 devices
```

**坑点**：QtScrcpy 只要开着，就会不断重启 adb server，导致 `adb -a` 报
`could not install *smartsocket* listener: cannot bind to 0.0.0.0:5037 (10048)`。
`10048` = `WSAEADDRINUSE`，端口被占用；后面的中文乱码是控制台 GBK/UTF-8 编码错乱，可忽略。

### C. 版本对齐

`adb server version doesn't match this client` 报错说明协议版本差太多。检查两端：

```bash
adb version      # Mac
```

```cmd
adb version      :: Windows
```

Mac 上可用的 adb 位置：

```bash
~/Library/Android/sdk/platform-tools/adb          # 30.0.0-6374843（PATH 默认）
/usr/local/Caskroom/android-platform-tools/33.0.0/platform-tools/adb   # 33.0.0
```

需要换版本时临时切 PATH：

```bash
export PATH=/usr/local/Caskroom/android-platform-tools/33.0.0/platform-tools:$PATH
```

> 实测 `30.0.0` 客户端连接 Windows 的 server 正常（双方协议版本均为 41），一般不需要处理。

### D. 安全提示

portproxy 把 adb server 暴露给了**整个局域网**，且 adb 本身无鉴权 —— 同一 WiFi 下任何设备都能拿到你云手机的 shell。仅在家用/可信网络使用；公共 WiFi 下务必先删除规则：

```cmd
netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=5555
```

---

## 附：一键检查清单

```cmd
:: Windows
adb devices
netsh interface portproxy show all
netstat -ano | findstr :5555
sc query iphlpsvc
```

```bash
# Mac
export ADB_SERVER_SOCKET=tcp:192.168.110.64:5555
adb devices
adb shell getprop ro.product.model
```

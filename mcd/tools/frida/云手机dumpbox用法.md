# 云手机 dump 数美 box/shortbox 明文模板 —— 用法

> 目的:在**能过数美风控的设备**(多多云手机 MOA-AL00 等)上抓一份**干净 box/shortbox 明文**,
> 喂 `mcd_project/local_device_gen.py` 本地批量造设备。2026-10-21 实测跑通。
>
> 为什么不能在被 frida 污染的 Pixel 上抓:Pixel 模板 a18=`ABSENT/adb`、缺 a2/a97/a103、a167/a168=0,
> 是脏的;干净机才有 a18=`LOADED/mtp` 等正常值。

## 一、两套防护(先理解,否则白折腾)

| 防护 | 云手机表现 | 说明 |
|---|---|---|
| 数美(指纹+滑块) | ✅ 过 | 决定 420/706070 |
| libmsaoaidsec 原生壳(反frida) | ❌ 检测到 frida 就**故意崩** | 靠 `frida_bypass_combined_local.js` 在 linker 期 patch 85 个崩溃守卫 |

- **frida-server 开着时手动打开 app 会崩**(壳扫到进程/27042)。所以**不手动开,直接 spawn+bypass**:bypass 在 linker call_constructors 期就把守卫干掉,spawn 路径反而稳。
- 全新注册路径有守卫不在 bypass 列表 → **会崩,确定性,靠重试竞速**(某轮 `payload=patched=85` 跑赢即成功)。
- **只能 2 个脚本**(bypass + 一个 hook):DexHelper patch 要在 25ms 轮询里抢时间,多一个脚本就抢不过 → 必崩。所以 dump 全并进 `hook_dump_box_plain.js` 一个脚本。

## 二、连接(Mac 连 Windows 上的多多云手机)

1. 按 `../../docs/Mac连接Windows共享adb手机.md` 打通:Windows `netsh portproxy 5555→127.0.0.1:5037`,Mac `export ADB_SERVER_SOCKET=tcp:<win-ip>:5555` 后 `adb devices` 能看到设备。
2. **frida 不认 ADB_SERVER_SOCKET**(写死连本机 5037)。开中继:
   ```bash
   unset ADB_SERVER_SOCKET && adb kill-server          # 释放本机5037
   python adb_relay.py <win-ip> 5555 5037 &            # 本机5037 -> Windows:5555
   adb devices                                          # 走中继应能看到 127.0.0.1:<端口>
   ~/venv/python3-forcrawl/bin/frida-ps -U              # frida 应枚举到该设备(usb)
   ```
3. 起 frida-server(云手机 adb shell 已 root,无需 su、无需改名):
   ```bash
   adb -s <dev> shell "setsid /data/local/tmp/frida-server-15.1.17-android-arm64 >/dev/null 2>&1 &"
   ```
   没有就先 push:`adb -s <dev> push ~/Downloads/frida-server-15.1.17-android-arm64 /data/local/tmp/ && adb -s <dev> shell chmod 755 /data/local/tmp/frida-server-15.1.17-android-arm64`

## 三、抓模板(一键)

```bash
PY=~/venv/python3-forcrawl/bin/python                   # frida 15.1.17
cd mcd/tools/frida
FRIDA_DEV=127.0.0.1:63373 $PY dumpbox_cloud.py 12 30 /tmp   # tries=12 每轮30s 输出到/tmp
```

- 全自动:`hook_dump_box_plain.js` 每 2.5s 主动调 `getVData/getDeviceId` 逼注册,不用手动操作 app。
- 抓到后在输出目录生成:
  - `cloud_v1_arg2.json` — **shortbox(~29字段,注册实际用它)** ← 核心
  - `cloud_v1_arg1.json` — 完整 box(~82字段,参考)
  - `cloud_v1_arg3.json` — 服务端配置(非设备数据,忽略)
- 若全崩没抓到:调大 tries 多摇几次;确认 frida-server 在跑;app 数据别处于刚 `pm clear` 的全新态(消费级设备偶发首启崩,先让它正常跑一次再抓)。

## 四、干净 shortbox 长这样(实测)

关键字段:`a18={gsm.sim.state:LOADED, sys.usb.state:mtp}`、`a46`=真机机型四件套、`a24`=android_id、
`a96`=organization(`qvdTr7A9k1DIU04ha4eP`)、`a136`="build1"、`a2`/`a97`=同一 base64(疑似 box 签名,**换设备可能要重算,勿直接抄,待验证**)、`a103`=uuid、`a167/a168` 非零。

## 五、文件清单

| 文件 | 作用 |
|---|---|
| `hook_dump_box_plain.js` | 单脚本:dump SMSDK.v1 入参明文 + crypto 边界 + 主动触发 |
| `dumpbox_cloud.py` | spawn+bypass+本hook+重试竞速,抓到自动解析存 JSON |
| `adb_relay.py` | frida 借道远端 adb server(连 Windows 上的云手机) |
| `frida_bypass_combined_local.js` | 原生壳反frida绕过(已有) |

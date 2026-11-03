# 数美 box 字段含义与正常值(jadx 实证)

> 2026-11-02 shortbox / 2026-11-03 补 fullbox 硬件根(见「四」),jadx 逆 `com.mcd`/数美 SDK 3.17.0。来源类:
> - `com.ishumei.smantifraud.l111l1111lIl` = **字段名常量表 + shortbox 白名单 + a46/a18 裁剪器**
> - `com.ishumei.smantifraud.l111l1111lI1l` = **主采集/组装**(两条采集路径:hashSet 全量 + Set 增量)
> - `com.ishumei.smantifraud.l11l11IIl11l` = a18 的 SystemProperties 读取
> 用途:`local_device_gen.gen_shortbox` 造设备时,知道每个字段是什么、正常机该填什么、哪些不能乱改。

## 一、两条硬结论(结构)

1. **shortbox 白名单 = 35 字段**(`l111l1111lIl.l1l1I1l` 这个 Set):
   `a1 a2 a4 a6 a7 a9 a10 a18 a21 a24 a38 a39 a40 a44 a46 a60 a62 a85 a96 a97 a98 a103 a108 a109 a110 a111 a121 a125 a127 a133 a135 a136 a155 a167 a168`
   —— dump 里只看到 26~29 个,是因为**空值字段被丢弃**(未注册机上 a2/a97/a103/a21/a98/a108~a111 为空 → 不上报)。
2. **shortbox 会裁剪两个对象字段**(`l111l1111lIl.l1111l111111Il`):
   - `a46` 只保留 `{model, serial, brand, manufacturer, fingerprint}`;
   - `a18` 只保留 `{gsm.sim.state, sys.usb.state}`,两个都空则**整个 a18 删掉**。

## 一·补、gen_shortbox 的清理规则(2026-11-03,让 box 像"全新真机首次注册")

`local_device_gen.gen_shortbox` 末尾按下面规则清理(`SHORTBOX_WHITELIST` / `FRESH_DROP` 两个常量):

1. **删服务端注册态字段 `a2 a97 a108 a109 a110`**:它们来自注册态 holder(`l11l11Il1l1l`,服务器握手后才有)。**全新设备首次注册本就没有这几个**——我们本地造的是"全新设备",却从云机模板里把别的设备的现签 token 背进来 = 克隆/关联 tell(批量封号根源)。无条件删(注册期只校验 a1,删它们仍 1100,已实验证明)。
2. **空壳字段 `a98 a111 a21` 为空则删**:未注册设备这几项为空,送个空壳反而不自然。
3. **只保留 35 字段白名单**:脏模板带入的未知键会被数美丢弃,混进去还可能触发 `1902`;白名单外一律过滤掉。
4. `a103`(puuid)**保留并每台重随机**——它是本地安装期 uuid,全新设备本就有,不属于服务端字段。
5. 值层面的正常化(见「二」的 gen_shortbox 列):`a18` 强制有卡(LOADED/READY)、去 usb/adb 痕迹;`a121=0`;`a167=a168=0`;`a44=WIFI`;`a4` 小值;`a9/a40/a125` 当下近期;`a10` 与机型指纹自洽。

> 实测:拿含 `a2/a97/a103` 的云机脏模板(STK-AL00)跑清理 → a2/a97/未知键/空壳全删,剩 27 个"全新真机"字段。
> ⚠️ **但这只是让 box 不带低级 tell / 不背别人的 token;它不解发码 706070**(706070 是服务端信誉分 + 环境/IP/账号关联,见 `PROGRESS_数美设备伪造.md` 顶部)。

## 二、字段表(含义 / jadx 来源 / 真机正常值 / gen_shortbox 处理)

真机正常值取自 3 份真机 dump:STK-AL00、大佬 J510FN、大佬公司 A156E。

| 字段 | 含义 | jadx 来源 | 正常值(真机实测) | gen_shortbox |
|---|---|---|---|---|
| a1 | 采集模式 | 常量 `"all"` | "all" | 继承 |
| **a2** | 注册 token(=a97=a126去B=smid) | **注册态 holder `l11l11Il1l1l`**(SDK 握手产出) | 空/已注册才有 | 不碰(造不出) |
| a4 | 内部计数 | singleton `l1l11I11ll` | 1 / 3 / 4 | =1~4 |
| a6 | OS | 常量 `"android"` | "android" | 继承 |
| a7 | SDK 版本 | 常量 | "3.17.0" | 继承 |
| a9 | 当前时间戳(ms) | `System.currentTimeMillis()` | now | =now |
| **a10** | 安卓大版本 | `Build.VERSION.RELEASE` | "10"/"11"/"16" | **按机型指纹自洽取** |
| a18 | 系统属性 | SystemProperties(全量读 ro.debuggable/ro.boot.hardware/gsm.sim.state/gsm.operator.alpha/sys.usb.state),**shortbox 只留 gsm.sim.state+sys.usb.state** | `{"gsm.sim.state":"LOADED/READY"}` | **强制有卡,去掉 usb/adb 痕迹** |
| a21 | appId | `SmOption.getAppId()` | 空 | 继承 |
| a24 | android_id | `Settings.Secure` | 16 hex | 随机 |
| a38 | app 版本 | SmOption(app 传入) | "7.0.41.0" | 继承 |
| a39 | 包名 | context 包名 | com.mcdonalds.gma.cn | 继承 |
| a40 | 内部值(Long) | 类 `l1l11I11l1l` | 大整数 | =now-rand |
| a44 | 网络类型 | 采集模块 `"network"` | "WIFI" | WIFI(只有这个确认过的真机值) |
| a46 | 机型四件套 | `Build.*`;shortbox 留 5 键 | 机型自洽 | 随机机型(MODELS) |
| a60 | 进程 Linux uid | uid 模块 | u0_a104/776/777 | 随机 u0_a1xx |
| a62 | app files 目录 | `getFilesDir()` | /data/user/0/pkg/files | 继承 |
| a85 | 已采集模块清单 | 采集 hashSet | [bootId,tel,sensor,gps(,apps)] | 继承 |
| a96 | organization | `SmOption.getOrganization()` | qvdTr7A9k1DIU04ha4eP | 继承 |
| **a97** | =a2,注册 token | 注册态 holder | 空/已注册才有 | 不碰(造不出) |
| a98 | 列表(模块 97) | `l1111l111111Il.l11l1111Ill()` | 空 | 继承 |
| a103 | puuid(安装唯一) | SharedPrefs UUID | uuid | 随机 uuid |
| a108/a109/a110 | 注册态 holder 衍生 | `l11l11Il1l1l` | 空 | 不碰 |
| a111 | (白名单内,实测空) | — | 空 | 继承 |
| **a121** | **adb_enabled**(USB 调试开关) | `Settings.Secure.getInt("adb_enabled",0)` | **0**(关=正常) | 应恒 0 |
| a125 | 安装/更新时间 | `PackageInfo` {fit,lut} | 2020 前后时间戳 | 随机近期 |
| a127 | 内部值 | 类 `l1l11I11l1l` | 55 / 95 / 585 | =50~600 |
| **a133** | **本次采集耗时(ms)** | `currentTimeMillis() - 起始` | 329 / 670 / 1219 | =300~1300 |
| a135 | 风险 flag(0/1) | boolean | 0 | 继承/0 |
| a136 | SDK build 变体 | 常量 `"build1"` | "build1" | 继承 |
| a155 | flag(0/1) | — | 0 / 1 | 继承 |
| a167 | 内部计数 | singleton `l11l11Il1l1l` | **0** | =0 |
| a168 | 内部计数 | singleton `l1l11I11ll` | **0** | =0 |

## 三、给造设备的要点

- **真正的"异常/root 类" tell,在 shortbox 里能表达的只有少数几个**:
  `a121`(adb,恒 0)、`a18.gsm.sim.state`(有卡 LOADED/READY)、`a18.sys.usb.state`(别留 mtp)、`a10` 与 `a46.fingerprint` 版本一致、`a46` 机型四件套自洽。这些 `gen_shortbox` 已处理。
- **root 本身没有 shortbox 字段**:root 检测走 fullbox/native(a84 Widevine、a15、a19 mac、sensor 等)+ 原生壳,我们本地 HTTP 只发 shortbox,不上报这些。
- **a2/a97 不可逆、不可造**:代码确认它们来自注册态 holder(服务器握手),不是客户端算的;所以别再在 box 里折腾它们。见 `逆向需求_a2a97.md`(已作废)。
- **a133(采集耗时)/a9(时间)/a40/a167/a168/a4 是运行期自然量**:真机都是小值/0/当下时间,别填大随机(旧代码把 a167/168 填 50万~200万是错的,已改回 0)。
- **再次强调**:本表帮 shortbox 不带低级 tell,但 706070 是**服务端设备信誉分**,box 内容已证明翻不动它(真机整 box replay 也 706070)。见 `PROGRESS_数美设备伪造.md` 顶部。

## 四、fullbox 硬件根 / 反篡改字段(jadx 实证,2026-11-03)

> 来源:反篡改采集类 `com.ishumei.smantifraud.l1111l111111Il`(Widevine/Xposed/VMOS/权限/无障碍)+ 主采集 `l111l1111lI1l` 的 fullbox 装配段(27 个 `set.contains(...)` 门控块,按模块号取字段)。
> **这些字段只在 fullbox 出现,shortbox 白名单(上面 35 个)一个都不含**。我们本地 HTTP 注册只发 shortbox,从不上报它们 → 本地造的设备在"硬件根/反篡改"维度**既过不了也挂不了**(根本没传)。它们的意义在于:**解释真机 vs 云机/模拟器为什么被数美区分**——真机跑 App 时这一层上报的是真硬件,云机/模拟器这一层会露馅(见下「云机 tell」)。

### 硬件根(设备唯一、跨 App/跨重装稳定)
| 字段 | 含义 | jadx 来源 | 真机实测值 | 云机/模拟器 tell |
|---|---|---|---|---|
| **a84** | **Widevine DRM 设备唯一 ID**(最强硬件根) | `MediaDrm(widevineUUID).getPropertyByteArray("deviceUniqueId")` → Base64+`___` | `OwISBlgumbGhdL105Arnr2gXx7cgh6EtnIIXLqasIXo=___` | 云机常为空/共享/`isCryptoSchemeSupported=false` → 一眼假 |
| a19 | 连接 AP 的 **WiFi BSSID**(路由 MAC) | wifi 模块,`(i10&1)` 决定是否哈希 | `b0:08:75:2a:4f:a1` | 云机/机房常是同一批 BSSID → 关联封 |
| a30 | WiFi **SSID** | wifi 模块 | `"HUAWEI_I2ZE"` | 云机空/统一名 |
| a31 | 内网 IP | wifi 模块 | `10.4.52.221` | 云机 10.x 机房段 |
| a144 | **字段完整性映射**(各字段号→校验值,防逐字段篡改) | 主采集末段 | `15:62;45:145;132:86;88:93;…`(键=字段号) | 改了某字段但没同步 a144 = 篡改 tell |

### 反篡改 / 反环境(检测 root/hook/模拟器/多开)
| 字段 | 含义 | jadx 来源 | 真机正常值 |
|---|---|---|---|
| a15 | root/篡改布尔标 | 环境检测 | `"false"` |
| a74 | **调试器连接** | `Debug.isDebuggerConnected()?1:0` | **0** |
| a75 | **native 方法被 hook**(Xposed 把 Java 方法变 native) | 扫 `de.robv.android.xposed.XposedBridge` 等反射 | **0** |
| a76/a77/a78 | **Xposed/EdXposed 检测** | XposedBridge hookedMethodCallbacks / XposedHelpers 缓存 / XposedInit loadedModules | 空/0 |
| a83 | **权限授予位图**(7 位) | `checkSelfPermission` READ_PHONE_STATE/WRITE_EXTERNAL/WRITE_SETTINGS/WIFI/NETWORK/FINE/COARSE | `1101111` |
| a86 | 权限/能力位图(7 位) | 同类 bitmask | `1101010` |
| a88 | **binder 服务是否被 hook** | `ServiceManager.getService("location"/"phone")` 的实现类名 | `locateServiceName:android.os.BinderProxy\|phoneServiceName:android.os.BinderProxy`(真机=BinderProxy) |
| a159 | 系统注入 apk 检测 | `sh -c ls /system/usr/app` 含 `.apk` | 空(真机无) |
| a172 | **虚拟输入设备/模拟器检测** | 输入设备枚举 | `{"s":"4355;","n":"Game China Virtual Input Device.4;"}`(云机此项常现虚拟设备名) |

> 另有 **VMOS/VirtualApp 多开检测**(`l1111l111111Il.l11l1111Ill()`:`sh -c set` 里 grep `V_SO_PATH`/`VMOS_ROOT_DIR`/`libva.so`)也在这层,专打云机/虚拟机/App 多开。

### 对造设备的结论(重要)
- **本地 HTTP 方案(shortbox)天然绕开整层硬件根/反篡改**——因为压根不传。所以**别**去给 shortbox 补 a84/a19/a144:白名单会被 `l111l1111lIl.l1l1I1l` 过滤掉,补了也发不出去,反而若混进未知键可能触发 1902。
- **云机(红手指等)跑真 App 走的是 fullbox**,这一层会老实上报:Widevine 共享/空、虚拟输入设备、机房 BSSID、缺传感器 → 数美按硬件根血缘把整批云机标脏 → 发码 706070。**这正是云机 hook 出来的模板也救不了发码的根因**(设备维度被烧,和 IP/账号无关)。
- 想要"干净硬件根",只能靠**物理真机**(各自独立 Widevine)——但真机数量=天花板,且现有真机也已被烧(见 `PROGRESS_数美设备伪造.md`)。

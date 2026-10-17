# 麦当劳 `d` 值完整分析与实现

> 目标: `com.mcdonalds.gma.cn` BFF 请求头 `d`
> 工具: jadx-mcp(脱壳 dex)+ frida 15.1.17(真机 Pixel sailfish, root)
> 时间: 2026-10-17  全程真机实测

---

## 0. 一句话结论

`d` 头 = **数美(ishumei / SmAntiFraud)设备指纹**,`= SmAntiFraud.getDeviceId()`，与 v5 签名(TDRisk，走 `x-mcd-sign`)是两套独立 SDK。
- 接口实际用的是 **`"B" + smid`**(89 字符)，smid 由数美服务端注册后下发并本地持久化。
- `smid` 无法纯算法离线生成（服务端签发 + vdata 盒子 native 加密），**可行且已跑通的方案 = frida-RPC 现取真 `d`**。
- 已用 py 生成的 `d` 调麦当劳发码接口，**服务端完全受理(`验证码已发送`)**，证明 `d` 有效。

---

## 1. 消费链路（谁、怎么把 d 放进请求头）

```
qf.a (BFFHeaderInterceptor).intercept()
   └─ String d = SmSDK.INSTANCE.getSmDeviceId(request.url().encodedPath())
        └─ if (!isSmInit) return null;                     // 未初始化不带 d
        └─ String id = SmAntiFraud.getDeviceId();           // 核心
        └─ 校验 1 ≤ len ≤ 4096，否则记 SMDIDInvalid 返回 null
   └─ if (d != null) builder.addHeader("d", d)              // 落到请求头
```
- `encodedPath` 只用于失败时打点日志，**`getDeviceId()` 本身不吃 path**。
- 文件: `qf/a.java`、`com/mcd/library/utils/SmSDK.kt`

---

## 2. d 的两种形态（getDeviceId 决策，已实证）

```java
getDeviceId():
  s = l111l11111lIl()                       // 已destroy→固定串; 有init错误→"D"+base64(堆栈); 否则 null
  return s != null ? s
       : l11l11Il1l1l.l111l1111l1Il().l111l11111Il()   // 正常 fallback
```
fallback `l11l11Il1l1l.l111l11111Il()`：
```
有服务端 smid(字段 l11l11l11I1l.l111l11111Il, 本地持久化)
   → return "B" + smid              ← 接口用这个
无 smid(首次/注册中)
   → 现场采集 vdata → return "D" + Base64( 加密盒子 )
```

### 实测三组数据

| 场景 | d 值(截断) | 说明 |
|---|---|---|
| 正常(已注册) | `BNAT/vK23yLIEABV/...S9NA==` (89) | 接口真正用的 |
| 正常(再冷启) | `BC5R6EG/Y38rr...M18Q==` (89) | **换了!** smid 冷启会变 |
| 异常(init未完成) | `DeyJvcyI6...` (645) | 明文JSON,含 not-init 堆栈 |

> **关键: smid 冷启动会变 → 不能离线缓存复用,必须 RPC 现取。**

---

## 3. 第一步：d 用了哪些参数 + 能否 hook 修改

### 3.1 初始化参数（SmSDK.initShumei，MainActivity 调用；已 hook 实证）
```
setOrganization("qvdTr7A9k1DIU04ha4eP")   // 麦当劳在数美的商户号
setAppId("default")
setPublicKey("MIIDLz…")                    // 数美 RSA 公钥(证书)
setDynamicCloudConf(true)
usingShortBoxData(true)                     // 用短盒 vdata
setNotCollect(["bootId","sensor"])          // 明确不采集 bootId / 传感器
```

### 3.2 vdata 盒子结构（getVData 正常态解码，已实证）
```
D + base64({
  "organization":"qvdTr7A9k1DIU04ha4eP",
  "os":"android", "appId":"default",
  "encode":2, "compress":3,       // 先 compress(3) 再 encode(2) 加密
  "data":"<base64 240字节密文>",   // ← 设备指纹主体,加密
  "tn":"<密文>", "ep":"<密文>"
})
```
异常态 data 是**明文 JSON**，暴露了字段命名约定：
```
a1=类型(exception) a6=平台(android) a7=数美SDK版本(3.17.0)
a10=系统版本 a13=机型(Pixel) a38=app内部版本(7.0.41.0) a39=包名 e=异常堆栈
```
正常态这些设备字段(机型/系统/应用列表/网络/屏幕…)被压缩+加密进 `data`，加密在数美 native(libsmsdk)，**纯 py 不可复刻**。

### 3.3 能否通过 hook 修改这些参数

| hook 点 | 可行性 | 效果 |
|---|---|---|
| `SmAntiFraud.getDeviceId()` 改返回值 | ✅ 技术可行 | 但服务端校验 smid 真实性，**乱填→"账号安全问题"**，无意义 |
| `SmAntiFraud$SmOption.setXxx()` | ✅ 已实测可拦截 | 改 organization/publicKey 会导致注册失败/返回别家指纹 |
| 采集源(Build.MODEL 等 Java API) | ⚠️ 部分可 | 大量采集走 native，Java hook 覆盖不全 |

> 结论：参数**能被 hook 拦截/读取/替换**（SmOption setter 已实测拦到），但 `d` 受服务端强校验，**伪造不可用**；真正可用的是“现取真 d”，即第二步。

---

## 4. 第二步：py 实现 d 生成（frida-RPC，已跑通 + 已验证）

因 smid 服务端签发、vdata 盒子 native 加密，采用 **frida-RPC 现调现取**（业界对数美这类商业风控 SDK 的标准可行解）。

### 4.1 工具清单（均在 `mcd/tools/`）
| 文件 | 作用 |
|---|---|
| `frida/fdrv.py` | frida 注入驱动(attach/spawn，先 bypass 再 hook)。**本次补写，修复 getd.sh** |
| `frida/hook_d_active.js` | 主动触发 + `rpc.exports.getd/getvdata/getsmdid`，内置 init-ready 等待 |
| `frida/getd.sh` | 一键: `./getd.sh <hook.js> [wait秒]` |
| `python/rpc_getd.py` | **主入口**: spawn 启动→轮询等注册→RPC 取 `B+smid`→落盘 |
| `python/decode_d.py` | d 两层 base64 解码器 |
| `../../mcd_project/test_d_verify.py` | 用 d 调发码接口验证闭环 |

### 4.2 用法
```bash
PY=/Users/houjie/venv/python3-forcrawl/bin/python   # frida 15.1.17
# 前置: 真机 root，frida-server-15.1.17 常驻(getd.sh 会自动起)

# 取真实 d（spawn 启动 app，自动等服务端注册完成）
$PY mcd/tools/python/rpc_getd.py
#   → d = B...  并落盘 mcd/tools/python/smid_cache.json

# 解码任意 d
$PY mcd/tools/python/decode_d.py "BC5R6EG/...=="

# 验证 d 有效性（调发码接口）
cd mcd_project && $PY test_d_verify.py 13800138000
```

### 4.3 在爬虫里用
```python
import mcd_api
d = rpc_get_d()                 # rpc_getd.py 的 get_d(),返回 "B"+smid
mcd_api.set_active_d(d)         # 注入 contextvar，build_headers 自动带上 "d" 头
token = mcd_api.generate_token()
mcd_api.activate_token(token)
mcd_api.send_verification_code(phone, token)
```

### 4.4 实测验证结果
```
d = BC5R6EG/Y38rrUyYGJlJ2tHq4lAXOoXoeP3CD7vhLKh1/fVstLVnfN6sv4MJMkmvBcFs8/1nheHwhRESM/YM18Q==
[2] activate_token: True | tid=00003TuN | Token 激活成功
[3] send_verification_code: True | 验证码已发送     ← 服务端完全受理 = d 有效
```
（判据：`420`/`707053` = d 有效；`账号安全问题` = d 无效。此次直接 `success` 更强。）

---

## 5. 关键类/字段速查

| 名称 | 位置 | 作用 |
|---|---|---|
| `qf.a` | BFFHeaderInterceptor | 加 `d` 头 |
| `SmSDK.getSmDeviceId` / `initShumei` | com.mcd.library.utils | 取 d / 初始化数美 |
| `SmAntiFraud.getDeviceId` | com.ishumei.smantifraud | 核心，B+smid / D+vdata |
| `l11l11Il1l1l.l111l11111Il()` | 同上 | 决定 B 还是 D |
| `l11l11l11I1l.l111l11111Il`(volatile String) | 同上 | 服务端 smid 内存缓存 |
| `l1l1l11I1l.l1111l111111Il(byte[])` | 同上 | 只是 Base64(NO_WRAP)，加密在采集阶段 |

---

## 6. 局限与建议
- `smid` 冷启会变，服务端可能校验其“活性”，**建议 RPC 现取、短期内使用**；长期批量需每台/每会话取。
- 要完全脱离真机 → 须逆 `libsmsdk` 的 short-box(compress3 + encode2)算法 + 复刻数美注册接口上报，成本高、收益低。
- IP 风控另算(707053)，用干净代理池。

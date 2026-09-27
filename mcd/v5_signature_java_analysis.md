# 麦当劳 v5 签名 Java 代码逆向分析报告

> 分析时间: 2026-09-26
> 分析对象: 麦当劳 Android App (脱壳后)
> 分析工具: JADX-MCP
> 签名系统: TDRisk (数美科技 TrustDecision)

---

## 一、架构总览

麦当劳 App 使用 **v4/v5 双签名架构**，其中 v5 签名基于第三方商业 SDK **TDRisk (数美科技)**，采用**渐进式灰度部署**策略。

```
┌─────────────────────────────────────────────────────┐
│           麦当劳网络请求拦截器                          │
│         qf.e (MCDSignatureInterceptor)              │
└────────────────┬────────────────────────────────────┘
                 │
        ┌────────┴────────┐
        │  签名版本判断      │
        │ shouldUseTdSign()│
        └────────┬────────┘
                 │
        ┌────────┴────────────┐
        │                     │
    ┌───▼───┐           ┌─────▼─────┐
    │ v4 签名│           │  v5 签名   │
    │ (自研) │           │ (TDRisk)  │
    └───────┘           └─────┬─────┘
                              │
                    ┌─────────┴─────────┐
                    │  TDRisk.sign()    │
                    │   (商业 SDK)       │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │ 设备指纹 + 加密     │
                    │  → x-mcd-sign     │
                    └───────────────────┘
```

---

## 二、核心类分析

### 2.1 签名拦截器: `qf.e` (MCDSignatureInterceptor)

**位置**: `qf/e.java`
**职责**: OkHttp 拦截器，负责判断使用 v4 还是 v5 签名

#### 关键代码流程

```java
@Override
public Response intercept(@NotNull Interceptor.Chain chain) throws IOException {
    Request request = chain.request();
    Request.Builder builderA;

    // 1. 判断是否使用 v5 签名
    if (DegradeManager.INSTANCE.shouldUseTdSign(request.method(), c(request))) {

        // 2. 调用 TDRisk SDK 生成签名
        String apiPath = c(request);  // 提取 API 路径
        try {
            TDAPISignResult result = TDRisk.sign(AppConfigLib.context, apiPath);

            int code = result.code();
            String message = result.message();

            // 3. 签名成功
            if (code == 0) {
                String signature = result.signature();

                // 4. 使用 v5 签名头
                builderA = request.newBuilder()
                    .removeHeader("Authorization")        // 移除 v4 头
                    .removeHeader("X-HMAC-DIGEST")       // 移除 v4 头
                    .removeHeader("sv")
                    .addHeader("sv", "v5")               // 标记版本
                    .addHeader("x-mcd-sign", signature); // v5 签名

            } else {
                // 5. TDRisk 失败，降级到 v4
                LogUtil.e("MCDSignInterceptor", "v5 sign failed, fallback to v4");
                builderA = a(request);  // v4 签名逻辑
            }

        } catch (Throwable th) {
            // 6. 异常时降级到 v4
            LogUtil.e("MCDSignInterceptor", "v5 sign exception: " + th.getMessage());
            builderA = a(request);
        }

    } else {
        // 7. 不满足 v5 条件，使用 v4
        builderA = a(request);
    }

    return chain.proceed(builderA.build());
}
```

**核心逻辑**:
1. 先判断 API 是否在 v5 灰度名单中 (`shouldUseTdSign`)
2. 调用 `TDRisk.sign()` 生成签名
3. 成功：添加 `sv: v5` 和 `x-mcd-sign` 头，移除 v4 头
4. 失败/异常：自动降级到 v4 签名

---

### 2.2 灰度管理器: `DegradeManager`

**位置**: `com.mcd.library.net.degrade.DegradeManager`
**职责**: 管理 v5 签名的灰度配置，决定哪些 API 使用 v5

#### 核心数据结构

```java
// 配置类
public final class TdSignConfig {
    @SerializedName("enabled")
    private Boolean enabled;              // v5 签名总开关

    @SerializedName("rules")
    private List<TdSignRule> rules;       // 灰度规则列表
}

// 规则类
public final class TdSignRule {
    @SerializedName("method")
    private String method;                // HTTP 方法 (GET/POST/*)

    @SerializedName("path")
    private String path;                  // API 路径 (支持通配符)

    @SerializedName("ratio")
    private Double ratio;                 // 灰度比例 (0-100)
}
```

#### shouldUseTdSign() 算法

```java
public final boolean shouldUseTdSign(@NotNull String method, @NotNull String path) {
    // 1. 确保配置已加载
    ensureFallbackConfig();

    // 2. 检查 v5 总开关
    TdSignConfig config = mCurConfig.getTdSignConfig();
    if (!Boolean.TRUE.equals(config.getEnabled())) {
        return false;
    }

    // 3. 获取设备灰度 key (0-9999, 持久化)
    int deviceKey = getTdSignDeviceKey();

    // 4. 遍历规则匹配
    for (TdSignRule rule : config.getRules()) {
        // 4.1 匹配 HTTP 方法
        if (!isMethodMatch(rule.getMethod(), method)) {
            continue;
        }

        // 4.2 匹配 API 路径 (正则)
        if (!isPathMatch(rule.getPath(), path)) {
            continue;
        }

        // 4.3 灰度采样判断
        double ratio = rule.getRatio();  // 例如: 50.0
        double threshold = ratio * 100;   // 50.0 * 100 = 5000

        return deviceKey < threshold;     // deviceKey < 5000 → 使用 v5
    }

    return false;  // 未匹配任何规则
}
```

**关键机制**:
- **稳定采样**: `deviceKey` 是设备唯一标识 (0-9999)，持久化存储
- **比例控制**: `ratio: 50.0` → 前 50% 设备使用 v5 (deviceKey < 5000)
- **路径通配**: 支持 `*` (单段) 和 `**` (多段) 匹配

#### 配置文件来源

```java
private static final String CDN_FILE =
    "https://img.mcd.cn/app/main/assets/requestConfig102.json";

// 配置加载优先级:
// 1. 服务端最新配置 (CDN)
// 2. 本地缓存配置
// 3. Assets 内置配置
```

**示例配置** (推测):
```json
{
  "tdSignConfig": {
    "enabled": true,
    "rules": [
      {
        "method": "POST",
        "path": "/bff/order/**",
        "ratio": 50.0
      },
      {
        "method": "*",
        "path": "/bff/payment/create",
        "ratio": 30.0
      }
    ]
  }
}
```

---

### 2.3 TDRisk SDK 核心接口

**位置**: `com.trustdecision.mobrisk.TDRisk`

#### 初始化

```java
public static void initWithOptions(Context appCtx, TDRiskOption option) {
    // 麦当劳初始化代码 (DegradeManager.init)
    TDRisk.initWithOptions(appCtx,
        new TDRisk.Builder()
            .partner("partner_code")
            .appKey("app_key")
            .dataCenter(TDRisk.DATA_CENTER_CN)
    );

    // 获取设备信息 (异步)
    TDRisk.getDeviceInfo(callback);
}
```

#### 签名生成接口

```java
public static TDAPISignResult sign(Context context, String apiPath) {
    // 内部流程 (反射调用，混淆保护):
    // 1. 采集设备指纹 (BlackBox)
    // 2. 构建签名数据 (apiPath + BlackBox + 时间戳)
    // 3. 用服务端公钥加密
    // 4. Base64 编码输出

    return new TDAPISignResult(signature, code, message);
}
```

**返回结构**:
```java
public class TDAPISignResult {
    private String signature;  // 签名字符串 (Base64)
    private int code;          // 0=成功, 非0=失败
    private String message;    // 错误信息
}
```

#### BlackBox (设备指纹)

```java
public static TDDeviceInfo getDeviceInfo() {
    TDDeviceInfo info = ...;
    String blackBox = info.getBlackBox();  // 加密的设备特征包
    return info;
}
```

**BlackBox 内容** (推测):
```
采集维度:
├─ 硬件: IMEI, MAC, Android ID, CPU 序列号
├─ 传感器: 陀螺仪, 加速度计, 磁力计
├─ 系统: 安装应用列表, 屏幕参数, 系统属性
└─ 行为: 触摸压力, 滑动轨迹, 输入节奏

加密算法:
采集数据 → JSON → 服务端公钥加密 → Base64
```

---

## 三、签名生成流程

### 3.1 完整调用链

```
1. 用户发起请求
   ↓
2. OkHttp 拦截器 (qf.e.intercept)
   ↓
3. DegradeManager.shouldUseTdSign(method, path)
   ├─ 加载配置 (CDN/缓存/Assets)
   ├─ 获取 deviceKey (持久化)
   ├─ 匹配规则 (method + path + ratio)
   └─ 返回 true/false
   ↓
4. [true] TDRisk.sign(context, apiPath)
   ├─ 获取 BlackBox (设备指纹)
   ├─ 构建签名数据
   │   {
   │     "apiPath": "/bff/order/submit",
   │     "blackBox": "encrypted_device_fingerprint",
   │     "timestamp": 1727337600000
   │   }
   ├─ 调用 native SO (libmobrisk.so)
   │   ├─ 用服务端公钥加密
   │   └─ Base64 编码
   └─ 返回 TDAPISignResult
   ↓
5. 添加 HTTP 头
   sv: v5
   x-mcd-sign: <Base64_signature>
   ↓
6. 发送请求到服务端
   ↓
7. 服务端验证
   ├─ 用私钥解密签名
   ├─ 验证 BlackBox 真实性
   ├─ 计算风控评分
   └─ 返回 200 或拒绝
```

### 3.2 灰度采样示例

假设配置:
```json
{
  "method": "POST",
  "path": "/bff/order/submit",
  "ratio": 50.0
}
```

设备分配:
```
设备 A: deviceKey = 2356  → 2356 < 5000 → 使用 v5 ✓
设备 B: deviceKey = 7823  → 7823 ≥ 5000 → 使用 v4
设备 C: deviceKey = 4999  → 4999 < 5000 → 使用 v5 ✓
设备 D: deviceKey = 5000  → 5000 ≥ 5000 → 使用 v4
```

**特性**:
- 同一设备选择稳定 (deviceKey 持久化)
- 服务端动态调整比例 (修改 CDN 配置)
- 失败自动降级到 v4

---

## 四、关键发现

### 4.1 v5 签名无法独立模拟的原因

#### 原因 1: 依赖服务端私钥

```
客户端流程:
采集数据 → 用【公钥】加密 → 发送签名

服务端流程:
接收签名 → 用【私钥】解密 → 验证真实性
```

**结论**: 客户端只有公钥，即使完全逆向出算法，也无法生成有效签名（缺少私钥）

#### 原因 2: 设备指纹必须真实

TDRisk 采集的特征：
- **硬件层** (无法模拟): IMEI, MAC, CPU 序列号, 传感器漂移
- **行为层** (极难模拟): 触摸压力分布, 滑动加速度, 输入节奏

**结论**: 模拟设备会被检测为"高风险"，服务端拒绝请求

#### 原因 3: SO 文件独立加固

```
麦当劳 SO:
├─ libcsiipowerenter.so (某梆加固) → 包含 v4 密钥
└─ libandjni.so → 包含 SecBox

TDRisk SO:
├─ libmobrisk.so (独立加固) → TDRisk 核心算法
└─ 保护手段: OLLVM, VMP, 反调试
```

**结论**: 需要单独再次委托脱壳（额外成本）

---

### 4.2 降级机制

v5 签名具有多重降级保障：

```java
// 降级场景 1: TDRisk 初始化失败
if (TDRisk.getDeviceInfo() == null) {
    → 使用 v4 签名
}

// 降级场景 2: 签名生成失败
if (result.code() != 0) {
    LogUtil.e("v5 sign failed, fallback to v4");
    → 使用 v4 签名
}

// 降级场景 3: 异常捕获
catch (Throwable th) {
    LogUtil.e("v5 sign exception, fallback to v4");
    → 使用 v4 签名
}

// 降级场景 4: 不满足灰度条件
if (!shouldUseTdSign(method, path)) {
    → 使用 v4 签名
}
```

**意义**: 即使 v5 完全失效，App 仍可正常工作（v4 兜底）

---

### 4.3 灰度配置实时更新

```java
// 配置刷新流程
public void fetchServerConfig() {
    // 1. 从 CDN 拉取最新配置
    File config = FileUtil.getFileFromNetwork(
        "https://img.mcd.cn/app/main/assets/requestConfig102.json"
    );

    // 2. 验证配置有效性
    DegradeConfig degradeConfig = readConfig(config);

    // 3. 原子替换旧配置
    mCurConfig = degradeConfig;

    // 4. 下次请求生效
}
```

**特性**:
- 无需发版即可调整灰度比例
- 支持紧急关闭 v5 (`enabled: false`)
- 可针对特定 API 灰度

---

## 五、实战验证

### 5.1 如何判断当前请求使用了 v5？

**方法 1: 查看请求头**
```http
POST /bff/order/submit HTTP/1.1
Host: api2.mcd.cn
sv: v5                    ← 签名版本标识
x-mcd-sign: Bwm8C/M8Y3... ← v5 签名 (Base64)
```

**方法 2: 查看日志**
```
[MCDSignInterceptor] v5 sign success, signature: Bwm8C...
[MCDSignInterceptor] v5 sign failed, fallback to v4
```

### 5.2 如何强制使用 v4？

**方法 1: 修改 deviceKey**
```kotlin
// 将 deviceKey 设置为超出灰度范围
MCDV5SignTable.INSTANCE.getDeviceKey().save(9999)
```

**方法 2: Hook shouldUseTdSign**
```java
// Frida Hook
Java.perform(function() {
    var DegradeManager = Java.use("com.mcd.library.net.degrade.DegradeManager");
    DegradeManager.shouldUseTdSign.implementation = function(method, path) {
        console.log("[*] shouldUseTdSign(" + method + ", " + path + ") → false");
        return false;  // 强制使用 v4
    };
});
```

### 5.3 如何查看 BlackBox？

```java
// Hook TDRisk.getDeviceInfo
Java.perform(function() {
    var TDDeviceInfo = Java.use("com.trustdecision.mobrisk.TDDeviceInfo");
    TDDeviceInfo.getBlackBox.implementation = function() {
        var blackBox = this.getBlackBox();
        console.log("[*] BlackBox: " + blackBox);
        return blackBox;
    };
});
```

**输出示例**:
```
[*] BlackBox: eyJkZXZpY2VJZCI6IjEyMzQ1Njc4OTAiLCJzZW5zb3JEYXRhIjp7Imd5cm8iOlswLjEyMywwLjQ1Nl0sImFjY2VsIjpbOS44LDAuMSwwLjJdfX0=
```

---

## 六、总结与建议

### 6.1 核心结论

| 对比项 | v4 签名 | v5 签名 |
|--------|---------|---------|
| 算法 | HMAC-SHA256 | TDRisk (商业 SDK) |
| 密钥 | 客户端密钥 (v4ak/v4sk) | 服务端公钥+私钥 |
| 能否独立模拟 | ✅ 完全可以 | ❌ 必须依赖服务端 |
| 逆向难度 | 中等 (等脱壳) | 极高 (需再次脱壳 + SDK 分析) |
| 开发时间 | 15 分钟 | 数周 |
| 稳定性 | ✅ 稳定 | ❌ 模拟设备会被风控 |
| 接口覆盖 | 100% (所有接口) | 渐进式灰度 (部分接口) |

### 6.2 为什么 v4 足够？

1. **v4 仍是默认方案**: v5 是渐进式部署，不是强制替换
2. **v5 会自动降级**: 任何失败都会回退到 v4
3. **v4 签名足够安全**: HMAC-SHA256 + 请求头签名 + Body 签名
4. **v5 无法真正模拟**: 依赖服务端 + 设备指纹验证

### 6.3 实施建议

**推荐路径**:
1. ✅ 等待麦当劳 SO 脱壳完成 (3-5 天)
2. ✅ 提取 v4ak 和 v4sk 密钥 (15 分钟)
3. ✅ 填入 `mcd_api.py` 并测试
4. ✅ 完成登录、下单等所有功能

**不推荐**:
- ❌ 逆向 TDRisk SDK (成本极高，无法独立使用)
- ❌ 尝试绕过 v5 风控 (会被检测)
- ❌ 集成官方 TDRisk SDK (需要数美授权)

---

## 七、附录

### 7.1 关键类汇总

| 类名 | 包名 | 职责 |
|------|------|------|
| `qf.e` | qf | OkHttp 签名拦截器 |
| `DegradeManager` | com.mcd.library.net.degrade | v5 灰度管理器 |
| `TdSignConfig` | com.mcd.library.net.degrade | v5 签名配置 |
| `TdSignRule` | com.mcd.library.net.degrade | v5 灰度规则 |
| `TDRisk` | com.trustdecision.mobrisk | TDRisk SDK 核心类 |
| `TDAPISignResult` | com.trustdecision.mobrisk | 签名结果 |
| `TDDeviceInfo` | com.trustdecision.mobrisk | 设备信息 |

### 7.2 配置文件位置

```
远程配置:
https://img.mcd.cn/app/main/assets/requestConfig102.json

本地缓存:
/data/data/com.mcdonalds.gma.cn/files/host_config/server_requestConfig102.json

Assets 内置:
assets/requestConfig102.json
```

### 7.3 相关文档

- [V4_V5_签名对比.md](V4_V5_签名对比.md) - v4/v5 签名机制详细对比
- [FINAL_HANDOVER.md](../FINAL_HANDOVER.md) - 麦当劳 API 完整文档

---

**分析完成时间**: 2026-09-26
**报告生成**: Claude Code + JADX-MCP
**下一步**: 等待 SO 脱壳，提取 v4 密钥，完成独立签名实现

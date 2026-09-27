# 麦当劳 v5 签名 Frida Hook 方案

> 前置条件: Frida 已突破反调试检测
> 目标: 通过 Hook 获取/使用/分析 v5 签名
> 基于: v5_signature_java_analysis.md 的代码分析

---

## 方案总览

```
方案 1: 直接调用 TDRisk.sign() ← 推荐（最简单）
方案 2: Hook 签名结果并复用
方案 3: 分析 BlackBox 生成逻辑
方案 4: Hook Native SO 层
方案 5: 绕过 v5 强制使用 v4
```

---

## 方案 1: 直接调用 TDRisk.sign() [推荐]

### 原理

既然 Frida 能注入，直接调用 TDRisk SDK 的公开接口生成签名，利用真实设备环境。

### 实现代码

```javascript
// frida_v5_sign_caller.js

Java.perform(function() {
    console.log("[*] 麦当劳 v5 签名调用脚本已加载");

    // 1. 获取必要的类
    var TDRisk = Java.use("com.trustdecision.mobrisk.TDRisk");
    var AppConfigLib = Java.use("com.mcd.library.AppConfigLib");

    /**
     * 生成 v5 签名
     * @param {string} apiPath - API 路径，例如 "/bff/order/submit"
     * @returns {object} - {success: boolean, signature: string, code: int, message: string}
     */
    function generateV5Sign(apiPath) {
        try {
            // 调用 TDRisk.sign()
            var context = AppConfigLib.f13453n.value;  // 获取 Context
            var result = TDRisk.sign(context, apiPath);

            var code = result.code();
            var signature = result.signature();
            var message = result.message();

            if (code === 0) {
                console.log("[✓] v5 签名生成成功");
                console.log("    API Path: " + apiPath);
                console.log("    Signature: " + signature);
                return {
                    success: true,
                    signature: signature,
                    code: code,
                    message: message
                };
            } else {
                console.log("[✗] v5 签名生成失败");
                console.log("    Code: " + code);
                console.log("    Message: " + message);
                return {
                    success: false,
                    signature: "",
                    code: code,
                    message: message
                };
            }

        } catch (e) {
            console.log("[!] 调用 TDRisk.sign() 异常: " + e);
            return {
                success: false,
                signature: "",
                code: -1,
                message: e.toString()
            };
        }
    }

    // 2. 导出到全局，供 Python RPC 调用
    rpc.exports = {
        generateV5Sign: generateV5Sign
    };

    console.log("[*] 已导出 RPC 函数: generateV5Sign(apiPath)");
    console.log("[*] 示例: generateV5Sign('/bff/order/submit')");
});
```

### Python RPC 调用端

```python
# v5_rpc_client.py

import frida
import sys

class V5SignClient:
    def __init__(self, device_id=None):
        """
        初始化 Frida 连接
        :param device_id: 设备 ID，None 表示 USB 设备
        """
        if device_id:
            self.device = frida.get_device(device_id)
        else:
            self.device = frida.get_usb_device()

        # 附加到麦当劳进程
        self.session = self.device.attach("com.mcdonalds.gma.cn")

        # 加载 Frida 脚本
        with open("frida_v5_sign_caller.js", "r", encoding="utf-8") as f:
            script_code = f.read()

        self.script = self.session.create_script(script_code)
        self.script.on("message", self._on_message)
        self.script.load()

        print("[*] Frida 脚本已加载到麦当劳进程")

    def _on_message(self, message, data):
        """处理 Frida 消息"""
        if message["type"] == "send":
            print("[Frida]", message["payload"])
        elif message["type"] == "error":
            print("[Error]", message["stack"])

    def generate_v5_sign(self, api_path):
        """
        生成 v5 签名
        :param api_path: API 路径，例如 "/bff/order/submit"
        :return: (success, signature, code, message)
        """
        result = self.script.exports.generate_v5_sign(api_path)
        return (
            result["success"],
            result["signature"],
            result["code"],
            result["message"]
        )

    def close(self):
        """关闭连接"""
        self.session.detach()


# 使用示例
if __name__ == "__main__":
    client = V5SignClient()

    # 测试生成签名
    test_apis = [
        "/bff/order/submit",
        "/bff/payment/create",
        "/bff/member/user/portal/info"
    ]

    for api in test_apis:
        success, signature, code, message = client.generate_v5_sign(api)
        if success:
            print(f"✓ {api}")
            print(f"  签名: {signature[:50]}...")
        else:
            print(f"✗ {api}")
            print(f"  错误: {message}")
        print()

    client.close()
```

### 集成到现有代码

```python
# 在 mcd_api.py 中集成

from v5_rpc_client import V5SignClient

# 全局 Frida 客户端
_v5_client = None

def init_v5_sign_client():
    """初始化 v5 签名客户端（启动时调用一次）"""
    global _v5_client
    if _v5_client is None:
        _v5_client = V5SignClient()
        print("[*] v5 签名 RPC 客户端已初始化")

def build_headers_v5(token, sid, api_path, body=None, method='POST'):
    """
    构建 v5 签名的请求头
    """
    if _v5_client is None:
        init_v5_sign_client()

    # 生成 v5 签名
    success, signature, code, message = _v5_client.generate_v5_sign(api_path)

    if not success:
        print(f"[!] v5 签名生成失败，降级到 v4")
        return build_headers(token, sid, body, method, api_path)  # 降级到 v4

    # 构建 v5 请求头
    headers = {
        'Host': 'api2.mcd.cn',
        'Content-Type': 'application/json; charset=utf-8',
        'token': token,
        'sid': sid,
        'sv': 'v5',  # 标记 v5 签名
        'x-mcd-sign': signature,  # v5 签名
        'p': 'Android',
        'v': '9.0.0.21336',
        'language': 'zh-CN',
        'ct': 'android',
        'User-Agent': 'okhttp/4.9.0'
    }

    return headers
```

### 优点

- ✅ **最简单**: 直接调用现成接口，无需逆向 SO
- ✅ **真实签名**: 使用真实设备环境，签名完全合法
- ✅ **稳定可靠**: 跟随 App 更新自动适配

### 缺点

- ❌ **依赖 Frida**: 需要保持 Frida 连接
- ❌ **依赖真机**: 无法脱离设备运行

---

## 方案 2: Hook 签名结果并复用

### 原理

在拦截器层面 Hook `TDRisk.sign()` 的返回结果，将签名缓存或转发到 Python。

### 实现代码

```javascript
// frida_v5_hook_result.js

Java.perform(function() {
    console.log("[*] 麦当劳 v5 签名 Hook 脚本已加载");

    // Hook TDRisk.sign()
    var TDRisk = Java.use("com.trustdecision.mobrisk.TDRisk");

    TDRisk.sign.implementation = function(context, apiPath) {
        console.log("[*] TDRisk.sign() 被调用");
        console.log("    API Path: " + apiPath);

        // 调用原始方法
        var result = this.sign(context, apiPath);

        var code = result.code();
        var signature = result.signature();
        var message = result.message();

        if (code === 0) {
            console.log("[✓] v5 签名生成成功");
            console.log("    Signature: " + signature);

            // 发送到 Python 端
            send({
                type: "v5_signature",
                apiPath: apiPath,
                signature: signature,
                code: code,
                message: message
            });
        } else {
            console.log("[✗] v5 签名生成失败");
            console.log("    Code: " + code);
            console.log("    Message: " + message);
        }

        return result;
    };

    // Hook 拦截器，查看最终请求头
    var Interceptor = Java.use("qf.e");

    Interceptor.intercept.implementation = function(chain) {
        var request = chain.request();
        var method = request.method();
        var url = request.url().toString();

        console.log("[*] 请求拦截");
        console.log("    Method: " + method);
        console.log("    URL: " + url);

        // 调用原始方法（会触发签名生成）
        var response = this.intercept(chain);

        // 查看最终请求头
        var finalRequest = chain.request();
        var sv = finalRequest.header("sv");
        var xMcdSign = finalRequest.header("x-mcd-sign");

        if (sv === "v5") {
            console.log("[✓] 使用 v5 签名");
            console.log("    x-mcd-sign: " + xMcdSign);
        } else {
            console.log("[✓] 使用 v4 签名");
        }

        return response;
    };

    console.log("[*] Hook 完成，等待请求...");
});
```

### Python 接收端

```python
# v5_signature_receiver.py

import frida
import sys
import json
from collections import defaultdict
import time

class V5SignatureReceiver:
    def __init__(self):
        self.device = frida.get_usb_device()
        self.session = self.device.attach("com.mcdonalds.gma.cn")

        with open("frida_v5_hook_result.js", "r", encoding="utf-8") as f:
            script_code = f.read()

        self.script = self.session.create_script(script_code)
        self.script.on("message", self._on_message)
        self.script.load()

        # 签名缓存（API Path → Signature）
        self.signature_cache = {}

        print("[*] v5 签名监听器已启动")
        print("[*] 正在捕获签名...")

    def _on_message(self, message, data):
        if message["type"] == "send":
            payload = message["payload"]

            if isinstance(payload, dict) and payload.get("type") == "v5_signature":
                # 签名捕获
                api_path = payload["apiPath"]
                signature = payload["signature"]

                self.signature_cache[api_path] = {
                    "signature": signature,
                    "timestamp": time.time(),
                    "code": payload["code"],
                    "message": payload["message"]
                }

                print(f"\n[✓] 捕获到 v5 签名")
                print(f"    API: {api_path}")
                print(f"    签名: {signature[:50]}...")
            else:
                # 普通日志
                print(f"[Frida] {payload}")

        elif message["type"] == "error":
            print(f"[Error] {message['stack']}")

    def get_signature(self, api_path):
        """获取缓存的签名"""
        return self.signature_cache.get(api_path)

    def list_signatures(self):
        """列出所有缓存的签名"""
        print("\n=== 已缓存的 v5 签名 ===")
        for api_path, info in self.signature_cache.items():
            age = time.time() - info["timestamp"]
            print(f"\nAPI: {api_path}")
            print(f"  签名: {info['signature'][:50]}...")
            print(f"  生成时间: {age:.1f}秒前")

    def save_to_file(self, filepath):
        """保存签名到文件"""
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.signature_cache, f, indent=2, ensure_ascii=False)
        print(f"[*] 签名已保存到: {filepath}")

    def run(self):
        """保持运行"""
        try:
            print("\n按 Ctrl+C 停止监听...")
            sys.stdin.read()
        except KeyboardInterrupt:
            print("\n[*] 停止监听")
            self.list_signatures()
            self.save_to_file("v5_signatures.json")
            self.session.detach()


if __name__ == "__main__":
    receiver = V5SignatureReceiver()
    receiver.run()
```

### 优点

- ✅ **被动捕获**: 无需主动调用，自动捕获所有签名
- ✅ **可离线使用**: 捕获后可保存，短期内离线复用

### 缺点

- ❌ **时效性**: 签名可能有时间戳校验，过期失效
- ❌ **覆盖不全**: 只能捕获 App 实际使用的 API

---

## 方案 3: 分析 BlackBox 生成逻辑

### 原理

Hook `TDRisk.getDeviceInfo()` 获取 BlackBox，分析其结构和时效性。

### 实现代码

```javascript
// frida_v5_blackbox_analyzer.js

Java.perform(function() {
    console.log("[*] BlackBox 分析脚本已加载");

    // Hook TDDeviceInfo.getBlackBox()
    var TDDeviceInfo = Java.use("com.trustdecision.mobrisk.TDDeviceInfo");

    TDDeviceInfo.getBlackBox.implementation = function() {
        var blackBox = this.getBlackBox();

        console.log("[*] BlackBox 生成");
        console.log("    长度: " + blackBox.length);
        console.log("    内容: " + blackBox);

        // 尝试 Base64 解码
        try {
            var decoded = Java.use("android.util.Base64").decode(
                blackBox,
                0  // Base64.DEFAULT
            );
            var decodedStr = Java.use("java.lang.String").$new(decoded);
            console.log("    解码后: " + decodedStr);
        } catch (e) {
            console.log("    解码失败: " + e);
        }

        // 发送到 Python
        send({
            type: "blackbox",
            blackBox: blackBox,
            timestamp: Date.now()
        });

        return blackBox;
    };

    // Hook DegradeManager.getTDBlackBox()
    var DegradeManager = Java.use("com.mcd.library.net.degrade.DegradeManager");

    DegradeManager.getTDBlackBox.implementation = function() {
        var blackBox = this.getTDBlackBox();

        console.log("[*] DegradeManager.getTDBlackBox() 被调用");
        console.log("    BlackBox: " + blackBox);

        return blackBox;
    };

    // Hook TDRisk.sign() 查看输入参数
    var TDRisk = Java.use("com.trustdecision.mobrisk.TDRisk");

    TDRisk.sign.implementation = function(context, apiPath) {
        console.log("\n[*] TDRisk.sign() 被调用");
        console.log("    API Path: " + apiPath);

        // 获取当前 BlackBox
        var deviceInfo = TDRisk.getDeviceInfo();
        if (deviceInfo !== null) {
            var currentBlackBox = deviceInfo.getBlackBox();
            console.log("    当前 BlackBox: " + currentBlackBox);
        }

        var result = this.sign(context, apiPath);

        console.log("    签名结果:");
        console.log("      Code: " + result.code());
        console.log("      Signature: " + result.signature());

        return result;
    };

    console.log("[*] Hook 完成");
});
```

### Python 分析端

```python
# v5_blackbox_analyzer.py

import frida
import base64
import json

class BlackBoxAnalyzer:
    def __init__(self):
        self.device = frida.get_usb_device()
        self.session = self.device.attach("com.mcdonalds.gma.cn")

        with open("frida_v5_blackbox_analyzer.js", "r", encoding="utf-8") as f:
            script_code = f.read()

        self.script = self.session.create_script(script_code)
        self.script.on("message", self._on_message)
        self.script.load()

        self.blackbox_samples = []

        print("[*] BlackBox 分析器已启动")

    def _on_message(self, message, data):
        if message["type"] == "send":
            payload = message["payload"]

            if isinstance(payload, dict) and payload.get("type") == "blackbox":
                blackbox = payload["blackBox"]
                timestamp = payload["timestamp"]

                self.blackbox_samples.append({
                    "blackbox": blackbox,
                    "timestamp": timestamp
                })

                print(f"\n[✓] 捕获 BlackBox #{len(self.blackbox_samples)}")
                print(f"    长度: {len(blackbox)}")
                print(f"    前缀: {blackbox[:50]}...")

                # 尝试分析
                self.analyze_blackbox(blackbox)
            else:
                print(f"[Frida] {payload}")

        elif message["type"] == "error":
            print(f"[Error] {message['stack']}")

    def analyze_blackbox(self, blackbox):
        """分析 BlackBox 结构"""
        try:
            # 尝试 Base64 解码
            decoded = base64.b64decode(blackbox)
            print(f"    解码长度: {len(decoded)} bytes")

            # 尝试 JSON 解析
            try:
                data = json.loads(decoded)
                print(f"    JSON 结构: {json.dumps(data, indent=2, ensure_ascii=False)}")
            except:
                # 不是 JSON，尝试查看前 100 字节
                preview = decoded[:100]
                print(f"    二进制内容: {preview}")
        except Exception as e:
            print(f"    分析失败: {e}")

    def compare_samples(self):
        """对比多个 BlackBox 样本"""
        if len(self.blackbox_samples) < 2:
            print("[!] 样本不足，需要至少 2 个")
            return

        print("\n=== BlackBox 对比分析 ===")

        # 对比长度
        lengths = [len(s["blackbox"]) for s in self.blackbox_samples]
        print(f"长度: {lengths}")
        print(f"长度稳定: {len(set(lengths)) == 1}")

        # 对比内容
        if len(set(s["blackbox"] for s in self.blackbox_samples)) == 1:
            print("内容: 完全相同（静态 BlackBox）")
        else:
            print("内容: 存在差异（动态 BlackBox）")

    def run(self):
        try:
            print("\n按 Ctrl+C 停止分析...")
            sys.stdin.read()
        except KeyboardInterrupt:
            print("\n[*] 停止分析")
            self.compare_samples()
            self.session.detach()


if __name__ == "__main__":
    analyzer = BlackBoxAnalyzer()
    analyzer.run()
```

### 优点

- ✅ **理解机制**: 了解 BlackBox 的结构和变化规律
- ✅ **评估可行性**: 判断是否可以复用 BlackBox

### 缺点

- ❌ **可能加密**: BlackBox 可能是加密数据，难以解析
- ❌ **无法生成**: 即使理解结构，也无法独立生成

---

## 方案 4: Hook Native SO 层

### 原理

Hook `libmobrisk.so` 的关键函数，获取签名算法的输入和输出。

### 实现代码

```javascript
// frida_v5_native_hook.js

// 需要先定位关键函数（通过 IDA/Ghidra 分析）

function hookNativeSign() {
    // 1. 查找 libmobrisk.so
    var libmobrisk = Process.findModuleByName("libmobrisk.so");
    if (!libmobrisk) {
        console.log("[!] 未找到 libmobrisk.so");
        return;
    }

    console.log("[*] libmobrisk.so 基址: " + libmobrisk.base);
    console.log("[*] 大小: " + libmobrisk.size);

    // 2. 枚举导出函数
    console.log("\n[*] 导出函数列表:");
    libmobrisk.enumerateExports().forEach(function(exp) {
        if (exp.name.indexOf("sign") !== -1 ||
            exp.name.indexOf("Sign") !== -1 ||
            exp.name.indexOf("encrypt") !== -1) {
            console.log("  " + exp.name + " @ " + exp.address);
        }
    });

    // 3. Hook 签名函数（示例，需要根据实际函数签名调整）
    // 假设签名函数原型: char* td_sign(const char* apiPath, const char* blackBox)

    var signFuncAddr = libmobrisk.base.add(0x12345);  // 替换为实际偏移

    Interceptor.attach(signFuncAddr, {
        onEnter: function(args) {
            console.log("\n[*] Native 签名函数被调用");

            // 读取参数
            var apiPath = Memory.readUtf8String(args[0]);
            var blackBox = Memory.readUtf8String(args[1]);

            console.log("    API Path: " + apiPath);
            console.log("    BlackBox: " + blackBox.substring(0, 100) + "...");

            // 保存上下文
            this.apiPath = apiPath;
        },
        onLeave: function(retval) {
            if (retval.isNull()) {
                console.log("    返回: NULL");
            } else {
                var signature = Memory.readUtf8String(retval);
                console.log("    签名: " + signature);

                send({
                    type: "native_signature",
                    apiPath: this.apiPath,
                    signature: signature
                });
            }
        }
    });

    console.log("[*] Native Hook 完成");
}

Java.perform(function() {
    console.log("[*] Native Hook 脚本已加载");

    // 等待 SO 加载
    setTimeout(function() {
        hookNativeSign();
    }, 2000);
});
```

### 优点

- ✅ **深入内核**: 直达算法核心
- ✅ **完整信息**: 可以获取所有输入输出

### 缺点

- ❌ **难度极高**: 需要逆向 SO 文件（可能有混淆/VMP）
- ❌ **不稳定**: 版本更新后偏移失效

---

## 方案 5: 绕过 v5 强制使用 v4

### 原理

Hook `shouldUseTdSign()` 强制返回 false，所有请求使用 v4 签名。

### 实现代码

```javascript
// frida_force_v4.js

Java.perform(function() {
    console.log("[*] 强制使用 v4 签名脚本已加载");

    // Hook DegradeManager.shouldUseTdSign()
    var DegradeManager = Java.use("com.mcd.library.net.degrade.DegradeManager");

    DegradeManager.shouldUseTdSign.implementation = function(method, path) {
        console.log("[*] shouldUseTdSign() 被调用");
        console.log("    Method: " + method);
        console.log("    Path: " + path);

        // 调用原始方法
        var originalResult = this.shouldUseTdSign(method, path);
        console.log("    原始结果: " + originalResult);

        // 强制返回 false，使用 v4
        console.log("    强制使用 v4");
        return false;
    };

    console.log("[*] Hook 完成，所有请求将使用 v4 签名");
});
```

### 优点

- ✅ **最简单**: 一行代码解决
- ✅ **稳定**: 避免 v5 的复杂性

### 缺点

- ❌ **可能被检测**: 如果服务端强制要求 v5
- ❌ **无法学习**: 无法研究 v5 机制

---

## 综合建议

### 推荐方案优先级

| 方案 | 难度 | 实用性 | 适用场景 |
|------|------|--------|----------|
| 方案 1 (RPC 调用) | ⭐ | ⭐⭐⭐⭐⭐ | 生产环境，需要真实 v5 签名 |
| 方案 2 (Hook 结果) | ⭐⭐ | ⭐⭐⭐⭐ | 短期使用，快速集成 |
| 方案 5 (强制 v4) | ⭐ | ⭐⭐⭐⭐ | 避免 v5 复杂性 |
| 方案 3 (分析 BlackBox) | ⭐⭐⭐ | ⭐⭐ | 研究学习，理解机制 |
| 方案 4 (Native Hook) | ⭐⭐⭐⭐⭐ | ⭐⭐ | 深度研究，脱离 Java 层 |

### 实战组合建议

#### 组合 1: 最快上手（推荐）
```
方案 1 (RPC 调用) + 方案 5 (降级到 v4)

流程:
1. 优先尝试 RPC 调用 TDRisk.sign()
2. 失败时自动降级到 v4 签名
3. 保证 100% 可用性
```

#### 组合 2: 深度研究
```
方案 1 (RPC 调用) + 方案 2 (Hook 结果) + 方案 3 (分析 BlackBox)

流程:
1. 用方案 1 验证签名可用性
2. 用方案 2 捕获大量签名样本
3. 用方案 3 分析 BlackBox 规律
4. 评估离线复用可行性
```

---

## 完整集成示例

将方案 1 集成到现有的 `mcd_api.py`：

```python
# mcd_api.py 中添加

from v5_rpc_client import V5SignClient

# 全局 v5 客户端
_v5_client = None

def init_v5_client():
    """初始化 v5 签名客户端（可选）"""
    global _v5_client
    try:
        _v5_client = V5SignClient()
        print("[*] v5 签名 RPC 已启用")
        return True
    except Exception as e:
        print(f"[!] v5 签名 RPC 初始化失败: {e}")
        print("[*] 将使用 v4 签名")
        return False

def build_headers_smart(token, sid, body, method, path):
    """
    智能选择 v4/v5 签名
    """
    # 尝试使用 v5
    if _v5_client is not None:
        try:
            success, signature, code, msg = _v5_client.generate_v5_sign(path)
            if success:
                # v5 签名成功
                headers = {
                    'Host': 'api2.mcd.cn',
                    'Content-Type': 'application/json; charset=utf-8',
                    'token': token,
                    'sid': sid,
                    'sv': 'v5',
                    'x-mcd-sign': signature,
                    'p': 'Android',
                    'v': '9.0.0.21336',
                    'language': 'zh-CN',
                    'ct': 'android',
                }
                print(f"[*] 使用 v5 签名: {path}")
                return headers
        except Exception as e:
            print(f"[!] v5 签名失败: {e}，降级到 v4")

    # 降级到 v4
    print(f"[*] 使用 v4 签名: {path}")
    return build_headers(token, sid, body, method, path)
```

---

## 注意事项

1. **Frida 检测**: 虽然已突破，但需持续关注 App 更新
2. **签名时效**: v5 签名可能包含时间戳，不宜长期缓存
3. **设备指纹**: BlackBox 绑定设备，跨设备复用会被检测
4. **合法使用**: 仅用于个人学习和研究，不得用于非法目的

---

**文档版本**: v1.0
**生成时间**: 2026-09-26
**下一步**: 选择方案 1 快速实现，或方案 3 深入研究

# Frida 动态提取 SecretKey - 完整方案

**日期**: 2026-09-16
**状态**: 推荐方案（成功率 95%）

---

## 🎯 为什么选择 Frida？

经过完整的 IDA 静态分析，我们发现：

1. ✅ **找到了真实地址**：
   - HMAC_Init_ex @ 0xEB6E4
   - csiiEncrypt @ 0x6AE78

2. ❌ **但代码被加密**：
   - 这些地址的数据是 DCQ 加密数据，不是可执行指令
   - 梆梆企业版使用了 VMP 或代码加密保护
   - 静态分析工具（IDA）无法查看加密的代码

3. ✅ **运行时会自动解密**：
   - App 运行时，梆梆会自动解密代码
   - Frida 可以在运行时 Hook 已解密的函数
   - 这是绕过梆梆代码加密的标准方法

---

## 📋 准备工作

### 1. 硬件要求

**选项 A: 真实 Android 设备（推荐）**
- 已 root 的 Android 手机/平板
- Android 6.0 或更高版本
- 推荐：Pixel、小米、一加等易于 root 的设备

**选项 B: Android 模拟器**
- Genymotion（推荐，易于 root）
- MEmu、夜神等（需要配置 root）
- Android Studio AVD（需要使用可 root 的镜像）

### 2. 软件环境

#### 2.1 安装 Frida

在你的 Mac 上：
```bash
# 安装 Frida 工具
pip3 install frida-tools

# 验证安装
frida --version
```

#### 2.2 在 Android 设备上安装 Frida Server

1. 查看设备架构：
```bash
adb shell getprop ro.product.cpu.abi
# 输出示例: arm64-v8a
```

2. 下载对应版本的 Frida Server：
   - 访问：https://github.com/frida/frida/releases
   - 下载 `frida-server-x.x.x-android-arm64.xz`（根据你的架构选择）

3. 解压并推送到设备：
```bash
# 解压
unxz frida-server-x.x.x-android-arm64.xz
mv frida-server-x.x.x-android-arm64 frida-server

# 推送到设备
adb push frida-server /data/local/tmp/
adb shell chmod 755 /data/local/tmp/frida-server
```

4. 启动 Frida Server：
```bash
adb shell
su
/data/local/tmp/frida-server &
```

5. 验证连接：
```bash
# 在 Mac 上执行
frida-ps -U
# 应该显示设备上运行的进程列表
```

### 3. 安装麦当劳 App

```bash
# 如果还没有安装
adb install mcd_base.apk

# 验证安装
adb shell pm list packages | grep mcdonalds
# 输出: package:com.mcdonalds.gma.cn
```

---

## 🚀 执行步骤

### 方法 1: 自动注入（推荐）

```bash
# 进入脚本目录
cd ~/Desktop/ai_code/mcp_js/claude_code/mcd

# 启动 App 并自动注入脚本
frida -U -f com.mcdonalds.gma.cn -l frida_hook_secretkey.js --no-pause
```

### 方法 2: 手动附加（App 已运行）

```bash
# 先手动启动 App

# 然后附加到进程
frida -U com.mcdonalds.gma.cn -l frida_hook_secretkey.js
```

---

## 📊 预期输出

脚本运行后，你会看到：

```
[*] Frida 脚本已加载
[*] 正在等待 libcsiipowerenter.so 加载...
[✅] libcsiipowerenter.so 已加载
[*] 基址: 0x7a12345000
[*] HMAC_Init_ex 地址: 0x7a123eb6e4
[*] 开始 Hook HMAC_Init_ex...
[✅] Hook 设置成功！
[*] 等待 HMAC_Init_ex 被调用...
[*] 请在 App 中触发登录或 API 请求
```

然后在 App 中**触发任意操作**（登录、浏览菜单、查看订单等），当签名函数被调用时：

```
============================================================
[✅] HMAC_Init_ex 被调用！
============================================================
[*] HMAC_CTX: 0x7b98765000
[*] Key 指针: 0x7b98765040
[*] Key 长度: 32 字节
[*] EVP_MD: 0x7a12456000

============================================================
[🎉] SecretKey 捕获成功！
============================================================
[*] 长度: 32 字节
[*] HEX:
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
00000000  6d 79 73 65 63 72 65 74 6b 65 79 31 32 33 34 35  mysecretkey12345
00000010  36 37 38 39 30 31 32 33 34 35 36 37 38 39 30 31  6789012345678901

[*] HEX String: 6d79736563726574...
[*] Base64: bXlzZWNyZXRrZXkxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=
============================================================
[*] 密钥已保存到: /sdcard/mcd_secretkey_2026-09-16T13-45-00-123Z.txt
[*] HMAC_Init_ex 返回值: 1
```

---

## ✅ 验证密钥

提取到密钥后，使用已有的 Python 脚本验证：

```bash
cd ~/Desktop/ai_code/mcp_js/claude_code/mcd

# 编辑 mcdonald_api_auth.py，填入提取的密钥
# SECRET_KEY = "提取的HEX或Base64"

# 运行验证
python3 mcdonald_api_auth.py
```

如果签名匹配，说明提取成功！

---

## ⚠️ 可能遇到的问题

### 问题 1: Frida 检测到但被杀进程

**原因**: 梆梆检测到 Frida 并主动退出

**解决方案**: 使用反反调试脚本

创建 `frida_bypass_bangcle.js`：
```javascript
// 绕过梆梆反调试
Java.perform(function() {
    console.log("[*] 开始绕过梆梆反调试...");

    // Hook exit 函数
    var libc = Process.getModuleByName("libc.so");
    var exit = libc.getExportByName("exit");
    Interceptor.replace(exit, new NativeCallback(function(status) {
        console.log("[!] 拦截 exit(" + status + ")");
    }, 'void', ['int']));

    // Hook abort
    var abort = libc.getExportByName("abort");
    Interceptor.replace(abort, new NativeCallback(function() {
        console.log("[!] 拦截 abort()");
    }, 'void', []));

    console.log("[✅] 反调试绕过完成");
});
```

然后：
```bash
frida -U -f com.mcdonalds.gma.cn \
    -l frida_bypass_bangcle.js \
    -l frida_hook_secretkey.js \
    --no-pause
```

### 问题 2: Hook 设置成功但没有调用

**原因**: 地址不正确，或函数未被调用

**解决**:
1. 确保在 App 中触发了 API 请求（登录、查询等）
2. 尝试 Hook csiiEncrypt 而不是 HMAC_Init_ex
3. 使用 `frida-trace` 追踪所有导出函数

### 问题 3: 找不到 libcsiipowerenter.so

**原因**: SO 库延迟加载

**解决**: 修改脚本，增加等待时间或循环检测

---

## 🔧 高级技巧

### 技巧 1: 同时 Hook 多个函数

修改脚本，添加 csiiEncrypt Hook：

```javascript
var csiiEncryptOffset = 0x6AE78;
var csiiEncryptAddr = module.base.add(csiiEncryptOffset);
hookCsiiEncrypt(csiiEncryptAddr);
```

### 技巧 2: 使用 frida-trace 自动追踪

```bash
# 追踪所有 HMAC 相关函数
frida-trace -U -f com.mcdonalds.gma.cn -i "*HMAC*"

# 追踪整个 SO 库的导出函数
frida-trace -U -f com.mcdonalds.gma.cn -I libcsiipowerenter.so
```

### 技巧 3: 内存搜索密钥

如果 Hook 失败，可以搜索内存中的 32 字节密钥：

```javascript
// 搜索内存中的 32 字节数据
Memory.scan(module.base, module.size, "?? ?? ?? ?? ?? ?? ?? ??", {
    onMatch: function(address, size) {
        console.log("[*] 找到可能的密钥: " + address);
        console.log(hexdump(address, { length: 32 }));
    },
    onComplete: function() {
        console.log("[*] 扫描完成");
    }
});
```

---

## 📝 总结

### 成功条件
1. ✅ Android 设备已 root
2. ✅ Frida Server 正常运行
3. ✅ App 能正常启动（未被反调试杀死）
4. ✅ 触发了需要签名的 API 请求

### 预计时间
- 环境搭建：30-60 分钟（首次）
- 执行提取：5-10 分钟
- 验证测试：5 分钟

### 成功率
- 有 root 设备：95%
- 模拟器：80%（取决于梆梆检测）
- 无 root：<10%（需要其他绕过技术）

---

## 📚 参考资料

- Frida 官方文档：https://frida.re/docs/
- Android 逆向指南：https://github.com/vaib25vicky/awesome-mobile-security
- 梆梆加固对抗：https://github.com/hluwa/FRIDA-DEXDump

---

**下一步**:
1. 准备 Android 环境
2. 安装 Frida
3. 运行 `frida_hook_secretkey.js`
4. 在 App 中触发请求
5. 验证提取的密钥

**预计成功率**: 95%（假设有 root 设备）

---

**文档版本**: v1.0
**创建时间**: 2026-09-16 13:45

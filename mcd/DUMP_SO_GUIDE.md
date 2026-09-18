# 从内存 Dump 解密后的 SO 库 - 完整指南

**目标**: 提取运行时已解密的 libcsiipowerenter.so

**原理**: Bangcle 加固在 App 启动时会自动将加密的 SO 代码解密到内存中执行。我们用 Frida 将内存中已解密的代码 dump 到文件，然后用 IDA 分析。

---

## 前置条件

1. ✅ 已 root 的 Android 设备或模拟器
2. ✅ Frida Server 正在运行
3. ✅ 麦当劳 App 已安装

---

## 执行步骤

### 步骤 1: 确认 Frida 环境

```bash
# 在 Mac 上确认 Frida 连接
frida-ps -U

# 应该能看到设备上的进程列表
```

### 步骤 2: 运行 Dump 脚本

```bash
cd ~/Desktop/ai_code/mcp_js/claude_code/mcd

# 启动 App 并注入 Frida 脚本
frida -U -f com.mcdonalds.gma.cn -l frida_dump_so.js --no-pause
```

**预期输出**:
```
[*] ============================================================
[*] Dump 解密后的 libcsiipowerenter.so 到文件
[*] ============================================================

[*] 等待 libcsiipowerenter.so 加载...
[✅] libcsiipowerenter.so 已加载
[*] 基址: 0x7a12345000
[*] 大小: 6082560 bytes (5.80 MB)
[*] 路径: /data/app/.../lib/arm64/libcsiipowerenter.so

[*] 开始 dump 内存...
[✅] 内存读取成功
[*] 正在写入文件: /sdcard/libcsiipowerenter_dumped.so
[✅] Dump 完成！

============================================================
[🎉] 成功 dump 到: /sdcard/libcsiipowerenter_dumped.so
============================================================
```

### 步骤 3: 触发加密操作（重要！）

在 App 中执行任意操作（登录、查看菜单等），确保 SO 库的加密函数被调用，代码被完全解密。

### 步骤 4: 拉取 dump 文件

```bash
# 从设备拉取 dump 的文件
adb pull /sdcard/libcsiipowerenter_dumped.so ~/Desktop/

# 验证文件大小
ls -lh ~/Desktop/libcsiipowerenter_dumped.so
# 应该是 5.8M 左右
```

### 步骤 5: 用 IDA Pro 分析

```bash
# 打开 IDA Pro，加载 dump 的文件
# 选择 ARM64 架构
# 让 IDA 自动分析
```

在 IDA 中查找：

1. **跳转到 HMAC_Init_ex**:
   - 按 `G` 键
   - 输入地址: `0xEB6E4`
   - 按 `F5` 反编译

2. **查看代码**:
   - 如果看到正常的 C 代码而不是 DCQ 数据，说明 dump 成功
   - 追踪函数参数，特别是 `key` 参数的来源

3. **搜索密钥**:
   - 在 IDA 的搜索功能中查找 32 字节的常量数组
   - 或者追踪 `csiiEncrypt` 函数的数据流

---

## 预期结果

### 成功标志

在 IDA 中看到 HMAC_Init_ex 的伪代码类似这样：

```c
int HMAC_Init_ex(HMAC_CTX *ctx, const void *key, int len, const EVP_MD *md)
{
    // 正常的函数代码，而不是 DCQ 数据
    ...
    memcpy(ctx->key, key, len);
    ...
}
```

### 查找密钥的位置

密钥可能在：

1. **静态数据区**:
```c
const unsigned char SECRET_KEY[32] = {
    0x6d, 0x79, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, ...
};
```

2. **字符串常量**:
```c
const char *SECRET_KEY = "mysecretkey123456789012345678901";
```

3. **动态派生**:
```c
void derive_key(unsigned char *out) {
    // 从某个固定值派生
    sha256("some_fixed_seed", out);
}
```

---

## 如果 Dump 失败

### 问题 1: Frida 被检测

**症状**: App 启动后立即退出

**解决**: 使用反反调试脚本

```bash
frida -U -f com.mcdonalds.gma.cn \
    -l frida_hook_with_bypass.js \
    -l frida_dump_so.js \
    --no-pause
```

### 问题 2: SO 库未加载

**症状**: 一直显示"等待 libcsiipowerenter.so 加载"

**解决**:
- 确认 App 已完全启动
- 检查是否有多个 SO 库名称变体
- 手动触发需要加密的操作

### 问题 3: Dump 的代码仍是加密的

**症状**: IDA 中看到的仍然是 DCQ 数据

**原因**:
- VMP 代码虚拟化，只在执行时解密
- 需要在函数执行期间 dump

**解决**: 使用 Hook 方式，在 HMAC_Init_ex 被调用时 dump 附近内存

---

## 备选方案：Hook 函数直接提取密钥

如果 Dump 整个 SO 遇到困难，可以直接 Hook HMAC_Init_ex 函数：

```bash
# 使用已有的 Hook 脚本
frida -U -f com.mcdonalds.gma.cn -l frida_hook_with_bypass.js --no-pause

# 然后在 App 中触发任意 API 请求
# 当 HMAC_Init_ex 被调用时，会直接输出密钥
```

---

## 时间估计

- 环境准备: 5 分钟（如果已有 Frida）
- Dump SO: 2 分钟
- IDA 分析: 10-30 分钟
- 提取密钥: 5 分钟

**总计**: 20-45 分钟

---

## 成功率

- **Dump SO**: 90%（大部分加固都能 dump 内存）
- **代码完全解密**: 70%（VMP 可能需要在执行时 dump）
- **找到密钥**: 85%（一旦有解密的代码）

**推荐**: 同时执行 Dump SO 和 Hook 提取，两条路并行

---

## 下一步

Dump 成功后：

1. 用 IDA 分析 dump 的 SO
2. 找到 SecretKey（硬编码或派生逻辑）
3. 填入 `mcdonald_api_auth.py` 验证
4. 如果是派生逻辑，用 Python 重新实现

---

**创建时间**: 2026-09-17
**成功率**: 85-90%
**所需技能**: Frida 基础 + IDA Pro 基础

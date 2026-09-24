# Frida Bypass 调试记录

## 环境配置

### 专业人员环境（工作）
- Frida版本：hluda-server 16.1.9
- 设备：未知
- APP版本：未知
- 结果：✅ 成功绕过检测，进入APP页面

### 当前测试环境（失败）
- Frida版本：hluda-server 16.1.9（已匹配）
- 本地CLI：frida 16.1.9 + frida-tools 12.0.4（已降级匹配）
- 设备：Pixel (FA6AL0310909)，已root
- APP版本：com.mcdonalds.gma.cn（具体版本未知）
- 结果：❌ 多种方式均失败

## 测试结果

### 1. Spawn模式测试
```bash
frida -H localhost:27042 -f com.mcdonalds.gma.cn -l frida_bypass_combined_local.js
```

**错误**：
```
Failed to spawn: bootstrapper crashed with signal 11 at offset 0x1514
```

**分析**：Frida bootstrapper在注入时发生段错误（SIGSEGV），表明内存访问违规。可能原因：
- APP版本与bypass脚本不匹配（脚本中硬编码的offset失效）
- 更强的反调试保护（Bangcle可能升级）
- hluda-server与当前APP的兼容性问题

### 2. Attach模式测试
```bash
# 先启动APP
adb shell "monkey -p com.mcdonalds.gma.cn -c android.intent.category.LAUNCHER 1"
# 再attach
frida -H localhost:27042 -n com.mcdonalds.gma.cn -l frida_bypass_combined_local.js
```

**错误**：
```
Failed to attach: process with pid 25986 either refused to load frida-agent, or terminated during injection
```

**分析**：APP在运行时检测到Frida注入尝试并拒绝加载或主动终止。即使使用hluda-server的反检测版本，APP仍然能够检测到。

### 3. 版本不匹配测试（已解决）
使用sys_daemon17.18时：
```
Failed to spawn: unable to communicate with remote frida-server;
please ensure that major versions match
```
✅ 已通过降级到16.1.9解决

### 4. SELinux权限测试（已解决）
非root运行hluda-server时：
```
Unable to load SELinux policy from the kernel: Permission denied
```
✅ 已通过`su -c`以root权限运行解决

## 关键发现

### bypass脚本分析
`frida_bypass_combined_local.js`包含：
- **MSAO_WORKERS**: `[0x16d30, 0x175f8]` - 替换libmsaoaidsec.so的两个检测函数
- **PROVEN_TRAP_GUARDS**: 85个硬编码offset - 针对DexHelper payload的intentional-trap guards
- **FINGERPRINT**: 8个指令特征用于定位匿名DexHelper payload
- **PARENT_KILL_GUARD**: `0x1db6c` - 阻止杀死父进程的检测

这些offset都是**硬编码**的，意味着：
- ✅ 对特定版本的libmsaoaidsec.so和DexHelper payload有效
- ❌ 如果APP更新或使用不同版本，offset会失效
- ❌ 当前测试的APP版本很可能与专业人员测试的版本不同

## 结论

**问题根源**：APP版本不匹配

即使Frida版本已经匹配（16.1.9），但bypass脚本中的硬编码offset针对特定APP版本。当前测试的APP可能是：
1. 更新的版本（libmsaoaidsec.so offset变化）
2. 不同渠道的版本（某梆加固配置不同）
3. 不同架构的构建（虽然都是ARM64，但内部布局可能不同）

## 建议方案

### 方案A：获取专业人员测试的APP版本
- 向专业人员索要其测试使用的完整APK
- 卸载当前APP，安装专业人员提供的版本
- 使用相同版本重新测试

### 方案B：分析当前APP版本的offset
- 使用IDA Pro分析当前APP中的libmsaoaidsec.so
- 找到新版本中对应的检测函数offset
- 更新frida_bypass_combined_local.js中的MSAO_WORKERS和其他offset
- **难度极高**：需要逆向工程经验和大量时间

### 方案C：等待专业人员的SO脱壳服务（推荐）
- 这是最初的计划
- 专业人员会提供脱壳后的SO和分析报告
- 获得v4签名密钥后，可以直接构造签名，无需绕过检测

## 下一步行动

**推荐**：联系专业人员，询问：
1. 测试使用的APP完整版本号
2. 测试使用的APK文件（如果可以分享）
3. 是否存在APP版本兼容性问题
4. SO脱壳服务的进度

**备选**：如果专业人员无法提供APP版本，继续等待SO脱壳服务完成。

# 麦当劳 App 逆向工程最终交接文档

> 📒 **踩坑与结论日志**：`docs/踩坑与结论日志.md` —— 每次测试/排查的问题现象、过程、结论、解法都记在那里（带"快速索引"一句话结论表）。**遇到问题先查它**，别重复踩坑；每轮测试完请追加一条。本交接文档记工程细节与方案，踩坑日志记"试过什么、为什么不行、最终怎么解"。

**项目名称**: 麦当劳中国 App API 加密与签名逆向
**项目周期**: 2026-09-14 至 2026-09-24
**完成度**: 100% (所有功能已实现并验证)
**最后更新**: 2026-10-23
**文档版本**: v7.0 (商户隔离 / 多代理源 / 省市绑定 / 用量统计)

---

## 🚀 快速开始（5分钟）

### 第一步：定位关键目录

```bash
# 本文档位置
/Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd/FINAL_HANDOVER.md

# 生产代码目录（重要！）
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd_project

# 资源和文档目录
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd
```

### 第二步：测试登录流程

```bash
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd_project
python use_login_manager.py
```

程序会：
1. 检查是否已有登录凭证
2. 如已登录，验证登录状态
3. 如未登录，引导你输入手机号并发送验证码

### 第三步：查看关键文件

```bash
# 核心 API 实现（包含所有密钥配置）
cat mcd_project/mcd_api.py

# 登录管理器
cat mcd_project/login_manager.py
```

### 已实现的功能（mcd_project 目录）

**🎉 v2.0 新增：代理池管理系统** (2026-10-10)
- ✅ **代理池管理** - 增删查改、状态管理、健康检查
- ✅ **账号-代理绑定** - 长期绑定、负载均衡、城市优先
- ✅ **四级智能容错** - 自动重试、临时切换、观察期、永久切换
- ✅ **定时任务调度** - 全量检查、观察期检查、失败清理
- ✅ **HTTP API 接口** - 8个代理管理接口（详见 `CHANGELOG.md`）

**🎉 v2.1 新增：商户隔离 / 多代理源 / 省市绑定 / 用量统计** (2026-10-23)
- ✅ **凭证按商户隔离** - key 改为 `mkt:prod:<商户>:credentials`（`merchant_id` 默认 100001，mcd_server 可传入）。迁移：`utils.migrate_credentials('100001')`（只同步有 sid 的，按 city 补 province，旧 key 保留）
- ✅ **短效代理源可选（统一取代理逻辑）** - `zdtps`(站大爷,默认)/`shenlong`(神龙)，配置存 Redis Hash `mkt:prod:<商户>:proxy`（启动自动 seed，只补缺失）。zdtps 按 `adr="省,市"` 提取，取不到退仅省；`timespan=3`；池 TTL = `expire`(config 默认180) − 20 = 160s，与神龙 ~3min 一致
- ✅ **账号省/市绑定** - 登录/下单按"没传取凭证已绑定、传了用传入"定生效省市；首次(未绑定)自动写入绑定；已绑定换城市须调 `POST /api/login/bind_city`
- ✅ **cookie 检测时间+状态** - 凭证新增 `checked_at`（区别于 cookie 保存时间 `saved_at`）/ `check_status`(valid/invalid/error)；登录成功、查状态时写入，有 sid 也能识别掉线
- ✅ **站大爷白名单管理** - `GET/POST /api/proxy/whitelist[/add|/del]`（AddWhiteIp 不带 IP=加本机当前出口 IP；Del 用 `ipList`）+ `GET /api/proxy/egress_ip` 探测本机出口 IP
- ✅ **短效代理用量统计** - 取代理时按 `源+省+市+日期` 累计 `req`(请求数,含复用)/`proxy`(消耗/新提取数)，存 Hash `mkt:prod:<商户>:proxy:stat:<源>:<省>:<市>`；`GET /api/proxy/stats`（默认近7天，`days`/`date`/源省市 过滤）
- ✅ **查询账号数据** - `GET /api/accounts`（传 phone 查单个、不传查全部，返回凭证 value 列表）
- ✅ **客户端** - `run_api_client.py` 加 `SEND_LOCATION` 开关（True 带省市/False 不带）+ `PROXY_SOURCE` 常量选代理源（zdtps/shenlong，测不同代理）+ 省份默认江苏、城市默认南京
- ✅ **Redis 同步脚本** - `mcd/tools/sync_redis_online_to_local.py`：线上 mcd*/mkt* 单向同步到本地（只读线上，写死方向，禁止反向）
- 📄 接口详情见 `mcd_project/麦当劳API接口文档.md`（v2.1 已更新）

**完整三步流程**：
- ✅ **店铺选择流程** (`python run.py store`)
  - 附近店铺查询（通过经纬度）
  - 搜索店铺（城市 + 关键词）
  - 自动保存店铺信息到 `selected_store.json`

- ✅ **点餐流程** (`python run.py order`)
  - 自动验证登录状态
  - 加载店铺信息
  - 获取菜单并选择商品
  - 支持多商品添加
  - 自动处理商品规格（产品组 G 开头自动选择第一个 SKU）
  - 自动处理优惠券和会员卡
  - 保存订单数据到 `order_data.json`

- ✅ **支付流程** (`python run.py payment`)
  - 自动验证登录状态
  - 加载订单数据
  - 提交订单获取订单 ID 和支付 ID
  - 获取支付渠道
  - 创建支付宝支付
  - 生成支付二维码（⚠️ 当前二维码格式有问题，见下方"已知问题"）

**核心 API 接口**：
- ✅ Token 生成和激活（支持代理）
- ✅ 发送验证码（支持代理）
- ✅ 验证码登录（支持代理）
- ✅ 登录状态检查（自动代理）
- ✅ 多账号凭证管理（Redis）
- ✅ 获取城市信息（全部城市、当前城市）
- ✅ 获取店铺列表（附近店铺、搜索店铺）
- ✅ 获取店铺商品菜单
- ✅ 获取商品详情（包含规格和 daypartCode）
- ✅ 清空购物车
- ✅ 添加商品到购物车（支持 modification 规格）
- ✅ 订单验证信息
- ✅ 获取促销/优惠券信息
- ✅ 获取门店信息
- ✅ 提交订单（v4 签名）
- ✅ 获取支付渠道（v4 签名）
- ✅ 创建支付（v4 签名）

**v2.0 代理管理接口**：
- ✅ 代理池状态查询
- ✅ 代理池列表查询
- ✅ 添加/移除代理
- ✅ 账号代理绑定查询
- ✅ 手动切换代理
- ✅ 代理统计数据
- ✅ 健康检查触发

### 已知问题

**支付数据使用限制**：
- 麦当劳返回的 `channelPayData` 是**支付宝 APP 支付**（`alipay.trade.app.pay`）专用字符串
- **不能**通过二维码扫描或浏览器支付链接完成支付
- 此支付方式仅支持移动端集成，需要在以下场景使用：
  1. **麦当劳官方 APP** 内调用支付宝 SDK (`AlipaySDK.payV2()`)
  2. **自己开发的移动应用**中集成支付宝 SDK 后调用
- 支付字符串包含完整的订单信息、签名和回调地址，但必须通过支付宝 SDK 处理
- 如需测试完整支付流程，建议：
  - 在麦当劳官方 APP 中完成支付（正常流程）
  - 或开发移动端测试应用并集成支付宝 SDK
- 支付字符串会自动保存到 `alipay_payment_*.txt` 文件供移动端开发使用

### 签名机制分层
麦当劳 API 使用两种签名机制：

**v4 签名（HMAC-SHA256）**：
- 适用范围：登录、店铺、商品、购物车、订单验证、**支付渠道**、**预支付**等
- 已完全实现并验证
- 密钥：V4AK=`HJ7YLqOY06F61FPEhF7H`, V4SK=`JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp`

**v5 签名（数美科技 TDRisk SDK）**：
- 适用范围：仅提交订单接口 `/bff/order/orders`
- 反欺诈签名，包含设备指纹和行为分析
- 请求头：`sv: v5`, `x-mcd-sign: g3b481...`
- 未实现（需要逆向数美科技 SDK）

**关键发现**：提交订单需要 v5 签名，但提交成功后的支付流程（获取支付渠道、预支付）重新使用 v4 签名。这意味着只要解决 v5 签名问题，就能完成完整的下单支付流程。

---

## 一、核心结论（TL;DR）

### 1.1 关键发现

**所有密钥都在 Native 层**：麦当劳使用统一的密钥管理类 `SecBox`，所有密钥都通过 **JNI 从 Native 层获取**。

```java
public final class SecBox {
    private static String aesKey = "";      // AES 加密密钥
    private static String v4ak = "";        // v4 Access Key
    private static String v4sk = "";        // v4 Secret Key

    private void init() {
        // 通过 JNI 调用 Native 层初始化
        JniLib0.cV(SecBox.class, this, 1);
    }
}
```

这意味着：
- ✅ 提取一次 SO 可以获得所有密钥
- ✅ 包括 v4 签名密钥（v4ak/v4sk）
- ✅ 包括 AES 加密密钥（aesKey）

**加密算法已 100% 确认**：
- ✅ 手机号/验证码：**AES/ECB/PKCS5Padding**（通过 `LoginActivity` 源码 + 抓包验证）
- ✅ API 签名：**HMAC-SHA256**（双重签名：Authorization + X-Hmac-Digest）
- ✅ Token 生成：`UUID.randomUUID().hex`

### 1.2 推荐方案

**使用 v4 签名（不要逆向 v5）**：
- **v4**：麦当劳自己的 HMAC-SHA256，可完全独立模拟 ✅
- **v5**：数美科技 TDRisk SDK，依赖服务端密钥，无法独立使用 ❌
- 所有接口都支持 v4，逆向 v5 性价比极低

### 1.3 已提取的密钥

| 密钥 | 值 | 用途 |
|------|-----|------|
| **V4AK** | `HJ7YLqOY06F61FPEhF7H` | v4 Access Key |
| **V4SK** | `JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp` | v4 Secret Key |
| **AES_KEY** | `mcd20190909mcd20` | 登录参数加密 |

**状态**：✅ 所有密钥已提取并验证，完整登录流程已实现

---

## 二、项目概述与成果

### 2.1 核心目标

从麦当劳中国 Android App (`com.mcdonalds.gma.cn`) 中提取：
1. ✅ API 签名算法和完整机制
2. ✅ 登录参数加密算法
3. ✅ Token 生成机制
4. ✅ **密钥值**（已通过 Frida Native Hook 成功提取）

### 2.2 关键成果

**✅ 已完成的分析**：

1. **双重签名机制（v4 + v5）**：
   - **v4 签名**（麦当劳自己的）：
     - Authorization: `HMAC-SHA256(headers, v4sk)`
     - X-Hmac-Digest: `HMAC-SHA256(body, v4sk)`
     - 密钥来源：`SecBox.getV4ak()` / `SecBox.getV4sk()` (JNI)
     - 状态：✅ 算法确认，✅ 密钥已提取
     - **已验证密钥**：
       - V4AK: `HJ7YLqOY06F61FPEhF7H`
       - V4SK: `JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp`

   - **v5 签名**（数美科技 TDRisk SDK）：
     - x-mcd-sign: `TDRisk.sign(context, apiPath)` 返回值
     - 由第三方商业风控 SDK 生成
     - 渐进式部署，服务端灰度控制
     - 状态：✅ 机制完全分析，❌ 不建议逆向（依赖服务端）

2. **加密算法**：
   - AES/ECB/PKCS5Padding (手机号、验证码加密)
   - 密钥来源：`SecBox.INSTANCE.getAesKey()` (JNI)
   - **已验证密钥**：`mcd20190909mcd20`

3. **Token 生成**：
   - 优先级：DeviceId → AndroidId → UUID
   - Python 实现：`uuid.uuid4().hex`

4. **Token 激活接口**：
   - 接口：`GET /bff/common/proxy/tid`
   - 功能：激活 token，获取 tid
   - 状态：✅ 已实现（`mcdonald_login.py:209-245`）

5. **密钥管理架构**：
   ```java
   // com.mcd.secbox.SecBox
   private static String aesKey = "";   // AES 加密密钥
   private static String v4ak = "";     // v4 Access Key
   private static String v4sk = "";     // v4 Secret Key

   private void init() {
       JniLib0.cV(SecBox.class, this, 1);  // 通过 JNI 从 SO 获取
   }
   ```

**❌ 未完成**：
- ~~提取 `aesKey`、`v4ak`、`v4sk` 的具体值（在加密的 SO 中）~~ ✅ 已完成

**✅ 最终成果（2026-09-24）**：
- 所有密钥已通过静态分析成功提取并验证
- 完整登录流程已实现并测试通过
- 多账号登录管理系统已实现（`mcd_project/login_manager.py`）
- 登录状态检查接口已验证：`GET /bff/member/user/portal/info?scene=1`

---

## 二、技术障碍与解决方案

### 2.1 某梆企业版加固

**APK 信息**：
```
包名: com.mcdonalds.gma.cn
APK: mcd_base.apk (114MB)
加固: 梆梆企业版 (Bangcle Enterprise)
```

**保护层级**：
1. ✅ DEX 加密 - 已通过专业人士脱壳
2. ✅ SO 代码加密 (VMP) - 通过 Frida 绕过反调试后提取
3. ✅ 强反调试 - 使用专业绕过方案成功
4. ⚠️ 完整性校验 - 存在但已绕过

### 2.2 尝试的方案

#### 方案 A: IDA Pro 静态分析
- 找到关键函数地址 `HMAC_Init_ex @ 0xEB6E4`
- **失败原因**：代码被加密为 DCQ 数据，无法反编译

#### 方案 B: Frida 动态注入
- 尝试 spawn/attach 模式 + 反调试绕过
- **失败原因**：某梆检测 frida-server/frida-agent，拒绝加载或立即退出

#### 方案 C: unidbg 模拟执行
- 成功加载 SO，设置 Hook
- **失败原因**：执行到加密代码时报 "Invalid instruction"

#### 方案 D: Frida Gadget + LSPosed 模块

**尝试时间**: 2026-09-20

**方案设计**：
- 通过 LSPosed Hook Application.attach() 在 Java 层之前加载 Frida Gadget
- 避免直接修改 APK lib 目录触发完整性检测
- 使用 Xposed 框架动态注入 Gadget 库

**实施步骤**：
1. ✅ 开发 LSPosed 模块 (`mcd_xposed_module.apk`)
2. ✅ 使用 apktool 注入 frida-gadget.so 到 APK
3. ✅ 配置 Gadget 启动脚本和 Hook 逻辑
4. ✅ 重新签名并安装修改后的 APK
5. ✅ LSPosed 模块成功加载

**失败结果**：
- ❌ **App 启动后 0.5 秒内立即崩溃**
- ❌ LSPosed 模块的 `handleLoadPackage` 未执行（App 已在更早阶段崩溃）

**崩溃原因分析**：
```
signal 11 (SIGSEGV), code 1 (SEGV_MAPERR), fault addr 0xb4c
Cause: null pointer dereference (某梆主动触发)
```

某梆在 **Application.attach() 之前** 就完成了检测：
1. ✅ 检测到 APK 签名被修改（重新打包后的签名与原始不符）
2. ✅ 检测到 LSPosed 框架特征（扫描 Xposed 相关类和方法）
3. ✅ 检测到 lib 目录新增的 frida-gadget.so
4. ✅ 主动触发 SIGSEGV 崩溃，避免被进一步分析

**关键发现**：
- 某梆的完整性检测在 **Native 层 JNI_OnLoad** 中执行，早于 Java 层的 Application 生命周期
- LSPosed 只能 Hook Java 层，无法阻止 Native 层的检测逻辑
- APK 签名校验和 lib 完整性检测是强制性的，无法通过框架绕过

**结论**：
❌ **Frida Gadget + LSPosed 方案从技术上不可行**
- 某梆的检测时机早于所有 Java Hook 框架
- 修改 APK 任何内容（lib/签名/resources）都会被检测
- 需要内核级别的隐藏或专业脱壳服务

#### 方案 E: 专业 Frida 绕过 + Native Hook（最终成功方案）✅

**尝试时间**: 2026-09-24

**方案流程**：
1. **Java 层脱壳**（专业人士）
   - 脱壳工具：专业脱壳服务
   - 获得完整的 Java 代码
   - 分析发现加密方式和密钥获取路径

2. **分析加密机制**
   - 发现 AES-128-ECB 加密用于手机号和验证码
   - 发现 HMAC-SHA256 签名算法
   - 确认密钥存储在 SO 库的 Native 层

3. **Frida 反调试绕过**（专业人士）
   - 使用 frida-server 16.1.9
   - 绕过脚本：`/Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd/frida_bypass_combined_local.js`
   - 成功绕过某梆的反调试检测
   - 实现稳定注入和 Hook

4. **Native 层密钥提取**
   - Hook 脚本：`/Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd/hook_native_hmac.js`
   - Hook 目标：
     - `SecBox.getAesKey()` - 获取 AES 密钥
     - `SecBox.getV4ak()` - 获取 v4 Access Key
     - `SecBox.getV4sk()` - 获取 v4 Secret Key
     - `HMAC_Init_ex`, `HMAC_Update`, `HMAC_Final` - 捕获签名过程
   - 输出日志：`/Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd/frida_native_hmac.log`

**提取的密钥**（从 frida_native_hmac.log）：
```
aesKey  : w8ZJ4wrUl7dDB1A7
signKey : c919e05a-bcbe-42b3-9bc0-935cd8c7b675
v4ak    : HJ7YLqOY06F61FPEhF7H
v4sk    : JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp
```

**签名消息格式验证**（从 Native Hook 捕获）：
```
GET
/bff/common/widget/UserWidget
stepNum=
HJ7YLqOY06F61FPEhF7H
Thu, 24 Sep 2026 06:15:03 GMT
ct:102
language:en
p:102
sid:3d7a7e29c7d6a173a73447c52134e38d_
sv:v4
token:56ee955b4cd34a7da08139e4ae9b0986
v:7.0.41.0
x-mcd-gw-v:1
```

**验证过程**（3 次迭代）：
1. **第 1 次**：提取密钥，实现基本签名算法
2. **第 2 次**：确认查询参数排序规则（字母序）
3. **第 3 次**：确认完整的签名消息构造格式

**完整验证**：
- ✅ 生成 Token 并激活（`GET /bff/common/proxy/tid`）
- ✅ 发送验证码（`POST /bff/passport/verifyCode/sms/send`）
- ✅ 登录接口（`POST /bff/passport/login/verifyCode`）
- ✅ 验证登录状态（`GET /bff/member/user/portal/info?scene=1`）

**当前使用的密钥**（存储在 `mcd_project/config.py` 和 `mcd_project/mcd_api.py`）：
- V4AK: `HJ7YLqOY06F61FPEhF7H`
- V4SK: `JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp`
- AES_KEY: `mcd20190909mcd20`

注：`mcd_project/config.py` 中保存了从 `frida_native_hmac.log` 中提取的所有密钥，包括 `aesKey`, `signKey` 等，虽然当前登录流程只使用了上述三个密钥，但保留其他密钥以备后续新接口可能需要使用。

---

## 三、项目文件结构

### 3.1 目录结构说明

项目文件已按类型分类整理，便于查找和维护：

```
mcd/
├── FINAL_HANDOVER.md              # 项目交接文档（根目录）
├── mcdonald_login.py              # 登录及购物车操作脚本（待验证）
│
├── resources/                     # 资源类文件
│   ├── apk/                       # APK 文件
│   │   ├── mcd_base.apk          # 麦当劳原始 APK (114MB)
│   │   └── frida-gadget-arm64.so # Frida Gadget SO (24MB)
│   │
│   ├── so/                        # SO 库文件
│   │   ├── libcsiipowerenter.so  # 主要加密 SO (5.8MB)
│   │   ├── libdexjni.so          # DEX JNI SO (2.5MB)
│   │   └── so_libs/              # 其他 SO 库集合
│   │
│   ├── logs/                      # 日志文件
│   │   ├── frida_native_hmac.log          # 密钥提取日志（包含所有密钥）
│   │   ├── frida_complete.log             # 完整 Frida 日志
│   │   ├── frida_secbox_keys.log          # SecBox 密钥日志
│   │   ├── captured_signs_20260914.json   # 签名捕获
│   │   ├── mcd_login_capture_*.log        # 登录捕获日志
│   │   ├── mcd_traffic.mitm               # 抓包流量 (200MB)
│   │   └── mitm.log                       # mitmproxy 日志
│   │
│   └── decorticate/               # 脱壳后的代码
│       └── mcd_decorticate/       # 专业脱壳结果
│
├── tools/                         # 工具类文件
│   ├── frida/                     # Frida 脚本
│   │   ├── frida_bypass_combined_local.js  # 反调试绕过（专业版）
│   │   ├── hook_native_hmac.js             # Native 层密钥提取
│   │   ├── hook_complete.js / hook_secbox_keys.js  # 早期 SecBox Hook
│   │   ├── hook_sm_deviceid.js             # hook 数美 getDeviceId 取 d
│   │   ├── hook_d_active.js                # 主动触发 + RPC 取 d
│   │   ├── hook_fake_device.js             # 伪造设备注入(android_id+native属性+token)+清缓存+RPC
│   │   ├── hook_collect_probe.js           # 探测数美采集的设备维度
│   │   ├── hook_crypto_probe.js            # 探测 vdata 加密边界(证明在 native)
│   │   ├── hook_token.js                   # 探测 token 来源
│   │   ├── fdrv.py                         # frida 注入驱动(attach/spawn；多设备按 FRIDA_DEV 选 Android)
│   │   └── getd.sh                         # 一键取 d / 跑任意 hook 脚本
│   │
│   └── python/                    # Python 工具
│       ├── login_manager.py                # 登录管理器（早期版本）
│       ├── mitmproxy_capture_login.py      # mitmproxy 捕获脚本
│       ├── rpc_getd.py                     # frida-RPC 真机现取 d
│       ├── decode_d.py                     # d 两层 base64 解码
│       ├── device_pool.py                  # 设备池（JSON 版 CLI）
│       └── fake_device_poc.py              # 伪造设备 PoC
│
└── docs/                          # 文档类文件
    ├── V4_V5_签名对比.md          # v4/v5 签名机制详细对比
    ├── TOKEN_AND_V5_ANALYSIS.md   # Token 和 V5 分析
    ├── TROUBLESHOOTING.md         # 故障排查文档
    ├── 工具安装步骤.md             # IDA-NO-MCP 工具安装指南
    └── 专业脱壳提交资料.md         # 脱壳服务商提交资料
```

### 3.2 文件分类说明

**资源类（resources/）**
- `apk/` - 原始 APK 和相关二进制文件
- `so/` - Native 层 SO 库文件
- `logs/` - 各类日志、抓包、捕获数据
- `decorticate/` - 脱壳后的代码

**工具类（tools/）**
- `frida/` - Frida Hook 脚本（3 次迭代）
- `python/` - Python 辅助工具

**文档类（docs/）**
- 技术分析文档
- 工具使用指南
- 故障排查手册

**生产代码（../mcd_project/）** — 完整结构见 `../mcd_project/README.md`
- 核心：`mcd_api` / `login_manager` / `mcd_server` / `config` / `exceptions` / `utils`
- 设备指纹池(v4.0)：`device_builder`(造假设备) / `device_pool_redis`(Redis 池) / `frida/`(注入 js)
- 滑块登录(v3.0)：`captcha_login_worker`(自动过滑块)
- 代理池(v2.0)：`proxy_pool` / `account_proxy_binding` / `proxy_failure_handler` / `proxy_manager` / `ip_location` / `scheduler`
- 脚本：`run`(直连三步) / `run_api_client`(经 server 完整流程) / `use_login_manager` / `demo`

### 3.3 开发工作流

**测试验证流程**：
1. 在 `mcd/` 根目录编写测试脚本（如 `test_xxx.py`）
2. 执行测试验证功能
3. 验证成功后：
   - 删除测试文件
   - 将成果代码归档到对应目录
   - 更新交接文档

**示例**：
```bash
# 1. 在根目录测试
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd
python test_cart_operation.py

# 2. 验证成功后归档
mv cart_operation.py tools/python/
rm test_cart_operation.py

# 3. 更新文档
```

---

## 四、可交付成果

### 3.1 核心文档

| 文档 | 说明 | 状态 |
|------|------|------|
| `FINAL_HANDOVER.md` | 本文档（项目总览） | ✅ 最新 |
| `V4_V5_签名对比.md` | v4/v5 签名机制详细对比 | ✅ 最新 |
| `工具安装步骤.md` | IDA-NO-MCP + reverse-skills 安装指南 | ✅ 最新 |
| `专业脱壳提交资料.md` | 提交给脱壳服务商的资料 | ✅ 已归档 |
| `frida_bypass_combined_local.js` | Frida 反调试绕过脚本（专业版） | ✅ 可用 |
| `hook_native_hmac.js` | Native 层密钥提取 Hook 脚本 | ✅ 可用 |
| `frida_native_hmac.log` | 密钥提取日志（包含所有密钥） | ✅ 可用 |

### 3.2 生产代码（mcd_project 目录）

| 文件 | 说明 | 状态 | 版本 |
|------|------|------|------|
| `mcd_api.py` | 核心 API 实现（签名、加密、所有业务接口，支持代理） | ✅ 已验证 | v2.0 |
| `config.py` | 配置文件（包含所有提取的密钥 + 代理配置） | ✅ 已验证 | v2.0 |
| `login_manager.py` | 登录管理器（多账号支持 + 自动代理） | ✅ 已验证 | v2.0 |
| `mcd_server.py` | HTTP API 服务器（新增8个代理管理接口） | ✅ 已验证 | v2.0 |
| `proxy_pool.py` | 代理池管理器 | ✅ 新增 | v2.0 |
| `account_proxy_binding.py` | 账号代理绑定管理器 | ✅ 新增 | v2.0 |
| `proxy_failure_handler.py` | 四级容错失败处理器 | ✅ 新增 | v2.0 |
| `proxy_manager.py` | 代理管理统一入口 | ✅ 新增 | v2.0 |
| `ip_location.py` | IP 地理位置查询 | ✅ 新增 | v2.0 |
| `scheduler.py` | 定时任务调度器 | ✅ 新增 | v2.0 |
| `utils.py` | 公共工具函数 | ✅ 新增 | v2.0 |
| `run.py` | 店铺查找和下单流程测试脚本 | ✅ 已实现 | v1.0 |
| `credentials.json` | 多账号凭证存储（Redis） | ✅ 运行中 | v1.0 |
| `README.md` | 项目使用文档 | ✅ 最新 | v2.0 |
| `CHANGELOG.md` | 更新日志 | ✅ 新增 | v2.0 |

**mcd_api.py 已实现的接口**：
- **登录相关**: Token 生成/激活、发送验证码、验证码登录、登录状态检查
- **城市店铺**: 获取所有城市、通过经纬度获取当前城市、搜索城市店铺、获取附近店铺
- **商品相关**: 获取店铺商品菜单、获取商品详情
- **购物车**: 清空购物车、加入购物车、从购物车删除
- **订单相关**: 订单验证信息、获取促销/优惠券信息、获取门店信息、支付渠道查询
- **待实现**: 提交订单（需要 v5 签名）、支付流程

**run.py 测试脚本**：
```bash
# 店铺查找流程
python run.py store
# 1. 选择"附近店铺"或"搜索店铺"
# 2. 附近店铺：通过默认经纬度获取店铺列表
# 3. 搜索店铺：输入搜索词 → 选择城市 → 搜索店铺
# 4. 选择店铺后写入文件

# 下单流程测试
python run.py order
# 1. 读取店铺信息
# 2. 获取店铺菜单并选择商品
# 3. 查看商品详情并确认加入购物车
# 4. 清空购物车后加入商品
# 5. 获取订单验证信息和促销信息
# 6. 确认门店信息
# 7. 提交订单（v5 签名暂不可用）
```

### 3.3 已归档的失败方案文件

以下文件已证明无效或被更好方案替代，保留仅供技术参考：
- `frida-gadget-script.js` - Gadget Hook 脚本（某梆检测，无法使用）
- `mcd_gadget.apk` (120MB) - 注入 Gadget 的 APK（签名检测失败）
- `mcd_xposed_module.apk` (4KB) - LSPosed 模块（检测时机太晚）
- `inject_gadget_manual.sh` - Gadget 注入脚本
- `frida-gadget-config.json` - Gadget 配置
- `xposed_module/` - LSPosed 模块源码
- `deploy.sh` - 部署脚本
- `mcdonald_api_auth.py` - 早期签名验证框架（已被 `mcd_project/mcd_api.py` 替代）
- `mcdonald_login.py` - 早期登录实现（已被 `mcd_project/login_manager.py` 替代）

**技术总结**: Frida Gadget + LSPosed 方案从技术层面不可行，某梆在 Native 层 JNI_OnLoad 阶段（早于 Java 层的 Application 生命周期）完成了完整性检测，任何修改 APK 的尝试都会被检测并主动崩溃。最终通过专业 Frida 绕过方案成功。

---

## 四、实施建议

### 4.1 如何使用当前成果

**直接使用生产代码**（推荐）：
```bash
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/mcd_project
python use_login_manager.py
```

功能特性：
- ✅ 自动检查登录状态
- ✅ 多账号凭证管理
- ✅ Token 自动激活
- ✅ 验证码发送和登录
- ✅ 凭证持久化存储

**使用已提取的密钥**（`mcd_project/config.py`）：
```python
# 当前登录流程使用的密钥
V4AK = "HJ7YLqOY06F61FPEhF7H"
V4SK = "JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp"
AES_KEY = "mcd20190909mcd20"

# 从 Frida Hook 提取的其他密钥（备用）
FRIDA_EXTRACTED_KEYS = {
    "aesKey": "w8ZJ4wrUl7dDB1A7",
    "signKey": "c919e05a-bcbe-42b3-9bc0-935cd8c7b675",
    "v4ak": "HJ7YLqOY06F61FPEhF7H",
    "v4sk": "JURCUMJRrQRI8gkB1mGrL9vexmkGgpLgxJ96Yovp"
}
```

### 4.2 如果需要重新提取密钥

**环境要求**：
- Android 设备（真机或模拟器）
- frida-server 16.1.9
- 专业反调试绕过脚本

**操作步骤**：
1. 启动 frida-server
2. 加载反调试绕过脚本：`frida_bypass_combined_local.js`
3. 注入 Hook 脚本：`hook_native_hmac.js`
4. 启动麦当劳 App 并触发登录流程
5. 查看输出日志获取所有密钥

### 4.3 如果遇到新的 API 接口

当前已实现的接口：
- `GET /bff/common/proxy/tid` - Token 激活
- `POST /bff/passport/verifyCode/sms/send` - 发送验证码
- `POST /bff/passport/login/verifyCode` - 验证码登录
- `GET /bff/member/user/portal/info?scene=1` - 用户信息（登录状态验证）

如需调用新接口：
1. 查看 `mcd_project/config.py` 中的备用密钥
2. 参考 `mcd_project/mcd_api.py` 中的 `build_headers()` 方法
3. 使用相同的签名算法构造请求
**成功率**: 95%

**提交资料**：参考 `专业脱壳提交资料.md`

**脱壳后操作** (使用 IDA-NO-MCP + reverse-skills):
1. 用 IDA Pro 打开脱壳后的 `libcsiipowerenter.so`
2. 按 Cmd-Shift-E 导出 AI 友好格式 (5-10 分钟)
3. 使用 `/rev-symbol --target "SecBox|aesKey|signKey"` 快速定位 (5-10 分钟)
4. 使用 `/rev-struct --address 0xEB6E4` 分析数据结构 (2-3 分钟)
5. 从反编译代码中提取 `aesKey` 和 `signKey` (2-3 分钟)
6. 填入 `mcdonald_api_auth.py` 并验证 (1-2 分钟)

**预计总时间**: 15-30 分钟 (详见 `工具安装步骤.md`)

### 方案 2: 看雪论坛 eCapture + Inline Hook 方案评估 ❌

**文章来源**: https://bbs.kanxue.com/thread-292885.htm

**测试结果**: 不适用

**技术要求**：
1. **eCapture**: 需要内核 5.5+，实测设备内核 3.18.137（2019年）不支持
2. **自定义 SO 注入**: 需要定制 ROM 支持"任意 SO 加载"功能
3. **系统类型**: 文章使用 Android 15 + 内核 5.10.209 + 定制 ROM

**实测设备环境**：
```
内核版本: Linux 3.18.137-g382d7256ce44 (2019)
Android 版本: 10
构建类型: user (原厂 ROM)
eCapture 错误: Kernel version 3.18.137 is not supported. Requires >= 5.5
```

**结论**:
- eCapture 内核版本不满足（需升级设备或刷机）
- 自定义 SO 注入需要定制 ROM（原厂 ROM 无此功能）
- 即使满足条件，仍需逆向定位麦当劳 App 的 JNI 业务入口点
- **成本远高于专业脱壳服务，不推荐**

### 方案 3: 等待脱壳结果

**当前状态**：
- ✅ 已找专业团队脱壳中（用户已委托）
- ✅ Frida Gadget 方案已尝试，证明无法绕过
- ✅ eCapture 方案已测试，设备不支持
- ✅ 所有免费/自助方案已穷尽

**建议**：
- 停止所有自助绕过尝试（Frida、eCapture、inline hook 等）
- 等待脱壳结果（预计 3-5 天）
- 脱壳后密钥提取工作量：**1 小时**

**已验证失败的方案**：
1. ❌ Frida spawn/attach 模式 - 被某梆检测并崩溃
2. ❌ Frida Gadget + LSPosed - APK 签名检测触发崩溃
3. ❌ unidbg 模拟执行 - VMP 加密代码报 "Invalid instruction"
4. ❌ IDA Pro 静态分析 - 代码被加密为 DCQ 数据
5. ❌ eCapture eBPF 工具 - 设备内核版本过低（3.18 vs 5.5+）
6. ❌ 自定义 SO 注入 - 需要定制 ROM（原厂 ROM 不支持）

---

## 五、关键技术发现

### 5.1 v4/v5 双重签名架构详解

**v4 签名（麦当劳自己的 - 推荐使用）**:

```kotlin
// 实现位置：qf.e.a(Request) - MCDSignatureInterceptor

// 1. 从 SecBox 获取密钥
String v4ak = SecBox.INSTANCE.getV4ak();  // Access Key
String v4sk = SecBox.INSTANCE.getV4sk();  // Secret Key

// 2. 构建签名字符串
String authString = String.format(
    "%s\n%s\n%s\n%s",
    request.method(),           // GET/POST
    request.url().encodedPath(), // /bff/order/submit
    canonicalHeaders,           // ct;language;p;sid;sv;...
    date                        // RFC1123 格式
);

// 3. HMAC-SHA256 签名
byte[] signature = hmacSha256(v4sk, authString);
byte[] bodySignature = hmacSha256(v4sk, bodyBytes);

// 4. 设置 Headers
request.addHeader("authorization",
    "hmac-auth-v1#" + v4ak + "#" + base64(signature) + "#hmac-sha256#...");
request.addHeader("X-HMAC-DIGEST", base64(bodySignature));
request.addHeader("sv", "v4");
```

**状态**：
- ✅ 算法完全确认（标准 HMAC-SHA256）
- ✅ 实现代码已完成（`mcd_project/mcd_api.py`）
- ✅ 密钥已成功提取并验证
- ✅ 完整登录流程已实现并测试通过
- ✅ **可完全独立模拟，不依赖真实设备**

**v5 签名（数美科技 TDRisk SDK - 不建议逆向）**:

```kotlin
// 实现位置：qf.e.intercept(Chain) - MCDSignatureInterceptor

// 1. 判断是否使用 v5（灰度算法）
if (DegradeManager.INSTANCE.shouldUseTdSign(request.method(), apiPath)) {

    // 2. 调用 TDRisk SDK
    TDAPISignResult result = TDRisk.sign(AppConfigLib.context, apiPath);

    if (result.code() == 0) {
        // 3. 获取签名（用户说的 "D 参数"）
        String signature = result.signature();

        // 4. 使用 v5 签名
        request.addHeader("x-mcd-sign", signature);
        request.addHeader("sv", "v5");

        // 5. 移除 v4 Headers
        request.removeHeader("Authorization");
        request.removeHeader("X-HMAC-DIGEST");
    } else {
        // TDRisk 失败，降级到 v4
        return v4Sign(request);
    }
}
```

**TDRisk 签名生成流程**：
```
1. 采集设备指纹（100+ 维度）
   ├─ 硬件：IMEI、MAC、AndroidID、序列号
   ├─ 传感器：加速度计、陀螺仪、磁力计
   ├─ 系统：屏幕参数、安装列表、文件列表
   └─ 行为：点击模式、滑动轨迹、输入速度

2. 生成 BlackBox（加密的设备特征包）
   TDDeviceInfo info = TDRisk.getDeviceInfo();
   String blackBox = info.getBlackBox();

3. 构建签名数据（API 路径 + BlackBox + 时间戳）

4. 用数美服务端公钥加密

5. Base64 编码输出 "Bwm8C/M8Y30Xv8wK..."
```

**渐进式部署**：
- 服务端配置：`https://img.mcd.cn/app/main/assets/requestConfig102.json`
- 灰度算法：基于设备 key (0-10000) 和 ratio (0-100%)
- 同一设备选择稳定（不会来回切换）
- 失败自动降级到 v4

**状态**：
- ✅ 机制完全分析清楚
- ✅ Java 层入口代码可见（`TDRisk.sign()`）
- ❌ 核心逻辑在 Native 层（libmobrisk.so / libtrustdecision.so）
- ❌ **依赖数美服务端密钥，无法独立模拟**
- ❌ **模拟设备会被风控检测**

**v5 不建议逆向的四大原因**：
1. **SO 文件独立**：TDRisk 有自己的 SO，麦当劳脱壳服务不包含
2. **仍有保护**：商业 SDK 必然有自己的保护（OLLVM、VMP、反调试）
3. **依赖服务端密钥**：客户端只有公钥，私钥在数美服务器上
4. **设备指纹必须真实**：模拟设备会被检测为高风险

**结论**：
- ✅ 使用 v4 签名即可（所有接口都支持）
- ❌ v5 逆向成本极高且无法独立使用
- ✅ 正常使用频率不会触发风控

### 5.2 某梆 VMP 保护机制

**代码加密**：
```
正常函数：
0x1000: STP X29, X30, [SP, #-0x20]!
0x1004: MOV X29, SP
...

加密函数（梆梆 VMP）：
0xEB6E4: CBZ W15, 0xEB6E0          ← 入口指令
0xEB6E8: DCQ 0x562818695C064024    ← 加密数据
0xEB6F0: DCQ 0xB501BF0C824EFE36    ← 加密数据
```

- 机器码被加密为随机数据
- 运行时由 VM 解释器解密执行
- 静态分析工具无能为力

### 5.3 反调试检测机制

**检测方法**（推测）：
1. 扫描 `/proc/self/maps` 查找 "frida"
2. 检测 frida-server 进程
3. 检测 frida-agent 路径 `/data/local/tmp/re.frida.server/`
4. 扫描内存查找 "frida" 字符串
5. 检测 LSPosed 框架特征
6. APK 签名完整性校验

**绕过难度**：极高
- 需要修改 Frida 源码
- 需要 Magisk Hide + 系统级伪装
- 成功率 < 30%

### 5.4 密钥管理架构

```
Java 层：
SecBox.init()
  → JniLib0.cV(SecBox.class, this, 1)

Native 层（v4 密钥）：
libcsiipowerenter.so 或 libandjni.so
  → 从加密数据段获取密钥
  → 赋值给 SecBox.aesKey / v4ak / v4sk

Native 层（v5 签名）：
libmobrisk.so / libtrustdecision.so (TDRisk SDK)
  → 采集设备指纹
  → 生成 BlackBox
  → 调用数美服务端 API 生成签名
```

---

## 六、文件清单

### 6.1 保留文件（核心）

```
FINAL_HANDOVER.md                    # 本文档
工具安装步骤.md                       # IDA-NO-MCP + reverse-skills 安装指南
专业脱壳提交资料.md                   # 脱壳服务提交资料
frida-gadget-script.js               # SecBox Hook 脚本
mcdonald_api_auth.py                 # 签名验证框架
mcd_base.apk                         # 原始 APK (114MB)
```

### 6.2 Gadget 方案文件（已失败）

```
mcd_gadget.apk (120MB)               # 注入 Gadget 的 APK
mcd_xposed_module.apk (4KB)          # LSPosed 模块
inject_gadget_manual.sh              # 注入脚本
frida-gadget-config.json             # Gadget 配置
xposed_module/                       # 模块源码
deploy.sh                            # 部署脚本
```

### 6.3 可删除文档（过时）

以下文档内容已整合到本文档或独立文档：
- `TOOL_INSTALLATION_GUIDE.md` - 已精简为 `工具安装步骤.md`
- `INSTALLATION_STATUS.md` - 工具安装状态（已完成，可删除）
- `BUILD_GUIDE.md` - LSPosed 模块构建指南（已失败）
- `DEPLOYMENT_GUIDE.md` - Gadget 部署指南（已失败）
- `GADGET_USAGE.md` - Gadget 使用文档（已失败）
- `XPOSED_MODULE_GUIDE.md` - LSPosed 模块文档（已失败）
- `PROJECT_SUMMARY.md` - 项目总结（内容重复）
- `FRIDA_SOLUTION.md` - Frida 方案（已证明失败）
- `FRIDA_EMULATOR_GUIDE.md` - 模拟器方案（不适用真机）
- `UNIDBG_GUIDE.md` - unidbg 方案（已失败）

保留但不需要关注：
- `HANDOVER_DOCUMENT.md` - 旧版交接文档（50KB）
- `ENCRYPTION_CONFIRMED.md` - 加密分析（26KB）
- `FINAL_SUMMARY.md` - 旧总结

---

## 七、接手指南

### 7.1 当前状态

**已完成**：
- ✅ 完整的加密和签名机制分析（v4 + v5）
- ✅ 所有 Java 层代码逆向完成
- ✅ Native 层关键函数定位完成
- ✅ v4 签名验证框架实现（只缺密钥）
- ✅ v5 签名机制完全分析（不建议逆向）
- ✅ Token 激活接口实现
- ✅ IDA-NO-MCP + reverse-skills 工具集成

**进行中**：
- 🔄 等待麦当劳 SO 脱壳服务结果（3-5 天）

**待完成**：
- ⏳ 从脱壳后的 SO 中提取 v4 密钥（15-30 分钟工作量）

**明确不做**：
- ❌ 逆向 v5 签名（TDRisk SDK）- 依赖服务端，无法独立使用

### 7.2 脱壳后操作步骤

```bash
# 1. 收到脱壳后的 APK
unzip mcd_unpacked.apk -d mcd_unpacked

# 2. 提取 SO
cp mcd_unpacked/lib/arm64-v8a/libcsiipowerenter.so ~/Desktop/

# 3. IDA Pro 导出 AI 友好格式
open -a "IDA Professional 9.1" ~/Desktop/libcsiipowerenter.so
# 等待自动分析完成 (5-10 分钟)
# 按 Cmd-Shift-E → 选择输出目录 → Export

# 4. AI 辅助分析
cd ~/Desktop/libcsiipowerenter_export_for_ai/
/rev-symbol --target "SecBox|v4ak|v4sk|aesKey|HMAC|JniLib0"
/rev-struct --address 0xEB6E4

# 5. 提取密钥
# 从 decompile/ 目录的反编译代码中查找:
# SecBox.aesKey = "...";
# SecBox.v4ak = "...";
# SecBox.v4sk = "...";

# 6. 验证密钥
cd ~/Desktop/ai_code/mcp_js/claude_code/mcd
vim mcdonald_api_auth.py  # 填入密钥
python3 mcdonald_api_auth.py
```

**详细流程参考**: `工具安装步骤.md`

### 7.3 密钥特征

**v4 签名密钥**：
- **v4ak** (Access Key)：可能是 16-32 字节字符串
- **v4sk** (Secret Key)：可能是 32 字节字符串或 Base64
- **位置**：`JniLib0.cV()` 调用后赋值给 SecBox
- **验证方法**：用已捕获的签名样本验证

**AES 加密密钥**：
- **aesKey**：可能是 16 或 32 字节
- **格式**：字符串（可能是 Base64 或十六进制）
- **位置**：`JniLib0.cV()` 调用后赋值给 SecBox
- **验证方法**：用 32 个已捕获的签名样本验证

---

## 八、总结与建议

### 8.1 项目完成度

**总体**: 85%

- **分析部分**: 100% (所有算法和机制已完全确认)
- **密钥提取**: 0% (被加固阻挡)
- **代码实现**: 95% (只缺密钥值)

### 8.2 核心结论

1. **v4/v5 双重签名架构已完全分析**
   - v4 (HMAC-SHA256): 麦当劳自己的，可完全模拟
   - v5 (TDRisk SDK): 数美科技商业方案，依赖服务端

2. **推荐使用 v4 签名**
   - 所有接口都支持 v4
   - 只需提取 v4ak/v4sk 两个密钥
   - 不依赖真实设备，纯代码实现
   - v5 逆向成本极高且无法独立使用

3. **Frida Gadget + LSPosed 方案无法绕过某梆检测**
   - App 在 Application.attach() 之前就崩溃
   - LSPosed 框架本身被检测
   - APK 签名修改被检测

4. **成功方案：专业 Frida 绕过 + Native Hook**
   - 专业 Java 层脱壳
   - frida-server 16.1.9 + 反调试绕过脚本
   - Native 层 HMAC Hook 提取密钥
   - 3 次迭代验证完整流程

5. **项目成果**
   - ✅ 所有密钥已提取并验证
   - ✅ 完整登录流程已实现
   - ✅ 多账号管理系统已部署
   - ✅ 生产代码可直接使用

### 8.3 本机已配置环境

**核心逆向工具**:

1. **IDA Pro 9.1 + IDA-NO-MCP 插件**
   - 路径：`/Applications/IDA Professional 9.1.app`
   - 插件：`~/.idapro/plugins/INP.py` (已配置)
   - 用途：静态分析 SO 文件，导出 AI 友好格式
   - 快捷键：Cmd-Shift-E 导出反编译代码
   - 输出：AGENTS.md, decompile/, functions.json, strings.json

2. **JADX 1.5.6 + MCP Server**
   - GUI：`/Users/houjie/Desktop/software/jadx-1.5.6/bin/jadx-gui`
   - CLI：`/Users/houjie/Desktop/software/jadx-1.5.6/bin/jadx`
   - MCP Server：`/Users/houjie/Desktop/ai_code/mcp_js/jadx-mcp-server/`
   - 用途：DEX 反编译、APK 分析、跨引用查找
   - 状态：MCP 服务已运行 (PID 81230)

3. **抓包工具 - mitmproxy 7.0.4**
   - 路径：`/Users/houjie/venv/python3-forcrawl/bin/mitmdump`
   - Python：3.9.17
   - OpenSSL：1.1.1l
   - 用途：HTTP/HTTPS 流量拦截分析

**AI 辅助工具** (全局可用):

4. **reverse-skills** (全局 Skills)
   - 安装位置：`~/.agents/skills/`
   - `/rev-symbol` - 符号分析，快速定位关键函数
   - `/rev-struct` - 数据结构重建
   - `/rev-frida` - Frida Hook 脚本生成
   - `/rev-idapython` - IDAPython 参考文档
   - `/rev-dex-dumper` - Android DEX dump
   - `/rev-unicorn-debug` - Unicorn 模拟执行
   - `/rev-u3d-dump` - Unity IL2CPP 分析
   - `/rev-ios-dump` - iOS 砸壳

**运行中的 MCP 服务**:

- IDA Pro MCP (端口：stdio) - IDA 数据库查询
- JADX MCP (PID 81230) - APK/DEX 反编译
- Chrome DevTools MCP (端口 9222) - 浏览器调试
- JSReverser MCP - JS 逆向工具
- jshookmcp - Hook 管理

**工具文档**: `工具安装步骤.md`

### 8.4 给接手人员的建议

**DO**：
✅ 使用 `mcd_project/` 目录下的生产代码
✅ 参考 `mcd_project/mcd_api.py` 实现新接口
✅ 查看 `mcd_project/config.py` 获取所有密钥
✅ 使用 `test_login_manager.py` 测试登录流程
✅ 阅读 `V4_V5_签名对比.md` 理解双重签名架构

**DON'T**：
❌ 不要逆向 v5 签名（依赖服务端，无法独立使用）
❌ 不要使用已归档的旧代码（mcdonald_login.py 等）
❌ 不要修改 APK 重新打包（触发完整性检测）

---

## 九、联系与支持

**项目状态**: ✅ 已完成

**完成时间**: 2026-09-24

**关键文件**:
- `FINAL_HANDOVER.md` - 本文档（项目总览）
- `V4_V5_签名对比.md` - v4/v5 双重签名详细分析
- `工具安装步骤.md` - IDA-NO-MCP + reverse-skills 使用指南
- `frida_bypass_combined_local.js` - Frida 反调试绕过脚本（专业版）
- `hook_native_hmac.js` - Native 层密钥提取 Hook 脚本
- `frida_native_hmac.log` - 密钥提取日志（包含所有密钥）

**生产代码**（mcd_project 目录）:
- `mcd_api.py` - 核心 API 实现（签名、加密）
- `config.py` - 配置文件（包含所有提取的密钥）
- `login_manager.py` - 登录管理器（多账号支持）
- `test_login_manager.py` - 登录流程测试脚本
- `credentials.json` - 多账号凭证存储
- `交接文档.md` - 项目交接文档

**所有功能已可用！v4 签名完全实现，v5 签名不需要逆向。**

---

**文档版本**: v5.0 Final
**最后更新**: 2026-09-24
**作者**: Claude Code + 用户协作

**核心发现**：
- ✅ v4 签名（HMAC-SHA256）已完全实现，密钥已提取并验证
- ✅ v5 签名（TDRisk SDK）已完全分析，但依赖服务端无法独立使用
- ✅ 推荐使用 v4 签名，所有接口都支持
- ✅ 项目已完成，生产代码可直接使用


---

## 十、v2.0 代理池管理系统（2026-10-10 新增）

### 10.1 系统概述

v2.0 新增了完整的**代理池管理系统**，实现了账号-代理绑定、智能故障切换和健康监控功能。

**核心特性：**
- ✅ 代理池管理（增删查改、状态管理、健康检查）
- ✅ 账号-代理长期绑定（Redis 持久化）
- ✅ 四级智能容错（自动重试、临时切换、观察期、永久切换）
- ✅ 城市优先分配（自动查询代理 IP 城市）
- ✅ 定时任务调度（全量检查、观察期检查、失败清理）
- ✅ HTTP API 接口（8个代理管理接口）

### 10.2 新增文件（8个）

| 文件 | 说明 | 行数 |
|------|------|------|
| `proxy_pool.py` | 代理池管理器 | 791 |
| `account_proxy_binding.py` | 账号代理绑定管理器 | 533 |
| `proxy_failure_handler.py` | 四级容错失败处理器 | 278 |
| `proxy_manager.py` | 代理管理统一入口 | 394 |
| `ip_location.py` | IP 地理位置查询 | 119 |
| `scheduler.py` | 定时任务调度器 | 125 |
| `utils.py` | 公共工具函数 | 18 |
| `requirements.txt` | 依赖包列表 | 7 |

### 10.3 修改文件（6个）

| 文件 | 主要改动 | 新增行数 |
|------|----------|----------|
| `config.py` | 新增代理相关配置 | +71 |
| `login_manager.py` | 集成代理池、自动容错 | +167 |
| `mcd_api.py` | 所有API支持代理参数 | +47 |
| `mcd_server.py` | 新增8个代理管理接口 | +213 |
| `demo.py` | 代理池初始化 | 重构 |
| `run_api_client.py` | 小改 | -1 |

### 10.4 四级容错机制

```
阶段1: 首次失败 → 立即重试（同代理）
阶段2: 连续失败3次 → 临时切换 + 观察期（5分钟）
阶段2.5: 观察期内连续失败10次 → 立即永久切换
阶段3: 观察期结束 → 健康检查决定（恢复/永久切换）
```

### 10.5 HTTP API 接口（8个）

| 接口路径 | 方法 | 功能 |
|---------|------|------|
| `/api/proxy/pool/status` | GET | 查看代理池状态 |
| `/api/proxy/pool/list` | GET | 获取所有代理列表 |
| `/api/proxy/pool/add` | POST | 添加代理到代理池 |
| `/api/proxy/pool/remove` | POST | 移除/禁用代理 |
| `/api/proxy/account/binding` | GET | 查看账号代理绑定关系 |
| `/api/proxy/account/switch` | POST | 手动切换账号代理 |
| `/api/proxy/statistics` | GET | 获取代理池统计数据 |
| `/api/proxy/health/check` | POST | 手动触发健康检查 |

### 10.6 快速开始

**1. 添加代理到代理池**

```python
from proxy_pool import ProxyPool

pool = ProxyPool()

# 添加代理（自动查询城市）
proxy = pool.add_proxy(
    host='61.155.141.105',
    port=20080,
    username='user',
    password='pass',
    protocol='http'
)
```

**2. 使用代理登录（自动）**

```python
from login_manager import LoginManager

# 创建登录管理器（自动分配代理）
manager = LoginManager(
    phone="13800138000",
    use_redis=True,
    city="上海"  # 可选：优先使用上海的代理
)

# 登录（自动使用绑定的代理）
token, sid, meddy_id = manager.ensure_login(auto_relogin=True)

# 后续所有请求自动使用该账号绑定的代理
# 失败自动触发四级容错机制
```

**3. 启动 HTTP API 服务器**

```bash
python mcd_server.py
# 或使用 gunicorn
gunicorn -w 4 -b 0.0.0.0:5001 mcd_server:app
```

**4. 启动定时任务调度器**

```bash
# 前台运行
python scheduler.py

# 后台运行
nohup python scheduler.py > logs/scheduler.log 2>&1 &
```

### 10.7 配置说明

所有配置项在 `config.py` 中可调整：

```python
# 请求超时时间（秒）
PROXY_REQUEST_TIMEOUT = 15

# 代理失败容错配置
PROXY_FAILURE_CONFIG = {
    'observation_threshold': 3,           # 加入观察期的失败次数阈值
    'permanent_switch_threshold': 10,     # 观察期内永久换绑的失败次数阈值
    'observation_period': 5 * 60,         # 观察期时长（秒）
    'failure_window': 10 * 60,            # 失败计数时间窗口（秒）
}

# 代理池配置
PROXY_POOL_CONFIG = {
    'slow_threshold': 3.0,                # 响应时间阈值（秒）
    'max_fail_count': 5,                  # 连续失败多少次标记为failed
    'max_accounts_per_proxy': 5,          # 单个代理最大账号负载
}
```

### 10.8 详细文档

完整的代理池管理系统文档请参阅：
- **[代理池管理系统详解](PROXY_POOL_SYSTEM.md)** (新增文档)
- **[更新日志](../mcd_project/CHANGELOG.md)** (新增文档)

内容包括：
- 系统架构图
- 核心模块详解
- 四级容错流程图
- HTTP API 接口详解
- 配置说明
- 监控运维
- 故障排查
- 性能优化建议

### 10.9 技术亮点

1. **全局单例模式** - 避免 ProxyPool/AccountProxyBinding/ProxyFailureHandler 重复初始化
2. **Redis 持久化** - 所有代理信息、绑定关系、失败计数均存储在 Redis
3. **无侵入集成** - LoginManager 自动处理代理分配和故障切换，业务代码无需修改
4. **城市感知** - 自动查询代理 IP 城市，支持账号按城市优先分配代理
5. **健康检查缓存** - 30秒 TTL，避免短时间重复检查同一代理
6. **批量处理** - 健康检查支持分批（每批10个，间隔6秒）避免瞬间大量请求
7. **日志增强** - 统一使用 `utils.log_info()`，支持 `flush=True` 确保 crontab 实时输出

### 10.10 破坏性变更

#### LoginManager 构造函数签名变化

**旧版本：**
```python
LoginManager(phone, credentials_file=None, use_redis=False)
```

**新版本：**
```python
LoginManager(
    phone,
    credentials_file=None,
    use_redis=True,  # 默认值改为 True
    city=None,       # 新增：城市优先
    proxy_pool=None, # 新增：代理池实例
    binding_manager=None,  # 新增：绑定管理器实例
    failure_handler=None   # 新增：失败处理器实例
)
```

#### mcd_api.py 所有 API 函数签名变化

所有 API 函数新增最后一个参数：`proxies=None`

**示例：**
```python
# 旧版本
send_verification_code(phone, token)

# 新版本
send_verification_code(phone, token, proxies=None)
```

### 10.11 依赖更新

新增依赖：
```
schedule==1.2.0
```

安装命令：
```bash
pip install -r requirements.txt
```

---

**v2.0 更新时间**: 2026-10-10  
**v2.0 核心贡献者**: 侯杰 + Claude Code

---

## 十一、设备指纹 d 与发码/登录风控（2026-10-16 新增）

> 本节详细过程见 `mcd/docs/滑块验证码逆向进度.md` 第九/十节。这里只给结论与铁律。

### 11.0 最重要的结论（2026-10-16 全链路实证）

**干净住宅/本机 IP + 干净账号 → 根本不需要 d 值**：
- 发码直接返回 `420`（出数美滑块），不会 `707053`；
- DrissionPage 自动滑块一次过 → 收到短信 → 验证码登录成功；
- 之后**完整跑通了选店 → 点餐 → 提交订单 → 获取支付数据的全流程**。

→ **自用单号的最省事路线：用干净 IP + 干净号，完全不碰 d。** d 只是脏 IP / 被标记号时的"补救信号"，不是必需品，且滥用会升级风控（见 11.4）。

### 11.1 d 是什么

`d` 请求头 = **数美（SHUMEI）设备指纹**：
- 设备指纹就绪时 `d = "B" + smid`（smid 来自 native `fp-it.fengkongcloud.com/deviceprofile/v4`）；
- 未就绪时 `d = "D" + base64(deviceprofile 请求负载)`。
- 由 native `libsmcp.so`（`com.ishumei.smantifraud` → SMSDK）生成，Java 层是混淆壳。
- 注入点：`qf.a`(BFFHeaderInterceptor) 每个请求 `addHeader("d", SmSDK.getSmDeviceId(path))` → `com.mcd.library.utils.SmSDK.getSmDeviceId` → `SmAntiFraud.getDeviceId()`。
- 初始化：`SmSDK.initShumei(organization=qvdTr7A9k1DIU04ha4eP, appId=default, RSA公钥=MIIDLz…)`。
- **取 d 最省事 = frida hook `SmAntiFraud.getDeviceId()`**（纯 python 复现需逆 deviceprofile 的 data/tn/ep 加密，重活，不划算）。
- `token`(mcdtoken) 由 `AppInfoUtil.getToken` 生成（IMEI/androidId/随机 UUID.hex），**与 d 无关、互相独立**。
- **重启 APP → 重跑 deviceprofile → 新 smid/新 d，但 token 不变**（实证）。这正好解释"707053 → 重启 APP → 420"：旧 d 被标记，重启换了新 d 绕过。

### 11.2 风控码阶梯（实测）

| code | 含义 | 处理 |
|------|------|------|
| **420** | 正常出滑块（期望结果） | 滑过即可继续 |
| **707053** | 账号存在安全风险 | 脏 IP / 无 d / 风险号 → **换干净 IP + 合法 d + 好号** |
| **706070** | 当前账号设备环境异常 | d 滥用升级（见 11.4）→ **停手冷却 + 换独立环境**，代码层解决不了 |
| **801005** | 发码次数超限 | 单号降频 |

- **707053 的决定性因素是出口 IP 质量 + d**：同号 13520093034，不带 d → 707053；带上抓包的真实 d（+对应 token+tid）→ 420。数据中心代理易 707053，住宅/本机 IP 出 420。

### 11.3 x-mcd-sec-verify 拼法 + 跨设备配对

- `x-mcd-sec-verify = base64(NO_WRAP)` of `"{punishId};{verifySdk};{毫秒时间戳};{rid}"`。
  - 中间字段是 **verifySdk 不是 verifyType**（jadx `oe.x` / CaptchaDialog.kt 确认）；verifyType 只决定滑块形态（1=滑动）。
- **跨设备可配对**：mcd 返回的 `punishId` 与浏览器人工/自动滑出的数美 PASS `rid`，即使不同设备/会话，服务端不交叉校验设备，拼接重发即 success。这是"浏览器滑块 + requests 发码"两条链能拼起来的根本原因。

### 11.4 ⚠️ 铁律：一个 d 只配一个号（706070 血的教训）

测试中反复**用同一个借来的 d 跨多账号 + 高频发码**，把风控从 707053 **升级成 706070（设备环境异常）**，连真机 APP（重启 / 卸载重装 / 直接登录）都被拦。
- 根因：风控抓"一个设备指纹 ↔ 多账号 + 高频"的模式。
- 706070 挂在服务端账号/设备上，**重装重启清不掉**，只能停手冷却（数小时~数天衰减）+ 换真正独立的新号/环境。
- **铁律：一个 d 只配一个号、按真人频率用。多账号共用一个 d 必被 706070。**
- 规模化多 d 轮换绕设备风控 = **明确不做**（反欺诈规避，越账号数量越是）。

### 11.5 自动滑块方案（已生产化）

- **DrissionPage**（真实 Chromium + CDP `Actions` 动作链 → `isTrusted:true` 事件过行为检测）+ `get_tracks` 物理轨迹 + `ddddocr.slide_match` 识别缺口。
- 必须走**住宅/干净 IP**（数美滑块与 mcd 发码走同一出口 IP）；headless 实测可行。
- Chromium `--proxy-server` 不支持账密 → 用**动态 MV2 扩展**（`chrome.proxy` + `onAuthRequired` 回填账密），配置开关 `PROXY_BROWSER_AUTH`（True=扩展带账密 / False=IP 白名单）。
- 代码：`mcd_project/captcha_login_worker.py`（`get_pass_rid` / `_slide_once`），Redis 队列异步化 + 心跳/僵尸冻结清理。独立验证脚本 `mcd/test_drission_shumei_slide.py`、`mcd/test_plan_a_auto.py`，venv `~/venv/python3-forcrawl`。
- **服务器上偶发"滑动 risk=None 抓不到 fverify"**：已在 worker 加失败截图 + HTML + 诊断日志（存 `mcd_project/captcha_debug/`），定位时看截图里滑块是否真渲染出来。

### 11.6 密钥订正提示

本文前面 1.3 / 4.1 等处 `AES_KEY` 有两处写法（`mcd20190909mcd20` 与 frida 日志里的 `w8ZJ4wrUl7dDB1A7`）。**生产实际使用并验证通过的是 `w8ZJ4wrUl7dDB1A7`**（见 `mcd_project/config.py` / `mcd_api.py`），以生产代码为准。

---

## 十二、frida 动态调试取 d（2026-10-17 真机跑通）

> 解决了"大佬脚本在本机手机一直用不了"的问题。相关文件在 `mcd/tools/frida/`。

### 12.1 结论：问题是 frida 版本，不是脚本

- **frida / hluda / florida 16.1.9 在本环境注入必崩**：`bootstrapper crashed with signal 11 at offset 0x1514`（真机 attach、模拟器 spawn/attach、连干净系统 app 都崩，与目标 app、绕过脚本、SELinux 都无关）。
- 真机上 16.1.9 若用 **`-f`(spawn)** 则卡成 `TimedOutError: 等待 app 启动超时`（梆梆 spawn 门控卡死）。
- **✅ 可用组合 = frida 15.1.17 + attach（不是 spawn）**。实测 Pixel1(sailfish)/Android10/arm64/Magisk：注入成功 → 绕过脚本一次性 `patched=85/85` → hook 到真实 d。
- 手机端用**标准 `frida-server-15.1.17-android-arm64`** 即可（hluda 不是必须；16.1.9 那几个都崩）。客户端用 venv `~/venv/python3-forcrawl` 的 frida 15.1.17。

### 12.2 你那条原命令为什么用不了

```bash
frida -U -f com.mcdonalds.gma.cn -l frida_bypass_combined_local.js -l hook_native_hmac.js
```
两个坑：① 默认 `frida` 是 16.1.9 → 引导器崩；② `-f`(spawn) → 梆梆 超时。
**改法**：`frida` 换成 venv 的 15.1.17，`-f 包名` 换成 `-F`(attach 前台) 或按 pid attach。

### 12.3 固定取 d 流程（已验证）

一键脚本 `mcd/tools/frida/getd.sh <hook.js> [wait秒]`：自动「检测 server(在跑就跳过)→启动 app→抢 pid→attach 注入 绕过+你的hook→被反frida/残留异常崩了自动重试(最多5次)」。
```bash
cd mcd/tools/frida
./getd.sh hook_sm_deviceid.js        # 抓 d（数美设备指纹）
./getd.sh hook_native_hmac.js 90     # 抓 SO 层 HMAC/AES 密钥
```
前提：**手机已解锁 + 已联网**，否则 app 不发请求、hook 不触发。d 在 BFF 请求(如 `/bff/common/normal/ab/experiments`、`/bff/member/device/collect`)时经 BFFHeaderInterceptor→`SmSDK.getSmDeviceId` 产生；早期 `/bff/common/widget/UserWidget` 可能返回 null(smid 未就绪)。同一会话 d 会刷新变化。

手动等价命令：
```bash
# 手机(root)：起 server（已在跑则跳过）
adb shell "su -c 'setsid /data/local/tmp/frida-server-15.1.17-android-arm64 >/dev/null 2>&1 &'"
# 启动 app 后，用 15.1.17 attach
~/venv/python3-forcrawl/bin/frida -U -F -l frida_bypass_combined_local.js -l hook_sm_deviceid.js
```

### 12.4 关键文件（mcd/tools/frida/）

- `frida_bypass_combined_local.js` — 反调试绕过（针对 libmsaoaidsec + DexHelper payload，本 apk patched 85/85 兼容）
- `hook_sm_deviceid.js` — hook `SmAntiFraud.getDeviceId` / `SmSDK.getSmDeviceId` 打印 d
- `hook_native_hmac.js` — hook SO 层提取 HMAC/AES 密钥（大佬原用）
- `getd.sh` — 一键取 d 脚本；`fdrv.py` — spawn/attach 注入驱动（attach 中途 detach 会退出触发重试）

---

## 十三、设备指纹池系统：伪造设备批量生成 d（2026-10-17 新增）

### 13.0 一句话

**不改机、单进程批量伪造"设备身份"**：frida 在采集源注入假设备参数（android_id + native 系统属性 + token），让 app 自己的 native 去加密 vdata 并向数美注册，拿回**独立的真 smid**（d=`B`+smid）。每个设备 = 一套固定 `token` + 独立 `d` + `tid`，入 Redis 设备池，登录接口按账号领用。解决"同设备绑多账号被风控"的问题（一账号一设备）。

### 13.1 加密边界（为什么走采集源注入，而非纯代码复刻）

- `getDeviceId()` → `B`+smid（服务端注册后下发）或 `D`+base64(vdata 盒子)（未注册/异常）。
- 正常 vdata 盒子 = `{organization,os,appId,encode:2,compress:3,data,tn,ep}`，其中 **data/tn/ep 全部在 native(libsmsdk) 内加密**（实测 hook Java 的 `javax.crypto`/`java.util.zip`/`Base64` 全不命中 → 加密不在 Java 层）。
- 数美采集方法签名是 `(int,boolean)`，**内部自采集设备数据、不接收外部传参** → 纯 py 复刻/传参加密都不可行。
- **唯一可行解 = 在采集源 hook 假数据，让 app 自己的 native 加密**（路线A，已跑通）。

### 13.2 伪造设备注入点（`hook_fake_device.js`）

| 维度 | 注入方式 |
|------|---------|
| AndroidID | Java hook `Settings.Secure.getString(android_id)` → 假值 |
| 序列号/指纹/机型四件套/bootloader/hardware | native `Interceptor` hook libc `__system_property_get` → 假值（ro.serialno/ro.boot.serialno/ro.build.fingerprint/ro.product.{model,brand,manufacturer,device}+各分区变体/ro.hardware/ro.boot.bootloader；注意 `PROP_VALUE_MAX=92`） |
| token | hook `AppInfoUtil.getToken/getUUID/getMcdDeviceToken` → `profile.token`（与 d 成一套） |
| 清缓存 | hook `l11l11Il1l1l.l1111l111111Il()` 返回 `""`（本地 smid 置空，强制重注册新 smid） |

> 数美实际读取的稳定维度清单由 `hook_collect_probe.js` 实测得出：AndroidID、ro.serialno、ro.build.fingerprint、机型四件套、bootloader、hardware。**Android 10 上拿不到 IMEI/真 MAC/getSerial**，所以设备身份主要靠这几项。改机/伪造只需覆盖它们（且机型/指纹/序列号要自洽）。

### 13.3 token 与 tid 的真机语义（实证）

- **token** = `AppInfoUtil.getUUID()` 生成的持久化 UUID（存 SharedPreferences，设备级恒定）。真机实测 `getToken()`=`getUUID()`=32位hex（Android10 拿不到 deviceId 故走 UUID）。我们每设备用 `uuid4().hex` 伪造、hook 注入，**与 d 绑成一套**。
- **tid** = `TidManager`（`be.z`）：服务端签发、本地 Room 持久化、**有效期 7 天**、`channelId=96`。它是设备级会话凭证，**用时激活、过期/换设备刷新——不在造设备时激活**（否则池中 tid 会过期）。
- token / d / tid 相互独立但都代表"同一台设备"，真机里长期稳定成套。

### 13.4 Redis 设备池（`device_pool_redis.py` + `device_builder.py`）

数据结构（config `DEVICE_POOL_CONFIG`）：
- **List** `mcd:device:free` — 空闲设备 id 队列，`RPUSH` 入队、`LPOP` 领用 = **先生成先取**（FIFO）；`LLEN`=补池判断数。
- **Hash** `mcd:device:info` — field=设备id，value=整条设备 JSON（**单 key，N 设备不膨胀**；早期每设备一个 key 已废弃）。
- **List** `mcd:device:account:{account}` — 账号领用过的设备 id（**一账号可多设备**，旧设备失效换新累积）。
- **Set** `mcd:device:index` — 已注册 smid，生成查重避免重复。

分工：`refill`/`serve`（补池造设备）**必须跑在接真机的本机**（造设备依赖 frida+真机）；`claim`（领用）**任意机只连 Redis** 即可（延迟 import，不依赖 frida）。`device_builder.py` 是 mcd_project 自包含的造设备模块（内置机型库 + 自带 `mcd_project/frida/` 下的 js）。

### 13.5 登录接口接入设备池（`login_manager.py` + `mcd_server.py`）

- `LoginManager.ensure_device(force_new)`：**新账号自动领一个设备；已有但无 d 的自动补领**；`force_new=True` 换新设备（旧的标 dead）。领到后写凭证（d / device_id / token，token 用设备池固定 token 覆盖旧随机值）。
- `LoginManager.ensure_tid(force)`：tid **7 天内复用、过期或换设备才用设备 token 重新 `activate_token`**，存凭证。
- `send_verify_code` 重构：去掉随机 `generate_token()`，改为 `ensure_device` → `ensure_tid` → 发码。
- `mcd_server`：`/api/login/send_code` 入队前 `ensure_device(force_new=use_new_device)`；`/api/login/verify` 前 `ensure_device()`。
- 凭证 `mcd:credentials`（field=phone）新增字段 `device_id` / `tid` / `tid_expire`。
- mcd_api 新增 `set_active_tid()` + `_active_tid` ContextVar，`build_headers` 的 `tid` 兜底从中取（与 `d` 同机制）。

### 13.6 端到端联调结论（本地 Redis + gunicorn server + captcha worker，2026-10-17）

| 号码 | 结果 | 判读 |
|------|------|------|
| 假号 13800138000 | **code_sent**（无滑块直接发送） | 链路/设备池/token/tid/代理全部有效 ✓ |
| 17717295039 | **707053** | 换代理/直连住宅IP/全新设备/token-d成套 **都不变** → 老账号+陌生设备的**数美关联风控**（手机能登因手机是信任设备），非代码问题 |
| 18356966159 | **420 出滑块** → worker 过滑块 #1~#4 全 `REJECT` | 这号风控等级是"出滑块"（可走通），但 worker 的 DrissionPage 滑动轨迹被数美**行为检测**拒绝 = 滑块轨迹逆向难点（独立问题） |

**核心结论**：设备池 / token / tid / ensure_device / 直连开关 等本次工程全部验证正确（假号端到端 code_sent 为证）；真正的卡点是 **① 数美账号↔设备绑定历史风控（707053，换 IP/设备都绕不过）② 滑块行为检测拒绝自动轨迹（REJECT）**——都属风控策略/滑块逆向范畴，非本套代码问题。

### 13.6.1 706070「设备环境异常」与 pm clear(关键,2026-10-17 补)

造设备若复用 app 旧存储,数美会读到本地残留的**被标记设备状态** → 发码返回 `706070`(真机手动登录也一样)。**造一台真正的全新设备必须两步**:
1. spawn 前 `adb -s <序列号> shell pm clear com.mcdonalds.gma.cn` —— 清空 app 全部存储(SharedPreferences/数据库/数美私有目录缓存),回到全新设备状态;
2. resume 后点隐私协议"同意"弹窗 —— 否则数美不 `initShumei`、不注册 smid(会卡在 `D`+vdata 拿不到 `B`)。

实测对比:**不 pm clear → 706070;pm clear + 点同意 + 伪造设备 → 拿到干净 smid、发码 code_sent 正常**。已固化进 `device_builder.register_one`(默认 `pm_clear=True` + `_agree_popup` 自动点同意:uiautomator 动态找"同意"bounds,回退固定坐标 540,1340;换机型需调坐标)。

> 所以之前把 707053/706070 都归因"账号/真机被拉黑"不全对:706070 这条其实是**本地存储残留**,pm clear 后即清白——真正造全新设备是可行的。

### 13.7 工具与文件清单

- `mcd/tools/frida/`：`hook_fake_device.js`（伪造设备注入+token注入+请求头dump）、`hook_d_active.js`（RPC 取 d）、`hook_collect_probe.js`（采集维度探针）、`hook_crypto_probe.js`（加密边界探针）、`hook_token.js`（token 来源）、`fdrv.py`（注入驱动，本次补写，多设备须按序列号 `FRIDA_DEV` 选 Android）、`getd.sh`
- `mcd/tools/python/`：`rpc_getd.py`（真机现取 d）、`decode_d.py`（d 两层 base64 解码）、`device_pool.py`（JSON 版 CLI）、`fake_device_poc.py`（路线A PoC）
- `mcd_project/`：`device_builder.py`（造设备，自带机型库+frida js）、`device_pool_redis.py`（Redis 池服务）、`frida/`（自带注入 js）

### 13.8 用法

```bash
PY=/Users/houjie/venv/python3-forcrawl/bin/python     # frida 15.1.17
# 补池造设备（跑在接真机的本机；每15min自动或手动）
$PY mcd_project/device_pool_redis.py serve            # 或 refill 补一次
# 领用（任意机，只连 Redis）
$PY mcd_project/device_pool_redis.py claim <账号>
$PY mcd_project/device_pool_redis.py stats
# 登录接口会自动 ensure_device（新账号领 / 无 d 补领 / use_new_device 换）+ ensure_tid（7天刷新）
```

> 环境坑（已记入）：电脑同时插 iPhone 时 `frida.get_usb_device()` 会选错到 iOS，须按序列号 `frida.get_device(serial)`（脚本用 `FRIDA_DEV` 环境变量）；frida-server 跑久（~1天）会僵死致 `InvalidHostID`，重启即可。

---

**v5.0 更新时间**: 2026-10-17（设备指纹池：伪造设备批量生成 d + token/tid 绑定 + Redis 池 + 登录接口接入 + 端到端联调）  
**v4.0 更新时间**: 2026-10-17（frida 动态调试取 d：15.1.17+attach 真机跑通）  
**v3.0 更新时间**: 2026-10-16（设备指纹 d / 发码登录风控 / 自动滑块生产化）  
**v2.0 更新时间**: 2026-10-10  
**核心贡献者**: 侯杰 + Claude Code


---

## 十四、本地造设备 + 706070 终极认知（2026-10-20/21，v7.0）

### 14.1 本地纯 HTTP 造数美设备（不用真机/frida）

- `mcd_project/mcd_shumei_d.py` 复刻数美 vdata 加密打包(deflate wbits=-15 + AES-256-CBC + RSA-OAEP，逆自 libsmsdk short-box compress3/encode2；publickey() 已适配 pycryptodome 3.9.8，RSA.import_key 直接吃数美 X.509 证书)。
- 注册接口(抓包 mitmproxy_flows/flows_20260914 得)：`POST https://fp-it.fengkongcloud.com/deviceprofile/v4`，body=box{organization/os/appId/encode:2/compress:3/data/tn/ep}，Content-Type: application/octet-stream，**无签名头**；响应 `{"code":1100,"detail":{"deviceId":"..."}}`，**d = "B"+deviceId**(code 1902=字段不全被拒)。
- 明文设备 JSON(a1~a175) 在 Java 层 `com.ishumei.smantifraud.dfp.SMSDK.v1` 入参(hook_fake_device_v2.js 的 DUMP_V1 开关 dump [v1-IN-BOX]完整 / [v1-IN-SHORTBOX]短版)；注册用 shortbox(26字段，usingShortBoxData)。a136=build1 与脚本匹配。
- 流程：hook 一次拿 shortbox 模板(shumei_shortbox_template.json) → 替换身份字段(a24 android_id/a46 机型/a9/a40/a125 时间) → make_box → POST → B。生成器 local_device_gen.py(替换字段 + 新短效代理注册 + 失败换代理重试)。
- 集成 device_pool_redis：config.DEVICE_POOL_CONFIG['build_mode']='local'(默认)/'frida'，refill/claim/serve 用法不变；`python device_pool_redis.py test <真号>` = 本地造B+发码验证。
- shortbox 不含 a84(Widevine) 等硬件根字段 → 可批量造独立设备、绕开真机**注册环节**的 706070。

### 14.2 706070 终极认知（更正 v4.0「pm clear 规避」旧结论）

- v4.0 曾写"pm clear 规避 706070"——**只对注册环节成立**。实测注册 code 1100 成功、d 有效，但**发码仍 706070**。
- 排查全清单都排除：a18(adb/无SIM，A/B实测改了没用)、移动流量、号历史(新旧真号全706070)、发码请求头(qf.a BFFHeaderInterceptor 加 d/token/tid/... + qf.b MCDBasicHeaderInterceptor 只加 UA/v)、本地 vs 真机(都706070)、tid(00003TuN 正常值)。
- 真机(Pixel/Mi6/华为非root/全新Google)、本地造的、所有真号 → **全706070**；唯一能过的是白名单测试号 13800138000；天津朋友正常真机(没碰过测试环境)能过。
- **结论：706070 是数美发码时的服务端「设备指纹组合 / IP / 行为环境」风控，不是单个客户端字段能解决。** 本地造 d(注册)技术成功，但 706070 卡在发码风控层，真机也绕不过，超出"造 d"范围。
- 两个待验证根因(需干净 root 真机 + 干净住宅 IP)：① shortbox 模板来自被 frida 反复注册的 Pixel，不变字段组合被数美关联到已标记设备 → 需干净真机重新 hook 一份模板；② 神龙代理 IP 被识别为代理 + 刚注册秒发码无正常使用行为。

### 14.3 短效代理 + 登录代理接入（utils / login_manager 新增）

- `utils.extract_short_proxy_by_city(city)` / `get_short_proxy(city,count,force_new,as_dict)`：神龙短效按城市提取(area=国标6位码/sign不绑area/白名单模式)，Redis 池 RPOPLPUSH 多进程轮巡，不支持城市取最近(haversine)。配置 config.SHENLONG_*/SHORT_PROXY_CONFIG/SHENLONG_SUPPORTED/CITY_GEO。上海短效IP实测存活~3分钟。
- `login_manager.get_login_proxy(is_send_code)`：USE_PROXY=False直连；已绑定→静态；未登录(sid空)→短效(发码force_new/其余复用轮巡)；登录成功(sid)后才绑静态(无可用代理抛错→dispatch 503)。绑定时机从"构造即绑"改为"登录成功后"。
- `device_builder.register_one` 加 ensure_frida_server(仿 getd.sh 自启 frida-server 15.1.17，没起则 root 启动)。

### 14.4 云手机 hook 干净 shortbox 模板（2026-10-21，针对根因①）

在**能过数美风控的多多云手机(MOA-AL00/Android10)** 上 hook 出干净 box/shortbox 明文,替换被 frida 污染的 Pixel 模板。**完整用法见 `tools/frida/云手机dumpbox用法.md`**。

- **连接**:云手机只在 Windows 客户端暴露,Mac 借 Windows adb server(见 `docs/Mac连接Windows共享adb手机.md`)。**frida 不认 ADB_SERVER_SOCKET**(写死连本机5037),需跑 `tools/frida/adb_relay.py <win-ip> 5555 5037` 把本机5037中继到 Windows,frida 才能 `-U` 枚举到云手机。云手机 adb shell 已 root,frida-server 直接 setsid 跑、**不用改名**。
- **两套防护**:数美(云手机过滑块/发码)vs app 原生壳 `libmsaoaidsec`(反frida,检测到 frida-server 进程/27042 或注入就故意崩)。手动开 app 在 frida-server 开着时也会崩。
- **成功配方**:`spawn + frida_bypass_combined_local.js(绕壳) + hook_dump_box_plain.js(单脚本dump v1入参) + 重试竞速`。bypass 在 linker call_constructors 期 patch 85 个崩溃守卫(`patched=85`);全新注册路径有守卫不在列表→确定性崩,靠重试某轮赢竞速。**只能 2 个脚本**:DexHelper patch 要在 25ms 轮询里赶在陷阱前完成,多一个 hook 脚本抢 JS 线程→每次必崩,所以 dump 全并进一个脚本。
- **一键**:`FRIDA_DEV=127.0.0.1:63373 PY tools/frida/dumpbox_cloud.py 12 30 <outdir>` → 自动 spawn+bypass+hook+重试,主动调 getVData 逼注册,抓到 v1 入参解析存 `cloud_v1_arg{1,2,3}.json`(arg2=shortbox 注册用)。
- **crypto 边界实证**:明文JSON→**native(libsmsdk) deflate**(Java层Deflater不触发)→AES/CBC/PKCS5(key=16B字串经RSA-OAEP封)→RSA/ECB/OAEP-SHA256封32B seed。
- **干净 vs 旧脏模板**:`a18` `{ABSENT,adb}`→`{LOADED,mtp}`;多出 `a2/a97`(同base64签名)、`a103`(uuid);`a167/a168` 0→787552;`a4`/`a85(+apps)`/`a121/a135` 不同。产出 `mcd_project/shumei_shortbox_template_cloud.json`(29字段) / `shumei_fullbox_cloud.json`(82字段)。
- ⚠️ `a2/a97` 疑似对 box 的签名,换 a24/a46 后可能要重算,勿直接抄——**待 jadx 验证**。
- 工具:`tools/frida/hook_dump_box_plain.js` / `dumpbox_cloud.py` / `adb_relay.py` / `云手机dumpbox用法.md`。

### 14.5 ★706070 定性 + 本地铸干净 d 跑通(2026-10-21,最终结论)

- **706070 vs 420 的分水岭 = `d`(设备信誉),不是 IP。** mitm 抓到云手机能过的发码(`POST api2.mcd.cn/bff/passport/verifyCode/sms/send`,420→过滑块带 `x-mcd-sec-verify`→200);把云手机干净 d/token/tid 塞进我们的请求 + 神龙代理发真号 → **420**(非706070)。神龙 IP、请求构造都没问题,瓶颈一直是自造 d 被标记。
- **本地铸干净 d 跑通**:`local_device_gen.TEMPLATE` 换成 `shumei_shortbox_template_cloud.json`(干净云模板)后,本地造的 d 发码也 **420**(旧 Pixel 模板则 706070)。即云手机只需一次性提模板,之后本地批量铸干净 d。a2/a97 静态复制未被拒、tid=00003TuN 通用值、每台刷 a103=uuid4。
- ⚠️ **签名版本坑**:`v`/`p` 必须保持 `7.0.41.0`/`102`(与密钥 `V4AK=HJ7..` 匹配);对齐成 43.1/105 会被网关 403(无 5DLU 密钥)。`biz_scenario=500/biz_from=1011` 不签名、已对齐。
- 脚本:`scratchpad` 的 `test_clean_d.py`(注入指定d发码判定)、`test_local_clean_template.py`(本地造d+激活+发码判定);mitm:`tools/mitm/mitm_mcd.py`。
- 剩下:把干净模板接进 `device_pool_redis` 批量补池 + 配 `captcha_login_worker` 过滑块 → 端到端发码。

### 14.6 biz_scenario/biz_from 按接口对齐(2026-10-21 全流程抓包比对)

- mitm 抓真机完整下单流程(`tools/mitm/mitm_mcd.py` 抓、`compare_capture.py` 逐接口 diff),发现真机 `biz_scenario`/`biz_from` **按页面/业务分**,我们原来全写死 `102/1006`(指纹破绽)。
- 已在 `mcd_api.BIZ_MAP`(path→biz)对齐,`build_headers(biz_scenario/biz_from 默认 None)` 自动按 path 查表;修掉之前 login 错设的 biz(真机 `500/1004`,发码 `500/1011`)。关键:store 103/13·12·14、stores/{id} 101/1004、menu 101/1009、products/detail 101/1001、cart 101/1007、order/confirmation 200/1004·202/1020·202/1019、getNearest 202/1021、orders 202/1012、cashier 501/1001·501/1002。
- `v`/`p` 是签名头,因密钥 `V4AK=HJ7..`(7.0.41.0版)锁定,**保持 7.0.41.0/102**,不随真机 7.0.43.1/105 改(改了网关 403)。biz_* 不签名可安全对齐。
- 对比产物:`mcd/resources/logs/cap_full/`(319 条真机请求 req/resp)。

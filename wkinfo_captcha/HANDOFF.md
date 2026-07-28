# wkinfo_captcha 交接文档

目标网站：`https://law.wkinfo.com.cn`（威科先行法律数据库）。目标是脱离浏览器、纯 Node.js/Python 算法还原其验证码、注册、登录相关接口，最终能稳定拿到已登录 session 去访问数据接口。

本文档面向"接着做风控/反爬处理"的后续工作，先说清楚**现在做到了什么**、**怎么做到的**、**踩过哪些坑**，最后列出**还没做、值得优先看的风控相关点**。逐条细节证据见同目录 `请求链路.md`（抓包记录+踩坑原始记录），这里是提炼总结。

## 目录结构

```
wkinfo_captcha/
├── aes_encrypt.js      # Node，唯一职责：AES-128-ECB/PKCS7 加密（生成加密参数）
├── recognize.py        # Python(ddddocr)，验证码图像识别
├── captcha_client.py   # Python，验证码全流程编排 + 注册全流程编排
├── login.py            # Python，登录 + session 缓存
├── sessions/           # login.py 的 session 缓存目录（按用户名存 json，gitignore 建议加）
└── 请求链路.md          # 详细抓包证据 + 踩坑记录（按时间线，本文档是它的摘要+索引）
```

运行环境：
- Node：系统自带即可，`aes_encrypt.js` 只用内置 `crypto` 模块，**没有任何 npm 依赖**（之前装过 pngjs 后来废弃删了，不要再装）。
- Python：必须用 `/Users/houjie/venv/python3-forcrawl/bin/python`（已装 ddddocr/opencv/numpy/pillow），不是系统 python3。
- `captcha_client.py` 和 `login.py` 里都是同步阻塞的 `requests` 调用，没有做异步/并发。

## 已实现能力

### 1. 验证码识别 + 加密（`recognize.py` + `aes_encrypt.js` + `captcha_client.py`）

网站验证码是开源 **AJ-Captcha**（anji-plus/captcha）的定制部署，两种类型：`blockPuzzle`（滑块）和 `clickWord`（点选文字），共用同一套接口和加密算法。

**加密算法**（源码定位于 `assets/js/verify-slipping/ase.js`，非猜测）：
- AES-128-ECB / PKCS7 padding，key = `secretKey`（服务端下发）的原始 UTF-8 字节，无 IV
- `pointJson` 明文：blockPuzzle 是 `{"x":..,"y":5}`；clickWord 是 `[{"x":..,"y":..}, ...]`（按 wordList 顺序）
- `captchaVerification`（提交给 `/csi/captcha/verify` 时用）= AES(`token + '---' + 上面那个明文`, secretKey)
- **明文格式必须和 JS 的 `JSON.stringify` 完全一致**（见下面踩坑）

**图像识别**（`recognize.py`，用 ddddocr，说明见该文件 docstring）：
- blockPuzzle 缺口：`ddddocr.slide_match(piece, bg, simple_target=True)`。注意 `simple_target=True`（不裁剪透明边）比默认的 `False` 准得多（实测 8/8 vs 6/8）——网站背景图有 1 个真缺口 + 2 个干扰假缺口，早期用手写边缘检测/相关系数被骗过一次。
- clickWord 点选：`ddddocr.detection()` 找字符框很稳，但单个分类模型（default/old/beta）识别这种旋转+彩色+自然背景的汉字准确率不高，改成三个模型识别结果**取并集**（+裁剪时四周留 4px），整单命中率从 0~33% 提到 60%。因为重新拉验证码不要钱，`solve()` 用"不 confident 或 check 失败就换一张重试"策略（`max_attempts=6`），不是死磕单张图片精度。

**接口链路**：`POST /csi/captcha/get` → 识别 → `POST /csi/captcha/check` → `POST /csi/captcha/verify`（verifyType=register 时带 phoneNumber）。

### 2. 完整注册流程（`captcha_client.py` 的 `full_registration_flow`）

`captcha/verify` 成功后：
- `GET /csi/user/captcha?telephone=&password=&email=` —— 这一步才是真正触发发短信的调用
- 用户输入手机收到的真实验证码（`sms_code=None` 时会 `input()` 提示，不要瞎填）
- `POST /csi/user` —— 最终提交注册，body 里 `code` 字段固定值 `"01BE01"`（观察到两次抓包都一样，像是这个"law"分组试用渠道的固定值，不是动态生成的）

### 3. 登录 + Session 缓存（`login.py`）

静态分析 `main.<hash>.js` 定位到（不是抓包猜的）：
- **登录**：`POST /csi/account/validate/ex`，body 明文 `{"username","password"}`（不加密、不需要验证码！和注册流程完全不同）。成功直接返回完整用户 profile，`Set-Cookie: connect.sid=...`
- **登出**：`GET /api/logout`
- **session 有效性检查**：`GET /api/autoLogin`，返回 `{"login":false}` 说明当前 cookie 已失效；返回 `{"login":true}` 时服务端还会顺带补发一批 cookie（`userInfo`/`autologin`/`cinfo` 等）

`get_session(username, password)` 逻辑：先读 `sessions/<username>.json` 缓存 → 检查本地记的 cookie 过期时间 → 用 `/api/autoLogin` 二次确认真的还有效 → 都过才复用；否则才真正调登录接口并覆盖缓存。**二次确认这一步是必须的**，因为服务端有单并发限制（见下），别处一登录可能直接把老 session 踢了，光看 cookie 自带的 Expires 不可靠。

已验证账号：`1558109546@qq.com` / `315128abc`。**注意** `18356966159` 是这个账号的手机号，不是登录用户名，登录必须用邮箱。

## 关键踩坑（后续加风控逻辑时容易踩到同样的坑）

1. **JSON 数字格式必须和 JS 字节级一致**：Python `json.dumps({"y":5.0})` 输出 `"y": 5.0`（带空格、保留 `.0`），JS `JSON.stringify` 输出 `"y":5`。这个差异让服务端在 AES 解密后解析明文时直接抛 `NullPointerException`（HTTP 500），而不是正常的业务错误码——**服务端对解密后明文的处理不是纯粹的 JSON 解析**，格式必须做到跟 JS 输出一模一样。`captcha_client.py` 里 `_js_num()`/`_point_str()` 就是干这个的。
2. **"更像浏览器"的请求头反而更容易触发异常**：给 `/csi/user/captcha` 和 `/csi/user` 加上 `identification`/`module`/`ucv`/`appversion`（模仿 Angular HTTP 拦截器实际发的那套）之后，直接返回 `E_000_003 注册验证码校验失败`；换回 `captcha/get|check|verify` 用的极简 header 集合（`verify.js` 原生 `$.ajax` 那套）就完全正常。**没深挖是哪个字段导致的**，只是发现"更完整的头"不代表"更安全/更像人"，可能触发了服务端另一条校验路径。这点对接下来做风控处理很重要——不能想当然地认为"头越全越好"。
3. **默认 `python-requests` 的 User-Agent 会被 WAF 拦**：早期测试中直接用 `requests` 默认 UA 打接口，收到过 `403 Forbidden ... denied by UA ACL = blacklist`（Tengine 层的 UA 黑名单）。所有代码里都已经固定用真实 Chrome UA 字符串，没有再复现过，但这是**目前唯一实锤过的、纯 UA 层面的风控拦截**，说明至少有一层基于 UA 的黑名单存在。
4. **验证码背景图里的干扰缺口**：blockPuzzle 有 2 个假缺口专门用来骗自动化识别，clickWord 的字符也是刻意做了旋转/变色/自然照片背景来干扰 OCR。这两个本身就是这个站点风控体系的一部分（AJ-Captcha 的"干扰"配置项），已经用相关系数模板匹配 + 多模型 OCR 取并集的方式绕过。
5. **账号单并发登录限制**：同一账号同时只能有一个有效 session，重复登录会收到 `{"code":"C_002_001","message":"用户并发超标"}`。不是 bug，是业务规则，但如果后续要做"多账号池轮换"之类的风控对抗，这个限制要考虑进去。

## 观察到但还没处理的风控信号（后续重点）

目前所有测试都是**低频、人工触发**的，没有主动去压测/高频调用，所以下面这些"看到了但没验证过阈值/触发条件"的信号，都是接下来做风控处理时要重点关注的：

- **`acw_tc` cookie**：阿里云 WAF（Tengine 反爬）常见的第一跳 cookie。目前所有接口调用里都能拿到这个 cookie（服务端 Set-Cookie 下发），且没有遇到需要额外 JS challenge 才能拿到有效值的情况——但这可能只是因为请求频率低、UA 正常，没有触发更严格的二跳校验（`acw_sc__v2` 之类）。高频调用时需要重点盯这个。
- **`x-alicdn-da-ups-status` 响应头**：在 `/csi/user` 的 400 响应里见过 `x-alicdn-da-ups-status: endOs,0,400`，阿里云"用户保护系统"相关的状态头，具体含义和触发条件没有深挖。
- **`uber-trace-id` / `traceparent` / `b3` 请求头**：真实浏览器每次 XHR 都会带（Zipkin/OpenTelemetry 格式的分布式追踪 ID），我们的 Python 请求里完全没带这些头，目前看不影响功能——但如果服务端有"检查这些 trace id 内部一致性/是否存在"的风控逻辑，这可能是一个能被识别出"不是真实浏览器"的信号，值得后续验证要不要伪造。
- **`boldrum-trace`（阿里云 SLS）行为埋点**：真实浏览器会不断上报点击、页面性能、错误等行为数据到 `boldrum-trace.cn-beijing.log.aliyuncs.com`。我们的请求完全没有这些伴随的埋点流量。如果服务端有"这次 API 调用前后有没有对应的行为埋点"这类关联风控，纯 API 调用会很显眼——目前没证据表明服务端真的做了这层关联，但这是最大的一个未知项。
- **请求频率/间隔**：目前脚本里 `solve()` 的重试是没有 sleep/退避的，连续失败会很快重试。如果之后要跑更大量级的验证码识别测试，建议加上退避策略，避免因为频率触发新的风控（目前没触发过，但也没测过高频场景）。

### 建议的下一步

1. 先做一次**可控的压测**（比如连续跑 20~50 次 `captcha_client.py` 的 `solve()`），观察是否会新增 `acw_tc`/`acw_sc__v2` challenge，或者被 UA ACL/频率限制拦截，摸清阈值。
2. 针对 `login.py` 里目前"登录失败直接抛异常"的地方，补充对常见风控错误码（如果压测跑出来的话）的识别和处理（重试/退避/换 UA 等），而不只是处理 `C_002_001`。
3. 评估要不要伪造 `uber-trace-id`/`traceparent`/`b3` 和配套的 `boldrum-trace` 埋点流量，让请求"看起来"更完整——但先确认服务端是否真的关联校验这些，不要没验证就加复杂度。

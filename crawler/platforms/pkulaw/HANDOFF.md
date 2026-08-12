# pkulaw 平台交接文档

目标网站：`https://www.pkulaw.com`（北大法宝）。跟 `platforms/wkinfo/` 的关系：都是
`crawler/` 这套通用多平台框架下的一个平台插件，用法和目录结构参考
`crawler/README.md`，这里只记 pkulaw 特有的部分。

## 目录结构

```
platforms/pkulaw/
├── config.py            # PLATFORM名/验证码aid/CAS登录常量/邮箱后缀白名单
├── pow_solver.py        # 纯 Python，腾讯验证码的工作量证明题（MD5爆破）
├── recognize.py         # ddddocr，点选验证码识别（跟 wkinfo 的 clickWord 同套路）
├── tdc_bridge.js / tdc_client.py     # Node沙箱跑 tdc.js 本身，拿 collect/eks
├── eo_challenge_bridge.js / eo_challenge.py  # Node沙箱跑 EdgeOne 的 JS cookie 挑战
├── account_type.py        # 判断 identifier 是手机号还是邮箱，供 login.py 自动分发
├── auth.py               # 验证码+短信/邮箱验证码+登录表单+改密码+token，全是"怎么做"这一层
├── login.py              # Redis session 缓存 + 自动判断账号类型分发（"要不要重新登录/走哪条逻辑"）
├── document_store.py     # _id = "{category}_{doc_id}"，URL<->category/doc_id互转
├── detail_client.py      # 文章详情页采集，缓存优先，自动处理 EdgeOne 挑战
├── platform.py           # core.platform_base.Platform 的实现
└── HANDOFF.md            # 本文档
```

`pkulaw_captcha/`（`crawler/` 之外，仓库根目录同级）是这些代码最早成型、单独调试
用的地方，`auth.py`/`eo_challenge.py`/`tdc_client.py` 等都是从那边原样搬过来的
（只改了 import 方式），那边的 `reverse-records/请求链路.md` 记录了更详细的踩坑/
抓包证据链，这份 HANDOFF.md 是面向"怎么在 crawler 框架里用"的摘要。

## 已实现能力

### 1. 验证码 + 登录（`auth.py` + `login.py`）

跟 wkinfo 完全不同的验证码厂商：pkulaw 用的是**腾讯 Turing 验证码**（天御），不是
AJ-Captcha。核心难点和解法：

- **点选坐标**（`ans` 字段）：图上有若干汉字，题目文字给出点击顺序，用 ddddocr 识别，
  跟 wkinfo 的 clickWord 是同一类问题。
- **`collect`/`eks` 字段**：来自 `window.TDC.getData(true)`/`getInfo().info`，真实
  实现封在 `tdc.js` 里，而 `tdc.js` 本身是一个自定义字节码虚拟机（JSVMP）。**没有做
  opcode 级别的逆向**（工作量太大、大概率不稳定，见 pkulaw_captcha 项目里跟用户的
  讨论），而是把 `tdc.js` 原始代码丢进一个只有十几个手写 stub（window/document/
  navigator/canvas-2d/localStorage）的 Node `vm` 沙箱里，让它自己的逻辑真实跑一遍。
  **这是经用户明确认可的折中方案**——不是浏览器/Puppeteer/Selenium，只是给 tdc.js
  一个能让它自己跑起来的最小环境。已用真实请求验证通过（服务端返回
  `errorCode:"0"`）。
- **`pow_answer`/`pow_calc_time`**：标准工作量证明，本地爆破整数 N 直到
  `md5(prefix+N)==目标md5`，纯 Python，跟 tdc.js 完全无关，已验证跟真实抓包数据
  完全吻合。

登录本身分两段，**中间有个坑**：
1. `sms/code/verify-email` 只校验邮箱验证码本身对不对（204=对，400+CodeNotExist=错），
   **不会产生登录态**。
2. 真正登录靠 `login-actions/authenticate`（提交 Keycloak 的邮箱登录表单）+ 后续一次
   授权码兑换。第一跳是标准 302，`requests` 能自动跟；但落地到
   `static.pkulaw.com/statics/kc/index.html?redirect_path=...&code=...` 之后**不会
   继续自动跳**，因为下一跳是那个页面自己的 JS 决定的，而且那段 JS 字面拼接
   （`redirect_path + '?code=' + code`）其实是错的（会拼出两个问号），实测真正生效
   的是用 `&` 拼接——`auth.complete_login()` 已经手动处理了这两跳，不需要执行 JS。

`login.py` 提供 Redis session 缓存（跟 wkinfo `login.py` 是同一个模式）：
`get_session(email)` 先看缓存还有没有效（`auth.is_logged_in()` 二次确认），没有才
真正走 `login_interactive()`。**跟 wkinfo 的关键区别**：pkulaw 的邮箱登录**没有
密码**，每次真正登录都要一个真人去读一下邮箱验证码
（`login_interactive(email, sms_code_getter)`，`sms_code_getter` 默认 `input()`，
可替换，跟 wkinfo 项目 `sms_provider.py` 是同一个设计），所以这里**没有做**
`core.account_registry`/`core.scheduler` 那套多账号自动调度——不是没做完，是
pkulaw 这边目前确实没法在没有真人参与的情况下触发登录，做了也用不上。

**已知限制（2026-07-30 更新，推翻了之前"只有非白名单后缀才转审核"的假设）**：
`config.KNOWN_EMAIL_DOMAINS`（`gmail.com`/`qq.com`/`hotmail.com`/`icloud.com`）**不是
判断会不会被转人工审核的可靠依据**——真实测试中 `xingchi660@gmail.com`（后缀在白
名单里）三次登录都卡在 `login-actions/authenticate` 没走到中转页，最后在收件箱里
找到网站发来的人工审核邮件（发件人 `market@chinalawinfo.com`，要求回复姓名/行业/
工作单位/手机号），证明白名单域名一样会被转审核，大概率是按 IP/风控信号判断，不是
单纯按邮箱后缀。

真正可靠的判据是 `login-actions/authenticate` 失败时**响应页面 HTML 本身**会带一行
提示文案"请回复邮件进行审核，如有疑问可联系客服。"（真实抓包确认，这行文字被服务端
同时塞进了页面里多个 tab 的 `.tips` 占位 div，不管当时激活的是哪个 tab，稳定出现）。
`auth.py` 的 `_extract_tip_messages()` 是通用解析：正则提取所有非空的
`<div id="xxxTip" class="tips">文案</div>`（HTML 实体反转义、去重），不是只认这一句
固定文案——服务端往这些 tip 占位 div 里塞的是当次失败的真实原因，转人工审核只是
目前唯一实测到的一种，以后遇到别的失败原因（比如验证码相关的提示）也会被这个解析
逻辑原样提取出来，不用再靠 `_response_snippet` 截 300 字符的 HTML 头去猜。
`complete_login`/`complete_login_by_phone`/`complete_login_by_password` 三条路径落地页
判断失败时都会先解析 tips：解析出的文案里含 `_PENDING_REVIEW_MARKER`
（"请回复邮件进行审核"）就抛 `AccountPendingReviewError`（区分真正的"转审核"），否则
抛普通 `RuntimeError`，但异常信息里会带上解析出的具体文案（解析不到才退化成
`_response_snippet`）。`login.login_interactive()` 精确捕获
`auth.AccountPendingReviewError` 才会转成 `RegistrationPendingReview`，验证码真错了
照常抛普通异常，不会再误导调用方去查邮箱。
`send_email_code` 也有当日发送次数限制，遇到过 `{"error":"limit_day"}`。

### 账号模型（3 个业务流程，用户确认过，2026-07-30 起全部改成自动判断账号类型）

**所有对外接口的账号参数都叫 `identifier`（手机号或邮箱），不是专门的 `email`
参数**——`account_type.detect(identifier)` 用正则（11位1开头纯数字=手机号）+
是否含`@`（邮箱）自动判断，`login.py` 里的 `login_interactive`/`get_session`/
`change_password` 全部先判断类型再分发到对应的手机/邮箱具体逻辑，调用方不需要
自己说是哪种。

1. **注册**：邮箱未注册过时，走一遍验证码+邮箱验证码通常是"直接注册并登录"（跟
   登录是同一个接口，`auth.complete_login`）；但也可能被转人工审核，白名单后缀
   （gmail/qq/hotmail/icloud）不保证不会被转审核，见上。手机号这条路
   （`auth.complete_login_by_phone`）也已经逆向实现，没观察到手机号被转审核的情况——
   跟邮箱是同一个 `login-actions/authenticate` 机制，字段不同：
   `loginType=0&tabType=phoneValidate&source=&phoneNumber=X&smsCode=Y`（真实抓包
   确认，2026-07-30，之前误以为的 `cas-ipv6.pkulaw.com/sms/ipv6-login` 只是个 IP
   检测请求）。判定"注册成功"的标准就是 `auth.is_logged_in()` 测出来真的登录了。
2. **改密码**：`login.change_password(identifier, new_password)` ——手机号和邮箱
   两条路**都已经实现并且都真实验证过**（手机号：**用我们自己的 Python 代码**
   完整跑通一次真实改密码成功——`login.get_session` -> `get_access_token` ->
   `auth.solve_and_verify` -> `auth.send_phone_code` -> `auth.modify_password_by_phone`
   -> `login.set_password`，每一步都是真实请求，不是照抓包对出来的；邮箱：请求体
   形状对称验证正确，测试邮箱本身不存在导致业务层面失败，接口本身接对了）。
   成功后调用 `login.set_password(identifier, password)` 把新密码写回
   `account:{platform}:{identifier}` 这条 Redis 记录，只更新 password 字段。
3. **登录**：`login.get_session(identifier)` 会看账号记录里 `password` 字段是不是
   空的——空的走验证码登录（`login_interactive`，需要真人读验证码）；有值走密码
   登录（`auth.password_login`）。**密码登录已实现并用我们自己的代码真实验证成功**
   （手机号+密码；邮箱理论上同一个表单/同一套代码，还没单独测过，见"待办"）。
   跟验证码登录不同，手机号和邮箱是**同一个表单**（`id="kc-form-login"`，不带数字
   后缀），不需要分手机/邮箱两条路。

账号记录用 `core.account_registry`（Redis Hash `account:{platform}:{identifier}`），
字段里 `password` 默认是空字符串（表示"这个账号目前只能用验证码登录"）。
pkulaw 账号没有 wkinfo 那种"试用期"概念，`expires_at` 只是为了满足
`core.account_registry` 的数据模型存了一个很远的时间，不是真的会过期。

### 2. EdgeOne（腾讯云 CDN/WAF）JS Cookie 挑战（`eo_challenge.py`）

某些请求会被拦下来，返回体不是真实内容，是一段重度混淆的 `<script>`，核心逻辑是
设置 `__tst_status`/`EO_Bot_Ssid` 两个 cookie 再 reload。**同样没有逐层手动反混淆**
（数组轮转量/case顺序每次大概率不一样，`EO_Bot_Ssid` 这个值本身也是每次挑战不一样，
写死复用不安全，实测过两次不同请求算出来的值确实不一样），做法跟 tdc.js 一致：把
挑战脚本丢进 Node `vm` 沙箱真实跑一遍，拿它自己产生的 cookie 赋值。

`is_challenge_page(resp)` 判断撞没撞上（响应短 + 含 `EO_Bot_Ssid` 字符串），
`solve(html)` 跑沙箱拿 cookie dict，`get_with_challenge_retry(session, url, ...)`
是能直接替代 `session.get` 的封装，撞上了自动解一次再重试；`prime(session, url)`
是主动预热版本，登录后调一次能提前把这两个 cookie 准备好。

**实测这个挑战不是每次请求都会触发**，具体触发条件（请求频率/IP信誉/特定header
缺失）还没摸清楚，目前策略是"撞上了就解"，不是"每次都主动先解一遍"。

### 3. 文章详情页采集（`document_store.py` + `detail_client.py`）

存储结构（库 `crawler`，collection `pkulaw`，用的是 `core.document_store` 通用层）：
```
_id: "{category}_{doc_id}"          例如 "qikan_5c6347f6bc4c4866bdca50e0aff747f0bdfb"
category: "qikan"                    从 URL 路径解析出来
docId: "5c6347f6bc4c4866bdca50e0aff747f0bdfb"
url: 完整文章 URL
contentHtml: 正文HTML（lxml xpath '//*[@class="content"]' 提出来的那个节点）
rawHtmlLength: 整页原始HTML长度，调试用
cctime / crawl_time: core.document_store 自动加的采集时间戳
```

`view_detail(category, doc_id, identifier)` / `view_detail_by_url(url, identifier)`
两种调用方式都支持（`identifier` 是手机号或邮箱），缓存优先（MongoDB 命中直接
返回，不需要登录、不发请求）；没命中才用 `identifier` 对应的登录 session 真实
请求，过程中自动处理 EdgeOne 挑战。

**还没有做真实的端到端测试**（写完当天用户的测试账号触发了 `limit_day` 限流，
约好隔天再测）——`auth.py`/`eo_challenge.py`/`tdc_client.py` 这些底层部分本身已经
在 `pkulaw_captcha/` 项目里用真实请求验证过，搬进 `crawler/` 框架时只改了 import
方式和加了 Redis 缓存层，逻辑没有变化，但 `login.py` 的 Redis 缓存路径和
`detail_client.py` 整条链路还没有跑过真实请求，是接下来第一件要做的验证。

## 改密码流程（真实抓包确认，2026-07-30）

用户手动走了一遍"手机号登录 -> 改密码"和"邮箱改密码"两条路，抓包确认了完整链路，
已经实现在代码里（`auth.py` 新增的几个函数 + `login.change_password`，统一入口，
自动判断手机号/邮箱，见上面"账号模型"）：

**关键发现：gateway.pkulaw.com 接口不认 Keycloak session cookie，要另一套独立的
Bearer access_token**：
```
POST https://www.pkulaw.com/gateway/account/auth/token
body: {"clientId":"pkulaw","code":"<登录时那个OAuth授权码>","redirectUri":"<跟登录时传给Keycloak的一致>"}
-> {"access_token":"...", "refresh_token":"..."}
```
实测 `exp-iat` 是 1800 秒（30分钟）。过期后刷新：
```
POST https://www.pkulaw.com/gateway/account/auth/refreshtoken
body: {"access_token":"<旧token>","client_id":"pkulaw"}
-> {"access_token":"<新token>","refresh_token":null}
```
**这个刷新接口实际靠的是 session 自带的 Keycloak cookie 校验，不是 JWT 里
`refresh_token` 字段真的生效**——实测抓包里这个请求带着完整的登录 cookie
（`cookie:` 头），response 里 `refresh_token` 直接是 `null`。所以 `session` 必须是
登录时那个还带 cookie 的 session，不能拿旧 access_token 随便配一个新 session 去刷新。

之后所有 `gateway.pkulaw.com` 接口都要带 `Authorization: Bearer {access_token}`。
真实抓包里就撞上过一次：`access_token` 刚过期 96 秒时去调
`modify-password-by-phone`，服务端返回 `401 {"message":"Missing JWT token in request"}`，
刷新 token 后重试就成功了——`login.get_access_token(identifier, session)` 已经实现了
"缓存的 token 还有效就用、快过期/过期了就刷新、完全没有就报错要求重新登录"这套逻辑。

**改密码本身**（手机号+邮箱两条路都已真实测试）：
```
POST cas.pkulaw.com/.../sms/code/send?phoneNumber=X&randstr=...&ticket=...      # 手机
POST cas.pkulaw.com/.../sms/code/send-email?email=X&randstr=...&ticket=...     # 邮箱，同注册流程
-> {"expiresIn":300}

PUT gateway.pkulaw.com/user-register/user/modify-password-by-phone
    body: {"phone":"17717295039","code":"655072","password":"123456abc"}
-> {"code":"200"}                                                    # 真实改密码成功

PUT gateway.pkulaw.com/user-register/user/modify-password-by-email
    body: {"email":"15346232321@qq.com","code":"453212","password":"123456abc"}
-> {"msg":"邮箱不存在","code":"A0206"}                                 # 请求体形状验证正确；
                                                                       # 测试邮箱本身没注册过，
                                                                       # 属于业务层面的预期失败
```
**跟登录不一样的地方**：不需要像登录那样先单独调 `sms/code/verify`（或
`verify-email`）校验一次验证码对不对，验证码直接跟新密码一起提交给
`modify-password-by-*` 这一个接口。**成功/失败都是 HTTP 200**，靠 response body 里
的 `code` 字段区分（`"200"`=成功，`"A0206"`=邮箱不存在，其他错误码还没见过），
`login.change_password()` 已经按这个判断。

**账号身份用 access_token 里的 sub 隐式绑定，不需要显式传 userId**——`modify-password-
by-phone`/`by-email` 的 body 里都没有 userId 字段，账号是靠 Bearer token 反解出来的
（服务端应该是从 access_token 的 JWT payload 里取 `sub`/`phoneNumber`/`email` 对应
哪个账号），这也是为什么"必须是登录状态，且要改的账号必须是当前登录的这个账号"——
用户自己确认过这条业务规则，代码里 `login.change_password()` 也是按"先
`get_session(identifier)` 确保是这个账号登录着，再用它的 access_token 去改"这个
顺序写的。

**之前"还没确认的细节"，现在有实测结论了（2026-07-30）**：`complete_login()` 里那个
OAuth `code` **确实是一次性消费的**——用我们自己的代码实测：`complete_login_by_password`
自己那次落地 GET 用掉 code 之后，紧接着拿同一个 code 去调
`exchange_code_for_token`，服务端直接返回 `401 Authorization Required`。所以
`exchange_code_for_token` 这条路目前**换不到能用的 token**（除非以后找到一种
"只拿 code、不做落地 GET"的用法，但这样一来 www.pkulaw.com 的登录 cookie 又没法
正常种下，两者要一起要目前看是矛盾的）。

**解法：不走这个一次性 code，改用 `auth.get_token_from_page(session)`**——已登录
状态下随便请求一个 www.pkulaw.com 页面（比如 `/case?way=topGuid`），服务端会在
返回的 HTML 里塞一份 `<input type="hidden" id="access_token" value="...">`，
是当前这一刻服务端自己生成、确定有效的 token，不依赖任何一次性凭证。
`login_interactive`/`get_session`（密码登录分支）/`get_access_token`（缓存里
完全没有旧 token 时的兜底）现在都统一用这个方法，已实测可靠。
`exchange_code_for_token` 函数还留着（记录这条路径本身的探索结论），但不建议
依赖它。

## 密码登录 + 退出登录（真实抓包确认，2026-07-30）

用户操作"退出登录 -> 手机号+密码重新登录"，抓包确认了两条链路，都已实现并且都
**用我们自己的代码真实跑通过**：

**退出登录**：
```
GET https://www.pkulaw.com/logout/?ReturnUrl=<url>
-> 302 -> GET https://cas.pkulaw.com/auth/realms/fabao/sms/remove-sessions/{session_state}?redirect_uri=<url>
-> 302 -> GET <url>
```
`session_state` 是服务端自己从当前请求带的 cookie 里读出来拼进 Location 头的，
不需要客户端自己传。全程是标准 302，`requests` 默认 `allow_redirects=True` 就能
自动跟完，不需要执行 JS。`auth.logout(session)` 实现了这个；`login.logout(identifier)`
包了一层：调完 `auth.logout` 后把 Redis 里缓存的 session 也删掉，避免下次
`get_session()` 命中一个服务端已经失效的缓存。

**密码登录**：手机号和邮箱是**同一个表单**（登录页面 HTML 里 `id="kc-form-login"`，
不带数字后缀），核心难点是密码字段的加密——从登录页内联 `<script>` 里的
`encryption()` 函数原样翻译出来的，**已用真实抓包的 (密码, encryptionKey, 密文)
三元组验证完全匹配**：
```js
function encryption(sourceword) {
    var keyStr = $('#encryptionKey').val();               // 服务端每次登录页面下发，32字符
    var key = CryptoJS.enc.Utf8.parse(keyStr);             // 原始UTF-8字节，32字节=AES-256
    var iv = CryptoJS.enc.Utf8.parse("5485693214587452");  // 固定写死的IV，不是每次变的
    var encrypted = CryptoJS.AES.encrypt(CryptoJS.enc.Utf8.parse(sourceword), key, {
        iv: iv, mode: CryptoJS.mode.CBC, padding: CryptoJS.pad.Pkcs7,
    });
    return encrypted.ciphertext.toString();  // 密文的十六进制
}
```
`auth.encrypt_password(password, encryption_key)` 就是这个函数的 Python 版本。
`encryptionKey` 本身也是从登录页面 HTML 里解析出来的（`get_cas_session()` 已经
顺带解析进 `login_ctx["encryption_key"]`，跟 `session_code`/`execution`/`tab_id`
一样不用额外请求）。

完整提交（真实抓包确认字段）：
```
GET  cas.pkulaw.com/.../sms/check-username-login?username=X&password=<加密后>&encryptionKey=Y
     # 登录页JS在真正提交表单前的条件判断，照抄这个前置检查
POST cas.pkulaw.com/.../login-actions/authenticate?session_code=...&execution=...&tab_id=...
     body: loginType=1&tabType=passValidate&redirect_uri=...&source=&encryptionKey=Y
           &password=<加密后>&email-phone=X&passwordFront=<明文>
     # 注意 password(加密) 和 passwordFront(明文) 都传了，没细究服务端到底靠哪个校验，两个都带最稳妥
```
后面两跳授权码兑换跟验证码登录是同一个机制，`auth.complete_login_by_password()`
逻辑照抄 `complete_login`。`auth.password_login(identifier, password)` 是完整入口
（从头建 session -> get_cas_session -> complete_login_by_password），
`login.get_session()` 在账号有密码时会自动走这条路。

## 拿已登录的浏览器 session 直接喂给我们的代码（调试/测试用的捷径）

不想每次测试都真走一遍注册/登录（要发短信/邮件、要真人输入验证码）时，可以直接把
浏览器里已经登录的账号 cookie + access_token 喂给 `login.save_session()`，跳过登录
这一步：

```python
import requests
from platforms.pkulaw import auth, login

session = requests.Session()
session.headers.update({"user-agent": config.UA})
session.cookies.update(cookies)   # 浏览器里那份完整 cookie（见下面"要拿全"的坑）
assert auth.is_logged_in(session)  # 确认真的有效
login.save_session(identifier, session, access_token=access_token)  # 存进 Redis，后面 get_session() 就会直接命中缓存
```

**坑**：`get_storage(type="cookies")` 这个工具**只能拿到当前页面 origin 下非
HttpOnly、且这个 tab 实际发生过写入的那部分 cookie**，很容易漏掉关键字段（比如
`pkulaw_v6_sessionid`/`userislogincookie`/`LoginAccount` 这几个 `auth.is_logged_in()`
真正依赖的字段，实测就漏过一次，导致第一次这么干的时候 `is_logged_in()` 是
`False`）。**更可靠的做法是直接从一次真实网络请求的 Request Header 里的
`cookie:` 字段整段抄**（`get_network_request` 工具能看到，这是浏览器实际发出去的、
包含 HttpOnly cookie 在内的完整值）；access_token 也一样，与其自己去走一遍
`exchange_code_for_token`，不如直接从任意一个已登录页面（比如
`www.pkulaw.com/case?way=topGuid`）返回的 HTML 里那个
`<input type="hidden" id="access_token" value="...">` 隐藏字段抄一份现成的、
当前仍然有效的。

这么接管进来的账号如果要在 `core.account_registry` 里补一条完整记录（不只是
`login.set_password()` 顺手写的那个只有 `password` 字段的残缺记录），要自己补一次
`account_registry.save_account(platform, identifier, login._NO_EXPIRY, {"password": ...})`
——`save_session()` 本身不会自动创建账号记录，那是 `login_interactive()` 里
`_save_account_if_new()` 才做的事，直接接管 session 跳过了这一步。

## 代理池 + 账号池调度（2026-07-30）

跟 wkinfo 平台是同一套 `core.proxy_pool`/`core.account_registry`/
`core.quota_tracker`/`core.scheduler`，接入方式也是同一个模式（"账号绑定一个固定
代理，终身复用"，见 `platforms/wkinfo/HANDOFF.md`），这里记 pkulaw 侧的接入细节：

- `login._ensure_proxy_bound(identifier)`：`login_interactive()`（验证码登录/注册）
  和 `get_session()` 的密码登录分支都会先调这个，账号没绑过代理就从
  `core.proxy_pool.pick_for_new_account(platform, config.MAX_ACCOUNTS_PER_IP)`
  挑一个绑上；代理池为空或都满了直接抛异常，不会静默直连。
- `login._proxied_session(identifier)`：建 session 时自动查这个账号绑的代理并用上，
  包括从 Redis 缓存恢复 session 时（`_session_from_cookies` 也会重新挂上代理）—— 
  跟 wkinfo 平台一样，同一个账号一辈子只用一个 IP，不会中途换。
- **这个平台目前没有观察到明确的访问次数限制**（不像 wkinfo 确认过
  40次/20次那种硬限额），`config.DETAIL_LIMIT_PER_DAY = 5000` 是我们自己定的
  "每个账号每天最多主动跑多少次"上限，不是照服务端实测出来的真实限额，可以按需
  调整。
- `detail_client.view_detail_pooled(category, doc_id)`：不指定账号，走
  `core.scheduler.dispatch` 自动从账号池里挑一个还有配额的账号（挑号、记配额、
  记流水的逻辑照抄 wkinfo `search_client._dispatch_and_call`）。跟原来那个
  `view_detail(category, doc_id, identifier)`（指定账号，不走配额/账号池，适合
  调试）是两个互不影响的入口，都缓存优先。

**已用真实请求验证过代理池确实生效**：`crawler/pkulaw_account.py`（见下）跑完后，
账号 17717295039 被自动绑定到了真实代理（`byjsnode133.vpsnb.net`，用户提供的3个
代理之一），后续步骤（改密码、退出、重新登录、再请求详情页）全部通过这个代理发出。

## 验收脚本 `crawler/pkulaw_account.py`

在 `crawler/` 目录下直接跑（不是包内模块，不需要 `-m`）：
```bash
cd /Users/houjie/Desktop/ai_code/mcp_js/claude_code/crawler
/Users/houjie/venv/python3-forcrawl/bin/python pkulaw_demo.py
```
完整走一遍：手机验证码登录 -> 校验登录态+请求详情页 -> 手机验证码改密码 ->
退出登录 -> 账号+密码重新登录 -> 校验登录态+强制重新请求详情页。中间两处需要
真人输入短信验证码（`input()`），其他全部自动。用的测试账号是 17717295039，
详情页是 `https://www.pkulaw.com/chl/7105c80db9a10f98bdfb.html`
（`chl` 这个类目之前没测过，`document_store.parse_url` 的通用正则能处理任意
类目名，不需要为 `chl` 专门加代码）。

## 验证码发送节流（`core/code_throttle.py`，所有平台共用）

真实撞到过 `{"error":"limit_minute"}`（手机号登录发一次验证码，紧接着改密码又
要发一次，间隔太短触发）之后加的：`auth.send_email_code`/`send_phone_code` 现在
调用前都会先过 `core.code_throttle.wait_before_send(platform, identifier,
config.CODE_SEND_MIN_INTERVAL_SECONDS)`——距上次给这个账号发验证码不到75秒
（`config.CODE_SEND_MIN_INTERVAL_SECONDS`）就打日志说明还要等多少秒，然后真的
`sleep` 那么久再继续，主动防住这个限流，不是被动等服务端拒绝了再处理。

这是 `core/` 下的通用能力（不是 pkulaw 专属），Redis key 按 `platform` 分区，
其他平台（比如 wkinfo，如果以后哪个环节也要发验证码）可以直接复用，不用重新实现。
用法是固定的三步：`wait_before_send()` -> 真的调发送接口 -> `mark_sent()`
（`send_email_code`/`send_phone_code` 内部已经按这个顺序接好了，调用方不需要
自己操心）。

## 待办 / 已知缺口

- `search`（列表检索）还没有逆向，`platform.py` 里调用会直接抛
  `NotImplementedError`。
- `detail_client.view_detail`/`view_detail_by_url`（指定账号那条路）等
  `crawler/pkulaw_account.py` 跑完就有真实端到端验证了；`view_detail_pooled`
  （账号池自动调度那条路）目前还没真实跑过——只过了 import 检查，池子里目前也
  只有 17717295039 一个账号，等有第二个账号了应该测一下"配额用完自动换下一个
  账号"这个分支。
- 邮箱版改密码（`modify_password_by_email`）的**成功**路径还没真实验证过（只验证过
  "邮箱不存在"这个失败路径的请求体形状是对的）；邮箱版密码登录
  （`complete_login_by_password`）逻辑上跟手机号是同一个表单/同一份代码，但也还没
  单独真实测过——下次找一个真实存在、当前登录的邮箱账号一起测一次。
- 邮箱注册转人工审核那条路（`RegistrationPendingReview`）只验证了"卡住"这一半，
  "真人回复邮件 -> 审核通过 -> 收到成功邮件 -> 重新登录成功"这个完整循环还没
  真的走完过一次。
- `config.DETAIL_LIMIT_PER_DAY = 5000` 是我们自己定的上限，不是服务端实测出来的
  真实限额（这个平台目前没观察到明确的访问次数限制）——如果哪天真的撞到限流，
  记得把真实的错误码/消息文本补进 `detail_client.py`，跟 wkinfo 平台确认过
  `E_010_015` 那样精确识别，而不是只能靠这个数字硬顶。

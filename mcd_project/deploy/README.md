# Windows 服务器部署 captcha_login_worker

在一台**全新的 Windows 服务器**上把 `mcd_project/captcha_login_worker.py` 跑起来。

---

## 一、跑之前要准备什么

| 项 | 要求 | 说明 |
|---|---|---|
| 系统 | Windows 10 / Server 2019 及以上 | 需要能装 Chrome（Server Core 无桌面版跑不了有头，无头也缺依赖，建议带桌面体验） |
| 权限 | 管理员 | 装软件、开 OpenSSH 功能需要 |
| 网络 | 能访问 pypi 与 github.com | 国内机器见第五节「GitHub 连不上」 |
| 账号 | 有 `HLFYY/claude_code` 仓库权限 | 用来加 Deploy key |

脚本会自动处理：Git、Python 3.12、Chrome、OpenSSH 客户端、venv、依赖。

> **注意**：脚本本身也在仓库里，所以第一次要先把它弄到服务器上——直接复制粘贴这两个文件的内容即可：
> `deploy/setup.bat` 和 `deploy/setup_windows.ps1`。

---

## 二、三步部署

### 第 1 步：跑脚本

把两个文件放在同一个目录（比如 `C:\setup\`），然后：

- 双击 `setup.bat`
- 或右键 `setup.bat` → **以管理员身份运行**（推荐）

### 第 2 步：把公钥加到 GitHub

脚本会在阶段 2 把公钥打印出来，并**自动复制到剪贴板**。两种情况：

| 情况 | 脚本行为 |
|---|---|
| 已有 `~/.ssh/id_ed25519.pub` 或 `id_rsa.pub` | 直接展示现有公钥，**不会覆盖** |
| 一个都没有 | 生成 `id_ed25519`（无口令）后再展示 |

然后手动操作一次：

1. 打开 <https://github.com/HLFYY/claude_code/settings/keys>
2. **Add deploy key**
3. Title 随便（如 `win-server`）
4. Key 粘贴公钥（**整行**，从 `ssh-ed25519` 或 `ssh-rsa` 开头到结尾）
5. **Allow write access 不用勾**（只读拉代码足够）
6. Add key 保存
7. 回到服务器终端，**按回车**

### 第 3 步：等它自己跑完

按回车后脚本自动：验证 SSH → clone 代码 → 建 venv → 装依赖 → 验 Redis → 生成启动脚本。

结束时你会看到：

```
代码目录   C:\claude_code
Python     C:\claude_code\mcd_project\.venv\Scripts\python.exe
SSH key    C:\Users\<你>\.ssh\id_ed25519
```

---

## 三、部署完怎么跑

| 目标 | 命令 |
|---|---|
| 正常跑（无头） | `C:\claude_code\run_worker.bat` |
| 带界面看浏览器 | `C:\claude_code\run_worker_debug.bat` |
| 直接调 Python | `C:\claude_code\mcd_project\.venv\Scripts\python.exe captcha_login_worker.py` |

### 注册成开机自启（可选，推荐 NSSM）

```cmd
winget install NSSM.NSSM
nssm install mcd-captcha-worker "C:\claude_code\mcd_project\.venv\Scripts\python.exe" "C:\claude_code\mcd_project\captcha_login_worker.py"
nssm set mcd-captcha-worker AppDirectory "C:\claude_code\mcd_project"
nssm set mcd-captcha-worker AppStdout "C:\Users\<你>\logs\worker.log"
nssm set mcd-captcha-worker AppStderr "C:\Users\<你>\logs\worker.err.log"
nssm set mcd-captcha-worker AppRotateFiles 1
nssm start mcd-captcha-worker
```

---

## 四、脚本到底做了什么（阶段表）

| 阶段 | 动作 | 幂等性 |
|---|---|---|
| 1 | 检查/安装 OpenSSH、Git、Python 3.12、Chrome | 已存在则跳过 |
| 2 | 查 SSH key：有则展示，无则 `ssh-keygen -t ed25519` 生成后展示 | 有 key 绝不覆盖 |
| 3 | 暂停等你加 Deploy key（加 `-SkipPause` 可跳过） | — |
| 4 | `ssh -T` 验证 → `git clone` 或 `git pull` → 设仓库级 `core.sshCommand` | 仓库已存在就 pull |
| 5 | `python -m venv` → 装依赖 → 校验模块可导入 | venv 已存在就复用 |
| 6 | 连 Redis ping 一次，打印队列长度 | 只读检测 |
| 7 | 生成 `run_worker.bat` / `run_worker_debug.bat` | 覆盖重写 |

### 装了哪些依赖

```
flask==3.0.0  requests==2.31.0  redis==5.0.1
pycryptodome==3.19.0  schedule==1.2.0
DrissionPage>=4.0.4,<5   ddddocr>=1.4.11   Pillow>=10.0
```

两点说明：

- **没装 gunicorn**：Windows 不支持，worker 也不需要它（它只用 Flask 的时候才需要，worker 不用）。
- **仓库的 `requirements.txt` 里没有 DrissionPage / ddddocr / Pillow**，但 worker 实际依赖它们。脚本里单独装了，别只跑 `pip install -r requirements.txt`。

---

## 五、常见问题

### 1. GitHub 连不上（22 端口）

现象：`ssh: connect to host github.com port 22: Connection timed out`

国内服务器常见。脚本会识别并提示。改走 443：

在 `C:\Users\<你>\.ssh\config` 写：

```
Host github.com
    HostName ssh.github.com
    Port 443
    User git
```

验证：`ssh -T git@github.com`，看到 `successfully authenticated` 就行。

### 2. `Permission denied (publickey)`

按概率排：

1. 公钥加到了**账号级 SSH keys**（`settings/keys`）而不是**仓库级 Deploy keys**（`仓库/settings/keys`）
2. 复制公钥时**漏了前缀或末尾**——必须一行完整
3. 加完 key 没点 Add key 保存
4. 服务器上有多个 key，ssh 用了错的那个 → 脚本已用 `-i` + `IdentitiesOnly=yes` 强制指定

### 3. `10048` 端口占用 / winget 装不上

脚本用 winget 装 Git/Python/Chrome。若报 `winget 不是内部或外部命令`，说明系统没 App Installer（Server 常见）。手动装：

- Git：<https://git-scm.com/download/win>
- Python 3.12：<https://www.python.org/downloads/windows/>（**勾 Add python.exe to PATH**）
- Chrome：<https://www.google.cn/chrome/>

装完重跑脚本即可（幂等）。

### 4. 依赖装到一半失败

国内网络加个源重跑：

```powershell
powershell -ExecutionPolicy Bypass -File setup_windows.ps1 -PipIndex https://pypi.tuna.tsinghua.edu.cn/simple
```

### 5. `ModuleNotFoundError: No module named 'onnxruntime'` / ddddocr 报错

ddddocr 依赖 onnxruntime，3.13 上可能没有 wheel。装 Python 3.12 重建 venv：

```powershell
Remove-Item C:\claude_code\mcd_project\.venv -Recurse -Force
# 装好 3.12 后重跑脚本
```

### 6. Redis 连不上

`config.py` 默认连 `db.selfmediaai.cn:26379`。要换就用环境变量，不改代码：

```powershell
setx MCD_REDIS_HOST 127.0.0.1
setx MCD_REDIS_PORT 6379
setx MCD_REDIS_DB 0
setx MCD_REDIS_PASSWORD ""
```

`setx` 之后要**重开终端**才生效。

### 7. 过滑块一直失败

无头模式下先看现场取证：

```
C:\Users\<你>\datas\mcd\captcha_debug\
```

里面有截图和 HTML。常见原因：

- Chrome 没装好 / 版本太老 → 更新 Chrome
- 代理连不通 → 看日志里的 `打开数美页面失败`
- `deliverNativeParams 15s 未就绪` → 数美 SDK 没加载完，多半是代理太慢

想直接看浏览器就改用 `run_worker_debug.bat`（`--show`）。

---

## 六、脚本参数

```powershell
powershell -ExecutionPolicy Bypass -File setup_windows.ps1 `
    -RepoUrl "git@github.com:HLFYY/claude_code.git" `
    -TargetDir "D:\code\claude_code" `
    -PipIndex "https://pypi.tuna.tsinghua.edu.cn/simple" `
    -SkipPause `
    -SkipChrome
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-RepoUrl` | `git@github.com:HLFYY/claude_code.git` | 仓库地址 |
| `-TargetDir` | `C:\claude_code` | clone 到哪 |
| `-PipIndex` | 空 | pip 镜像源 |
| `-SkipPause` | 关 | 跳过「等加 key」的暂停（已加过 key 时用） |
| `-SkipChrome` | 关 | 不检查/安装 Chrome |

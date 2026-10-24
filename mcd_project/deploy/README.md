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

脚本会自动处理：Git、Python **3.9.13**、Chrome、OpenSSH 客户端、Python 依赖。

> **版本严格对齐 Mac 环境**：目标是与 `/Users/houjie/venv/python3-forcrawl`（Python 3.9.17）
> **完全一致的包版本**，清单见 `deploy/requirements-win-py39.txt`
> （**36 个包**，只含 `captcha_login_worker.py` 的导入闭包，全部钉死版本）。
> Windows 上装 python.org 最后一个带安装包的 3.9.13 —— cp39 ABI 与 3.9.17 相同，所有 cp39 wheel 通用。

> **注意**：脚本本身也在仓库里，所以第一次要先把它弄到服务器上——直接复制粘贴这三个文件的内容即可：
> `deploy/setup.bat`、`deploy/setup_windows.ps1`、`deploy/requirements-win-py39.txt`
> （**三个都要**，第三个是版本锁定清单；漏了脚本会去仓库里的 `mcd_project\deploy\` 找）。
>
> **代码默认拉到「脚本所在目录」下的 `claude_code\`**（例如脚本在 `C:\setup\`，代码就在 `C:\setup\claude_code\`）。
> 如果脚本本身就在已经 clone 好的仓库里跑（比如在 `claude_code\mcd_project\deploy\` 下双击），
> 会**直接复用那个仓库**，不会再多套一层。
> 想换位置用 `-TargetDir D:\code\claude_code`。
>
> **默认不创建虚拟环境**，依赖直接装到系统 Python。想用 venv 就加 `-UseVenv`。

---

## 二、三步部署

### 第 1 步：跑脚本

把两个文件放在同一个目录（比如 `C:\setup\`），然后：

- 双击 `setup.bat`
- 或右键 `setup.bat` → **以管理员身份运行**（推荐）

### 第 2 步：把公钥加到 GitHub

脚本会在阶段 4 把公钥打印出来，并**自动复制到剪贴板**。两种情况：

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

按回车后脚本自动：验证 SSH → clone 代码 → 装依赖 → 验 Redis → 生成启动脚本。

结束时你会看到（路径以脚本放在 `C:\setup\` 为例）：

```
代码目录   C:\setup\claude_code
Python     C:\Program Files\Python39\python.exe  (3.9.13)
SSH key    C:\Users\<你>\.ssh\id_ed25519
依赖快照   C:\setup\claude_code\pip_freeze_windows.txt
```

---

## 三、部署完怎么跑

| 目标 | 命令 |
|---|---|
| 正常跑（无头） | `C:\setup\claude_code\run_worker.bat` |
| 带界面看浏览器 | `C:\setup\claude_code\run_worker_debug.bat` |
| 直接调 Python | `cd C:\setup\claude_code\mcd_project` 然后 `python captcha_login_worker.py` |

### 注册成开机自启（可选，推荐 NSSM）

```cmd
winget install NSSM.NSSM
nssm install mcd-captcha-worker "C:\Program Files\Python39\python.exe" "C:\setup\claude_code\mcd_project\captcha_login_worker.py"
nssm set mcd-captcha-worker AppDirectory "C:\setup\claude_code\mcd_project"
nssm set mcd-captcha-worker AppStdout "C:\Users\<你>\logs\worker.log"
nssm set mcd-captcha-worker AppStderr "C:\Users\<你>\logs\worker.err.log"
nssm set mcd-captcha-worker AppRotateFiles 1
nssm start mcd-captcha-worker
```

---

## 四、脚本到底做了什么（阶段表）

| 阶段 | 动作 | 幂等性 |
|---|---|---|
| 1 | 检查/安装 OpenSSH、Git | 已存在则跳过 |
| 2 | **清理旧 Python 环境**：卸载 registry 里所有非 3.9 的 Python、删残留目录、删旧 `.venv` | 保留 3.9.x，`-NoPurge` 可跳过 |
| 3 | 复用已有 3.9.x，否则装 **Python 3.9.13**；把该解释器的第三方包全部卸干净 | 重复执行等于「重装环境」 |
| 4 | 查 SSH key：有则展示，无则 `ssh-keygen -t ed25519` 生成后展示 | 有 key 绝不覆盖 |
| 5 | 暂停等你加 Deploy key（加 `-SkipPause` 可跳过） | — |
| 6 | `ssh -T` 验证 → `git clone` 或 `git pull` → 设仓库级 `core.sshCommand` | 仓库已存在就 pull |
| 7 | 对齐 `pip==26.0.1`/`setuptools==60.5.0` → 按锁定清单装依赖 → **逐包比对版本** → 导出 `pip_freeze_windows.txt` | 已装好的包不重装 |
| 8 | 连 Redis ping 一次，打印队列长度 | 只读检测 |
| 9 | 检查 Chrome/Edge，生成 `run_worker.bat` / `run_worker_debug.bat` | 覆盖重写 |

### 装了哪些依赖

**只装 `captcha_login_worker.py` 真正用到的**：从它出发、只跟模块级 import 递归求闭包
（本地模块 → `config` / `login_manager` / `mcd_api` / `mcd_shumei_d` / `proxy_pool` /
`proxy_failure_handler` / `account_proxy_binding` / `ip_location` / `utils` / `exceptions`），
第三方顶层只有 6 个，再展开传递依赖，一共 **36 个包**，全部钉死版本：
见 **`deploy/requirements-win-py39.txt`**。

```
直接依赖   redis 3.2.1      requests 2.31.0     Pillow 9.5.0
           pycryptodome 3.9.9                 DrissionPage 4.0.4.22
           ddddocr 1.4.7

过渡依赖   lxml 6.0.2       cssselect 1.2.0     tldextract 5.1.3
           click 8.1.8      colorama 0.4.6      psutil 7.2.2
           websocket-client 1.9.0              DownloadKit 2.0.5
           DataRecorder 3.4.14                 openpyxl 3.0.9
           onnxruntime 1.15.1                  numpy 1.24.0
           opencv-python-headless 4.7.0.72     protobuf 6.33.6
           sympy 1.12       packaging 20.9      certifi / idna / urllib3 …
```

**刻意不装**（worker 用不到，装了只是拖慢和引入冲突）：

| 包 | 为什么不需要 |
|---|---|
| flask / werkzeug / jinja2 / markupsafe / itsdangerous | 那是 `mcd_server.py` 用的，Windows 只跑 worker |
| schedule | `scheduler.py` 用的 |
| paramiko / bcrypt / cryptography / pynacl / cffi / pycparser | 只有 `deploy/remote_run.py` 用 |
| frida | PyPI 只发 `py3.9-win-amd64.egg`，pip 装不上；且 `device_builder` 是 `device_pool_redis` 里**函数内的惰性导入**，不在 worker 运行链上 |
| gunicorn | Windows 不支持 |

> `openpyxl` 看着像无关的，其实是 DrissionPage → DataRecorder 的硬依赖，必须留。

与 Mac 环境的偏差只有 **2 处**（其余 34 个逐版本一致）：

| 包 | Mac | Windows | 原因 |
|---|---|---|---|
| pycryptodome | 3.9.8 | 3.9.9 | 3.9.8 最高只有 cp38 的 win wheel（Mac 那份是本地源码编译的） |
| pyreadline3 | 无 | 3.5.4 | humanfriendly 的 Windows-only 依赖，Mac 上不存在 |

> 仓库的 `requirements.txt` 是**服务器（树莓派）**用的旧清单，与 Windows worker 无关，别拿它装 Windows 环境。

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

### 3. winget 卡死 / 装不上 Git、Python

脚本**已经不用 winget** 装 Git 和 Python 了（winget 在国内会卡在 GitHub CDN 的 0 字节），改成华为云镜像直下：

| 组件 | 镜像 |
|---|---|
| Git | `mirrors.huaweicloud.com/git-for-windows/`（解析 `.index.json` 取最新版） |
| Python | `mirrors.huaweicloud.com/python/3.9.13/`（3.9.13 是 python.org **最后一个带 Windows 安装包**的 3.9 版本） |

winget 只在镜像全挂时兜底。若连镜像也下不动，手动装：

- Git：<https://mirrors.huaweicloud.com/git-for-windows/>
- Python：<https://mirrors.huaweicloud.com/python/3.9.13/>（**勾 Add python.exe to PATH**）
- Chrome：<https://www.google.cn/chrome/>

装完重跑脚本即可（幂等）。

### 4. 依赖装到一半失败

国内网络加个源重跑：

```powershell
powershell -ExecutionPolicy Bypass -File setup_windows.ps1 -PipIndex https://pypi.tuna.tsinghua.edu.cn/simple
```

### 5. `ImportError: DLL load failed ... onnxruntime_pybind11_state`

**不是缺 MSVC 运行库**（以前这么判是错的），真实原因基本只有两个：

1. Python 不是 3.9 —— `onnxruntime==1.15.1` 只有 cp39 的 wheel；
2. 装成了别的 onnxruntime 版本。

处理：让脚本重跑一次（它会把非 3.9 的 Python 卸掉、依赖全部重装）：

```powershell
powershell -ExecutionPolicy Bypass -File setup_windows.ps1 -SkipPause
```

自检：

```cmd
python -V                                  :: 必须是 3.9.x
python -c "import onnxruntime; print(onnxruntime.__version__)"   :: 必须是 1.15.1
```

### 5.1 想确认「和 Mac 是否一致」

脚本跑完会导出 `pip_freeze_windows.txt`。与 Mac 环境对比：

```bash
# Mac 上执行
/Users/houjie/venv/python3-forcrawl/bin/python -m pip freeze > pip_freeze_mac.txt
```

两份文件按包名逐行比对即可：除第四节列出的 2 处偏差（pycryptodome 3.9.8→3.9.9、pyreadline3 仅 Windows）+ 不装 flask/schedule/paramiko/frida/gunicorn 之外，应完全一致（34/36 逐版本相同）。脚本自己的校验在阶段 7 末尾直接打印「不符」列表。

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
| `-TargetDir` | `<脚本所在目录>\claude_code` | clone 到哪 |
| `-PipIndex` | `https://pypi.tuna.tsinghua.edu.cn/simple` | pip 镜像源（传 `""` 关掉） |
| `-SkipPause` | 关 | 跳过「等加 key」的暂停（已加过 key 时用） |
| `-SkipChrome` | 关 | 不检查 Chrome/Edge |
| `-UseVenv` | 关 | 在 `mcd_project\.venv` 建虚拟环境并装进去 |
| `-FullClone` | 关 | 完整 clone（含 `mcd/resources` 约 130MB，很慢） |
| `-NoPurge` | 关 | **不**卸载旧 Python、不清理旧依赖（只想补装时才用） |

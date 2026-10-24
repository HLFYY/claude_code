#Requires -Version 5.1
<#
.SYNOPSIS
    mcd_project / captcha_login_worker.py  Windows 全新服务器一键部署
    Python 与所有依赖版本严格对齐 Mac 环境 /Users/houjie/venv/python3-forcrawl

.DESCRIPTION
    阶段一  环境自检 + 安装 Git / OpenSSH
            （全部优先走国内镜像：华为云 git-for-windows、清华 PyPI）
    阶段二  清理旧 Python 环境（默认执行，-NoPurge 可跳过）
            · 卸载 registry 里所有非 3.9 的 Python（3.12 / 3.11 / 3.13 …）
            · 删除残留目录（Program Files\Python3xx、%LOCALAPPDATA%\Programs\Python\Python3xx、
              %APPDATA%\Python、pip 缓存）
            · 保留已存在的 3.9.x 解释器，只把它的第三方包全部卸干净（等同于「重装环境」）
            · 删掉旧的 .venv
    阶段三  安装 Python 3.9.13（python.org 最后一个 Windows 安装版；cp39 ABI 与 Mac 的
            3.9.17 完全一致，wheel 通用），静默装到 Program Files 并加 PATH
    阶段四  SSH key：已存在就直接展示，不存在就生成后展示
    阶段五  暂停，等你把公钥加进 GitHub Deploy keys，回车继续
    阶段六  验证 SSH 连接 -> 稀疏 clone（只取 mcd_project，约 1.2MB）
    阶段七  按 deploy/requirements-win-py39.txt 精确装机（36 个包，全部指定版本；
            只含 captcha_login_worker 的导入闭包，不装 flask / schedule / paramiko 等）
            + 逐包比对版本，打印偏离项
    阶段八  验证 Redis 连通
    阶段九  生成 run_worker.bat

    默认代码目录（-TargetDir 可改）：
      · 脚本被单独复制到一个空目录 → 代码 clone 到「脚本所在目录\claude_code」
      · 脚本本身就在已 clone 的仓库里 → 直接复用那个仓库，不再嵌套 clone
    默认不创建虚拟环境，直接用系统 Python（加 -UseVenv 才建 .venv）

    幂等设计：重复执行安全（key 不覆盖、仓库存在则 pull、已装好的包不重装）。

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File setup_windows.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File setup_windows.ps1 -Yes

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File setup_windows.ps1 -NoPurge -TargetDir D:\code\claude_code
#>
[CmdletBinding()]
param(
    [string] $RepoUrl   = "git@github.com:HLFYY/claude_code.git",
    [string] $TargetDir = "",
    [string] $PipIndex  = "https://pypi.tuna.tsinghua.edu.cn/simple",
    [switch] $SkipPause,
    [switch] $SkipChrome,
    [switch] $FullClone,
    [switch] $UseVenv,
    [switch] $NoPurge
)

# 默认把代码拉到「脚本所在目录」下面，不再固定 C:\claude_code
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }

function Find-GitRoot([string]$dir) {
    $d = $dir
    while ($d) {
        if (Test-Path (Join-Path $d ".git")) { return $d }
        $parent = Split-Path $d -Parent
        if (-not $parent -or $parent -eq $d) { return $null }
        $d = $parent
    }
    return $null
}

if (-not $TargetDir) {
    # 情况 A：脚本本身就在已 clone 好的仓库里（如在 mcd_project\deploy\ 下双击）→ 直接复用，不再嵌套 clone
    $gitRoot = Find-GitRoot $ScriptDir
    if ($gitRoot -and (Test-Path (Join-Path $gitRoot "mcd_project\captcha_login_worker.py"))) {
        $TargetDir = $gitRoot
    }
    # 情况 B：脚本是单独复制到某个目录的 → 代码拉到该目录下的 claude_code\
    else {
        $TargetDir = Join-Path $ScriptDir "claude_code"
    }
}
$TargetDir = [IO.Path]::GetFullPath($TargetDir)

# 目标版本（与 Mac 对齐）
$TargetPyShort = "3.9"
$TargetPyFull  = "3.9.13"     # python.org 最后一个带 Windows 安装包的版本
$TargetPipVer  = "26.0.1"     # 与 Mac 的 pip 一致
$TargetSetuptoolsVer = "60.5.0"

$ErrorActionPreference = "Continue"

# 国内服务器：强制 TLS1.2，否则 PS 5.1 的 Invoke-WebRequest 在国内镜像上会握手失败
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

# ---------------- 输出helper ----------------
function Step ($n, $m) {
    Write-Host ""
    Write-Host ("  " + ("-" * 62)) -ForegroundColor DarkGray
    Write-Host ("  阶段 $n  " + $m) -ForegroundColor Cyan
    Write-Host ("  " + ("-" * 62)) -ForegroundColor DarkGray
}
function Ok   ($m) { Write-Host ("  [OK]   " + $m) -ForegroundColor Green }
function Info ($m) { Write-Host ("         " + $m) -ForegroundColor Gray }
function Warn ($m) { Write-Host ("  [WARN] " + $m) -ForegroundColor Yellow }
function Fail ($m) { Write-Host ("  [FAIL] " + $m) -ForegroundColor Red }
function Die  ($m) { Fail $m; Write-Host ""; Write-Host "  已中止。修好后重新执行本脚本即可（幂等）。" -ForegroundColor Yellow; exit 1 }

function Refresh-Path {
    $m = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $u = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = ($m, $u, "$env:USERPROFILE\.local\bin") -join ";"
}
function Has-Exe ($name) { return [bool](Get-Command $name -ErrorAction SilentlyContinue) }

# ---------------- 国内镜像直下（绕开 winget / GitHub CDN） ----------------
$MirrorGitIndex    = "https://mirrors.huaweicloud.com/git-for-windows/.index.json"
$MirrorGitBase     = "https://mirrors.huaweicloud.com/git-for-windows"
$MirrorGitFallback = "https://mirrors.huaweicloud.com/git-for-windows/v2.56.0.windows.2/Git-2.56.0.2-64-bit.exe"
$MirrorPyBase      = "https://mirrors.huaweicloud.com/python"

function Get-MirrorFile {
    param([string] $Url, [string] $OutFile, [string] $Label = "文件")

    Info ("下载 " + $Label + " ...")
    Info ("  " + $Url)
    $oldProgress = $ProgressPreference
    $ProgressPreference = "SilentlyContinue"   # 进度条会让 Invoke-WebRequest 慢好几倍
    try {
        for ($i = 1; $i -le 3; $i++) {
            try {
                Invoke-WebRequest -Uri $Url -OutFile $OutFile -UseBasicParsing -TimeoutSec 900
                if (Test-Path $OutFile) {
                    $len = (Get-Item $OutFile).Length
                    if ($len -gt 100000) {
                        Ok ("下载完成 " + [math]::Round($len / 1MB, 1) + " MB")
                        return $true
                    }
                    Warn ("文件过小，可能不是安装包（" + $len + " 字节）")
                }
            }
            catch { Warn ("第 " + $i + " 次下载失败：" + $_.Exception.Message) }
            Start-Sleep -Seconds 2
        }
    }
    finally { $ProgressPreference = $oldProgress }
    return $false
}

function Get-LatestGitUrl {
    # 华为云的版本目录名与 exe 文件名不一致（v2.56.0.windows.2 -> Git-2.56.0.2-64-bit.exe），
    # 所以必须解析 .index.json 拿真实文件名，不能靠字符串拼接
    try {
        $idx = Invoke-RestMethod -Uri $MirrorGitIndex -UseBasicParsing -TimeoutSec 20
        $node = $idx."git-for-windows"
        if ($node) {
            $cands = @()
            foreach ($k in $node.PSObject.Properties.Name) {
                if ($k -notmatch "^v(\d+)\.(\d+)\.(\d+)\.windows\.(\d+)$") { continue }
                $ver = "" + $Matches[1] + "." + $Matches[2] + "." + $Matches[3] + "." + $Matches[4]
                $item = $node.$k
                $exe = $item.files | Where-Object { $_ -match "-64-bit\.exe$" } | Select-Object -First 1
                if (-not $exe) { continue }
                $cands += [pscustomobject]@{ Ver = $ver; Url = ($MirrorGitBase + "/" + $exe) }
            }
            if ($cands.Count -gt 0) {
                $newest = $cands | Sort-Object { [version]$_.Ver } -Descending | Select-Object -First 1
                Info ("镜像最新稳定版 Git：" + $newest.Ver)
                return $newest.Url
            }
        }
    }
    catch { Warn ("解析 Git 镜像索引失败：" + $_.Exception.Message) }
    return $null
}

function Ensure-Ssh {
    # Git for Windows 自带 ssh / ssh-keygen，可作为 OpenSSH 客户端的兜底
    if (Has-Exe "ssh") { return $true }
    foreach ($c in @("$env:ProgramFiles\Git\usr\bin\ssh.exe", "$env:ProgramFiles\Git\bin\ssh.exe")) {
        if (Test-Path $c) {
            $env:Path = (Split-Path $c) + ";" + $env:Path
            Ok ("使用 Git 自带的 ssh：" + $c)
            return $true
        }
    }
    return $false
}

function Install-Git {
    if (Has-Exe "git") { Ok ("Git 已安装 (" + (Get-Command git).Source + ")"); return $true }

    Info "未检测到 Git，改用华为云镜像安装（不走 winget）"
    $url = Get-LatestGitUrl
    if (-not $url) { Warn "取不到最新版本，使用固定版本兜底"; $url = $MirrorGitFallback }

    $exe = Join-Path $env:TEMP "git-setup-x64.exe"
    if (Get-MirrorFile -Url $url -OutFile $exe -Label "Git for Windows") {
        Info "静默安装 Git（约 1~2 分钟）..."
        $p = Start-Process -FilePath $exe -ArgumentList "/VERYSILENT", "/NORESTART", "/SP-", "/NOCANCEL" -Wait -PassThru
        Remove-Item $exe -Force -ErrorAction SilentlyContinue
        Refresh-Path
        if (Has-Exe "git") {
            Ok ("Git 安装完成 (" + (Get-Command git).Source + ")")
            Ensure-Ssh | Out-Null
            return $true
        }
        Warn ("镜像安装未生效（退出码 " + $p.ExitCode + "）")
    }

    Warn "镜像安装失败，回退 winget ..."
    if (Has-Exe "winget") {
        & winget install --id Git.Git -e --source winget `
            --accept-package-agreements --accept-source-agreements `
            --silent --disable-interactivity 2>&1 | Out-Null
        Refresh-Path
        if (Has-Exe "git") {
            Ok "Git 安装完成（winget 兜底）"
            Ensure-Ssh | Out-Null
            return $true
        }
    }
    Fail "Git 安装失败"
    Info "手动下载后重跑本脚本：https://mirrors.huaweicloud.com/git-for-windows/"
    return $false
}

# ---------------- Python 探测 / 清理 / 安装 ----------------
function Get-PyVersion([string]$exe) {
    try {
        $v = (& $exe -c "import sys;print('%d.%d.%d'%sys.version_info[:3])" 2>$null | Out-String).Trim()
        if ($v -match '^3\.\d+\.\d+$') { return $v }
    }
    catch {}
    return $null
}

# 返回 3.9.x 的解释器；优先 Path 上的，其次 python.org 的固定安装位置
function Get-Python39 {
    $cands = @()
    foreach ($c in @("python", "python3")) {
        if (Has-Exe $c) {
            $src = (Get-Command $c).Source
            if ($src -and $src -notmatch 'WindowsApps') { $cands += $src }
        }
    }
    $cands += @(
        "$env:ProgramFiles\Python39\python.exe",
        "${env:ProgramFiles(x86)}\Python39\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python39\python.exe"
    )
    # py 启动器兜底
    if (Has-Exe "py") {
        $pyPath = (& py -3.9 -c "import sys;print(sys.executable)" 2>$null | Out-String).Trim()
        if ($pyPath) { $cands += $pyPath }
    }
    foreach ($p in $cands) {
        if (-not (Test-Path $p)) { continue }
        $v = Get-PyVersion $p
        if ($v -and $v -like "$TargetPyShort.*") { return @{ Exe = $p; Ver = $v } }
    }
    return $null
}

# 从注册表列出所有已安装的 Python
function Get-PythonInstalls {
    $keys = @(
        "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*",
        "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*",
        "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*"
    )
    $all = @()
    foreach ($k in $keys) {
        $all += Get-ItemProperty $k -ErrorAction SilentlyContinue |
            Where-Object { $_.DisplayName -match '^Python\s+3\.\d+\.\d+' -and $_.UninstallString }
    }
    return $all | Sort-Object DisplayVersion -Unique
}

function Uninstall-PythonEntry($entry) {
    $dn  = $entry.DisplayName
    $us  = $entry.UninstallString
    Info ("卸载 " + $dn + " ...")
    if ($us -match 'MsiExec\.exe\s+/[IX]?\s*(\{[0-9A-Fa-f\-]+\})') {
        Start-Process -FilePath "msiexec.exe" -ArgumentList @("/x$($Matches[1])", "/qn", "/norestart") -Wait
        return
    }
    $exe = $us
    if ($exe -match '^\s*"([^"]+)"') { $exe = $Matches[1] }
    elseif ($exe -match '^\s*(\S+\.exe)') { $exe = $Matches[1] }
    if (Test-Path $exe) {
        Start-Process -FilePath $exe -ArgumentList @("/uninstall", "/quiet") -Wait -ErrorAction SilentlyContinue
    }
    else {
        Warn ("找不到卸载程序：" + $us)
    }
}

function Remove-DirSafe([string]$path) {
    if (-not (Test-Path $path)) { return }
    try {
        Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction Stop
        Info ("已删除 " + $path)
    }
    catch { Warn ("删除失败（可能被占用）：" + $path + "  " + $_.Exception.Message) }
}

function Install-Python3913 {
    $url = "$MirrorPyBase/$TargetPyFull/python-$TargetPyFull-amd64.exe"
    $exe = Join-Path $env:TEMP "python-$TargetPyFull-amd64.exe"
    if (-not (Get-MirrorFile -Url $url -OutFile $exe -Label "Python $TargetPyFull")) {
        return $null
    }

    Info "静默安装 Python $TargetPyFull ..."
    # 管理员 → 全用户装到 Program Files；否则退回当前用户装到 %LOCALAPPDATA%\Programs\Python
    $allUsers = 1
    if (-not $isAdmin) { $allUsers = 0; Warn "非管理员：改为当前用户安装" }
    $p = Start-Process -FilePath $exe -Wait -PassThru -ArgumentList @(
        "/quiet", "InstallAllUsers=$allUsers", "PrependPath=1",
        "Include_test=0", "Include_launcher=1", "Shortcuts=0",
        "Include_pip=1", "AssociateFiles=0"
    )
    Remove-Item $exe -Force -ErrorAction SilentlyContinue
    Refresh-Path

    $found = Get-Python39
    if ($found) { Ok ("Python " + $found.Ver + " 安装完成：" + $found.Exe); return $found }
    Warn ("Python $TargetPyFull 安装后仍未检测到（退出码 " + $p.ExitCode + "）")
    return $null
}

# 把某个解释器里的第三方包全部卸掉（保留 pip / setuptools / wheel）
function Clear-SitePackages([string]$vpy) {
    $frozen = (& $vpy -m pip freeze --all 2>$null) | Where-Object {
        $_ -match '==' -and $_ -notmatch '^(pip|setuptools|wheel|distribute|pkg[-_]resources)=='
    }
    if (-not $frozen -or $frozen.Count -eq 0) { Ok "该解释器没有第三方包，无需清理"; return }
    Info ("待卸载 " + $frozen.Count + " 个包 ...")
    $lst = Join-Path $env:TEMP "mcd_purge.txt"
    Set-Content -Path $lst -Value $frozen -Encoding ASCII
    & $vpy -m pip uninstall -y -r $lst 2>&1 | Select-Object -Last 2 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    Remove-Item $lst -Force -ErrorAction SilentlyContinue
    Ok "第三方包已全部卸除"
}

# ========================================================
Write-Host ""
Write-Host "  mcd captcha_login_worker  Windows 部署脚本" -ForegroundColor White
Write-Host ("  目标环境 Python " + $TargetPyFull + "  /  依赖与 Mac venv 完全一致") -ForegroundColor DarkGray
Write-Host ("  主机 " + $env:COMPUTERNAME + "   时间 " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss")) -ForegroundColor DarkGray

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($isAdmin) { Ok "当前是管理员权限" }
else { Warn "当前不是管理员：卸载/安装 Python、打开 OpenSSH 功能可能失败，建议右键「以管理员身份运行」" }

try {
    $os = Get-CimInstance Win32_OperatingSystem -ErrorAction Stop
    Info ("系统: " + $os.Caption + "  " + $os.OSArchitecture + "  " + $os.Version)
}
catch { Info "系统: $([Environment]::OSVersion.VersionString)" }

# ========================================================
Step 1 "环境自检 / 安装基础软件（Git / OpenSSH）"

if (-not (Has-Exe "ssh")) {
    Warn "未检测到 ssh.exe，尝试安装 OpenSSH 客户端 ..."
    if ($isAdmin) {
        try {
            Add-WindowsCapability -Online -Name OpenSSH.Client~~~~0.0.1.0 -ErrorAction Stop | Out-Null
            Ok "已通过 Windows 功能安装 OpenSSH 客户端"
        }
        catch { Warn "Windows 功能安装失败：$($_.Exception.Message)" }
        Refresh-Path
    }
    if (-not (Has-Exe "ssh")) {
        # 不再走 winget（Microsoft.OpenSSH.Boot 同样从 GitHub 拉，国内会卡住）
        # Git for Windows 自带 ssh / ssh-keygen，等会儿装完 Git 自动兜底
        Warn "OpenSSH 客户端暂不可用，稍后用 Git 自带的 ssh.exe 兜底"
    }
}
if (Has-Exe "ssh") { Ok "OpenSSH 就绪 ($((Get-Command ssh).Source))" }

if (-not (Install-Git)) { Die "缺少 Git" }

# ========================================================
Step 2 "清理旧 Python 环境（对齐到 Python $TargetPyShort）"

if ($NoPurge) {
    Warn "已指定 -NoPurge，跳过旧 Python / 旧依赖清理"
}
else {
    $installs = Get-PythonInstalls
    if ($installs -and $installs.Count -gt 0) {
        Info ("注册表里发现 " + $installs.Count + " 个 Python：")
        foreach ($i in $installs) { Info ("  · " + $i.DisplayName + "  [" + $i.DisplayVersion + "]") }

        $keep = $installs | Where-Object { $_.DisplayVersion -like "$TargetPyShort.*" }
        $drop = $installs | Where-Object { $_.DisplayVersion -notlike "$TargetPyShort.*" }
        if ($drop -and $drop.Count -gt 0) {
            if (-not $isAdmin) { Warn "非管理员：卸载其他版本 Python 可能失败，建议以管理员重跑" }
            foreach ($d in $drop) { Uninstall-PythonEntry $d }
            Refresh-Path
            Ok ("已卸载 " + $drop.Count + " 个非 $TargetPyShort 版本")
        }
        if ($keep) { Ok ("保留 Python $TargetPyShort（" + ($keep | Select-Object -First 1).DisplayName + "），只清依赖") }
    }
    else { Info "注册表里没有 python.org 安装记录" }

    # 残留目录（只删非 3.9 的）
    foreach ($base in @("$env:ProgramFiles", ${env:ProgramFiles(x86)}, "$env:LOCALAPPDATA\Programs\Python")) {
        if (-not $base -or -not (Test-Path $base)) { continue }
        Get-ChildItem -Path $base -Directory -Filter "Python3*" -ErrorAction SilentlyContinue | ForEach-Object {
            if ($_.Name -notmatch '^Python39$') { Remove-DirSafe $_.FullName }
        }
    }
    Remove-DirSafe (Join-Path $env:APPDATA "Python")
    Remove-DirSafe (Join-Path $env:LOCALAPPDATA "pip\cache")

    # 旧 venv
    $oldVenv = Join-Path (Join-Path $TargetDir "mcd_project") ".venv"
    if (Test-Path $oldVenv) {
        Remove-DirSafe $oldVenv
        Ok "已删除旧虚拟环境 $oldVenv"
    }
}

# ========================================================
Step 3 "准备 Python $TargetPyShort"

$py = Get-Python39
if ($py) {
    Ok ("复用已有 Python " + $py.Ver + " (" + $py.Exe + ")")
    if ($py.Ver -ne $TargetPyFull) {
        Warn ("本机是 " + $py.Ver + "，Mac 是 " + $TargetPyFull + "；同为 cp39，wheel 通用，可继续")
    }
}
else {
    $py = Install-Python3913
    if (-not $py) {
        Fail "缺少可用的 Python $TargetPyShort"
        Info "手动安装后重跑：https://mirrors.huaweicloud.com/python/$TargetPyFull/"
        Info "安装时务必勾选 Add python.exe to PATH"
        Die "中止"
    }
}
$pyExe = $py.Exe
$pyVer = $py.Ver

if (-not $NoPurge) {
    Info "清空该解释器里的旧依赖 ..."
    Clear-SitePackages $pyExe
}

# ========================================================
Step 4 "SSH key：存在则展示，不存在则生成"

$sshDir = Join-Path $env:USERPROFILE ".ssh"
if (-not (Test-Path $sshDir)) { New-Item -ItemType Directory -Path $sshDir -Force | Out-Null }

$keyPath = $null
$pubPath = $null
foreach ($n in @("id_ed25519", "id_rsa")) {
    $p = Join-Path $sshDir "$n.pub"
    if (Test-Path $p) {
        $keyPath = Join-Path $sshDir $n
        $pubPath = $p
        break
    }
}

if ($pubPath) {
    Ok "已存在 SSH 公钥，直接复用：$pubPath"
}
else {
    $keyPath = Join-Path $sshDir "id_ed25519"
    $pubPath = "$keyPath.pub"
    Info "未找到现成 key，正在生成 ed25519（无口令）..."
    & ssh-keygen -q -t ed25519 -C ("mcd-deploy@" + $env:COMPUTERNAME) -N '""' -f $keyPath 2>&1 | Out-Null
    if (-not (Test-Path $pubPath)) {
        # PowerShell 5.1 对空参数处理有坑，退回 cmd 再试一次
        Info "重试用 cmd 生成 ..."
        & cmd /c "ssh-keygen -q -t ed25519 -C `"mcd-deploy@$env:COMPUTERNAME`" -N `"`" -f `"$keyPath`"" 2>&1 | Out-Null
    }
    if (-not (Test-Path $pubPath)) { Die "ssh-keygen 执行失败，请检查 OpenSSH 安装" }
    Ok "已生成：$keyPath"
}

$pubText = (Get-Content $pubPath -Raw).Trim()

Write-Host ""
Write-Host "  ====== 复制下面这一整行（公钥）======" -ForegroundColor Yellow
Write-Host ""
Write-Host "  $pubText" -ForegroundColor White
Write-Host ""
Write-Host "  ====================================" -ForegroundColor Yellow

try { Set-Clipboard -Value $pubText; Ok "已自动复制到剪贴板" }
catch { Warn "复制到剪贴板失败，请手动选中上面那一行" }

# ========================================================
Step 5 "把公钥加进 GitHub Deploy keys（需要你手动做一次）"

Write-Host ""
Write-Host "  1) 打开  https://github.com/HLFYY/claude_code/settings/keys" -ForegroundColor White
Write-Host "  2) 点 Add deploy key" -ForegroundColor White
Write-Host "  3) Title 随便填（例如 win-server）" -ForegroundColor White
Write-Host "  4) Key 粘贴上面的公钥（整行）" -ForegroundColor White
Write-Host "  5) Allow write access 不用勾（只读拉代码足够）" -ForegroundColor White
Write-Host "  6) 点 Add key 保存" -ForegroundColor White
Write-Host ""

if ($SkipPause) {
    Warn "已指定 -SkipPause，跳过等待，直接尝试连接"
}
else {
    while ($true) {
        $ans = Read-Host "  加好后按回车继续（输入 q 退出）"
        if ($ans -eq "q" -or $ans -eq "Q") { Die "用户退出" }
        break
    }
}

# ========================================================
Step 6 "验证 SSH 连接并拉取代码"

Write-Host "  测试 git@github.com ..."
$sshOut = (& ssh -i "$keyPath" -o IdentitiesOnly=yes -o BatchMode=yes `
        -o StrictHostKeyChecking=accept-new -T git@github.com 2>&1) | Out-String
Write-Host ("  " + $sshOut.Trim()) -ForegroundColor DarkGray

if ($sshOut -match "successfully authenticated" -or $sshOut -match "Hi .+! You've successfully") {
    Ok "SSH 认证通过"
}
else {
    if ($sshOut -match "timed out|Connection refused|Could not resolve|Network is unreachable") {
        Fail "连 github.com:22 不通（不是 key 的问题，是网络/防火墙拦了 22 端口）"
        Info "国内服务器常见。改用 443 端口："
        Info "  1) 在 ~/.ssh/config 里写："
        Info "       Host github.com"
        Info "           HostName ssh.github.com"
        Info "           Port 443"
        Info "           User git"
        Info "  2) 或者给 git 设代理：git config --global http.proxy http://127.0.0.1:7890"
        Info "配置好后再重跑本脚本（键值已生成，不会重复生成）"
        Die "网络不通，先解决 github.com:22 连通性"
    }
    Fail "SSH 认证失败（常见原因：公钥没加、加到了账号 SSH keys 而不是仓库 Deploy keys、公钥复制不完整）"
    Write-Host "  重新展示公钥：" -ForegroundColor Yellow
    Write-Host "  $pubText" -ForegroundColor White
    if (-not $SkipPause) {
        $ans = Read-Host "  处理完后按回车重试（输入 q 退出）"
        if ($ans -eq "q" -or $ans -eq "Q") { Die "用户退出" }
        $sshOut = (& ssh -i "$keyPath" -o IdentitiesOnly=yes -o BatchMode=yes `
                -o StrictHostKeyChecking=accept-new -T git@github.com 2>&1) | Out-String
        if ($sshOut -notmatch "successfully authenticated" -and $sshOut -notmatch "Hi .+! You've successfully") {
            Die "SSH 仍不通，检查 Deploy key 是否加在 HLFYY/claude_code 这个仓库下"
        }
        Ok "SSH 认证通过"
    }
    else { Die "SSH 未通过" }
}

# 让 ssh 默认就用这把 key（已有 github.com 配置则不覆盖）
$cfgPath = Join-Path $sshDir "config"
$cfgText = if (Test-Path $cfgPath) { Get-Content $cfgPath -Raw } else { "" }
if ($cfgText -notmatch "github\.com") {
    $keyForCfg = $keyPath.Replace("\", "/")
    $block = "`r`nHost github.com`r`n    HostName github.com`r`n    User git`r`n    IdentityFile $keyForCfg`r`n    IdentitiesOnly yes`r`n    StrictHostKeyChecking accept-new`r`n"
    Add-Content -Path $cfgPath -Value $block -Encoding ASCII
    Ok "已写入 ~/.ssh/config（Host github.com）"
}
else {
    Warn "~/.ssh/config 里已有 github.com 配置，不改动；本次用 -i 显式指定 key"
}

$env:GIT_SSH_COMMAND = "ssh -i `"$keyPath`" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30 -o Compression=yes"

# 仓库里 mcd/resources 等逆向资料约 130MB，部署只需要 mcd_project（约 1.2MB）。
# 默认走「浅克隆 + 无 blob 过滤 + 稀疏检出」，只下 mcd_project，几十秒完成。
# 需要完整仓库时加 -FullClone。
$SparsePaths = @("mcd_project")

function Test-GitRepoReady([string]$dir) {
    if (-not (Test-Path (Join-Path $dir ".git"))) { return $false }
    & git -C $dir rev-parse --verify HEAD *> $null
    return ($LASTEXITCODE -eq 0)
}

if (Test-GitRepoReady $TargetDir) {
    Info "仓库已存在，执行 pull ..."
    & git -C $TargetDir pull --ff-only 2>&1 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
}
elseif (Test-Path (Join-Path $TargetDir ".git")) {
    Warn "$TargetDir 里有未完成的 .git（上次 clone 被中断），清掉重来 ..."
    Remove-Item -Path (Join-Path $TargetDir ".git") -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path $TargetDir -Force -ErrorAction SilentlyContinue |
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    & git -c core.autocrlf=false clone --depth 1 --filter=blob:none --sparse $RepoUrl $TargetDir 2>&1 |
        ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    & git -C $TargetDir sparse-checkout set $SparsePaths 2>&1 |
        ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
}
else {
    if (Test-Path $TargetDir) {
        $isEmpty = -not (Get-ChildItem -Path $TargetDir -Force -ErrorAction SilentlyContinue | Select-Object -First 1)
        if (-not $isEmpty) { Die "$TargetDir 已存在且不是 git 仓库、也不为空。换个 -TargetDir 或手动清空后重跑" }
    }
    else { New-Item -ItemType Directory -Path $TargetDir -Force | Out-Null }

    if ($FullClone) {
        Info "完整 clone 到 $TargetDir（含 mcd/resources 约 130MB，国内可能很慢）..."
        & git -c core.autocrlf=false clone $RepoUrl $TargetDir 2>&1 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    }
    else {
        Info "稀疏 clone 到 $TargetDir（仅 mcd_project，跳过逆向资源）..."
        & git -c core.autocrlf=false clone --depth 1 --filter=blob:none --sparse $RepoUrl $TargetDir 2>&1 |
            ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
        if ($LASTEXITCODE -ne 0) { Die "git clone 失败，检查网络到 github.com:22 是否可用" }
        & git -C $TargetDir sparse-checkout set $SparsePaths 2>&1 |
            ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
        Ok "已按稀疏方式检出 mcd_project"
    }
}
if (-not (Test-Path (Join-Path $TargetDir "mcd_project\captcha_login_worker.py"))) {
    Die "代码没拉下来或路径不对，检查 $TargetDir\mcd_project"
}
Ok "代码就绪：$TargetDir"

& git -C $TargetDir config core.autocrlf false
& git -C $TargetDir config core.sshCommand "ssh -i `"$keyPath`" -o IdentitiesOnly=yes"
Ok "已设置仓库级 core.sshCommand（以后 git pull 不用再管 key）"

$projDir = Join-Path $TargetDir "mcd_project"

# ========================================================
Step 7 "安装依赖（按 requirements-win-py39.txt 精确版本）"

if ($UseVenv) {
    $venvDir = Join-Path $projDir ".venv"
    $vpy = Join-Path $venvDir "Scripts\python.exe"

    if (Test-Path $vpy) { Ok "venv 已存在，复用 $venvDir" }
    else {
        Info "创建 venv：$venvDir"
        & $pyExe -m venv $venvDir 2>&1 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
        if (-not (Test-Path $vpy)) { Die "venv 创建失败" }
        Ok "venv 创建完成"
    }
}
else {
    # 默认不建虚拟环境，直接装到系统 Python
    $vpy = (Get-Command $pyExe -ErrorAction SilentlyContinue).Source
    if (-not $vpy) { $vpy = $pyExe }
    Ok "跳过虚拟环境，使用系统 Python：$vpy"
    Info "（如需隔离环境，重跑时加 -UseVenv）"
}

$pipArgs = @()
if ($PipIndex) {
    $pipArgs = @("-i", $PipIndex)
    # 走 http 源时需要显式信任域名；https 也加上无害
    $pipHost = ([Uri]$PipIndex).Host
    if ($pipHost) { $pipArgs += @("--trusted-host", $pipHost) }
    Info ("pip 源：" + $PipIndex)
    & $vpy -m pip config set global.index-url $PipIndex 2>&1 | Out-Null
}
# 非管理员时系统 Python 的 site-packages 可能没写权限，退回用户目录
if (-not $UseVenv -and -not $isAdmin) {
    $pipArgs += @("--user")
    Warn "非管理员运行，pip 加 --user 装到用户目录"
}

Info ("对齐 pip=" + $TargetPipVer + " setuptools=" + $TargetSetuptoolsVer + "（与 Mac 一致）...")
& $vpy -m pip install --upgrade "pip==$TargetPipVer" "setuptools==$TargetSetuptoolsVer" @pipArgs 2>&1 |
    Select-Object -Last 3 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
$installedPip = (& $vpy -m pip --version 2>$null | Out-String).Trim()
Ok $installedPip

# 锁定清单：优先脚本同目录，其次仓库里的 deploy 目录
$lockFile = Join-Path $ScriptDir "requirements-win-py39.txt"
if (-not (Test-Path $lockFile)) { $lockFile = Join-Path $projDir "deploy\requirements-win-py39.txt" }
if (-not (Test-Path $lockFile)) { Die "找不到依赖清单 requirements-win-py39.txt（应在 deploy\ 下）" }
Ok ("依赖清单：" + $lockFile)

$lockPkgs = Get-Content $lockFile | Where-Object { $_ -match '\S' -and $_ -notmatch '^\s*#' } |
    ForEach-Object { ($_ -split '#')[0].Trim() } | Where-Object { $_ }
Info ("清单共 " + $lockPkgs.Count + " 个包，全部带 win_amd64/cp39 wheel（无需编译）")

Info "开始安装（首次约 3~6 分钟）..."
$installLog = Join-Path $env:TEMP "mcd_pip_install.log"
& $vpy -m pip install --only-binary=:all: @pipArgs -r $lockFile 2>&1 |
    Tee-Object -FilePath $installLog | Select-Object -Last 15 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
if ($LASTEXITCODE -ne 0) {
    Warn "带 --only-binary 安装失败，去掉该限制重试（看 $installLog）"
    & $vpy -m pip install @pipArgs -r $lockFile 2>&1 |
        Tee-Object -FilePath $installLog -Append | Select-Object -Last 15 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    if ($LASTEXITCODE -ne 0) { Die "依赖安装失败，日志：$installLog（可换源：-PipIndex https://mirrors.aliyun.com/pypi/simple）" }
}
Ok "依赖安装完成"

# ---------------- 逐包校验版本 ----------------
Info "逐包校验版本 ..."
$frozenLines = (& $vpy -m pip freeze 2>$null)
$installed = @{}
foreach ($l in $frozenLines) {
    if ($l -match '^([^=]+)==(.+)$') { $installed[$Matches[1].ToLower().Replace('_','-').Replace('.','-')] = $Matches[2].Trim() }
}
$bad = @()
foreach ($p in $lockPkgs) {
    if ($p -notmatch '^([^=]+)==(.+)$') { continue }
    $pkgName = $Matches[1]
    $n = $pkgName.ToLower().Replace('_','-').Replace('.','-')
    $v = $Matches[2].Trim()
    if (-not $installed.ContainsKey($n)) { $bad += ("缺失  " + $pkgName + "==" + $v) }
    elseif ($installed[$n] -ne $v) { $bad += ("版本不符  " + $pkgName + " 期望 " + $v + " 实际 " + $installed[$n]) }
}
if ($bad.Count -eq 0) {
    Ok ("全部 " + $lockPkgs.Count + " 个包版本与锁定清单完全一致")
}
else {
    Fail ("有 " + $bad.Count + " 项不符：")
    $bad | ForEach-Object { Info $_ }
}

# 多余包（清单外）只提示，不卸载
$lockNames = @{}
foreach ($p in $lockPkgs) { if ($p -match '^([^=]+)==') { $lockNames[$Matches[1].ToLower().Replace('_','-').Replace('.','-')] = 1 } }
$extra = @()
foreach ($k in $installed.Keys) {
    if ($lockNames.ContainsKey($k)) { continue }
    if ($k -in @('pip','setuptools','wheel')) { continue }
    $extra += ($k + "==" + $installed[$k])
}
if ($extra.Count -gt 0) {
    Warn ("环境里还有 " + $extra.Count + " 个清单外的包（不影响运行）：" + ($extra -join ", "))
}

Info "校验关键模块 ..."
$check = & $vpy -c "import redis,requests,Crypto,PIL,DrissionPage,ddddocr;print('modules-ok')" 2>&1 | Out-String
if ($check -match "modules-ok") { Ok "模块导入正常" }
else { Fail "模块导入异常：$($check.Trim())"; Info "看上面的 ImportError；ddddocr 报 onnxruntime 就说明 Python 不是 3.9" }

Info "试导 worker 自身（会连带 config/login_manager/mcd_api 整条链）..."
Push-Location $projDir
$chain = & $vpy -c "import login_manager, proxy_pool, mcd_api; print('chain-ok')" 2>&1 | Out-String
Pop-Location
if ($chain -match "chain-ok") { Ok "worker 依赖链导入正常" }
else { Fail "worker 依赖链导入失败：$($chain.Trim())" }

# 依赖版本快照（便于和 Mac 对比）
$snap = Join-Path $TargetDir "pip_freeze_windows.txt"
& $vpy -m pip freeze 2>$null | Set-Content -Path $snap -Encoding UTF8
Ok "已导出本机依赖快照：$snap"

# ========================================================
Step 8 "验证 Redis 连通（worker 靠它收发任务）"

Push-Location $projDir
$pyCode = "from config import make_redis; r = make_redis(); print('redis-ok', r.ping()); print('queue', r.llen('mcd:captcha:login_queue'))"
$redisOut = & $vpy -c $pyCode 2>&1 | Out-String
Pop-Location

if ($redisOut -match "redis-ok") {
    Ok ($redisOut.Trim().Replace("`r", "").Replace("`n", "  |  "))
}
else {
    Warn "Redis 连不上（不影响环境搭建，但 worker 跑不起来）："
    Write-Host ("  " + $redisOut.Trim()) -ForegroundColor DarkGray
    Info "config.py 默认连 db.selfmediaai.cn:26379；如要换，设环境变量 MCD_REDIS_HOST / _PORT / _DB / _PASSWORD"
}

# ========================================================
Step 9 "浏览器检查 / 生成启动脚本"

if (-not $SkipChrome) {
    $chromeCandidates = @(
        (Join-Path $env:ProgramFiles "Google\Chrome\Application\chrome.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Google\Chrome\Application\chrome.exe"),
        (Join-Path $env:LOCALAPPDATA "Google\Chrome\Application\chrome.exe")
    ) | Where-Object { $_ -and (Test-Path $_) }

    $edgeCandidates = @(
        (Join-Path ${env:ProgramFiles(x86)} "Microsoft\Edge\Application\msedge.exe"),
        (Join-Path $env:ProgramFiles "Microsoft\Edge\Application\msedge.exe")
    ) | Where-Object { $_ -and (Test-Path $_) }

    if ($chromeCandidates) {
        Ok ("Chrome 已安装 (" + $chromeCandidates[0] + ")")
        Ok "worker 将使用 Chrome（优先级最高）"
    }
    elseif ($edgeCandidates) {
        Ok ("检测到 Edge，跳过 Chrome 下载 (" + $edgeCandidates[0] + ")")
        Warn "worker 会用 Edge 驱动。Edge 与 Chrome 同为 Chromium 内核，通常可用；"
        Warn "若滑块始终不过，再装 Chrome 重试（Edge 的 UA/指纹可能被数美判得更严）"
    }
    else {
        Warn "未检测到 Chrome，也未检测到 Edge —— 过滑块会失败"
        Info "国内不要用 winget 装 Chrome（Google CDN 常被墙，会像 Git 那样卡死）"
        Info "请手动安装其一后重跑本脚本："
        Info "  Chrome: https://www.google.cn/chrome/"
        Info "  Edge  : 系统自带的 Microsoft Edge 即可，无需下载"
    }
}

$batPath = Join-Path $TargetDir "run_worker.bat"
$bat = @"
@echo off
chcp 65001 >nul
cd /d "$projDir"
"$vpy" captcha_login_worker.py %*
"@
Set-Content -Path $batPath -Value $bat -Encoding ASCII
Ok "已生成 $batPath"

$batDbg = Join-Path $TargetDir "run_worker_debug.bat"
$bat2 = @"
@echo off
chcp 65001 >nul
cd /d "$projDir"
"$vpy" captcha_login_worker.py --show %*
"@
Set-Content -Path $batDbg -Value $bat2 -Encoding ASCII
Ok "已生成 $batDbg（有头模式，方便看浏览器）"

# ========================================================
Write-Host ""
Write-Host ("  " + ("=" * 62)) -ForegroundColor DarkCyan
Write-Host "  部署完成" -ForegroundColor Green
Write-Host ("  " + ("=" * 62)) -ForegroundColor DarkCyan
Write-Host ""
Write-Host "  代码目录   $TargetDir" -ForegroundColor White
Write-Host "  Python     $vpy  ($pyVer)" -ForegroundColor White
Write-Host "  SSH key    $keyPath" -ForegroundColor White
Write-Host "  依赖快照   $snap" -ForegroundColor White
Write-Host ""
Write-Host "  下一步：" -ForegroundColor Yellow
Write-Host "   1) 前台试跑：  $batPath" -ForegroundColor White
Write-Host "   2) 想看成套流程：$batDbg" -ForegroundColor White
Write-Host "   3) 注册为开机自启服务（可选，推荐 NSSM）：" -ForegroundColor White
Write-Host "        nssm install mcd-captcha-worker `"$vpy`" `"$projDir\captcha_login_worker.py`"" -ForegroundColor DarkGray
Write-Host "        nssm set mcd-captcha-worker AppDirectory `"$projDir`"" -ForegroundColor DarkGray
Write-Host "        nssm set mcd-captcha-worker AppStdout `"$env:USERPROFILE\logs\worker.log`"" -ForegroundColor DarkGray
Write-Host "        nssm start mcd-captcha-worker" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  提醒：worker 无头运行，若过滑块失败，先看 ~\datas\mcd\captcha_debug\ 里的截图和 HTML" -ForegroundColor DarkGray
Write-Host ""

#Requires -Version 5.1
<#
.SYNOPSIS
    mcd_project / captcha_login_worker.py  Windows 全新服务器一键部署
    Python 与所有依赖版本严格对齐 Mac 环境 /Users/houjie/venv/python3-forcrawl

.DESCRIPTION
    阶段一  环境自检 + 安装 Git / OpenSSH
            （全部优先走国内镜像：华为云 git-for-windows、清华 PyPI）
    阶段二  清理旧 Python 环境（默认执行，-NoPurge 可跳过）
            · 先结束占用 Python 的进程 / 停掉以它为 exe 的服务
              （旧版本卸载失败基本都是「文件被占用」，而残留的 MSI 会让后面安装 1603）
            · 卸载 registry 里所有非 3.9 的 Python（3.12 / 3.11 / 3.13 …），并复查是否真卸干净
            · 删除残留目录（Program Files\Python3xx、%LOCALAPPDATA%\Programs\Python\Python3xx、
              %APPDATA%\Python、pip 缓存）
            · 保留已存在的 3.9.x 解释器，只把它的第三方包全部卸干净（等同于「重装环境」）
            · 删掉旧的 .venv
    阶段三  准备 Python 3.9.13（python.org 最后一个 Windows 安装版；cp39 ABI 与 Mac 的
            3.9.17 完全一致，wheel 通用）
            · 先全盘探测已有 3.9：PATH / py -0p / where / 注册表 InstallPath / 常见目录 / 各盘根目录
              （注册表那条是关键，能覆盖装到自定义目录或另一个用户目录的情况）
            · 找不到才装，且分 4 级重试：全用户安装 → /repair → 清掉残骸重装 → 当前用户安装，
              最后还有「MSI 组件逐个装」兜底；日志写 %TEMP%\python39_install.log
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
    param([string] $Url, [string] $OutFile, [string] $Label = "文件", [int] $MinBytes = 100000)

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
                    if ($len -gt $MinBytes) {
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

# 注册表里登记的解释器路径（PythonCore\<tag>\InstallPath）。
# 这是唯一能覆盖「装到自定义目录」或「装在另一个用户的 LOCALAPPDATA」的信息源，
# 光扫 PATH + 几个固定目录会漏，漏了就会重复安装同一版本 → MSI 返回 1603。
function Get-RegisteredPythonPaths {
    $out = @()
    foreach ($root in @(
        "HKLM:\SOFTWARE\Python\PythonCore",
        "HKLM:\SOFTWARE\WOW6432Node\Python\PythonCore",
        "HKCU:\SOFTWARE\Python\PythonCore"
    )) {
        if (-not (Test-Path $root)) { continue }
        foreach ($tag in (Get-ChildItem $root -ErrorAction SilentlyContinue)) {
            $ipKey = Join-Path $tag.PSPath "InstallPath"
            if (-not (Test-Path $ipKey)) { continue }
            $props = Get-ItemProperty $ipKey -ErrorAction SilentlyContinue
            if (-not $props) { continue }
            foreach ($cand in @($props.'(default)', $props.ExecutablePath)) {
                if (-not $cand) { continue }
                if ($cand -match '\.exe$') { $out += $cand }
                else { $out += (Join-Path $cand "python.exe") }
            }
        }
    }
    return $out | Where-Object { $_ } | Select-Object -Unique
}

# 所有可能存在的 python.exe（不判断版本），去重后返回
function Get-PythonCandidates {
    $c = @()

    # 1) PATH
    foreach ($n in @("python", "python3", "python3.9")) {
        $cmd = Get-Command $n -ErrorAction SilentlyContinue
        if ($cmd -and $cmd.Source -and $cmd.Source -notmatch 'WindowsApps') {
            $real = (& $cmd.Source -c "import sys;print(sys.executable)" 2>$null | Out-String).Trim()
            if ($real) { $c += $real } else { $c += $cmd.Source }
        }
    }

    # 2) py 启动器（-0p 会列出所有已注册版本及其真实路径）
    if (Has-Exe "py") {
        $list = (& py -0p 2>$null | Out-String)
        foreach ($line in ($list -split "`r?`n")) {
            if ($line -match '([A-Za-z]:\\.*python\.exe)') { $c += $Matches[1] }
        }
        foreach ($t in @("-3.9", "-3")) {
            $e = (& py $t -c "import sys;print(sys.executable)" 2>$null | Out-String).Trim()
            if ($e) { $c += $e }
        }
    }

    # 3) where.exe
    if (Has-Exe "where.exe") {
        $w = (& where.exe python 2>$null | Out-String)
        if ($w) { $c += ($w -split "`r?`n" | Where-Object { $_ -match 'python\.exe\s*$' }) }
    }

    # 4) 注册表登记的 InstallPath
    $c += Get-RegisteredPythonPaths

    # 5) python.org 默认安装位置
    $c += @(
        "$env:ProgramFiles\Python39\python.exe",
        "${env:ProgramFiles(x86)}\Python39\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python39\python.exe",
        "C:\Python39\python.exe"
    )

    # 6) 各盘根目录
    foreach ($d in (Get-PSDrive -PSProvider FileSystem -ErrorAction SilentlyContinue)) {
        if ($d.Root) { $c += (Join-Path $d.Root "Python39\python.exe") }
    }

    return $c | Where-Object { $_ -and ($_ -match '\.exe\s*$') } |
        ForEach-Object { $_.Trim() } | Select-Object -Unique
}

# 把探测结果打出来，装不上时至少知道机器上到底有什么
function Show-PythonScan {
    $rows = @()
    foreach ($p in (Get-PythonCandidates)) {
        if (-not (Test-Path $p)) { continue }
        $v = Get-PyVersion $p
        if ($v) { $rows += ("  · " + $p + "   ->   " + $v) }
        else { $rows += ("  · " + $p + "   ->   无法运行（文件损坏 / 缺 DLL / 权限不足）") }
    }
    if ($rows.Count -eq 0) { Info "  没找到任何 python.exe" }
    else { $rows | ForEach-Object { Info $_ } }
}

# 返回 3.9.x 的解释器；找不到返回 $null
function Get-Python39 {
    foreach ($p in (Get-PythonCandidates)) {
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

function Get-InstallerHint([int]$code) {
    switch ($code) {
        0     { return "成功" }
        3010  { return "成功，需要重启" }
        1602  { return "用户取消" }
        1603  { return "致命错误：常见原因是「有未完成的挂起重启」「同版本已注册但文件不全」「上级安装事务失败残留」「没真正提权」" }
        1605  { return "该产品未安装" }
        1618  { return "另一个安装正在进行（MSI 全局互斥），稍后重试" }
        1619  { return "安装包无法打开" }
        1638  { return "该版本已安装过" }
        -1    { return "安装程序没能启动" }
        default { return "未知错误" }
    }
}

# 列出所有 python 进程（含真实路径，拿不到路径的说明是别的用户的进程）
function Get-PythonProcs {
    $out = @()
    foreach ($p in (Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -match '^python' })) {
        $path = $null
        try { $path = $p.Path } catch {}
        $out += [pscustomobject]@{ Id = $p.Id; Name = $p.ProcessName; Path = $path }
    }
    return $out
}

# 结束占着某个目录的进程 + 停掉以该目录为 exe 的服务。
# 3.12 卸载失败的根因就是 python.exe 被别的程序（服务 / IDE / 旧 worker）打开着。
function Release-PathHolders([string]$pathLike) {
    $norm = $pathLike.TrimEnd('\')

    $svcs = @()
    try {
        $svcs = @(Get-CimInstance Win32_Service -ErrorAction Stop | Where-Object {
            $_.PathName -and ($_.PathName -like ("*" + $norm + "*")) -and $_.State -eq "Running"
        })
    }
    catch {}
    foreach ($s in $svcs) {
        Info ("停止服务 " + $s.Name + "（" + $s.DisplayName + "）...")
        & sc.exe stop $s.Name | Out-Null
    }
    if ($svcs.Count -gt 0) { Start-Sleep -Seconds 3 }

    $holders = @(Get-PythonProcs | Where-Object {
        $_.Path -and ($_.Path -like ($norm + "\*"))
    })
    foreach ($h in $holders) {
        Warn ("结束占用进程 " + $h.Name + " (PID " + $h.Id + ")  " + $h.Path)
        try { Stop-Process -Id $h.Id -Force -ErrorAction Stop }
        catch { Warn ("  结束失败：" + $_.Exception.Message) }
    }
    if ($holders.Count -gt 0) { Start-Sleep -Seconds 2 }

    $left = @(Get-PythonProcs | Where-Object { $_.Path -and ($_.Path -like ($norm + "\*")) })
    return ($left.Count -eq 0)
}

function Remove-DirSafe([string]$path) {
    if (-not (Test-Path $path)) { return }
    for ($i = 1; $i -le 2; $i++) {
        try {
            Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction Stop
            Info ("已删除 " + $path)
            return
        }
        catch {
            if ($i -eq 1) {
                Release-PathHolders $path | Out-Null
                Start-Sleep -Seconds 2
            }
            else {
                Warn ("删除失败（仍被占用）：" + $path)
                Warn ("  " + $_.Exception.Message)
                Warn "  处理：先关掉占用它的程序/服务（或重启服务器）再重跑；不处理也不影响后面的安装"
            }
        }
    }
}

function Uninstall-PythonEntry($entry) {
    $dn = $entry.DisplayName
    $us = $entry.UninstallString
    Info ("卸载 " + $dn + " ...")

    # 先放开占用，否则 msiexec 会 1603、目录也删不掉
    $ver = $entry.DisplayVersion
    $tag = $null
    if ($ver -match '^(\d+)\.(\d+)\.') { $tag = $Matches[1] + "." + $Matches[2] }
    elseif ($dn -match 'Python\s+(\d+)\.(\d+)') { $tag = $Matches[1] + "." + $Matches[2] }
    if ($tag) {
        $dirTag = "Python" + ($tag -replace '\.', '')
        foreach ($base in @("$env:ProgramFiles", "${env:ProgramFiles(x86)}", "$env:LOCALAPPDATA\Programs\Python")) {
            if (-not $base) { continue }
            $d = Join-Path $base $dirTag
            if (Test-Path $d) { Release-PathHolders $d | Out-Null }
        }
    }

    if ($us -match 'MsiExec\.exe\s+/[IX]?\s*(\{[0-9A-Fa-f\-]+\})') {
        $msi = $Matches[1]
        $p = Start-Process -FilePath "msiexec.exe" -ArgumentList @("/x$msi", "/qn", "/norestart") -Wait -PassThru
        $code = if ($p) { $p.ExitCode } else { -1 }
        if ($code -ne 0 -and $code -ne 3010 -and $code -ne 1605) {
            Warn ("  卸载返回 " + $code + "（" + (Get-InstallerHint $code) + "）")
        }
        return
    }

    $exe = $us
    if ($exe -match '^\s*"([^"]+)"') { $exe = $Matches[1] }
    elseif ($exe -match '^\s*(\S+\.exe)') { $exe = $Matches[1] }
    if (Test-Path $exe) {
        $p = Start-Process -FilePath $exe -ArgumentList @("/uninstall", "/quiet") -Wait -PassThru -ErrorAction SilentlyContinue
        if ($p -and $p.ExitCode -ne 0) { Warn ("  卸载返回 " + $p.ExitCode) }
    }
    else { Warn ("找不到卸载程序：" + $us) }
}

# MSI 事务是否残留 / 是否有挂起重启（两者都会让新安装直接 1603）
function Test-PendingReboot {
    try {
        $v = (Get-ItemProperty "HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager" `
            -Name PendingFileRenameOperations -ErrorAction SilentlyContinue).PendingFileRenameOperations
        if ($v) { return $true }
    }
    catch {}
    if (Test-Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending") { return $true }
    if (Test-Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired") { return $true }
    return $false
}

# 单独跑一次安装包，返回退出码；日志留着好查 1603 的真实原因
function Invoke-PySetup {
    param([string]$Exe, [string]$ArgStr, [string]$LogFile)
    $full = $ArgStr.Trim() + ' /log "' + $LogFile + '"'
    try {
        $p = Start-Process -FilePath $Exe -ArgumentList $full -Wait -PassThru -ErrorAction Stop
        if ($p) { return [int]$p.ExitCode }
    }
    catch { Warn ("启动安装程序失败：" + $_.Exception.Message) }
    return -1
}

# 兜底：直接用 python.org 的 MSI 组件装（等于手工执行安装包内部的每一步）
function Install-Python39ByMsi {
    $allUsers = if ($isAdmin) { 1 } else { 0 }
    $target = if ($allUsers) { "$env:ProgramFiles\Python39" } else { "$env:LOCALAPPDATA\Programs\Python\Python39" }

    $msiDir = Join-Path $env:TEMP "py39msi"
    New-Item -ItemType Directory -Path $msiDir -Force -ErrorAction SilentlyContinue | Out-Null

    # 顺序有讲究：ucrt 要先装，path 最后写环境变量
    $order = @("ucrt.msi", "core.msi", "lib.msi", "exe.msi", "tcltk.msi", "tools.msi", "pip.msi", "path.msi")
    foreach ($f in $order) {
        $dst = Join-Path $msiDir $f
        if (Test-Path $dst) { continue }
        $u = "$MirrorPyBase/$TargetPyFull/amd64/$f"
        if (-not (Get-MirrorFile -Url $u -OutFile $dst -Label ("Python 组件 " + $f) -MinBytes 512)) {
            Warn ("拿不到 " + $f + "，MSI 方式放弃")
            return $false
        }
    }

    Info ("用 MSI 组件安装到 " + $target + " ...")
    foreach ($f in $order) {
        $argStr = '/i "' + (Join-Path $msiDir $f) + '" /qn /norestart TARGETDIR="' + $target + '"'
        if ($allUsers) { $argStr += ' ALLUSERS=1' }
        $p = Start-Process -FilePath "msiexec.exe" -ArgumentList $argStr -Wait -PassThru -ErrorAction SilentlyContinue
        $code = if ($p) { $p.ExitCode } else { -1 }
        if ($code -ne 0 -and $code -ne 3010 -and $code -ne 1605) {
            Warn ("  " + $f + " 返回 " + $code + "（" + (Get-InstallerHint $code) + "）")
        }
    }
    Refresh-Path
    return [bool](Get-Python39)
}

function Install-Python3913 {
    $url = "$MirrorPyBase/$TargetPyFull/python-$TargetPyFull-amd64.exe"
    $exe = Join-Path $env:TEMP "python-$TargetPyFull-amd64.exe"
    if (-not (Get-MirrorFile -Url $url -OutFile $exe -Label "Python $TargetPyFull")) { return $null }

    $logFile = Join-Path $env:TEMP "python39_install.log"
    Remove-Item $logFile -Force -ErrorAction SilentlyContinue

    if (Test-PendingReboot) {
        Warn "系统有未完成的挂起重启（PendingFileRenameOperations / CBS RebootPending）"
        Warn "一体化安装包 python-$TargetPyFull-amd64.exe 在这种机器上基本必报 1603"
        Info "先改走 MSI 组件逐个装（绕开安装包的事务逻辑，通常能过）..."
        if (Install-Python39ByMsi) {
            $found = Get-Python39
            if ($found) {
                Ok ("Python " + $found.Ver + " 安装完成（MSI 组件方式）：" + $found.Exe)
                Remove-Item $exe -Force -ErrorAction SilentlyContinue
                return $found
            }
        }
        Warn "MSI 组件方式也没成，继续试一体化安装包 ..."
        Info "若仍 1603：先跑 deploy\install_python39.ps1（含 zip 兜底），不必重启服务器"
    }
    $msiexecs = @(Get-Process msiexec -ErrorAction SilentlyContinue)
    if ($msiexecs.Count -gt 0) {
        Warn ("有 " + $msiexecs.Count + " 个 msiexec 进程在跑（多半是上次失败的安装/卸载残留），等 15 秒 ...")
        Start-Sleep -Seconds 15
    }
    if (Test-Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Installer\InProgress") {
        Warn "注册表残留 Installer\InProgress（上次安装被中断），这种状态会一直 1603，建议重启后再装"
    }

    $allUsersDir = "$env:ProgramFiles\Python39"
    $userDir     = "$env:LOCALAPPDATA\Programs\Python\Python39"
    $commonArgs  = "PrependPath=1 Include_test=0 Include_launcher=1 Shortcuts=0 Include_pip=1 AssociateFiles=0"

    # 注册表里还挂着 3.9 的登记，但探测不到 python.exe → 说明是「注册了但文件不全」的残骸
    $stale = @(Get-PythonInstalls | Where-Object { $_.DisplayVersion -like "$TargetPyShort.*" })

    $attempts = @()
    if ($isAdmin) {
        if ($stale.Count -gt 0) {
            # 已经「注册了但文件不全」，直接装同一个版本必然 1603，先走修复/清残骸
            Warn ("注册表里挂着 " + $stale.Count + " 个 3.9 登记但找不到 python.exe，先修复再装")
            $attempts += @{ Name = "修复已有 3.9 安装（/repair）"; Args = ("/quiet /repair InstallAllUsers=1 TargetDir=`"$allUsersDir`"") }
            $attempts += @{ Name = "清掉 3.9 残骸后重装"; Args = ("/quiet InstallAllUsers=1 TargetDir=`"$allUsersDir`" " + $commonArgs); PurgeFirst = $true }
            $attempts += @{ Name = "全用户安装到 Program Files"; Args = ("/quiet InstallAllUsers=1 TargetDir=`"$allUsersDir`" " + $commonArgs) }
        }
        else {
            $attempts += @{ Name = "全用户安装到 Program Files"; Args = ("/quiet InstallAllUsers=1 TargetDir=`"$allUsersDir`" " + $commonArgs) }
        }
    }
    $attempts += @{ Name = "当前用户安装到 %LOCALAPPDATA%"; Args = ("/quiet InstallAllUsers=0 TargetDir=`"$userDir`" " + $commonArgs) }

    $i = 0
    foreach ($a in $attempts) {
        $i++
        Info ("[" + $i + "/" + ($attempts.Count + 1) + "] " + $a.Name + " ...")

        if ($a.PurgeFirst) {
            foreach ($s in $stale) { Uninstall-PythonEntry $s }
            Start-Sleep -Seconds 2
            if (Test-PendingReboot) { Warn "  清完残骸后系统要求重启，安装可能仍失败" }
        }

        $code = Invoke-PySetup -Exe $exe -ArgStr $a.Args -LogFile $logFile
        Refresh-Path
        $found = Get-Python39
        if ($found) {
            Ok ("Python " + $found.Ver + " 安装完成：" + $found.Exe)
            Remove-Item $exe -Force -ErrorAction SilentlyContinue
            return $found
        }
        Warn ("  失败，退出码 " + $code + "（" + (Get-InstallerHint $code) + "）")
        if ($code -eq 1618) { Start-Sleep -Seconds 20 }
    }

    Info ("[" + ($attempts.Count + 1) + "/" + ($attempts.Count + 1) + "] 改用 MSI 组件安装 ...")
    if (Install-Python39ByMsi) {
        $found = Get-Python39
        if ($found) {
            Ok ("Python " + $found.Ver + " 安装完成（MSI 方式）：" + $found.Exe)
            Remove-Item $exe -Force -ErrorAction SilentlyContinue
            return $found
        }
    }

    Remove-Item $exe -Force -ErrorAction SilentlyContinue
    if (Test-Path $logFile) {
        Warn ("安装日志：" + $logFile + "（末尾 12 行）")
        Get-Content $logFile -Tail 12 -ErrorAction SilentlyContinue |
            ForEach-Object { Write-Host ("  " + $_) -ForegroundColor DarkGray }
    }
    return $null
}

# 把某个解释器里的第三方包全部卸掉（保留 pip / setuptools / wheel）
function Clear-SitePackages([string]$vpy) {
    # 残缺的安装可能连 pip 都没有，先 ensurepip 补上，否则下面全白做
    $probe = (& $vpy -m pip --version 2>&1 | Out-String)
    if ($probe -notmatch '^pip \d') {
        Info "该解释器没有 pip，用 ensurepip 补 ..."
        & $vpy -m ensurepip --default-pip 2>&1 | Out-Null
        $probe = (& $vpy -m pip --version 2>&1 | Out-String)
        if ($probe -notmatch '^pip \d') {
            Warn "pip 仍不可用，跳过依赖清理（阶段 7 会重新装 pip）"
            return
        }
    }

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
    }
    else { Info "注册表里没有 python.org 安装记录" }

    # 先把占着 Python 的进程 / 服务放掉。
    # 旧版 3.12 卸载失败就是 python.exe 被别的程序打开着，msiexec 直接 1603，
    # 而残留的 MSI 事务会让后面装 3.9 也跟着 1603。
    $procs = @(Get-PythonProcs)
    if ($procs.Count -gt 0) {
        Info ("发现 " + $procs.Count + " 个 python 进程在运行：")
        foreach ($p in $procs) {
            $pp = if ($p.Path) { $p.Path } else { "<路径不可见，可能是其他用户的进程>" }
            Info ("  · " + $p.Name + " (PID " + $p.Id + ")  " + $pp)
        }

        $pySvc = @()
        try {
            $pySvc = @(Get-CimInstance Win32_Service -ErrorAction Stop | Where-Object {
                $_.PathName -match '\\Python3\d+\\' -and $_.State -eq 'Running'
            })
        }
        catch {}
        foreach ($s in $pySvc) {
            Info ("停止服务 " + $s.Name + "（" + $s.DisplayName + "）...")
            & sc.exe stop $s.Name | Out-Null
        }
        if ($pySvc.Count -gt 0) { Start-Sleep -Seconds 3 }

        if ($isAdmin) {
            $killed = 0
            foreach ($p in $procs) {
                try { Stop-Process -Id $p.Id -Force -ErrorAction Stop; $killed++ } catch {}
            }
            Start-Sleep -Seconds 2
            if ($killed -gt 0) { Ok ("已结束 " + $killed + " 个占用进程（否则旧版本卸不干净）") }
            $left = @(Get-PythonProcs)
            if ($left.Count -gt 0) {
                Warn ("还有 " + $left.Count + " 个 python 进程没能结束（多半属于其他用户或系统服务）：")
                foreach ($p in $left) { Warn ("  · " + $p.Name + " (PID " + $p.Id + ")") }
            }
        }
        else { Warn "非管理员：无法结束这些进程，卸载很可能失败，建议右键「以管理员身份运行」重跑" }
    }

    if ($installs -and $installs.Count -gt 0) {
        $keep = $installs | Where-Object { $_.DisplayVersion -like "$TargetPyShort.*" }
        $drop = $installs | Where-Object { $_.DisplayVersion -notlike "$TargetPyShort.*" }
        if ($drop -and $drop.Count -gt 0) {
            if (-not $isAdmin) { Warn "非管理员：卸载其他版本 Python 可能失败，建议以管理员重跑" }
            foreach ($d in $drop) { Uninstall-PythonEntry $d }
            Refresh-Path
            Start-Sleep -Seconds 2

            # 校验是否真卸掉了。注册表还挂着 = 失败（文件被占用是最常见原因），
            # 这种残骸留着，后面装 3.9 会直接 1603，所以必须明确告警。
            $still = @(Get-PythonInstalls | Where-Object { $_.DisplayVersion -notlike "$TargetPyShort.*" })
            if ($still.Count -eq 0) {
                Ok ("已卸载 " + $drop.Count + " 个非 " + $TargetPyShort + " 版本")
            }
            else {
                Warn ("有 " + $still.Count + " 个没卸干净：")
                foreach ($s in $still) { Warn ("  · " + $s.DisplayName + "  [" + $s.DisplayVersion + "]") }
                Warn "  这很可能是文件被占用导致的。最省事的办法是重启服务器后重跑本脚本；"
                Warn "  也可以到「设置 → 应用」里手动卸载这几个，再重跑。"
                Warn "  不处理的话，后面安装 3.9 大概率直接返回 1603。"
            }
        }
        if ($keep) {
            Ok ("保留 Python $TargetPyShort（" + ($keep | Select-Object -First 1).DisplayName + "），只清依赖")
            $kp = Get-RegisteredPythonPaths | Where-Object { $_ -like "*Python39*" }
            if ($kp) { foreach ($x in $kp) { Info ("  注册表登记位置：" + $x) } }
        }
    }

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
    Info "已扫到的 python.exe："
    Show-PythonScan
    Info "没有可用的 3.9.x，开始安装 ..."

    $py = Install-Python3913
    if (-not $py) {
        Fail "缺少可用的 Python $TargetPyShort"
        Info ("安装日志：" + (Join-Path $env:TEMP "python39_install.log"))
        if (Test-PendingReboot) {
            Info "系统有未完成的挂起重启 —— 这是 1603 最常见的原因"
            Info "但不能重启的话，先跑这个（三条路兜底，含纯 zip 解压，不需要重启）："
            Info ("  powershell -ExecutionPolicy Bypass -File " + (Join-Path $ScriptDir "install_python39.ps1"))
        }
        else {
            Info "如果日志里是 1603：重启服务器后重跑（可清掉残留的 MSI 事务）"
            Info ("也可以先跑 " + (Join-Path $ScriptDir "install_python39.ps1") + " 绕开一体化安装包")
        }
        Info ("手动安装：" + "$MirrorPyBase/$TargetPyFull/")
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

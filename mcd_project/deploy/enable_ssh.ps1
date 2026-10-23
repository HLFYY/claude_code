# enable_ssh.ps1 — 在目标 Windows 上开启 OpenSSH Server（供 Mac 侧远程部署使用）
#
# 用法（在目标机的 RDP 桌面里，以管理员身份运行 PowerShell）：
#   powershell -NoProfile -ExecutionPolicy Bypass -File enable_ssh.ps1
#   powershell -NoProfile -ExecutionPolicy Bypass -File enable_ssh.ps1 -Port 2222
#
# 说明：
#   - 默认端口 22；若公网环境担心扫描，用 -Port 2222 之类的非标端口。
#   - 脚本会同时开本机防火墙入站规则。
#   - 云服务器还必须到控制台安全组里放行同一端口，脚本改不了安全组。

param(
    [int]    $Port = 22,
    [string] $ListenAddress = "0.0.0.0"
)

$ErrorActionPreference = "Stop"

function Ok($m)   { Write-Host "[OK]   $m" -ForegroundColor Green }
function Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host "[FAIL] $m" -ForegroundColor Red; exit 1 }

# ---- 管理员检查 ----
$isAdmin = ([Security.Principal.WindowsPrincipal] `
    [Security.Principal.WindowsIdentity]::GetCurrent()
).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) { Die "请以管理员身份运行 PowerShell" }

Write-Host "=== 1/5 系统信息 ==="
$os = Get-CimInstance Win32_OperatingSystem
Ok "$($os.Caption)  $($os.Version)"

Write-Host "`n=== 2/5 安装 OpenSSH Server ==="
$cap = Get-WindowsCapability -Online -Name "OpenSSH.Server*" -ErrorAction SilentlyContinue
if ($cap -and $cap.State -eq "Installed") {
    Ok "已安装，跳过"
} else {
    try {
        Add-WindowsCapability -Online -Name "OpenSSH.Server~~~~0.0.1.0" | Out-Null
        Ok "安装完成"
    } catch {
        # Server 2016 等旧版本走 WindowsFeature
        Warn "Add-WindowsCapability 失败，尝试 Get-WindowsFeature"
        $f = Get-WindowsFeature -Name OpenSSH-Server -ErrorAction SilentlyContinue
        if ($f -and -not $f.Installed) { Install-WindowsFeature -Name OpenSSH-Server | Out-Null }
        if (-not (Get-Command sshd.exe -ErrorAction SilentlyContinue)) {
            Die "OpenSSH Server 安装失败，请手动安装后再试"
        }
        Ok "通过 WindowsFeature 安装完成"
    }
}

Write-Host "`n=== 3/5 配置并启动 sshd ==="
$sshd = Get-Service sshd -ErrorAction SilentlyContinue
if (-not $sshd) { Die "找不到 sshd 服务，安装可能不完整" }

# 端口与监听地址写进配置，避免和默认值冲突
$confPath = "$env:ProgramData\ssh\sshd_config"
if (Test-Path $confPath) {
    $lines = Get-Content $confPath
    $lines = $lines | Where-Object { $_ -notmatch "^\s*Port\s+\d+" -and $_ -notmatch "^\s*ListenAddress\s+" }
    $lines += "Port $Port"
    $lines += "ListenAddress $ListenAddress"
    # 确保允许密码登录（remote_run.py 走密码认证）
    if (-not ($lines | Where-Object { $_ -match "^\s*PasswordAuthentication\s+yes" })) {
        $lines += "PasswordAuthentication yes"
    }
    Set-Content -Path $confPath -Value $lines -Encoding ASCII
    Ok "sshd_config 已更新：Port $Port, ListenAddress $ListenAddress"
} else {
    Warn "未找到 $confPath，使用默认配置（端口 22）"
    if ($Port -ne 22) { Warn "自定义端口 $Port 可能不生效，请手动检查配置" }
}

Set-Service sshd -StartupType Automatic
Restart-Service sshd
Ok "sshd 已设为自动启动并重启"

Write-Host "`n=== 4/5 防火墙 ==="
$ruleName = "OpenSSH-Server-In-TCP-$Port"
Remove-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue
New-NetFirewallRule -Name $ruleName -DisplayName "OpenSSH Server (TCP $Port)" `
    -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort $Port | Out-Null
Ok "已放行本机防火墙 TCP $Port"

Write-Host "`n=== 5/5 验证 ==="
Start-Sleep -Seconds 2
$svc = Get-Service sshd
Ok "服务状态：$($svc.Status)"
$listen = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($listen) {
    Ok "正在监听 $($listen.LocalAddress -join ', ')`:$Port"
} else {
    Warn "端口 $Port 未处于监听状态，检查 sshd 日志：$env:ProgramData\ssh\logs\sshd.log"
}

Write-Host "`n当前管理员账户：$env:USERNAME"
Write-Host ""
Write-Host "===== 下一步 =====" -ForegroundColor Cyan
Write-Host "1. 如果这是云服务器，请到控制台【安全组 / 防火墙】放行 TCP $Port" -ForegroundColor Cyan
Write-Host "   这一步脚本改不了，不放行外网依然连不上" -ForegroundColor Cyan
Write-Host "2. 回到 Mac 上执行：" -ForegroundColor Cyan
Write-Host "   cd mcd_project/deploy" -ForegroundColor White
Write-Host "   python3 remote_run.py --probe --host <公网IP> --probe-ports $Port" -ForegroundColor White
Write-Host "   MCD_PASS='<密码>' python3 remote_run.py --host <公网IP> --port $Port --user $env:USERNAME" -ForegroundColor White

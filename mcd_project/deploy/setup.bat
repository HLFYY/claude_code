@echo off
chcp 65001 >nul
title mcd worker deploy
cd /d "%~dp0"

rem ---------- self-elevate to administrator ----------
rem fltmc requires admin; failure means we are not elevated yet
fltmc >nul 2>&1
if errorlevel 1 (
    echo.
    echo   Requesting administrator privileges...
    powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -ArgumentList '%*' -Verb RunAs -Wait" >nul 2>&1
    if errorlevel 1 (
        echo.
        echo   [FAIL] Elevation was cancelled or denied.
        echo          Right-click this file and choose "Run as administrator".
        echo.
        pause
    )
    exit /b
)

echo.
echo   ============================================
echo    mcd captcha_login_worker - Windows deploy
echo   ============================================
echo    Elevated: yes
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_windows.ps1" %*

echo.
pause

@echo off
chcp 65001 >nul
echo ==========================================
echo       Git 拉取最新代码脚本
echo ==========================================

REM 切换到脚本所在目录
cd /d "%~dp0"

echo [1/1] 正在拉取远程最新代码 (git pull)...
git pull
if %errorlevel% neq 0 (
    echo.
    echo [错误] git pull 失败，请检查冲突或网络连接。
    pause
    exit /b %errorlevel%
)

echo.
echo ==========================================
echo       代码拉取成功！
echo ==========================================
pause

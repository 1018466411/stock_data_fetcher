@echo off
chcp 65001 >nul
echo ==========================================
echo       Git 自动更新并提交脚本
echo ==========================================

REM 切换到脚本所在目录
cd /d "%~dp0"

echo [1/3] 正在拉取远程最新代码 (git pull)...
git pull
if %errorlevel% neq 0 (
    echo [错误] git pull 失败，请检查冲突或网络连接。
    pause
    exit /b %errorlevel%
)

echo.
echo [2/3] 正在添加所有更改 (git add .)...
git add .

echo.
echo [3/3] 正在提交更改并推送到远程...
set /p commit_msg="请输入提交说明 (直接回车默认使用 'auto update'): "

if "%commit_msg%"=="" (
    set commit_msg=auto update
)

git commit -m "%commit_msg%"
if %errorlevel% neq 0 (
    echo [提示] 没有需要提交的更改。
    pause
    exit /b 0
)

git push
if %errorlevel% neq 0 (
    echo [错误] git push 失败，请检查网络连接或权限。
    pause
    exit /b %errorlevel%
)

echo.
echo ==========================================
echo       代码更新并推送成功！
echo ==========================================
pause

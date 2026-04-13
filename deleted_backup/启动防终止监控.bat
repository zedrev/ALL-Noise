@echo off
chcp 65001 >nul
title 防终止监控

echo ========================================
echo 启动防终止监控
echo ========================================
echo 此程序将防止任务二因长时间运行而被系统终止
echo.

cd /d "%~dp0"

echo 检查Python环境...
python --version
if errorlevel 1 (
    echo 错误: Python未安装或不在PATH中
    pause
    exit /b 1
)

echo 检查psutil包...
python -c "import psutil" 2>nul
if errorlevel 1 (
    echo 正在安装psutil...
    pip install psutil --quiet
)

echo 启动防终止监控...
echo 注意: 不要关闭此窗口
echo.

python keep_alive_monitor.py

echo.
echo 防终止监控已停止
pause
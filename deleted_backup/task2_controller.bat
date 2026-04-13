@echo off
chcp 65001 >nul
echo ========================================
echo 任务二控制器
echo ========================================
echo.

:menu
echo 请选择操作:
echo 1. 启动任务二
echo 2. 暂停任务二
echo 3. 查看任务二状态
echo 4. 清理并重新开始
echo 5. 退出
echo.
set /p choice="请输入选项 (1-5): "

if "%choice%"=="1" goto start_task2
if "%choice%"=="2" goto stop_task2
if "%choice%"=="3" goto check_status
if "%choice%"=="4" goto cleanup_restart
if "%choice%"=="5" goto exit_program
echo 无效选项，请重新输入
goto menu

:start_task2
echo.
echo 启动任务二...
echo 注意：程序将运行在后台
echo 可以使用暂停功能停止
echo.
timeout /t 2 /nobreak >nul
python "day2 运行程序.py"
goto menu

:stop_task2
echo.
echo 暂停任务二...
echo 正在停止进程...
taskkill /f /im python.exe 2>nul
echo 进程已停止
echo.
echo 当前进度已保存
echo 下次启动将从上次进度继续
echo.
pause
goto menu

:check_status
echo.
python check_day2_status.py
echo.
pause
goto menu

:cleanup_restart
echo.
echo 警告：这将删除所有任务二进度！
set /p confirm="确定要清理并重新开始吗？(y/n): "
if /i "%confirm%"=="y" (
    echo 正在清理...
    del progress_day2.pkl 2>nul
    del day2_total_pools.txt 2>nul
    del task2_output.log 2>nul
    del task2_manager.log 2>nul
    del task2_pid.txt 2>nul
    echo 清理完成！可以重新启动任务二
) else (
    echo 取消清理
)
pause
goto menu

:exit_program
echo.
echo 退出任务二控制器
exit
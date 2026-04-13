@echo off
chcp 65001 >nul
echo ========================================
echo 智能任务一增强启动脚本
echo 版本: v2.0 (带详细监控)
echo ========================================
echo.

echo [1] 初始化检查...
echo 检查时间: %date% %time%
echo.

echo [2] 清理环境...
taskkill /f /im python.exe 2>nul
echo 已停止现有Python进程
echo.

echo [3] 检查智能系统文件...
if not exist day1_smart_pv1.py (
    echo 错误: day1_smart_pv1.py 不存在
    pause
    exit /b 1
)

if not exist smart_alpha_generator.py (
    echo 错误: smart_alpha_generator.py 不存在
    pause
    exit /b 1
)

echo 智能系统文件检查通过
echo.

echo [4] 检查进度...
set progress_file=progress_smart_pv1.pkl
set total_file=total_pools_smart_pv1.txt

if exist %progress_file% (
    if exist %total_file% (
        python -c "
import pickle
try:
    with open('progress_smart_pv1.pkl', 'rb') as f:
        progress = pickle.load(f)
    with open('total_pools_smart_pv1.txt', 'r') as f:
        total = int(f.read().strip())
    
    print('检测到现有进度:')
    print('  当前进度: {}/{}'.format(progress, total))
    if total > 0:
        percent = progress / total * 100
        print('  完成比例: {:.1f}%%'.format(percent))
        remaining = total - progress
        print('  剩余任务池: {}'.format(remaining))
        
        if remaining > 0:
            print('  程序将从当前进度继续运行')
        else:
            print('  任务已完成！')
            print('  如需重新运行，请删除进度文件')
    else:
        print('  总任务池未知，将重新检查')
except Exception as e:
    print('读取进度失败:', e)
    print('将从头开始运行')
"
        echo.
        set /p confirm="是否继续从当前进度运行？(Y/N): "
        if /i "%confirm%" neq "Y" (
            echo 取消运行
            pause
            exit /b 0
        )
    ) else (
        echo 无总任务池文件，将重新开始
    )
) else (
    echo 无进度文件，将从头开始
)

echo.
echo [5] 启动监控脚本...
start cmd /k "python monitor_smart_task1.py"

echo.
echo [6] 启动智能任务一主程序...
echo 程序将在新窗口中运行，请查看监控窗口了解进度
echo 启动命令: python day1_smart_pv1.py
echo.

timeout /t 5 /nobreak

echo 正在启动主程序...
start cmd /k "python day1_smart_pv1.py"

echo.
echo ========================================
echo 启动完成！
echo 现在有两个窗口：
echo 1. 监控窗口 - 显示实时进度
echo 2. 主程序窗口 - 运行智能任务一
echo ========================================
echo.

pause
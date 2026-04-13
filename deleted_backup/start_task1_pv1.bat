@echo off
chcp 65001 >nul
echo ========================================
echo 启动任务一（使用Price Volume Data for Equity）
echo ========================================
echo.

echo 配置信息:
echo • 数据集: Price Volume Data for Equity (pv1)
echo • 区域: USA
echo • 股票池: TOP3000
echo • 目标: 1000个一阶alpha
echo.

echo [1] 检查当前状态...
if exist "progress.pkl" (
    python -c "
import pickle
try:
    with open('progress.pkl', 'rb') as f:
        progress = pickle.load(f)
    print('当前进度:', progress)
    print('注意：这是基于pv1数据集的新进度')
except Exception as e:
    print('读取进度失败:', e)
"
) else (
    echo 无进度文件，任务一将从头开始
)

echo.
echo [2] 启动任务一程序...
echo 注意：程序将运行在后台
echo 可以使用 Ctrl+C 停止程序
echo.

timeout /t 3 /nobreak >nul

REM 启动任务一程序
python "day1运行程序.py"

echo.
echo ========================================
echo 任务一程序运行结束
echo ========================================
echo.

REM 显示最终状态
if exist "progress.pkl" (
    python -c "
import pickle
try:
    with open('progress.pkl', 'rb') as f:
        progress = pickle.load(f)
    print('最终进度:', progress)
    print('任务一已完成进度: {}/1000个alpha'.format(progress * 3))
except Exception as e:
    print('读取最终进度失败:', e)
"
)

echo.
pause
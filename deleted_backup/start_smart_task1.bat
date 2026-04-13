@echo off
chcp 65001 >nul
echo ========================================
echo 启动智能任务一（pv1数据集优化版）
echo ========================================
echo.

echo 智能系统配置:
echo • 数据集: Price Volume Data for Equity (pv1)
echo • 策略: balanced（平衡策略）
echo • 智能功能: 字段分类 + 操作符优化
echo • 目标: 1000个智能alpha
echo.

echo [1] 检查当前状态...
if exist "progress_smart_pv1.pkl" (
    if exist "total_pools_smart_pv1.txt" (
        python -c "
import pickle
try:
    with open('progress_smart_pv1.pkl', 'rb') as f:
        progress = pickle.load(f)
    with open('total_pools_smart_pv1.txt', 'r') as f:
        total = int(f.read().strip())
    print('当前进度: {}/{} (完成比例: {:.1f}%%)'.format(progress, total, progress/total*100))
    if progress >= total:
        echo 智能任务一已完成！无需重新运行。
        echo 如需重新运行，请删除 progress_smart_pv1.pkl 和 total_pools_smart_pv1.txt 文件
        pause
        exit
    else:
        echo 智能任务一未完成，继续运行...
except Exception as e:
    print('读取进度失败:', e)
    echo 继续运行智能任务一...
"
    ) else (
        echo 无总任务池文件，重新开始
    )
) else (
    echo 无进度文件，任务一将从头开始
)

echo.
echo [2] 启动智能任务一程序...
echo 注意：程序将运行在后台
echo 可以使用 Ctrl+C 停止程序
echo.

echo 正在启动智能alpha生成系统...
timeout /t 3 /nobreak >nul

REM 启动智能任务一程序
python "day1_smart_pv1.py"

echo.
echo ========================================
echo 智能任务一程序运行结束
echo ========================================
echo.

REM 显示最终状态
if exist "progress_smart_pv1.pkl" (
    if exist "total_pools_smart_pv1.txt" (
        python -c "
import pickle
try:
    with open('progress_smart_pv1.pkl', 'rb') as f:
        progress = pickle.load(f)
    with open('total_pools_smart_pv1.txt', 'r') as f:
        total = int(f.read().strip())
    print('最终进度: {}/{} (完成比例: {:.1f}%%)'.format(progress, total, progress/total*100))
    if progress >= total:
        print('恭喜！智能任务一已完成！')
        print('请前往WorldQuant网站查看结果')
    else:
        print('智能任务一未完成')
        print('可以重新运行此脚本继续')
except Exception as e:
    print('读取最终进度失败:', e)
"
    )
)

echo.
pause
@echo off
chcp 65001 >nul
echo ========================================
echo 简化版任务一启动脚本
echo 针对pv1数据集的基础版本
echo ========================================
echo.

echo 配置信息:
echo 数据集: Price Volume Data for Equity (pv1)
echo 目标alpha数量: 500个
echo 进度文件: progress_simple_pv1.pkl
echo.

echo 检查现有进度...
if exist progress_simple_pv1.pkl (
    if exist total_pools_simple_pv1.txt (
        python -c "
import pickle
try:
    with open('progress_simple_pv1.pkl', 'rb') as f:
        progress = pickle.load(f)
    with open('total_pools_simple_pv1.txt', 'r') as f:
        total = int(f.read().strip())
    
    if total > 0:
        percent = progress / total * 100
        print('当前进度: {}/{} ({:.1f}%%)'.format(progress, total, percent))
        if progress >= total:
            echo 任务已完成!
            echo 如需重新运行，请删除进度文件
            pause
            exit
        else:
            echo 将从当前进度继续运行
    else:
        echo 总任务池未知，将重新开始
except Exception as e:
    print('读取进度失败:', e)
    echo 将从头开始运行
"
    ) else (
        echo 无总任务池文件，将从头开始
    )
) else (
    echo 无进度文件，将从头开始
)

echo.
echo 启动简化版任务一...
echo 程序将:
echo 1. 登录WorldQuant API
echo 2. 获取pv1数据集字段
echo 3. 生成500个基础alpha
echo 4. 进行模拟回测
echo.
echo 注意: 此版本去掉了智能系统，确保稳定运行
echo 可以使用 Ctrl+C 停止程序
echo.

cd /d "%~dp0"
python day1_simple_pv1.py

echo.
echo ========================================
echo 程序执行完成
echo 查看日志: task1_simple_pv1.log
echo 查看进度: python -c "
try:
    import pickle
    with open('progress_simple_pv1.pkl', 'rb') as f:
        progress = pickle.load(f)
    with open('total_pools_simple_pv1.txt', 'r') as f:
        total = int(f.read().strip())
    print('最终进度: {}/{} ({:.1f}%%)'.format(progress, total, progress/total*100))
except:
    print('无法读取最终进度')
"
echo ========================================
echo.

pause
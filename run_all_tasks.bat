@echo off
chcp 65001 >nul
title WorldQuant Task Manager

:main_menu
cls
echo.
echo ========================================
echo       WorldQuant Task Manager
echo ========================================
echo.
echo Please select task:
echo.
echo   [1] Task 1: Generate 1000 first-order Alphas
echo   [2] Task 2: Use Task 1 Alphas for simulation
echo   [3] Task 3: High-order Alpha generation
echo   [4] Run all tasks sequentially
echo   [5] Check current progress
echo   [6] Clear all process files
echo   [0] Exit
echo.
set /p choice="Enter choice (0-6): "

if "%choice%"=="1" goto task1
if "%choice%"=="2" goto task2
if "%choice%"=="3" goto task3
if "%choice%"=="4" goto run_all
if "%choice%"=="5" goto check_progress
if "%choice%"=="6" goto clear_process
if "%choice%"=="0" goto exit
goto main_menu

:task1
cls
title Task 1: Generate 1000 first-order Alphas
echo.
echo ========================================
echo       Task 1 Start Script
echo ========================================
echo.
echo Function description:
echo   1. Run day1运行程序.py
echo   2. Generate 1000 first-order Alphas
echo   3. Support field filtering and multiple datasets
echo   4. After generating Alphas, you can choose:
echo       4.1. Do platform backtest (5-6 hours)
echo       4.2. Skip backtest and start Task 2 immediately
echo       4.3. Just save Alpha list and exit
echo.
echo Generated Alphas will be saved to: day1_alpha_list.pkl
echo.
set /p confirm="Start Task 1? (y/n): "
if /i not "%confirm%"=="y" goto main_menu

echo.
echo Starting Task 1...
echo Do not close this window
echo Press Ctrl+C to safely interrupt
echo.
python "day1运行程序.py"

echo.
echo ========================================
echo       Task 1 Completed
echo ========================================
echo.
if exist day1_alpha_list.pkl (
    echo Successfully generated Alpha list: day1_alpha_list.pkl
    echo.
    set /p next="Continue to Task 2? (y/n): "
    if /i "%next%"=="y" goto task2_auto
    echo.
    echo Next step suggestion:
    echo   1. Run Task 2 using these Alphas
    echo   2. Or use Task Manager to run all tasks
    echo.
) else (
    echo No Alpha list generated, task may not be completed
    echo Can rerun or check for errors
)

echo.
pause
goto main_menu

:task2_auto
cls
echo.
echo ========================================
echo       Task 2: Second-order Alpha Generation
echo ========================================
echo.
echo Detected Task 1 Alpha list: day1_alpha_list.pkl
echo Will prune first-order Alphas to generate second-order Alphas
echo Using group operators: group_neutralize, group_rank, group_zscore
echo.
set /p confirm="Start Task 2? (y/n): "
if /i not "%confirm%"=="y" goto main_menu

echo.
echo Starting Task 2...
echo Do not close this window
echo Press Ctrl+C to safely interrupt
echo.
timeout /t 3 >nul
python "day2 运行程序.py"

echo.
echo ========================================
echo       Task 2 Completed
echo ========================================
echo.
set /p next="Continue to Task 3? (y/n): "
if /i "%next%"=="y" goto task3_auto

echo.
pause
goto main_menu

:task2
cls
echo.
echo ========================================
echo       Task 2: Second-order Alpha Generation
echo ========================================
echo.
echo Function description:
echo   1. Prune and filter first-order Alphas
echo   2. Generate second-order Alpha expressions
echo   3. Using group operators: group_neutralize, group_rank, group_zscore
echo   4. Can automatically get first-order Alphas from date range
echo   5. Or use Task 1 generated day1_alpha_list.pkl
echo.
if exist day1_alpha_list.pkl (
    echo Detected Alpha list: day1_alpha_list.pkl
    echo File size: 
    for %%F in (day1_alpha_list.pkl) do echo   %%~zF bytes
    echo.
    set /p confirm="Use these Alphas to start Task 2? (y/n): "
) else (
    echo Warning: day1_alpha_list.pkl not found
    echo Please run Task 1 first to generate Alphas
    echo.
    pause
    goto main_menu
)

if /i not "%confirm%"=="y" goto main_menu

echo.
echo Starting Task 2...
echo Do not close this window
echo Press Ctrl+C to safely interrupt
echo.
timeout /t 3 >nul
python "day2 运行程序.py"

echo.
echo ========================================
echo       Task 2 Completed
echo ========================================
echo.
set /p next="Continue to Task 3? (y/n): "
if /i "%next%"=="y" goto task3

echo.
pause
goto main_menu

:task3_auto
cls
echo.
echo ========================================
echo       Task 3: High-order Alpha Generation
echo ========================================
echo.
echo Will start Task 3: High-order Alpha Generation
echo.
set /p confirm="Start Task 3? (y/n): "
if /i not "%confirm%"=="y" goto main_menu

echo.
echo Starting Task 3...
echo Do not close this window
echo Press Ctrl+C to safely interrupt
echo.
timeout /t 3 >nul
python "day3 运行程序.py"

echo.
echo ========================================
echo       All Tasks Completed!
echo ========================================
echo.
pause
goto main_menu

:task3
cls
echo.
echo ========================================
echo       Task 3: High-order Alpha Generation
echo ========================================
echo.
echo Function description:
echo   1. Generate high-order Alpha expressions
echo   2. More complex strategy combinations
echo   3. Based on Task 1 and Task 2 results
echo.
set /p confirm="Start Task 3? (y/n): "
if /i not "%confirm%"=="y" goto main_menu

echo.
echo Starting Task 3...
echo Do not close this window
echo Press Ctrl+C to safely interrupt
echo.
timeout /t 3 >nul
python "day3 运行程序.py"

echo.
echo ========================================
echo       Task 3 Completed
echo ========================================
echo.
pause
goto main_menu

:run_all
cls
title Run All Tasks Sequentially
echo.
echo ========================================
echo       Run All Tasks Sequentially
echo ========================================
echo.
echo Will run in sequence:
echo   1. Task 1 (5-6 hours)
echo   2. Task 2 (based on Task 1 results)
echo   3. Task 3 (high-order Alphas)
echo.
echo Total runtime: approx 10-12 hours
echo Will pause after each stage to ask if continue
echo.
set /p confirm="Start running all tasks sequentially? (y/n): "
if /i not "%confirm%"=="y" goto main_menu

echo.
echo Starting Task 1...
echo Stage 1: Generate 1000 first-order Alphas
echo Do not close this window
echo Press Ctrl+C to safely interrupt
echo.
python "day1运行程序.py"

echo.
echo ========================================
echo       Task 1 Completed, checking results...
echo ========================================
echo.
if exist day1_alpha_list.pkl (
    echo Successfully generated Alpha list: day1_alpha_list.pkl
    echo.
    set /p next="Continue to Task 2? (y/n): "
    if /i not "%next%"=="y" goto all_complete
    echo.
    echo Starting Task 2...
    echo Stage 2: Prune first-order Alphas to generate second-order Alphas
    echo.
    python "day2 运行程序.py"
    
    echo.
    echo ========================================
    echo       Task 2 Completed
    echo ========================================
    echo.
    set /p next="Continue to Task 3? (y/n): "
    if /i not "%next%"=="y" goto all_complete
    
    echo.
    echo Starting Task 3...
    echo Stage 3: High-order Alpha Generation
    echo.
    python "day3 运行程序.py"
) else (
    echo No Alpha list generated, skipping Task 2 and Task 3
    echo Task may not be completed, can rerun or check errors
)

:all_complete
echo.
echo ========================================
echo       All Tasks Completed!
echo ========================================
echo.
echo Task execution status:
if exist day1_alpha_list.pkl (
    echo   1. Task 1: Completed (generated day1_alpha_list.pkl)
) else (
    echo   1. Task 1: Not completed or not started
)

if exist progress_day2.pkl (
    echo   2. Task 2: Completed (generated progress_day2.pkl)
) else (
    echo   2. Task 2: Not completed or not started
)

if exist progress_day3.pkl (
    echo   3. Task 3: Completed (generated progress_day3.pkl)
) else (
    echo   3. Task 3: Not completed or not started
)

echo.
echo Suggested actions:
echo   1. View generated Alphas: type day1_fixed.log (if exists)
echo   2. Rerun single task: return to main menu and select
echo   3. Check progress: select option 5
echo.
pause
goto main_menu

:check_progress
cls
title Check Current Progress
echo.
echo ========================================
echo       Check Current Progress
echo ========================================
echo.
echo File status:
echo.
echo Core result files:
if exist day1_alpha_list.pkl (
    for %%F in (day1_alpha_list.pkl) do echo   [Exists] day1_alpha_list.pkl (%%~zF bytes)
) else (
    echo   [Missing] day1_alpha_list.pkl (Task 1 result)
)

echo.
echo Task progress files:
if exist progress_day2.pkl (
    for %%F in (progress_day2.pkl) do echo   [Exists] progress_day2.pkl (%%~zF bytes)
) else (
    echo   [Missing] progress_day2.pkl (Task 2 progress)
)

if exist progress_day3.pkl (
    for %%F in (progress_day3.pkl) do echo   [Exists] progress_day3.pkl (%%~zF bytes)
) else (
    echo   [Missing] progress_day3.pkl (Task 3 progress)
)

echo.
echo Process folder status:
if exist process_files (
    dir /b process_files\*.pkl 2>nul | findstr "." >nul && (
        echo   [Exists] process_files\*.pkl
        echo   Contains: day1_progress.pkl and other process files
    ) || (
        echo   [Exists] process_files\ folder (empty)
    )
) else (
    echo   [Missing] process_files folder
)

echo.
echo Other related files:
dir /b *.log 2>nul | findstr "." >nul && (
    echo   [Exists] log files (*.log)
) || (
    echo   [Missing] log files
)

echo.
echo Suggested actions:
if not exist day1_alpha_list.pkl (
    echo   1. Run Task 1 first to generate Alphas (option 1 or 4)
) else if not exist progress_day2.pkl (
    echo   1. Can run Task 2 using generated Alphas (option 2)
    echo   2. Or use Task 1 Alphas to continue (option 4)
) else if not exist progress_day3.pkl (
    echo   1. Can run Task 3 for high-order generation (option 3)
    echo   2. Or continue to complete all tasks (option 4)
) else (
    echo   1. All tasks may be completed
    echo   2. Can rerun single tasks for optimization
)

echo.
echo Management actions:
echo   1. Clear all process files: select option 6
echo   2. Restart tasks: clear then select option 1
echo.
pause
goto main_menu

:clear_process
cls
title Clear All Process Files
echo.
echo ========================================
echo       Clear All Process Files
echo ========================================
echo.
echo Warning: This will delete all task progress files!
echo.
echo Will delete the following files:
echo.
echo Task progress files:
if exist progress_day2.pkl (
    echo   [Delete] progress_day2.pkl
) else (
    echo   [Missing] progress_day2.pkl
)

if exist progress_day3.pkl (
    echo   [Delete] progress_day3.pkl
) else (
    echo   [Missing] progress_day3.pkl
)

echo.
echo Process folder:
if exist process_files (
    echo   [Clear] process_files\ folder
    echo   Contains: day1_progress.pkl and other temporary files
) else (
    echo   [Missing] process_files folder
)

echo.
echo Other files:
if exist day2_total_pools.txt (
    echo   [Delete] day2_total_pools.txt
) else (
    echo   [Missing] day2_total_pools.txt
)

if exist day3_total_pools.txt (
    echo   [Delete] day3_total_pools.txt
) else (
    echo   [Missing] day3_total_pools.txt
)

echo.
set /p confirm="Confirm to delete all process files? (y/n): "
if /i not "%confirm%"=="y" (
    echo.
    echo Operation cancelled
    echo.
    pause
    goto main_menu
)

echo.
echo Deleting files...
echo.

:: Delete day2 progress file
if exist progress_day2.pkl (
    del progress_day2.pkl
    echo   Deleted: progress_day2.pkl
)

:: Delete day3 progress file
if exist progress_day3.pkl (
    del progress_day3.pkl
    echo   Deleted: progress_day3.pkl
)

:: Delete day2 task pool file
if exist day2_total_pools.txt (
    del day2_total_pools.txt
    echo   Deleted: day2_total_pools.txt
)

:: Delete day3 task pool file
if exist day3_total_pools.txt (
    del day3_total_pools.txt
    echo   Deleted: day3_total_pools.txt
)

:: Clear process folder
if exist process_files (
    del process_files\*.pkl 2>nul
    del process_files\*.tmp 2>nul
    del process_files\*.log 2>nul
    echo   Cleared: process_files\ folder
)

:: Delete possible old version day1 progress file
if exist progress.pkl (
    del progress.pkl
    echo   Deleted: progress.pkl (old version)
)

echo.
echo ========================================
echo       All Process Files Cleared
echo ========================================
echo.
echo Note: Following files are preserved:
echo   1. day1_alpha_list.pkl (Task 1 result)
echo   2. 安全启动指南.md
echo   3. Core program files (*.py)
echo.
echo Now can restart tasks.
echo.
pause
goto main_menu

:exit
cls
echo.
echo ========================================
echo       Thank you for using WorldQuant Task Manager
echo ========================================
echo.
echo Program exited
echo.
timeout /t 2 >nul
exit
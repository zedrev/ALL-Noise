#!/usr/bin/env python3
"""
只保留关键部分文件
"""

import os
import shutil

def list_and_clean():
    print("=== 保留关键文件清理工具 ===")
    print()
    
    # 关键文件列表（必须保留）
    essential_files = [
        # 主程序
        'day1运行程序.py',
        'day1_smart_pv1.py',
        'day2 运行程序.py',
        'day2_fixed.py',
        'day3 运行程序.py',
        'machine_lib.py',
        
        # 智能系统核心
        'smart_alpha_generator.py',
        'smart_field_processor.py',
        'smart_operator_allocator.py',
        
        # 进度文件
        'progress_smart_pv1.pkl',
        'progress_day2.pkl',
        'total_pools_smart_pv1.txt',
        'total_pools_day2_fixed.txt',
        
        # 监控脚本
        'check_task1_status.py',
        'check_day2_status.py',
        'check_day3_status.py',
        
        # 批处理文件
        'task_manager.bat',
        'start_day3_safely.bat',
        'stop_all_tasks.bat',
        
        # 文档
        '安全启动指南.md',
        'README_ME_FIRST.txt',
    ]
    
    # 可选文件（可以删除但有用）
    optional_files = [
        'day1_simple_pv1.py',
        'day2_simple.py',
        'day2_use_today.py',
        'day2_pv1_only.py',
        'monitor_smart_task1.py',
        'monitor_task3.py',
        'task2_controller.bat',
        'start_smart_task1.bat',
        'start_smart_task1_enhanced.bat',
        'start_simple_task1.bat',
        'start_task1_pv1.bat',
        'test_smart_system_simple.py',
    ]
    
    # 创建备份文件夹
    backup_dir = 'deleted_backup'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    print("当前目录文件:")
    files = os.listdir('.')
    files.sort()
    
    print()
    print("将保留以下关键文件:")
    kept_count = 0
    for f in files:
        if f in essential_files:
            if os.path.exists(f):
                size = os.path.getsize(f)
                print(f"[保留] {f} ({size}字节)")
                kept_count += 1
            else:
                print(f"[不存在] {f}")
    
    print()
    print("将移动以下文件到备份文件夹:")
    moved_count = 0
    
    for f in files:
        # 跳过备份文件夹本身
        if f == backup_dir:
            continue
        
        # 跳过关键文件
        if f in essential_files:
            continue
        
        # 跳过Python缓存文件夹
        if f == '__pycache__':
            continue
        
        # 处理可选文件和其他文件
        if os.path.isfile(f):
            try:
                # 移动到备份文件夹
                target = os.path.join(backup_dir, f)
                if os.path.exists(target):
                    # 如果已存在，添加时间戳
                    import time
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    name, ext = os.path.splitext(f)
                    target = os.path.join(backup_dir, f"{name}_{timestamp}{ext}")
                
                shutil.move(f, target)
                print(f"[移动] {f} -> {os.path.basename(target)}")
                moved_count += 1
            except Exception as e:
                print(f"[移动失败] {f}: {e}")
    
    print()
    print("=" * 50)
    print(f"清理完成:")
    print(f"- 保留: {kept_count} 个关键文件")
    print(f"- 移动: {moved_count} 个文件到备份文件夹")
    print(f"- 备份文件夹: {backup_dir}")
    print()
    print("现在工作空间只包含最关键的运行文件。")
    print("可以从当前状态继续任务。")

if __name__ == "__main__":
    list_and_clean()
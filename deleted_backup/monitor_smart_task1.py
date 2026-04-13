#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能任务一监控脚本
实时监控智能任务一的运行状态
"""

import os
import time
import pickle
import subprocess

def monitor_smart_task1():
    print("智能任务一监控系统")
    print("=" * 60)
    print("开始时间:", time.strftime("%Y-%m-%d %H:%M:%S"))
    print()
    
    # 监控计数器
    cycle_count = 0
    last_progress = -1
    
    # 监控文件
    progress_file = 'progress_smart_pv1.pkl'
    total_file = 'total_pools_smart_pv1.txt'
    log_file = 'task1_smart_output.log'
    
    while True:
        cycle_count += 1
        current_time = time.strftime("%H:%M:%S")
        
        print(f"[监控周期 {cycle_count}] {current_time}")
        print("-" * 40)
        
        # 1. 检查进程状态
        try:
            result = subprocess.run(['powershell', '-Command', 'Get-Process python -ErrorAction SilentlyContinue'], 
                                  capture_output=True, text=True, shell=True)
            if 'python' in result.stdout.lower():
                print("[状态] Python进程正在运行")
            else:
                print("[警告] 未发现Python进程，任务可能已停止")
        except:
            print("[状态] 无法检查进程状态")
        
        # 2. 检查进度文件
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'rb') as f:
                    current_progress = pickle.load(f)
                
                if last_progress == -1:
                    last_progress = current_progress
                
                progress_changed = current_progress != last_progress
                
                if os.path.exists(total_file):
                    with open(total_file, 'r') as f:
                        total_pools = int(f.read().strip())
                    
                    if total_pools > 0:
                        percent = current_progress / total_pools * 100
                        print(f"[进度] {current_progress}/{total_pools} (完成比例: {percent:.1f}%)")
                        print(f"[剩余] {total_pools - current_progress} 个任务池")
                        
                        # 计算进度变化
                        if progress_changed:
                            progress_delta = current_progress - last_progress
                            print(f"[变化] +{progress_delta} 任务池")
                            last_progress = current_progress
                            
                            # 估算剩余时间
                            if cycle_count > 3 and progress_delta > 0:
                                remaining_pools = total_pools - current_progress
                                if remaining_pools > 0:
                                    estimated_seconds = remaining_pools * 60  # 假设每个池1分钟
                                    if estimated_seconds > 3600:
                                        hours = estimated_seconds // 3600
                                        minutes = (estimated_seconds % 3600) // 60
                                        print(f"[预计] 剩余约 {hours}小时{minutes}分钟")
                                    else:
                                        minutes = estimated_seconds // 60
                                        print(f"[预计] 剩余约 {minutes}分钟")
                    else:
                        print(f"[进度] {current_progress} (总任务池未知)")
                else:
                    print(f"[进度] {current_progress} (总任务池文件不存在)")
                    
            except Exception as e:
                print(f"[错误] 读取进度失败: {e}")
        else:
            print("[状态] 进度文件不存在 (程序可能还在初始化)")
        
        # 3. 检查日志文件
        if os.path.exists(log_file):
            try:
                size = os.path.getsize(log_file)
                print(f"[日志] task1_smart_output.log ({size:,}字节)")
                
                # 显示最后几行
                if size > 0 and size < 50000:
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                    
                    if lines:
                        last_lines = lines[-5:]  # 最后5行
                        print("[日志最后5行]:")
                        for line in last_lines:
                            line = line.strip()
                            if line:
                                print(f"  {line}")
            except Exception as e:
                print(f"[错误] 读取日志失败: {e}")
        
        # 4. 显示系统时间
        uptime = time.time()
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        print(f"[运行时间] {hours:02d}:{minutes:02d}")
        
        print()
        print("=" * 60)
        
        # 等待下一轮监控
        print("等待下一轮监控 (10秒后刷新)...")
        for i in range(10, 0, -1):
            print(f"\r倒计时: {i:2d}秒", end='', flush=True)
            time.sleep(1)
        
        print()
        print()

if __name__ == "__main__":
    try:
        monitor_smart_task1()
    except KeyboardInterrupt:
        print()
        print("监控已停止")
        print("智能任务一仍在后台运行")
        print("可以运行 python check_task1_status.py 查看最新状态")
    except Exception as e:
        print(f"监控错误: {e}")
        print("请检查程序状态")
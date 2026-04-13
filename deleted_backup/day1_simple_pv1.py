#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版任务一 - 针对pv1数据集
去掉了智能系统，使用基础功能确保能运行
"""

import os
import sys
import pickle
import random
import logging
import time
import signal
from datetime import datetime

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from machine_lib import (
    login, get_datafields, process_datafields, 
    ts_factory, first_order_factory, ts_ops,
    get_alphas, load_task_pool_single, single_simulate
)

# 参数配置
DATASET_ID = 'pv1'  # Price Volume Data for Equity
REGION = 'USA'
UNIVERSE = 'TOP3000'
DELAY = 1
INIT_DECAY = 6
POOL_SIZE = 3
TARGET_ALPHA_COUNT = 500  # 简化目标，减少API调用

# 进度文件
PROGRESS_FILE = 'progress_simple_pv1.pkl'
TOTAL_POOLS_FILE = 'total_pools_simple_pv1.txt'

# 全局变量
interrupted = False

def signal_handler(signum, frame):
    """处理中断信号"""
    global interrupted
    interrupted = True
    logging.info("收到中断信号，正在安全退出...")

def setup_session():
    """设置会话，包含错误处理"""
    try:
        s = login()
        logging.info("登录成功")
        return s
    except Exception as e:
        logging.error(f"登录失败: {e}")
        raise

def check_completion_status():
    """检查完成状态"""
    if os.path.exists(PROGRESS_FILE) and os.path.exists(TOTAL_POOLS_FILE):
        try:
            with open(PROGRESS_FILE, 'rb') as f:
                progress = pickle.load(f)
            with open(TOTAL_POOLS_FILE, 'r') as f:
                total = int(f.read().strip())
            
            if progress >= total:
                return True
        except:
            pass
    return False

def save_progress(progress):
    """保存进度"""
    try:
        with open(PROGRESS_FILE, 'wb') as f:
            pickle.dump(progress, f)
        logging.debug(f"进度已保存: {progress}")
    except Exception as e:
        logging.error(f"保存进度失败: {e}")

def save_total_pools(total):
    """保存总任务池"""
    try:
        with open(TOTAL_POOLS_FILE, 'w') as f:
            f.write(str(total))
        logging.debug(f"总任务池已保存: {total}")
    except Exception as e:
        logging.error(f"保存总任务池失败: {e}")

def main():
    global interrupted
    
    # 检查完成状态
    print("=" * 60)
    print("简化版任务一：针对pv1数据集")
    print(f"数据集: {DATASET_ID}")
    print(f"目标alpha数量: {TARGET_ALPHA_COUNT}")
    print("=" * 60)
    
    if check_completion_status():
        print("程序已完成所有任务，无需再次运行。")
        print(f"如需重新运行，请删除 {PROGRESS_FILE} 和 {TOTAL_POOLS_FILE} 文件")
        return
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 登录
        s = setup_session()
        if interrupted: return
        
        print("\n[阶段1] 获取数据字段并生成alpha...")
        print(f"数据集: {DATASET_ID}")
        
        # 获取数据字段
        df = get_datafields(s, dataset_id=DATASET_ID)
        
        if df.empty:
            logging.error("无法获取数据字段")
            print("错误：无法获取数据字段，可能达到API限制")
            print("请等待几分钟后重试")
            return
        
        # 处理字段
        datafields = process_datafields(df)
        print(f"获取到 {len(datafields)} 个数据字段")
        
        # 生成alpha
        print(f"使用 {len(ts_ops)} 个时间序列操作生成alpha...")
        fo_alpha_list = first_order_factory(datafields, ts_ops)
        
        if not fo_alpha_list:
            logging.error("alpha生成失败")
            return
            
        logging.info(f"生成了 {len(fo_alpha_list)} 个alpha")
        
        # 随机化
        random.seed(42)
        random.shuffle(fo_alpha_list)
        
        # 限制数量
        if len(fo_alpha_list) > TARGET_ALPHA_COUNT:
            fo_alpha_list = fo_alpha_list[:TARGET_ALPHA_COUNT]
        
        print(f"最终alpha数量: {len(fo_alpha_list)}")
        
        if interrupted: return
        
        # 加载任务池
        print("\n[阶段2] 创建任务池...")
        n = len(fo_alpha_list)
        total_pools = n // POOL_SIZE
        if n % POOL_SIZE != 0:
            total_pools += 1
        
        # 保存总任务池
        save_total_pools(total_pools)
        
        # 加载进度
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'rb') as f:
                pool_num = pickle.load(f)
        else:
            pool_num = 0
        
        print(f"总任务池数: {total_pools}")
        print(f"当前进度: {pool_num}/{total_pools}")
        
        if interrupted: return
        
        # 使用原始方法创建任务池和模拟
        print("\n[阶段3] 创建任务池并开始模拟...")
        fo_pools = load_task_pool_single(fo_alpha_list, POOL_SIZE)
        total_pools = len(fo_pools)
        
        # 保存总任务池
        save_total_pools(total_pools)
        
        print(f"创建了 {total_pools} 个任务池")
        print(f"每个任务池包含 {POOL_SIZE} 个alpha")
        
        # 加载进度
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'rb') as f:
                processed = pickle.load(f)
        else:
            processed = 0
        
        print(f"当前进度: {processed}/{total_pools}")
        
        if interrupted: return
        
        try:
            for i, pool in enumerate(fo_pools):
                if i < processed:
                    continue
                    
                print(f"\n处理任务池 {i+1}/{total_pools}")
                single_simulate([pool], "no", REGION, UNIVERSE, i)
                
                # 更新进度
                processed = i + 1
                save_progress(processed)
                
                # 显示进度
                percent = processed / total_pools * 100
                print(f"进度: {processed}/{total_pools} ({percent:.1f}%)")
                
                if interrupted:
                    break
                    
        except Exception as e:
            logging.error(f"模拟过程中出错: {e}")
            save_progress(processed)
        
        if interrupted:
            print("\n程序被中断，进度已保存")
        else:
            print("\n" + "=" * 60)
            print("恭喜！简化版任务一已完成！")
            print(f"总共处理了 {pool_num} 个任务池")
            print("请前往WorldQuant网站查看结果")
            print("=" * 60)
            
    except Exception as e:
        logging.error(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n程序结束")

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('task1_simple_pv1.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    main()
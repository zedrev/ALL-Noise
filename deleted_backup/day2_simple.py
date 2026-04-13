#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版任务二 - 基于智能任务一的结果
去掉tqdm等复杂依赖，确保稳定运行
"""

import logging
import random
import os
import signal
import sys
import pickle
import time
from machine_lib import login, get_alphas, prune, get_group_second_order_factory, load_task_pool_single, single_simulate

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# 参数配置
START_DATE = "03-25"    # 开始日期（昨天 - 2026-03-25）
END_DATE = "03-26"      # 结束日期（今天 - 2026-03-26）
THRESHOLD_HIGH = 1.0    # Sharpe阈值上限
THRESHOLD_LOW = 0.7     # Sharpe阈值下限
REGION = 'USA'
TOP_N = 100             # 获取alpha数量
PRUNE_TYPE = 'anl4'
PRUNE_COUNT = 5
INIT_DECAY = 6          # 初始decay值
POOL_SIZE = 3

# 进度文件
PROGRESS_FILE = 'progress_day2_simple.pkl'
TOTAL_POOLS_FILE = 'total_pools_day2_simple.txt'

# 全局变量
interrupted = False

def signal_handler(signum, frame):
    """处理中断信号"""
    global interrupted
    interrupted = True
    logging.info("收到中断信号，正在安全退出...")

def setup_session():
    """设置会话"""
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
    
    print("=" * 60)
    print("简化版任务二：二阶alpha生成")
    print(f"日期范围: {START_DATE} 到 {END_DATE}")
    print("=" * 60)
    
    if check_completion_status():
        print("任务二已完成，无需再次运行。")
        print(f"如需重新运行，请删除 {PROGRESS_FILE} 和 {TOTAL_POOLS_FILE} 文件")
        return
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 登录
        s = setup_session()
        if interrupted: return
        
        print("\n[阶段1] 获取一阶alpha...")
        
        # 首先检查是否有任务一保存的alpha列表
        alpha_source = ""
        if os.path.exists('day1_alpha_list.pkl'):
            print("使用任务一保存的alpha列表...")
            with open('day1_alpha_list.pkl', 'rb') as f:
                first_order_alphas = pickle.load(f)
            alpha_source = "任务一保存"
        else:
            print("从API获取昨天生成的一阶alpha...")
            # 获取昨天生成的一阶alpha
            first_order_alphas = get_alphas(
                START_DATE, END_DATE, 
                THRESHOLD_HIGH, THRESHOLD_LOW,
                REGION, TOP_N, "alpha"
            )
            alpha_source = "API获取"
        
        if not first_order_alphas:
            logging.error("无法获取一阶alpha")
            return
        
        print(f"获取到 {len(first_order_alphas)} 个一阶alpha ({alpha_source})")
        
        # 剪枝处理
        print(f"应用剪枝 ({PRUNE_TYPE}, 保留{PRUNE_COUNT}个)...")
        pruned_alphas = prune(first_order_alphas, PRUNE_TYPE, PRUNE_COUNT)
        
        if not pruned_alphas:
            logging.error("剪枝后没有alpha")
            return
        
        print(f"剪枝后剩余 {len(pruned_alphas)} 个alpha")
        
        if interrupted: return
        
        print("\n[阶段2] 生成二阶alpha...")
        group_ops = ["group_neutralize", "group_rank", "group_zscore"]
        
        second_order_alphas = []
        for expr, decay in pruned_alphas:
            # 为每个一阶alpha生成二阶alpha
            alphas = get_group_second_order_factory([expr], group_ops, REGION)
            for alpha in alphas:
                second_order_alphas.append((alpha, decay))
        
        if not second_order_alphas:
            logging.error("无法生成二阶alpha")
            return
        
        print(f"生成了 {len(second_order_alphas)} 个二阶alpha")
        
        # 随机化
        random.seed(42)
        random.shuffle(second_order_alphas)
        
        # 限制数量（避免太多）
        MAX_ALPHAS = 300  # 简化版本，减少数量
        if len(second_order_alphas) > MAX_ALPHAS:
            second_order_alphas = second_order_alphas[:MAX_ALPHAS]
            print(f"限制到 {MAX_ALPHAS} 个二阶alpha")
        
        if interrupted: return
        
        print("\n[阶段3] 创建任务池...")
        so_pools = load_task_pool_single(second_order_alphas, POOL_SIZE)
        total_pools = len(so_pools)
        
        # 保存总任务池
        save_total_pools(total_pools)
        
        print(f"创建了 {total_pools} 个任务池")
        print(f"每个任务池包含 {POOL_SIZE} 个二阶alpha")
        
        # 加载进度
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'rb') as f:
                processed = pickle.load(f)
        else:
            processed = 0
        
        print(f"当前进度: {processed}/{total_pools}")
        
        if interrupted: return
        
        print("\n[阶段4] 开始模拟回测...")
        
        try:
            for i in range(processed, total_pools):
                if interrupted:
                    break
                    
                print(f"\n处理任务池 {i+1}/{total_pools}")
                pool = so_pools[i]
                
                # 单个任务池模拟
                single_simulate([pool], "no", REGION, "TOP3000", 0)
                
                # 更新进度
                processed = i + 1
                save_progress(processed)
                
                # 显示进度
                percent = processed / total_pools * 100
                print(f"进度: {processed}/{total_pools} ({percent:.1f}%)")
                
                # 小延时避免API限制
                time.sleep(2)
            
            if interrupted:
                print("\n程序被中断，进度已保存")
            else:
                print("\n" + "=" * 60)
                print("恭喜！简化版任务二已完成！")
                print(f"总共处理了 {processed} 个任务池")
                print(f"生成并回测了 {processed * POOL_SIZE} 个二阶alpha")
                print("请前往WorldQuant网站查看结果")
                print("=" * 60)
                
        except Exception as e:
            logging.error(f"模拟过程中出错: {e}")
            save_progress(processed)
            
    except Exception as e:
        logging.error(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n程序结束")

if __name__ == "__main__":
    main()
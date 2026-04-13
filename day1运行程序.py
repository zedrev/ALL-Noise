#!/usr/bin/env python3
"""
day1运行程序 - 多数据集Alpha生成
支持从多个数据集获取字段，并生成跨数据集逻辑组合Alpha
"""

import logging
import random
import os
import signal
import sys
import pickle
import pandas as pd
from machine_lib import (login, get_datafields, process_datafields, first_order_factory, 
                          load_task_pool_single, single_simulate, get_vec_fields,
                          ALL_DATASETS, cross_dataset_factory)
from tqdm import tqdm

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# [迭代修改点] 参数配置区域
# 以下参数控制 Alpha 生成的核心行为：
#   REGION/UNIVERSE/DELAY - 市场和延迟设置
#   INIT_DECAY - 初始衰减系数
#   POOL_SIZE - 每批并行模拟的alpha数量
#   ALPHA_LIMIT - 最终生成的alpha总上限
REGION = 'USA'
UNIVERSE = 'TOP3000'
DELAY = 1
INIT_DECAY = 6          # [迭代修改点] 初始decay值，影响换手率
POOL_SIZE = 3           # [迭代修改点] 每批并行数，过大可能触发API限流
ALPHA_LIMIT = 1000      # [迭代修改点] alpha生成总量上限

# 多数据集配置
SELECTED_DATASETS = []           # 运行时由用户选择填充
ENABLE_CROSS_DATASET = True     # [迭代修改点] 是否启用跨数据集组合
FIELDS_PER_DATASET = 20         # [迭代修改点] 每个数据集选取的字段数量上限

# 进程文件管理配置
PROCESS_FILES_DIR = 'process_files'
DAY1_PROGRESS_FILE = os.path.join(PROCESS_FILES_DIR, 'day1_progress.pkl')
DAY1_ALPHA_LIST_FILE = 'day1_alpha_list.pkl'

interrupted = False

def signal_handler(sig, frame):
    global interrupted
    logging.warning("Interrupt signal received, stopping...")
    interrupted = True

def setup_process_files():
    if not os.path.exists(PROCESS_FILES_DIR):
        os.makedirs(PROCESS_FILES_DIR)

def save_progress(processed_count):
    setup_process_files()
    with open(DAY1_PROGRESS_FILE, 'wb') as f:
        pickle.dump(processed_count, f)

def load_progress():
    try:
        with open(DAY1_PROGRESS_FILE, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        try:
            old_file = 'progress.pkl'
            if os.path.exists(old_file):
                with open(old_file, 'rb') as f:
                    progress = pickle.load(f)
                save_progress(progress)
                os.remove(old_file)
                return progress
        except:
            pass
        return 0

def check_completion_status(total_pools=334):
    try:
        current_progress = load_progress()
        if current_progress >= total_pools:
            logging.info(f"任务已完成: {current_progress}/{total_pools}")
            return True
        else:
            logging.info(f"任务未完成: {current_progress}/{total_pools}")
            return False
    except Exception as e:
        logging.error(f"检查完成状态时出错: {e}")
        return False

def get_user_config():
    """获取用户配置 - 支持多数据集选择"""
    global SELECTED_DATASETS, ENABLE_CROSS_DATASET, FIELDS_PER_DATASET, ALPHA_LIMIT
    
    print("=" * 60)
    print("          世界量化 - 任务一配置")
    print("=" * 60)
    print()
    
    # 数据集多选
    print("可用数据集:")
    for i, ds in enumerate(ALL_DATASETS, 1):
        print(f"  {i:2d}. {ds['id']:16s} - {ds['desc']}")
    
    print()
    print("输入格式: 用逗号分隔，如 1,3,6 表示选择第1/3/6个数据集")
    print("输入 'all' 选择所有数据集")
    print("输入 'fund' 选择所有基本面类数据集")
    print("输入 'rec' 使用推荐组合 (pv1 + fundamental6 + analyst4 + news18)")
    print()
    
    raw_input = input("请选择数据集: ").strip().lower()
    
    selected_ids = []
    
    if raw_input == 'all':
        selected_ids = [ds['id'] for ds in ALL_DATASETS]
    elif raw_input == 'fund':
        selected_ids = [ds['id'] for ds in ALL_DATASETS if ds['category'] in ('fundamental', 'analyst')]
    elif raw_input == 'rec':
        selected_ids = ['pv1', 'fundamental6', 'analyst4', 'news18']
    else:
        try:
            indices = [int(x.strip()) for x in raw_input.split(',')]
            for idx in indices:
                if 1 <= idx <= len(ALL_DATASETS):
                    selected_ids.append(ALL_DATASETS[idx - 1]['id'])
                else:
                    print(f"  警告: 无效选项 {idx}，已跳过")
        except ValueError:
            print("  输入格式错误，使用默认推荐组合")
            selected_ids = ['pv1', 'fundamental6', 'analyst4', 'news18']
    
    if not selected_ids:
        print("  未选择任何数据集，使用默认推荐组合")
        selected_ids = ['pv1', 'fundamental6', 'analyst4', 'news18']
    
    print(f"\n已选择 {len(selected_ids)} 个数据集:")
    for ds_id in selected_ids:
        ds_info = next((ds for ds in ALL_DATASETS if ds['id'] == ds_id), None)
        desc = ds_info['desc'] if ds_info else '未知'
        print(f"  - {ds_id}: {desc}")
    
    # 每个数据集的字段数量
    print()
    try:
        field_num = input(f"每个数据集选取字段数量 (默认{FIELDS_PER_DATASET}): ").strip()
        if field_num:
            fields_per_ds = int(field_num)
        else:
            fields_per_ds = FIELDS_PER_DATASET
    except:
        fields_per_ds = FIELDS_PER_DATASET
    
    # 是否启用跨数据集组合
    print()
    cross_input = input("是否生成跨数据集组合Alpha? (y/n, 默认y): ").strip().lower()
    enable_cross = cross_input != 'n'
    
    # Alpha数量上限
    print()
    try:
        limit_input = input(f"Alpha生成数量上限 (默认{ALPHA_LIMIT}): ").strip()
        if limit_input:
            alpha_limit = int(limit_input)
        else:
            alpha_limit = ALPHA_LIMIT
    except:
        alpha_limit = ALPHA_LIMIT
    
    # 配置摘要
    print()
    print("=" * 60)
    print("配置摘要:")
    print(f"  数据集数量: {len(selected_ids)}")
    print(f"  每数据集字段数: {fields_per_ds}")
    print(f"  跨数据集组合: {'启用' if enable_cross else '禁用'}")
    print(f"  Alpha上限: {alpha_limit}")
    print("=" * 60)
    print()
    
    # 更新全局变量
    SELECTED_DATASETS = selected_ids
    ENABLE_CROSS_DATASET = enable_cross
    FIELDS_PER_DATASET = fields_per_ds
    ALPHA_LIMIT = alpha_limit
    
    confirm = input("确认配置并开始任务? (y/n): ").strip().lower()
    return confirm == 'y'

def run_day2_immediately():
    print("\n" + "=" * 60)
    print("正在启动任务二...")
    print("=" * 60)
    print()
    
    try:
        if os.path.exists("day2 运行程序.py"):
            import subprocess
            import sys
            
            print("开始执行任务二...")
            print("任务二将使用day1生成的Alpha列表")
            print()
            
            result = subprocess.run([sys.executable, "day2 运行程序.py"], 
                                  capture_output=False, text=True)
            
            if result.returncode == 0:
                print("任务二执行完成!")
            else:
                print(f"任务二执行失败，返回码: {result.returncode}")
        else:
            print("错误: day2 运行程序.py 不存在")
            
    except Exception as e:
        print(f"启动任务二时出错: {e}")
        print("请手动运行: python \"day2 运行程序.py\"")

def main():
    global interrupted, SELECTED_DATASETS, ENABLE_CROSS_DATASET, FIELDS_PER_DATASET, ALPHA_LIMIT
    
    if not get_user_config():
        print("用户取消操作")
        return
    
    print("\n" + "=" * 60)
    print("任务一：多数据集Alpha生成")
    print(f"数据集: {', '.join(SELECTED_DATASETS)}")
    print("=" * 60)
    
    if check_completion_status():
        print("程序已完成所有任务，无需再次运行。")
        print("如需重新运行，请删除进度文件")
        return
    
    print("开始执行...")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        s = login()
        if interrupted: return
        
        # ===== 第一步：从多个数据集获取字段 =====
        logging.info(f"开始从 {len(SELECTED_DATASETS)} 个数据集获取字段...")
        
        dataset_fields = {}
        raw_fields = {}  # 存储原始字段ID用于跨数据集组合
        
        for ds_id in SELECTED_DATASETS:
            try:
                logging.info(f"获取数据集 [{ds_id}] 的字段...")
                df = get_datafields(s, dataset_id=ds_id, region=REGION, universe=UNIVERSE, delay=DELAY)
                
                if df.empty:
                    logging.warning(f"数据集 [{ds_id}] 没有可用字段，跳过")
                    continue
                
                # 按alphaCount取前N个
                if 'alphaCount' in df.columns:
                    df = df.sort_values('alphaCount', ascending=False).head(FIELDS_PER_DATASET)
                    logging.info(f"  [{ds_id}] 按使用频率选取前 {FIELDS_PER_DATASET} 个字段")
                else:
                    logging.info(f"  [{ds_id}] 无alphaCount，使用全部 {len(df)} 个字段")
                
                # 提取原始字段ID（用于跨数据集组合）
                raw_ids = df['id'].tolist() if 'id' in df.columns else []
                if raw_ids:
                    # 对VECTOR字段应用vec处理，MATRIX字段直接使用
                    vector_ids = df[df['type'] == 'VECTOR']['id'].tolist() if 'type' in df.columns else []
                    matrix_ids = df[df['type'] == 'MATRIX']['id'].tolist() if 'type' in df.columns else []
                    processed_raw = matrix_ids + get_vec_fields(vector_ids)
                    raw_fields[ds_id] = processed_raw
                
                # 处理后的字段（用于单数据集一阶Alpha，含winsorize/ts_backfill包装）
                processed = process_datafields(df)
                dataset_fields[ds_id] = processed
                logging.info(f"  [{ds_id}] {len(df)} 个原始字段 -> {len(processed)} 个处理后字段")
                
            except Exception as e:
                logging.error(f"获取数据集 [{ds_id}] 失败: {e}")
                continue
            
            if interrupted: return
        
        if not dataset_fields:
            logging.error("所有数据集获取失败，无法继续")
            return
        
        total_fields = sum(len(v) for v in dataset_fields.values())
        logging.info(f"总共获取 {total_fields} 个字段，来自 {len(dataset_fields)} 个数据集")
        
        # ===== 第二步：生成单数据集一阶Alpha =====
        # [迭代修改点] 一阶Alpha使用的算子列表
        # 注意: 此列表独立于 machine_lib.py 中的 ops_set
        # 修改此处可调整一阶Alpha的算子组合
        ts_ops = ["ts_delta", "ts_sum", "ts_product", "ts_std_dev", "ts_mean", 
                  "ts_arg_min", "ts_arg_max", "ts_scale", "normalize", "zscore"]
        
        all_first_order = []
        for ds_id, fields in dataset_fields.items():
            logging.info(f"为 [{ds_id}] 生成一阶Alpha ({len(fields)} 个字段)...")
            fo = first_order_factory(fields, ts_ops)
            all_first_order.extend(fo)
            if interrupted: return
        
        logging.info(f"单数据集一阶Alpha总数: {len(all_first_order)}")
        
        # ===== 第三步：生成跨数据集组合Alpha =====
        cross_alphas = []
        if ENABLE_CROSS_DATASET and len(raw_fields) > 1:
            logging.info("开始生成跨数据集组合Alpha...")
            cross_alphas = cross_dataset_factory(raw_fields, max_combinations=300)
            logging.info(f"跨数据集组合Alpha数量: {len(cross_alphas)}")
            if interrupted: return
        
        # ===== 第四步：合并并采样 =====
        # [迭代修改点] 单数据集Alpha和跨数据集Alpha的合并比例
        # 当前策略: 7:3 比例（单数据集:跨数据集），跨数据集数量 = 单数据集总数 * 0.3
        # 修改 0.3 可调整跨数据集Alpha的占比
        cross_count = int(len(all_first_order) * 0.3) if all_first_order else 0
        if cross_count > len(cross_alphas):
            cross_count = len(cross_alphas)
        
        if cross_count > 0:
            # [迭代修改点] 随机种子，确保采样可复现
            # 修改种子值会改变跨数据集Alpha的选取结果
            random.seed(42)
            sampled_cross = random.sample(cross_alphas, cross_count)
        else:
            sampled_cross = []
        
        merged_alphas = all_first_order + sampled_cross
        random.seed(42)
        random.shuffle(merged_alphas)
        
        final_alphas = merged_alphas[:ALPHA_LIMIT]
        actual_cross = min(cross_count, len(sampled_cross))
        actual_single = len(final_alphas) - actual_cross
        logging.info(f"最终Alpha数量: {len(final_alphas)} (单数据集: {actual_single}, 跨数据集: {actual_cross})")
        if interrupted: return
        
        # ===== 第五步：构建alpha列表并保存 =====
        fo_alpha_list = [(alpha, INIT_DECAY) for alpha in tqdm(final_alphas, desc="构建alpha列表")]
        
        try:
            with open(DAY1_ALPHA_LIST_FILE, 'wb') as f:
                pickle.dump(fo_alpha_list, f)
            logging.info(f"已保存 {len(fo_alpha_list)} 个一阶alpha到 {DAY1_ALPHA_LIST_FILE}")
        except Exception as e:
            logging.error(f"保存alpha列表失败: {e}")
            return
        
        # ===== 第六步：用户选择 =====
        print("\n" + "=" * 60)
        print("Alpha生成完成!")
        print("=" * 60)
        print(f"单数据集Alpha: {actual_single}")
        print(f"跨数据集Alpha: {actual_cross}")
        print(f"Alpha总量: {len(fo_alpha_list)}")
        print(f"已保存到: {DAY1_ALPHA_LIST_FILE}")
        print()
        print("请选择下一步操作:")
        print("  1. 进行平台回测（需要较长时间）")
        print("  2. 跳过回测，直接开始任务二")
        print("  3. 只保存Alpha列表，退出程序")
        print()
        
        while True:
            try:
                choice = input("请输入选择 (1/2/3): ").strip()
                if choice == "1":
                    do_backtest = True
                    start_task2_after = False
                    break
                elif choice == "2":
                    do_backtest = False
                    start_task2_after = True
                    break
                elif choice == "3":
                    do_backtest = False
                    start_task2_after = False
                    break
                else:
                    print("无效选择，请输入 1, 2 或 3")
            except KeyboardInterrupt:
                print("\n用户中断")
                return
        
        print()
        
        if do_backtest:
            fo_pools = load_task_pool_single(fo_alpha_list, POOL_SIZE)
            if interrupted: return
            
            processed = load_progress()
            logging.info(f"Resuming from pool {processed}")
            
            try:
                logging.info("Starting first-order simulation...")
                for i, pool in enumerate(fo_pools):
                    if i < processed:
                        continue
                    if interrupted:
                        save_progress(i)
                        return
                    # [迭代修改点] 中性化方式: SUBINDUSTRY
                    # 可选值: MARKET, SECTOR, INDUSTRY, SUBINDUSTRY, NONE
                    single_simulate([pool], "SUBINDUSTRY", REGION, UNIVERSE, 0)
                    save_progress(i + 1)
                
                logging.info("First-order simulation completed")
                
                print("=" * 60)
                print("任务一已完成!")
                print(f"已成功处理所有 {len(fo_pools)} 个任务池")
                print(f"总计完成 {len(fo_pools) * POOL_SIZE} 个一阶alpha模拟")
                print("请前往WorldQuant网站查看结果")
                print("=" * 60)
                
            except Exception as e:
                logging.error(f"Simulation failed: {e}")
                save_progress(processed)
                return
        
        elif start_task2_after:
            print("=" * 60)
            print("跳过平台回测")
            print(f"已保存 {len(fo_alpha_list)} 个一阶Alpha表达式")
            print("现在开始任务二...")
            print("=" * 60)
            print()
            run_day2_immediately()
        
        else:
            print("=" * 60)
            print("任务一完成（跳过回测）")
            print(f"已保存 {len(fo_alpha_list)} 个一阶Alpha表达式到 {DAY1_ALPHA_LIST_FILE}")
            print()
            print("现在可以:")
            print("  1. 稍后运行任务二: python \"day2 运行程序.py\"")
            print("  2. 或使用任务管理器: 双击 run_all_tasks.bat")
            print("=" * 60)
    
    except KeyboardInterrupt:
        logging.warning("Program interrupted by user")
        if 'processed' in locals():
            save_progress(processed)
        else:
            save_progress(0)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

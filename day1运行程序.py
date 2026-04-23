#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

def search_datafields_interactive(s):
    """交互式搜索数据字段"""
    print("\n" + "=" * 60)
    print("          数据字段搜索")
    print("=" * 60)
    print("输入关键字搜索数据字段（支持字段ID和描述词）")
    print("输入 'q' 返回主菜单")
    print("输入 'v' 只显示 VECTOR 字段")
    print("输入 'm' 只显示 MATRIX 字段")
    print("输入 'a' 显示所有字段（当前数据集）")
    print("输入 'all' 跨所有数据集搜索关键字")
    print("输入 'act' 查找预估字段对应的真实字段")
    print("输入 'ds' 选择数据集")
    print("=" * 60)

    # 默认过滤类型: None表示显示全部, 'VECTOR'只显示VECTOR, 'MATRIX'只显示MATRIX
    type_filter = None
    # 当前选中的数据集
    current_dataset = 'pv1'
    # 缓存所有数据集字段（避免重复请求）
    dataset_cache = {}

    # 预设的预估-真实字段映射关系（根据WorldQuant命名规则）
    est_act_mapping = {
        # analyst4 预估 -> 真实字段映射
        'anl4_basicdetaillt_estvalue': 'anl4_basicdetaillt_actualvalue',
        'anl4_dez1basicafv4_est': 'anl4_dez1basicafv4_actual',
        'anl4_guibasicqfv4_est': 'anl4_guibasicqfv4_actual',
        'anl4_baz1v110_estvalue': 'anl4_baz1v110_actualvalue',
        'anl4_eaz1laf_estvalue': 'anl4_eaz1laf_actualvalue',
        'anl4_ads1detailqfv110_estvalue': 'anl4_ads1detailqfv110_actualvalue',
        'anl4_dez1basicafv4v104_est': 'anl4_dez1basicafv4v104_actual',
        'anl4_dez1basicqfv4_est': 'anl4_dez1basicqfv4_actual',
        'anl4_eaz2lqfv110_estvalue': 'anl4_eaz2lqfv110_actualvalue',
        'anl4_guiafv4_est': 'anl4_guiafv4_actual',
        'anl4_basicdetailqfv110_estvalue': 'anl4_basicdetailqfv110_actualvalue',
        'anl4_detailltv4v104_est': 'anl4_detailltv4v104_actual',
        'anl4_dez1afv4_est': 'anl4_dez1afv4_actual',
        'anl4_dez1basicqfv4v104_est': 'anl4_dez1basicqfv4v104_actual',
        'anl4_eaz2lafv110_estvalue': 'anl4_eaz2lafv110_actualvalue',
        'anl4_eaz2lltv110_estvalue': 'anl4_eaz2lltv110_actualvalue',
        'anl4_guiqfv4_est': 'anl4_guiqfv4_actual',
        'anl4_ads1detailafv110_estvalue': 'anl4_ads1detailafv110_actualvalue',
        'anl4_detailrecv4v104_est': 'anl4_detailrecv4v104_actual',
        'anl4_dez1qfv4_est': 'anl4_dez1qfv4_actual',
        'anl4_dez1safv4_est': 'anl4_dez1safv4_actual',
        'anl4_eaz1lqfv110_estvalue': 'anl4_eaz1lqfv110_actualvalue',
        # sales_estimate 系列 -> actual
        'sales_estimate_value': 'sales_actual_value',
        'sales_estimate_maximum': 'sales_actual_maximum',
        'sales_estimate_minimum': 'sales_actual_minimum',
        'sales_estimate_maximum_quarterly': 'sales_actual_maximum_quarterly',
        'sales_estimate_minimum_quarterly': 'sales_actual_minimum_quarterly',
        # dividend_estimate -> actual
        'dividend_estimate_value': 'dividend_actual_value',
        # earnings 系列
        'earnings_per_share_median_value': 'earnings_per_share_actual_median',
        'earnings_per_share_maximum': 'earnings_per_share_actual_maximum',
        'earnings_per_share_minimum': 'earnings_per_share_actual_minimum',
    }

    while True:
        print()
        keyword = input("请输入关键字或命令(v/m/a/all/act/ds/q): ").strip()

        if keyword.lower() == 'q':
            print("返回主菜单")
            return
        elif keyword.lower() == 'v':
            type_filter = 'VECTOR'
            print(f"已切换为只显示 VECTOR 字段")
            continue
        elif keyword.lower() == 'm':
            type_filter = 'MATRIX'
            print(f"已切换为只显示 MATRIX 字段")
            continue
        elif keyword.lower() == 'a':
            type_filter = None
            print(f"已切换为显示所有字段")
            continue
        elif keyword.lower() == 'all':
            # 跨所有数据集搜索关键字 - 使用WQ API原生搜索
            keyword = input("输入要搜索的关键字: ").strip()
            if not keyword:
                print("请输入有效的关键字")
                continue
            
            print(f"\n正在跨所有数据集搜索 '{keyword}' (使用WQ原生搜索)...")
            print("这可能需要一些时间...\n")
            
            all_results = []
            
            for ds in ALL_DATASETS:
                ds_id = ds['id']
                print(f"  搜索数据集 [{ds_id}]...", end=" ", flush=True)
                try:
                    # 直接使用WQ API搜索功能
                    df = get_datafields(s, dataset_id=ds_id, region=REGION, universe=UNIVERSE, search=keyword)
                    
                    if not df.empty:
                        # 本地过滤：只保留description中包含关键词的记录
                        df_filtered = df[df['description'].str.contains(keyword, case=False, na=False)]
                        if not df_filtered.empty:
                            df_filtered = df_filtered.copy()
                            df_filtered['_dataset'] = ds_id
                            all_results.append(df_filtered)
                            print(f"找到 {len(df_filtered)} 个")
                        else:
                            print("无匹配")
                    else:
                        print("无匹配")
                except Exception as e:
                    print(f"失败: {e}")
            
            if not all_results:
                print(f"\n未在所有数据集中找到包含 '{keyword}' 的数据字段")
                continue
            
            # 合并结果
            df_all = pd.concat(all_results, ignore_index=True)
            
            # 根据类型过滤
            if type_filter:
                df_all = df_all[df_all['type'] == type_filter]
            
            filter_desc = f" ({type_filter}类型)" if type_filter else ""
            print(f"\n" + "=" * 100)
            print(f"搜索结果: 共找到 {len(df_all)} 个包含 '{keyword}' 的字段{filter_desc}")
            print("=" * 100)
            print(f"{'数据集':15s} | {'字段ID':40s} | {'类型':10s} | {'Description'}")
            print("-" * 100)
            
            field_ids = []
            for _, row in df_all.iterrows():
                field_id = row.get('id', '')
                field_type = row.get('type', '')
                field_desc = str(row.get('description', ''))[:45] if row.get('description') else ''
                ds_name = row.get('_dataset', current_dataset)
                print(f"  {ds_name:13s} | {field_id:40s} | {field_type:10s} | {field_desc}")
                field_ids.append(field_id)
            
            print("-" * 100)
            print(f"\n所有字段ID列表（逗号分隔，可直接复制）:")
            print("  " + ",".join(field_ids))
            print()
            continue
        elif keyword.lower() == 'ds':
            # 选择数据集
            print("\n可用数据集:")
            for i, ds in enumerate(ALL_DATASETS, 1):
                print(f"  {i:2d}. {ds['id']:16s} - {ds['desc']}")
            ds_input = input("选择数据集编号 (默认1=pv1): ").strip()
            try:
                idx = int(ds_input) if ds_input else 1
                current_dataset = ALL_DATASETS[idx - 1]['id'] if 1 <= idx <= len(ALL_DATASETS) else 'pv1'
            except:
                current_dataset = 'pv1'
            print(f"已切换到数据集: {current_dataset}")
            # 清除缓存
            if current_dataset in dataset_cache:
                del dataset_cache[current_dataset]
            continue
        elif keyword.lower() == 'act':
            # 查找预估字段对应的真实字段
            print("\n" + "=" * 60)
            print("          预估 -> 真实字段映射")
            print("=" * 60)
            print("正在从 analyst4 数据集获取真实值字段...")

            df_all = get_datafields(s, dataset_id='analyst4', region=REGION, universe=UNIVERSE)
            
            if df_all.empty:
                print("获取 analyst4 字段失败")
                continue
            
            # 提取所有真实字段ID
            actual_fields = set(df_all['id'].tolist()) if 'id' in df_all.columns else set()
            
            # 查找 _actual 或 actualvalue 后缀的字段
            actual_mapping = {}
            for est_field, guess_actual in est_act_mapping.items():
                if guess_actual in actual_fields:
                    actual_mapping[est_field] = guess_actual
                else:
                    # 尝试其他变体
                    candidates = [
                        guess_actual,
                        est_field.replace('_estvalue', '_actualvalue').replace('_est', '_actual'),
                        est_field.replace('estimate', 'actual'),
                        est_field.replace('_est_', '_actual_'),
                    ]
                    for cand in candidates:
                        if cand in actual_fields:
                            actual_mapping[est_field] = cand
                            break
            
            if actual_mapping:
                print(f"\n找到 {len(actual_mapping)} 个对应的真实字段:")
                print("-" * 80)
                print(f"{'预估字段':45s} -> {'真实字段'}")
                print("-" * 80)
                actual_list = []
                for est, act in actual_mapping.items():
                    print(f"  {est:43s} -> {act}")
                    actual_list.append(act)
                print("-" * 80)
                print("\n真实字段ID列表（可直接复制）:")
                print("  " + ",".join(actual_list))
            else:
                print("未在 analyst4 中找到对应的真实字段")
            
            print()
            continue
        
        if not keyword:
            print("请输入有效的关键字")
            continue

        print(f"\n正在使用 WQ API 搜索 '{keyword}' (数据集: {current_dataset})...")

        # 直接调用 WQ API 的搜索功能
        df = get_datafields(s, dataset_id=current_dataset, region=REGION, universe=UNIVERSE, search=keyword)

        if df.empty:
            print(f"未在 [{current_dataset}] 数据集中找到包含 '{keyword}' 的数据字段")
            print(f"提示: 输入 'all' 可跨所有数据集搜索，或输入 'ds' 切换数据集")
            continue

        # 根据类型过滤
        if type_filter:
            df = df[df['type'] == type_filter]

        if df.empty:
            filter_name = "VECTOR" if type_filter == 'VECTOR' else "MATRIX"
            print(f"未找到类型为 {filter_name} 的字段")
            continue

        filter_desc = f" ({type_filter}类型)" if type_filter else ""
        print(f"\n找到 {len(df)} 个匹配字段{filter_desc} (数据集: {current_dataset}):")
        print("-" * 100)
        print(f"{'字段ID':40s} | {'类型':10s} | {'Description'}")
        print("-" * 100)

        # 显示字段信息
        field_ids = []
        for _, row in df.iterrows():
            field_id = row.get('id', '')
            field_type = row.get('type', '')
            field_desc = str(row.get('description', ''))[:50] if row.get('description') else ''
            print(f"  {field_id:38s} | {field_type:10s} | {field_desc}")
            field_ids.append(field_id)

        print("-" * 100)
        print(f"共 {len(df)} 个字段")
        print()

        # 输出逗号分隔的格式
        print("字段ID列表（可直接复制）:")
        print("  " + ",".join(field_ids))
        print()

def get_user_config():
    """获取用户配置 - 支持多数据集选择"""
    global SELECTED_DATASETS, ENABLE_CROSS_DATASET, FIELDS_PER_DATASET, ALPHA_LIMIT
    
    print("=" * 60)
    print("          世界量化 - 任务一配置")
    print("=" * 60)
    print()
    print("功能菜单:")
    print("  0. 搜索数据字段（输入关键字查找字段）")
    print("  1-7. 选择数据集编号")
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
    
    if raw_input == '0':
        # 先登录再搜索
        s = login()
        search_datafields_interactive(s)
        print()
        raw_input = input("请选择数据集: ").strip().lower()
        if raw_input == 'q':
            return False
    
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


def quick_field_backtest():
    """
    极简字段筛选回测 - Alpha来自数据，不来自算子

    策略核心:
    - 80% 精力筛选字段（高覆盖率 >90%，低使用频率）
    - 20% 精力表达式设计
    - 只用 rank(ts_delta(field, N)) 一个模板
    - N 从 [5, 10, 20, 60, 120] 中选择
    - 过 Sharpe 1.0 的再加 group/neutralization 优化
    """
    print("\n" + "=" * 60)
    print("     极简字段筛选回测 - 字段优先策略")
    print("=" * 60)
    print()
    print("核心理念: Alpha来自数据，不来自算子")
    print("冷门高质量字段 + rank + ts_delta > 热门字段 + 复杂嵌套")
    print()
    print("策略:")
    print("  1. 筛选高覆盖率(>90%)、低使用频率的字段")
    print("  2. 只用一个模板: rank(ts_delta(field, N))")
    print("  3. N 从 [5, 10, 20, 60, 120] 中选择")
    print("  4. Sharpe >= 1.0 的标记为候选因子")
    print("=" * 60)
    print()

    # 1. 登录
    s = login()

    # 2. 选择数据集
    print("可用数据集:")
    for i, ds in enumerate(ALL_DATASETS, 1):
        print(f"  {i:2d}. {ds['id']:16s} - {ds['desc']}")
    print()

    raw_input = input("请选择数据集编号 (默认1=pv1): ").strip()
    try:
        idx = int(raw_input) if raw_input else 1
        dataset_id = ALL_DATASETS[idx - 1]['id'] if 1 <= idx <= len(ALL_DATASETS) else 'pv1'
    except:
        dataset_id = 'pv1'

    # 3. 获取字段
    print(f"\n正在从 [{dataset_id}] 获取字段...")
    df = get_datafields(s, dataset_id=dataset_id, region=REGION, universe=UNIVERSE, delay=DELAY)

    if df.empty:
        print("获取字段失败")
        return

    print(f"共获取 {len(df)} 个字段")

    # 4. 字段筛选标准
    print("\n" + "=" * 60)
    print("字段筛选标准")
    print("=" * 60)

    # 覆盖率筛选 (默认 >90%)
    print("\n覆盖率筛选 (留空使用默认值 90):")
    cov_input = input("最低覆盖率 % (0-100): ").strip()
    min_coverage = int(cov_input) if cov_input else 90

    # 使用频率筛选 (默认低 - 即 userCount 低)
    print("\n使用频率筛选:")
    print("  1. 极低 (< 50 用户) - 推荐冷门高质")
    print("  2. 低 (< 200 用户)")
    print("  3. 中 (< 500 用户)")
    print("  4. 全部不限制")
    freq_choice = input("请选择 (1/2/3/4, 默认1): ").strip() or "1"

    freq_map = {"1": 50, "2": 200, "3": 500, "4": None}
    max_users = freq_map.get(freq_choice, 50)

    # 字段类型
    print("\n字段类型:")
    print("  1. MATRIX 字段")
    print("  2. VECTOR 字段")
    print("  3. 全部字段")
    type_choice = input("请选择 (1/2/3, 默认1): ").strip() or "1"

    # 应用筛选
    df_filtered = df.copy()

    if 'coverage' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['coverage'] > min_coverage / 100]
        print(f"\n覆盖率 > {min_coverage}%: {len(df_filtered)} 个字段")

    if max_users and 'userCount' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['userCount'] < max_users]
        print(f"使用频率 < {max_users} 用户: {len(df_filtered)} 个字段")

    if type_choice == "1":
        df_filtered = df_filtered[df_filtered['type'] == "MATRIX"]
    elif type_choice == "2":
        df_filtered = df_filtered[df_filtered['type'] == "VECTOR"]

    print(f"最终筛选: {len(df_filtered)} 个候选字段")
    print()

    # 5. 显示候选字段
    if len(df_filtered) > 0:
        print("候选字段预览:")
        display_cols = ['id', 'name', 'coverage', 'userCount'] if 'coverage' in df_filtered.columns else ['id', 'name', 'userCount']
        available_cols = [c for c in display_cols if c in df_filtered.columns]
        print(df_filtered[available_cols].head(10).to_string(index=False))
        if len(df_filtered) > 10:
            print(f"... 还有 {len(df_filtered) - 10} 个字段")
        print()

    # 6. 时间窗口
    print("=" * 60)
    print("时间窗口设置 (ts_delta 参数)")
    print("=" * 60)
    print("推荐: [5, 10, 20, 60, 120]")
    windows_input = input("输入逗号分隔的时间窗口 (留空使用默认): ").strip()
    if windows_input:
        try:
            windows = [int(w.strip()) for w in windows_input.split(',')]
        except:
            windows = [5, 10, 20, 60, 120]
    else:
        windows = [5, 10, 20, 60, 120]
    print(f"使用时间窗口: {windows}")

    # 7. 中性化方式
    print("\n中性化方式:")
    print("  1. SUBINDUSTRY")
    print("  2. INDUSTRY")
    print("  3. SECTOR")
    print("  4. MARKET")
    print("  5. NONE (首轮测试用)")
    neut_choice = input("请选择 (1/2/3/4/5, 默认5): ").strip() or "5"

    neut_map = {"1": "SUBINDUSTRY", "2": "INDUSTRY", "3": "SECTOR", "4": "MARKET", "5": "NONE"}
    neut = neut_map.get(neut_choice, "NONE")

    # 8. 预估数量
    all_fields = df_filtered['id'].tolist()
    vector_count = len([f for f in all_fields if f in df_filtered[df_filtered['type'] == 'VECTOR']['id'].tolist()])
    total_alphas = len(all_fields) * len(windows)
    print(f"\n预估Alpha数量: {len(all_fields)} 字段 x {len(windows)} 窗口 = {total_alphas}")
    print(f"  (其中 VECTOR 字段约 {vector_count} 个将用 vec_avg 处理)")

    if total_alphas == 0:
        print("没有符合条件的字段，取消操作")
        return

    # 9. 确认
    print()
    confirm = input("确认开始回测? (y/n): ").strip().lower()
    if confirm != 'y':
        print("取消操作")
        return

    # 10. 分离 VECTOR 和 MATRIX 字段
    vector_fields = df_filtered[df_filtered['type'] == 'VECTOR']['id'].tolist()
    matrix_fields = df_filtered[df_filtered['type'] == 'MATRIX']['id'].tolist()

    print(f"\n字段类型分布:")
    print(f"  VECTOR 字段: {len(vector_fields)} 个 (将使用 vec_avg 处理)")
    print(f"  MATRIX 字段: {len(matrix_fields)} 个 (直接使用)")

    # 11. 生成Alpha表达式
    print(f"\n正在生成Alpha表达式...")
    alpha_list = []

    # MATRIX 字段: 直接使用
    for field in matrix_fields:
        for n in windows:
            # 极简模板: rank(ts_delta(field, N))
            alpha_expr = f"rank(ts_delta({field}, {n}))"
            alpha_list.append((alpha_expr, 0))

    # VECTOR 字段: 使用 vec_avg 处理
    for field in vector_fields:
        for n in windows:
            # VECTOR 字段需要 vec_avg 转换为标量
            alpha_expr = f"rank(ts_delta(vec_avg({field}), {n}))"
            alpha_list.append((alpha_expr, 0))

    print(f"生成完成: {len(alpha_list)} 个Alpha")
    print(f"  - MATRIX: {len(matrix_fields) * len(windows)} 个")
    print(f"  - VECTOR: {len(vector_fields) * len(windows)} 个 (已用 vec_avg 处理)")

    # 11. 开始回测
    print(f"\n开始回测 (中性化={neut})...")
    print("提示: Sharpe >= 1.0 的Alpha为候选因子")

    pools = load_task_pool_single(alpha_list, POOL_SIZE)
    single_simulate(pools, neut, REGION, UNIVERSE, 0)

    # 12. 结果分析提示
    print("\n" + "=" * 60)
    print("回测完成!")
    print("=" * 60)
    print()
    print("下一步建议:")
    print("  1. 登录 WorldQuant 网站查看回测结果")
    print("  2. 筛选 Sharpe >= 1.0 的Alpha")
    print("  3. 对候选Alpha添加一层 group/neutralization 优化:")
    print("     - group_neutralize(rank(ts_delta(field, N)), subindustry)")
    print("     - group_rank(rank(ts_delta(field, N)), subindustry)")
    print("  4. 再次回测优化后的Alpha")
    print()
    print("核心理念回顾:")
    print("  Alpha来自数据，不来自算子")
    print("  冷门高质量字段 + rank + ts_delta > 热门字段 + 复杂嵌套")
    print("=" * 60)


if __name__ == "__main__":
    print("=" * 60)
    print("          世界量化 - 功能选择")
    print("=" * 60)
    print()
    print("  1. 任务一：多数据集Alpha生成")
    print("  2. 搜索数据字段")
    print("  3. 极简字段筛选回测 (字段优先策略)")
    print()
    print("=" * 60)

    choice = input("请选择功能 (1/2/3): ").strip()

    if choice == "2":
        # 搜索功能需要先登录
        s = login()
        search_datafields_interactive(s)
    elif choice == "3":
        quick_field_backtest()
    else:
        main()

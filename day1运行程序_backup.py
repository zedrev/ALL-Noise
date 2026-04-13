import logging
import random
import os
import signal
import sys
import pickle
import pandas as pd
from machine_lib import login, get_datafields, process_datafields, first_order_factory, load_task_pool_single, single_simulate
from tqdm import tqdm

# ??????
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# ????
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# 参数配置 - 支持多个数据集
DATASET_ID = os.getenv('WQ_DATASET_ID', 'pv1')  # 默认: pv1，可选: fundamental6, tcf4
REGION = 'USA'
UNIVERSE = 'TOP3000'
DELAY = 1
INIT_DECAY = 6
POOL_SIZE = 3

# 字段筛选配置
USE_FIELD_FILTER = True  # 是否启用字段筛选
FIELD_SELECTION_MODE = 'auto'  # auto:自动, top_n:按alphaCount排序前N个, category:按类别, all:全部
FIELD_SELECTION_PARAM = 20     # 筛选参数：数量或类别名
PREFER_ANALYST_FIELDS = True   # 是否优先选择Analyst Estimate字段

# 进程文件管理配置
PROCESS_FILES_DIR = 'process_files'  # 进程文件专用目录
DAY1_PROGRESS_FILE = os.path.join(PROCESS_FILES_DIR, 'day1_progress.pkl')
DAY1_ALPHA_LIST_FILE = 'day1_alpha_list.pkl'  # 结果文件保留在根目录

interrupted = False

def signal_handler(sig, frame):
    global interrupted
    logging.warning("Interrupt signal received, stopping...")
    interrupted = True

def setup_process_files():
    """设置进程文件目录"""
    if not os.path.exists(PROCESS_FILES_DIR):
        os.makedirs(PROCESS_FILES_DIR)
        logging.info(f"创建进程文件目录: {PROCESS_FILES_DIR}")

def save_progress(processed_count):
    """保存进度到专用进程文件夹"""
    setup_process_files()
    with open(DAY1_PROGRESS_FILE, 'wb') as f:
        pickle.dump(processed_count, f)
    logging.debug(f"进度已保存到: {DAY1_PROGRESS_FILE}")

def load_progress():
    """从进程文件夹加载进度"""
    try:
        with open(DAY1_PROGRESS_FILE, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        # 兼容旧版本：尝试从根目录加载
        try:
            old_file = 'progress.pkl'
            if os.path.exists(old_file):
                with open(old_file, 'rb') as f:
                    progress = pickle.load(f)
                # 迁移到新位置
                save_progress(progress)
                os.remove(old_file)
                logging.info(f"迁移旧进度文件: {old_file} -> {DAY1_PROGRESS_FILE}")
                return progress
        except:
            pass
        return 0

def check_completion_status(total_pools=334):
    """检查是否已完成所有任务"""
    try:
        current_progress = load_progress()
        if current_progress >= total_pools:
            print("=" * 60)
            print("🎉 任务已完成！")
            print(f"当前进度: {current_progress}/{total_pools} 个任务池")
            print(f"已处理alpha: {current_progress * 3} 个 (总目标: 1000)")
            print(f"完成比例: {(current_progress/total_pools)*100:.1f}%")
            print("=" * 60)
            return True
        else:
            print("=" * 60)
            print("📊 任务进度报告")
            print(f"当前进度: {current_progress}/{total_pools} 个任务池")
            print(f"已处理alpha: {current_progress * 3} 个 (总目标: 1000)")
            print(f"完成比例: {(current_progress/total_pools)*100:.1f}%")
            print("=" * 60)
            return False
    except Exception as e:
        logging.error(f"检查完成状态时出错: {e}")
        return False

def filter_fields(df, mode='auto', param=20, prefer_analyst=True):
    """
    智能字段筛选函数
    mode: 'auto'自动, 'top_n'按alphaCount排序, 'category'按类别, 'all'全部
    param: 数量或类别名
    prefer_analyst: 对于fundamental6数据集，是否优先选择Analyst Estimate字段
    """
    logging.info(f"开始字段筛选: mode={mode}, param={param}, prefer_analyst={prefer_analyst}")
    
    original_count = len(df)
    logging.info(f"原始字段数量: {original_count}")
    
    if mode == 'all':
        logging.info("使用全部字段")
        return df
    
    # 检查是否有alphaCount列
    if 'alphaCount' not in df.columns:
        logging.warning("DataFrame中没有alphaCount列，无法按使用频率排序")
        # 如果没有alphaCount，只按category筛选
        if mode == 'category' and 'category' in df.columns:
            filtered = df[df['category'].str.contains(str(param), case=False, na=False)]
            logging.info(f"按类别筛选: 找到 {len(filtered)} 个字段 (类别包含 '{param}')")
            return filtered
        return df
    
    # 预处理：按alphaCount降序排序
    df_sorted = df.sort_values('alphaCount', ascending=False).copy()
    
    # 分析alphaCount分布
    if len(df_sorted) > 0:
        top_alpha = df_sorted.iloc[0]['alphaCount'] if pd.notna(df_sorted.iloc[0]['alphaCount']) else 0
        avg_alpha = df_sorted['alphaCount'].mean() if pd.notna(df_sorted['alphaCount']).any() else 0
        logging.info(f"Alpha使用频率统计: 最高 {top_alpha}, 平均 {avg_alpha:.1f}")
    
    # 对于fundamental6数据集，优先选择Analyst Estimate字段
    if prefer_analyst:
        # Analyst Estimate相关的关键词
        analyst_keywords = ['analyst', 'estimate', 'consensus', 'rating', 'earnings', 'eps', 'forecast', 'price target']
        
        # 检查是否是fundamental6数据集
        if len(df_sorted) > 100:  # fundamental6数据集通常有800+字段
            logging.info("检测到可能的基本面数据集，开始筛选Analyst Estimate字段")
            
            if 'category' in df_sorted.columns:
                # 先筛选Analyst Estimate字段
                analyst_mask = df_sorted['category'].str.contains('|'.join(analyst_keywords), case=False, na=False)
                analyst_fields = df_sorted[analyst_mask]
                non_analyst_fields = df_sorted[~analyst_mask]
                
                if len(analyst_fields) > 0:
                    logging.info(f"找到 {len(analyst_fields)} 个Analyst Estimate字段")
                    
                    # 分析Analyst Estimate字段的alphaCount
                    analyst_stats = analyst_fields['alphaCount'].describe()
                    logging.info(f"Analyst Estimate字段统计: 最高 {analyst_stats['max']:.0f}, 平均 {analyst_stats['mean']:.1f}")
                    
                    # 从Analyst Estimate字段中取前param个
                    if mode == 'top_n':
                        top_analyst = analyst_fields.head(param)
                        logging.info(f"选择前 {len(top_analyst)} 个Analyst Estimate字段")
                        logging.info(f"AlphaCount范围: {top_analyst['alphaCount'].min():.0f} - {top_analyst['alphaCount'].max():.0f}")
                        return top_analyst
                    
                    # auto模式：取前param个字段，优先包含Analyst Estimate
                    elif mode == 'auto':
                        # 确保至少有一定比例的Analyst Estimate字段（但不超过总数）
                        max_analyst_ratio = 0.7  # 最多70%是Analyst Estimate
                        analyst_count = min(int(param * max_analyst_ratio), len(analyst_fields), param)
                        non_analyst_count = param - analyst_count
                        
                        top_analyst = analyst_fields.head(analyst_count)
                        top_non_analyst = non_analyst_fields.head(non_analyst_count)
                        
                        result = pd.concat([top_analyst, top_non_analyst])
                        logging.info(f"智能选择: {analyst_count}个Analyst Estimate字段 + {non_analyst_count}个其他字段")
                        logging.info(f"总AlphaCount: {result['alphaCount'].sum():.0f}, 平均 {result['alphaCount'].mean():.1f}")
                        return result
                    
                    # category模式：如果param包含analyst关键词，只返回Analyst Estimate字段
                    elif mode == 'category' and any(keyword in str(param).lower() for keyword in analyst_keywords):
                        # 按alphaCount排序，取前param个（如果param是数字）或全部（如果param是字符串）
                        try:
                            n = int(param)
                            top_analyst = analyst_fields.head(n)
                        except:
                            top_analyst = analyst_fields
                        
                        logging.info(f"选择 {len(top_analyst)} 个Analyst Estimate字段")
                        return top_analyst
    
    # 通用筛选逻辑
    if mode == 'top_n':
        top_fields = df_sorted.head(param)
        logging.info(f"按alphaCount排序，选择前 {len(top_fields)} 个字段")
        logging.info(f"AlphaCount范围: {top_fields['alphaCount'].min():.0f} - {top_fields['alphaCount'].max():.0f}")
        return top_fields
    
    elif mode == 'category':
        if 'category' in df_sorted.columns:
            filtered = df_sorted[df_sorted['category'].str.contains(str(param), case=False, na=False)]
            logging.info(f"按类别筛选: 找到 {len(filtered)} 个字段 (类别包含 '{param}')")
            # 在类别内按alphaCount排序，取前param个（如果param是数字）
            try:
                n = int(param)
                return filtered.head(n)
            except:
                return filtered
        else:
            logging.warning("DataFrame中没有category列，无法按类别筛选")
            return df_sorted.head(param)
    
    elif mode == 'auto':
        # auto模式：选择前param个按alphaCount排序的字段
        auto_fields = df_sorted.head(param)
        logging.info(f"自动选择前 {len(auto_fields)} 个按alphaCount排序的字段")
        logging.info(f"AlphaCount范围: {auto_fields['alphaCount'].min():.0f} - {auto_fields['alphaCount'].max():.0f}")
        return auto_fields
    
    logging.warning(f"未知的筛选模式: {mode}，使用全部字段")
    return df

def setup_session():
    """???????????"""
    try:
        s = login()
        logging.info("????")
        return s
    except Exception as e:
        logging.error(f"????: {e}")
        raise

def get_user_config():
    """获取用户配置"""
    print("=" * 60)
    print("          世界量化 - 任务一配置")
    print("=" * 60)
    print()
    
    # 数据集选择
    datasets = {
        '1': 'pv1 - Price Volume Data for Equity',
        '2': 'fundamental6 - Company Fundamental Data for Equity',
        '3': 'tcf4 - Technical Composite Factor 4'
    }
    
    print("请选择数据集:")
    for key, desc in datasets.items():
        print(f"  {key}. {desc}")
    
    dataset_choice = input(f"\n选择数据集 (1-3, 默认1): ").strip()
    if dataset_choice == '2':
        dataset_id = 'fundamental6'
    elif dataset_choice == '3':
        dataset_id = 'tcf4'
    else:
        dataset_id = 'pv1'
    
    print()
    print(f"选择的数据集: {dataset_id}")
    
    # 字段筛选模式
    print("\n字段筛选选项:")
    print("  1. 自动模式 (auto) - 推荐")
    print("  2. 按使用频率排序 (top_n)")
    print("  3. 按类别筛选 (category)")
    print("  4. 使用全部字段 (all)")
    
    mode_choice = input(f"\n选择筛选模式 (1-4, 默认1): ").strip()
    if mode_choice == '2':
        field_mode = 'top_n'
    elif mode_choice == '3':
        field_mode = 'category'
    elif mode_choice == '4':
        field_mode = 'all'
    else:
        field_mode = 'auto'
    
    # 筛选参数
    field_param = 20
    if field_mode in ['top_n', 'auto']:
        try:
            param_input = input(f"选择字段数量 (默认20): ").strip()
            if param_input:
                field_param = int(param_input)
        except:
            pass
    elif field_mode == 'category':
        category_name = input(f"输入类别名称 (如 'Analyst Estimate'): ").strip()
        field_param = category_name if category_name else 'analyst'
    
    # 是否优先Analyst Estimate字段
    prefer_analyst = True
    if dataset_id == 'fundamental6':
        print("\n" + "-" * 60)
        print("Analyst Estimate字段说明:")
        print("在WorldQuant平台上，Analyst Estimate字段包括:")
        print("  • 分析师预测 (earnings estimates)")
        print("  • 收益估计 (consensus ratings)")
        print("  • 目标价格 (price targets)")
        print("  • 评级变化 (rating changes)")
        print("这些字段通常被大量用户用于创建高质量的Alpha")
        print("-" * 60)
        analyst_choice = input(f"\n是否优先选择Analyst Estimate字段? (y/n, 默认y): ").strip().lower()
        prefer_analyst = analyst_choice != 'n'
    
    print()
    print("=" * 60)
    print("配置摘要:")
    print(f"  数据集: {dataset_id}")
    print(f"  筛选模式: {field_mode}")
    print(f"  筛选参数: {field_param}")
    if dataset_id == 'fundamental6':
        print(f"  优先Analyst Estimate: {'是' if prefer_analyst else '否'}")
    print("=" * 60)
    print()
    
    # 更新全局变量
    global DATASET_ID, USE_FIELD_FILTER, FIELD_SELECTION_MODE, FIELD_SELECTION_PARAM, PREFER_ANALYST_FIELDS
    DATASET_ID = dataset_id
    FIELD_SELECTION_MODE = field_mode
    FIELD_SELECTION_PARAM = field_param
    PREFER_ANALYST_FIELDS = prefer_analyst
    
    # 确认开始
    confirm = input("确认配置并开始任务? (y/n): ").strip().lower()
    return confirm == 'y'

def main():
    global interrupted
    
    # 获取用户配置
    if not get_user_config():
        print("用户取消操作")
        return
    
    # 检查完成状态
    print("\n" + "=" * 60)
    print("任务一：1000个一阶alpha模拟")
    print(f"数据集: {DATASET_ID}")
    print("=" * 60)
    
    if check_completion_status():
        print("程序已完成所有任务，无需再次运行。")
        print("如需重新运行，请删除 progress.pkl 文件")
        return
    
    print("开始执行...")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    print("信号处理器已设置")

    try:
        # ??
        s = setup_session()
        if interrupted: return

        # ????
        try:
            df = get_datafields(s, dataset_id=DATASET_ID, region=REGION, universe=UNIVERSE, delay=DELAY)
            logging.info(f"??????: ?? {len(df)} ?")
            
            # ?????????
            if USE_FIELD_FILTER:
                logging.info(f"??????: ?? {FIELD_SELECTION_MODE}, ?? {FIELD_SELECTION_PARAM}")
                df = filter_fields(df, FIELD_SELECTION_MODE, FIELD_SELECTION_PARAM, PREFER_ANALYST_FIELDS)
                logging.info(f"??????: ?? {len(df)} ?")
            
        except Exception as e:
            logging.error(f"??????: {e}")
            return
        if interrupted: return

        # ????
        pc_fields = process_datafields(df)
        if interrupted: return

        # ????alpha
        ts_ops = ["ts_delta", "ts_sum", "ts_product", "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max", "ts_scale", "normalize", "zscore"]
        first_order = first_order_factory(pc_fields, ts_ops)
        logging.info(f"Number of first-order expressions: {len(first_order)}")
        logging.debug(f"?10????: {first_order[:10]}")
        if interrupted: return

        # ??alpha??
        logging.info("Generating first-order alpha list...")
        fo_alpha_list = [(alpha, INIT_DECAY) for alpha in tqdm(first_order, desc="Generating alpha list")]
        random.seed(42)  # ???????????
        random.shuffle(fo_alpha_list)
        # ???????1000?alpha?????
        fo_alpha_list = fo_alpha_list[:1000]
        logging.info(f"Optimized alpha count: {len(fo_alpha_list)}")
        logging.debug(f"?5?alpha: {fo_alpha_list[:5]}")
        if interrupted: return

        # 保存一阶alpha列表供任务二使用（无论是否回测都保存）
        try:
            with open(DAY1_ALPHA_LIST_FILE, 'wb') as f:
                import pickle
                pickle.dump(fo_alpha_list, f)
            logging.info(f"已保存 {len(fo_alpha_list)} 个一阶alpha到 {DAY1_ALPHA_LIST_FILE}")
        except Exception as e:
            logging.error(f"保存alpha列表失败: {e}")
            return

        # 用户选择：是否进行平台回测
        print("\n" + "=" * 60)
        print("🎯 一阶Alpha生成完成！")
        print("=" * 60)
        print(f"已生成 {len(fo_alpha_list)} 个一阶Alpha表达式")
        print(f"已保存到: {DAY1_ALPHA_LIST_FILE}")
        print()
        print("请选择下一步操作:")
        print("  1. 进行平台回测（原流程，需要5-6小时）")
        print("  2. 跳过回测，直接开始任务二")
        print("  3. 只保存Alpha列表，退出程序")
        print()
        
        while True:
            try:
                choice = input("请输入选择 (1/2/3): ").strip()
                if choice == "1":
                    print("选择: 进行平台回测")
                    do_backtest = True
                    start_task2_after = False
                    break
                elif choice == "2":
                    print("选择: 跳过回测，直接开始任务二")
                    do_backtest = False
                    start_task2_after = True
                    break
                elif choice == "3":
                    print("选择: 只保存Alpha列表，退出程序")
                    do_backtest = False
                    start_task2_after = False
                    break
                else:
                    print("无效选择，请输入 1, 2 或 3")
            except KeyboardInterrupt:
                print("\n用户中断")
                return
        
        print()

        # 如果选择进行回测
        if do_backtest:
            # ????????
            fo_pools = load_task_pool_single(fo_alpha_list, POOL_SIZE)
            logging.info(f"?????: {fo_pools[0]}")
            if interrupted: return

            # ????
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
                single_simulate([pool], "SUBINDUSTRY", REGION, UNIVERSE, 0)
                save_progress(i + 1)
            logging.info("First-order simulation completed")
            
            # 保存一阶alpha列表供任务二使用
            try:
                with open(DAY1_ALPHA_LIST_FILE, 'wb') as f:
                    import pickle
                    pickle.dump(fo_alpha_list, f)
                logging.info(f"已保存 {len(fo_alpha_list)} 个一阶alpha到 {DAY1_ALPHA_LIST_FILE}")
            except Exception as e:
                logging.error(f"保存alpha列表失败: {e}")
            
            print("=" * 60)
            print("🎉 恭喜！任务一已完成！")
            print(f"已成功处理所有 {len(fo_pools)} 个任务池")
            print(f"总计完成 {len(fo_pools) * POOL_SIZE} 个一阶alpha模拟")
            print("已保存一阶alpha列表供任务二使用")
            print("请前往WorldQuant网站查看结果")
            print("=" * 60)
        except Exception as e:
            logging.error(f"Simulation failed: {e}")
            save_progress(processed)

    except KeyboardInterrupt:
        logging.warning("Program interrupted by user")
        if 'processed' in locals():
            save_progress(processed)
        else:
            save_progress(0)

if __name__ == "__main__":
    main()
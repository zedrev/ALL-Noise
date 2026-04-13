# day2 二阶alpha生成程序
import logging
import random
import os
import signal
import sys
import pickle
from machine_lib import login, get_alphas, prune, get_group_second_order_factory, load_task_pool_single, single_simulate
from tqdm import tqdm

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# [迭代修改点] 参数配置区域
# 二阶Alpha生成参数:
#   THRESHOLD_HIGH/LOW - 从平台获取alpha的Sharpe范围
#   TOP_N - 获取的alpha总数上限
#   PRUNE_TYPE - 剪枝时的字段前缀（需与一阶alpha字段匹配）
#   PRUNE_COUNT - 每个字段保留的top alpha数量
#   POOL_SIZE - 每批并行模拟数量
DEFAULT_START_DATE = "03-30"
DEFAULT_END_DATE = "03-30"
THRESHOLD_HIGH = 1.0    # [迭代修改点] Sharpe阈值上限
THRESHOLD_LOW = 0.7     # [迭代修改点] Sharpe阈值下限
REGION = 'USA'
TOP_N = 100             # [迭代修改点] 获取alpha数量
PRUNE_TYPE = 'anl4'     # [迭代修改点] 剪枝字段前缀，需与实际字段匹配
PRUNE_COUNT = 5         # [迭代修改点] 每个字段保留的alpha数量
INIT_DECAY = 6          # [迭代修改点] 初始decay值
POOL_SIZE = 3           # [迭代修改点] 每批并行模拟数

# 运行时变量
START_DATE = DEFAULT_START_DATE
END_DATE = DEFAULT_END_DATE

interrupted = False

def signal_handler(sig, frame):
    global interrupted
    logging.warning("Interrupt signal received, stopping...")
    interrupted = True

def save_progress(processed_count):
    with open('progress_day2.pkl', 'wb') as f:
        pickle.dump(processed_count, f)

def load_progress():
    try:
        with open('progress_day2.pkl', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return 0

def save_run_dates(start_date, end_date):
    """保存本次运行的日期范围，供续跑时提示"""
    with open('day2_dates.pkl', 'wb') as f:
        pickle.dump({'start': start_date, 'end': end_date}, f)

def load_run_dates():
    """加载上次运行的日期范围"""
    try:
        with open('day2_dates.pkl', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None

def check_completion_status():
    """检查day2任务是否已完成"""
    try:
        current_progress = load_progress()
        
        import os
        if os.path.exists('day2_total_pools.txt'):
            with open('day2_total_pools.txt', 'r') as f:
                total_pools = int(f.read().strip())
        else:
            return False
            
        if total_pools == 0:
            return False
            
        if current_progress >= total_pools:
            print("=" * 60)
            print("任务二已完成！")
            print(f"当前进度: {current_progress}/{total_pools} 个任务池")
            print(f"已处理二阶alpha: {current_progress * POOL_SIZE} 个")
            print(f"完成比例: {(current_progress/total_pools)*100:.1f}%")
            print("=" * 60)
            return True
        else:
            print("=" * 60)
            print("任务二进度报告")
            print(f"当前进度: {current_progress}/{total_pools} 个任务池")
            print(f"已处理二阶alpha: {current_progress * POOL_SIZE} 个")
            print(f"完成比例: {(current_progress/total_pools)*100:.1f}%")
            print("=" * 60)
            return False
    except Exception as e:
        logging.error(f"检查完成状态时出错: {e}")
        return False

def setup_session():
    """设置会话，包含错误处理"""
    try:
        s = login()
        logging.info("登录成功")
        return s
    except Exception as e:
        logging.error(f"登录失败: {e}")
        raise

def get_user_config():
    """获取用户配置"""
    global START_DATE, END_DATE
    print("=" * 60)
    print("          任务二 - 日期配置")
    print("=" * 60)
    print()
    print(f"默认开始日期: {DEFAULT_START_DATE}")
    print(f"默认结束日期: {DEFAULT_END_DATE}")
    print()
    print("说明: 从平台获取指定日期范围内提交的alpha")
    print("格式: MM-DD (例如 03-26)")
    print()
    
    start_input = input(f"请输入开始日期 (默认 {DEFAULT_START_DATE}): ").strip()
    if start_input:
        START_DATE = start_input
    
    end_input = input(f"请输入结束日期 (默认 {DEFAULT_END_DATE}): ").strip()
    if end_input:
        END_DATE = end_input
    
    print()
    print("=" * 60)
    print("配置摘要:")
    print(f"  开始日期: {START_DATE}")
    print(f"  结束日期: {END_DATE}")
    print(f"  Sharpe范围: {THRESHOLD_LOW} ~ {THRESHOLD_HIGH}")
    print(f"  区域: {REGION}")
    print("=" * 60)
    print()
    
    confirm = input("确认配置并开始任务二? (y/n): ").strip().lower()
    return confirm == 'y'

def main():
    global interrupted, START_DATE, END_DATE
    
    print("=" * 60)
    print("任务二：二阶alpha模拟")
    print("=" * 60)
    
    # 第一步：检查是否已有进度
    has_progress = os.path.exists('progress_day2.pkl')
    has_pools = os.path.exists('day2_total_pools.txt')
    
    if has_progress and has_pools:
        # 显示当前进度
        check_completion_status()
        
        current_progress = load_progress()
        with open('day2_total_pools.txt', 'r') as f:
            total_pools = int(f.read().strip())
        
        if current_progress >= total_pools:
            print("程序已完成所有任务，无需再次运行。")
            print("如需重新运行，请删除 progress_day2.pkl 和 day2_total_pools.txt 文件")
            return
        
        # 询问用户是否继续
        print()
        choice = input("检测到未完成的进度，是否继续? (y=继续/n=重新开始): ").strip().lower()
        if choice == 'y':
            # 继续上次的任务：直接登录后恢复模拟
            print(f"\n从第 {current_progress} 个任务池继续...")
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            try:
                s = setup_session()
                if interrupted: return
                
                # 需要重新生成alpha列表（因为没保存列表文件）
                # 直接用当前进度继续，但需要重新获取alpha来生成完整池
                # 由于续跑需要相同的alpha列表，提示用户输入相同日期
                print()
                # 提示上次使用的日期范围
                last_dates = load_run_dates()
                if last_dates:
                    print(f"上次使用的日期范围: {last_dates['start']} ~ {last_dates['end']}")
                    print("请输入相同的日期以保证任务池一致")
                else:
                    print("未找到上次运行的日期记录，请凭记忆输入")
                print()
                if not get_user_config():
                    print("用户取消操作")
                    return
                
                fo_tracker = get_alphas(START_DATE, END_DATE, THRESHOLD_HIGH, THRESHOLD_LOW, REGION, TOP_N, "track")
                if len(fo_tracker) == 0:
                    print("错误: 未获取到任何alpha，无法续跑。请检查日期是否正确。")
                    return
                
                fo_layer = prune(fo_tracker, PRUNE_TYPE, PRUNE_COUNT)
                if len(fo_layer) == 0:
                    print("错误: 剪枝后剩余0个alpha，无法续跑。")
                    return
                
                so_alpha_list = []
                # [迭代修改点] 续跑时的分组算子，需与首次运行保持一致
                group_ops = ["group_neutralize", "group_rank", "group_zscore"]
                for expr, decay in tqdm(fo_layer, desc="重建二阶alpha列表"):
                    if interrupted: return
                    alphas = get_group_second_order_factory([expr], group_ops, REGION)
                    so_alpha_list.extend([(alpha, decay) for alpha in alphas])
                
                random.seed(42)
                random.shuffle(so_alpha_list)
                so_pools = load_task_pool_single(so_alpha_list, POOL_SIZE)
                new_total = len(so_pools)
                
                if new_total != total_pools:
                    print(f"\n警告: 新生成的任务池数量({new_total})与上次({total_pools})不同!")
                    print("这可能是因为日期或平台数据发生了变化")
                    print(f"将使用新的总数量: {new_total}")
                    total_pools = new_total
                    with open('day2_total_pools.txt', 'w') as f:
                        f.write(str(total_pools))
                
                logging.info(f"从第 {current_progress} 个任务池继续，共 {total_pools} 个")
                
                try:
                    logging.info("Starting second-order simulation...")
                    for i, pool in enumerate(so_pools):
                        if i < current_progress:
                            continue
                        if interrupted:
                            save_progress(i)
                            return
                        single_simulate([pool], 'SUBINDUSTRY', REGION, 'TOP3000', 0)
                        save_progress(i + 1)
                    logging.info("Second-order simulation completed")
                    print("=" * 60)
                    print("恭喜！任务二已完成！")
                    print(f"已成功处理所有 {total_pools} 个任务池")
                    print(f"总计完成 {total_pools * POOL_SIZE} 个二阶alpha模拟")
                    print("请前往WorldQuant网站查看结果")
                    print("=" * 60)
                except Exception as e:
                    logging.error(f"Simulation failed: {e}")
                    save_progress(current_progress)
                    
            except KeyboardInterrupt:
                logging.warning("Program interrupted by user")
                save_progress(current_progress)
            return
        else:
            # 重新开始：清除进度
            os.remove('progress_day2.pkl')
            if os.path.exists('day2_total_pools.txt'):
                os.remove('day2_total_pools.txt')
            if os.path.exists('day2_dates.pkl'):
                os.remove('day2_dates.pkl')
            print("\n已清除上次进度，将重新开始。")
    
    # 第二步：没有进度或选择重新开始 → 配置日期
    print()
    if not get_user_config():
        print("用户取消操作")
        return
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 登录
        s = setup_session()
        if interrupted: return

        # 从平台按日期获取一阶alpha
        try:
            logging.info(f"从日期 {START_DATE} 到 {END_DATE} 获取一阶alpha")
            save_run_dates(START_DATE, END_DATE)  # 保存日期供续跑时提示
            fo_tracker = get_alphas(START_DATE, END_DATE, THRESHOLD_HIGH, THRESHOLD_LOW, REGION, TOP_N, "track")
            logging.info(f"获取了 {len(fo_tracker)} 个alpha跟踪器")
        except Exception as e:
            logging.error(f"获取alpha失败: {e}")
            return
        if interrupted: return

        # 检查是否获取到alpha
        if len(fo_tracker) == 0:
            print("=" * 60)
            print("错误: 在指定日期范围内未获取到任何alpha!")
            print("=" * 60)
            print(f"  日期范围: {START_DATE} ~ {END_DATE}")
            print(f"  Sharpe范围: {THRESHOLD_LOW} ~ {THRESHOLD_HIGH}")
            print()
            print("可能原因:")
            print("  1. 日期范围内没有提交过alpha")
            print("  2. 日期格式不正确（应为 MM-DD）")
            print("  3. Sharpe阈值过高，没有符合条件的alpha")
            print()
            print("请检查日期后重新运行程序")
            print("=" * 60)
            return

        # 剪枝
        fo_layer = prune(fo_tracker, PRUNE_TYPE, PRUNE_COUNT)
        logging.info(f"剪枝后保留 {len(fo_layer)} 个一阶alpha")
        if interrupted: return

        # 检查剪枝后是否还有alpha
        if len(fo_layer) == 0:
            print("=" * 60)
            print("错误: 剪枝后剩余0个alpha!")
            print("=" * 60)
            print(f"获取了 {len(fo_tracker)} 个alpha，但剪枝后全部被过滤")
            print()
            print("可能原因:")
            print("  1. 同一字段的alpha重复过多")
            print("  2. 剪枝参数 PRUNE_COUNT={PRUNE_COUNT} 过小")
            print()
            print("建议: 调大 PRUNE_COUNT 或扩大日期范围后重新运行")
            print("=" * 60)
            return

        # 生成二阶alpha
        so_alpha_list = []
        # [迭代修改点] 二阶Alpha使用的分组算子
        # 可选: "group_neutralize", "group_rank", "group_zscore", "group_mean", "group_normalize" 等
        # 新增算子需确保已在 WQ Brain 平台获得权限
        group_ops = ["group_neutralize", "group_rank", "group_zscore"]

        logging.info("Generating second-order alpha list...")
        for expr, decay in tqdm(fo_layer, desc="生成二阶alpha"):
            if interrupted: return
            alphas = get_group_second_order_factory([expr], group_ops, REGION)
            so_alpha_list.extend([(alpha, decay) for alpha in alphas])

        random.seed(42)
        random.shuffle(so_alpha_list)
        logging.info(f"Total second-order alphas: {len(so_alpha_list)}")
        if interrupted: return

        # 创建任务池
        so_pools = load_task_pool_single(so_alpha_list, POOL_SIZE)
        total_pools = len(so_pools)
        
        # 保存总任务池数量到文件
        with open('day2_total_pools.txt', 'w') as f:
            f.write(str(total_pools))
        
        logging.info(f"总任务池数量: {total_pools}")
        logging.info(f"总计二阶alpha数量: {total_pools * POOL_SIZE}")
        if interrupted: return

        # 开始模拟
        try:
            logging.info("Starting second-order simulation...")
            for i, pool in enumerate(so_pools):
                if interrupted:
                    save_progress(i)
                    return
                single_simulate([pool], 'SUBINDUSTRY', REGION, 'TOP3000', 0)
                save_progress(i + 1)
            logging.info("Second-order simulation completed")
            print("=" * 60)
            print("恭喜！任务二已完成！")
            print(f"已成功处理所有 {total_pools} 个任务池")
            print(f"总计完成 {total_pools * POOL_SIZE} 个二阶alpha模拟")
            print("请前往WorldQuant网站查看结果")
            print("=" * 60)
        except Exception as e:
            logging.error(f"Simulation failed: {e}")
            save_progress(load_progress())

    except KeyboardInterrupt:
        logging.warning("Program interrupted by user")
        save_progress(load_progress())

if __name__ == "__main__":
    main()

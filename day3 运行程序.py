# day3 三阶alpha生成程序
import logging
import random
import os
import signal
import sys
import pickle
from machine_lib import login, get_alphas, prune, trade_when_factory, load_task_pool_single, single_simulate
from tqdm import tqdm

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# [迭代修改点] 参数配置区域
# 三阶Alpha生成参数（trade_when条件交易）:
#   THRESHOLD_HIGH/LOW - 从平台获取二阶alpha的Sharpe范围（通常比day2更高）
#   TOP_N - 获取的alpha总数上限
#   PRUNE_TYPE - 剪枝字段前缀
#   PRUNE_COUNT - 每个字段保留的top alpha数量
#   INIT_DECAY - 初始decay值
#   POOL_SIZE - 每批并行模拟数量
DEFAULT_START_DATE = "03-25"
DEFAULT_END_DATE = "03-26"
THRESHOLD_HIGH = 1.3    # [迭代修改点] 三阶Sharpe阈值上限（比day2更高）
THRESHOLD_LOW = 1.0     # [迭代修改点] 三阶Sharpe阈值下限
REGION = 'USA'
TOP_N = 200             # [迭代修改点] 获取alpha数量
PRUNE_TYPE = 'anl4'     # [迭代修改点] 剪枝字段前缀
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
    with open('progress_day3.pkl', 'wb') as f:
        pickle.dump(processed_count, f)

def load_progress():
    try:
        with open('progress_day3.pkl', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return 0

def save_run_dates(start_date, end_date):
    """保存本次运行的日期范围，供续跑时提示"""
    with open('day3_dates.pkl', 'wb') as f:
        pickle.dump({'start': start_date, 'end': end_date}, f)

def load_run_dates():
    """加载上次运行的日期范围"""
    try:
        with open('day3_dates.pkl', 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None

def check_completion_status():
    """检查day3任务是否已完成"""
    try:
        current_progress = load_progress()
        
        # 需要先计算总任务池数量
        import os
        if os.path.exists('day3_total_pools.txt'):
            with open('day3_total_pools.txt', 'r') as f:
                total_pools = int(f.read().strip())
        else:
            # 如果还没有生成alpha列表，无法知道总数量
            return False
            
        if total_pools == 0:
            return False
            
        if current_progress >= total_pools:
            print("=" * 60)
            print("任务三已完成！")
            print(f"当前进度: {current_progress}/{total_pools} 个任务池")
            print(f"已处理三阶alpha: {current_progress * POOL_SIZE} 个")
            print(f"完成比例: {(current_progress/total_pools)*100:.1f}%")
            print("=" * 60)
            return True
        else:
            print("=" * 60)
            print("任务三进度报告")
            print(f"当前进度: {current_progress}/{total_pools} 个任务池")
            print(f"已处理三阶alpha: {current_progress * POOL_SIZE} 个")
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
    print("          任务三 - 日期配置")
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
    
    confirm = input("确认配置并开始任务三? (y/n): ").strip().lower()
    return confirm == 'y'

def main():
    global interrupted, START_DATE, END_DATE
    
    print("=" * 60)
    print("任务三：三阶alpha模拟")
    print("=" * 60)
    
    # 第一步：检查是否已有进度
    has_progress = os.path.exists('progress_day3.pkl')
    has_pools = os.path.exists('day3_total_pools.txt')
    
    if has_progress and has_pools:
        # 显示当前进度
        check_completion_status()
        
        current_progress = load_progress()
        with open('day3_total_pools.txt', 'r') as f:
            total_pools = int(f.read().strip())
        
        if current_progress >= total_pools:
            print("程序已完成所有任务，无需再次运行。")
            print("如需重新运行，请删除 progress_day3.pkl 和 day3_total_pools.txt 文件")
            return
        
        # 询问用户是否继续
        print()
        choice = input("检测到未完成的进度，是否继续? (y=继续/n=重新开始): ").strip().lower()
        if choice == 'y':
            # 继续上次的任务
            print(f"\n从第 {current_progress} 个任务池继续...")
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            try:
                s = setup_session()
                if interrupted: return
                
                # 需要重新获取alpha列表来重建任务池
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
                
                so_tracker = get_alphas(START_DATE, END_DATE, THRESHOLD_HIGH, THRESHOLD_LOW, REGION, TOP_N, "track")
                if len(so_tracker) == 0:
                    print("错误: 未获取到任何alpha，无法续跑。请检查日期是否正确。")
                    return
                
                so_layer = prune(so_tracker, PRUNE_TYPE, PRUNE_COUNT)
                if len(so_layer) == 0:
                    print("错误: 剪枝后剩余0个alpha，无法续跑。")
                    return
                
                th_alpha_list = []
                for expr, decay in tqdm(so_layer, desc="重建三阶alpha列表"):
                    if interrupted: return
                    alphas = trade_when_factory("trade_when", expr, REGION)
                    th_alpha_list.extend([(alpha, decay) for alpha in alphas])
                
                random.seed(42)
                random.shuffle(th_alpha_list)
                th_pools = load_task_pool_single(th_alpha_list, POOL_SIZE)
                new_total = len(th_pools)
                
                if new_total != total_pools:
                    print(f"\n警告: 新生成的任务池数量({new_total})与上次({total_pools})不同!")
                    print("这可能是因为日期或平台数据发生了变化")
                    print(f"将使用新的总数量: {new_total}")
                    total_pools = new_total
                    with open('day3_total_pools.txt', 'w') as f:
                        f.write(str(total_pools))
                
                logging.info(f"从第 {current_progress} 个任务池继续，共 {total_pools} 个")
                
                try:
                    logging.info("Starting third-order simulation...")
                    for i, pool in enumerate(th_pools):
                        if i < current_progress:
                            continue
                        if interrupted:
                            save_progress(i)
                            return
                        single_simulate([pool], 'SUBINDUSTRY', REGION, 'TOP3000', 0)
                        save_progress(i + 1)
                    logging.info("Third-order simulation completed")
                    print("=" * 60)
                    print("恭喜！任务三已完成！")
                    print(f"已成功处理所有 {total_pools} 个任务池")
                    print(f"总计完成 {total_pools * POOL_SIZE} 个三阶alpha模拟")
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
            os.remove('progress_day3.pkl')
            if os.path.exists('day3_total_pools.txt'):
                os.remove('day3_total_pools.txt')
            if os.path.exists('day3_dates.pkl'):
                os.remove('day3_dates.pkl')
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

        # 获取alpha跟踪器
        try:
            save_run_dates(START_DATE, END_DATE)  # 保存日期供续跑时提示
            so_tracker = get_alphas(START_DATE, END_DATE, THRESHOLD_HIGH, THRESHOLD_LOW, REGION, TOP_N, "track")
            logging.info(f"Retrieved {len(so_tracker)} second-order alpha trackers")
        except Exception as e:
            logging.error(f"获取alpha失败: {e}")
            return
        if interrupted: return

        # 检查是否获取到alpha
        if len(so_tracker) == 0:
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
        so_layer = prune(so_tracker, PRUNE_TYPE, PRUNE_COUNT)
        logging.info(f"Pruned to {len(so_layer)} alphas")
        if interrupted: return

        # 检查剪枝后是否还有alpha
        if len(so_layer) == 0:
            print("=" * 60)
            print("错误: 剪枝后剩余0个alpha!")
            print("=" * 60)
            print(f"获取了 {len(so_tracker)} 个alpha，但剪枝后全部被过滤")
            print()
            print("可能原因:")
            print("  1. 同一字段的alpha重复过多")
            print("  2. 剪枝参数 PRUNE_COUNT={PRUNE_COUNT} 过小")
            print()
            print("建议: 调大 PRUNE_COUNT 或扩大日期范围后重新运行")
            print("=" * 60)
            return

        # 生成三阶alpha
        # [迭代修改点] 三阶Alpha使用 trade_when 条件交易框架
        # 交易事件条件在 machine_lib.py 的 trade_when_factory() 中定义
        # 修改 open_events / exit_events / 地区事件 可调整交易策略
        th_alpha_list = []
        logging.info("Generating third-order alpha list...")
        for expr, decay in tqdm(so_layer, desc="生成三阶alpha"):
            if interrupted: return
            alphas = trade_when_factory("trade_when", expr, REGION)
            th_alpha_list.extend([(alpha, decay) for alpha in alphas])

        random.seed(42)
        random.shuffle(th_alpha_list)
        logging.info(f"Total third-order alphas: {len(th_alpha_list)}")
        if interrupted: return

        # 加载任务池并模拟
        th_pools = load_task_pool_single(th_alpha_list, POOL_SIZE)
        total_pools = len(th_pools)
        
        # 保存总任务池数量到文件
        with open('day3_total_pools.txt', 'w') as f:
            f.write(str(total_pools))
        
        logging.info(f"总任务池数量: {total_pools}")
        logging.info(f"总计三阶alpha数量: {total_pools * POOL_SIZE}")
        if interrupted: return

        # 开始模拟
        try:
            logging.info("Starting third-order simulation...")
            for i, pool in enumerate(th_pools):
                if interrupted:
                    save_progress(i)
                    return
                single_simulate([pool], 'SUBINDUSTRY', REGION, 'TOP3000', 0)
                save_progress(i + 1)
            logging.info("Third-order simulation completed")
            print("=" * 60)
            print("恭喜！任务三已完成！")
            print(f"已成功处理所有 {total_pools} 个任务池")
            print(f"总计完成 {total_pools * POOL_SIZE} 个三阶alpha模拟")
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

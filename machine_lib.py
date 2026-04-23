# -*- coding: utf-8 -*-
import requests
from os import environ, path
from time import sleep
import time
import json
import pandas as pd
import random
import pickle
import logging
from itertools import product
from itertools import combinations
from collections import defaultdict
from urllib.parse import urljoin

# 配置文件路径（不在 Git 中）
CREDENTIALS_FILE = path.join(path.dirname(path.abspath(__file__)), 'credentials.json')


def _load_credentials():
    """加载本地保存的账号密码"""
    if path.exists(CREDENTIALS_FILE):
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                data = json.load(f)
                return data.get('username', ''), data.get('password', '')
        except Exception:
            pass
    return None, None


def _save_credentials(username, password):
    """保存账号密码到本地"""
    with open(CREDENTIALS_FILE, 'w') as f:
        json.dump({'username': username, 'password': password}, f)


def _prompt_credentials():
    """提示用户输入账号密码"""
    print("\n" + "="*50)
    print("首次使用，请输入 WorldQuant 账号信息")
    print("="*50)
    username = input("用户名/邮箱: ").strip()
    password = input("密码: ").strip()
    
    while not username or not password:
        print("用户名和密码不能为空！")
        username = input("用户名/邮箱: ").strip()
        password = input("密码: ").strip()
    
    save = input("是否保存登录信息？(Y/n): ").strip().lower()
    if save != 'n':
        _save_credentials(username, password)
        print("已保存到 credentials.json")
    else:
        print("本次使用，不保存")
    
    return username, password


def login():
    """登录 WorldQuant Brain API
    首次使用会要求输入账号密码，后续自动读取本地保存的信息
    """
    username, password = _load_credentials()
    
    if not username or not password:
        username, password = _prompt_credentials()
    else:
        print(f"使用已保存的账号: {username}")

    # Create a session to persistently store the headers
    s = requests.Session()
 
    # Save credentials into session
    s.auth = (username, password)
 
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(response.content)
    return s
 

# ============================================================
# [迭代修改点] 一阶Alpha算子配置
#   - basic_ops: 基础截面算子（reverse, rank, zscore等）
#   - ts_ops:    时间序列算子（ts_delta, ts_mean等）
#   - ops_set:   合并后的完整算子集合
#   注意: day1运行程序.py 中的 ts_ops 是独立定义的，
#   修改此处不会影响 day1，如需统一请同步修改两处。
#   新增算子需确保在 WQ Brain 平台上已获得使用权限。
# ============================================================
basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]

ts_ops = ["ts_delta", "ts_sum", "ts_product",
          "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max", "ts_scale", "normalize", "zscore"]

ops_set = basic_ops + ts_ops

def normalize(df):
    """[迭代修改点] 归一化模板函数
    为每个字段生成3种归一化变体:
      1. normalize(useStd=false)
      2. ts_delta(ts_delta(x, 20), 20)  - 二阶差分
      3. group_rank(subindustry) - group_rank(market)  - 行业相对排名
      4. group_rank(market) - group_rank(group_mean(subindustry), market)  - 行业均值偏离
    修改此处可增删归一化变体模板。
    """
    add = []
    for i in df:
        add.append("normalize(%s, useStd = false, limit = 0.0)"%i)
    for j in df:
        add.append("ts_delta(ts_delta(%s, 20),20)"%j)
    for k in df:
        add.append("group_rank(%s, subindustry)-group_rank(%s, market)"%(k, k))
    for l in df:
        add.append(" group rank(%s, market)-group_rank(group_mean(%s, subindustry), market)"%(l, l))
        
    return add
def add():
    """[迭代修改点/已废弃] 旧版手动alpha模板函数
    硬编码了 fundamental6 (fnd6) 数据集的特定字段组合。
    当前版本已改用多数据集自动获取 + cross_dataset_factory 方式，
    此函数仅供历史参考，不建议在新的迭代中使用。
    如需复用此模式，请将 fnd6 字段替换为当前使用的字段ID。
    """
    datafields = [
"ts_arg_min(fnd6_cptmfmq_opepsq/fnd6_txw, 120)",
"ts_sum(fnd6_cibegni/fnd6_newa2v1300_ni, 240)",
"ts_rank(fnd6_city/fnd6_newa1v1300_lo, 66)",
"ts_sum(fnd6_mrct/fnd6_newa1v1300_cshfd, 66)",
"ts_delay(winsorize(ts_backfill(fnd6_acodo, 120), std=4)/winsorize(ts_backfill(fnd6_cptmfmq_atq, 120), std=4), 5)",
"ts_sum(winsorize(ts_backfill(fnd6_fyrc, 120), std=4)/winsorize(ts_backfill(vec_avg(fnd6_stype), 120), std=4), 120)",
"ts_sum(winsorize(ts_backfill(fnd6_cptnewqv1300_rectq, 120), std=4)/winsorize(ts_backfill(fnd6_newa1v1300_at, 120), std=4), 240)",
"ts_delta(winsorize(ts_backfill(fnd6_cshtr, 120), std=4)/winsorize(ts_backfill(fnd6_mfma1_csho, 120), std=4), 22)",
"ts_arg_min(winsorize(ts_backfill(fnd6_newqv1300_ciotherq, 120), std=4)/winsorize(ts_backfill(fnd6_newa1v1300_invt, 120), std=4), 240)",
"ts_arg_min(interest_expense/fnd6_invwip, 22)",
"ts_arg_max(fnd6_newa2v1300_rdipd/fnd6_newa1v1300_epsfx, 120)",
"ts_arg_min(fnd6_newa2v1300_rdipa/fnd6_mfma2_opeps, 120)",
"ts_arg_min(fnd6_fopox/fnd6_newa2v1300_rdipeps, 240)",
"ts_arg_min(fnd6_newa1v1300_invt/fnd6_dd1, 66)",
"ts_sum(winsorize(ts_backfill(vec_avg(fnd6_newqeventv110_optrfrq), 120), std=4)/winsorize(ts_backfill(fnd6_newa2v1300_mib, 120), std=4), 66)",
"ts_arg_max(fnd6_cshtrq/fnd6_newqv1300_esopnrq, 240)",
"ts_sum(winsorize(ts_backfill(fnd6_newa1v1300_dpc, 120), std=4)/winsorize(ts_backfill(fnd6_newqv1300_lseq, 120), std=4), 22)",
"ts_arg_min(fnd6_intan/fnd6_dvpa, 120)",
"ts_arg_min(fnd6_newa1v1300_gp/fnd6_currencya_curcd, 120)",
"ts_scale(winsorize(ts_backfill(fnd6_optca, 120), std=4)/winsorize(ts_backfill(fnd6_cik, 120), std=4), 66)",
"ts_sum(fnd6_cptmfmq_saleq/fnd6_newqv1300_lseq, 240)",
"ts_delta(fnd6_newa1v1300_epspi/fnd6_newqv1300_ibadj12, 240)",
"ts_delta(winsorize(ts_backfill(fnd6_cptmfmq_ceqq, 120), std=4)/winsorize(ts_backfill(liabilities_curr, 120), std=4), 66)",
"ts_mean(winsorize(ts_backfill(fnd6_newa1v1300_capx, 120), std=4)/winsorize(ts_backfill(fnd6_ch, 120), std=4), 22)",
"ts_sum(fnd6_dd3/cash_st, 5)",
"ts_arg_max(fnd6_txdbca/fnd6_newa2v1300_sale, 240)",
"ts_arg_min(fnd6_txbco/fnd6_cptnewqv1300_lctq, 120)",
"ts_std_dev(winsorize(ts_backfill(liabilities, 120), std=4)/winsorize(ts_backfill(fnd6_newa1v1300_epspi, 120), std=4), 66)",
"-ts_std_dev(fnd6_city/fnd6_xaccq, 66)",
"-ts_std_dev(fnd6_cptnewqv1300_opepsq/fnd6_cptnewqv1300_oeps12, 5)",
"-ts_quantile(winsorize(ts_backfill(fnd6_dlto, 120), std=4)/winsorize(ts_backfill(fnd6_txtubpospdec, 120), std=4), 5)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_mfmq_piq, 120), std=4)/winsorize(ts_backfill(assets_curr, 120), std=4), 66)",
"-ts_std_dev(fnd6_newa2v1300_seq/fnd6_xopr, 5)",
"-ts_delay(fnd6_cik/fnd6_cptnewqv1300_lctq, 120)",
"-ts_std_dev(winsorize(ts_backfill(goodwill, 120), std=4)/winsorize(ts_backfill(fnd6_newqv1300_icaptq, 120), std=4), 5)",
"-ts_std_dev(winsorize(ts_backfill(equity, 120), std=4)/winsorize(ts_backfill(fnd6_dpvieb, 120), std=4), 10)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_newqv1300_capsq, 120), std=4)/winsorize(ts_backfill(fnd6_cptnewqv1300_lctq, 120), std=4), 120)",
"-ts_sum(fnd6_mfma1_csho/fnd6_newa1v1300_dpc, 22)",
"-ts_std_dev(fnd6_ein/fnd6_newa2v1300_txt, 66)",
"-winsorize(ts_backfill(fnd6_newqv1300_seqq, 120), std=4)/winsorize(ts_backfill(fnd6_newqv1300_lseq, 120), std=4)",
"-ts_delay(winsorize(ts_backfill(fnd6_currencyqv1300_curcd, 120), std=4)/winsorize(ts_backfill(cash, 120), std=4), 120)",
"-ts_mean(fnd6_cptmfmq_ceqq/fnd6_xad, 22)",
"-ts_scale(winsorize(ts_backfill(fnd6_newa2v1300_optexd, 120), std=4)/winsorize(ts_backfill(fnd6_newa1v1300_csho, 120), std=4), 66)",
"-ts_sum(winsorize(ts_backfill(fnd6_mfmq_cshprq, 120), std=4)/winsorize(ts_backfill(assets_curr, 120), std=4), 120)",
"-winsorize(ts_backfill(fnd6_cik, 120), std=4)/winsorize(ts_backfill(fnd6_xaccq, 120), std=4)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_newa1v1300_epsfx, 120), std=4)/winsorize(ts_backfill(liabilities_curr, 120), std=4), 240)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_newqv1300_ibmiiq, 120), std=4)/winsorize(ts_backfill(fnd6_xrent, 120), std=4), 66)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_fopox, 120), std=4)/winsorize(ts_backfill(fnd6_newa2v1300_ppent, 120), std=4), 5)",
"-ts_delay(winsorize(ts_backfill(fnd6_cshtr, 120), std=4)/winsorize(ts_backfill(cogs, 120), std=4), 120)",
"-ts_delay(winsorize(ts_backfill(fnd6_adesinda_curcd, 120), std=4)/winsorize(ts_backfill(vec_avg(fnd6_newqeventv110_optrfrq), 120), std=4), 5)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_newa1v1300_cshfd, 120), std=4)/winsorize(ts_backfill(fnd6_ivaeq, 120), std=4), 22)",
"-ts_sum(winsorize(ts_backfill(debt, 120), std=4)/winsorize(ts_backfill(fnd6_donr, 120), std=4), 5)",
"-ts_std_dev(fnd6_cisecgl/fnd6_newqv1300_miiq, 22)",
"-ts_std_dev(winsorize(ts_backfill(fnd6_mfma1_at, 120), std=4)/winsorize(ts_backfill(liabilities_curr, 120), std=4), 120)"
]
    added = []
    for i in datafields:
        for j in datafields:
            if i != j:
                added.append("winsorize((%s+%s),std=4)"%(i, j))
    return added

def get_datasets(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" +\
        f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df


def get_datafields(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000',
    dataset_id: str = '',
    search: str = ''
):
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        
        # 先获取第一页看看响应结构
        try:
            max_retries = 3
            retry_delay = 10  # 10秒
            
            for attempt in range(max_retries):
                first_page = s.get(url_template.format(x=0))
                
                if first_page.status_code == 429:
                    # 速率限制，等待后重试
                    print(f"API速率限制 (429)，等待 {retry_delay}秒后重试 (尝试 {attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                    retry_delay += 5  # 每次重试增加等待时间
                    continue
                elif first_page.status_code != 200:
                    print(f"获取数据字段失败，状态码: {first_page.status_code}")
                    print(f"响应内容: {first_page.content[:500]}")
                    return pd.DataFrame()
                else:
                    # 成功获取
                    break
            else:
                # 所有重试都失败了
                print("API请求多次失败，可能达到并发限制上限")
                return pd.DataFrame()
                
            first_data = first_page.json()
            if 'count' not in first_data:
                print(f"响应中没有'count'字段: {list(first_data.keys())}")
                return pd.DataFrame()
                
            count = first_data['count']
            print(f"数据字段总数: {count}")
            
        except Exception as e:
            print(f"获取数据字段时发生错误: {e}")
            return pd.DataFrame()
        
    else:
        # 使用WQ平台原生搜索功能
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}" +\
            (f"&dataset.id={dataset_id}" if dataset_id else "") +\
            f"&limit=50" +\
            f"&search={search}" +\
            "&offset={x}"
        
        # 先获取第一页确认搜索有效性和总数
        try:
            first_page = s.get(url_template.format(x=0))
            if first_page.status_code != 200:
                print(f"搜索请求失败，状态码: {first_page.status_code}")
                return pd.DataFrame()
            
            first_data = first_page.json()
            if 'count' in first_data:
                count = min(first_data['count'], 500)  # 限制最多500条
                print(f"搜索结果总数: {count}")
            else:
                count = 0
                print(f"搜索无结果")
                return pd.DataFrame()
            
            if 'results' in first_data and first_data['results']:
                datafields_list = [first_data['results']]
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"搜索请求错误: {e}")
            return pd.DataFrame()
    
    # 通用分页获取逻辑（非搜索模式会设置count/url_template，搜索模式会跳过）
    if not datafields_list:  # 只有在datafields_list为空时才执行
        for x in range(0, count, 50):
            max_retries = 3
            retry_delay = 10
            
            for attempt in range(max_retries):
                try:
                    datafields = s.get(url_template.format(x=x))
                    
                    if datafields.status_code == 429:
                        print(f"第{x}页: API速率限制 (429)，等待 {retry_delay}秒后重试...")
                        time.sleep(retry_delay)
                        retry_delay += 5
                        continue
                    elif datafields.status_code != 200:
                        print(f"获取第{x}页失败，状态码: {datafields.status_code}")
                        if attempt == max_retries - 1:
                            break
                        else:
                            time.sleep(retry_delay)
                            retry_delay += 5
                            continue
                    
                    data_json = datafields.json()
                    if 'results' not in data_json:
                        print(f"第{x}页响应中没有'results'字段")
                        break
                        
                    datafields_list.append(data_json['results'])
                    sleep(2)
                    break
                    
                except Exception as e:
                    print(f"获取第{x}页时发生错误: {e}")
                    if attempt == max_retries - 1:
                        break
                    else:
                        time.sleep(retry_delay)
                        retry_delay += 5
                        continue
            else:
                print(f"第{x}页: 多次尝试失败，跳过这一页")

    if not datafields_list:
        print("没有获取到任何数据字段")
        return pd.DataFrame()
        
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    print(f"成功获取 {len(datafields_list_flat)} 个数据字段")

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df

def fnd6_fields(df, df1):
    """[迭代修改点] 字段比率组合函数（旧版，用于 fnd6 数据集内部比率）
    将 df1 中的字段两两做除法组合，套用 ts_backfill+winsorize 包装。
    当前跨数据集组合已由 cross_dataset_factory() 取代，
    如需对单数据集内部做比率组合可参考此模板。
    """
    vec_fields = []
    
    for field in df:
        for vec_op in df1:
            if vec_op != field:
                vec_fields.append("ts_backfill( winsorize(%s/%s,std=4),120)"%(vec_op, field))

    return vec_fields
def get_vec_fields(fields):
    """[迭代修改点] Vector字段预处理
    对 VECTOR 类型字段应用 vec 算子展开。
    当前支持的 vec_ops: vec_avg, vec_sum
    新增 vec 算子需在此处添加（确保已获得平台权限）。
    特殊算子 vec_choose 会生成 nth=-1 和 nth=0 两个变体。
    """
    # [迭代修改点] 请在此处添加获得权限的Vector操作符
    vec_ops = ["vec_avg", "vec_sum"]
    vec_fields = []
 
    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
            else:
                vec_fields.append("%s(%s)"%(vec_op, field))
 
    return(vec_fields)
def model77(df):
    """[迭代修改点/特定数据集] model77 数据集的预定义alpha模板
    包含人工设计的因子组合表达式，涵盖:
      - 价值评估类、成长性、风险效率、市场情绪、运营效率、特殊场景
    如需修改 model77 的alpha模板，直接编辑下方列表。
    注意: 这些表达式硬编码了 mdl77_ 前缀字段，仅适用于 model77 数据集。
    """
    doubao_fields_1 = [
    # 价值评估类组合
    # 经营性现金流与销售的企业价值比率对比，反映现金流转化效率
    "mdl77_2deepvaluefactor_pedwf - mdl77_2deepvaluefactor_estep", # 预期收益与远期收益差异，反映盈利预期分歧
    "(mdl77_2400_yen + mdl77_2deepvaluefactor_pfcfmtt) / 2", # 综合收益与自由现金流收益，衡量收益质量
    
    # 成长性组合
    "mdl77_2earningmomentumfactor400_gspea2y - mdl77_2earningmomentumfactor400_gspea1y", # 两年期与一年期EPS增长差异，反映成长加速度
    "mdl77_2gdna_pctchgocf * mdl77_2gdna_pctchgcf", # 经营现金流与总现金流的协同增长，验证成长可持续性
    "mdl77_2gdna_roic / mdl77_2gdna_susgrowth", # 资本回报率与可持续增长率的匹配度
    
    # 风险与效率指标
    "mdl77_2400_impvol - mdl77_2400_rmi", # 隐含波动率与历史波动率差值，衡量市场恐慌溢价
    "mdl77_2gdna_ttmaccu / mdl77_2gdna_ocfast", # 应计项目占经营性现金流的比例，检测盈余管理风险
    "mdl77_2gdna_debtcf * mdl77_2gdna_cfleverage", # 债务覆盖能力与杠杆率的交互作用
    
    # 市场情绪组合
    "mdl77_2400_chg12msip / mdl77_2400_chgshare", # 空头头寸变化与流通股变化的比值，反映做空动能
    "mdl77_2gdna_indrelrtn5d_ * mdl77_2gdna_visiratio", # 行业相对收益与交易量可见度的协同效应
    "mdl77_2earningmomentumfactor400_numrevq1 - mdl77_2earningmomentumfactor400_numrevy1", # 短期与长期预期修订差异
    # 运营效率组合
    "mdl77_2gdna_fixastto / mdl77_2gdna_astto", # 固定资产周转率与总资产周转率对比，检测资产结构效率
    "(mdl77_2gdna_ocfmargin - mdl77_2gdna_mpn) * 100", # 经营性现金流利润率与净利润率差值，衡量盈利现金含量
    "mdl77_2gdna_salerec / mdl77_2gdna_pca", # 销售与应收款增长匹配度，检测渠道健康度
    # 特殊场景指标
    "mdl77_2gdna_rel5yep / mdl77_2gdna_rel5yfcfp", # 五年期收益与自由现金流估值差异，识别价值陷阱
    "mdl77_2gdna_tobinq * mdl77_2gdna_pvan", # 托宾Q值与净资产价格比，检测资产定价异常
    "mdl77_2gdna_ebitdaev - mdl77_2gdna_vefcfmtt"  # EBITDA与自由现金流估值差异，识别资本支出压力
    "mdl77_2400_chg12msip / mdl77_2ad",  # 短期利息变动率与资产负债率的比率，反映融资成本压力与资本结构的关联
    "mdl77_2deepvaluefactor_ttmsaleev * mdl77_2gdna_gtl",  # 销售企业价值比与长期增长率乘积，衡量成长性估值匹配度
    "(mdl77_2gdna_roe - mdl77_2gdna_roic) / mdl77_2gdna_roic",  # ROE与ROIC差异率，反映资本结构对收益的影响
    "mdl77_2earningmomentumfactor400_sue / mdl77_2gdna_sigma",  # 标准化意外收益与股价波动率的比率，衡量盈利惊喜质量
    "mdl77_2mqf_ocfroi - mdl77_2mqf_cfroi",  # 经营性现金流回报与总现金流回报差值，识别核心业务质量
    "mdl77_2gdna_pctchg3yocf / mdl77_2gdna_pctchg3yfcf",  # 三年经营现金流与自由现金流增速比，评估现金流结构稳定性
    "mdl77_2deepvaluefactor_ydp / mdl77_2gdna_divyield",  # 预测股息率与TTM股息率比值，反映股息政策预期差
    "(mdl77_2gdna_curindep_ + mdl77_2gdna_curindfwdep_) / 2",  # 当前与预期EP行业相对值均值，构建双重估值锚
    "mdl77_2earningsqualityfactor_ttmaccu * mdl77_2gdna_betasigma",  # 会计应计与风险波动乘积，识别财务操纵风险
    "mdl77_2historicalgrowthfactor_saleg5y / mdl77_2gdna_equityto",  # 五年销售增长与权益周转率比率，衡量增长效率
    "mdl77_2pricemomentumfactor_rationalalpha - mdl77_2gdna_alpha60m",  # 理性衰减alpha与传统alpha差值，捕捉特殊动量因子
    "mdl77_2liquidityriskfactor_altmanz / mdl77_2gdna_booklev",  # Altman Z值与账面杠杆比率，量化破产风险层级
    "(mdl77_2gdna_rel5yep + mdl77_2gdna_rel5yfcfp) / 2",  # 五年相对EP与FCFP均值，构建复合估值指标
    "mdl77_2garpanalystmodel_qgp_roefcf * mdl77_2gdna_reinrate",  # ROE-FCF综合得分与再投资率乘积，评估可持续增长能力
    "mdl77_2gdna_ttmopincev / mdl77_2gdna_vefcfmtt",  # 经营利润EV比与自由现金流EV比，比较不同盈利口径估值
    "mdl77_2gdna_cashsev - mdl77_2deepvaluefactor_cashsev",  # 现金企业价值比模型间差异，捕捉估值分歧信号
    "mdl77_2earningmomentumfactor400_gspea2y / mdl77_2gdna_pedwf_cf",  # 两年盈利增速与预期EP比，构建动态PEG指标
    "mdl77_2gdna_indrelrtn5d_ * mdl77_2gdna_visiratio",  # 五日行业相对收益与交易可见度乘积，捕捉短期动量共振
    "(mdl77_2gdna_chgalpha12m + mdl77_2gdna_chgalpha36m) / 2",  # 多期alpha变化均值，监测风险因子漂移
    "mdl77_2gdna_salerec / mdl77_2gdna_cashc"  # 应收销售比与现金周期比率，评估营运资本效率

]
    return doubao_fields_1

def process_datafields1(df):
    """字段预处理（带 winsorize+ts_backfill 包装）
    MATRIX字段直接使用，VECTOR字段先做vec_avg/vec_sum展开。
    输出的每个字段表达式都会包裹: winsorize(ts_backfill(field, 120), std=4)
    """
    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    return ["winsorize(ts_backfill(%s, 120), std=4)"%field for field in datafields]

def process_datafields(df):
    """字段预处理（不带 winsorize/ts_backfill 包装，用于一阶alpha工厂）
    MATRIX字段直接使用，VECTOR字段先做vec_avg/vec_sum展开。
    一阶alpha工厂(first_order_factory)内部会自行添加算子包装。
    [迭代修改点] 如需在字段预处理阶段添加额外处理（如数据清洗），
    可在此函数中修改。
    """

    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    return datafields


def ts_factory(op, field):
    """[迭代修改点] 时间序列算子工厂
    为指定算子(field)生成多个时间窗口的alpha表达式。
    当前时间窗口: [5, 22, 66, 120, 240]（对应 ~1周/1月/3月/半年/1年）
    修改 days 列表可调整时间窗口。
    """
    output = []
    # [迭代修改点] 时间窗口配置 - 修改此处可增减时间跨度
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]
    
    for day in days:
    
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output

def first_order_factory(fields, ops_set):
    """[迭代修改点] 一阶Alpha生成工厂（核心函数）
    将每个字段与每个算子组合，生成一阶alpha表达式。
    
    算子分支逻辑:
      - ts_percentage / ts_decay_exp_window / ts_moment / ts_entropy:
        调用 ts_comp_factory 生成带参数的组合
      - ts_* 前缀: 调用 ts_factory 生成多时间窗口变体
      - vector 前缀: 调用 vector_factory 生成向量化变体
      - signed_power: 生成 power(field, 2)
      - normalize: 生成 normalize(field, useStd=false)
      - 其他: 直接包裹 field
    
    [迭代修改点] 如需新增算子类型，在 elif 分支中添加处理逻辑。
    新增算子必须是 WQ Brain 平台支持的 FASTEXPR 函数。
    """
    alpha_set = []
    #for field in fields:
    for field in fields:
        #reverse op does the work
        alpha_set.append(field)
        #alpha_set.append("-%s"%field)
        for op in ops_set:
 
            if op == "ts_percentage":
 
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])
 
 
            elif op == "ts_decay_exp_window":
 
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])
 
 
            elif op == "ts_moment":
 
                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])
 
            elif op == "ts_entropy":
 
                alpha_set += ts_comp_factory(op, field, "buckets", [10])
 
            elif op.startswith("ts_") or op == "inst_tvr":
 
                alpha_set += ts_factory(op, field)
 
            elif op.startswith("vector"):
 
                alpha_set += vector_factory(op, field)
 
            elif op == "signed_power":
 
                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)
            elif op == "normalize":
 
                alpha = "%s(%s, useStd = false, limit = 0.0)"%(op, field)
                alpha_set.append(alpha)
            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)
 
    return alpha_set

def load_task_pool_single(alpha_list, limit_of_single_simulations):

    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_single_simulations : number of concurrent single simulations
    Output:
        task : [3 * (alpha, decay)] for 3 single simulations
        pool : [ alpha_num/3 * [3 * (alpha, decay)] ] 
    '''


    pool = [alpha_list[i:i + limit_of_single_simulations] for i in range(0, len(alpha_list), limit_of_single_simulations)]
    return pool


def single_simulate(alpha_pool, neut, region, universe, start):
    """
    模拟Alpha，支持断线重连的递归版本

    参数:
        alpha_pool: Alpha池列表
        neut: 中性化方式
        region: 地区
        universe: 股票池
        start: 起始索引

    增强功能:
        - 自动检测认证失效(401/403)并重新认证
        - 自动检测连接错误并重试
        - 完善的错误日志记录
    """
    _single_simulate_recursive(alpha_pool, neut, region, universe, start, pool_index=0)


def _single_simulate_recursive(alpha_pool, neut, region, universe, start, pool_index=0, max_retries=3, retry_delay=10):
    """
    递归模拟Alpha，支持断线重连

    参数:
        alpha_pool: Alpha池列表
        neut: 中性化方式
        region: 地区
        universe: 股票池
        start: 当前池内的起始索引
        pool_index: 池索引
        max_retries: 最大重试次数
        retry_delay: 重试延迟(秒)
    """
    # 基准情况：如果已处理完所有池，返回
    if pool_index >= len(alpha_pool):
        print("Simulate done - all pools completed")
        return

    task = alpha_pool[pool_index]
    if pool_index < start:
        return _single_simulate_recursive(alpha_pool, neut, region, universe, start, pool_index + 1, max_retries, retry_delay)

    print(f"\n{'='*60}")
    print(f"处理任务池 {pool_index + 1}/{len(alpha_pool)}")
    print(f"{'='*60}")

    # 确保有有效的session
    try:
        s = login()
    except Exception as e:
        print(f"认证失败: {e}")
        if max_retries > 0:
            print(f"等待 {retry_delay} 秒后重试...")
            sleep(retry_delay)
            return _single_simulate_recursive(alpha_pool, neut, region, universe, start, pool_index, max_retries - 1, retry_delay)
        else:
            print("达到最大重试次数，程序退出")
            return

    progress_urls = []
    failed_alphas = []  # 记录失败的alpha索引

    # 第一步：提交所有Alpha
    print(f"正在提交 {len(task)} 个Alpha...")
    for y, (alpha, decay) in enumerate(task):
        if y < start and pool_index == start:
            continue

        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': universe,
                'delay': 1,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'testPeriod': 'P0Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha
        }

        try:
            simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=simulation_data)

            # 检测认证错误
            if simulation_response.status_code == 401 or simulation_response.status_code == 403:
                print(f"检测到认证失效 (状态码: {simulation_response.status_code})，重新认证...")
                sleep(retry_delay)
                new_s = login()
                # 重试当前alpha
                retry_resp = new_s.post('https://api.worldquantbrain.com/simulations', json=simulation_data)
                if retry_resp.status_code in (200, 201):
                    progress_urls.append(retry_resp.headers['Location'])
                else:
                    print(f"重试失败: {retry_resp.status_code} - {alpha[:50]}...")
                    failed_alphas.append((y, alpha))
                s = new_s
                continue

            if simulation_response.status_code not in (200, 201):
                print(f"提交失败 ({simulation_response.status_code}): {alpha[:50]}...")
                failed_alphas.append((y, alpha))
                continue

            simulation_progress_url = simulation_response.headers.get('Location')
            if simulation_progress_url:
                progress_urls.append(simulation_progress_url)
            else:
                print(f"无Location头: {simulation_response.content[:100]}")
                failed_alphas.append((y, alpha))

        except requests.exceptions.ConnectionError as e:
            print(f"连接错误: {str(e)[:50]}...")
            print(f"等待 {retry_delay} 秒后重试...")
            sleep(retry_delay)
            try:
                s = login()
                print("重新认证成功")
            except Exception as auth_error:
                print(f"重新认证失败: {auth_error}")
                failed_alphas.append((y, alpha))
        except Exception as e:
            print(f"提交异常: {str(e)[:50]}...")
            failed_alphas.append((y, alpha))

    print(f"任务池 {pool_index + 1} 提交完成: 成功 {len(progress_urls)}, 失败 {len(failed_alphas)}")

    # 第二步：等待并检查模拟结果
    print(f"等待 {len(progress_urls)} 个模拟结果...")
    completed_count = 0
    failed_results = []

    for j, progress in enumerate(progress_urls):
        try:
            while True:
                simulation_progress = s.get(progress)
                retry_after = simulation_progress.headers.get("Retry-After", "0")

                if retry_after == "0" or retry_after == 0:
                    break
                sleep(float(retry_after))

            resp_json = simulation_progress.json()
            status = resp_json.get("status", 0)
            alpha_id = resp_json.get("alpha", "N/A")

            if status in ("COMPLETE", "WARNING"):
                completed_count += 1
                if status == "WARNING":
                    print(f"  [{j+1}] {alpha_id}: {status}")
            else:
                print(f"  [{j+1}] {alpha_id}: 状态={status}")
                failed_results.append(progress)

        except requests.exceptions.ConnectionError as e:
            print(f"连接错误 (检查结果): {str(e)[:50]}...")
            print("尝试重新连接...")
            sleep(retry_delay)
            try:
                s = login()
            except:
                pass
            failed_results.append(progress)
        except KeyError as e:
            print(f"响应格式错误: {e} - {progress}")
            failed_results.append(progress)
        except Exception as e:
            print(f"检查结果异常: {str(e)[:50]}...")
            failed_results.append(progress)

    print(f"任务池 {pool_index + 1} 完成: 成功 {completed_count}/{len(progress_urls)}, 失败 {len(failed_results)}")

    if failed_alphas:
        print(f"提交失败的Alpha: {len(failed_alphas)} 个")
    if failed_results:
        print(f"结果检查失败的URL: {len(failed_results)} 个")

    print(f"任务池 {pool_index + 1} 模拟完成")

    # 递归处理下一个池
    return _single_simulate_recursive(alpha_pool, neut, region, universe, 0, pool_index + 1, max_retries, retry_delay)


def fnd6_fields(pc):
    """[旧版/已废弃] 字段比率函数 - 与上方同名函数功能不同
    此版本生成简单比率 field_a/field_b（无 winsorize/ts_backfill 包装）。
    保留仅供历史参考，新代码请使用 cross_dataset_factory。
    """
    vec_fields = []
    for field in pc:
        for vec_op in pc:
            if vec_op != field:
                vec_fields.append("%s/%s"%(vec_op, field))
    return vec_fields

def set_alpha_properties(
    s,
    alpha_id,
    name: str = None,
    color: str = None,
    selection_desc: str = "None",
    combo_desc: str = "None",
    tags: str = ["ace_tag"],
):
    """
    Function changes alpha's description parameters
    """
 
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": None},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )


def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2026-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2026-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2026-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2026-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            #print(response.json())
            try:
                alpha_list = response.json()["results"]
                #print(response.json())
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1
                    #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s"%exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay*4)
                        elif turnover > 0.6:
                            rec.append(decay*3+3)
                        elif turnover > 0.5:
                            rec.append(decay*3)
                        elif turnover > 0.4:
                            rec.append(decay*2)
                        elif turnover > 0.35:
                            rec.append(decay+4)
                        elif turnover > 0.3:
                            rec.append(decay+2)
                        output.append(rec)
            except:
                print("%d finished re-login"%i)
                s = login()

    print("count: %d"%count)
    return output

def prune(next_alpha_recs, prefix, keep_num):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s"%field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp,decay])
    return output

def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order


def group_factory(op, field, region):
    """[迭代修改点] 分组算子工厂（用于二阶alpha生成）
    将一阶alpha按不同分组方式应用 group 操作。
    
    分组方式:
      - 标准分组: market, sector, industry, subindustry
      - 自定义分组: cap_bucket, asset_bucket, sector_cap, sector_asset, vol, liquidity
      - 地区特有分组: usa_group_13（仅USA区域有效）
    
    [迭代修改点] 如需新增分组方式:
      1. 在 groups 列表中添加新的分组表达式
      2. 对于地区特有分组，在对应的 xxx_group 列表中添加
    """
    output = []
    # [迭代修改点] 向量化分组因子，可添加 "sharesout", "volume" 等
    vectors = ["cap"] 
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"

    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"

    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry",
              cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]
    
    groups += usa_group_13
        
    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))"%(op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)"%(op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))"%(op, field, group)
            output.append(alpha)
        
    return output

def trade_when_factory(op, field, region):
    """[迭代修改点] trade_when 条件交易工厂（用于三阶alpha生成）
    将二阶alpha嵌入条件交易框架: trade_when(open_event, alpha, exit_event)
    
    事件类型:
      - open_events:  触发买入的事件条件（量价关系、波动率、回归信号等）
      - exit_events:  触发卖出的事件条件（-1表示持仓直到下一个open事件）
      - 地区事件:     usa_events, asi_events, eur_events, glb_events, chn_events, kor_events, twn_events
    
    [迭代修改点] 如需新增交易事件:
      1. 在 open_events 中添加新的触发条件表达式
      2. 在对应区域的 xxx_events 中添加地区特有事件
      3. 注意: 事件表达式必须是有效的 FASTEXPR 布尔表达式
      4. open_events 和 exit_events 做笛卡尔积组合，数量会相乘
    """
    output = []
    # [迭代修改点] 开仓事件条件 - 修改此处可增减交易触发条件
    open_events = ["pcr_oi_270<1","ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3", "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3", "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]

    exit_events = ["abs(returns) > 0.1", "-1"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8", "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8", "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1",]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)"%(op, oe, field, ee)
            output.append(alpha)
    return output


def check_submission(alpha_bag, gold_bag, start):
    depot = []
    s = login()
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            s = login()
        #print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
            print("check self-corrlation error")
            sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append(g)
        else:
            print(g)
            gold_bag.append((g, pc))
    print(depot)
    return gold_bag


def get_check_submission(s, alpha_id):
    while True:
        result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    try:
        if result.json().get("is", 0) == 0:
            print("logged out")
            return "sleep"
        checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
        )
        pc = checks_df[checks_df.name == "SELF_CORRELATION"]["value"].values[0]
        if not any(checks_df["result"] == "FAIL"):
            return pc
        else:
            return "fail"
    except:
        print("catch: %s"%(alpha_id))
        return "error"
    
def view_alphas(gold_bag):
    s = login()
    sharp_list = []
    for gold, pc in gold_bag:

        triple = locate_alpha(s, gold)
        info = [triple[0], triple[2], triple[3], triple[4], triple[5], triple[6], triple[1]]
        info.append(pc)
        sharp_list.append(info)

    sharp_list.sort(reverse=True, key = lambda x : x[1])
    for i in sharp_list:
        print(i)
 
def locate_alpha(s, alpha_id):
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    #print(metrics["regular"]["code"])
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']
    
    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    return triple
            

# Consultant methods
def multi_simulate(alpha_pools, neut, region, universe, start):

    s = login()

    brain_api_url = 'https://api.worldquantbrain.com'

    for x, pool in enumerate(alpha_pools):
        if x < start: continue
        progress_urls = []
        for y, task in enumerate(pool):
            # 10 tasks, 10 alpha in each task
            sim_data_list = generate_sim_data(task, region, universe, neut)
            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                simulation_progress_url = simulation_response.headers['Location']
                progress_urls.append(simulation_progress_url)
            except:
                print("loc key error: %s"%simulation_response.content)
                sleep(600)
                s = login()

        print("pool %d task %d post done"%(x,y))

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)
                if status != "COMPLETE":
                    print("Not complete : %s"%(progress))

                """
                #alpha_id = simulation_progress.json()["alpha"]
                children = simulation_progress.json().get("children", 0)
                children_list = []
                for child in children:
                    child_progress = s.get(brain_api_url + "/simulations/" + child)
                    alpha_id = child_progress.json()["alpha"]

                    set_alpha_properties(s,
                            alpha_id,
                            name = "%s"%name,
                            color = None,)
                """
            except KeyError:
                print("look into: %s"%progress)
            except:
                print("other")


        print("pool %d task %d simulate done"%(x, j))
    
    print("Simulate done")

def generate_sim_data(alpha_list, region, uni, neut):
    sim_data_list = []
    for alpha, decay in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': 1,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'testPeriod': 'P2Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha}

        sim_data_list.append(simulation_data)
    return sim_data_list

def load_task_pool(alpha_list, limit_of_children_simulations, limit_of_multi_simulations):
    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_multi_simulations : number of children simulation in a multi-simulation
        limit_of_multi_simulations : number of simultaneous multi-simulations
    Output:
        task : [10 * (alpha, decay)] for a multi-simulation
        pool : [10 * [10 * (alpha, decay)]] for simultaneous multi-simulations
        pools : [[10 * [10 * (alpha, decay)]]]

    '''
    tasks = [alpha_list[i:i + limit_of_children_simulations] for i in range(0, len(alpha_list), limit_of_children_simulations)]
    pools = [tasks[i:i + limit_of_multi_simulations] for i in range(0, len(tasks), limit_of_multi_simulations)]
    return pools


# some other factory for other operators
def vector_factory(op, field):
    output = []
    vectors = ["cap"]
    
    for vector in vectors:
    
        alpha = "%s(%s, %s)"%(op, field, vector)
        output.append(alpha)
    
    return output
 
def ts_comp_factory(op, field, factor, paras):
    """[迭代修改点] 带参数的时间序列算子工厂
    为 ts_percentage, ts_decay_exp_window, ts_moment, ts_entropy 等算子
    生成 (时间窗口 x 参数) 的笛卡尔积组合。
    
    [迭代修改点] 修改 l1 (时间窗口列表) 和 l2 (参数列表) 可调整组合粒度。
    """
    output = []
    # [迭代修改点] 时间窗口和参数配置
    #l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 240], paras
    comb = list(product(l1, l2))
    
    for day,para in comb:
        
        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)"%(op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)"%(op, field, day, factor, para)
            
        output.append(alpha)
    
    return output
 
def twin_field_factory(op, field, fields):
    """[迭代修改点] 双字段时间序列算子工厂
    将指定字段与其他所有字段配对，生成双输入的时间序列alpha。
    例如: ts_corr(fieldA, fieldB, day)
    
    [迭代修改点] 修改 days 列表可调整时间窗口。
    """
    output = []
    # [迭代修改点] 时间窗口配置
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))
    
    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)"%(op, field, counterpart, day)
            output.append(alpha)
    
    return output
 
# ==================== 跨数据集Alpha组合 ====================
# ============================================================
# [迭代修改点] 跨数据集配置区域
#   以下三个数据结构是多数据集Alpha生成的核心配置：
#   1. ALL_DATASETS - 可用数据集注册表
#   2. CROSS_DATASET_RULES - 跨数据集组合规则
#   3. cross_dataset_factory() - 组合生成逻辑
#
#   新增数据集: 在 ALL_DATASETS 中添加条目
#   新增组合规则: 在 CROSS_DATASET_RULES 中添加规则元组
#   修改组合逻辑: 修改 cross_dataset_factory() 函数
# ============================================================

# [迭代修改点] 可用数据集注册表
# 每个条目: {id, category, desc}
# - id: WQ Brain平台的数据集ID，必须与API返回的一致
# - category: 分类标签，用于跨数据集规则匹配
# - desc: 数据集中文描述，仅用于UI显示
# 新增数据集时: 确保category与CROSS_DATASET_RULES中使用的类别一致
ALL_DATASETS = [
    {'id': 'analyst4',      'category': 'analyst',     'desc': '分析师一致预期（EPS/销售额/股息等）'},
    {'id': 'fundamental2',   'category': 'fundamental', 'desc': '财务报表补充项目'},
    {'id': 'fundamental6',   'category': 'fundamental', 'desc': '全球大公司基本面数据库'},
    {'id': 'model51',        'category': 'model',       'desc': '风险模型指标'},
    {'id': 'news12',         'category': 'news',        'desc': '财经新闻匹配数据'},
    {'id': 'news18',         'category': 'news',        'desc': '新闻情绪指标'},
    {'id': 'option8',        'category': 'option',      'desc': '期权隐含波动率'},
    {'id': 'option9',        'category': 'option',      'desc': '期权情绪指标'},
    {'id': 'pv1',            'category': 'pv',          'desc': '价格成交量数据'},
    {'id': 'socialmedia12',  'category': 'socialmedia', 'desc': '社交媒体情绪分析'},
    {'id': 'socialmedia8',   'category': 'socialmedia', 'desc': 'Twitter情绪数据'},
]

# [迭代修改点] 跨数据集逻辑组合规则
# 格式: (分子类别, 分母类别, 描述, 前缀)
# - 分子类别/分母类别: 必须与 ALL_DATASETS 中的 category 字段匹配
# - 描述: 用于日志显示
# - 前缀: 目前仅用于日志标识
#
# 新增规则示例:
#   ('socialmedia', 'analyst', '社交媒体/分析师', 'soc_anl'),
# 修改组合数上限请调整 cross_dataset_factory() 的 max_combinations 参数
CROSS_DATASET_RULES = [
    ('fundamental', 'pv',          '基本面/价格',       'fnd_pv'),
    ('fundamental', 'analyst',     '基本面/分析师预期',  'fnd_anl'),
    ('analyst',     'pv',          '分析师预期/价格',    'anl_pv'),
    ('news',        'pv',          '新闻情绪/价格',      'news_pv'),
    ('socialmedia', 'pv',          '社交媒体/价格',      'soc_pv'),
    ('fundamental', 'news',        '基本面/新闻情绪',    'fnd_news'),
    ('analyst',     'news',        '分析师/新闻情绪',    'anl_news'),
    ('option',      'pv',          '期权指标/价格',      'opt_pv'),
    ('model',       'pv',          '风险指标/价格',      'mdl_pv'),
    ('news',        'fundamental', '新闻/基本面',        'news_fnd'),
    ('socialmedia', 'fundamental', '社交媒体/基本面',    'soc_fnd'),
    ('analyst',     'option',      '分析师预期/期权',    'anl_opt'),
    ('news',        'socialmedia', '新闻/社交媒体',      'news_soc'),
]

def get_fields_by_category(dataset_fields_dict, category):
    """
    根据数据集类别获取已处理的字段表达式列表
    dataset_fields_dict: {dataset_id: [field_expressions]}
    category: 数据集类别 (fundamental, pv, analyst, news, socialmedia, option, model)
    """
    fields = []
    category_map = {ds['id']: ds['category'] for ds in ALL_DATASETS}
    for ds_id, exprs in dataset_fields_dict.items():
        if category_map.get(ds_id) == category:
            fields.extend(exprs)
    return fields

def cross_dataset_factory(dataset_fields_dict, max_combinations=500):
    """
    生成跨数据集的逻辑组合Alpha
    dataset_fields_dict: {dataset_id: [processed_field_expressions]}
    max_combinations: 每组规则的最大组合数
    
    注意：跨数据集组合直接使用原始字段表达式做比率，
    避免多层嵌套 winsorize/ts_backfill
    """
    output = []
    
    # 将数据集字段按类别分组
    category_fields = {}
    category_map = {ds['id']: ds['category'] for ds in ALL_DATASETS}
    for ds_id, exprs in dataset_fields_dict.items():
        cat = category_map.get(ds_id, 'unknown')
        if cat not in category_fields:
            category_fields[cat] = []
        category_fields[cat].extend(exprs)
    
    # 统计各类别字段数
    logging.info("各类别字段数量:")
    for cat, fields in category_fields.items():
        logging.info(f"  {cat}: {len(fields)}")
    
    # 为每组规则生成组合
    for num_field_cat, den_field_cat, desc, prefix in CROSS_DATASET_RULES:
        num_fields = category_fields.get(num_field_cat, [])
        den_fields = category_fields.get(den_field_cat, [])
        
        if not num_fields or not den_fields:
            continue
        
        # 控制组合数量
        combos = 0
        for nf in num_fields:
            if combos >= max_combinations:
                break
            for df_field in den_fields:
                if combos >= max_combinations:
                    break
                # 避免自引用
                if nf == df_field:
                    continue
                ratio = f"winsorize(ts_backfill({nf}/{df_field}, 120), std=4)"
                output.append(ratio)
                combos += 1
        
        logging.info(f"  {desc}: {combos} 个组合")
    
    return output

def fetch_multi_dataset_fields(s, dataset_ids, region='USA', universe='TOP3000', delay=1):
    """
    从多个数据集获取字段并处理
    返回: {dataset_id: [processed_field_expressions]}
    """
    result = {}
    
    for ds_id in dataset_ids:
        try:
            logging.info(f"获取数据集 {ds_id} 的字段...")
            df = get_datafields(s, dataset_id=ds_id, region=region, universe=universe, delay=delay)
            
            if df.empty:
                logging.warning(f"数据集 {ds_id} 没有可用字段，跳过")
                continue
            
            processed = process_datafields(df)
            result[ds_id] = processed
            logging.info(f"  {ds_id}: {len(df)} 个原始字段, {len(processed)} 个处理后字段")
            
        except Exception as e:
            logging.error(f"获取数据集 {ds_id} 失败: {e}")
            continue
    
    return result

def login_hk():
    """登录 WorldQuant Brain API (支持生物认证版本)
    首次使用会要求输入账号密码，后续自动读取本地保存的信息
    """
    username, password = _load_credentials()
    
    if not username or not password:
        username, password = _prompt_credentials()
    else:
        print(f"使用已保存的账号: {username}")
    
    # Create a session to persistently store the headers
    s = requests.Session()
    
    # Save credentials into session
    s.auth = (username, password)
    
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    
    if response.status_code == requests.codes.unauthorized:
        # Check if biometrics is required
        if response.headers.get("WWW-Authenticate") == "persona":
            print(
                "Complete biometrics authentication by scanning your face. Follow the link: \n"
                + urljoin(response.url, response.headers["Location"]) + "\n"
            )
            input("Press any key after you complete the biometrics authentication.")
            
            # Retry the authentication after biometrics
            biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
            
            while biometrics_response.status_code != 201:
                input("Biometrics authentication is not complete. Please try again and press any key when completed.")
                biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
                
            print("Biometrics authentication completed.")
        else:
            print("\nIncorrect username or password. Please check your credentials.\n")
            # 删除错误的凭证文件，提示重新输入
            if path.exists(CREDENTIALS_FILE):
                import os
                os.remove(CREDENTIALS_FILE)
            print("已删除保存的凭证，请重新运行程序输入正确的账号密码。")
    else:
        print("Logged in successfully.")
    
    return s 

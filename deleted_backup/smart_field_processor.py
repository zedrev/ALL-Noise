#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能字段处理系统
为Price Volume Data for Equity (pv1)数据集专门设计
"""
import re
from collections import defaultdict

class SmartFieldProcessor:
    """智能字段处理器"""
    
    def __init__(self):
        # 字段分类规则
        self.field_categories = {
            'price': ['close', 'open', 'high', 'low', 'prc', 'price'],
            'volume': ['volume', 'vol', 'tvr', 'turnover', 'adv'],
            'return': ['return', 'ret', 'change', 'rtn'],
            'market_cap': ['cap', 'marketcap', 'mktcap'],
            'dividend': ['dividend', 'div'],
            'shares': ['shares', 'shrs'],
            'volatility': ['std', 'volatility', 'var'],
            'liquidity': ['liq', 'liquid', 'trading'],
        }
        
        # 字段类型对应的推荐操作符
        self.recommended_ops = {
            'price': {
                'ts_ops': ['ts_delta', 'ts_mean', 'ts_rank', 'ts_zscore', 'ts_std_dev', 'ts_corr'],
                'basic_ops': ['rank', 'zscore', 'normalize', 'reverse'],
                'decay_range': (3, 6),  # 短期到中期衰减
            },
            'volume': {
                'ts_ops': ['ts_sum', 'ts_delta', 'ts_mean', 'ts_rank', 'ts_std_dev'],
                'basic_ops': ['rank', 'zscore', 'normalize'],
                'decay_range': (6, 9),  # 中期衰减
            },
            'return': {
                'ts_ops': ['ts_sum', 'ts_delta', 'ts_std_dev', 'ts_mean', 'ts_zscore'],
                'basic_ops': ['rank', 'zscore', 'normalize'],
                'decay_range': (4, 8),  # 中短期衰减
            },
            'market_cap': {
                'ts_ops': ['ts_delta', 'ts_rank', 'ts_mean'],
                'basic_ops': ['rank', 'normalize'],
                'decay_range': (12, 20),  # 长期缓慢衰减
            },
            'default': {
                'ts_ops': ['ts_delta', 'ts_mean', 'ts_rank', 'ts_zscore'],
                'basic_ops': ['rank', 'normalize'],
                'decay_range': (6, 12),  # 默认衰减
            }
        }
        
        # 智能字段组合规则
        self.field_combinations = {
            ('price', 'volume'): ['ratio', 'product', 'correlation'],
            ('price', 'return'): ['momentum', 'reversal'],
            ('volume', 'return'): ['volume_price_trend'],
            ('price', 'market_cap'): ['size_adjusted_price'],
        }
    
    def classify_field(self, field_name):
        """智能分类字段"""
        field_lower = field_name.lower()
        
        for category, keywords in self.field_categories.items():
            for keyword in keywords:
                if keyword in field_lower:
                    return category
        
        # 检查是否为衍生字段（包含运算符）
        if '/' in field_name or '*' in field_name or '+' in field_name or '-' in field_name:
            return 'derived'
        
        return 'default'
    
    def get_recommended_ops(self, field_category, alpha_type='momentum'):
        """获取推荐的操作符"""
        if field_category not in self.recommended_ops:
            field_category = 'default'
        
        ops_config = self.recommended_ops[field_category]
        
        # 根据alpha类型微调
        if alpha_type == 'momentum':
            # 动量策略：侧重变化率
            ops_config['ts_ops'] = ['ts_delta', 'ts_rank', 'ts_zscore'] + ops_config['ts_ops']
        elif alpha_type == 'mean_reversion':
            # 均值回归：侧重标准化和排名
            ops_config['ts_ops'] = ['ts_zscore', 'ts_rank', 'ts_mean'] + ops_config['ts_ops']
        elif alpha_type == 'volatility':
            # 波动率策略：侧重标准差
            ops_config['ts_ops'] = ['ts_std_dev', 'ts_scale'] + ops_config['ts_ops']
        
        return ops_config
    
    def generate_smart_decay(self, field_category, alpha_expr):
        """智能生成decay值"""
        if field_category not in self.recommended_ops:
            field_category = 'default'
        
        decay_min, decay_max = self.recommended_ops[field_category]['decay_range']
        
        # 根据alpha表达式复杂度调整decay
        alpha_lower = alpha_expr.lower()
        
        # 简单表达式：使用较小的decay
        complexity_score = 0
        if 'ts_delta' in alpha_lower:
            complexity_score += 1
        if 'ts_mean' in alpha_lower:
            complexity_score += 2
        if 'ts_std_dev' in alpha_lower:
            complexity_score += 3
        if 'ts_corr' in alpha_lower:
            complexity_score += 4
        
        # 根据复杂度调整decay
        if complexity_score <= 1:
            return decay_min
        elif complexity_score <= 3:
            return (decay_min + decay_max) // 2
        else:
            return decay_max
    
    def generate_field_combinations(self, fields_dict):
        """生成智能字段组合"""
        combinations = []
        
        # 按类别组织字段
        categorized_fields = defaultdict(list)
        for field in fields_dict.get('all_fields', []):
            category = self.classify_field(field)
            categorized_fields[category].append(field)
        
        # 生成类别内组合
        for category, field_list in categorized_fields.items():
            if len(field_list) >= 2:
                # 生成简单比率组合
                for i in range(len(field_list)):
                    for j in range(i+1, len(field_list)):
                        combinations.append(f"{field_list[i]}/{field_list[j]}")
                        combinations.append(f"{field_list[i]} - {field_list[j]}")
        
        # 生成跨类别组合（基于规则）
        for (cat1, cat2), combo_types in self.field_combinations.items():
            if cat1 in categorized_fields and cat2 in categorized_fields:
                for field1 in categorized_fields[cat1][:3]:  # 限制数量
                    for field2 in categorized_fields[cat2][:3]:
                        for combo_type in combo_types:
                            if combo_type == 'ratio':
                                combinations.append(f"{field1}/{field2}")
                            elif combo_type == 'product':
                                combinations.append(f"{field1}*{field2}")
                            elif combo_type == 'correlation':
                                combinations.append(f"ts_corr({field1}, {field2}, 20)")
                            elif combo_type == 'momentum':
                                combinations.append(f"ts_delta({field1}, 5)/{field2}")
        
        return combinations
    
    def optimize_time_windows(self, field_category, alpha_type='momentum'):
        """优化时间窗口参数"""
        # 针对不同策略的时间窗口
        if alpha_type == 'high_frequency':
            return [1, 3, 5, 10, 20]  # 日内/超短期
        elif alpha_type == 'momentum':
            return [5, 10, 20, 60, 120]  # 中短期动量
        elif alpha_type == 'trend_following':
            return [20, 60, 120, 240]  # 中长期趋势
        elif alpha_type == 'mean_reversion':
            return [1, 5, 10, 20]  # 短期均值回归
        
        # 默认基于字段类别
        if field_category == 'price':
            return [5, 10, 20, 60, 120]
        elif field_category == 'volume':
            return [1, 5, 10, 20, 60]
        elif field_category == 'return':
            return [1, 5, 10, 20]
        else:
            return [5, 20, 60, 120, 240]
    
    def create_smart_alpha_factory(self, fields, strategy='balanced'):
        """创建智能alpha工厂"""
        all_alphas = []
        
        for field in fields:
            category = self.classify_field(field)
            ops_config = self.get_recommended_ops(category, strategy)
            
            # 应用时间序列操作
            for ts_op in ops_config['ts_ops']:
                if ts_op.startswith('ts_'):
                    # 智能选择时间窗口
                    time_windows = self.optimize_time_windows(category, strategy)
                    for window in time_windows:
                        alpha = f"{ts_op}({field}, {window})"
                        decay = self.generate_smart_decay(category, alpha)
                        all_alphas.append((alpha, decay))
            
            # 应用基础操作
            for basic_op in ops_config['basic_ops']:
                if not basic_op.startswith('ts_'):
                    alpha = f"{basic_op}({field})"
                    decay = self.generate_smart_decay(category, alpha)
                    all_alphas.append((alpha, decay))
        
        return all_alphas

# 导出实例
smart_processor = SmartFieldProcessor()
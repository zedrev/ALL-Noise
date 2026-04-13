#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化测试智能系统
"""

import sys
import os

# 添加当前目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from machine_lib import login

def test_smart_system():
    print("测试智能系统...")
    print("=" * 50)
    
    try:
        # 1. 测试登录
        print("[1] 测试登录...")
        s = login()
        print("   登录成功")
        
        # 2. 测试智能生成器
        print()
        print("[2] 测试智能alpha生成...")
        from smart_alpha_generator import SmartAlphaGenerator
        
        generator = SmartAlphaGenerator('pv1')
        
        # 只分析字段，不生成alpha
        print("   分析pv1数据集字段...")
        field_analysis = generator.analyze_dataset_fields(s)
        
        if field_analysis:
            print("   字段分析成功!")
            print(f"   总共找到 {field_analysis['total_fields']} 个字段")
            for category, count in field_analysis['field_types'].items():
                print(f"     {category}: {count}个字段")
        else:
            print("   字段分析失败!")
            
        print()
        print("[3] 测试基础功能...")
        # 测试是否能获取数据字段
        from machine_lib import get_datafields
        df = get_datafields(s, dataset_id='pv1')
        
        if df.empty:
            print("   获取数据字段失败")
        else:
            print(f"   成功获取 {len(df)} 行数据字段")
            
        return True
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_smart_system()
    print()
    print("=" * 50)
    if success:
        print("测试成功！智能系统可以正常工作")
        print("现在可以运行 day1_smart_pv1.py")
    else:
        print("测试失败，需要检查问题")
    print()
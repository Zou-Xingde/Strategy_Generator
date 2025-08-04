#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查XAUUSD D1數據的完整性和時間範圍
確認數據是否從2003到2025完整
"""

import sys
import os
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
import logging
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_data_completeness():
    """檢查數據完整性"""
    print("🔍 檢查XAUUSD D1數據完整性...")
    
    # 連接資料庫
    conn = duckdb.connect(DUCKDB_PATH)
    
    try:
        # 1. 檢查基本統計
        print("\n📊 1. 基本統計...")
        basic_query = """
        SELECT 
            COUNT(*) as total_count,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(DISTINCT DATE(timestamp)) as unique_days
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        """
        
        basic_stats = conn.execute(basic_query).fetchone()
        print(f"   總記錄數: {basic_stats[0]}")
        print(f"   最早日期: {basic_stats[1]}")
        print(f"   最新日期: {basic_stats[2]}")
        print(f"   唯一天數: {basic_stats[3]}")
        
        # 2. 檢查年份分佈
        print("\n📊 2. 年份分佈...")
        year_query = """
        SELECT 
            YEAR(timestamp) as year,
            COUNT(*) as count,
            MIN(timestamp) as year_start,
            MAX(timestamp) as year_end
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        GROUP BY YEAR(timestamp)
        ORDER BY year
        """
        
        year_stats = conn.execute(year_query).fetchdf()
        print(f"   年份分佈:")
        for idx, row in year_stats.iterrows():
            print(f"     {row['year']}: {row['count']} 條 ({row['year_start']} 到 {row['year_end']})")
        
        # 3. 檢查缺失的日期
        print("\n📊 3. 檢查缺失日期...")
        
        # 獲取完整的日期範圍
        start_date = basic_stats[1]
        end_date = basic_stats[2]
        
        print(f"   檢查範圍: {start_date} 到 {end_date}")
        
        # 生成完整的日期序列
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        print(f"   預期天數: {len(date_range)}")
        
        # 獲取實際的日期
        actual_dates_query = """
        SELECT DISTINCT DATE(timestamp) as date
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        ORDER BY date
        """
        
        actual_dates = conn.execute(actual_dates_query).fetchdf()
        actual_date_set = set(actual_dates['date'].dt.date)
        
        print(f"   實際天數: {len(actual_date_set)}")
        
        # 找出缺失的日期
        missing_dates = []
        for date in date_range:
            if date.date() not in actual_date_set:
                missing_dates.append(date.date())
        
        print(f"   缺失天數: {len(missing_dates)}")
        
        if missing_dates:
            print(f"   缺失日期 (前20個):")
            for date in missing_dates[:20]:
                print(f"     {date}")
            
            if len(missing_dates) > 20:
                print(f"     ... 還有 {len(missing_dates) - 20} 個缺失日期")
        
        # 4. 檢查連續性
        print("\n📊 4. 檢查數據連續性...")
        
        # 按時間排序獲取數據
        continuity_query = """
        SELECT timestamp, open, high, low, close
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        ORDER BY timestamp
        """
        
        df = conn.execute(continuity_query).fetchdf()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # 檢查時間間隔
        time_gaps = []
        for i in range(1, len(df)):
            gap = (df.iloc[i]['timestamp'] - df.iloc[i-1]['timestamp']).days
            if gap > 1:  # 超過1天的間隔
                time_gaps.append({
                    'index': i,
                    'gap_days': gap,
                    'from_date': df.iloc[i-1]['timestamp'],
                    'to_date': df.iloc[i]['timestamp']
                })
        
        print(f"   發現 {len(time_gaps)} 個時間間隔")
        
        if time_gaps:
            print(f"   時間間隔詳情 (前10個):")
            for gap in time_gaps[:10]:
                print(f"     間隔 {gap['gap_days']} 天: {gap['from_date']} -> {gap['to_date']}")
        
        # 5. 檢查數據質量
        print("\n📊 5. 檢查數據質量...")
        
        # 檢查價格邏輯
        invalid_prices = []
        for idx, row in df.iterrows():
            if row['high'] < max(row['open'], row['close']) or row['low'] > min(row['open'], row['close']):
                invalid_prices.append({
                    'index': idx,
                    'timestamp': row['timestamp'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close']
                })
        
        print(f"   價格邏輯錯誤: {len(invalid_prices)} 條")
        
        # 檢查極端值
        price_stats = {
            'open': df['open'].describe(),
            'high': df['high'].describe(),
            'low': df['low'].describe(),
            'close': df['close'].describe()
        }
        
        print(f"   價格統計:")
        for col, stats in price_stats.items():
            print(f"     {col}: 最小值={stats['min']:.2f}, 最大值={stats['max']:.2f}, 平均值={stats['mean']:.2f}")
        
        # 6. 檢查最近幾年的數據密度
        print("\n📊 6. 檢查最近幾年數據密度...")
        
        recent_years = [2020, 2021, 2022, 2023, 2024, 2025]
        for year in recent_years:
            year_query = f"""
            SELECT COUNT(*) as count
            FROM candlestick_data_new 
            WHERE symbol = 'XAUUSD' 
            AND timeframe = 'D1'
            AND YEAR(timestamp) = {year}
            """
            
            year_count = conn.execute(year_query).fetchone()[0]
            print(f"     {year}年: {year_count} 條數據")
        
        # 7. 檢查是否有重複數據
        print("\n📊 7. 檢查重複數據...")
        
        duplicate_query = """
        SELECT timestamp, COUNT(*) as count
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        GROUP BY timestamp
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        """
        
        duplicates = conn.execute(duplicate_query).fetchdf()
        print(f"   重複時間戳: {len(duplicates)} 個")
        
        if not duplicates.empty:
            print(f"   重複詳情 (前10個):")
            for idx, row in duplicates.head(10).iterrows():
                print(f"     {row['timestamp']}: {row['count']} 次")
        
    except Exception as e:
        print(f"❌ 檢查過程中發生錯誤: {e}")
        logger.error(f"檢查錯誤: {e}")
    finally:
        conn.close()

def check_swing_data_completeness():
    """檢查波段數據完整性"""
    print("\n🔍 檢查波段數據完整性...")
    
    conn = duckdb.connect(DUCKDB_PATH)
    
    try:
        # 檢查波段數據的時間範圍
        swing_query = """
        SELECT 
            COUNT(*) as total_count,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(DISTINCT DATE(timestamp)) as unique_days
        FROM swing_data 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        """
        
        swing_stats = conn.execute(swing_query).fetchone()
        print(f"   波段數據總數: {swing_stats[0]}")
        print(f"   波段時間範圍: {swing_stats[1]} 到 {swing_stats[2]}")
        print(f"   波段天數: {swing_stats[3]}")
        
        # 檢查波段數據的年份分佈
        swing_year_query = """
        SELECT 
            YEAR(timestamp) as year,
            COUNT(*) as count
        FROM swing_data 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        GROUP BY YEAR(timestamp)
        ORDER BY year
        """
        
        swing_years = conn.execute(swing_year_query).fetchdf()
        print(f"   波段年份分佈:")
        for idx, row in swing_years.iterrows():
            print(f"     {row['year']}: {row['count']} 個波段")
        
    except Exception as e:
        print(f"❌ 檢查波段數據時發生錯誤: {e}")
    finally:
        conn.close()

def main():
    print("🔍 XAUUSD D1數據完整性檢查工具")
    print("=" * 60)
    
    # 檢查K線數據完整性
    check_data_completeness()
    
    # 檢查波段數據完整性
    check_swing_data_completeness()
    
    print(f"\n📋 檢查完成!")

if __name__ == "__main__":
    main() 
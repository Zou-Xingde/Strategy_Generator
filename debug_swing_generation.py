#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
調試XAUUSD D1波段生成問題
檢查為什麼只生成了3個波段
"""

import sys
import os
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime
import logging
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.algorithms.zigzag import ZigZagAlgorithm
from config.settings import DUCKDB_PATH

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def debug_xauusd_d1_swing_generation():
    """調試XAUUSD D1波段生成"""
    print("🔍 調試XAUUSD D1波段生成問題...")
    
    # 連接資料庫
    conn = duckdb.connect(DUCKDB_PATH)
    
    try:
        # 1. 檢查K線數據總數
        print("\n📊 1. 檢查K線數據...")
        candle_query = """
        SELECT COUNT(*) as total_count,
               MIN(timestamp) as earliest,
               MAX(timestamp) as latest
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        """
        
        candle_stats = conn.execute(candle_query).fetchone()
        print(f"   K線總數: {candle_stats[0]}")
        print(f"   時間範圍: {candle_stats[1]} 到 {candle_stats[2]}")
        
        # 2. 獲取完整的K線數據
        print("\n📊 2. 獲取完整K線數據...")
        data_query = """
        SELECT timestamp, open, high, low, close, volume
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        ORDER BY timestamp
        """
        
        df = conn.execute(data_query).fetchdf()
        print(f"   獲取到 {len(df)} 條K線數據")
        
        # 3. 檢查數據質量
        print("\n📊 3. 檢查數據質量...")
        print(f"   數據範圍: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
        print(f"   價格範圍: {df['low'].min():.2f} - {df['high'].max():.2f}")
        
        # 檢查是否有缺失值
        missing_data = df.isnull().sum()
        print(f"   缺失值統計:")
        for col, count in missing_data.items():
            if count > 0:
                print(f"     {col}: {count}")
        
        # 4. 執行ZigZag算法
        print("\n📊 4. 執行ZigZag算法...")
        algorithm = ZigZagAlgorithm(deviation=3.0, depth=12)
        
        # 轉換時間戳格式
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # 執行算法
        result_df = algorithm.calculate(df)
        
        # 5. 檢查ZigZag結果
        print("\n📊 5. 檢查ZigZag結果...")
        zigzag_mask = result_df['zigzag_price'].notna()
        zigzag_data = result_df[zigzag_mask].copy()
        
        print(f"   找到 {len(zigzag_data)} 個ZigZag點")
        
        if not zigzag_data.empty:
            print(f"   ZigZag點詳情:")
            for idx, row in zigzag_data.iterrows():
                print(f"     {row['timestamp']}: {row['zigzag_type']} @ {row['zigzag_price']:.2f}")
        
        # 6. 檢查算法參數
        print("\n📊 6. 檢查算法參數...")
        print(f"   deviation: {algorithm.deviation}")
        print(f"   depth: {algorithm.depth}")
        
        # 7. 嘗試不同的參數
        print("\n📊 7. 嘗試不同的參數...")
        
        # 測試不同的deviation值
        for deviation in [1.0, 2.0, 3.0, 5.0, 10.0]:
            test_algorithm = ZigZagAlgorithm(deviation=deviation, depth=12)
            test_result = test_algorithm.calculate(df)
            test_zigzag = test_result[test_result['zigzag_price'].notna()]
            print(f"   deviation={deviation}: {len(test_zigzag)} 個點")
        
        # 測試不同的depth值
        for depth in [5, 8, 12, 15, 20]:
            test_algorithm = ZigZagAlgorithm(deviation=3.0, depth=depth)
            test_result = test_algorithm.calculate(df)
            test_zigzag = test_result[test_result['zigzag_price'].notna()]
            print(f"   depth={depth}: {len(test_zigzag)} 個點")
        
        # 8. 檢查現有的波段數據
        print("\n📊 8. 檢查現有波段數據...")
        swing_query = """
        SELECT timestamp, zigzag_price, zigzag_type, zigzag_strength
        FROM swing_data 
        WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        ORDER BY timestamp
        """
        
        existing_swings = conn.execute(swing_query).fetchdf()
        print(f"   現有波段數: {len(existing_swings)}")
        
        if not existing_swings.empty:
            print(f"   現有波段詳情:")
            for idx, row in existing_swings.iterrows():
                print(f"     {row['timestamp']}: {row['zigzag_type']} @ {row['zigzag_price']:.2f}")
        
    except Exception as e:
        print(f"❌ 調試過程中發生錯誤: {e}")
        logger.error(f"調試錯誤: {e}")
    finally:
        conn.close()

def test_small_dataset():
    """測試小數據集"""
    print("\n🔍 測試小數據集...")
    
    # 創建測試數據
    test_data = pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01', periods=100, freq='D'),
        'open': np.random.uniform(1800, 2000, 100),
        'high': np.random.uniform(1800, 2000, 100),
        'low': np.random.uniform(1800, 2000, 100),
        'close': np.random.uniform(1800, 2000, 100),
        'volume': np.random.randint(1000, 10000, 100)
    })
    
    # 確保high >= max(open, close), low <= min(open, close)
    test_data['high'] = test_data[['open', 'close']].max(axis=1) + np.random.uniform(0, 10, 100)
    test_data['low'] = test_data[['open', 'close']].min(axis=1) - np.random.uniform(0, 10, 100)
    
    print(f"   測試數據: {len(test_data)} 條")
    
    # 執行ZigZag算法
    algorithm = ZigZagAlgorithm(deviation=3.0, depth=12)
    result = algorithm.calculate(test_data)
    
    zigzag_points = result[result['zigzag_price'].notna()]
    print(f"   找到 {len(zigzag_points)} 個ZigZag點")
    
    if not zigzag_points.empty:
        print(f"   ZigZag點詳情:")
        for idx, row in zigzag_points.iterrows():
            print(f"     {row['timestamp']}: {row['zigzag_type']} @ {row['zigzag_price']:.2f}")

def main():
    print("🔍 XAUUSD D1波段生成調試工具")
    print("=" * 60)
    
    # 調試XAUUSD D1
    debug_xauusd_d1_swing_generation()
    
    # 測試小數據集
    test_small_dataset()
    
    print(f"\n📋 調試完成!")

if __name__ == "__main__":
    main() 
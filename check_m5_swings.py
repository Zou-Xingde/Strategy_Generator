#!/usr/bin/env python3
"""
專門檢查M5波段數據的腳本
"""

import sys
import os
import pandas as pd
import duckdb
import logging
from datetime import datetime

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database.connection import DuckDBConnection
from config.settings import DUCKDB_PATH

# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_m5_swings():
    """檢查M5波段數據"""
    print("🔍 檢查M5波段數據")
    print("=" * 60)
    
    with DuckDBConnection(str(DUCKDB_PATH)) as db:
        # 檢查M5 K線數據
        print("📊 檢查M5 K線數據:")
        print("-" * 40)
        
        m5_kline_count = db.conn.execute("""
            SELECT COUNT(*) as count
            FROM candlestick_data_new 
            WHERE timeframe = 'M5' AND data_source = 'm5_regenerate'
        """).fetchone()[0]
        
        print(f"M5 K線數據: {m5_kline_count:,} 筆")
        
        if m5_kline_count > 0:
            m5_kline_range = db.conn.execute("""
                SELECT 
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM candlestick_data_new 
                WHERE timeframe = 'M5' AND data_source = 'm5_regenerate'
            """).fetchone()
            
            print(f"時間範圍: {m5_kline_range[0]} 到 {m5_kline_range[1]}")
        else:
            print("❌ 沒有M5 K線數據")
        
        # 檢查M5波段數據
        print("\n📈 檢查M5波段數據:")
        print("-" * 40)
        
        m5_swing_count = db.conn.execute("""
            SELECT COUNT(*) as count
            FROM swing_data 
            WHERE timeframe = 'M5' AND algorithm_name = 'zigzag'
        """).fetchone()[0]
        
        print(f"M5波段數據: {m5_swing_count:,} 筆")
        
        if m5_swing_count > 0:
            m5_swing_range = db.conn.execute("""
                SELECT 
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest,
                    COUNT(DISTINCT DATE(timestamp)) as unique_days
                FROM swing_data 
                WHERE timeframe = 'M5' AND algorithm_name = 'zigzag'
            """).fetchone()
            
            print(f"時間範圍: {m5_swing_range[0]} 到 {m5_swing_range[1]}")
            print(f"涵蓋天數: {m5_swing_range[2]}")
            
            # 檢查每日波段分佈
            daily_swings = db.conn.execute("""
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as swing_count
                FROM swing_data 
                WHERE timeframe = 'M5' AND algorithm_name = 'zigzag'
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
                LIMIT 10
            """).fetchdf()
            
            print(f"\n📅 最近10天的波段分佈:")
            for _, row in daily_swings.iterrows():
                print(f"   {row['date']}: {row['swing_count']} 個波段")
            
            # 顯示樣本數據
            print(f"\n📋 樣本波段數據 (最新10個):")
            sample_swings = db.conn.execute("""
                SELECT 
                    timestamp,
                    zigzag_price,
                    zigzag_type,
                    zigzag_strength,
                    zigzag_swing
                FROM swing_data 
                WHERE timeframe = 'M5' AND algorithm_name = 'zigzag'
                ORDER BY timestamp DESC
                LIMIT 10
            """).fetchall()
            
            for swing in sample_swings:
                print(f"   {swing[0]}: {swing[1]:.2f} ({swing[2]}) - 強度: {swing[3]:.2f}% - 波段#{swing[4]}")
            
            # 檢查波段類型分佈
            swing_types = db.conn.execute("""
                SELECT 
                    zigzag_type,
                    COUNT(*) as count
                FROM swing_data 
                WHERE timeframe = 'M5' AND algorithm_name = 'zigzag'
                GROUP BY zigzag_type
            """).fetchall()
            
            print(f"\n📊 波段類型分佈:")
            for swing_type, count in swing_types:
                print(f"   {swing_type}: {count:,} 個")
            
            # 檢查強度分佈
            strength_stats = db.conn.execute("""
                SELECT 
                    AVG(zigzag_strength) as avg_strength,
                    MIN(zigzag_strength) as min_strength,
                    MAX(zigzag_strength) as max_strength
                FROM swing_data 
                WHERE timeframe = 'M5' AND algorithm_name = 'zigzag'
            """).fetchone()
            
            print(f"\n💪 波段強度統計:")
            print(f"   平均強度: {strength_stats[0]:.3f}%")
            print(f"   最小強度: {strength_stats[1]:.3f}%")
            print(f"   最大強度: {strength_stats[2]:.3f}%")
            
        else:
            print("❌ 沒有M5波段數據")
        
        # 檢查所有時間框架的波段數據
        print(f"\n📊 所有時間框架的波段數據統計:")
        print("-" * 40)
        
        all_swings = db.conn.execute("""
            SELECT 
                timeframe,
                algorithm_name,
                COUNT(*) as count,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM swing_data 
            GROUP BY timeframe, algorithm_name
            ORDER BY timeframe, algorithm_name
        """).fetchall()
        
        for timeframe, algorithm, count, earliest, latest in all_swings:
            print(f"   {timeframe} ({algorithm}): {count:,} 筆")
            if earliest and latest:
                print(f"     時間範圍: {earliest} 到 {latest}")

def main():
    """主函數"""
    check_m5_swings()

if __name__ == "__main__":
    main() 
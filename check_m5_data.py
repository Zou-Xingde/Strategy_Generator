#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
檢查M5時間框架的數據情況
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection
import duckdb
from datetime import datetime

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_m5_data():
    """檢查M5時間框架的數據"""
    print("🔍 檢查M5時間框架數據")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查K線數據中的M5數據
        print("📊 檢查K線數據中的M5數據:")
        print("-" * 60)
        
        kline_query = """
        SELECT 
            symbol,
            COUNT(*) as kline_count,
            MIN(timestamp) as earliest_kline,
            MAX(timestamp) as latest_kline
        FROM candlestick_data 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        kline_m5 = conn.execute(kline_query).fetchdf()
        
        if not kline_m5.empty:
            for _, row in kline_m5.iterrows():
                symbol = row['symbol']
                kline_count = row['kline_count']
                earliest = row['earliest_kline']
                latest = row['latest_kline']
                
                print(f"🔸 {symbol} M5:")
                print(f"   K線數量: {kline_count:,}")
                print(f"   時間範圍: {earliest} 到 {latest}")
                print()
        else:
            print("❌ 沒有找到M5的K線數據")
        
        # 檢查波段數據中的M5數據
        print("📊 檢查波段數據中的M5數據:")
        print("-" * 60)
        
        swing_query = """
        SELECT 
            symbol,
            COUNT(*) as swing_count,
            MIN(timestamp) as earliest_swing,
            MAX(timestamp) as latest_swing
        FROM swing_data 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        swing_m5 = conn.execute(swing_query).fetchdf()
        
        if not swing_m5.empty:
            for _, row in swing_m5.iterrows():
                symbol = row['symbol']
                swing_count = row['swing_count']
                earliest = row['earliest_swing']
                latest = row['latest_swing']
                
                print(f"🔸 {symbol} M5:")
                print(f"   波段數量: {swing_count}")
                print(f"   時間範圍: {earliest} 到 {latest}")
                print()
        else:
            print("❌ 沒有找到M5的波段數據")
        
        # 檢查所有可用的時間框架
        print("📊 所有可用的時間框架:")
        print("-" * 60)
        
        all_timeframes_query = """
        SELECT DISTINCT symbol, timeframe 
        FROM candlestick_data 
        ORDER BY symbol, timeframe
        """
        
        all_timeframes = conn.execute(all_timeframes_query).fetchdf()
        
        current_symbol = None
        for _, row in all_timeframes.iterrows():
            symbol = row['symbol']
            timeframe = row['timeframe']
            
            if symbol != current_symbol:
                if current_symbol is not None:
                    print()
                print(f"🔸 {symbol}:")
                current_symbol = symbol
            
            print(f"   {timeframe}", end=" ")
        
        print("\n")
        
        # 檢查是否有M5數據但沒有對應的波段數據
        print("🔍 檢查M5數據缺失情況:")
        print("-" * 60)
        
        missing_m5_query = """
        SELECT DISTINCT c.symbol
        FROM candlestick_data c
        WHERE c.timeframe = 'M5'
        AND NOT EXISTS (
            SELECT 1 FROM swing_data s 
            WHERE s.symbol = c.symbol AND s.timeframe = 'M5'
        )
        """
        
        missing_m5 = conn.execute(missing_m5_query).fetchdf()
        
        if not missing_m5.empty:
            print("❌ 有K線數據但缺少波段數據的M5品種:")
            for _, row in missing_m5.iterrows():
                print(f"   - {row['symbol']}")
        else:
            print("✅ 所有有M5 K線數據的品種都有對應的波段數據")
        
        # 檢查 M5 波段數據
        print("\n=== 檢查 M5 波段數據 ===")
        
        # 檢查 M5 數據數量
        count_result = conn.execute('''
            SELECT COUNT(*) 
            FROM swing_data 
            WHERE symbol = 'XAUUSD' 
            AND timeframe = 'M5' 
            AND algorithm_name = 'zigzag_fixed'
        ''').fetchall()
        print(f"M5 波段數據總數: {count_result[0][0]}")
        
        # 檢查時間範圍
        time_range = conn.execute('''
            SELECT 
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM swing_data 
            WHERE symbol = 'XAUUSD' 
            AND timeframe = 'M5' 
            AND algorithm_name = 'zigzag_fixed'
        ''').fetchall()
        
        print(f"時間範圍: {time_range[0][0]} 到 {time_range[0][1]}")
        
        # 檢查樣本數據
        print("\n樣本數據 (最新10條):")
        sample_result = conn.execute('''
            SELECT 
                timestamp,
                zigzag_price,
                zigzag_type,
                zigzag_strength
            FROM swing_data 
            WHERE symbol = 'XAUUSD' 
            AND timeframe = 'M5' 
            AND algorithm_name = 'zigzag_fixed'
            ORDER BY timestamp DESC
            LIMIT 10
        ''').fetchall()
        
        for row in sample_result:
            timestamp = row[0]
            if timestamp:
                # 檢查時間戳格式
                if isinstance(timestamp, str):
                    print(f"  字符串時間戳: {timestamp}")
                else:
                    print(f"  時間戳對象: {timestamp} (類型: {type(timestamp)})")
                    print(f"  ISO格式: {timestamp.isoformat()}")
            else:
                print(f"  空時間戳")
            print(f"  價格: {row[1]}, 類型: {row[2]}, 強度: {row[3]}")
            print()
        
    except Exception as e:
        logger.error(f"檢查失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    check_m5_data() 
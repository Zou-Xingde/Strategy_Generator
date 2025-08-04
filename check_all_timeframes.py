#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
檢查所有時間框架的波段數據情況
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_all_timeframes():
    """檢查所有時間框架的波段數據"""
    print("🔍 檢查所有時間框架的波段數據")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 查詢所有時間框架的波段數據統計
        query = """
        SELECT 
            symbol,
            timeframe,
            COUNT(*) as swing_count,
            MIN(timestamp) as earliest_swing,
            MAX(timestamp) as latest_swing,
            COUNT(CASE WHEN zigzag_type = 'high' THEN 1 END) as high_count,
            COUNT(CASE WHEN zigzag_type = 'low' THEN 1 END) as low_count
        FROM swing_data 
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe
        """
        
        swing_stats = pd.read_sql_query(query, conn)
        
        if swing_stats.empty:
            print("❌ 沒有找到任何波段數據")
            return
        
        print("📊 各時間框架波段數據統計:")
        print("-" * 60)
        
        for _, row in swing_stats.iterrows():
            symbol = row['symbol']
            timeframe = row['timeframe']
            swing_count = row['swing_count']
            earliest = row['earliest_swing']
            latest = row['latest_swing']
            high_count = row['high_count']
            low_count = row['low_count']
            
            print(f"🔸 {symbol} {timeframe}:")
            print(f"   波段總數: {swing_count}")
            print(f"   高點波段: {high_count}")
            print(f"   低點波段: {low_count}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            
            # 檢查是否有數據缺失
            if swing_count < 10:
                print(f"   ⚠️  波段數量偏少")
            elif latest.year < 2020:
                print(f"   ⚠️  數據較舊，最新數據到 {latest.year}")
            else:
                print(f"   ✅ 數據正常")
            print()
        
        # 檢查缺失的時間框架
        print("🔍 檢查缺失的時間框架...")
        print("-" * 60)
        
        # 查詢所有可用的K線數據時間框架
        kline_query = """
        SELECT DISTINCT symbol, timeframe 
        FROM candlestick_data 
        ORDER BY symbol, timeframe
        """
        
        kline_timeframes = pd.read_sql_query(kline_query, conn)
        
        # 查詢所有波段數據時間框架
        swing_timeframes = pd.read_sql_query("""
        SELECT DISTINCT symbol, timeframe 
        FROM swing_data 
        ORDER BY symbol, timeframe
        """, conn)
        
        # 找出缺失的時間框架
        missing_timeframes = []
        for _, kline_row in kline_timeframes.iterrows():
            symbol = kline_row['symbol']
            timeframe = kline_row['timeframe']
            
            # 檢查是否有對應的波段數據
            has_swing_data = swing_timeframes[
                (swing_timeframes['symbol'] == symbol) & 
                (swing_timeframes['timeframe'] == timeframe)
            ].shape[0] > 0
            
            if not has_swing_data:
                missing_timeframes.append((symbol, timeframe))
        
        if missing_timeframes:
            print("❌ 缺失的時間框架:")
            for symbol, timeframe in missing_timeframes:
                print(f"   - {symbol} {timeframe}")
        else:
            print("✅ 所有時間框架都有波段數據")
        
        # 檢查K線數據量
        print("\n📈 K線數據量檢查:")
        print("-" * 60)
        
        kline_count_query = """
        SELECT 
            symbol,
            timeframe,
            COUNT(*) as kline_count,
            MIN(timestamp) as earliest_kline,
            MAX(timestamp) as latest_kline
        FROM candlestick_data 
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe
        """
        
        kline_stats = pd.read_sql_query(kline_count_query, conn)
        
        for _, row in kline_stats.iterrows():
            symbol = row['symbol']
            timeframe = row['timeframe']
            kline_count = row['kline_count']
            earliest = row['earliest_kline']
            latest = row['latest_kline']
            
            print(f"🔸 {symbol} {timeframe}:")
            print(f"   K線數量: {kline_count:,}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            
            # 檢查對應的波段數據
            swing_row = swing_stats[
                (swing_stats['symbol'] == symbol) & 
                (swing_stats['timeframe'] == timeframe)
            ]
            
            if not swing_row.empty:
                swing_count = swing_row.iloc[0]['swing_count']
                ratio = swing_count / kline_count * 100
                print(f"   波段數量: {swing_count}")
                print(f"   波段比例: {ratio:.2f}%")
                
                if ratio < 1:
                    print(f"   ⚠️  波段比例偏低")
                elif ratio > 10:
                    print(f"   ⚠️  波段比例偏高")
                else:
                    print(f"   ✅ 波段比例正常")
            else:
                print(f"   ❌ 無波段數據")
            print()
        
    except Exception as e:
        logger.error(f"檢查失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    check_all_timeframes() 
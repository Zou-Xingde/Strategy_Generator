#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
詳細檢查M5數據的情況
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_m5_details():
    """詳細檢查M5數據的情況"""
    print("🔍 詳細檢查M5數據情況")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查candlestick_data_new中的M5數據
        print("📊 candlestick_data_new中的M5數據:")
        print("-" * 60)
        
        kline_query = """
        SELECT 
            symbol,
            COUNT(*) as kline_count,
            MIN(timestamp) as earliest_kline,
            MAX(timestamp) as latest_kline
        FROM candlestick_data_new 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        kline_m5 = conn.execute(kline_query).fetchdf()
        
        for _, row in kline_m5.iterrows():
            symbol = row['symbol']
            kline_count = row['kline_count']
            earliest = row['earliest_kline']
            latest = row['latest_kline']
            
            print(f"🔸 {symbol} M5:")
            print(f"   K線數量: {kline_count:,}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            print()
        
        # 檢查swing_data中的M5數據
        print("📊 swing_data中的M5數據:")
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
        
        for _, row in swing_m5.iterrows():
            symbol = row['symbol']
            swing_count = row['swing_count']
            earliest = row['earliest_swing']
            latest = row['latest_swing']
            
            print(f"🔸 {symbol} M5:")
            print(f"   波段數量: {swing_count}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            print()
        
        # 檢查備份表中的M5數據
        print("📊 swing_data_backup中的M5數據:")
        print("-" * 60)
        
        backup_query = """
        SELECT 
            symbol,
            COUNT(*) as swing_count,
            MIN(timestamp) as earliest_swing,
            MAX(timestamp) as latest_swing
        FROM swing_data_backup_1754310077 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        backup_m5 = conn.execute(backup_query).fetchdf()
        
        for _, row in backup_m5.iterrows():
            symbol = row['symbol']
            swing_count = row['swing_count']
            earliest = row['earliest_swing']
            latest = row['latest_swing']
            
            print(f"🔸 {symbol} M5:")
            print(f"   波段數量: {swing_count}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            print()
        
        # 檢查缺失的M5波段數據
        print("🔍 檢查缺失的M5波段數據:")
        print("-" * 60)
        
        missing_query = """
        SELECT DISTINCT c.symbol
        FROM candlestick_data_new c
        WHERE c.timeframe = 'M5'
        AND NOT EXISTS (
            SELECT 1 FROM swing_data s 
            WHERE s.symbol = c.symbol AND s.timeframe = 'M5'
        )
        """
        
        missing_m5 = conn.execute(missing_query).fetchdf()
        
        if not missing_m5.empty:
            print("❌ 有K線數據但缺少波段數據的M5品種:")
            for _, row in missing_m5.iterrows():
                print(f"   - {row['symbol']}")
        else:
            print("✅ 所有有M5 K線數據的品種都有對應的波段數據")
        
        # 檢查XAUUSD M5的具體情況
        print("\n🔍 檢查XAUUSD M5的具體情況:")
        print("-" * 60)
        
        xauusd_kline_query = """
        SELECT COUNT(*) as count
        FROM candlestick_data_new 
        WHERE symbol = 'XAUUSD' AND timeframe = 'M5'
        """
        
        xauusd_kline_count = conn.execute(xauusd_kline_query).fetchdf()
        kline_count = xauusd_kline_count.iloc[0]['count']
        
        xauusd_swing_query = """
        SELECT COUNT(*) as count
        FROM swing_data 
        WHERE symbol = 'XAUUSD' AND timeframe = 'M5'
        """
        
        xauusd_swing_count = conn.execute(xauusd_swing_query).fetchdf()
        swing_count = xauusd_swing_count.iloc[0]['count']
        
        print(f"🔸 XAUUSD M5:")
        print(f"   K線數量: {kline_count:,}")
        print(f"   波段數量: {swing_count}")
        
        if kline_count > 0 and swing_count == 0:
            print(f"   ❌ 有K線數據但沒有波段數據")
        elif kline_count > 0 and swing_count > 0:
            ratio = swing_count / kline_count * 100
            print(f"   ⚠️  波段比例: {ratio:.4f}%")
            if ratio < 0.01:
                print(f"   ⚠️  波段比例過低")
        else:
            print(f"   ✅ 數據正常")
        
    except Exception as e:
        logger.error(f"檢查失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    check_m5_details() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查波段數據完整性
特別關注非M5時間週期的數據狀況
"""

import sys
import os
import duckdb
from datetime import datetime

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH

def check_swing_completeness():
    """檢查波段數據完整性"""
    print("=== 檢查波段數據完整性 ===")
    
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        
        # 檢查總體狀況
        print("\n1. 總體統計:")
        total_count = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()[0]
        print(f"   swing_data總筆數: {total_count}")
        
        # 按品種和時間週期統計
        print("\n2. 按品種和時間週期統計:")
        stats = conn.execute("""
            SELECT symbol, timeframe, COUNT(*) as count,
                   MIN(timestamp) as min_time, MAX(timestamp) as max_time
            FROM swing_data 
            GROUP BY symbol, timeframe 
            ORDER BY symbol, timeframe
        """).fetchall()
        
        for symbol, timeframe, count, min_time, max_time in stats:
            print(f"   {symbol} {timeframe}: {count} 筆")
            if min_time and max_time:
                print(f"       時間範圍: {min_time} 到 {max_time}")
        
        # 檢查K線數據對比
        print("\n3. K線數據對比:")
        kline_stats = conn.execute("""
            SELECT symbol, timeframe, COUNT(*) as count,
                   MIN(timestamp) as min_time, MAX(timestamp) as max_time
            FROM candlestick_data_new 
            GROUP BY symbol, timeframe 
            ORDER BY symbol, timeframe
        """).fetchall()
        
        for symbol, timeframe, count, min_time, max_time in kline_stats:
            print(f"   {symbol} {timeframe}: {count} 筆K線")
            if min_time and max_time:
                print(f"       時間範圍: {min_time} 到 {max_time}")
        
        # 檢查缺失的波段數據
        print("\n4. 缺失的波段數據:")
        missing_swings = conn.execute("""
            SELECT k.symbol, k.timeframe, k.count as kline_count, 
                   COALESCE(s.count, 0) as swing_count,
                   k.count - COALESCE(s.count, 0) as missing_count
            FROM (
                SELECT symbol, timeframe, COUNT(*) as count
                FROM candlestick_data_new 
                GROUP BY symbol, timeframe
            ) k
            LEFT JOIN (
                SELECT symbol, timeframe, COUNT(*) as count
                FROM swing_data 
                GROUP BY symbol, timeframe
            ) s ON k.symbol = s.symbol AND k.timeframe = s.timeframe
            WHERE COALESCE(s.count, 0) = 0 OR s.count IS NULL
            ORDER BY k.symbol, k.timeframe
        """).fetchall()
        
        if missing_swings:
            for symbol, timeframe, kline_count, swing_count, missing_count in missing_swings:
                print(f"   {symbol} {timeframe}: 有 {kline_count} 筆K線，但只有 {swing_count} 筆波段")
        else:
            print("   所有有K線數據的品種和時間週期都有對應的波段數據")
        
        # 檢查波段數據的版本
        print("\n5. 波段數據版本統計:")
        version_stats = conn.execute("""
            SELECT version_hash, COUNT(*) as count
            FROM swing_data 
            GROUP BY version_hash 
            ORDER BY count DESC
        """).fetchall()
        
        for version, count in version_stats:
            print(f"   版本 {version}: {count} 筆")
        
        # 檢查最近的波段數據
        print("\n6. 最近的波段數據:")
        recent_swings = conn.execute("""
            SELECT symbol, timeframe, timestamp, zigzag_type, zigzag_price
            FROM swing_data 
            ORDER BY timestamp DESC 
            LIMIT 10
        """).fetchall()
        
        for symbol, timeframe, timestamp, zigzag_type, zigzag_price in recent_swings:
            print(f"   {symbol} {timeframe}: {timestamp} {zigzag_type} {zigzag_price}")
        
        # 檢查最早的波段數據
        print("\n7. 最早的波段數據:")
        earliest_swings = conn.execute("""
            SELECT symbol, timeframe, timestamp, zigzag_type, zigzag_price
            FROM swing_data 
            ORDER BY timestamp ASC 
            LIMIT 10
        """).fetchall()
        
        for symbol, timeframe, timestamp, zigzag_type, zigzag_price in earliest_swings:
            print(f"   {symbol} {timeframe}: {timestamp} {zigzag_type} {zigzag_price}")
        
        conn.close()
        
    except Exception as e:
        print(f"檢查失敗: {e}")
        return False
    
    return True

if __name__ == "__main__":
    check_swing_completeness() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查資料庫結構
了解swing_data表的實際欄位
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def check_swing_table_structure():
    """檢查swing_data表的結構"""
    print("🔍 檢查swing_data表結構...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 查看表結構
            structure_query = """
            DESCRIBE swing_data
            """
            
            structure = conn.execute(structure_query).fetchall()
            
            print(f"📋 swing_data表結構:")
            for col in structure:
                print(f"   {col[0]}: {col[1]}")
            
            # 查看樣本數據
            sample_query = """
            SELECT * FROM swing_data LIMIT 5
            """
            
            sample_data = conn.execute(sample_query).fetchall()
            
            print(f"\n📊 樣本數據 (前5條):")
            for row in sample_data:
                print(f"   {row}")
            
            # 統計總數
            count_query = """
            SELECT COUNT(*) FROM swing_data
            """
            
            total_count = conn.execute(count_query).fetchone()[0]
            print(f"\n📈 總記錄數: {total_count}")
            
            # 按symbol統計
            symbol_query = """
            SELECT symbol, COUNT(*) as count 
            FROM swing_data 
            GROUP BY symbol 
            ORDER BY count DESC
            """
            
            symbol_stats = conn.execute(symbol_query).fetchall()
            
            print(f"\n📊 按品種統計:")
            for symbol, count in symbol_stats:
                print(f"   {symbol}: {count} 條")
            
            # 按timeframe統計
            timeframe_query = """
            SELECT timeframe, COUNT(*) as count 
            FROM swing_data 
            GROUP BY timeframe 
            ORDER BY count DESC
            """
            
            timeframe_stats = conn.execute(timeframe_query).fetchall()
            
            print(f"\n📊 按時間週期統計:")
            for timeframe, count in timeframe_stats:
                print(f"   {timeframe}: {count} 條")
            
            return structure
            
    except Exception as e:
        print(f"❌ 檢查表結構失敗: {e}")
        return None

def check_swing_data_content(symbol='XAUUSD', timeframe='D1'):
    """檢查特定symbol和timeframe的波段數據內容"""
    print(f"\n🔍 檢查 {symbol} {timeframe} 波段數據內容...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 查詢數據
            data_query = f"""
            SELECT * FROM swing_data 
            WHERE symbol = '{symbol}' 
            AND timeframe = '{timeframe}'
            ORDER BY timestamp DESC
            LIMIT 10
            """
            
            data = conn.execute(data_query).fetchall()
            
            if not data:
                print(f"❌ 沒有找到 {symbol} {timeframe} 的波段數據")
                return
            
            print(f"📋 最新10條波段數據:")
            for row in data:
                print(f"   {row}")
            
            # 統計
            count_query = f"""
            SELECT COUNT(*) FROM swing_data 
            WHERE symbol = '{symbol}' 
            AND timeframe = '{timeframe}'
            """
            
            count = conn.execute(count_query).fetchone()[0]
            print(f"\n📈 {symbol} {timeframe} 總波段數: {count}")
            
            # 檢查時間範圍
            time_range_query = f"""
            SELECT 
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM swing_data 
            WHERE symbol = '{symbol}' 
            AND timeframe = '{timeframe}'
            """
            
            time_range = conn.execute(time_range_query).fetchone()
            if time_range:
                print(f"📅 時間範圍: {time_range[0]} 到 {time_range[1]}")
            
    except Exception as e:
        print(f"❌ 檢查數據內容失敗: {e}")

def main():
    print("🔍 資料庫結構檢查工具")
    print("=" * 60)
    
    # 檢查表結構
    structure = check_swing_table_structure()
    
    # 檢查具體數據
    check_swing_data_content('XAUUSD', 'D1')
    
    print(f"\n📋 檢查完成!")

if __name__ == "__main__":
    main() 
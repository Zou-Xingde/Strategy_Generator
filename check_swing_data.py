#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查資料庫中的波段數據
分析數據量和完整性
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import duckdb

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def check_swing_data():
    try:
        conn = duckdb.connect('database/market_data.duckdb')
        
        print("=== 檢查波段數據庫 ===")
        
        # 檢查表結構
        print("\n1. Swing 表結構:")
        result = conn.execute('DESCRIBE swing_data').fetchall()
        for row in result:
            print(f"  {row}")
        
        # 檢查數據數量
        print("\n2. 數據統計:")
        count_result = conn.execute('SELECT COUNT(*) FROM swing_data').fetchall()
        print(f"  總波段數據: {count_result[0][0]}")
        
        # 檢查 XAUUSD D1 zigzag 數據
        print("\n3. XAUUSD D1 ZigZag 數據:")
        xauusd_result = conn.execute('''
            SELECT COUNT(*) 
            FROM swing_data 
            WHERE symbol = 'XAUUSD' 
            AND timeframe = 'D1' 
            AND algorithm_name = 'zigzag'
        ''').fetchall()
        print(f"  XAUUSD D1 ZigZag 數據量: {xauusd_result[0][0]}")
        
        # 檢查所有品種和時間週期
        print("\n4. 所有品種和時間週期:")
        symbols_result = conn.execute('''
            SELECT symbol, timeframe, algorithm_name, COUNT(*) as count
            FROM swing_data 
            GROUP BY symbol, timeframe, algorithm_name
            ORDER BY symbol, timeframe, algorithm_name
        ''').fetchall()
        
        for row in symbols_result:
            print(f"  {row[0]} {row[1]} {row[2]}: {row[3]} 條")
        
        # 檢查是否有數據
        if xauusd_result[0][0] > 0:
            print("\n5. 樣本數據:")
            sample_result = conn.execute('''
                SELECT * 
                FROM swing_data 
                WHERE symbol = 'XAUUSD' 
                AND timeframe = 'D1' 
                AND algorithm_name = 'zigzag'
                LIMIT 5
            ''').fetchall()
            
            for row in sample_result:
                print(f"  {row}")
        
        conn.close()
        
    except Exception as e:
        print(f"錯誤: {e}")

if __name__ == "__main__":
    check_swing_data() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查D1資料庫資料
"""

import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def check_d1_data():
    """檢查D1資料"""
    
    print("🔍 檢查D1資料庫資料...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            symbol = "EXUSA30IDXUSD"
            timeframe = "D1"
            
            # 獲取D1資料
            df = db.get_candlestick_data(symbol, timeframe)
            
            if len(df) > 0:
                print(f"✅ 成功獲取 {len(df)} 筆D1資料")
                print(f"📊 資料欄位: {list(df.columns)}")
                
                # 檢查時間戳欄位名稱
                time_col = None
                for col in ['timestamp', 'Timestamp', 'time', 'Time']:
                    if col in df.columns:
                        time_col = col
                        break
                
                if time_col:
                    print(f"📅 資料範圍: {df[time_col].min()} 到 {df[time_col].max()}")
                
                print(f"📈 最高價: {df['high'].max():.2f}")
                print(f"📉 最低價: {df['low'].min():.2f}")
                print(f"📊 平均成交量: {df['volume'].mean():,.0f}")
                
                print("\n📋 前5筆資料:")
                print(df.head())
                
                print("\n📋 後5筆資料:")
                print(df.tail())
                
            else:
                print("⚠️ 沒有找到D1資料")
                
    except Exception as e:
        print(f"❌ 檢查失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_d1_data() 
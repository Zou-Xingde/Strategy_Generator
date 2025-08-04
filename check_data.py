#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查數據庫中的數據狀態
"""

import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def check_data():
    """檢查數據庫中的數據"""
    
    print("🔍 檢查數據庫狀態...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            # 檢查所有時間週期的數據量
            print("\n📊 各時間週期數據量:")
            print("-" * 50)
            
            timeframes = ["M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN"]
            
            for tf in timeframes:
                try:
                    result = db.conn.execute(
                        "SELECT COUNT(*) as count FROM candlestick_data WHERE timeframe = ?", 
                        [tf]
                    ).fetchone()
                    
                    count = result[0] if result else 0
                    print(f"{tf:>4}: {count:>8,} 筆")
                    
                except Exception as e:
                    print(f"{tf:>4}: 查詢失敗 - {e}")
            
            # 檢查總數據量
            print("-" * 50)
            total_result = db.conn.execute("SELECT COUNT(*) as total FROM candlestick_data").fetchone()
            total_count = total_result[0] if total_result else 0
            print(f"總計: {total_count:>8,} 筆")
            
            # 檢查數據時間範圍
            print("\n📅 數據時間範圍:")
            print("-" * 50)
            
            time_range = db.conn.execute("""
                SELECT 
                    MIN(timestamp) as start_time,
                    MAX(timestamp) as end_time
                FROM candlestick_data
            """).fetchone()
            
            if time_range and time_range[0]:
                print(f"開始時間: {time_range[0]}")
                print(f"結束時間: {time_range[1]}")
            else:
                print("暫無數據")
            
            # 檢查數據庫大小
            print("\n💾 數據庫大小:")
            print("-" * 50)
            
            import os
            db_size = os.path.getsize(str(DUCKDB_PATH))
            print(f"數據庫文件大小: {db_size / (1024*1024):.2f} MB")
            
    except Exception as e:
        print(f"❌ 檢查數據庫時發生錯誤: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_data() 
#!/usr/bin/env python3
"""快速檢查資料庫內容"""

import sys
from pathlib import Path

# 添加專案根目錄到路徑
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from src.database.connection import DuckDBConnection
from config.settings import DUCKDB_PATH

def main():
    print("檢查資料庫內容...")
    print(f"資料庫路徑: {DUCKDB_PATH}")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            # 檢查candlestick_data表
            result = db.conn.execute("""
                SELECT symbol, timeframe, COUNT(*) as count, 
                       MIN(timestamp) as first_date, MAX(timestamp) as last_date
                FROM candlestick_data 
                GROUP BY symbol, timeframe 
                ORDER BY symbol, timeframe
            """).fetchall()
            
            print("\n=== Candlestick Data ===")
            if result:
                for row in result:
                    print(f"{row[0]} {row[1]}: {row[2]} 筆資料, {row[3]} 到 {row[4]}")
            else:
                print("無蠟燭圖資料")
            
            # 檢查swing_data表
            result2 = db.conn.execute("""
                SELECT symbol, timeframe, algorithm_name, COUNT(*) as count
                FROM swing_data 
                GROUP BY symbol, timeframe, algorithm_name 
                ORDER BY symbol, timeframe, algorithm_name
            """).fetchall()
            
            print("\n=== Swing Data ===")
            if result2:
                for row in result2:
                    print(f"{row[0]} {row[1]} {row[2]}: {row[3]} 筆波段資料")
            else:
                print("無波段資料")
                
    except Exception as e:
        print(f"錯誤: {e}")

if __name__ == "__main__":
    main()

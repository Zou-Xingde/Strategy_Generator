#!/usr/bin/env python3
"""快速检查数据库中的数据"""

from src.database.connection import DuckDBConnection
from pathlib import Path

def check_available_data():
    db_path = Path("database/market_data.duckdb")
    db = DuckDBConnection(str(db_path))
    
    print("=== 检查可用的表格 ===")
    tables = db.conn.execute("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  {table[0]}")
    
    print("\n=== 检查 candlestick_data 表的数据概览 ===")
    try:
        result = db.conn.execute("""
            SELECT symbol, timeframe, 
                   MIN(timestamp) as min_date, 
                   MAX(timestamp) as max_date, 
                   COUNT(*) as count 
            FROM candlestick_data 
            GROUP BY symbol, timeframe 
            ORDER BY symbol, timeframe
        """).fetchall()
        
        print("Symbol\tTimeframe\tMin Date\t\tMax Date\t\tCount")
        print("-" * 70)
        for row in result:
            print(f"{row[0]}\t{row[1]}\t\t{row[2]}\t{row[3]}\t{row[4]}")
            
    except Exception as e:
        print(f"Error accessing candlestick_data: {e}")
    
    print("\n=== 检查 EURUSD 的具体数据 ===")
    try:
        result = db.conn.execute("""
            SELECT timeframe, 
                   MIN(timestamp) as min_date, 
                   MAX(timestamp) as max_date, 
                   COUNT(*) as count 
            FROM candlestick_data 
            WHERE symbol = 'EURUSD'
            GROUP BY timeframe
            ORDER BY timeframe
        """).fetchall()
        
        print("Timeframe\tMin Date\t\tMax Date\t\tCount")
        print("-" * 60)
        for row in result:
            print(f"{row[0]}\t\t{row[1]}\t{row[2]}\t{row[3]}")
            
    except Exception as e:
        print(f"Error accessing EURUSD data: {e}")
    
    db.close()

if __name__ == "__main__":
    check_available_data()

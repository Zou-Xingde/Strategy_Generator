#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
資料庫架構檢查腳本
查看資料庫的表結構、索引、資料統計等詳細資訊
"""

import sys
import os
import pandas as pd
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def check_database_schema():
    """檢查資料庫架構"""
    
    print("🔍 資料庫架構檢查開始...")
    print(f"📁 資料庫路徑: {DUCKDB_PATH}")
    print("=" * 80)
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 1. 查看所有表
            print("📋 資料庫表列表:")
            print("-" * 40)
            tables = conn.execute("SHOW TABLES").fetchall()
            for table in tables:
                print(f"  • {table[0]}")
            print()
            
            # 2. 查看表結構
            for table in tables:
                table_name = table[0]
                print(f"📊 表結構: {table_name}")
                print("-" * 40)
                
                # 查看欄位資訊
                columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                print("欄位資訊:")
                for col in columns:
                    print(f"  • {col[0]}: {col[1]} ({col[2]})")
                print()
                
                # 查看資料統計
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"資料筆數: {count:,}")
                
                if count > 0:
                    # 查看範例資料
                    sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                    print("範例資料:")
                    for row in sample:
                        print(f"  • {row}")
                print()
            
            # 3. 查看蠟燭圖資料統計
            print("📈 蠟燭圖資料統計:")
            print("-" * 40)
            
            # 按時間週期統計
            timeframe_stats = conn.execute("""
                SELECT timeframe, COUNT(*) as count, 
                       MIN(timestamp) as min_date, 
                       MAX(timestamp) as max_date
                FROM candlestick_data 
                GROUP BY timeframe 
                ORDER BY timeframe
            """).fetchall()
            
            for tf in timeframe_stats:
                print(f"  {tf[0]}: {tf[1]:,} 筆 ({tf[2]} ~ {tf[3]})")
            print()
            
            # 按交易對統計
            symbol_stats = conn.execute("""
                SELECT symbol, COUNT(*) as count
                FROM candlestick_data 
                GROUP BY symbol 
                ORDER BY symbol
            """).fetchall()
            
            print("交易對統計:")
            for sym in symbol_stats:
                print(f"  {sym[0]}: {sym[1]:,} 筆")
            print()
            
            # 4. 查看Tick資料統計
            print("📊 Tick資料統計:")
            print("-" * 40)
            
            tick_count = conn.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0]
            print(f"Tick資料總筆數: {tick_count:,}")
            
            if tick_count > 0:
                tick_symbols = conn.execute("""
                    SELECT symbol, COUNT(*) as count,
                           MIN(timestamp) as min_date, 
                           MAX(timestamp) as max_date
                    FROM tick_data 
                    GROUP BY symbol 
                    ORDER BY symbol
                """).fetchall()
                
                for sym in tick_symbols:
                    print(f"  {sym[0]}: {sym[1]:,} 筆 ({sym[2]} ~ {sym[3]})")
            print()
            
            # 5. 查看Swing資料統計
            print("📊 Swing資料統計:")
            print("-" * 40)
            
            swing_count = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()[0]
            print(f"Swing資料總筆數: {swing_count:,}")
            
            if swing_count > 0:
                swing_stats = conn.execute("""
                    SELECT symbol, timeframe, COUNT(*) as count
                    FROM swing_data 
                    GROUP BY symbol, timeframe
                    ORDER BY symbol, timeframe
                """).fetchall()
                
                for stat in swing_stats:
                    print(f"  {stat[0]} {stat[1]}: {stat[2]:,} 筆")
            print()
            
            # 6. 查看資料庫大小
            print("💾 資料庫大小資訊:")
            print("-" * 40)
            
            # 獲取資料庫文件大小
            db_path = Path(DUCKDB_PATH)
            if db_path.exists():
                size_mb = db_path.stat().st_size / (1024 * 1024)
                print(f"資料庫文件大小: {size_mb:.2f} MB")
            print()
            
            # 7. 查看最近的資料更新
            print("🕒 最近資料更新:")
            print("-" * 40)
            
            recent_updates = conn.execute("""
                SELECT symbol, timeframe, MAX(created_at) as last_update
                FROM candlestick_data 
                GROUP BY symbol, timeframe
                ORDER BY last_update DESC
                LIMIT 10
            """).fetchall()
            
            for update in recent_updates:
                print(f"  {update[0]} {update[1]}: {update[2]}")
            print()
            
            # 8. 查看備份文件
            print("💾 備份文件:")
            print("-" * 40)
            backup_dir = Path("database")
            backup_files = list(backup_dir.glob("market_data_backup_*.duckdb"))
            for backup in backup_files:
                size_mb = backup.stat().st_size / (1024 * 1024)
                print(f"  {backup.name}: {size_mb:.2f} MB")
            print()
            
    except Exception as e:
        print(f"❌ 檢查失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database_schema() 
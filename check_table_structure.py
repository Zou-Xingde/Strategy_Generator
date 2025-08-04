#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
檢查swing_data表的實際結構
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_table_structure():
    """檢查swing_data表的結構"""
    print("🔍 檢查swing_data表結構")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查swing_data表的結構
        structure_query = "DESCRIBE swing_data"
        structure = conn.execute(structure_query).fetchdf()
        
        print("📋 swing_data表結構:")
        print("-" * 60)
        for _, row in structure.iterrows():
            print(f"欄位: {row['column_name']}, 類型: {row['column_type']}")
        
        print("\n📊 檢查swing_data表數據:")
        print("-" * 60)
        
        # 查看前幾行數據
        sample_query = "SELECT * FROM swing_data LIMIT 5"
        sample_data = conn.execute(sample_query).fetchdf()
        
        if not sample_data.empty:
            print("前5行數據:")
            print(sample_data)
        else:
            print("❌ swing_data表為空")
        
        # 檢查總記錄數
        count_query = "SELECT COUNT(*) as total_count FROM swing_data"
        total_count = conn.execute(count_query).fetchdf()
        print(f"\n總記錄數: {total_count.iloc[0]['total_count']}")
        
        # 檢查不同symbol和timeframe的組合
        if not sample_data.empty:
            distinct_query = """
            SELECT DISTINCT symbol, timeframe 
            FROM swing_data 
            ORDER BY symbol, timeframe
            """
            distinct_combinations = conn.execute(distinct_query).fetchdf()
            
            print(f"\n📈 不同的symbol-timeframe組合:")
            print("-" * 60)
            for _, row in distinct_combinations.iterrows():
                print(f"  {row['symbol']} {row['timeframe']}")
        
    except Exception as e:
        logger.error(f"檢查失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    check_table_structure() 
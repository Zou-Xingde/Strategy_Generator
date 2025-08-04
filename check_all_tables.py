#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
檢查數據庫中的所有表
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_all_tables():
    """檢查數據庫中的所有表"""
    print("🔍 檢查數據庫中的所有表")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 查詢所有表
        tables_query = "SHOW TABLES"
        tables = conn.execute(tables_query).fetchdf()
        
        print("📋 數據庫中的所有表:")
        print("-" * 60)
        for _, row in tables.iterrows():
            table_name = row['name']
            print(f"🔸 {table_name}")
        
        print("\n📊 檢查每個表的數據量:")
        print("-" * 60)
        
        for _, row in tables.iterrows():
            table_name = row['name']
            
            # 檢查表的記錄數
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = conn.execute(count_query).fetchdf()
            record_count = count_result.iloc[0]['count']
            
            print(f"🔸 {table_name}: {record_count:,} 條記錄")
            
            # 如果是candlestick_data或swing_data，檢查時間框架分佈
            if table_name in ['candlestick_data', 'swing_data']:
                timeframe_query = f"""
                SELECT timeframe, COUNT(*) as count
                FROM {table_name}
                GROUP BY timeframe
                ORDER BY timeframe
                """
                timeframes = conn.execute(timeframe_query).fetchdf()
                
                print(f"   時間框架分佈:")
                for _, tf_row in timeframes.iterrows():
                    tf = tf_row['timeframe']
                    tf_count = tf_row['count']
                    print(f"     {tf}: {tf_count:,}")
                print()
        
        # 檢查是否有其他包含M5數據的表
        print("🔍 檢查是否有其他包含M5數據的表:")
        print("-" * 60)
        
        for _, row in tables.iterrows():
            table_name = row['name']
            
            # 檢查表結構是否包含timeframe欄位
            try:
                structure_query = f"DESCRIBE {table_name}"
                structure = conn.execute(structure_query).fetchdf()
                
                has_timeframe = any('timeframe' in col.lower() for col in structure['column_name'])
                
                if has_timeframe:
                    # 檢查是否有M5數據
                    m5_query = f"""
                    SELECT COUNT(*) as count
                    FROM {table_name}
                    WHERE timeframe = 'M5'
                    """
                    m5_result = conn.execute(m5_query).fetchdf()
                    m5_count = m5_result.iloc[0]['count']
                    
                    if m5_count > 0:
                        print(f"🔸 {table_name}: 有 {m5_count:,} 條M5數據")
                    else:
                        print(f"🔸 {table_name}: 無M5數據")
                else:
                    print(f"🔸 {table_name}: 無timeframe欄位")
                    
            except Exception as e:
                print(f"🔸 {table_name}: 檢查失敗 - {e}")
        
    except Exception as e:
        logger.error(f"檢查失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    check_all_tables() 
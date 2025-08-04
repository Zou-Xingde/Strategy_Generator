#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
檢查ID衝突
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_id_conflicts():
    """檢查ID衝突"""
    print("🔍 檢查ID衝突")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查現有數據的ID範圍
        current_ids_query = """
        SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as count
        FROM swing_data
        """
        
        current_ids = conn.execute(current_ids_query).fetchdf()
        print("📊 現有數據ID範圍:")
        print(f"   最小ID: {current_ids.iloc[0]['min_id']}")
        print(f"   最大ID: {current_ids.iloc[0]['max_id']}")
        print(f"   總記錄數: {current_ids.iloc[0]['count']}")
        print()
        
        # 檢查備份表的ID範圍
        backup_ids_query = """
        SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as count
        FROM swing_data_backup_1754310077
        WHERE timeframe = 'M5'
        """
        
        backup_ids = conn.execute(backup_ids_query).fetchdf()
        print("📊 備份表M5數據ID範圍:")
        print(f"   最小ID: {backup_ids.iloc[0]['min_id']}")
        print(f"   最大ID: {backup_ids.iloc[0]['max_id']}")
        print(f"   總記錄數: {backup_ids.iloc[0]['count']}")
        print()
        
        # 檢查ID重疊
        overlap_query = """
        SELECT s.id
        FROM swing_data s
        INNER JOIN swing_data_backup_1754310077 b ON s.id = b.id
        WHERE b.timeframe = 'M5'
        ORDER BY s.id
        """
        
        overlap_ids = conn.execute(overlap_query).fetchdf()
        
        if not overlap_ids.empty:
            print("❌ 發現ID衝突:")
            print(f"   衝突的ID數量: {len(overlap_ids)}")
            print(f"   衝突的ID範圍: {overlap_ids.iloc[0]['id']} 到 {overlap_ids.iloc[-1]['id']}")
            print("   前10個衝突ID:", overlap_ids['id'].head(10).tolist())
        else:
            print("✅ 沒有ID衝突")
        
        # 建議解決方案
        print("\n💡 建議解決方案:")
        print("1. 刪除現有數據中與備份M5數據ID衝突的記錄")
        print("2. 或者為備份數據分配新的ID範圍")
        
        # 檢查衝突的記錄是什麼
        if not overlap_ids.empty:
            conflict_query = """
            SELECT s.id, s.symbol, s.timeframe, s.timestamp
            FROM swing_data s
            WHERE s.id IN (
                SELECT b.id 
                FROM swing_data_backup_1754310077 b 
                WHERE b.timeframe = 'M5'
            )
            ORDER BY s.id
            LIMIT 5
            """
            
            conflict_records = conn.execute(conflict_query).fetchdf()
            print("\n📋 衝突記錄示例:")
            for _, row in conflict_records.iterrows():
                print(f"   ID {row['id']}: {row['symbol']} {row['timeframe']} {row['timestamp']}")
        
    except Exception as e:
        logger.error(f"檢查失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    check_id_conflicts() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
簡單恢復M5波段數據
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def restore_m5_simple():
    """簡單恢復M5波段數據"""
    print("🔍 簡單恢復M5波段數據")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查現有數據的最大ID
        max_id_query = "SELECT MAX(id) as max_id FROM swing_data"
        max_id_result = conn.execute(max_id_query).fetchdf()
        current_max_id = max_id_result.iloc[0]['max_id'] or 0
        print(f"📊 當前最大ID: {current_max_id}")
        
        # 從備份表獲取M5數據
        backup_query = """
        SELECT * FROM swing_data_backup_1754310077 
        WHERE timeframe = 'M5'
        ORDER BY id
        """
        
        backup_data = conn.execute(backup_query).fetchdf()
        
        if backup_data.empty:
            print("❌ 備份表中沒有M5數據")
            return
        
        print(f"📈 從備份表獲取到 {len(backup_data)} 條M5數據")
        
        # 為每條記錄分配新的ID
        new_id_start = current_max_id + 1
        print(f"🆔 新ID範圍: {new_id_start} 到 {new_id_start + len(backup_data) - 1}")
        
        # 逐條插入數據
        inserted_count = 0
        for idx, row in backup_data.iterrows():
            new_id = new_id_start + idx
            
            insert_query = """
            INSERT INTO swing_data (
                id, symbol, timeframe, algorithm_name, version_hash, timestamp,
                zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
                swing_high, swing_low, swing_range, swing_duration, swing_direction
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            try:
                conn.execute(insert_query, [
                    new_id, row['symbol'], row['timeframe'], row['algorithm_name'], 
                    row['version_hash'], row['timestamp'], row['zigzag_price'],
                    row['zigzag_type'], row['zigzag_strength'], row['zigzag_swing'],
                    row['swing_high'], row['swing_low'], row['swing_range'], 
                    row['swing_duration'], row['swing_direction']
                ])
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    print(f"📥 已插入 {inserted_count} 條記錄...")
                    
            except Exception as e:
                print(f"⚠️ 插入記錄 {new_id} 失敗: {e}")
                continue
        
        print(f"✅ 成功插入 {inserted_count} 條M5波段數據")
        
        # 驗證結果
        verify_query = """
        SELECT 
            symbol,
            COUNT(*) as swing_count,
            MIN(timestamp) as earliest_swing,
            MAX(timestamp) as latest_swing
        FROM swing_data 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        verify_result = conn.execute(verify_query).fetchdf()
        
        print("📊 恢復後的M5數據:")
        print("-" * 60)
        
        for _, row in verify_result.iterrows():
            symbol = row['symbol']
            swing_count = row['swing_count']
            earliest = row['earliest_swing']
            latest = row['latest_swing']
            
            print(f"🔸 {symbol} M5:")
            print(f"   波段數量: {swing_count}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            print()
        
    except Exception as e:
        logger.error(f"恢復失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    restore_m5_simple() 
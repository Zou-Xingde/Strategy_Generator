#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
從備份表中恢復M5波段數據
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def restore_m5_from_backup():
    """從備份表中恢復M5波段數據"""
    print("🔍 從備份表中恢復M5波段數據")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查備份表
        backup_tables_query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'swing_data_backup_%'
        ORDER BY name DESC
        """
        
        backup_tables = conn.execute(backup_tables_query).fetchdf()
        
        if backup_tables.empty:
            print("❌ 沒有找到備份表")
            return
        
        # 使用最新的備份表
        latest_backup = backup_tables.iloc[0]['name']
        print(f"📋 使用備份表: {latest_backup}")
        
        # 檢查備份表中的M5數據
        backup_m5_query = f"""
        SELECT 
            symbol,
            COUNT(*) as swing_count,
            MIN(timestamp) as earliest_swing,
            MAX(timestamp) as latest_swing
        FROM {latest_backup} 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        backup_m5 = conn.execute(backup_m5_query).fetchdf()
        
        if backup_m5.empty:
            print("❌ 備份表中沒有M5數據")
            return
        
        print("📊 備份表中的M5數據:")
        print("-" * 60)
        total_backup_swings = 0
        
        for _, row in backup_m5.iterrows():
            symbol = row['symbol']
            swing_count = row['swing_count']
            earliest = row['earliest_swing']
            latest = row['latest_swing']
            
            print(f"🔸 {symbol} M5:")
            print(f"   波段數量: {swing_count}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            total_backup_swings += swing_count
            print()
        
        print(f"📈 備份表中總共有 {total_backup_swings} 個M5波段")
        
        # 清除現有的M5數據
        print("🗑️ 清除現有的M5波段數據...")
        delete_query = "DELETE FROM swing_data WHERE timeframe = 'M5'"
        conn.execute(delete_query)
        
        # 從備份表恢復M5數據（使用新的ID）
        print("📥 從備份表恢復M5波段數據...")
        restore_query = f"""
        INSERT INTO swing_data (
            id, symbol, timeframe, algorithm_name, version_hash, timestamp,
            zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
            swing_high, swing_low, swing_range, swing_duration, swing_direction
        )
        SELECT 
            id, symbol, timeframe, algorithm_name, version_hash, timestamp,
            zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
            swing_high, swing_low, swing_range, swing_duration, swing_direction
        FROM {latest_backup} 
        WHERE timeframe = 'M5'
        """
        
        conn.execute(restore_query)
        
        # 檢查恢復結果
        restored_query = """
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
        
        restored_m5 = conn.execute(restored_query).fetchdf()
        
        print("✅ 恢復完成!")
        print("📊 恢復後的M5數據:")
        print("-" * 60)
        
        total_restored_swings = 0
        for _, row in restored_m5.iterrows():
            symbol = row['symbol']
            swing_count = row['swing_count']
            earliest = row['earliest_swing']
            latest = row['latest_swing']
            
            print(f"🔸 {symbol} M5:")
            print(f"   波段數量: {swing_count}")
            print(f"   時間範圍: {earliest} 到 {latest}")
            total_restored_swings += swing_count
            print()
        
        print(f"📈 成功恢復 {total_restored_swings} 個M5波段")
        
        if total_restored_swings == total_backup_swings:
            print("✅ 所有M5波段數據已成功恢復")
        else:
            print(f"⚠️ 恢復的數據量 ({total_restored_swings}) 與備份數據量 ({total_backup_swings}) 不匹配")
        
    except Exception as e:
        logger.error(f"恢復失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    restore_m5_from_backup() 
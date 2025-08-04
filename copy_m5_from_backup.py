#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接從備份表複製M5數據
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def copy_m5_from_backup():
    """直接從備份表複製M5數據"""
    print("🔍 直接從備份表複製M5數據")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查備份表中的M5數據
        backup_query = """
        SELECT 
            symbol,
            COUNT(*) as swing_count,
            MIN(timestamp) as earliest_swing,
            MAX(timestamp) as latest_swing
        FROM swing_data_backup_1754310077 
        WHERE timeframe = 'M5'
        GROUP BY symbol
        ORDER BY symbol
        """
        
        backup_m5 = conn.execute(backup_query).fetchdf()
        
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
        
        # 直接複製數據
        print("📥 直接複製M5數據...")
        
        # 先刪除現有的M5數據
        delete_query = "DELETE FROM swing_data WHERE timeframe = 'M5'"
        conn.execute(delete_query)
        print("🗑️ 已清除現有M5數據")
        
        # 直接複製所有M5數據
        copy_query = """
        INSERT INTO swing_data 
        SELECT * FROM swing_data_backup_1754310077 
        WHERE timeframe = 'M5'
        """
        
        conn.execute(copy_query)
        print("✅ 已複製M5數據")
        
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
        
        print("📊 複製後的M5數據:")
        print("-" * 60)
        
        total_restored_swings = 0
        for _, row in verify_result.iterrows():
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
        logger.error(f"複製失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    copy_m5_from_backup() 
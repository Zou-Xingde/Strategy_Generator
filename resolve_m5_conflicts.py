#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
解決M5數據的ID衝突
"""

import pandas as pd
import logging
from src.database.connection import DuckDBConnection

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def resolve_m5_conflicts():
    """解決M5數據的ID衝突"""
    print("🔍 解決M5數據的ID衝突")
    print("=" * 60)
    
    db = DuckDBConnection("database/market_data.duckdb")
    conn = db.conn
    
    try:
        # 檢查衝突的記錄
        conflict_query = """
        SELECT s.id, s.symbol, s.timeframe, s.timestamp
        FROM swing_data s
        WHERE s.id IN (
            SELECT b.id 
            FROM swing_data_backup_1754310077 b 
            WHERE b.timeframe = 'M5'
        )
        ORDER BY s.id
        """
        
        conflict_records = conn.execute(conflict_query).fetchdf()
        
        if conflict_records.empty:
            print("✅ 沒有ID衝突，可以直接恢復M5數據")
        else:
            print(f"❌ 發現 {len(conflict_records)} 個ID衝突")
            print("📋 衝突記錄詳情:")
            for _, row in conflict_records.iterrows():
                print(f"   ID {row['id']}: {row['symbol']} {row['timeframe']} {row['timestamp']}")
            
            print(f"\n🗑️ 刪除衝突的記錄...")
            
            # 刪除衝突的記錄
            delete_conflicts_query = """
            DELETE FROM swing_data
            WHERE id IN (
                SELECT b.id 
                FROM swing_data_backup_1754310077 b 
                WHERE b.timeframe = 'M5'
            )
            """
            
            conn.execute(delete_conflicts_query)
            print(f"✅ 已刪除 {len(conflict_records)} 個衝突記錄")
        
        # 現在恢復M5數據
        print("\n📥 恢復M5數據...")
        
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
        
        # 複製M5數據
        copy_query = """
        INSERT INTO swing_data 
        SELECT * FROM swing_data_backup_1754310077 
        WHERE timeframe = 'M5'
        """
        
        conn.execute(copy_query)
        print("✅ 已恢復M5數據")
        
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
        
        print("\n📊 恢復後的M5數據:")
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
        
        # 檢查總體數據狀態
        total_query = """
        SELECT 
            timeframe,
            COUNT(*) as count
        FROM swing_data 
        GROUP BY timeframe
        ORDER BY timeframe
        """
        
        total_result = conn.execute(total_query).fetchdf()
        
        print("\n📊 所有時間框架數據統計:")
        print("-" * 60)
        for _, row in total_result.iterrows():
            timeframe = row['timeframe']
            count = row['count']
            print(f"🔸 {timeframe}: {count} 個波段")
        
    except Exception as e:
        logger.error(f"解決衝突失敗: {e}")
        raise
    finally:
        db.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    resolve_m5_conflicts() 
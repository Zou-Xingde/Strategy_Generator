#!/usr/bin/env python3
"""
清理測試數據腳本 - 刪除所有測試數據，只保留真實市場數據
"""

import sys
from pathlib import Path

# 添加項目根目錄到Python路徑
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DuckDBConnection
from config.settings import DUCKDB_PATH

def cleanup_test_data():
    """清理測試數據"""
    print("🧹 開始清理測試數據...")
    
    with DuckDBConnection(str(DUCKDB_PATH)) as db:
        # 檢查數據分布
        print("📊 檢查數據分布...")
        
        # 總記錄數
        total_count = db.conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()[0]
        print(f"總記錄數: {total_count}")
        
        # 測試數據 (2024-07 到 2025-07)
        test_data_count = db.conn.execute("""
            SELECT COUNT(*) FROM candlestick_data 
            WHERE timestamp >= '2024-07-01' AND timestamp <= '2025-07-31'
        """).fetchone()[0]
        print(f"測試數據 (2024-2025): {test_data_count}")
        
        # 真實數據 (2011-2012)
        real_data_count = db.conn.execute("""
            SELECT COUNT(*) FROM candlestick_data 
            WHERE timestamp >= '2011-01-01' AND timestamp <= '2012-12-31'
        """).fetchone()[0]
        print(f"真實數據 (2011-2012): {real_data_count}")
        
        # 按時間框架統計
        print("\n📈 按時間框架統計:")
        timeframe_stats = db.conn.execute("""
            SELECT timeframe, COUNT(*) as count,
                   MIN(timestamp) as min_date,
                   MAX(timestamp) as max_date
            FROM candlestick_data 
            GROUP BY timeframe 
            ORDER BY timeframe
        """).fetchall()
        
        for tf, count, min_date, max_date in timeframe_stats:
            print(f"  {tf}: {count} 筆 ({min_date} 到 {max_date})")
        
        # 刪除測試數據
        print(f"\n🗑️ 刪除 {test_data_count} 筆測試數據...")
        deleted_count = db.conn.execute("""
            DELETE FROM candlestick_data 
            WHERE timestamp >= '2024-07-01' AND timestamp <= '2025-07-31'
        """).fetchone()[0]
        
        print(f"✅ 已刪除 {deleted_count} 筆測試數據")
        
        # 檢查清理後的數據
        remaining_count = db.conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()[0]
        print(f"📊 清理後剩餘記錄數: {remaining_count}")
        
        # 按時間框架統計清理後的數據
        print("\n📈 清理後按時間框架統計:")
        remaining_stats = db.conn.execute("""
            SELECT timeframe, COUNT(*) as count,
                   MIN(timestamp) as min_date,
                   MAX(timestamp) as max_date
            FROM candlestick_data 
            GROUP BY timeframe 
            ORDER BY timeframe
        """).fetchall()
        
        for tf, count, min_date, max_date in remaining_stats:
            print(f"  {tf}: {count} 筆 ({min_date} 到 {max_date})")
        
        print("\n🎉 測試數據清理完成！")

if __name__ == "__main__":
    cleanup_test_data() 
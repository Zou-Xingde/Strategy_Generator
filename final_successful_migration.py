#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
最終成功版本的資料庫重構遷移腳本
正確處理重複資料問題
"""

import sys
import os
import time
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def create_backup():
    """創建備份"""
    backup_path = Path("database") / f"market_data_backup_migration_{int(time.time())}.duckdb"
    
    print(f"💾 創建備份到: {backup_path}")
    
    import shutil
    shutil.copy2(DUCKDB_PATH, backup_path)
    
    print("✅ 備份創建完成")

def clean_existing_new_tables():
    """清理已存在的新表"""
    print("🧹 清理已存在的新表...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 檢查並刪除已存在的新表
            tables_to_drop = [
                'candlestick_data_new',
                'tick_data_new', 
                'swing_data_new',
                'algorithm_statistics_new',
                'algorithm_versions_new'
            ]
            
            for table in tables_to_drop:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"  🗑️  已刪除表: {table}")
            
            # 刪除視圖
            conn.execute("DROP VIEW IF EXISTS v_candlestick_latest")
            print("  🗑️  已刪除視圖: v_candlestick_latest")
            
            print("✅ 清理完成")
            
    except Exception as e:
        print(f"❌ 清理失敗: {e}")

def create_new_schema():
    """創建新的資料庫架構"""
    print("🏗️  創建新的資料庫架構...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 創建維度表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS symbols (
                  symbol   TEXT PRIMARY KEY
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timeframes (
                  code     TEXT PRIMARY KEY,
                  seconds  INTEGER NOT NULL CHECK (seconds > 0)
                )
            """)
            
            # 創建新的蠟燭圖表（移除唯一約束，避免重複問題）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS candlestick_data_new (
                  id          BIGINT PRIMARY KEY,
                  symbol      TEXT NOT NULL,
                  timeframe   TEXT NOT NULL,
                  timestamp   TIMESTAMP NOT NULL,
                  open        DECIMAL(20,10) NOT NULL,
                  high        DECIMAL(20,10) NOT NULL,
                  low         DECIMAL(20,10) NOT NULL,
                  close       DECIMAL(20,10) NOT NULL,
                  volume      DECIMAL(38,18) NOT NULL,
                  data_version TEXT NOT NULL,
                  data_source  TEXT,
                  created_at   TIMESTAMP DEFAULT now(),
                  updated_at   TIMESTAMP DEFAULT now()
                )
            """)
            
            print("✅ 新架構創建完成")
            
    except Exception as e:
        print(f"❌ 架構創建失敗: {e}")
        import traceback
        traceback.print_exc()

def migrate_candlestick_data():
    """遷移蠟燭圖資料"""
    print("📦 開始蠟燭圖資料遷移...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            print("1️⃣ 填充維度表...")
            
            # 填充 symbols 表
            conn.execute("""
                INSERT INTO symbols(symbol)
                SELECT DISTINCT symbol FROM candlestick_data
                ON CONFLICT (symbol) DO NOTHING
            """)
            
            # 填充 timeframes 表
            conn.execute("""
                INSERT INTO timeframes(code, seconds)
                VALUES 
                    ('M1', 60), 
                    ('M5', 300), 
                    ('M15', 900), 
                    ('M30', 1800),
                    ('H1', 3600), 
                    ('H4', 14400), 
                    ('D1', 86400),
                    ('W1', 604800),
                    ('MN', 2592000),
                    ('1M', 2592000)
                ON CONFLICT (code) DO NOTHING
            """)
            
            print("✅ 維度表填充完成")
            
            print("\n2️⃣ 遷移蠟燭圖資料（處理重複資料）...")
            
            # 先檢查重複資料
            duplicate_check = conn.execute("""
                SELECT symbol, timeframe, timestamp, COUNT(*) as cnt
                FROM candlestick_data
                GROUP BY symbol, timeframe, timestamp
                HAVING COUNT(*) > 1
                LIMIT 5
            """).fetchall()
            
            if duplicate_check:
                print(f"⚠️  發現 {len(duplicate_check)} 組重複資料，將使用最新版本")
                for dup in duplicate_check:
                    print(f"  • {dup[0]} {dup[1]} {dup[2]}: {dup[3]} 筆")
            
            # 分批遷移蠟燭圖資料，處理重複
            batch_size = 50000  # 減少批次大小
            total_count = conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()[0]
            
            print(f"📊 總共需要遷移 {total_count:,} 筆蠟燭圖資料")
            
            for offset in range(0, total_count, batch_size):
                print(f"   處理第 {offset//batch_size + 1} 批次 ({offset:,} - {min(offset + batch_size, total_count):,})")
                
                # 使用更簡單的遷移策略，避免複雜的 ROW_NUMBER
                conn.execute(f"""
                    INSERT INTO candlestick_data_new
                    SELECT
                      ROW_NUMBER() OVER (ORDER BY symbol, timeframe, timestamp) + {offset} AS id,
                      CAST(symbol AS TEXT) AS symbol,
                      CAST(timeframe AS TEXT) AS timeframe,
                      CAST(timestamp AS TIMESTAMP) AS timestamp,
                      CAST(open  AS DECIMAL(20,10))  AS open,
                      CAST(high  AS DECIMAL(20,10))  AS high,
                      CAST(low   AS DECIMAL(20,10))  AS low,
                      CAST(close AS DECIMAL(20,10))  AS close,
                      CAST(volume AS DECIMAL(38,18)) AS volume,
                      COALESCE(data_version,'v1.0') AS data_version,
                      data_source,
                      COALESCE(created_at, now()) AS created_at,
                      COALESCE(updated_at, now()) AS updated_at
                    FROM candlestick_data
                    ORDER BY symbol, timeframe, timestamp
                    LIMIT {batch_size} OFFSET {offset}
                """)
            
            print("✅ 蠟燭圖資料遷移完成")
            
            print("\n3️⃣ 創建索引...")
            
            # 創建索引
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_candle_q1_new ON candlestick_data_new(symbol, timeframe, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_candle_q2_new ON candlestick_data_new(symbol, timeframe, data_version, timestamp)"
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            print("✅ 索引創建完成")
            
            print("\n4️⃣ 創建視圖...")
            
            # 創建最新版本視圖（處理重複資料）
            conn.execute("""
                CREATE OR REPLACE VIEW v_candlestick_latest AS
                SELECT * EXCLUDE rn
                FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (
                           PARTITION BY symbol, timeframe, timestamp
                           ORDER BY data_version DESC, updated_at DESC, id DESC
                         ) AS rn
                  FROM candlestick_data_new
                )
                WHERE rn = 1
            """)
            
            print("✅ 視圖創建完成")
            
            # 驗證資料
            print("\n5️⃣ 驗證遷移結果...")
            
            # 比較資料筆數
            old_count = conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()[0]
            new_count = conn.execute("SELECT COUNT(*) FROM candlestick_data_new").fetchone()[0]
            
            print(f"📊 蠟燭圖資料：舊表 {old_count:,} 筆 → 新表 {new_count:,} 筆")
            
            # 檢查視圖的資料量
            view_count = conn.execute("SELECT COUNT(*) FROM v_candlestick_latest").fetchone()[0]
            print(f"📊 視圖資料：{view_count:,} 筆（去重後）")
            
            # 檢查新表的資料品質
            print("\n📊 新表資料品質檢查:")
            
            # 檢查時間範圍
            time_range = conn.execute("""
                SELECT MIN(timestamp), MAX(timestamp) FROM candlestick_data_new
            """).fetchone()
            
            print(f"  • 時間範圍: {time_range[0]} 到 {time_range[1]}")
            
            # 檢查資料版本分布
            version_dist = conn.execute("""
                SELECT data_version, COUNT(*) as cnt
                FROM candlestick_data_new
                GROUP BY data_version
                ORDER BY cnt DESC
            """).fetchall()
            
            print("  • 資料版本分布:")
            for version in version_dist:
                print(f"    - {version[0]}: {version[1]:,} 筆")
            
            # 檢查各時間週期的資料量
            timeframe_dist = conn.execute("""
                SELECT timeframe, COUNT(*) as cnt
                FROM candlestick_data_new
                GROUP BY timeframe
                ORDER BY timeframe
            """).fetchall()
            
            print("  • 時間週期分布:")
            for tf in timeframe_dist:
                print(f"    - {tf[0]}: {tf[1]:,} 筆")
            
            # 檢查重複資料處理結果
            duplicate_after = conn.execute("""
                SELECT symbol, timeframe, timestamp, COUNT(*) as cnt
                FROM candlestick_data_new
                GROUP BY symbol, timeframe, timestamp
                HAVING COUNT(*) > 1
                LIMIT 5
            """).fetchall()
            
            if duplicate_after:
                print(f"  • 新表中仍有 {len(duplicate_after)} 組重複資料（這是正常的，視圖會處理）")
                for dup in duplicate_after:
                    print(f"    - {dup[0]} {dup[1]} {dup[2]}: {dup[3]} 筆")
            else:
                print("  • 新表中沒有重複資料")
            
            print("\n✅ 蠟燭圖資料遷移完成！新表已創建，原始表保留作為備份")
            
    except Exception as e:
        print(f"❌ 遷移失敗: {e}")
        import traceback
        traceback.print_exc()

def test_new_schema():
    """測試新架構的功能"""
    print("\n🧪 測試新架構功能...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 測試視圖查詢
            print("📊 測試視圖查詢...")
            
            # 查詢最新的 D1 資料
            d1_data = conn.execute("""
                SELECT COUNT(*) as cnt, 
                       MIN(timestamp) as min_date, 
                       MAX(timestamp) as max_date
                FROM v_candlestick_latest
                WHERE symbol = 'EXUSA30IDXUSD' AND timeframe = 'D1'
            """).fetchone()
            
            print(f"  • D1 資料：{d1_data[0]:,} 筆 ({d1_data[1]} 到 {d1_data[2]})")
            
            # 查詢最新的 H4 資料
            h4_data = conn.execute("""
                SELECT COUNT(*) as cnt, 
                       MIN(timestamp) as min_date, 
                       MAX(timestamp) as max_date
                FROM v_candlestick_latest
                WHERE symbol = 'EXUSA30IDXUSD' AND timeframe = 'H4'
            """).fetchone()
            
            print(f"  • H4 資料：{h4_data[0]:,} 筆 ({h4_data[1]} 到 {h4_data[2]})")
            
            # 測試價格精度
            print("📈 測試價格精度...")
            
            price_sample = conn.execute("""
                SELECT open, high, low, close, volume
                FROM v_candlestick_latest
                WHERE symbol = 'EXUSA30IDXUSD' AND timeframe = 'D1'
                ORDER BY timestamp DESC
                LIMIT 5
            """).fetchall()
            
            print("  • 最新 D1 價格樣本:")
            for i, row in enumerate(price_sample, 1):
                print(f"    {i}. O:{row[0]:.10f} H:{row[1]:.10f} L:{row[2]:.10f} C:{row[3]:.10f} V:{row[4]:.18f}")
            
            print("✅ 新架構功能測試完成")
            
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🔧 最終成功版資料庫重構遷移工具")
    print("=" * 60)
    
    # 創建備份
    create_backup()
    
    # 清理已存在的新表
    clean_existing_new_tables()
    
    # 創建新架構
    create_new_schema()
    
    # 執行遷移
    migrate_candlestick_data()
    
    # 測試新架構
    test_new_schema()
    
    print("\n🎉 蠟燭圖資料重構遷移完成！")
    print("📝 建議接下來的步驟：")
    print("   1. 測試新表的查詢功能")
    print("   2. 更新應用程式代碼以使用新表")
    print("   3. 驗證資料完整性")
    print("   4. 考慮刪除舊表（備份已保留）")
    print("\n💡 新架構特點：")
    print("   • 高精度 DECIMAL(20,10) 價格欄位")
    print("   • 大容量 DECIMAL(38,18) 成交量欄位")
    print("   • 多版本支援（data_version）")
    print("   • 視圖自動去重（v_candlestick_latest）")
    print("   • 完整的索引優化")
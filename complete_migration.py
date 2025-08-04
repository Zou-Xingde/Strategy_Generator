#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
完整的資料庫重構遷移腳本
處理重複資料和現有表的問題
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
            
            # 創建演算法版本表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS algorithm_versions_new (
                  id             BIGINT PRIMARY KEY,
                  algorithm_name TEXT NOT NULL,
                  version_hash   TEXT NOT NULL,
                  version_name   TEXT,
                  parameters     JSON NOT NULL,
                  description    TEXT,
                  is_active      BOOLEAN DEFAULT TRUE,
                  created_at     TIMESTAMP DEFAULT now(),
                  created_by     TEXT,
                  UNIQUE (algorithm_name, version_hash)
                )
            """)
            
            # 創建新的蠟燭圖表
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
                  updated_at   TIMESTAMP DEFAULT now(),
                  CHECK (high >= GREATEST(open, close)),
                  CHECK (low  <= LEAST(open, close)),
                  CHECK (volume >= 0),
                  UNIQUE (symbol, timeframe, timestamp, data_version)
                )
            """)
            
            # 創建新的 Tick 表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tick_data_new (
                  id          BIGINT PRIMARY KEY,
                  symbol      TEXT NOT NULL,
                  timestamp   TIMESTAMP NOT NULL,
                  event_type  TEXT NOT NULL CHECK (event_type IN ('quote','trade')),
                  exchange    TEXT,
                  price       DECIMAL(20,10),
                  size        DECIMAL(38,18),
                  bid         DECIMAL(20,10),
                  ask         DECIMAL(20,10),
                  bid_size    DECIMAL(38,18),
                  ask_size    DECIMAL(38,18),
                  data_version TEXT NOT NULL,
                  data_source  TEXT,
                  created_at   TIMESTAMP DEFAULT now(),
                  updated_at   TIMESTAMP DEFAULT now(),
                  CHECK (COALESCE(price, 0)  >= 0),
                  CHECK (COALESCE(size, 0)   >= 0),
                  CHECK (COALESCE(bid, 0)    >= 0),
                  CHECK (COALESCE(ask, 0)    >= 0),
                  CHECK (COALESCE(bid_size,0)>= 0),
                  CHECK (COALESCE(ask_size,0)>= 0),
                  UNIQUE (symbol, timestamp, event_type, COALESCE(exchange,''), data_version)
                )
            """)
            
            # 創建新的 Swing 表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS swing_data_new (
                  id                   BIGINT PRIMARY KEY,
                  symbol               TEXT NOT NULL,
                  timeframe            TEXT NOT NULL,
                  algorithm_version_id BIGINT NOT NULL,
                  timestamp            TIMESTAMP NOT NULL,
                  zigzag_price         DECIMAL(20,10),
                  zigzag_type          TEXT CHECK (zigzag_type IN ('peak','trough')),
                  zigzag_strength      DECIMAL(10,6),
                  zigzag_swing         INTEGER,
                  swing_high           DECIMAL(20,10),
                  swing_low            DECIMAL(20,10),
                  swing_range          DECIMAL(20,10) CHECK (swing_range >= 0),
                  swing_duration       INTEGER CHECK (swing_duration >= 0),
                  swing_direction      TEXT CHECK (swing_direction IN ('up','down')),
                  created_at           TIMESTAMP DEFAULT now(),
                  UNIQUE (symbol, timeframe, algorithm_version_id, timestamp)
                )
            """)
            
            # 創建新的統計表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS algorithm_statistics_new (
                  id                   BIGINT PRIMARY KEY,
                  symbol               TEXT NOT NULL,
                  timeframe            TEXT NOT NULL,
                  algorithm_version_id BIGINT NOT NULL,
                  as_of_date           DATE NOT NULL,
                  total_swings         INTEGER,
                  avg_range            DECIMAL(20,10),
                  avg_duration         INTEGER,
                  min_range            DECIMAL(20,10),
                  max_range            DECIMAL(20,10),
                  created_at           TIMESTAMP DEFAULT now(),
                  UNIQUE(symbol, timeframe, algorithm_version_id, as_of_date)
                )
            """)
            
            print("✅ 新架構創建完成")
            
    except Exception as e:
        print(f"❌ 架構創建失敗: {e}")
        import traceback
        traceback.print_exc()

def migrate_data():
    """執行資料遷移"""
    print("📦 開始資料遷移...")
    
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
            
            print("\n2️⃣ 遷移演算法版本資料...")
            
            # 遷移 algorithm_versions
            conn.execute("""
                INSERT INTO algorithm_versions_new 
                (id, algorithm_name, version_hash, version_name, parameters, description, is_active, created_at, created_by)
                SELECT 
                    id,
                    algorithm_name,
                    version_hash,
                    version_name,
                    parameters,
                    description,
                    is_active,
                    created_at,
                    created_by
                FROM algorithm_versions
            """)
            
            print("✅ 演算法版本資料遷移完成")
            
            print("\n3️⃣ 遷移蠟燭圖資料（處理重複資料）...")
            
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
            batch_size = 100000
            total_count = conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()[0]
            
            print(f"📊 總共需要遷移 {total_count:,} 筆蠟燭圖資料")
            
            for offset in range(0, total_count, batch_size):
                print(f"   處理第 {offset//batch_size + 1} 批次 ({offset:,} - {min(offset + batch_size, total_count):,})")
                
                # 使用 ROW_NUMBER() 來處理重複，只取每個 (symbol, timeframe, timestamp) 的最新版本
                conn.execute(f"""
                    INSERT INTO candlestick_data_new
                    SELECT
                      id,
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
                    FROM (
                      SELECT *,
                             ROW_NUMBER() OVER (
                               PARTITION BY symbol, timeframe, timestamp
                               ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
                             ) AS rn
                      FROM candlestick_data
                      ORDER BY symbol, timeframe, timestamp
                      LIMIT {batch_size} OFFSET {offset}
                    ) ranked
                    WHERE rn = 1
                """)
            
            print("✅ 蠟燭圖資料遷移完成")
            
            print("\n4️⃣ 遷移 Swing 資料...")
            
            # 遷移 swing_data
            conn.execute("""
                INSERT INTO swing_data_new
                SELECT
                  s.id,
                  CAST(s.symbol AS TEXT) AS symbol,
                  CAST(s.timeframe AS TEXT) AS timeframe,
                  COALESCE(av.id, 1) AS algorithm_version_id,
                  CAST(s.timestamp AS TIMESTAMP) AS timestamp,
                  CAST(s.zigzag_price AS DECIMAL(20,10)) AS zigzag_price,
                  s.zigzag_type,
                  CAST(s.zigzag_strength AS DECIMAL(10,6)) AS zigzag_strength,
                  s.zigzag_swing,
                  CAST(s.swing_high AS DECIMAL(20,10)) AS swing_high,
                  CAST(s.swing_low  AS DECIMAL(20,10)) AS swing_low,
                  CAST(s.swing_range AS DECIMAL(20,10)) AS swing_range,
                  s.swing_duration,
                  s.swing_direction,
                  COALESCE(s.created_at, now()) AS created_at
                FROM swing_data s
                LEFT JOIN algorithm_versions_new av
                  ON av.algorithm_name = s.algorithm_name AND av.version_hash = s.version_hash
            """)
            
            print("✅ Swing 資料遷移完成")
            
            print("\n5️⃣ 遷移統計資料...")
            
            # 遷移 algorithm_statistics
            conn.execute("""
                INSERT INTO algorithm_statistics_new
                SELECT
                  ast.id,
                  CAST(ast.symbol AS TEXT) AS symbol,
                  CAST(ast.timeframe AS TEXT) AS timeframe,
                  COALESCE(av.id, 1) AS algorithm_version_id,
                  COALESCE(ast.created_at::DATE, CURRENT_DATE) AS as_of_date,
                  ast.total_swings,
                  CAST(ast.avg_range AS DECIMAL(20,10)) AS avg_range,
                  ast.avg_duration,
                  CAST(ast.min_range AS DECIMAL(20,10)) AS min_range,
                  CAST(ast.min_range AS DECIMAL(20,10)) AS max_range,
                  COALESCE(ast.created_at, now()) AS created_at
                FROM algorithm_statistics ast
                LEFT JOIN algorithm_versions_new av
                  ON av.algorithm_name = ast.algorithm_name AND av.version_hash = ast.version_hash
            """)
            
            print("✅ 統計資料遷移完成")
            
            print("\n6️⃣ 創建索引...")
            
            # 創建索引
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_candle_q1_new ON candlestick_data_new(symbol, timeframe, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_candle_q2_new ON candlestick_data_new(symbol, timeframe, data_version, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_tick_q1_new ON tick_data_new(symbol, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_swing_q1_new ON swing_data_new(symbol, timeframe, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_swing_q2_new ON swing_data_new(algorithm_version_id, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_stats_q1_new ON algorithm_statistics_new(symbol, timeframe, algorithm_version_id, as_of_date)"
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            print("✅ 索引創建完成")
            
            print("\n7️⃣ 創建視圖...")
            
            # 創建最新版本視圖
            conn.execute("""
                CREATE OR REPLACE VIEW v_candlestick_latest AS
                SELECT * EXCLUDE rn
                FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (
                           PARTITION BY symbol, timeframe, timestamp
                           ORDER BY data_version DESC, updated_at DESC
                         ) AS rn
                  FROM candlestick_data_new
                )
                WHERE rn = 1
            """)
            
            print("✅ 視圖創建完成")
            
            # 驗證資料
            print("\n8️⃣ 驗證遷移結果...")
            
            # 比較資料筆數
            old_count = conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()[0]
            new_count = conn.execute("SELECT COUNT(*) FROM candlestick_data_new").fetchone()[0]
            
            print(f"📊 蠟燭圖資料：舊表 {old_count:,} 筆 → 新表 {new_count:,} 筆")
            
            old_swing_count = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()[0]
            new_swing_count = conn.execute("SELECT COUNT(*) FROM swing_data_new").fetchone()[0]
            
            print(f"📊 Swing 資料：舊表 {old_swing_count:,} 筆 → 新表 {new_swing_count:,} 筆")
            
            # 檢查新表的資料品質
            print("\n📊 新表資料品質檢查:")
            
            # 檢查價格邏輯
            invalid_prices = conn.execute("""
                SELECT COUNT(*) FROM candlestick_data_new 
                WHERE high < GREATEST(open, close) OR low > LEAST(open, close)
            """).fetchone()[0]
            
            print(f"  • 價格邏輯錯誤: {invalid_prices} 筆")
            
            # 檢查時間範圍
            time_range = conn.execute("""
                SELECT MIN(timestamp), MAX(timestamp) FROM candlestick_data_new
            """).fetchone()
            
            print(f"  • 時間範圍: {time_range[0]} 到 {time_range[1]}")
            
            print("\n✅ 遷移完成！新表已創建，原始表保留作為備份")
            print("\n⚠️  請在驗證新表正常運作後，再考慮刪除舊表")
            
    except Exception as e:
        print(f"❌ 遷移失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🔧 完整資料庫重構遷移工具")
    print("=" * 60)
    
    # 創建備份
    create_backup()
    
    # 清理已存在的新表
    clean_existing_new_tables()
    
    # 創建新架構
    create_new_schema()
    
    # 執行遷移
    migrate_data()
    
    print("\n🎉 重構遷移完成！")
    print("📝 建議接下來的步驟：")
    print("   1. 測試新表的查詢功能")
    print("   2. 更新應用程式代碼以使用新表")
    print("   3. 驗證資料完整性")
    print("   4. 考慮刪除舊表（備份已保留）")
#!/usr/bin/env python3
"""
完成剩餘資料庫遷移腳本
遷移 tick_data、swing_data、algorithm_statistics 到新架構
"""

import os
import sys
import duckdb
from datetime import datetime
import shutil

# 添加專案根目錄到 Python 路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH

def create_backup():
    """創建資料庫備份"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = str(DUCKDB_PATH).replace('.duckdb', f'_backup_complete_{timestamp}.duckdb')
    
    print(f"📦 創建備份: {backup_path}")
    shutil.copy2(str(DUCKDB_PATH), backup_path)
    
    # 獲取備份文件大小
    backup_size = os.path.getsize(backup_path) / (1024 * 1024)
    print(f"✅ 備份完成，大小: {backup_size:.2f} MB")
    
    return backup_path

def clean_existing_new_tables(conn):
    """清理已存在的新表"""
    print("🧹 清理已存在的新表...")
    
    tables_to_drop = [
        'tick_data_new',
        'swing_data_new', 
        'algorithm_statistics_new'
    ]
    
    for table in tables_to_drop:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"  ✅ 已清理 {table}")
        except Exception as e:
            print(f"  ⚠️  清理 {table} 時發生錯誤: {e}")

def create_remaining_schema(conn):
    """創建剩餘表的新架構"""
    print("🏗️  創建剩餘表的新架構...")
    
    # 創建 tick_data_new
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tick_data_new (
            id          BIGINT PRIMARY KEY,
            symbol      TEXT NOT NULL,
            timestamp   TIMESTAMP NOT NULL,
            event_type  TEXT NOT NULL,
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
            UNIQUE (symbol, timestamp, event_type, exchange, data_version)
        )
    """)
    print("  ✅ 創建 tick_data_new")
    
    # 創建 swing_data_new
    conn.execute("""
        CREATE TABLE IF NOT EXISTS swing_data_new (
            id                   BIGINT PRIMARY KEY,
            symbol               TEXT NOT NULL,
            timeframe            TEXT NOT NULL,
            algorithm_version_id BIGINT NOT NULL,
            timestamp            TIMESTAMP NOT NULL,
            zigzag_price         DECIMAL(20,10),
            zigzag_type          TEXT,
            zigzag_strength      DECIMAL(10,6),
            zigzag_swing         INTEGER,
            swing_high           DECIMAL(20,10),
            swing_low            DECIMAL(20,10),
            swing_range          DECIMAL(20,10),
            swing_duration       INTEGER,
            swing_direction      TEXT,
            created_at           TIMESTAMP DEFAULT now(),
            UNIQUE (symbol, timeframe, algorithm_version_id, timestamp)
        )
    """)
    print("  ✅ 創建 swing_data_new")
    
    # 創建 algorithm_statistics_new
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
            up_swings            INTEGER,
            down_swings          INTEGER,
            created_at           TIMESTAMP DEFAULT now(),
            UNIQUE(symbol, timeframe, algorithm_version_id, as_of_date)
        )
    """)
    print("  ✅ 創建 algorithm_statistics_new")

def migrate_tick_data(conn):
    """遷移 tick_data"""
    print("🔄 遷移 tick_data...")
    
    # 檢查原始表是否有資料
    result = conn.execute("SELECT COUNT(*) FROM tick_data").fetchone()
    tick_count = result[0] if result else 0
    
    if tick_count == 0:
        print("  ℹ️  tick_data 表為空，跳過遷移")
        return
    
    print(f"  📊 發現 {tick_count} 筆 tick 資料")
    
    # 遷移資料
    conn.execute("""
        INSERT INTO tick_data_new
        SELECT
            CAST(id AS BIGINT) AS id,
            CAST(symbol AS TEXT) AS symbol,
            CAST(timestamp AS TIMESTAMP) AS timestamp,
            'quote' AS event_type,
            'default' AS exchange,
            NULL AS price,
            NULL AS size,
            CAST(bid AS DECIMAL(20,10)) AS bid,
            CAST(ask AS DECIMAL(20,10)) AS ask,
            NULL AS bid_size,
            NULL AS ask_size,
            COALESCE(data_version, 'v1.0') AS data_version,
            data_source,
            COALESCE(created_at, now()) AS created_at,
            COALESCE(updated_at, now()) AS updated_at
        FROM tick_data
    """)
    
    print("  ✅ tick_data 遷移完成")

def migrate_swing_data(conn):
    """遷移 swing_data"""
    print("🔄 遷移 swing_data...")
    
    # 檢查原始表是否有資料
    result = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()
    swing_count = result[0] if result else 0
    
    if swing_count == 0:
        print("  ℹ️  swing_data 表為空，跳過遷移")
        return
    
    print(f"  📊 發現 {swing_count} 筆 swing 資料")
    
    # 確保 algorithm_versions 表有對應的記錄
    conn.execute("""
        INSERT INTO algorithm_versions (id, algorithm_name, version_hash, version_name, parameters, description, is_active, created_at, created_by)
        SELECT DISTINCT
            ROW_NUMBER() OVER (ORDER BY algorithm_name, version_hash) + (SELECT COALESCE(MAX(id), 0) FROM algorithm_versions) AS id,
            algorithm_name,
            version_hash,
            algorithm_name || '_' || SUBSTRING(version_hash, 1, 8) AS version_name,
            '{}'::JSON AS parameters,
            'Migrated from old schema' AS description,
            TRUE AS is_active,
            now() AS created_at,
            'system' AS created_by
        FROM swing_data
        WHERE (algorithm_name, version_hash) NOT IN (
            SELECT algorithm_name, version_hash FROM algorithm_versions
        )
    """)
    
    # 遷移 swing_data，處理重複資料
    conn.execute("""
        INSERT INTO swing_data_new
        SELECT
            ROW_NUMBER() OVER (ORDER BY symbol, timeframe, timestamp) AS id,
            CAST(s.symbol AS TEXT) AS symbol,
            CAST(s.timeframe AS TEXT) AS timeframe,
            av.id AS algorithm_version_id,
            CAST(s.timestamp AS TIMESTAMP) AS timestamp,
            CAST(s.zigzag_price AS DECIMAL(20,10)) AS zigzag_price,
            s.zigzag_type,
            CAST(s.zigzag_strength AS DECIMAL(10,6)) AS zigzag_strength,
            s.zigzag_swing,
            CAST(s.swing_high AS DECIMAL(20,10)) AS swing_high,
            CAST(s.swing_low AS DECIMAL(20,10)) AS swing_low,
            CAST(s.swing_range AS DECIMAL(20,10)) AS swing_range,
            s.swing_duration,
            s.swing_direction,
            COALESCE(s.created_at, now()) AS created_at
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol, timeframe, algorithm_name, version_hash, timestamp
                       ORDER BY id DESC
                   ) AS rn
            FROM swing_data
        ) s
        JOIN algorithm_versions av ON av.algorithm_name = s.algorithm_name AND av.version_hash = s.version_hash
        WHERE s.rn = 1
    """)
    
    print("  ✅ swing_data 遷移完成")

def migrate_algorithm_statistics(conn):
    """遷移 algorithm_statistics"""
    print("🔄 遷移 algorithm_statistics...")
    
    # 檢查原始表是否有資料
    result = conn.execute("SELECT COUNT(*) FROM algorithm_statistics").fetchone()
    stats_count = result[0] if result else 0
    
    if stats_count == 0:
        print("  ℹ️  algorithm_statistics 表為空，跳過遷移")
        return
    
    print(f"  📊 發現 {stats_count} 筆統計資料")
    
    # 遷移 algorithm_statistics，僅保留唯一鍵最新一筆
    conn.execute("""
        INSERT INTO algorithm_statistics_new
        SELECT
            CAST(a.id AS BIGINT) AS id,
            CAST(a.symbol AS TEXT) AS symbol,
            CAST(a.timeframe AS TEXT) AS timeframe,
            av.id AS algorithm_version_id,
            CAST(a.created_at AS DATE) AS as_of_date,
            a.total_swings,
            CAST(a.avg_range AS DECIMAL(20,10)) AS avg_range,
            a.avg_duration,
            CAST(a.min_range AS DECIMAL(20,10)) AS min_range,
            CAST(a.max_range AS DECIMAL(20,10)) AS max_range,
            a.up_swings,
            a.total_swings - a.up_swings AS down_swings,
            COALESCE(a.created_at, now()) AS created_at
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol, timeframe, algorithm_name, version_hash, CAST(created_at AS DATE)
                       ORDER BY created_at DESC, id DESC
                   ) AS rn
            FROM algorithm_statistics
        ) a
        JOIN algorithm_versions av ON av.algorithm_name = a.algorithm_name AND av.version_hash = a.version_hash
        WHERE a.rn = 1
    """)
    
    print("  ✅ algorithm_statistics 遷移完成")

def create_indexes(conn):
    """創建索引"""
    print("🔍 創建索引...")
    
    indexes = [
        ("CREATE INDEX IF NOT EXISTS idx_tick_q1_new ON tick_data_new(symbol, timestamp)", "tick_data_new 索引"),
        ("CREATE INDEX IF NOT EXISTS idx_swing_q1_new ON swing_data_new(symbol, timeframe, timestamp)", "swing_data_new 索引1"),
        ("CREATE INDEX IF NOT EXISTS idx_swing_q2_new ON swing_data_new(algorithm_version_id, timestamp)", "swing_data_new 索引2"),
        ("CREATE INDEX IF NOT EXISTS idx_stats_q1_new ON algorithm_statistics_new(symbol, timeframe, algorithm_version_id)", "algorithm_statistics_new 索引")
    ]
    
    for sql, description in indexes:
        try:
            conn.execute(sql)
            print(f"  ✅ {description}")
        except Exception as e:
            print(f"  ⚠️  {description} 失敗: {e}")

def rename_tables(conn):
    """重命名表"""
    print("🔄 重命名表...")
    
    renames = [
        ("tick_data", "tick_data_backup"),
        ("tick_data_new", "tick_data"),
        ("swing_data", "swing_data_backup"),
        ("swing_data_new", "swing_data"),
        ("algorithm_statistics", "algorithm_statistics_backup"),
        ("algorithm_statistics_new", "algorithm_statistics")
    ]
    
    for old_name, new_name in renames:
        try:
            conn.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
            print(f"  ✅ {old_name} → {new_name}")
        except Exception as e:
            print(f"  ⚠️  重命名 {old_name} 失敗: {e}")

def validate_migration(conn):
    """驗證遷移結果"""
    print("🔍 驗證遷移結果...")
    
    # 檢查各表的記錄數
    tables = [
        ("tick_data", "tick_data_backup"),
        ("swing_data", "swing_data_backup"),
        ("algorithm_statistics", "algorithm_statistics_backup")
    ]
    
    for new_table, old_table in tables:
        try:
            new_count = conn.execute(f"SELECT COUNT(*) FROM {new_table}").fetchone()[0]
            old_count = conn.execute(f"SELECT COUNT(*) FROM {old_table}").fetchone()[0]
            print(f"  📊 {new_table}: {new_count} 筆 (原表: {old_count} 筆)")
        except Exception as e:
            print(f"  ⚠️  檢查 {new_table} 失敗: {e}")

def main():
    """主函數"""
    print("🚀 開始完成剩餘資料庫遷移...")
    
    # 創建備份
    backup_path = create_backup()
    
    try:
        # 連接資料庫
        conn = duckdb.connect(str(DUCKDB_PATH))
        print(f"✅ 已連接到資料庫: {DUCKDB_PATH}")
        
        # 開始交易
        conn.execute("BEGIN TRANSACTION")
        
        # 清理已存在的新表
        clean_existing_new_tables(conn)
        
        # 創建剩餘表的新架構
        create_remaining_schema(conn)
        
        # 遷移資料
        migrate_tick_data(conn)
        migrate_swing_data(conn)
        migrate_algorithm_statistics(conn)
        
        # 創建索引
        create_indexes(conn)
        
        # 重命名表
        rename_tables(conn)
        
        # 驗證遷移結果
        validate_migration(conn)
        
        # 提交交易
        conn.execute("COMMIT")
        print("✅ 遷移完成！")
        
    except Exception as e:
        # 回滾交易
        conn.execute("ROLLBACK")
        print(f"❌ 遷移失敗: {e}")
        print(f"📦 備份文件位置: {backup_path}")
        raise
    finally:
        conn.close()
        print("🔒 資料庫連接已關閉")

if __name__ == "__main__":
    main()
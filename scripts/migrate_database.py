#!/usr/bin/env python3
"""
資料庫遷移腳本

將現有的資料庫結構升級到支援版本控制的新架構
"""

import sys
import os
import pandas as pd
import duckdb
from pathlib import Path
import logging

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DUCKDB_PATH

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def backup_database():
    """備份現有資料庫"""
    try:
        db_path = Path(DUCKDB_PATH)
        backup_path = db_path.parent / f"{db_path.stem}_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
        
        # 複製資料庫檔案
        import shutil
        shutil.copy2(db_path, backup_path)
        
        logger.info(f"資料庫已備份到: {backup_path}")
        return backup_path
        
    except Exception as e:
        logger.error(f"備份資料庫失敗: {e}")
        raise


def check_existing_schema():
    """檢查現有資料庫結構"""
    try:
        conn = duckdb.connect(str(DUCKDB_PATH))
        
        # 檢查現有表結構
        tables = conn.execute("SHOW TABLES").fetchall()
        logger.info(f"現有資料表: {[table[0] for table in tables]}")
        
        # 檢查 candlestick_data 表結構
        if any(table[0] == 'candlestick_data' for table in tables):
            columns = conn.execute("DESCRIBE candlestick_data").fetchall()
            logger.info(f"candlestick_data 欄位: {[col[0] for col in columns]}")
            
            # 檢查是否有 data_version 欄位
            has_data_version = any(col[0] == 'data_version' for col in columns)
            logger.info(f"是否有 data_version 欄位: {has_data_version}")
            
            return has_data_version
        else:
            logger.info("candlestick_data 表不存在")
            return False
            
    except Exception as e:
        logger.error(f"檢查資料庫結構失敗: {e}")
        raise
    finally:
        if conn:
            conn.close()


def migrate_database():
    """執行資料庫遷移"""
    try:
        logger.info("開始資料庫遷移...")
        
        # 1. 備份現有資料庫
        backup_path = backup_database()
        
        # 2. 檢查現有結構
        has_data_version = check_existing_schema()
        
        if has_data_version:
            logger.info("資料庫已經是最新版本，無需遷移")
            return
        
        # 3. 執行遷移
        conn = duckdb.connect(str(DUCKDB_PATH))
        
        # 遷移 candlestick_data 表
        logger.info("遷移 candlestick_data 表...")
        
        # 清理可能存在的臨時表
        conn.execute("DROP TABLE IF EXISTS candlestick_data_new")
        
        # 創建臨時表
        conn.execute("""
            CREATE TABLE candlestick_data_new (
                id INTEGER,
                symbol VARCHAR(20),
                timeframe VARCHAR(5),
                timestamp TIMESTAMP,
                open DECIMAL(10, 5),
                high DECIMAL(10, 5),
                low DECIMAL(10, 5),
                close DECIMAL(10, 5),
                volume BIGINT,
                data_version VARCHAR(50) DEFAULT 'v1.0',
                data_source VARCHAR(100) DEFAULT 'migrated',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 複製現有資料到新表（使用 ROW_NUMBER 生成 id）
        conn.execute("""
            INSERT INTO candlestick_data_new 
            (id, symbol, timeframe, timestamp, open, high, low, close, volume, created_at)
            SELECT 
                ROW_NUMBER() OVER (ORDER BY timestamp) as id,
                symbol, timeframe, timestamp, open, high, low, close, volume, created_at
            FROM candlestick_data
        """)
        
        # 刪除舊表並重命名新表
        conn.execute("DROP TABLE candlestick_data")
        conn.execute("ALTER TABLE candlestick_data_new RENAME TO candlestick_data")
        
        # 遷移 tick_data 表
        logger.info("遷移 tick_data 表...")
        
        # 清理可能存在的臨時表
        conn.execute("DROP TABLE IF EXISTS tick_data_new")
        
        # 檢查 tick_data 表是否存在
        tables = conn.execute("SHOW TABLES").fetchall()
        if any(table[0] == 'tick_data' for table in tables):
            # 創建臨時表
            conn.execute("""
                CREATE TABLE tick_data_new (
                    id INTEGER PRIMARY KEY,
                    symbol VARCHAR(20),
                    timestamp TIMESTAMP,
                    bid DECIMAL(10, 5),
                    ask DECIMAL(10, 5),
                    volume BIGINT,
                    data_version VARCHAR(50) DEFAULT 'v1.0',
                    data_source VARCHAR(100) DEFAULT 'migrated',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 複製現有資料到新表
            conn.execute("""
                INSERT INTO tick_data_new 
                (symbol, timestamp, bid, ask, volume, created_at)
                SELECT symbol, timestamp, bid, ask, volume, created_at
                FROM tick_data
            """)
            
            # 刪除舊表並重命名新表
            conn.execute("DROP TABLE tick_data")
            conn.execute("ALTER TABLE tick_data_new RENAME TO tick_data")
        
        # 遷移 swing_data 表
        logger.info("遷移 swing_data 表...")
        
        # 清理可能存在的臨時表
        conn.execute("DROP TABLE IF EXISTS swing_data_new")
        
        if any(table[0] == 'swing_data' for table in tables):
            # 檢查 swing_data 表結構
            columns = conn.execute("DESCRIBE swing_data").fetchall()
            column_names = [col[0] for col in columns]
            
            if 'algorithm_name' not in column_names:
                # 創建臨時表
                conn.execute("""
                    CREATE TABLE swing_data_new (
                        id INTEGER PRIMARY KEY,
                        symbol VARCHAR(20),
                        timeframe VARCHAR(5),
                        algorithm_name VARCHAR(50),
                        version_hash VARCHAR(64),
                        timestamp TIMESTAMP,
                        zigzag_price DECIMAL(10, 5),
                        zigzag_type VARCHAR(10),
                        zigzag_strength DECIMAL(10, 5),
                        zigzag_swing INTEGER,
                        swing_high DECIMAL(10, 5),
                        swing_low DECIMAL(10, 5),
                        swing_range DECIMAL(10, 5),
                        swing_duration INTEGER,
                        swing_direction VARCHAR(10),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 複製現有資料到新表，將 algorithm 欄位映射到 algorithm_name
                if 'algorithm' in column_names:
                    conn.execute("""
                        INSERT INTO swing_data_new 
                        (symbol, timeframe, algorithm_name, timestamp, zigzag_price, zigzag_type, 
                         zigzag_strength, zigzag_swing, swing_high, swing_low, swing_range, 
                         swing_duration, swing_direction, created_at)
                        SELECT symbol, timeframe, algorithm, timestamp, zigzag_price, zigzag_type,
                               zigzag_strength, zigzag_swing, swing_high, swing_low, swing_range,
                               swing_duration, swing_direction, created_at
                        FROM swing_data
                    """)
                else:
                    # 如果沒有 algorithm 欄位，使用預設值
                    conn.execute("""
                        INSERT INTO swing_data_new 
                        (symbol, timeframe, algorithm_name, timestamp, zigzag_price, zigzag_type, 
                         zigzag_strength, zigzag_swing, swing_high, swing_low, swing_range, 
                         swing_duration, swing_direction, created_at)
                        SELECT symbol, timeframe, 'zigzag', timestamp, zigzag_price, zigzag_type,
                               zigzag_strength, zigzag_swing, swing_high, swing_low, swing_range,
                               swing_duration, swing_direction, created_at
                        FROM swing_data
                    """)
                
                # 刪除舊表並重命名新表
                conn.execute("DROP TABLE swing_data")
                conn.execute("ALTER TABLE swing_data_new RENAME TO swing_data")
        
        # 創建新的版本控制表
        logger.info("創建版本控制表...")
        
        # algorithm_versions 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS algorithm_versions (
                id INTEGER PRIMARY KEY,
                algorithm_name VARCHAR(50),
                version_hash VARCHAR(64),
                version_name VARCHAR(100),
                parameters JSON,
                description TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(50) DEFAULT 'system'
            )
        """)
        
        # algorithm_statistics 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS algorithm_statistics (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR(20),
                timeframe VARCHAR(5),
                algorithm_name VARCHAR(50),
                version_hash VARCHAR(64),
                total_swings INTEGER,
                avg_range DECIMAL(10, 5),
                avg_duration INTEGER,
                min_range DECIMAL(10, 5),
                max_range DECIMAL(10, 5),
                up_swings INTEGER,
                down_swings INTEGER,
                calculation_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # swing_data_archive 表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS swing_data_archive (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR(20),
                timeframe VARCHAR(5),
                algorithm_name VARCHAR(50),
                version_hash VARCHAR(64),
                timestamp TIMESTAMP,
                zigzag_price DECIMAL(10, 5),
                zigzag_type VARCHAR(10),
                zigzag_strength DECIMAL(10, 5),
                zigzag_swing INTEGER,
                swing_high DECIMAL(10, 5),
                swing_low DECIMAL(10, 5),
                swing_range DECIMAL(10, 5),
                swing_duration INTEGER,
                swing_direction VARCHAR(10),
                archive_date DATE,
                original_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 創建索引
        logger.info("創建索引...")
        
        # candlestick_data 索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candlestick_symbol_timeframe_timestamp 
            ON candlestick_data(symbol, timeframe, timestamp)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candlestick_version 
            ON candlestick_data(data_version)
        """)
        
        # tick_data 索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tick_symbol_timestamp 
            ON tick_data(symbol, timestamp)
        """)
        
        # swing_data 索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_swing_symbol_timeframe_algorithm_version 
            ON swing_data(symbol, timeframe, algorithm_name, version_hash)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_swing_timestamp 
            ON swing_data(timestamp)
        """)
        
        # algorithm_versions 索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_algorithm_versions_hash 
            ON algorithm_versions(version_hash)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_algorithm_versions_active 
            ON algorithm_versions(is_active)
        """)
        
        # algorithm_statistics 索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_statistics_symbol_timeframe_algorithm 
            ON algorithm_statistics(symbol, timeframe, algorithm_name, version_hash)
        """)
        
        # swing_data_archive 索引
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_archive_symbol_timeframe_algorithm 
            ON swing_data_archive(symbol, timeframe, algorithm_name, version_hash)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_archive_date 
            ON swing_data_archive(archive_date)
        """)
        
        # 驗證遷移結果
        logger.info("驗證遷移結果...")
        
        tables = conn.execute("SHOW TABLES").fetchall()
        logger.info(f"遷移後的資料表: {[table[0] for table in tables]}")
        
        # 檢查 candlestick_data 表結構
        columns = conn.execute("DESCRIBE candlestick_data").fetchall()
        logger.info(f"candlestick_data 欄位: {[col[0] for col in columns]}")
        
        # 檢查資料數量
        candlestick_result = conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()
        candlestick_count = candlestick_result[0] if candlestick_result else 0
        logger.info(f"candlestick_data 記錄數: {candlestick_count}")
        
        if any(table[0] == 'swing_data' for table in tables):
            swing_result = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()
            swing_count = swing_result[0] if swing_result else 0
            logger.info(f"swing_data 記錄數: {swing_count}")
        
        conn.close()
        
        logger.info("資料庫遷移完成！")
        
    except Exception as e:
        logger.error(f"資料庫遷移失敗: {e}")
        raise


def main():
    """主函數"""
    print("=" * 60)
    print("資料庫遷移工具")
    print("=" * 60)
    
    try:
        # 檢查資料庫是否存在
        db_path = Path(DUCKDB_PATH)
        if not db_path.exists():
            print("資料庫檔案不存在，將創建新的資料庫結構")
            migrate_database()
        else:
            print("檢測到現有資料庫，開始遷移...")
            migrate_database()
        
        print("\n" + "=" * 60)
        print("✅ 遷移完成！")
        print("=" * 60)
        print("現在可以執行以下命令測試新架構：")
        print("python test_version_control.py")
        print("python process_swings.py")
        
    except Exception as e:
        print(f"\n❌ 遷移失敗: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 
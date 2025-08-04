#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
修正swing_data表結構腳本
將id欄位改為自動遞增
"""

import duckdb
from config.settings import DUCKDB_PATH

def fix_swing_data_table():
    """修正swing_data表結構"""
    print("=== 修正swing_data表結構 ===")
    
    conn = duckdb.connect(DUCKDB_PATH)
    
    try:
        # 創建新的swing_data表
        print("創建新的swing_data表...")
        
        # 先刪除舊表（如果存在）
        conn.execute("DROP TABLE IF EXISTS swing_data")
        
        # 創建新表
        create_sql = """
        CREATE TABLE swing_data (
            id INTEGER PRIMARY KEY,
            symbol VARCHAR,
            timeframe VARCHAR,
            algorithm_name VARCHAR,
            version_hash VARCHAR,
            timestamp TIMESTAMP,
            zigzag_price DECIMAL(10,5),
            zigzag_type VARCHAR,
            zigzag_strength DECIMAL(10,5),
            zigzag_swing INTEGER,
            swing_high DECIMAL(10,5),
            swing_low DECIMAL(10,5),
            swing_range DECIMAL(10,5),
            swing_duration INTEGER,
            swing_direction VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        conn.execute(create_sql)
        print("新swing_data表創建成功")
        
        # 檢查新表結構
        print("\n新swing_data表結構:")
        result = conn.execute("DESCRIBE swing_data").fetchall()
        for row in result:
            print(f"  {row}")
        
        conn.commit()
        print("\nswing_data表修正完成！")
        
    except Exception as e:
        print(f"修正表結構失敗: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    fix_swing_data_table() 
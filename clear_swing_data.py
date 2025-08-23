#!/usr/bin/env python3
from src.database.connection import DuckDBConnection
from config.settings import DUCKDB_PATH

def clear_swing_data():
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            db.conn.execute('DELETE FROM swing_data')
            print('波段數據已清空')
    except Exception as e:
        print(f'清空失敗: {e}')

if __name__ == "__main__":
    clear_swing_data()

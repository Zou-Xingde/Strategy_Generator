#!/usr/bin/env python3
"""
資料庫性能優化腳本
"""

import duckdb
import time

def optimize_database():
    """優化資料庫性能"""
    print("🔧 開始優化資料庫性能...")
    
    conn = duckdb.connect('database/market_data.duckdb')
    
    try:
        # 檢查現有索引
        print("📊 檢查現有索引...")
        try:
            indexes = conn.execute("SHOW INDEXES").fetchall()
            for idx in indexes:
                print(f"  - {idx}")
        except:
            print("  - 無法檢查索引信息")
        
        # 為 v_candlestick_latest 視圖的基礎表添加索引
        print("\n🔧 創建性能索引...")
        
        # 為 candlestick_data_new 表添加複合索引
        start = time.time()
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candlestick_new_symbol_timeframe_timestamp 
            ON candlestick_data_new(symbol, timeframe, timestamp DESC)
        """)
        end = time.time()
        print(f"✅ 創建複合索引: {end-start:.2f}秒")
        
        # 為 data_version 添加索引
        start = time.time()
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_candlestick_new_data_version 
            ON candlestick_data_new(data_version)
        """)
        end = time.time()
        print(f"✅ 創建版本索引: {end-start:.2f}秒")
        
        # 分析表統計信息
        print("\n📈 分析表統計信息...")
        start = time.time()
        conn.execute("ANALYZE candlestick_data_new")
        end = time.time()
        print(f"✅ 分析完成: {end-start:.2f}秒")
        
        # 測試查詢性能
        print("\n🧪 測試查詢性能...")
        
        # 測試1: 簡單查詢
        start = time.time()
        result = conn.execute("""
            SELECT COUNT(*) FROM v_candlestick_latest 
            WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
        """).fetchone()
        end = time.time()
        print(f"✅ 計數查詢: {end-start:.3f}秒, 結果: {result[0]} 筆")
        
        # 測試2: 限制查詢
        start = time.time()
        result = conn.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM v_candlestick_latest
            WHERE symbol = 'XAUUSD' AND timeframe = 'D1'
            ORDER BY timestamp DESC
            LIMIT 100
        """).fetchall()
        end = time.time()
        print(f"✅ 限制查詢: {end-start:.3f}秒, 結果: {len(result)} 筆")
        
        # 測試3: 多商品查詢
        start = time.time()
        result = conn.execute("""
            SELECT symbol, timeframe, COUNT(*) as count
            FROM v_candlestick_latest
            GROUP BY symbol, timeframe
            ORDER BY symbol, timeframe
        """).fetchall()
        end = time.time()
        print(f"✅ 分組查詢: {end-start:.3f}秒, 結果: {len(result)} 組")
        
        conn.commit()
        print("\n🎉 資料庫優化完成！")
        
    except Exception as e:
        print(f"❌ 優化失敗: {e}")
        try:
            conn.rollback()
        except:
            pass
    finally:
        conn.close()

if __name__ == "__main__":
    optimize_database() 
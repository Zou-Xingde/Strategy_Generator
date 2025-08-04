#!/usr/bin/env python3
"""
修正資料庫結構並重建所有時間框架的蠟燭圖資料
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import shutil
from pathlib import Path
import time

# 設定
DUCKDB_PATH = "database/market_data.duckdb"
CHUNK_ROWS = 50_000  # 每次處理的資料筆數

# 時間框架配置
TIMEFRAMES = {
    "M1": "1min",
    "M5": "5min", 
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D"
}

def backup_database():
    """備份資料庫"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"database/backup_fix_{timestamp}.duckdb"
    
    if os.path.exists(DUCKDB_PATH):
        print(f"🔄 備份資料庫到: {backup_path}")
        shutil.copy2(DUCKDB_PATH, backup_path)
        print("✅ 資料庫備份完成")
    return backup_path

def fix_database_structure(conn):
    """修正資料庫結構"""
    print("🔧 修正資料庫結構...")
    
    # 檢查是否有資料
    result = conn.execute("SELECT COUNT(*) FROM candlestick_data_new").fetchone()
    has_data = result[0] > 0 if result else False
    
    if has_data:
        print("📊 發現現有資料，將保留並修正結構")
        # 如果有資料，創建備份表並重新創建主表
        conn.execute("DROP TABLE IF EXISTS candlestick_data_backup_temp")
        conn.execute("CREATE TABLE candlestick_data_backup_temp AS SELECT * FROM candlestick_data_new")
        conn.execute("DROP TABLE candlestick_data_new")
    else:
        print("📊 沒有現有資料，直接重建表結構")
        conn.execute("DROP TABLE IF EXISTS candlestick_data_new")
    
    # 重新創建表，使用序列生成 ID
    conn.execute("DROP SEQUENCE IF EXISTS candlestick_id_seq")
    conn.execute("CREATE SEQUENCE candlestick_id_seq START 1")
    
    conn.execute("""
        CREATE TABLE candlestick_data_new (
            id          BIGINT PRIMARY KEY DEFAULT nextval('candlestick_id_seq'),
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
            UNIQUE (symbol, timeframe, timestamp, data_version)
        )
    """)
    
    if has_data:
        # 恢復資料（不包括 id，讓其自動生成）
        print("🔄 恢復現有資料...")
        conn.execute("""
            INSERT INTO candlestick_data_new 
            (symbol, timeframe, timestamp, open, high, low, close, volume, 
             data_version, data_source, created_at, updated_at)
            SELECT symbol, timeframe, timestamp, open, high, low, close, volume, 
                   data_version, data_source, created_at, updated_at
            FROM candlestick_data_backup_temp
        """)
        conn.execute("DROP TABLE candlestick_data_backup_temp")
    
    # 重新創建視圖
    conn.execute("DROP VIEW IF EXISTS v_candlestick_latest")
    conn.execute("""
        CREATE VIEW v_candlestick_latest AS
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
    
    conn.commit()
    print("✅ 資料庫結構修正完成")

def process_single_symbol(symbol, tick_file, conn):
    """處理單個商品的所有時間框架"""
    print(f"\n🚀 開始處理 {symbol}...")
    
    # 清空該商品的資料
    conn.execute("DELETE FROM candlestick_data_new WHERE symbol = ? AND data_source = 'rebuild_test'", [symbol])
    
    try:
        # 讀取樣本資料測試
        print(f"📊 讀取 {symbol} 樣本資料...")
        chunk = pd.read_csv(tick_file, nrows=CHUNK_ROWS)
        
        print(f"原始資料: {chunk.shape[0]} 筆")
        
        # 資料清理
        chunk = chunk.rename(columns={"DateTime": "timestamp"})
        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
        chunk = chunk.dropna(subset=["timestamp"])
        chunk = chunk.sort_values("timestamp")
        
        print(f"清理後: {len(chunk)} 筆")
        print(f"時間範圍: {chunk['timestamp'].min()} 到 {chunk['timestamp'].max()}")
        
        # 計算中價
        chunk["price"] = (chunk["Bid"] + chunk["Ask"]) / 2
        chunk["volume"] = chunk.get("Volume", 1000000)
        
        # 設定索引
        chunk.set_index("timestamp", inplace=True)
        
        # 處理每個時間框架
        total_inserted = 0
        results = {}
        
        for tf_code, tf_pandas in TIMEFRAMES.items():
            print(f"\n🔸 處理 {tf_code} ({tf_pandas})...")
            
            try:
                # 重新取樣
                ohlc_data = chunk["price"].resample(tf_pandas).ohlc()
                volume_data = chunk["volume"].resample(tf_pandas).sum()
                
                # 合併資料
                candlestick_df = pd.DataFrame({
                    "open": ohlc_data["open"],
                    "high": ohlc_data["high"], 
                    "low": ohlc_data["low"],
                    "close": ohlc_data["close"],
                    "volume": volume_data
                })
                
                # 移除無效資料
                candlestick_df = candlestick_df.dropna()
                
                print(f"   生成 {len(candlestick_df)} 根蠟燭")
                
                if len(candlestick_df) > 0:
                    # 準備批次插入資料
                    records = []
                    for timestamp, row in candlestick_df.iterrows():
                        records.append({
                            'symbol': symbol,
                            'timeframe': tf_code,
                            'timestamp': timestamp,
                            'open': round(float(row["open"]), 10),
                            'high': round(float(row["high"]), 10),
                            'low': round(float(row["low"]), 10),
                            'close': round(float(row["close"]), 10),
                            'volume': round(float(row["volume"]), 18),
                            'data_version': 'v2.0',
                            'data_source': 'rebuild_test'
                        })
                    
                    # 使用 DataFrame 批次插入
                    df_insert = pd.DataFrame(records)
                    
                    # 插入到資料庫
                    conn.execute("""
                        INSERT INTO candlestick_data_new 
                        (symbol, timeframe, timestamp, open, high, low, close, volume, data_version, data_source)
                        SELECT symbol, timeframe, timestamp, open, high, low, close, volume, data_version, data_source
                        FROM df_insert
                    """)
                    
                    inserted_count = len(records)
                    print(f"   成功插入 {inserted_count} 根")
                    total_inserted += inserted_count
                    results[tf_code] = inserted_count
                else:
                    results[tf_code] = 0
                    
            except Exception as e:
                print(f"   ❌ {tf_code} 處理失敗: {e}")
                results[tf_code] = 0
        
        # 提交
        conn.commit()
        print(f"\n✅ {symbol} 處理完成，總計插入 {total_inserted} 根蠟燭")
        
        return results
        
    except Exception as e:
        print(f"❌ 處理 {symbol} 失敗: {e}")
        import traceback
        traceback.print_exc()
        return {}

def main():
    """主函數"""
    print("🔧 修正資料庫結構並重建時間框架資料")
    print("=" * 60)
    
    # 商品配置
    symbols_config = {
        "XAUUSD": {
            "name": "黃金",
            "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorXAUUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
        }
    }
    
    # 備份資料庫
    backup_path = backup_database()
    
    # 連接資料庫
    print("🔌 連接資料庫...")
    conn = duckdb.connect(DUCKDB_PATH)
    
    try:
        # 修正資料庫結構
        fix_database_structure(conn)
        
        # 處理每個商品
        all_results = {}
        
        for symbol, config in symbols_config.items():
            if not os.path.exists(config['tick_file']):
                print(f"❌ 檔案不存在: {config['tick_file']}")
                continue
                
            results = process_single_symbol(symbol, config['tick_file'], conn)
            all_results[symbol] = results
        
        # 驗證結果
        print(f"\n🔍 驗證結果...")
        for symbol in symbols_config.keys():
            print(f"\n🔸 {symbol}:")
            for tf_code in TIMEFRAMES.keys():
                result = conn.execute("""
                    SELECT COUNT(*) FROM candlestick_data_new 
                    WHERE symbol = ? AND timeframe = ? AND data_source = 'rebuild_test'
                """, [symbol, tf_code]).fetchone()
                
                count = result[0] if result else 0
                print(f"   {tf_code}: {count} 根")
        
        print(f"\n🎉 處理完成！")
        print(f"💾 備份檔案: {backup_path}")
        
    except Exception as e:
        print(f"❌ 主程序失敗: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
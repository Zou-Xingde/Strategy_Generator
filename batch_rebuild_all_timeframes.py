#!/usr/bin/env python3
"""
分批重新處理所有時間框架的蠟燭圖資料
包含完整的資料驗證和檢查
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import shutil
from pathlib import Path
from typing import Dict, List, Tuple
import time

# 設定
DUCKDB_PATH = "database/market_data.duckdb"
CHUNK_ROWS = 100_000  # 每次處理的資料筆數
MAX_RECORDS_PER_INSERT = 5_000  # 每次插入的最大記錄數

# 商品配置
SYMBOLS_CONFIG = {
    "XAUUSD": {
        "name": "黃金",
        "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorXAUUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
    },
    "US30": {
        "name": "道瓊斯",
        "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorUSA30IDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
    },
    "US100": {
        "name": "納斯達克",
        "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorUSATECHIDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
    }
}

# 時間框架配置
TIMEFRAMES = {
    "M1": "1min",
    "M5": "5min", 
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D",
    "W1": "1W",
    "MN": "1M"
}

def clear_screen():
    """清屏"""
    os.system('cls' if os.name == 'nt' else 'clear')

def backup_database():
    """備份資料庫"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"database/backup_before_rebuild_{timestamp}.duckdb"
    
    if os.path.exists(DUCKDB_PATH):
        print(f"🔄 備份資料庫到: {backup_path}")
        shutil.copy2(DUCKDB_PATH, backup_path)
        print("✅ 資料庫備份完成")
    else:
        print("⚠️ 資料庫檔案不存在")
    
    return backup_path

def check_file_exists(file_path: str) -> bool:
    """檢查檔案是否存在"""
    if not os.path.exists(file_path):
        print(f"❌ 檔案不存在: {file_path}")
        return False
    
    file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
    print(f"✅ 檔案存在: {file_path} ({file_size:.1f} MB)")
    return True

def verify_csv_structure(file_path: str, symbol: str) -> bool:
    """驗證 CSV 檔案結構"""
    try:
        print(f"🔍 驗證 {symbol} CSV 結構...")
        df_sample = pd.read_csv(file_path, nrows=10)
        
        print(f"📊 欄位: {list(df_sample.columns)}")
        print(f"📋 前3筆資料:")
        print(df_sample.head(3).to_string(index=False))
        
        # 檢查必要欄位
        required_columns = ["DateTime", "Bid", "Ask", "Volume"]
        missing_columns = [col for col in required_columns if col not in df_sample.columns]
        
        if missing_columns:
            print(f"❌ 缺少必要欄位: {missing_columns}")
            return False
        
        print("✅ CSV 結構驗證通過")
        return True
        
    except Exception as e:
        print(f"❌ CSV 結構驗證失敗: {e}")
        return False

def setup_database_schema(conn):
    """設定資料庫結構"""
    print("🔧 設定資料庫結構...")
    
    # 清空舊資料（保留結構）
    try:
        conn.execute("DELETE FROM candlestick_data_new")
        print("🗑️ 清空舊蠟燭圖資料")
    except:
        pass
    
    # 確保 symbols 表有正確的資料
    conn.execute("DELETE FROM symbols")
    for symbol in SYMBOLS_CONFIG.keys():
        conn.execute("INSERT INTO symbols (symbol) VALUES (?)", [symbol])
    
    # 確保 timeframes 表有正確的資料
    conn.execute("DELETE FROM timeframes") 
    for tf_code, tf_seconds in [
        ("M1", 60), ("M5", 300), ("M15", 900), ("M30", 1800),
        ("H1", 3600), ("H4", 14400), ("D1", 86400), ("W1", 604800), ("MN", 2592000)
    ]:
        conn.execute("INSERT INTO timeframes (code, seconds) VALUES (?, ?)", [tf_code, tf_seconds])
    
    # 重新創建 candlestick_data_new 表（如果需要）
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
            UNIQUE (symbol, timeframe, timestamp, data_version)
        )
    """)
    
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
    print("✅ 資料庫結構設定完成")

def process_tick_to_candlesticks(file_path: str, symbol: str, conn) -> Dict[str, int]:
    """處理 Tick 資料生成所有時間框架的蠟燭圖"""
    print(f"\n🚀 開始處理 {symbol} 的 Tick 資料...")
    
    total_processed = 0
    timeframe_counts = {tf: 0 for tf in TIMEFRAMES.keys()}
    
    try:
        # 讀取檔案總行數
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f) - 1  # 減去標題行
        
        print(f"📊 總計 {total_lines:,} 筆 Tick 資料")
        
        # 分批處理
        chunk_iter = pd.read_csv(file_path, chunksize=CHUNK_ROWS)
        chunk_num = 0
        
        for chunk in chunk_iter:
            chunk_num += 1
            chunk_start_time = time.time()
            
            print(f"\n📦 處理第 {chunk_num} 批次: {len(chunk):,} 筆")
            
            # 資料清理和轉換
            chunk = chunk.rename(columns={"DateTime": "timestamp"})
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
            chunk = chunk.dropna(subset=["timestamp"])
            chunk = chunk.sort_values("timestamp")
            
            # 計算 mid price 作為蠟燭圖資料
            chunk["price"] = (chunk["Bid"] + chunk["Ask"]) / 2
            chunk["volume"] = chunk.get("Volume", 1000000)  # 預設成交量
            
            # 設定索引為時間戳
            chunk.set_index("timestamp", inplace=True)
            
            # 為每個時間框架生成蠟燭圖資料
            for tf_code, tf_pandas in TIMEFRAMES.items():
                try:
                    # 重新取樣生成 OHLC 資料
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
                    
                    if len(candlestick_df) > 0:
                        # 準備插入資料
                        insert_data = []
                        current_time = datetime.now()
                        
                        for timestamp, row in candlestick_df.iterrows():
                            insert_data.append({
                                "symbol": symbol,
                                "timeframe": tf_code,
                                "timestamp": timestamp,
                                "open": round(float(row["open"]), 10),
                                "high": round(float(row["high"]), 10),
                                "low": round(float(row["low"]), 10),
                                "close": round(float(row["close"]), 10),
                                "volume": round(float(row["volume"]), 18),
                                "data_version": "v2.0",
                                "data_source": "batch_rebuild",
                                "created_at": current_time,
                                "updated_at": current_time
                            })
                        
                        # 分批插入避免記憶體問題
                        for i in range(0, len(insert_data), MAX_RECORDS_PER_INSERT):
                            batch = insert_data[i:i + MAX_RECORDS_PER_INSERT]
                            
                            # 使用 INSERT OR REPLACE 處理重複資料
                            for record in batch:
                                conn.execute("""
                                    INSERT OR REPLACE INTO candlestick_data_new 
                                    (symbol, timeframe, timestamp, open, high, low, close, volume, 
                                     data_version, data_source, created_at, updated_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, [
                                    record["symbol"], record["timeframe"], record["timestamp"],
                                    record["open"], record["high"], record["low"], record["close"],
                                    record["volume"], record["data_version"], record["data_source"],
                                    record["created_at"], record["updated_at"]
                                ])
                            
                            timeframe_counts[tf_code] += len(batch)
                        
                        print(f"   {tf_code}: +{len(insert_data):,} 根")
                        
                except Exception as e:
                    print(f"   ❌ {tf_code} 處理失敗: {e}")
            
            total_processed += len(chunk)
            chunk_elapsed = time.time() - chunk_start_time
            
            # 定期提交
            if chunk_num % 3 == 0:
                conn.commit()
                print(f"💾 已提交第 {chunk_num} 批次")
            
            progress_pct = (total_processed / total_lines) * 100
            print(f"📈 進度: {total_processed:,}/{total_lines:,} ({progress_pct:.1f}%) - 耗時: {chunk_elapsed:.1f}s")
            
        # 最終提交
        conn.commit()
        print(f"✅ {symbol} 處理完成！")
        
        return timeframe_counts
        
    except Exception as e:
        print(f"❌ 處理 {symbol} 時發生錯誤: {e}")
        return timeframe_counts

def verify_data_integrity(conn, symbol: str) -> bool:
    """驗證資料完整性"""
    print(f"\n🔍 驗證 {symbol} 資料完整性...")
    
    try:
        # 檢查每個時間框架的資料筆數
        for tf_code in TIMEFRAMES.keys():
            result = conn.execute("""
                SELECT COUNT(*) FROM candlestick_data_new 
                WHERE symbol = ? AND timeframe = ?
            """, [symbol, tf_code]).fetchone()
            
            count = result[0] if result else 0
            print(f"   {tf_code}: {count:,} 根")
        
        # 檢查資料範圍
        result = conn.execute("""
            SELECT MIN(timestamp) as start_time, MAX(timestamp) as end_time
            FROM candlestick_data_new 
            WHERE symbol = ?
        """, [symbol]).fetchone()
        
        if result and result[0]:
            print(f"   時間範圍: {result[0]} 到 {result[1]}")
        
        # 檢查資料品質
        result = conn.execute("""
            SELECT COUNT(*) FROM candlestick_data_new 
            WHERE symbol = ? AND (
                open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR
                high < GREATEST(open, close) OR low > LEAST(open, close)
            )
        """, [symbol]).fetchone()
        
        invalid_count = result[0] if result else 0
        if invalid_count > 0:
            print(f"   ⚠️ 發現 {invalid_count} 筆無效資料")
            return False
        else:
            print("   ✅ 資料品質檢查通過")
            return True
        
    except Exception as e:
        print(f"   ❌ 資料完整性驗證失敗: {e}")
        return False

def main():
    """主函數"""
    clear_screen()
    print("🚀 分批重建所有時間框架蠟燭圖資料")
    print("=" * 60)
    
    # 檢查檔案
    print("\n📁 檢查原始 Tick 檔案...")
    all_files_exist = True
    for symbol, config in SYMBOLS_CONFIG.items():
        print(f"\n🔸 {config['name']} ({symbol}):")
        file_exists = check_file_exists(config['tick_file'])
        if file_exists:
            csv_valid = verify_csv_structure(config['tick_file'], symbol)
            if not csv_valid:
                all_files_exist = False
        else:
            all_files_exist = False
    
    if not all_files_exist:
        print("\n❌ 部分檔案不存在或格式錯誤，請檢查檔案路徑")
        return
    
    # 備份資料庫
    print(f"\n💾 備份資料庫...")
    backup_path = backup_database()
    
    # 連接資料庫
    print(f"\n🔌 連接資料庫...")
    conn = duckdb.connect(DUCKDB_PATH)
    
    # 設定資料庫結構
    setup_database_schema(conn)
    
    # 處理每個商品
    total_start_time = time.time()
    all_results = {}
    
    for symbol, config in SYMBOLS_CONFIG.items():
        print(f"\n{'='*60}")
        print(f"🎯 處理 {config['name']} ({symbol})")
        print(f"{'='*60}")
        
        symbol_start_time = time.time()
        
        # 處理 Tick 資料生成蠟燭圖
        timeframe_counts = process_tick_to_candlesticks(
            config['tick_file'], symbol, conn
        )
        
        # 驗證資料完整性
        data_valid = verify_data_integrity(conn, symbol)
        
        symbol_elapsed = time.time() - symbol_start_time
        print(f"⏱️ {symbol} 處理耗時: {symbol_elapsed/60:.1f} 分鐘")
        
        all_results[symbol] = {
            "timeframe_counts": timeframe_counts,
            "data_valid": data_valid,
            "elapsed_time": symbol_elapsed
        }
    
    # 關閉資料庫連接
    conn.close()
    
    # 顯示總結報告
    total_elapsed = time.time() - total_start_time
    print(f"\n{'='*60}")
    print("📊 處理完成總結")
    print(f"{'='*60}")
    print(f"⏱️ 總耗時: {total_elapsed/60:.1f} 分鐘")
    print(f"💾 備份檔案: {backup_path}")
    
    print(f"\n📈 各商品處理結果:")
    for symbol, results in all_results.items():
        config = SYMBOLS_CONFIG[symbol]
        print(f"\n🔸 {config['name']} ({symbol}):")
        print(f"   狀態: {'✅ 成功' if results['data_valid'] else '❌ 失敗'}")
        print(f"   耗時: {results['elapsed_time']/60:.1f} 分鐘")
        
        for tf_code, count in results['timeframe_counts'].items():
            if count > 0:
                print(f"   {tf_code}: {count:,} 根")
    
    print(f"\n🎉 所有商品處理完成！")
    print(f"💡 您現在可以在前端查看所有時間框架的蠟燭圖資料")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
最終版完整重建所有商品的所有時間框架蠟燭圖資料
修正 INSERT OR REPLACE 問題
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
CHUNK_ROWS = 100_000  # 減少批次大小避免記憶體問題

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
    "D1": "1D"
}

def clear_screen():
    """清屏"""
    os.system('cls' if os.name == 'nt' else 'clear')

def backup_database():
    """備份資料庫"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"database/backup_final_{timestamp}.duckdb"
    
    if os.path.exists(DUCKDB_PATH):
        print(f"🔄 備份資料庫到: {backup_path}")
        shutil.copy2(DUCKDB_PATH, backup_path)
        print("✅ 資料庫備份完成")
    return backup_path

def setup_database_schema(conn):
    """設定資料庫結構"""
    print("🔧 設定資料庫結構...")
    
    # 清空舊的重建資料
    conn.execute("DELETE FROM candlestick_data_new WHERE data_source = 'final_rebuild'")
    
    # 確保 symbols 表有正確的資料
    for symbol in SYMBOLS_CONFIG.keys():
        conn.execute("INSERT OR IGNORE INTO symbols (symbol) VALUES (?)", [symbol])
    
    # 確保 timeframes 表有正確的資料
    for tf_code, tf_seconds in [
        ("M1", 60), ("M5", 300), ("M15", 900), ("M30", 1800),
        ("H1", 3600), ("H4", 14400), ("D1", 86400)
    ]:
        conn.execute("INSERT OR IGNORE INTO timeframes (code, seconds) VALUES (?, ?)", [tf_code, tf_seconds])
    
    conn.commit()
    print("✅ 資料庫結構設定完成")

def insert_candlestick_batch(conn, records, symbol, timeframe):
    """安全地批次插入蠟燭圖資料"""
    if not records:
        return 0
    
    inserted_count = 0
    
    for record in records:
        try:
            # 先檢查是否已存在
            existing = conn.execute("""
                SELECT id FROM candlestick_data_new 
                WHERE symbol = ? AND timeframe = ? AND timestamp = ? AND data_version = ?
            """, [record['symbol'], record['timeframe'], record['timestamp'], record['data_version']]).fetchone()
            
            if existing:
                # 更新現有記錄
                conn.execute("""
                    UPDATE candlestick_data_new 
                    SET open = ?, high = ?, low = ?, close = ?, volume = ?, 
                        data_source = ?, updated_at = now()
                    WHERE symbol = ? AND timeframe = ? AND timestamp = ? AND data_version = ?
                """, [
                    record['open'], record['high'], record['low'], record['close'], record['volume'],
                    record['data_source'], record['symbol'], record['timeframe'], 
                    record['timestamp'], record['data_version']
                ])
            else:
                # 插入新記錄
                conn.execute("""
                    INSERT INTO candlestick_data_new 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, data_version, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    record['symbol'], record['timeframe'], record['timestamp'],
                    record['open'], record['high'], record['low'], record['close'], record['volume'],
                    record['data_version'], record['data_source']
                ])
            
            inserted_count += 1
            
        except Exception as e:
            print(f"     插入錯誤: {e}")
            continue
    
    return inserted_count

def process_symbol_batch(symbol, tick_file, conn):
    """分批處理單個商品的資料"""
    print(f"\n{'='*60}")
    print(f"🎯 處理 {SYMBOLS_CONFIG[symbol]['name']} ({symbol})")
    print(f"{'='*60}")
    
    if not os.path.exists(tick_file):
        print(f"❌ 檔案不存在: {tick_file}")
        return {}
    
    # 獲取檔案大小
    file_size = os.path.getsize(tick_file) / 1024 / 1024  # MB
    print(f"📁 檔案大小: {file_size:.1f} MB")
    
    # 清空該商品的舊資料
    conn.execute("DELETE FROM candlestick_data_new WHERE symbol = ? AND data_source = 'final_rebuild'", [symbol])
    
    try:
        # 獲取總行數
        with open(tick_file, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f) - 1  # 減去標題行
        
        print(f"📊 總計 {total_lines:,} 筆 Tick 資料")
        
        # 分批處理
        chunk_iter = pd.read_csv(tick_file, chunksize=CHUNK_ROWS)
        chunk_num = 0
        total_processed = 0
        timeframe_counts = {tf: 0 for tf in TIMEFRAMES.keys()}
        
        start_time = time.time()
        
        for chunk in chunk_iter:
            chunk_num += 1
            chunk_start_time = time.time()
            
            print(f"\n📦 處理第 {chunk_num} 批次: {len(chunk):,} 筆")
            
            # 資料清理和轉換
            chunk = chunk.rename(columns={"DateTime": "timestamp"})
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
            chunk = chunk.dropna(subset=["timestamp"])
            chunk = chunk.sort_values("timestamp")
            
            if len(chunk) == 0:
                print("   ⚠️ 批次資料為空，跳過")
                continue
            
            print(f"   時間範圍: {chunk['timestamp'].min()} 到 {chunk['timestamp'].max()}")
            
            # 計算中價
            chunk["price"] = (chunk["Bid"] + chunk["Ask"]) / 2
            chunk["volume"] = chunk.get("Volume", 1000000)
            
            # 設定索引
            chunk.set_index("timestamp", inplace=True)
            
            # 為每個時間框架生成蠟燭圖資料
            batch_inserted = 0
            
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
                                'data_source': 'final_rebuild'
                            })
                        
                        # 安全地批次插入
                        count = insert_candlestick_batch(conn, records, symbol, tf_code)
                        timeframe_counts[tf_code] += count
                        batch_inserted += count
                        
                        print(f"     {tf_code}: +{count} 根")
                        
                except Exception as e:
                    print(f"     ❌ {tf_code} 處理失敗: {e}")
            
            total_processed += len(chunk)
            chunk_elapsed = time.time() - chunk_start_time
            
            # 定期提交
            if chunk_num % 5 == 0:
                conn.commit()
                print(f"💾 已提交第 {chunk_num} 批次")
            
            progress_pct = (total_processed / total_lines) * 100
            elapsed_total = time.time() - start_time
            print(f"📈 進度: {total_processed:,}/{total_lines:,} ({progress_pct:.1f}%)")
            print(f"⏱️ 批次耗時: {chunk_elapsed:.1f}s | 總耗時: {elapsed_total/60:.1f}min")
            
            # 每10批次顯示當前統計
            if chunk_num % 10 == 0:
                print(f"\n📊 目前 {symbol} 各時間框架統計:")
                for tf_code, count in timeframe_counts.items():
                    if count > 0:
                        print(f"     {tf_code}: {count:,} 根")
        
        # 最終提交
        conn.commit()
        
        total_elapsed = time.time() - start_time
        print(f"\n✅ {symbol} 處理完成！")
        print(f"⏱️ 總耗時: {total_elapsed/60:.1f} 分鐘")
        
        # 顯示結果統計
        print(f"\n📊 {symbol} 最終各時間框架統計:")
        total_candles = 0
        for tf_code, count in timeframe_counts.items():
            if count > 0:
                print(f"   {tf_code}: {count:,} 根")
                total_candles += count
        print(f"   總計: {total_candles:,} 根蠟燭")
        
        return timeframe_counts
        
    except Exception as e:
        print(f"❌ 處理 {symbol} 時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return {}

def verify_final_results(conn):
    """驗證最終結果"""
    print(f"\n{'='*60}")
    print("🔍 最終結果驗證")
    print(f"{'='*60}")
    
    for symbol, config in SYMBOLS_CONFIG.items():
        print(f"\n🔸 {config['name']} ({symbol}):")
        
        total_symbol_candles = 0
        for tf_code in TIMEFRAMES.keys():
            result = conn.execute("""
                SELECT COUNT(*) FROM candlestick_data_new 
                WHERE symbol = ? AND timeframe = ? AND data_source = 'final_rebuild'
            """, [symbol, tf_code]).fetchone()
            
            count = result[0] if result else 0
            if count > 0:
                print(f"   {tf_code}: {count:,} 根")
                total_symbol_candles += count
            else:
                print(f"   {tf_code}: 無資料")
        
        print(f"   小計: {total_symbol_candles:,} 根")
    
    # 總計統計
    result = conn.execute("""
        SELECT COUNT(*) FROM candlestick_data_new 
        WHERE data_source = 'final_rebuild'
    """).fetchone()
    
    total_all = result[0] if result else 0
    print(f"\n🎯 總計: {total_all:,} 根蠟燭")

def main():
    """主函數"""
    clear_screen()
    print("🚀 最終版完整重建所有商品的所有時間框架蠟燭圖資料")
    print("=" * 60)
    
    # 檢查所有檔案
    print("\n📁 檢查原始 Tick 檔案...")
    all_files_exist = True
    for symbol, config in SYMBOLS_CONFIG.items():
        if os.path.exists(config['tick_file']):
            file_size = os.path.getsize(config['tick_file']) / 1024 / 1024
            print(f"✅ {config['name']} ({symbol}): {file_size:.1f} MB")
        else:
            print(f"❌ {config['name']} ({symbol}): 檔案不存在")
            all_files_exist = False
    
    if not all_files_exist:
        print("\n❌ 部分檔案不存在，請檢查檔案路徑")
        return
    
    # 備份資料庫
    backup_path = backup_database()
    
    # 連接資料庫
    print("\n🔌 連接資料庫...")
    conn = duckdb.connect(DUCKDB_PATH)
    
    try:
        # 設定資料庫結構
        setup_database_schema(conn)
        
        # 處理每個商品
        total_start_time = time.time()
        all_results = {}
        
        for symbol, config in SYMBOLS_CONFIG.items():
            print(f"\n⏰ 開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 處理商品
            results = process_symbol_batch(symbol, config['tick_file'], conn)
            all_results[symbol] = results
        
        # 驗證最終結果
        verify_final_results(conn)
        
        # 計算總耗時
        total_elapsed = time.time() - total_start_time
        print(f"\n🎉 所有商品處理完成！")
        print(f"⏱️ 總耗時: {total_elapsed/60:.1f} 分鐘")
        print(f"💾 備份檔案: {backup_path}")
        
        print(f"\n💡 現在您可以在前端查看所有時間框架的完整蠟燭圖資料了！")
        
    except Exception as e:
        print(f"❌ 主程序失敗: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
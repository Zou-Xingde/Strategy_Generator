#!/usr/bin/env python3
"""
分批處理版本 - 更穩定的大檔案處理
"""
import os
import sys
import gc
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd

# -------- 路徑設定 --------
TICK_FILES = {
    "XAUUSD": r"D:\project\策略產生器\data\Strategy_GeneratorXAUUSD_dukascopy_TICK_UTC-TICK-No Session.csv",
    "US30":   r"D:\project\策略產生器\data\Strategy_GeneratorUSA30IDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv",
    "US100":  r"D:\project\策略產生器\data\Strategy_GeneratorUSATECHIDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv",
}

DB_PATH = Path("database/market_data.duckdb")

TIMEFRAMES = {
    "M1": "1min",
    "M5": "5min", 
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D",
}

CHUNK_ROWS = 500_000    # 進一步減少批次大小
MAX_RECORDS_PER_INSERT = 10_000  # 每次 INSERT 最大記錄數

def check_progress(conn):
    """檢查目前進度"""
    try:
        result = conn.execute("""
            SELECT symbol, COUNT(*) as total_candles 
            FROM candlestick_data_new 
            GROUP BY symbol 
            ORDER BY symbol
        """).fetchall()
        
        print("📊 目前進度:")
        total = 0
        for symbol, count in result:
            print(f"  {symbol}: {count:,} 根 K 線")
            total += count
        print(f"  總計: {total:,} 根 K 線\n")
        
        return {row[0]: row[1] for row in result}
    except Exception as e:
        print(f"檢查進度時出錯: {e}")
        return {}

def process_timeframe_batch(chunk, symbol, timeframe, freq, conn, id_counter):
    """處理單一時間框架的批次資料"""
    try:
        ohlcv = chunk["price"].resample(freq, label="left", closed="left").ohlc()
        vol = chunk["Volume"].resample(freq, label="left", closed="left").sum()
        df_tf = pd.concat([ohlcv, vol], axis=1).dropna()
        df_tf.reset_index(inplace=True)
        
        if df_tf.empty:
            return id_counter, 0
        
        # 分批插入以避免記憶體問題
        total_inserted = 0
        for i in range(0, len(df_tf), MAX_RECORDS_PER_INSERT):
            batch = df_tf.iloc[i:i+MAX_RECORDS_PER_INSERT]
            records = []
            
            for _, row in batch.iterrows():
                id_counter += 1
                records.append((
                    id_counter, symbol, timeframe, row["timestamp"],
                    round(row["open"], 10), round(row["high"], 10),
                    round(row["low"], 10), round(row["close"], 10),
                    round(row["Volume"], 18), "v1.0", "rebuild",
                    datetime.now(), datetime.now(),
                ))
            
            if records:
                conn.executemany(
                    """INSERT INTO candlestick_data_new
                    (id,symbol,timeframe,timestamp,open,high,low,close,volume,data_version,data_source,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    records
                )
                total_inserted += len(records)
        
        return id_counter, total_inserted
        
    except Exception as e:
        print(f"    ❌ 處理 {timeframe} 時出錯: {e}")
        return id_counter, 0

def process_tick_file_stable(symbol: str, path: str, conn: duckdb.DuckDBPyConnection):
    """穩定版本的 Tick 檔案處理"""
    print(f"\n🚀 處理 {symbol} Tick -> Candles ...")
    
    # 檢查該 symbol 是否已有足夠資料
    existing = conn.execute("SELECT COUNT(*) FROM candlestick_data_new WHERE symbol = ?", [symbol]).fetchone()[0]
    if existing > 100_000:  # 如果已有超過 10 萬根 K 線，認為已完成
        print(f"  ✅ {symbol} 已有 {existing:,} 根 K 線，跳過")
        return
    
    try:
        # 獲取檔案總行數（估算）
        with open(path, 'r') as f:
            total_lines = sum(1 for _ in f)
        print(f"  📄 檔案總行數約: {total_lines:,}")
        
        id_counter = conn.execute("SELECT COALESCE(MAX(id),0) FROM candlestick_data_new").fetchone()[0]
        chunk_count = 0
        total_candles = 0
        
        # 開始交易
        conn.execute("BEGIN TRANSACTION")
        
        for chunk in pd.read_csv(path, chunksize=CHUNK_ROWS):
            chunk_count += 1
            progress_pct = (chunk_count * CHUNK_ROWS / total_lines) * 100
            print(f"  📊 第 {chunk_count} 批 ({len(chunk):,} ticks, {progress_pct:.1f}%)")
            
            # 資料預處理
            chunk.rename(columns={"DateTime": "timestamp"}, inplace=True)
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], utc=True)
            chunk["price"] = (chunk["Bid"] + chunk["Ask"]) / 2
            chunk.set_index("timestamp", inplace=True)
            
            # 處理各時間框架
            batch_total = 0
            for tf, freq in TIMEFRAMES.items():
                id_counter, inserted = process_timeframe_batch(
                    chunk, symbol, tf, freq, conn, id_counter
                )
                batch_total += inserted
                if inserted > 0:
                    print(f"    ↳ {tf}: {inserted:,} 根")
            
            total_candles += batch_total
            print(f"    💾 批次總計: {batch_total:,} 根 (累計: {total_candles:,})")
            
            # 每 3 批提交一次
            if chunk_count % 3 == 0:
                conn.execute("COMMIT")
                conn.execute("BEGIN TRANSACTION")
                print(f"    ✅ 已提交前 {chunk_count} 批")
                
                # 強制記憶體清理
                del chunk
                gc.collect()
            
            # 每 10 批檢查一次進度
            if chunk_count % 10 == 0:
                current_total = conn.execute("SELECT COUNT(*) FROM candlestick_data_new WHERE symbol = ?", [symbol]).fetchone()[0]
                print(f"    📈 {symbol} 目前總計: {current_total:,} 根 K 線")
        
        # 最終提交
        conn.execute("COMMIT")
        print(f"✅ {symbol} 完成！總共寫入 {total_candles:,} 根 K 線")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  {symbol} 處理被中斷")
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        raise
    except Exception as e:
        print(f"\n❌ 處理 {symbol} 時發生錯誤: {e}")
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        raise

def main():
    if not DB_PATH.exists():
        print(f"找不到資料庫 {DB_PATH}")
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # 檢查目前進度
        progress = check_progress(conn)
        
        # 處理每個商品
        for sym, fpath in TICK_FILES.items():
            if not os.path.isfile(fpath):
                print(f"⚠️  找不到 {sym} Tick 檔案，跳過")
                continue
                
            try:
                process_tick_file_stable(sym, fpath, conn)
            except KeyboardInterrupt:
                print(f"\n⚠️  {sym} 處理被中斷，可稍後繼續")
                break
            except Exception as e:
                print(f"❌ {sym} 處理失敗: {e}")
                continue

        # 最終狀態檢查
        print("\n" + "="*50)
        check_progress(conn)
        print("🎉 批次處理完成！")
        
    finally:
        conn.close()
        print("🔒 資料庫連線已關閉")

if __name__ == "__main__":
    main() 
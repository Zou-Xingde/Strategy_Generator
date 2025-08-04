#!/usr/bin/env python3
"""
續行版本的重建腳本 - 可以從中斷處繼續
"""
import os
import sys
import shutil
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

CHUNK_ROWS = 1_000_000  # 減少每批大小以提升穩定性

def check_progress(conn):
    """檢查目前進度"""
    result = conn.execute("""
        SELECT symbol, COUNT(*) as total_candles 
        FROM candlestick_data_new 
        GROUP BY symbol 
        ORDER BY symbol
    """).fetchall()
    
    print("📊 目前進度:")
    for symbol, count in result:
        print(f"  {symbol}: {count:,} 根 K 線")
    
    return {row[0]: row[1] for row in result}

def process_tick_file_continue(symbol: str, path: str, conn: duckdb.DuckDBPyConnection):
    """繼續處理 Tick CSV"""
    print(f"\n🚀 處理 {symbol} Tick -> Candles ...")
    
    # 檢查該 symbol 是否已有資料
    existing = conn.execute("SELECT COUNT(*) FROM candlestick_data_new WHERE symbol = ?", [symbol]).fetchone()[0]
    if existing > 0:
        print(f"  ℹ️  {symbol} 已有 {existing:,} 根 K 線，跳過")
        return
    
    idx_start = conn.execute("SELECT COALESCE(MAX(id),0) FROM candlestick_data_new").fetchone()[0]
    id_counter = idx_start
    chunk_count = 0
    total_candles = 0

    try:
        for chunk in pd.read_csv(path, chunksize=CHUNK_ROWS):
            chunk_count += 1
            print(f"  📊 處理第 {chunk_count} 批 ({len(chunk):,} 筆 ticks)...")
            
            # CSV 具有: DateTime,Bid,Ask,Volume
            chunk.rename(columns={"DateTime": "timestamp"}, inplace=True)
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], utc=True)
            chunk["price"] = (chunk["Bid"] + chunk["Ask"]) / 2
            chunk.set_index("timestamp", inplace=True)

            batch_candles = 0
            for tf, freq in TIMEFRAMES.items():
                ohlcv = chunk["price"].resample(freq, label="left", closed="left").ohlc()
                vol   = chunk["Volume"].resample(freq, label="left", closed="left").sum()
                df_tf = pd.concat([ohlcv, vol], axis=1).dropna()
                df_tf.reset_index(inplace=True)
                
                if df_tf.empty:
                    continue

                records = []
                for _, row in df_tf.iterrows():
                    id_counter += 1
                    records.append(
                        (
                            id_counter, symbol, tf, row["timestamp"],
                            round(row["open"], 10), round(row["high"], 10),
                            round(row["low"], 10), round(row["close"], 10),
                            round(row["Volume"], 18), "v1.0", "rebuild",
                            datetime.now(), datetime.now(),
                        )
                    )
                
                if records:
                    conn.executemany(
                        """INSERT INTO candlestick_data_new
                        (id,symbol,timeframe,timestamp,open,high,low,close,volume,data_version,data_source,created_at,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        records,
                    )
                    batch_candles += len(records)
            
            total_candles += batch_candles
            print(f"    ↳ 批次寫入 {batch_candles:,} 根 K 線 (累計: {total_candles:,})")
            
            # 每 5 批 commit 一次以提升穩定性
            if chunk_count % 5 == 0:
                conn.commit()
                print(f"    💾 已提交至第 {chunk_count} 批")

    except KeyboardInterrupt:
        print(f"\n⚠️  處理被中斷，{symbol} 已處理 {total_candles:,} 根 K 線")
        conn.commit()
        raise
    except Exception as e:
        print(f"\n❌ 處理 {symbol} 時發生錯誤: {e}")
        conn.rollback()
        raise

    print(f"✅ {symbol} 完成，總共寫入 {total_candles:,} 根 K 線")

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
                print(f"⚠️  找不到 {sym} Tick 檔案: {fpath}，跳過")
                continue
                
            if sym in progress:
                print(f"✅ {sym} 已完成 ({progress[sym]:,} 根 K 線)")
                continue
                
            try:
                process_tick_file_continue(sym, fpath, conn)
            except KeyboardInterrupt:
                print(f"\n⚠️  {sym} 處理被中斷，可稍後繼續")
                break
            except Exception as e:
                print(f"❌ {sym} 處理失敗: {e}")
                continue

        print("\n🎉 重建腳本執行完成！")
        
    finally:
        conn.close()
        print("🔒 資料庫連線已關閉")

if __name__ == "__main__":
    main() 
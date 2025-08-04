#!/usr/bin/env python3
"""
rebuild_database.py
-------------------
1. 連接 DuckDB，備份舊資料庫檔。
2. 清空所有事務性資料表 (candlestick/tick/swing/statistics)。
3. 重新匯入三個商品 Tick CSV，計算 M1~D1 多週期 K 線，寫入 candlestick_data_new。
   (演示: 只計算 M1, M5, M15, M30, H1, H4, D1，若要更多可自行擴充)
4. 更新 symbols 表。
5. 重建 v_candlestick_latest 視圖。

注意: 此腳本示範流程，巨量 Tick CSV 可能需要調整 chunksize 與記憶體。
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
BACKUP_DIR = Path("database")
BACKUP_DIR.mkdir(exist_ok=True)

TIMEFRAMES = {
    "M1": "1min",
    "M5": "5min",
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D",
}

CHUNK_ROWS = 2_000_000  # 每次讀取 Tick 行數，可依硬體調大或調小

# -------- 輔助函式 --------

def backup_db():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"market_data_rebuild_backup_{ts}.duckdb"
    shutil.copy2(DB_PATH, backup_path)
    print(f"✅ 資料庫備份至 {backup_path}")


def init_schema(conn: duckdb.DuckDBPyConnection):
    """刪除舊資料並確保表存在 (不改欄位型別)"""
    tables_to_truncate = [
        "candlestick_data",
        "candlestick_data_new",
        "tick_data",
        "swing_data",
        "algorithm_statistics",
    ]
    for tbl in tables_to_truncate:
        try:
            conn.execute(f"DELETE FROM {tbl}")
            print(f"🗑️  清空 {tbl}")
        except duckdb.CatalogException:
            # 表不存在，跳過
            pass

    # symbols & timeframes (維度)
    conn.execute("DELETE FROM symbols")
    for sym in TICK_FILES.keys():
        conn.execute("INSERT INTO symbols(symbol) VALUES (?)", [sym])
    print("✅ symbols 更新完成")

    # timeframes 若不存在則新增
    for tf, freq in TIMEFRAMES.items():
        conn.execute(
            "INSERT OR REPLACE INTO timeframes(code, seconds) VALUES (?, ?)",
            [tf, pd.Timedelta(freq).total_seconds()],
        )
    print("✅ timeframes 更新完成")

    # candlestick_data_new 若不存在則創建 (簡化版 DDL)
    conn.execute(
        """
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
          UNIQUE(symbol,timeframe,timestamp,data_version)
        )
        """
    )

    # 重新建立視圖 v_candlestick_latest
    conn.execute("DROP VIEW IF EXISTS v_candlestick_latest")
    conn.execute(
        """
        CREATE VIEW v_candlestick_latest AS
        SELECT * EXCLUDE rn
        FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol,timeframe,timestamp ORDER BY data_version DESC, updated_at DESC) AS rn
          FROM candlestick_data_new
        ) WHERE rn=1;
        """
    )
    print("✅ schema 初始化完成")


def process_tick_file(symbol: str, path: str, conn: duckdb.DuckDBPyConnection):
    """讀取 Tick CSV，分批計算 K 線並寫入 DB"""
    print(f"\n🚀 處理 {symbol} Tick -> Candles ...")
    idx_start = conn.execute("SELECT COALESCE(MAX(id),0) FROM candlestick_data_new").fetchone()[0]
    id_counter = idx_start
    chunk_count = 0

    for chunk in pd.read_csv(path, chunksize=CHUNK_ROWS):
        chunk_count += 1
        print(f"  📊 處理第 {chunk_count} 批 ({len(chunk)} 筆 ticks)...")
        
        # CSV 具有: DateTime,Bid,Ask,Volume
        chunk.rename(columns={"DateTime": "timestamp"}, inplace=True)
        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], utc=True)
        # 轉 mid price
        chunk["price"] = (chunk["Bid"] + chunk["Ask"]) / 2
        chunk.set_index("timestamp", inplace=True)

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
                        id_counter,
                        symbol,
                        tf,
                        row["timestamp"],
                        round(row["open"], 10),
                        round(row["high"], 10),
                        round(row["low"], 10),
                        round(row["close"], 10),
                        round(row["Volume"], 18),
                        "v1.0",
                        "rebuild",
                        datetime.now(),
                        datetime.now(),
                    )
                )
            # 批次插入
            if records:
                conn.executemany(
                    """
                    INSERT INTO candlestick_data_new
                    (id,symbol,timeframe,timestamp,open,high,low,close,volume,data_version,data_source,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    records,
                )
                print(f"    ↳ {tf}: 寫入 {len(records)} 根 K 線")

    print(f"✅ {symbol} 完成，總共寫入 {id_counter - idx_start} 根 K 線")


def main():
    if not DB_PATH.exists():
        print(f"找不到資料庫 {DB_PATH}")
        sys.exit(1)

    backup_db()

    conn = duckdb.connect(DB_PATH.as_posix())
    try:
        init_schema(conn)
        for sym, fpath in TICK_FILES.items():
            if not os.path.isfile(fpath):
                print(f"⚠️  找不到 {sym} Tick 檔案: {fpath}，跳過")
                continue
            process_tick_file(sym, fpath, conn)

        print("\n🎉 全部商品 K 線重建完成！")
    finally:
        conn.close()
        print("🔒 資料庫連線已關閉")

if __name__ == "__main__":
    main() 
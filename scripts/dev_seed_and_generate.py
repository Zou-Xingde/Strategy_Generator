#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dev helper: seed synthetic candlestick data into DuckDB and run ZigZag swing generation.
This verifies the backend can generate and persist swing_data without relying on external CSVs.
"""

# --- bootstrap sys.path for src imports ---
import sys as _sys
import pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path.insert(0, str(_ROOT))            # <repo_root>
_sys.path.insert(0, str(_ROOT / "src"))    # <repo_root>/src
# -----------------------------------------

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from typing import Tuple

from config.settings import DUCKDB_PATH
from src.database.connection import duckdb
from src.data_processing.swing_processor import SwingProcessor

SYMBOL = "XAUUSD"
TIMEFRAME = "D1"
N = 400  # bars
START = datetime(2022, 1, 1)


def make_synthetic_ohlcv(n: int) -> pd.DataFrame:
    rng = pd.date_range(START, periods=n, freq="1D")
    # create a trending + oscillating series
    np.random.seed(42)
    trend = np.linspace(0, 300, n)
    noise = np.random.normal(0, 8, n)
    wave = 120 * np.sin(np.linspace(0, 8 * np.pi, n))
    base = 1900 + trend + wave + noise

    # build OHLCV around base
    close = base
    open_ = close + np.random.normal(0, 3, n)
    high = np.maximum(open_, close) + np.abs(np.random.normal(2, 2, n))
    low = np.minimum(open_, close) - np.abs(np.random.normal(2, 2, n))
    vol = (np.abs(np.random.normal(1000, 250, n)) + 100).astype(int)

    df = pd.DataFrame({
        "timestamp": rng,
        "open": open_.round(2),
        "high": high.round(2),
        "low": low.round(2),
        "close": close.round(2),
        "volume": vol,
    })
    return df


def seed_candles(df: pd.DataFrame, symbol: str, timeframe: str) -> Tuple[int, int]:
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        # ensure table exists by invoking DB setup via connection class indirectly
        # explicit cleanup existing rows for this pair
        con.execute(
            "DELETE FROM candlestick_data WHERE symbol = ? AND timeframe = ?",
            [symbol, timeframe],
        )
        con.register("df", df)
        con.execute(
            """
            INSERT INTO candlestick_data (symbol, timeframe, timestamp, open, high, low, close, volume)
            SELECT ?, ?, timestamp, open, high, low, close, volume FROM df
            """,
            [symbol, timeframe],
        )
        # count rows
        total = con.execute(
            "SELECT COUNT(*) FROM candlestick_data WHERE symbol = ? AND timeframe = ?",
            [symbol, timeframe],
        ).fetchone()[0]
        return len(df), int(total)
    finally:
        con.close()


def main() -> int:
    print(f"DB: {DUCKDB_PATH}")
    print("1) Seeding synthetic D1 candles...")
    df = make_synthetic_ohlcv(N)
    inserted, total = seed_candles(df, SYMBOL, TIMEFRAME)
    print(f"   inserted: {inserted}, total now: {total}")

    print("2) Running ZigZag swing generation via SwingProcessor...")
    proc = SwingProcessor(str(DUCKDB_PATH))
    result = proc.process_symbol_timeframe(symbol=SYMBOL, timeframe=TIMEFRAME, algorithm_name="zigzag", batch_size=50000)
    print("   result:", result)

    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        cnt = con.execute(
            "SELECT COUNT(*) FROM swing_data WHERE symbol = ? AND timeframe = ? AND algorithm_name = 'zigzag'",
            [SYMBOL, TIMEFRAME],
        ).fetchone()[0]
        print(f"3) swing_data rows for {SYMBOL} {TIMEFRAME} (zigzag):", cnt)
        head = con.execute(
            """
            SELECT timestamp, zigzag_price, zigzag_type, swing_direction
            FROM swing_data
            WHERE symbol = ? AND timeframe = ? AND algorithm_name = 'zigzag'
            ORDER BY timestamp
            LIMIT 5
            """,
            [SYMBOL, TIMEFRAME],
        ).fetchall()
        print("   sample:", head)
    finally:
        con.close()

    print("✓ Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

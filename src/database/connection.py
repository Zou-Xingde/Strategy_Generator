import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict
import logging

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBConnection:
    """DuckDB資料庫連接管理器"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.connect()
    
    def connect(self):
        """建立資料庫連接"""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Successfully connected to DuckDB at {self.db_path}")
            self.setup_database()
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise
    
    def setup_database(self):
        """設定資料庫結構"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            # 創建蠟燭圖資料表
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS candlestick_data (
                    symbol VARCHAR(20),
                    timeframe VARCHAR(5),
                    timestamp TIMESTAMP,
                    open DECIMAL(10, 5),
                    high DECIMAL(10, 5),
                    low DECIMAL(10, 5),
                    close DECIMAL(10, 5),
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 創建Tick資料表
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tick_data (
                    id INTEGER PRIMARY KEY,
                    symbol VARCHAR(20),
                    timestamp TIMESTAMP,
                    bid DECIMAL(10, 5),
                    ask DECIMAL(10, 5),
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 創建索引以提高查詢效率
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_candlestick_symbol_timeframe_timestamp 
                ON candlestick_data(symbol, timeframe, timestamp)
            """)
            
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tick_symbol_timestamp 
                ON tick_data(symbol, timestamp)
            """)
            
            logger.info("Database setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            raise
    
    def insert_candlestick_data(self, df: pd.DataFrame, timeframe: str, symbol: str):
        """插入蠟燭圖資料"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            # 添加必要的欄位
            df = df.copy()
            df['symbol'] = symbol
            df['timeframe'] = timeframe
            
            # 確保時間戳欄位正確
            if 'timestamp' not in df.columns:
                df['timestamp'] = df.index
            
            # 重置索引以確保有正確的行號
            df = df.reset_index(drop=True)
            
            # 批次插入資料
            self.conn.execute("""
                INSERT INTO candlestick_data 
                (symbol, timeframe, timestamp, open, high, low, close, volume)
                SELECT symbol, timeframe, timestamp, open, high, low, close, volume
                FROM df
            """)
            
            logger.info(f"Inserted {len(df)} {timeframe} candlestick records for {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to insert candlestick data: {e}")
            raise
    
    def insert_tick_data(self, df: pd.DataFrame, symbol: str):
        """插入Tick資料"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            df = df.copy()
            df['symbol'] = symbol
            
            self.conn.execute("""
                INSERT INTO tick_data 
                (symbol, timestamp, bid, ask, volume)
                SELECT symbol, timestamp, bid, ask, volume
                FROM df
            """)
            
            logger.info(f"Inserted {len(df)} tick records for {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to insert tick data: {e}")
            raise
    
    def get_candlestick_data(self, symbol: str, timeframe: str, 
                           start_date: Optional[str] = None, 
                           end_date: Optional[str] = None) -> pd.DataFrame:
        """取得蠟燭圖資料"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM candlestick_data
                WHERE symbol = ? AND timeframe = ?
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)
            
            query += " ORDER BY timestamp"
            
            df = self.conn.execute(query, params).fetchdf()
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get candlestick data: {e}")
            raise
    
    def get_available_timeframes(self, symbol: str) -> List[str]:
        """取得可用的時間週期"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            query = """
                SELECT DISTINCT timeframe
                FROM candlestick_data
                WHERE symbol = ?
                ORDER BY timeframe
            """
            result = self.conn.execute(query, [symbol]).fetchall()
            return [row[0] for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get available timeframes: {e}")
            raise
    
    def close(self):
        """關閉資料庫連接"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 
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
            
            # 創建波段資料表
            self.conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS swing_data_seq START 1
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS swing_data (
                    id INTEGER PRIMARY KEY DEFAULT nextval('swing_data_seq'),
                    symbol VARCHAR,
                    timeframe VARCHAR,
                    algorithm_name VARCHAR,
                    version_hash VARCHAR,
                    timestamp TIMESTAMP,
                    zigzag_price DECIMAL(10,5),
                    zigzag_type VARCHAR,
                    zigzag_strength DECIMAL(10,5),
                    zigzag_swing INTEGER,
                    swing_high DECIMAL(10,5),
                    swing_low DECIMAL(10,5),
                    swing_range DECIMAL(10,5),
                    swing_duration INTEGER,
                    swing_direction VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 創建演算法統計表
            self.conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS algorithm_statistics_seq START 1
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS algorithm_statistics (
                    id INTEGER PRIMARY KEY DEFAULT nextval('algorithm_statistics_seq'),
                    symbol VARCHAR,
                    timeframe VARCHAR,
                    algorithm_name VARCHAR,
                    version_hash VARCHAR,
                    total_swings INTEGER,
                    avg_swing_range DECIMAL(10,5),
                    avg_swing_duration DECIMAL(10,5),
                    max_swing_range DECIMAL(10,5),
                    min_swing_range DECIMAL(10,5),
                    swing_ranges JSON,
                    swing_durations JSON,
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
            
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_swing_symbol_timeframe_algorithm 
                ON swing_data(symbol, timeframe, algorithm_name)
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
                           end_date: Optional[str] = None,
                           limit: Optional[int] = None) -> pd.DataFrame:
        """取得蠟燭圖資料"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            # 使用 v_candlestick_latest 視圖來獲取最新版本的資料
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM v_candlestick_latest
                WHERE symbol = ? AND timeframe = ?
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)
            
            query += " ORDER BY timestamp DESC"  # 改為 DESC 以獲取最新資料
            
            # 添加 LIMIT 來限制返回資料量
            if limit:
                query += f" LIMIT {limit}"
            else:
                # 預設限制為 1000 筆，避免載入過多資料
                query += " LIMIT 1000"
            
            df = self.conn.execute(query, params).fetchdf()
            
            if not df.empty:
                df.set_index('timestamp', inplace=True)
                # 重新排序為升序，以便正確顯示
                df = df.sort_index()
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get candlestick data: {e}")
            # 如果視圖不存在，嘗試使用舊表
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
                
                query += " ORDER BY timestamp DESC"  # 改為 DESC
                
                # 添加 LIMIT
                if limit:
                    query += f" LIMIT {limit}"
                else:
                    query += " LIMIT 1000"
                
                df = self.conn.execute(query, params).fetchdf()
                
                if not df.empty:
                    df.set_index('timestamp', inplace=True)
                    df = df.sort_index()
                
                return df
            except Exception as e2:
                logger.error(f"Failed to get candlestick data from old table: {e2}")
                raise
    
    def get_available_timeframes(self, symbol: str) -> List[str]:
        """取得可用的時間週期"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            # 使用 v_candlestick_latest 視圖
            query = """
                SELECT DISTINCT timeframe
                FROM v_candlestick_latest
                WHERE symbol = ?
                ORDER BY timeframe
            """
            result = self.conn.execute(query, [symbol]).fetchall()
            return [row[0] for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get available timeframes: {e}")
            # 如果視圖不存在，嘗試使用舊表
            try:
                query = """
                    SELECT DISTINCT timeframe
                    FROM candlestick_data
                    WHERE symbol = ?
                    ORDER BY timeframe
                """
                result = self.conn.execute(query, [symbol]).fetchall()
                return [row[0] for row in result]
            except Exception as e2:
                logger.error(f"Failed to get available timeframes from old table: {e2}")
                raise
    
    def insert_swing_data(self, df: pd.DataFrame, symbol: str, timeframe: str, 
                         algorithm_name: str, algorithm_parameters: Dict, 
                         version_name: Optional[str] = None, description: str = ""):
        """插入波段資料並返回版本hash"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            import hashlib
            import json
            
            # 生成版本hash
            param_str = json.dumps(algorithm_parameters, sort_keys=True)
            version_hash = hashlib.md5(f"{algorithm_name}_{param_str}".encode()).hexdigest()[:8]
            
            # 準備資料
            swing_df = df[df['zigzag_price'].notna()].copy()
            if swing_df.empty:
                logger.warning(f"No swing data to insert for {symbol} {timeframe}")
                return version_hash
            
            # 重置索引，使timestamp成為一個欄位
            swing_df = swing_df.reset_index()
            
            # 添加必要欄位
            swing_df['symbol'] = symbol
            swing_df['timeframe'] = timeframe
            swing_df['algorithm_name'] = algorithm_name
            swing_df['version_hash'] = version_hash
            
            # 選擇需要的欄位
            columns_to_insert = [
                'symbol', 'timeframe', 'algorithm_name', 'version_hash', 'timestamp',
                'zigzag_price', 'zigzag_type', 'zigzag_strength', 'zigzag_swing',
                'swing_high', 'swing_low', 'swing_range', 'swing_duration', 'swing_direction'
            ]
            
            # 確保所有欄位都存在，不存在的設為NULL
            for col in columns_to_insert:
                if col not in swing_df.columns:
                    swing_df[col] = None
            
            swing_df = swing_df[columns_to_insert]
            
            # 刪除該symbol+timeframe+algorithm的舊資料
            self.conn.execute("""
                DELETE FROM swing_data 
                WHERE symbol = ? AND timeframe = ? AND algorithm_name = ?
            """, [symbol, timeframe, algorithm_name])
            
            # 插入新資料
            self.conn.execute("""
                INSERT INTO swing_data 
                (symbol, timeframe, algorithm_name, version_hash, timestamp,
                 zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
                 swing_high, swing_low, swing_range, swing_duration, swing_direction)
                SELECT symbol, timeframe, algorithm_name, version_hash, timestamp,
                       zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
                       swing_high, swing_low, swing_range, swing_duration, swing_direction
                FROM swing_df
            """)
            
            logger.info(f"Inserted {len(swing_df)} swing records for {symbol} {timeframe} {algorithm_name}")
            return version_hash
            
        except Exception as e:
            logger.error(f"Failed to insert swing data: {e}")
            raise
    
    def insert_algorithm_statistics(self, symbol: str, timeframe: str, algorithm_name: str, 
                                   version_hash: str, statistics: Dict):
        """插入演算法統計資訊"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            import json
            
            # 刪除舊統計資料
            self.conn.execute("""
                DELETE FROM algorithm_statistics 
                WHERE symbol = ? AND timeframe = ? AND algorithm_name = ? AND version_hash = ?
            """, [symbol, timeframe, algorithm_name, version_hash])
            
            # 插入新統計資料
            self.conn.execute("""
                INSERT INTO algorithm_statistics 
                (symbol, timeframe, algorithm_name, version_hash, total_swings,
                 avg_swing_range, avg_swing_duration, max_swing_range, min_swing_range,
                 swing_ranges, swing_durations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                symbol, timeframe, algorithm_name, version_hash,
                statistics.get('total_swings', 0),
                statistics.get('avg_swing_range', 0),
                statistics.get('avg_swing_duration', 0),
                statistics.get('max_swing_range', 0),
                statistics.get('min_swing_range', 0),
                json.dumps(statistics.get('swing_ranges', [])),
                json.dumps(statistics.get('swing_durations', []))
            ])
            
            logger.info(f"Inserted statistics for {symbol} {timeframe} {algorithm_name}")
            
        except Exception as e:
            logger.error(f"Failed to insert algorithm statistics: {e}")
            raise
    
    def get_swing_data(self, symbol: str, timeframe: str, algorithm: str,
                      start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """取得波段資料"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            query = """
                SELECT timestamp, zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
                       swing_high, swing_low, swing_range, swing_duration, swing_direction
                FROM swing_data
                WHERE symbol = ? AND timeframe = ? AND algorithm_name = ?
            """
            params = [symbol, timeframe, algorithm]
            
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)
            
            query += " ORDER BY timestamp"
            
            df = self.conn.execute(query, params).fetchdf()
            
            if not df.empty:
                df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get swing data: {e}")
            return pd.DataFrame()
    
    def get_available_algorithms(self, symbol: str, timeframe: str) -> List[str]:
        """取得可用的演算法"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        try:
            query = """
                SELECT DISTINCT algorithm_name
                FROM swing_data
                WHERE symbol = ? AND timeframe = ?
                ORDER BY algorithm_name
            """
            result = self.conn.execute(query, [symbol, timeframe]).fetchall()
            return [row[0] for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get available algorithms: {e}")
            return []
    
    def close(self):
        """關閉資料庫連接"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 
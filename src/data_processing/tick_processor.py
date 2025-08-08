import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Generator
import logging
from pathlib import Path
import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    TICK_CSV_PATH, TIMEFRAMES, BATCH_SIZE, CHUNK_SIZE, 
    DUCKDB_PATH, PARQUET_DATA_DIR
)
from src.database.connection import DuckDBConnection

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TickProcessor:
    """Tick資料處理器"""
    
    def __init__(self, csv_path: str = TICK_CSV_PATH, db_path: str = str(DUCKDB_PATH)):
        self.csv_path = csv_path
        self.db_path = db_path
        self.symbol = self._extract_symbol_from_path(csv_path)
        
        # 確保Parquet目錄存在
        PARQUET_DATA_DIR.mkdir(parents=True, exist_ok=True)
        
    def _extract_symbol_from_path(self, path: str) -> str:
        """從檔案路徑中提取交易品種名稱"""
        filename = Path(path).stem
        # 假設檔案名格式為 "EXUSA30IDXUSD-TICK-No Session"
        symbol = filename.split('-')[0]
        return symbol
    
    def read_csv_in_chunks(self, chunk_size: int = CHUNK_SIZE) -> Generator[pd.DataFrame, None, None]:
        """分批讀取CSV檔案"""
        try:
            logger.info(f"Starting to read CSV file: {self.csv_path}")
            
            # 嘗試讀取CSV文件的第一行來確定列名
            sample_df = pd.read_csv(self.csv_path, nrows=1)
            logger.info(f"CSV columns: {sample_df.columns.tolist()}")
            
            # 讀取CSV檔案
            chunk_iterator = pd.read_csv(
                self.csv_path,
                chunksize=chunk_size,
                parse_dates=['DateTime'] if 'DateTime' in sample_df.columns else None,
                dtype={
                    'Bid': np.float64,
                    'Ask': np.float64,
                    'Volume': np.int64
                } if all(col in sample_df.columns for col in ['Bid', 'Ask', 'Volume']) else None
            )
            
            for chunk_num, chunk in enumerate(chunk_iterator):
                logger.info(f"Processing chunk {chunk_num + 1}, size: {len(chunk)}")
                
                # 標準化列名
                chunk = self._standardize_columns(chunk)
                
                # 資料清洗
                chunk = self._clean_data(chunk)
                
                if not chunk.empty:
                    yield chunk
                    
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """標準化列名"""
        # 映射可能的列名到標準格式
        column_mapping = {
            'DateTime': 'timestamp',
            'Date': 'timestamp',
            'Time': 'timestamp',
            'Timestamp': 'timestamp',
            'Bid': 'bid',
            'Ask': 'ask',
            'Volume': 'volume',
            'Vol': 'volume',
            'BidPrice': 'bid',
            'AskPrice': 'ask'
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 如果沒有bid和ask，但有價格列，則使用價格作為bid和ask
        if 'bid' not in df.columns and 'ask' not in df.columns:
            if 'Price' in df.columns:
                df['bid'] = df['Price']
                df['ask'] = df['Price']
            elif 'Close' in df.columns:
                df['bid'] = df['Close']
                df['ask'] = df['Close']
        
        # 確保必要的列存在
        required_columns = ['timestamp', 'bid', 'ask']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                raise ValueError(f"Missing required column: {col}")
        
        # 如果沒有volume列，設置為1
        if 'volume' not in df.columns:
            df['volume'] = 1
            
        return df[['timestamp', 'bid', 'ask', 'volume']]
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗資料"""
        # 移除空值
        df = df.dropna()
        
        # 確保時間戳是datetime類型
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # 移除異常值（bid或ask為0或負數）
        df = df[(df['bid'] > 0) & (df['ask'] > 0)]
        
        # 確保ask >= bid
        df = df[df['ask'] >= df['bid']]
        
        # 移除重複的時間戳
        df = df.drop_duplicates(subset=['timestamp'])
        
        # 按時間戳排序
        df = df.sort_values('timestamp')
        
        return df
    
    def create_candlestick_data(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """將Tick資料轉換為蠟燭圖資料"""
        if df.empty:
            return pd.DataFrame()
            
        try:
            # 設置時間戳為索引
            df = df.set_index('timestamp')
            
            # 計算中間價格
            df['price'] = (df['bid'] + df['ask']) / 2
            
            # 根據時間週期重新採樣
            pandas_timeframe = TIMEFRAMES[timeframe]
            
            # 創建蠟燭圖資料
            candlestick = df['price'].resample(pandas_timeframe).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            })
            
            # 添加成交量
            volume = df['volume'].resample(pandas_timeframe).sum()
            candlestick['volume'] = volume
            
            # 移除空值
            candlestick = candlestick.dropna()
            
            # 重置索引
            candlestick = candlestick.reset_index()
            
            logger.info(f"Created {len(candlestick)} {timeframe} candlestick records")
            
            return candlestick
            
        except Exception as e:
            logger.error(f"Error creating candlestick data for {timeframe}: {e}")
            raise
    
    def save_to_parquet(self, df: pd.DataFrame, timeframe: str, batch_num: int):
        """將資料保存為Parquet格式"""
        try:
            filename = f"{self.symbol}_{timeframe}_batch_{batch_num}.parquet"
            filepath = PARQUET_DATA_DIR / filename
            
            df.to_parquet(filepath, compression='snappy')
            logger.info(f"Saved {len(df)} records to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving to parquet: {e}")
            raise
    
    def process_tick_data(self):
        """處理Tick資料的主要方法"""
        try:
            logger.info("Starting tick data processing...")
            
            # 初始化資料庫連接
            with DuckDBConnection(self.db_path) as db:
                
                # 用於累積不同時間週期的資料
                accumulated_data = {tf: [] for tf in TIMEFRAMES.keys()}
                batch_counters = {tf: 0 for tf in TIMEFRAMES.keys()}
                
                # 分批處理Tick資料
                for chunk in self.read_csv_in_chunks():
                    
                    # 為每個時間週期創建蠟燭圖資料
                    for timeframe in TIMEFRAMES.keys():
                        try:
                            candlestick_data = self.create_candlestick_data(chunk, timeframe)
                            
                            if not candlestick_data.empty:
                                accumulated_data[timeframe].append(candlestick_data)
                                
                                # 檢查是否需要批次保存
                                total_records = sum(len(df) for df in accumulated_data[timeframe])
                                
                                if total_records >= BATCH_SIZE:
                                    # 合併資料
                                    combined_df = pd.concat(accumulated_data[timeframe], ignore_index=True)
                                    
                                    # 保存到資料庫
                                    db.insert_candlestick_data(combined_df, timeframe, self.symbol)
                                    
                                    # 保存到Parquet
                                    self.save_to_parquet(combined_df, timeframe, batch_counters[timeframe])
                                    
                                    # 重置累積資料
                                    accumulated_data[timeframe] = []
                                    batch_counters[timeframe] += 1
                                    
                        except Exception as e:
                            logger.error(f"Error processing timeframe {timeframe}: {e}")
                            continue
                
                # 處理剩餘的資料
                for timeframe in TIMEFRAMES.keys():
                    if accumulated_data[timeframe]:
                        try:
                            combined_df = pd.concat(accumulated_data[timeframe], ignore_index=True)
                            db.insert_candlestick_data(combined_df, timeframe, self.symbol)
                            self.save_to_parquet(combined_df, timeframe, batch_counters[timeframe])
                            
                        except Exception as e:
                            logger.error(f"Error saving final batch for {timeframe}: {e}")
                
                logger.info("Tick data processing completed successfully!")
                
        except Exception as e:
            logger.error(f"Error in tick data processing: {e}")
            raise

def main():
    """主函數"""
    try:
        processor = TickProcessor()
        processor.process_tick_data()
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 
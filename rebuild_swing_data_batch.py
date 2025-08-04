#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批次重建所有品種的波段數據
支持分批處理大量數據，使用更寬鬆的參數設定
"""

import sys
import os
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime
import logging
from pathlib import Path
import time

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.algorithms.zigzag import ZigZagAlgorithm
from config.settings import DUCKDB_PATH, TIMEFRAMES

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rebuild_swing_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchSwingProcessor:
    """批次波段處理器"""
    
    def __init__(self):
        self.db_path = DUCKDB_PATH
        # 使用更寬鬆的參數：1%偏差，6個bar深度
        self.algorithm = ZigZagAlgorithm(deviation=1.0, depth=6)
        self.symbols = ['XAUUSD', 'US30', 'US100']
        self.timeframes = list(TIMEFRAMES.keys())
        self.batch_size = 10000  # 每批處理10000筆數據
        self.next_id = 1
        
    def connect_database(self):
        """連接資料庫"""
        try:
            conn = duckdb.connect(self.db_path)
            logger.info(f"成功連接資料庫: {self.db_path}")
            return conn
        except Exception as e:
            logger.error(f"連接資料庫失敗: {e}")
            raise
    
    def clear_swing_data(self, conn):
        """清除現有的swing_data"""
        try:
            conn.execute("DELETE FROM swing_data")
            logger.info("已清除現有的swing_data")
        except Exception as e:
            logger.error(f"清除swing_data失敗: {e}")
            raise
    
    def get_total_count(self, conn, symbol, timeframe):
        """獲取指定品種和時間週期的總數據量"""
        try:
            query = f"""
            SELECT COUNT(*) as total_count
            FROM candlestick_data_new 
            WHERE symbol = '{symbol}' AND timeframe = '{timeframe}'
            """
            result = conn.execute(query).fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"獲取 {symbol} {timeframe} 總數據量失敗: {e}")
            return 0
    
    def get_candlestick_batch(self, conn, symbol, timeframe, offset, limit):
        """分批獲取蠟燭圖資料"""
        try:
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM candlestick_data_new 
            WHERE symbol = '{symbol}' AND timeframe = '{timeframe}'
            ORDER BY timestamp
            LIMIT {limit} OFFSET {offset}
            """
            
            df = conn.execute(query).fetchdf()
            
            if df.empty:
                logger.warning(f"沒有找到 {symbol} {timeframe} 的資料 (offset={offset}, limit={limit})")
                return None
            
            logger.info(f"獲取 {symbol} {timeframe} 批次資料: {len(df)} 筆 (offset={offset})")
            return df
            
        except Exception as e:
            logger.error(f"獲取 {symbol} {timeframe} 批次資料失敗: {e}")
            return None
    
    def calculate_zigzag_swings(self, df, symbol, timeframe, batch_offset):
        """計算ZigZag波段"""
        try:
            logger.info(f"開始計算 {symbol} {timeframe} 批次ZigZag波段 (offset={batch_offset})...")
            
            # 執行ZigZag演算法
            result_df = self.algorithm.calculate(df)
            
            # 提取ZigZag點
            zigzag_mask = result_df['zigzag_price'].notna()
            zigzag_data = result_df[zigzag_mask].copy()
            
            if zigzag_data.empty:
                logger.warning(f"{symbol} {timeframe} 批次沒有找到ZigZag點 (offset={batch_offset})")
                return None
            
            # 準備匯入資料庫的資料
            swing_records = []
            for idx, row in zigzag_data.iterrows():
                # 安全地獲取值，避免KeyError
                try:
                    record = {
                        'id': self.next_id,
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'algorithm_name': 'zigzag',
                        'version_hash': 'batch_rebuild_v2',  # 新的版本雜湊
                        'timestamp': row['timestamp'],
                        'zigzag_price': float(row['zigzag_price']),
                        'zigzag_type': row['zigzag_type'],
                        'zigzag_strength': float(row['zigzag_strength']) if pd.notna(row.get('zigzag_strength', None)) else None,
                        'zigzag_swing': int(row['zigzag_swing']) if pd.notna(row.get('zigzag_swing', None)) else None,
                        'swing_high': float(row['swing_high']) if pd.notna(row.get('swing_high', None)) else None,
                        'swing_low': float(row['swing_low']) if pd.notna(row.get('swing_low', None)) else None,
                        'swing_range': float(row['swing_range']) if pd.notna(row.get('swing_range', None)) else None,
                        'swing_duration': int(row['swing_duration']) if pd.notna(row.get('swing_duration', None)) else None,
                        'swing_direction': row.get('swing_direction', None),
                        'created_at': datetime.now()
                    }
                    swing_records.append(record)
                    self.next_id += 1
                except Exception as e:
                    logger.warning(f"處理波段記錄失敗 (index={idx}): {e}")
                    continue
            
            logger.info(f"{symbol} {timeframe} 批次找到 {len(swing_records)} 個ZigZag點 (offset={batch_offset})")
            return swing_records
            
        except Exception as e:
            logger.error(f"計算 {symbol} {timeframe} 批次ZigZag波段失敗: {e}")
            return None
    
    def insert_swing_data(self, conn, swing_records):
        """匯入swing_data到資料庫"""
        if not swing_records:
            return
        
        try:
            # 逐筆插入，避免參數不匹配問題
            for record in swing_records:
                conn.execute("""
                    INSERT INTO swing_data (
                        id, symbol, timeframe, algorithm_name, version_hash,
                        timestamp, zigzag_price, zigzag_type, zigzag_strength,
                        zigzag_swing, swing_high, swing_low, swing_range,
                        swing_duration, swing_direction, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    record['id'],
                    record['symbol'],
                    record['timeframe'],
                    record['algorithm_name'],
                    record['version_hash'],
                    record['timestamp'],
                    record['zigzag_price'],
                    record['zigzag_type'],
                    record['zigzag_strength'],
                    record['zigzag_swing'],
                    record['swing_high'],
                    record['swing_low'],
                    record['swing_range'],
                    record['swing_duration'],
                    record['swing_direction'],
                    record['created_at']
                ])
            
            logger.info(f"成功匯入 {len(swing_records)} 筆波段資料")
            
        except Exception as e:
            logger.error(f"匯入swing_data失敗: {e}")
            raise
    
    def process_symbol_timeframe(self, conn, symbol, timeframe):
        """處理單一品種和時間週期的所有數據"""
        logger.info(f"開始處理 {symbol} {timeframe}...")
        
        # 獲取總數據量
        total_count = self.get_total_count(conn, symbol, timeframe)
        if total_count == 0:
            logger.warning(f"{symbol} {timeframe} 沒有數據")
            return
        
        logger.info(f"{symbol} {timeframe} 總數據量: {total_count}")
        
        # 分批處理
        offset = 0
        batch_count = 0
        total_swings = 0
        
        while offset < total_count:
            batch_count += 1
            logger.info(f"處理 {symbol} {timeframe} 第 {batch_count} 批 (offset={offset})")
            
            # 獲取批次數據
            df = self.get_candlestick_batch(conn, symbol, timeframe, offset, self.batch_size)
            if df is None:
                break
            
            # 計算ZigZag波段
            swing_records = self.calculate_zigzag_swings(df, symbol, timeframe, offset)
            if swing_records:
                # 匯入資料庫
                self.insert_swing_data(conn, swing_records)
                total_swings += len(swing_records)
            
            # 更新偏移量
            offset += self.batch_size
            
            # 短暫休息，避免過度消耗資源
            time.sleep(0.1)
        
        logger.info(f"{symbol} {timeframe} 處理完成，總共找到 {total_swings} 個波段點")
    
    def process_all_symbols(self):
        """處理所有品種和時間週期"""
        logger.info("開始批次重建所有波段數據...")
        
        try:
            conn = self.connect_database()
            
            # 清除現有數據
            self.clear_swing_data(conn)
            
            # 處理每個品種和時間週期
            total_combinations = len(self.symbols) * len(self.timeframes)
            current_combination = 0
            
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    current_combination += 1
                    logger.info(f"進度: {current_combination}/{total_combinations} - {symbol} {timeframe}")
                    
                    try:
                        self.process_symbol_timeframe(conn, symbol, timeframe)
                    except Exception as e:
                        logger.error(f"處理 {symbol} {timeframe} 失敗: {e}")
                        continue
            
            # 顯示統計信息
            self.show_statistics(conn)
            
            conn.close()
            logger.info("批次重建完成！")
            
        except Exception as e:
            logger.error(f"批次重建失敗: {e}")
            raise
    
    def show_statistics(self, conn):
        """顯示統計信息"""
        try:
            logger.info("=== 波段數據統計 ===")
            
            # 總體統計
            total_query = "SELECT COUNT(*) as total FROM swing_data"
            total_result = conn.execute(total_query).fetchone()
            total_swings = total_result[0] if total_result else 0
            logger.info(f"總波段點數: {total_swings}")
            
            # 按品種統計
            symbol_query = """
            SELECT symbol, COUNT(*) as count
            FROM swing_data
            GROUP BY symbol
            ORDER BY count DESC
            """
            symbol_results = conn.execute(symbol_query).fetchall()
            logger.info("按品種統計:")
            for symbol, count in symbol_results:
                logger.info(f"  {symbol}: {count} 個波段點")
            
            # 按時間週期統計
            timeframe_query = """
            SELECT timeframe, COUNT(*) as count
            FROM swing_data
            GROUP BY timeframe
            ORDER BY count DESC
            """
            timeframe_results = conn.execute(timeframe_query).fetchall()
            logger.info("按時間週期統計:")
            for timeframe, count in timeframe_results:
                logger.info(f"  {timeframe}: {count} 個波段點")
            
            # 按類型統計
            type_query = """
            SELECT zigzag_type, COUNT(*) as count
            FROM swing_data
            GROUP BY zigzag_type
            ORDER BY count DESC
            """
            type_results = conn.execute(type_query).fetchall()
            logger.info("按波段類型統計:")
            for swing_type, count in type_results:
                logger.info(f"  {swing_type}: {count} 個波段點")
            
        except Exception as e:
            logger.error(f"顯示統計信息失敗: {e}")

def main():
    """主函數"""
    logger.info("=== 批次重建波段數據開始 ===")
    
    processor = BatchSwingProcessor()
    
    try:
        processor.process_all_symbols()
        logger.info("=== 批次重建波段數據完成 ===")
    except Exception as e:
        logger.error(f"批次重建失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
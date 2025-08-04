#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
重新生成完整的波段數據
處理所有歷史數據，解決波段數據缺失問題
"""

import sys
import os
import time
import logging
from datetime import datetime
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH, TIMEFRAMES
from src.algorithms.zigzag_improved import ImprovedZigZagAlgorithm
import duckdb
import pandas as pd

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('regenerate_swings.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CompleteSwingRegenerator:
    """完整波段數據重新生成器"""
    
    def __init__(self):
        self.db_path = DUCKDB_PATH
        # 使用適合長期數據的參數
        self.algorithm = ImprovedZigZagAlgorithm(deviation=2.0, depth=15, min_swing_bars=3)
        self.symbols = ['XAUUSD', 'US30', 'US100']
        self.timeframes = ['D1', 'H4', 'H1', 'M30', 'M15', 'M5', 'M1']  # 按重要性排序
        self.next_id = 1
        self.batch_size = 50000  # 分批處理大小
        
    def connect_database(self):
        """連接資料庫"""
        try:
            conn = duckdb.connect(self.db_path)
            logger.info(f"成功連接資料庫: {self.db_path}")
            return conn
        except Exception as e:
            logger.error(f"連接資料庫失敗: {e}")
            raise
    
    def backup_swing_data(self, conn):
        """備份現有的swing_data"""
        try:
            # 檢查是否有現有數據
            count = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()[0]
            if count > 0:
                # 創建備份表
                backup_table = f"swing_data_backup_{int(time.time())}"
                conn.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM swing_data")
                logger.info(f"已備份 {count} 筆swing_data到 {backup_table}")
            else:
                logger.info("沒有現有的swing_data需要備份")
        except Exception as e:
            logger.error(f"備份swing_data失敗: {e}")
            raise
    
    def clear_swing_data(self, conn):
        """清除現有的swing_data"""
        try:
            conn.execute("DELETE FROM swing_data")
            logger.info("已清除現有的swing_data")
        except Exception as e:
            logger.error(f"清除swing_data失敗: {e}")
            raise
    
    def get_candlestick_data(self, conn, symbol, timeframe):
        """獲取完整的蠟燭圖資料"""
        try:
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM candlestick_data_new 
            WHERE symbol = '{symbol}' AND timeframe = '{timeframe}'
            ORDER BY timestamp
            """
            
            df = conn.execute(query).fetchdf()
            
            if df.empty:
                logger.warning(f"沒有找到 {symbol} {timeframe} 的資料")
                return None
            
            logger.info(f"獲取 {symbol} {timeframe} 資料: {len(df)} 筆")
            logger.info(f"時間範圍: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
            return df
            
        except Exception as e:
            logger.error(f"獲取 {symbol} {timeframe} 資料失敗: {e}")
            return None
    
    def calculate_zigzag_swings(self, df, symbol, timeframe):
        """計算ZigZag波段（分批處理）"""
        try:
            logger.info(f"開始計算 {symbol} {timeframe} 的ZigZag波段...")
            logger.info(f"總數據量: {len(df)} 筆")
            
            # 如果數據量太大，分批處理
            if len(df) > self.batch_size:
                logger.info(f"數據量超過 {self.batch_size}，啟用分批處理")
                return self._calculate_zigzag_swings_batch(df, symbol, timeframe)
            else:
                return self._calculate_zigzag_swings_single(df, symbol, timeframe)
            
        except Exception as e:
            logger.error(f"計算 {symbol} {timeframe} ZigZag波段失敗: {e}")
            return None
    
    def _calculate_zigzag_swings_single(self, df, symbol, timeframe):
        """單次計算ZigZag波段"""
        # 執行ZigZag演算法
        result_df = self.algorithm.calculate(df)
        
        # 提取ZigZag點
        zigzag_mask = result_df['zigzag_price'].notna()
        zigzag_data = result_df[zigzag_mask].copy()
        
        if zigzag_data.empty:
            logger.warning(f"{symbol} {timeframe} 沒有找到ZigZag點")
            return None
        
        # 準備匯入資料庫的資料
        swing_records = []
        for idx, row in zigzag_data.iterrows():
            record = {
                'id': self.next_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'algorithm_name': 'zigzag',
                'version_hash': 'complete_regeneration_v1',  # 新的版本雜湊
                'timestamp': row['timestamp'],
                'zigzag_price': float(row['zigzag_price']),
                'zigzag_type': row['zigzag_type'],
                'zigzag_strength': float(row['zigzag_strength']) if pd.notna(row['zigzag_strength']) else None,
                'zigzag_swing': int(row['zigzag_swing']) if pd.notna(row['zigzag_swing']) else None,
                'swing_high': float(row['swing_high']) if pd.notna(row['swing_high']) else None,
                'swing_low': float(row['swing_low']) if pd.notna(row['swing_low']) else None,
                'swing_range': float(row['swing_range']) if pd.notna(row['swing_range']) else None,
                'swing_duration': int(row['swing_duration']) if pd.notna(row['swing_duration']) else None,
                'swing_direction': row['swing_direction'],
                'created_at': datetime.now()
            }
            swing_records.append(record)
            self.next_id += 1
        
        logger.info(f"{symbol} {timeframe} 找到 {len(swing_records)} 個ZigZag點")
        return swing_records
    
    def _calculate_zigzag_swings_batch(self, df, symbol, timeframe):
        """分批計算ZigZag波段"""
        all_swing_records = []
        total_batches = (len(df) + self.batch_size - 1) // self.batch_size
        
        logger.info(f"將分 {total_batches} 批處理數據")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min((batch_num + 1) * self.batch_size, len(df))
            
            logger.info(f"處理批次 {batch_num + 1}/{total_batches}: 索引 {start_idx} 到 {end_idx}")
            
            # 獲取當前批次的數據
            batch_df = df.iloc[start_idx:end_idx].copy()
            
            # 計算當前批次的波段
            batch_records = self._calculate_zigzag_swings_single(batch_df, symbol, timeframe)
            
            if batch_records:
                all_swing_records.extend(batch_records)
                logger.info(f"批次 {batch_num + 1} 完成，找到 {len(batch_records)} 個波段點")
            else:
                logger.info(f"批次 {batch_num + 1} 沒有找到波段點")
        
        logger.info(f"{symbol} {timeframe} 分批處理完成，總共找到 {len(all_swing_records)} 個ZigZag點")
        return all_swing_records
    
    def insert_swing_data(self, conn, swing_records):
        """匯入swing_data到資料庫"""
        if not swing_records:
            return
        
        try:
            # 準備插入語句
            columns = [
                'id', 'symbol', 'timeframe', 'algorithm_name', 'version_hash',
                'timestamp', 'zigzag_price', 'zigzag_type', 'zigzag_strength',
                'zigzag_swing', 'swing_high', 'swing_low', 'swing_range',
                'swing_duration', 'swing_direction', 'created_at'
            ]
            
            # 批量插入
            values = []
            for record in swing_records:
                row_values = [record[col] for col in columns]
                values.append(row_values)
            
            # 執行插入
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO swing_data ({', '.join(columns)}) VALUES ({placeholders})"
            
            conn.executemany(insert_sql, values)
            conn.commit()
            
            logger.info(f"成功匯入 {len(swing_records)} 筆swing_data")
            
        except Exception as e:
            logger.error(f"匯入swing_data失敗: {e}")
            try:
                conn.rollback()
            except:
                pass
            raise
    
    def process_all_symbols(self):
        """處理所有品種的ZigZag波段"""
        logger.info("開始重新生成所有品種的ZigZag波段...")
        
        conn = self.connect_database()
        
        try:
            # 備份現有數據
            self.backup_swing_data(conn)
            
            # 清除現有數據
            self.clear_swing_data(conn)
            
            total_swings = 0
            start_time = time.time()
            
            # 處理每個品種和時間週期
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    logger.info(f"處理 {symbol} {timeframe}...")
                    
                    # 獲取蠟燭圖資料
                    df = self.get_candlestick_data(conn, symbol, timeframe)
                    if df is None:
                        continue
                    
                    # 計算ZigZag波段
                    swing_records = self.calculate_zigzag_swings(df, symbol, timeframe)
                    if swing_records is None:
                        continue
                    
                    # 匯入資料庫
                    self.insert_swing_data(conn, swing_records)
                    total_swings += len(swing_records)
                    
                    logger.info(f"完成 {symbol} {timeframe}: {len(swing_records)} 個波段")
            
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"所有品種處理完成，總共 {total_swings} 個ZigZag波段")
            logger.info(f"總耗時: {duration:.2f} 秒")
            
            # 顯示統計資訊
            self.show_statistics(conn)
            
        except Exception as e:
            logger.error(f"處理過程中發生錯誤: {e}")
            raise
        finally:
            conn.close()
    
    def show_statistics(self, conn):
        """顯示統計資訊"""
        try:
            # 總計統計
            total_count = conn.execute("SELECT COUNT(*) FROM swing_data").fetchone()[0]
            logger.info(f"swing_data總筆數: {total_count}")
            
            # 按品種統計
            symbol_stats = conn.execute("""
                SELECT symbol, COUNT(*) as count 
                FROM swing_data 
                GROUP BY symbol 
                ORDER BY count DESC
            """).fetchall()
            
            logger.info("按品種統計:")
            for symbol, count in symbol_stats:
                logger.info(f"  {symbol}: {count} 筆")
            
            # 按時間週期統計
            timeframe_stats = conn.execute("""
                SELECT timeframe, COUNT(*) as count 
                FROM swing_data 
                GROUP BY timeframe 
                ORDER BY count DESC
            """).fetchall()
            
            logger.info("按時間週期統計:")
            for timeframe, count in timeframe_stats:
                logger.info(f"  {timeframe}: {count} 筆")
            
            # 時間範圍統計
            time_range = conn.execute("""
                SELECT MIN(timestamp), MAX(timestamp) 
                FROM swing_data
            """).fetchone()
            
            if time_range[0] and time_range[1]:
                logger.info(f"波段時間範圍: {time_range[0]} 到 {time_range[1]}")
            
        except Exception as e:
            logger.error(f"顯示統計資訊失敗: {e}")


def main():
    """主函數"""
    logger.info("=== 開始重新生成完整波段數據 ===")
    
    regenerator = CompleteSwingRegenerator()
    
    try:
        regenerator.process_all_symbols()
        logger.info("=== 波段數據重新生成完成 ===")
    except Exception as e:
        logger.error(f"重新生成失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
重新生成完整的波段數據
確保處理所有K線數據，分批處理大量數據
"""

import sys
import os
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime
import logging
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.algorithms.zigzag import ZigZagAlgorithm
from config.settings import DUCKDB_PATH, TIMEFRAMES

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('swing_regeneration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SwingDataRegenerator:
    """波段數據重新生成器"""
    
    def __init__(self):
        self.db_path = DUCKDB_PATH
        self.algorithm = ZigZagAlgorithm(deviation=3.0, depth=12)  # 使用3%偏差，12個bar深度
        self.symbols = ['XAUUSD', 'US30', 'US100']
        self.timeframes = list(TIMEFRAMES.keys())
        self.batch_size = 10000  # 分批處理大小
        self.next_id = 1  # 手動管理ID
        
    def connect_database(self):
        """連接資料庫"""
        try:
            conn = duckdb.connect(self.db_path)
            logger.info(f"成功連接資料庫: {self.db_path}")
            return conn
        except Exception as e:
            logger.error(f"連接資料庫失敗: {e}")
            raise
    
    def get_total_candlestick_count(self, conn, symbol, timeframe):
        """獲取K線數據總數"""
        try:
            query = f"""
            SELECT COUNT(*) as total_count
            FROM candlestick_data_new 
            WHERE symbol = '{symbol}' AND timeframe = '{timeframe}'
            """
            
            result = conn.execute(query).fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"獲取 {symbol} {timeframe} 數據總數失敗: {e}")
            return 0
    
    def get_candlestick_data_batch(self, conn, symbol, timeframe, offset=0, limit=None):
        """分批獲取K線數據"""
        try:
            if limit is None:
                limit = self.batch_size
                
            query = f"""
            SELECT 
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM candlestick_data_new 
            WHERE symbol = '{symbol}' AND timeframe = '{timeframe}'
            ORDER BY timestamp
            LIMIT {limit} OFFSET {offset}
            """
            
            df = conn.execute(query).fetchdf()
            
            if df.empty:
                return None
            
            logger.info(f"獲取 {symbol} {timeframe} 批次數據: {len(df)} 筆 (offset={offset})")
            return df
            
        except Exception as e:
            logger.error(f"獲取 {symbol} {timeframe} 批次數據失敗: {e}")
            return None
    
    def calculate_zigzag_swings(self, df, symbol, timeframe):
        """計算ZigZag波段"""
        try:
            logger.info(f"開始計算 {symbol} {timeframe} 的ZigZag波段...")
            
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
                    'version_hash': 'regenerate_v1',  # 新版本雜湊
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
            
        except Exception as e:
            logger.error(f"計算 {symbol} {timeframe} ZigZag波段失敗: {e}")
            return None
    
    def insert_swing_data(self, conn, swing_records):
        """匯入swing_data到資料庫"""
        if not swing_records:
            return
        
        try:
            # 準備插入語句 - 包含id
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
    
    def clear_swing_data(self, conn):
        """清除現有的swing_data"""
        try:
            conn.execute("DELETE FROM swing_data")
            logger.info("已清除現有的swing_data")
        except Exception as e:
            logger.error(f"清除swing_data失敗: {e}")
            raise
    
    def process_symbol_timeframe(self, conn, symbol, timeframe):
        """處理單個品種和時間週期的波段數據"""
        logger.info(f"開始處理 {symbol} {timeframe}...")
        
        # 獲取總數據量
        total_count = self.get_total_candlestick_count(conn, symbol, timeframe)
        if total_count == 0:
            logger.warning(f"沒有找到 {symbol} {timeframe} 的K線數據")
            return 0
        
        logger.info(f"{symbol} {timeframe} 總共有 {total_count} 條K線數據")
        
        # 分批處理
        total_swings = 0
        offset = 0
        
        while offset < total_count:
            # 獲取批次數據
            df = self.get_candlestick_data_batch(conn, symbol, timeframe, offset, self.batch_size)
            if df is None:
                break
            
            # 計算波段
            swing_records = self.calculate_zigzag_swings(df, symbol, timeframe)
            if swing_records:
                # 匯入資料庫
                self.insert_swing_data(conn, swing_records)
                total_swings += len(swing_records)
            
            # 更新偏移量
            offset += self.batch_size
            
            # 進度報告
            progress = min(offset, total_count) / total_count * 100
            logger.info(f"{symbol} {timeframe} 處理進度: {progress:.1f}% ({offset}/{total_count})")
        
        logger.info(f"完成 {symbol} {timeframe}: {total_swings} 個波段")
        return total_swings
    
    def regenerate_all_swing_data(self):
        """重新生成所有波段數據"""
        logger.info("開始重新生成所有波段數據...")
        
        conn = self.connect_database()
        
        try:
            # 清除現有數據
            self.clear_swing_data(conn)
            
            total_swings = 0
            
            # 處理每個品種和時間週期
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    try:
                        swings = self.process_symbol_timeframe(conn, symbol, timeframe)
                        total_swings += swings
                    except Exception as e:
                        logger.error(f"處理 {symbol} {timeframe} 失敗: {e}")
                        continue
            
            logger.info(f"所有品種處理完成，總共 {total_swings} 個ZigZag波段")
            
            # 顯示統計資訊
            self.show_statistics(conn)
            
        except Exception as e:
            logger.error(f"重新生成過程中發生錯誤: {e}")
            raise
        finally:
            conn.close()
    
    def show_statistics(self, conn):
        """顯示統計資訊"""
        try:
            logger.info("顯示統計資訊...")
            
            # 總數統計
            total_query = "SELECT COUNT(*) FROM swing_data"
            total_count = conn.execute(total_query).fetchone()[0]
            logger.info(f"總波段數: {total_count}")
            
            # 按品種統計
            symbol_query = """
            SELECT symbol, COUNT(*) as count 
            FROM swing_data 
            GROUP BY symbol 
            ORDER BY count DESC
            """
            symbol_stats = conn.execute(symbol_query).fetchall()
            
            logger.info("按品種統計:")
            for symbol, count in symbol_stats:
                logger.info(f"  {symbol}: {count} 條")
            
            # 按時間週期統計
            timeframe_query = """
            SELECT timeframe, COUNT(*) as count 
            FROM swing_data 
            GROUP BY timeframe 
            ORDER BY count DESC
            """
            timeframe_stats = conn.execute(timeframe_query).fetchall()
            
            logger.info("按時間週期統計:")
            for timeframe, count in timeframe_stats:
                logger.info(f"  {timeframe}: {count} 條")
            
            # 詳細統計
            detail_query = """
            SELECT symbol, timeframe, COUNT(*) as count 
            FROM swing_data 
            GROUP BY symbol, timeframe 
            ORDER BY symbol, timeframe
            """
            detail_stats = conn.execute(detail_query).fetchall()
            
            logger.info("詳細統計:")
            for symbol, timeframe, count in detail_stats:
                logger.info(f"  {symbol} {timeframe}: {count} 條")
                
        except Exception as e:
            logger.error(f"顯示統計資訊失敗: {e}")

def main():
    """主函數"""
    print("🔄 波段數據重新生成工具")
    print("=" * 60)
    
    regenerator = SwingDataRegenerator()
    
    try:
        regenerator.regenerate_all_swing_data()
        print("✅ 波段數據重新生成完成!")
        
    except Exception as e:
        print(f"❌ 重新生成失敗: {e}")
        logger.error(f"重新生成失敗: {e}")

if __name__ == "__main__":
    main() 
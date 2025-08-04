#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重新生成M5時間框架的波段數據
"""

import pandas as pd
import logging
import time
from datetime import datetime
from src.database.connection import DuckDBConnection
from src.algorithms.zigzag_improved import ImprovedZigZagAlgorithm
# 移除不必要的依賴

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class M5SwingRegenerator:
    """M5波段重新生成器"""
    
    def __init__(self):
        self.db = DuckDBConnection("database/market_data.duckdb")
        self.conn = self.db.conn
        
        # 使用改進的ZigZag算法，針對M5時間框架調整參數
        self.algorithm = ImprovedZigZagAlgorithm(
            deviation=1.5,  # M5時間框架需要更敏感的偏差
            depth=10,       # 較小的深度適合M5
            min_swing_bars=3  # 最小波段條數
        )
        
        # 移除不必要的依賴
        self.batch_size = 50000  # M5數據量大，使用較小的批次
        
    def get_m5_symbols(self):
        """獲取所有有M5數據的品種"""
        query = """
        SELECT DISTINCT symbol
        FROM candlestick_data_new 
        WHERE timeframe = 'M5'
        ORDER BY symbol
        """
        
        result = self.conn.execute(query).fetchdf()
        return result['symbol'].tolist()
    
    def get_candlestick_data(self, symbol, limit=None):
        """獲取M5 K線數據"""
        query = f"""
        SELECT timestamp, open, high, low, close, volume
        FROM candlestick_data_new 
        WHERE symbol = '{symbol}' AND timeframe = 'M5'
        ORDER BY timestamp
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = self.conn.execute(query).fetchdf()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        return df
    
    def backup_existing_m5_data(self):
        """備份現有的M5波段數據"""
        timestamp = int(time.time())
        backup_table = f"swing_data_m5_backup_{timestamp}"
        
        backup_query = f"""
        CREATE TABLE {backup_table} AS 
        SELECT * FROM swing_data 
        WHERE timeframe = 'M5'
        """
        
        self.conn.execute(backup_query)
        logger.info(f"已備份現有M5數據到表 {backup_table}")
        
        # 檢查備份的數據量
        count_query = f"SELECT COUNT(*) as count FROM {backup_table}"
        count_result = self.conn.execute(count_query).fetchdf()
        backup_count = count_result.iloc[0]['count']
        logger.info(f"備份了 {backup_count} 條M5波段數據")
        
        return backup_table
    
    def clear_m5_swing_data(self):
        """清除現有的M5波段數據"""
        delete_query = "DELETE FROM swing_data WHERE timeframe = 'M5'"
        self.conn.execute(delete_query)
        logger.info("已清除現有M5波段數據")
    
    def calculate_zigzag_swings_batch(self, symbol, df):
        """批次計算ZigZag波段"""
        logger.info(f"開始計算 {symbol} M5 ZigZag波段...")
        logger.info(f"數據範圍: {df.index.min()} 到 {df.index.max()}")
        logger.info(f"總數據量: {len(df):,} 條")
        
        # 如果數據量太大，分批處理
        if len(df) > self.batch_size:
            logger.info(f"數據量較大，使用批次處理 (批次大小: {self.batch_size:,})")
            return self._calculate_zigzag_swings_batch(symbol, df)
        else:
            logger.info("數據量適中，使用單次處理")
            return self._calculate_zigzag_swings_single(symbol, df)
    
    def _calculate_zigzag_swings_single(self, symbol, df):
        """單次處理ZigZag波段"""
        try:
            # 計算ZigZag點
            zigzag_data = self.algorithm.calculate(df)
            
            if zigzag_data.empty:
                logger.warning(f"{symbol} M5: 沒有找到ZigZag點")
                return pd.DataFrame()
            
            # 計算波段信息
            zigzag_points = self.algorithm._calculate_zigzag_points(df)
            swing_data = self.algorithm._calculate_swing_info(df, zigzag_points)
            
            if swing_data.empty:
                logger.warning(f"{symbol} M5: 沒有找到有效波段")
                return pd.DataFrame()
            
            # 添加必要欄位
            swing_data['symbol'] = symbol
            swing_data['timeframe'] = 'M5'
            swing_data['algorithm_name'] = 'ImprovedZigZag'
            swing_data['version_hash'] = 'm5_regeneration_v1'
            
            logger.info(f"{symbol} M5: 找到 {len(swing_data)} 個波段點")
            return swing_data
            
        except Exception as e:
            logger.error(f"計算 {symbol} M5 ZigZag波段失敗: {e}")
            return pd.DataFrame()
    
    def _calculate_zigzag_swings_batch(self, symbol, df):
        """批次處理ZigZag波段"""
        all_swing_data = []
        total_batches = (len(df) + self.batch_size - 1) // self.batch_size
        
        logger.info(f"總批次數: {total_batches}")
        
        for i in range(0, len(df), self.batch_size):
            batch_num = i // self.batch_size + 1
            batch_df = df.iloc[i:i + self.batch_size]
            
            logger.info(f"處理批次 {batch_num}/{total_batches} ({len(batch_df):,} 條數據)")
            
            try:
                # 計算當前批次的ZigZag點
                zigzag_points = self.algorithm._calculate_zigzag_points(batch_df)
                
                if zigzag_points:
                    # 計算波段信息
                    swing_data = self.algorithm._calculate_swing_info(batch_df, zigzag_points)
                    
                    if not swing_data.empty:
                        # 添加必要欄位
                        swing_data['symbol'] = symbol
                        swing_data['timeframe'] = 'M5'
                        swing_data['algorithm_name'] = 'ImprovedZigZag'
                        swing_data['version_hash'] = 'm5_regeneration_v1'
                        
                        all_swing_data.append(swing_data)
                        logger.info(f"批次 {batch_num}: 找到 {len(swing_data)} 個波段點")
                    else:
                        logger.info(f"批次 {batch_num}: 沒有找到有效波段")
                else:
                    logger.info(f"批次 {batch_num}: 沒有找到ZigZag點")
                    
            except Exception as e:
                logger.error(f"批次 {batch_num} 處理失敗: {e}")
                continue
        
        if all_swing_data:
            combined_data = pd.concat(all_swing_data, ignore_index=True)
            combined_data = combined_data.sort_values('timestamp').reset_index(drop=True)
            logger.info(f"{symbol} M5: 總共找到 {len(combined_data)} 個波段點")
            return combined_data
        else:
            logger.warning(f"{symbol} M5: 所有批次都沒有找到有效波段")
            return pd.DataFrame()
    
    def insert_swing_data(self, swing_data):
        """插入波段數據到數據庫"""
        if swing_data.empty:
            logger.warning("沒有波段數據需要插入")
            return
        
        try:
            # 準備插入數據
            for _, row in swing_data.iterrows():
                # 處理可能的NaN值
                swing_high = float(row['swing_high']) if pd.notna(row['swing_high']) else None
                swing_low = float(row['swing_low']) if pd.notna(row['swing_low']) else None
                swing_range = float(row['swing_range']) if pd.notna(row['swing_range']) else None
                swing_duration = int(row['swing_duration']) if pd.notna(row['swing_duration']) else None
                
                insert_query = """
                INSERT INTO swing_data (
                    symbol, timeframe, algorithm_name, version_hash, timestamp,
                    zigzag_price, zigzag_type, zigzag_strength, zigzag_swing,
                    swing_high, swing_low, swing_range, swing_duration, swing_direction
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                self.conn.execute(insert_query, [
                    row['symbol'], row['timeframe'], row['algorithm_name'], 
                    row['version_hash'], row['timestamp'], row['zigzag_price'],
                    row['zigzag_type'], row['zigzag_strength'], row['zigzag_swing'],
                    swing_high, swing_low, swing_range, swing_duration, row['swing_direction']
                ])
            
            logger.info(f"成功插入 {len(swing_data)} 條波段數據")
            
        except Exception as e:
            logger.error(f"插入波段數據失敗: {e}")
            raise
    
    def regenerate_m5_swings(self):
        """重新生成所有M5波段數據"""
        logger.info("開始重新生成M5波段數據...")
        
        # 獲取所有M5品種
        symbols = self.get_m5_symbols()
        logger.info(f"找到 {len(symbols)} 個M5品種: {symbols}")
        
        # 備份現有數據
        backup_table = self.backup_existing_m5_data()
        
        # 清除現有數據
        self.clear_m5_swing_data()
        
        total_swings = 0
        
        # 處理每個品種
        for symbol in symbols:
            logger.info(f"\n{'='*50}")
            logger.info(f"處理 {symbol} M5...")
            logger.info(f"{'='*50}")
            
            try:
                # 獲取K線數據
                df = self.get_candlestick_data(symbol)
                
                if df.empty:
                    logger.warning(f"{symbol} M5: 沒有K線數據")
                    continue
                
                # 計算波段
                swing_data = self.calculate_zigzag_swings_batch(symbol, df)
                
                if not swing_data.empty:
                    # 插入數據
                    self.insert_swing_data(swing_data)
                    total_swings += len(swing_data)
                    logger.info(f"{symbol} M5: 成功生成 {len(swing_data)} 個波段")
                else:
                    logger.warning(f"{symbol} M5: 沒有生成任何波段")
                
            except Exception as e:
                logger.error(f"處理 {symbol} M5 失敗: {e}")
                continue
        
        logger.info(f"\n{'='*50}")
        logger.info(f"M5波段重新生成完成!")
        logger.info(f"總共生成: {total_swings} 個波段")
        logger.info(f"備份表: {backup_table}")
        logger.info(f"{'='*50}")
    
    def close(self):
        """關閉數據庫連接"""
        self.db.close()

def main():
    """主函數"""
    regenerator = M5SwingRegenerator()
    
    try:
        regenerator.regenerate_m5_swings()
    except Exception as e:
        logger.error(f"重新生成失敗: {e}")
        raise
    finally:
        regenerator.close()

if __name__ == "__main__":
    main() 
"""
波段處理器

整合各種波段識別演算法，計算波段資料並存儲到資料庫
"""

import pandas as pd
import numpy as np
import json
import logging
from typing import Dict, List, Optional, Any
import sys
import os
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config.settings import DUCKDB_PATH, TIMEFRAMES
from src.config.version_control import get_algorithm_parameters, get_version_description
from src.database.connection import DuckDBConnection
from src.algorithms.zigzag import ZigZagAlgorithm

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SwingProcessor:
    """波段處理器"""
    
    def __init__(self, db_path: str = str(DUCKDB_PATH)):
        self.db_path = db_path
        self.algorithms = {
            'zigzag': ZigZagAlgorithm()
        }
    
    def process_symbol_timeframe(self, symbol: str, timeframe: str, 
                               algorithm_name: str = 'zigzag', batch_size: int = 10000, limit: Optional[int] = None, **algorithm_params):
        """
        處理特定交易品種和時間週期的波段資料（分批處理）
        
        Args:
            symbol: 交易品種
            timeframe: 時間週期
            algorithm_name: 演算法名稱
            batch_size: 每批處理的資料筆數
            **algorithm_params: 演算法參數
        """
        try:
            logger.info(f"開始處理 {symbol} {timeframe} 的波段資料，使用 {algorithm_name} 演算法，批次大小: {batch_size}")
            
            # 獲取蠟燭圖資料
            with DuckDBConnection(self.db_path) as db:
                candlestick_df = db.get_candlestick_data(symbol, timeframe, limit=limit)
                
                if candlestick_df.empty:
                    logger.warning(f"沒有找到 {symbol} {timeframe} 的蠟燭圖資料")
                    return
                
                total_records = len(candlestick_df)
                logger.info(f"獲取到 {total_records} 筆蠟燭圖資料")
                
                # 執行演算法計算
                algorithm = self.algorithms.get(algorithm_name)
                if not algorithm:
                    raise ValueError(f"不支援的演算法: {algorithm_name}")
                
                # 設定演算法參數
                if algorithm_params:
                    algorithm.set_parameters(**algorithm_params)
                
                # 分批處理
                all_swing_points = []
                total_swing_points = 0
                
                # 計算需要多少批次
                num_batches = (total_records + batch_size - 1) // batch_size
                logger.info(f"將分 {num_batches} 批處理資料")
                
                for batch_num in range(num_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, total_records)
                    
                    logger.info(f"處理批次 {batch_num + 1}/{num_batches}: 索引 {start_idx} 到 {end_idx}")
                    
                    # 獲取當前批次的資料
                    batch_df = candlestick_df.iloc[start_idx:end_idx].copy()
                    
                    # 計算波段
                    result_df = algorithm.calculate(batch_df)
                    
                    # 收集波段點
                    batch_swing_points = algorithm.get_swing_points(result_df)
                    all_swing_points.extend(batch_swing_points)
                    total_swing_points += len(batch_swing_points)
                    
                    logger.info(f"批次 {batch_num + 1} 完成，找到 {len(batch_swing_points)} 個波段點")
                
                # 合併所有批次的結果並存儲到資料庫
                if all_swing_points:
                    # 創建包含所有波段點的DataFrame
                    combined_df = self._create_combined_swing_df(candlestick_df, all_swing_points)
                    
                    # 獲取演算法參數
                    algorithm_parameters = algorithm.get_parameters()
                    
                    # 生成版本描述
                    version_description = get_version_description(algorithm_name, f"{algorithm_name}_v1")
                    
                    # 存儲到資料庫（支援版本控制）
                    version_hash = db.insert_swing_data(
                        combined_df, symbol, timeframe, algorithm_name, algorithm_parameters,
                        version_name=None, description=version_description
                    )
                    
                    # 插入統計資料
                    stats = self._calculate_combined_statistics(all_swing_points)
                    db.insert_algorithm_statistics(symbol, timeframe, algorithm_name, version_hash, stats)
                
                # 計算統計資訊
                stats = self._calculate_combined_statistics(all_swing_points)
                
                logger.info(f"波段處理完成: {symbol} {timeframe} {algorithm_name}")
                logger.info(f"總共找到 {total_swing_points} 個波段點")
                logger.info(f"統計資訊: {stats}")
                
                return {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'algorithm': algorithm_name,
                    'total_records': total_records,
                    'swing_points': total_swing_points,
                    'statistics': stats,
                    'batches_processed': num_batches
                }
                
        except Exception as e:
            logger.error(f"處理波段資料失敗: {e}")
            raise
    
    def process_all_timeframes(self, symbol: str, algorithm_name: str = 'zigzag', 
                             timeframes: Optional[List[str]] = None, batch_size: int = 10000, **algorithm_params):
        """
        處理所有時間週期的波段資料
        
        Args:
            symbol: 交易品種
            algorithm_name: 演算法名稱
            timeframes: 要處理的時間週期列表，如果為None則處理所有可用時間週期
            **algorithm_params: 演算法參數
        """
        try:
            logger.info(f"開始處理 {symbol} 的所有時間週期波段資料")
            
            with DuckDBConnection(self.db_path) as db:
                # 獲取可用的時間週期
                if timeframes is None:
                    available_timeframes = db.get_available_timeframes(symbol)
                else:
                    available_timeframes = [tf for tf in timeframes if tf in TIMEFRAMES]
                
                if not available_timeframes:
                    logger.warning(f"沒有找到 {symbol} 的可用時間週期")
                    return []
                
                results = []
                
                for timeframe in available_timeframes:
                    try:
                        result = self.process_symbol_timeframe(
                            symbol, timeframe, algorithm_name, batch_size, **algorithm_params
                        )
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.error(f"處理 {symbol} {timeframe} 失敗: {e}")
                        continue
                
                logger.info(f"完成處理 {symbol} 的波段資料，成功處理 {len(results)} 個時間週期")
                return results
                
        except Exception as e:
            logger.error(f"處理所有時間週期失敗: {e}")
            raise
    
    def get_swing_data(self, symbol: str, timeframe: str, algorithm: str,
                      start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        獲取波段資料
        
        Args:
            symbol: 交易品種
            timeframe: 時間週期
            algorithm: 演算法名稱
            start_date: 開始日期
            end_date: 結束日期
            
        Returns:
            波段資料DataFrame
        """
        try:
            with DuckDBConnection(self.db_path) as db:
                return db.get_swing_data(symbol, timeframe, algorithm, start_date, end_date)
        except Exception as e:
            logger.error(f"獲取波段資料失敗: {e}")
            raise
    
    def get_available_algorithms(self, symbol: str, timeframe: str) -> List[str]:
        """
        獲取可用的演算法
        
        Args:
            symbol: 交易品種
            timeframe: 時間週期
            
        Returns:
            可用演算法列表
        """
        try:
            with DuckDBConnection(self.db_path) as db:
                return db.get_available_algorithms(symbol, timeframe)
        except Exception as e:
            logger.error(f"獲取可用演算法失敗: {e}")
            return []
    
    def get_swing_statistics(self, symbol: str, timeframe: str, algorithm: str):
        """
        獲取波段統計資訊
        
        Args:
            symbol: 交易品種
            timeframe: 時間週期
            algorithm: 演算法名稱
            
        Returns:
            統計資訊字典
        """
        try:
            swing_df = self.get_swing_data(symbol, timeframe, algorithm)
            
            if swing_df.empty:
                return {
                    'total_swings': 0,
                    'avg_swing_range': 0,
                    'avg_swing_duration': 0,
                    'max_swing_range': 0,
                    'min_swing_range': 0
                }
            
            # 計算統計資訊
            swing_points = []
            for idx, row in swing_df.iterrows():
                zigzag_price = row['zigzag_price']
                if zigzag_price is not None and not (isinstance(zigzag_price, float) and np.isnan(zigzag_price)):
                    swing_points.append({
                        'timestamp': idx,
                        'price': zigzag_price,
                        'type': row['zigzag_type'],
                        'strength': row['zigzag_strength']
                    })
            
            if len(swing_points) < 2:
                return {
                    'total_swings': 0,
                    'avg_swing_range': 0,
                    'avg_swing_duration': 0,
                    'max_swing_range': 0,
                    'min_swing_range': 0
                }
            
            # 計算波段統計
            swing_ranges = []
            swing_durations = []
            
            for i in range(len(swing_points) - 1):
                current = swing_points[i]
                next_point = swing_points[i + 1]
                
                range_val = abs(next_point['price'] - current['price'])
                
                # 確保 timestamp 是 pandas.Timestamp 型別
                current_timestamp = current['timestamp']
                next_timestamp = next_point['timestamp']
                
                if isinstance(current_timestamp, (int, float)):
                    current_timestamp = pd.to_datetime(current_timestamp, unit='s')
                elif isinstance(current_timestamp, str):
                    try:
                        current_timestamp = pd.to_datetime(current_timestamp)
                    except:
                        current_timestamp = pd.to_datetime(float(current_timestamp), unit='s')
                elif not isinstance(current_timestamp, pd.Timestamp):
                    current_timestamp = pd.to_datetime(current_timestamp)
                    
                if isinstance(next_timestamp, (int, float)):
                    next_timestamp = pd.to_datetime(next_timestamp, unit='s')
                elif isinstance(next_timestamp, str):
                    try:
                        next_timestamp = pd.to_datetime(next_timestamp)
                    except:
                        next_timestamp = pd.to_datetime(float(next_timestamp), unit='s')
                elif not isinstance(next_timestamp, pd.Timestamp):
                    next_timestamp = pd.to_datetime(next_timestamp)
                
                duration = (next_timestamp - current_timestamp).total_seconds() / 3600  # 小時
                
                swing_ranges.append(range_val)
                swing_durations.append(duration)
            
            return {
                'total_swings': len(swing_points) - 1,
                'avg_swing_range': np.mean(swing_ranges),
                'avg_swing_duration': np.mean(swing_durations),
                'max_swing_range': np.max(swing_ranges),
                'min_swing_range': np.min(swing_ranges),
                'swing_ranges': swing_ranges,
                'swing_durations': swing_durations
            }
            
        except Exception as e:
            logger.error(f"獲取波段統計失敗: {e}")
            raise
    
    def _create_combined_swing_df(self, candlestick_df: pd.DataFrame, swing_points: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        創建包含所有波段點的DataFrame
        
        Args:
            candlestick_df: 原始蠟燭圖資料
            swing_points: 波段點列表
            
        Returns:
            包含波段點的DataFrame
        """
        # 複製原始資料
        result_df = candlestick_df.copy()

        # 修正：確保 index 唯一，避免 reindexing only valid with uniquely valued Index objects
        if not result_df.index.is_unique:
            result_df = result_df.loc[~result_df.index.duplicated(keep='first')].copy()

        # 初始化ZigZag相關欄位
        result_df['zigzag_price'] = np.nan
        result_df['zigzag_type'] = None
        result_df['zigzag_strength'] = np.nan
        result_df['zigzag_swing'] = np.nan
        result_df['swing_high'] = np.nan
        result_df['swing_low'] = np.nan
        result_df['swing_range'] = np.nan
        result_df['swing_duration'] = np.nan
        result_df['swing_direction'] = None
        
        # 填充波段點
        for i, point in enumerate(swing_points):
            timestamp = point['timestamp']
            # 強制轉換 timestamp 為 pandas.Timestamp 型別
            if isinstance(timestamp, (int, float)):
                # 如果是數字，視為 unix timestamp
                timestamp = pd.to_datetime(timestamp, unit='s')
            elif isinstance(timestamp, str):
                # 如果是字串，嘗試解析
                try:
                    timestamp = pd.to_datetime(timestamp)
                except:
                    # 如果解析失敗，嘗試作為 unix timestamp
                    timestamp = pd.to_datetime(float(timestamp), unit='s')
            elif not isinstance(timestamp, pd.Timestamp):
                # 其他型別，強制轉換
                timestamp = pd.to_datetime(timestamp)
            # 找到最接近的時間戳
            if timestamp in result_df.index:
                result_df.loc[timestamp, 'zigzag_price'] = point['price']
                result_df.loc[timestamp, 'zigzag_type'] = point['type']
                result_df.loc[timestamp, 'zigzag_strength'] = point['strength']
                result_df.loc[timestamp, 'zigzag_swing'] = i
            else:
                # 如果找不到完全匹配的時間戳，找到最接近的
                try:
                    closest_idx = result_df.index.get_indexer([timestamp], method='nearest')[0]
                    if closest_idx >= 0:
                        actual_timestamp = result_df.index[closest_idx]
                        result_df.loc[actual_timestamp, 'zigzag_price'] = point['price']
                        result_df.loc[actual_timestamp, 'zigzag_type'] = point['type']
                        result_df.loc[actual_timestamp, 'zigzag_strength'] = point['strength']
                        result_df.loc[actual_timestamp, 'zigzag_swing'] = i
                except Exception:
                    continue
        
        # 計算波段資訊
        result_df = self._calculate_swing_info_from_points(result_df, swing_points)
        
        return result_df
    
    def _calculate_swing_info_from_points(self, df: pd.DataFrame, swing_points: List[Dict[str, Any]]) -> pd.DataFrame:
        """從波段點計算波段資訊"""
        if len(swing_points) < 2:
            return df
        
        # 計算波段資訊
        for i in range(len(swing_points) - 1):
            current_point = swing_points[i]
            next_point = swing_points[i + 1]
            
            current_timestamp = current_point['timestamp']
            next_timestamp = next_point['timestamp']
            
            # 強制轉換 timestamp 為 pandas.Timestamp 型別
            if isinstance(current_timestamp, (int, float)):
                current_timestamp = pd.to_datetime(current_timestamp, unit='s')
            elif isinstance(current_timestamp, str):
                try:
                    current_timestamp = pd.to_datetime(current_timestamp)
                except:
                    current_timestamp = pd.to_datetime(float(current_timestamp), unit='s')
            elif not isinstance(current_timestamp, pd.Timestamp):
                current_timestamp = pd.to_datetime(current_timestamp)
                
            if isinstance(next_timestamp, (int, float)):
                next_timestamp = pd.to_datetime(next_timestamp, unit='s')
            elif isinstance(next_timestamp, str):
                try:
                    next_timestamp = pd.to_datetime(next_timestamp)
                except:
                    next_timestamp = pd.to_datetime(float(next_timestamp), unit='s')
            elif not isinstance(next_timestamp, pd.Timestamp):
                next_timestamp = pd.to_datetime(next_timestamp)
            
            # 找到時間範圍內的所有索引
            mask = (df.index >= current_timestamp) & (df.index <= next_timestamp)
            
            # 波段範圍
            if current_point['type'] == 'low' and next_point['type'] == 'high':
                # 上升波段
                df.loc[mask, 'swing_direction'] = 'up'
                df.loc[mask, 'swing_low'] = current_point['price']
                df.loc[mask, 'swing_high'] = next_point['price']
                df.loc[mask, 'swing_range'] = next_point['price'] - current_point['price']
                
            elif current_point['type'] == 'high' and next_point['type'] == 'low':
                # 下降波段
                df.loc[mask, 'swing_direction'] = 'down'
                df.loc[mask, 'swing_high'] = current_point['price']
                df.loc[mask, 'swing_low'] = next_point['price']
                df.loc[mask, 'swing_range'] = current_point['price'] - next_point['price']
            
            # 波段持續時間
            # 確保 timestamp 是 pandas.Timestamp 型別
            if isinstance(current_timestamp, (int, float)):
                current_timestamp = pd.to_datetime(current_timestamp, unit='s')
            elif isinstance(current_timestamp, str):
                current_timestamp = pd.to_datetime(current_timestamp)
                
            if isinstance(next_timestamp, (int, float)):
                next_timestamp = pd.to_datetime(next_timestamp, unit='s')
            elif isinstance(next_timestamp, str):
                next_timestamp = pd.to_datetime(next_timestamp)
            
            duration = (next_timestamp - current_timestamp).total_seconds() / 3600  # 小時
            df.loc[mask, 'swing_duration'] = duration
        
        return df
    
    def _calculate_combined_statistics(self, swing_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        計算合併後的統計資訊
        
        Args:
            swing_points: 波段點列表
            
        Returns:
            統計資訊字典
        """
        if len(swing_points) < 2:
            return {
                'total_swings': 0,
                'avg_swing_range': 0,
                'avg_swing_duration': 0,
                'max_swing_range': 0,
                'min_swing_range': 0
            }
        
        # 計算波段統計
        swing_ranges = []
        swing_durations = []
        
        for i in range(len(swing_points) - 1):
            current = swing_points[i]
            next_point = swing_points[i + 1]
            
            range_val = abs(next_point['price'] - current['price'])
            
            # 確保 timestamp 是 pandas.Timestamp 型別
            current_timestamp = current['timestamp']
            next_timestamp = next_point['timestamp']
            
            # 強制轉換為 pandas.Timestamp
            if isinstance(current_timestamp, (int, float)):
                current_timestamp = pd.to_datetime(current_timestamp, unit='s')
            elif isinstance(current_timestamp, str):
                try:
                    current_timestamp = pd.to_datetime(current_timestamp)
                except:
                    current_timestamp = pd.to_datetime(float(current_timestamp), unit='s')
            elif not isinstance(current_timestamp, pd.Timestamp):
                current_timestamp = pd.to_datetime(current_timestamp)
                
            if isinstance(next_timestamp, (int, float)):
                next_timestamp = pd.to_datetime(next_timestamp, unit='s')
            elif isinstance(next_timestamp, str):
                try:
                    next_timestamp = pd.to_datetime(next_timestamp)
                except:
                    next_timestamp = pd.to_datetime(float(next_timestamp), unit='s')
            elif not isinstance(next_timestamp, pd.Timestamp):
                next_timestamp = pd.to_datetime(next_timestamp)
            
            duration = (next_timestamp - current_timestamp).total_seconds() / 3600  # 小時
            
            swing_ranges.append(range_val)
            swing_durations.append(duration)
        
        return {
            'total_swings': len(swing_points) - 1,
            'avg_swing_range': np.mean(swing_ranges),
            'avg_swing_duration': np.mean(swing_durations),
            'max_swing_range': np.max(swing_ranges),
            'min_swing_range': np.min(swing_ranges),
            'swing_ranges': swing_ranges,
            'swing_durations': swing_durations
        }


def main():
    """主函數 - 用於測試和批量處理"""
    import argparse
    
    parser = argparse.ArgumentParser(description='波段資料處理器')
    parser.add_argument('--symbol', type=str, default='EXUSA30IDXUSD', help='交易品種')
    parser.add_argument('--timeframe', type=str, help='時間週期 (可選，不指定則處理所有時間週期)')
    parser.add_argument('--algorithm', type=str, default='zigzag', help='演算法名稱')
    parser.add_argument('--deviation', type=float, default=5.0, help='ZigZag最小變動百分比')
    parser.add_argument('--depth', type=int, default=12, help='ZigZag回溯深度')
    parser.add_argument('--batch-size', type=int, default=10000, help='每批處理的資料筆數')
    
    args = parser.parse_args()
    
    processor = SwingProcessor()
    
    if args.timeframe:
        # 處理單一時間週期
        result = processor.process_symbol_timeframe(
            args.symbol, args.timeframe, args.algorithm,
            batch_size=args.batch_size,
            deviation=args.deviation, depth=args.depth
        )
        print(f"處理結果: {result}")
    else:
        # 處理所有時間週期
        results = processor.process_all_timeframes(
            args.symbol, args.algorithm,
            batch_size=args.batch_size,
            deviation=args.deviation, depth=args.depth
        )
        print(f"處理結果: {results}")


if __name__ == "__main__":
    main() 
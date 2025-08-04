"""
ZigZag演算法實現

ZigZag是一種經典的技術分析工具，用於識別市場的重要轉折點。
它通過設定最小價格變動百分比來過濾雜訊，只保留重要的高點和低點。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging

from .base import BaseAlgorithm

# 設定日誌
logger = logging.getLogger(__name__)


class ZigZagAlgorithm(BaseAlgorithm):
    """ZigZag波段識別演算法"""
    
    def __init__(self, deviation: float = 5.0, depth: int = 12):
        """
        初始化ZigZag演算法
        
        Args:
            deviation: 最小價格變動百分比 (預設5%)
            depth: 回溯深度，用於確認轉折點 (預設12個bar)
        """
        super().__init__(
            name="ZigZag",
            description="經典的轉折點識別算法，通過設定最小價格變動來過濾雜訊"
        )
        
        self.deviation = deviation
        self.depth = depth
        
        # 設定參數
        self.set_parameters(
            deviation=deviation,
            depth=depth
        )
    
    def calculate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        執行ZigZag演算法計算
        
        Args:
            df: 包含OHLCV資料的DataFrame
            **kwargs: 可選參數 (deviation, depth)
            
        Returns:
            包含ZigZag結果的DataFrame
        """
        # 更新參數
        if kwargs:
            self.set_parameters(**kwargs)
            self.deviation = self.parameters.get('deviation', self.deviation)
            self.depth = self.parameters.get('depth', self.depth)
        
        # 驗證資料
        if not self.validate_data(df):
            raise ValueError("輸入資料格式無效")
        
        try:
            # 複製資料避免修改原始資料
            result_df = df.copy()
            
            # 計算ZigZag點
            zigzag_points = self._calculate_zigzag_points(result_df)
            
            # 添加ZigZag相關欄位
            result_df['zigzag_price'] = np.nan
            result_df['zigzag_type'] = None  # 'high', 'low', None
            result_df['zigzag_strength'] = np.nan  # 轉折點強度
            result_df['zigzag_swing'] = np.nan  # 波段編號
            
            # 填充ZigZag點
            swing_count = 0
            for i, point in enumerate(zigzag_points):
                idx = point['index']
                result_df.loc[idx, 'zigzag_price'] = point['price']
                result_df.loc[idx, 'zigzag_type'] = point['type']
                result_df.loc[idx, 'zigzag_strength'] = point['strength']
                result_df.loc[idx, 'zigzag_swing'] = swing_count
                swing_count += 1
            
            # 計算波段資訊
            result_df = self._calculate_swing_info(result_df, zigzag_points)
            
            logger.info(f"ZigZag計算完成，找到 {len(zigzag_points)} 個轉折點")
            
            return result_df
            
        except Exception as e:
            logger.error(f"ZigZag計算失敗: {e}")
            raise
    
    def _calculate_zigzag_points(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        計算ZigZag轉折點
        
        Args:
            df: OHLCV資料
            
        Returns:
            轉折點列表
        """
        points = []
        n = len(df)
        
        if n < 3:
            return points
        
        # 初始化
        high_prices = df['high'].values
        low_prices = df['low'].values
        close_prices = df['close'].values
        
        # 尋找第一個轉折點
        first_point = self._find_first_point(high_prices, low_prices, close_prices)
        if first_point is None:
            return points
        
        points.append(first_point)
        current_type = first_point['type']  # 'high' 或 'low'
        current_price = first_point['price']
        current_index = first_point['index']
        
        # 尋找後續轉折點
        i = current_index + 1
        while i < n - 1:
            if current_type == 'high':
                # 尋找下一個低點
                next_point = self._find_next_low(
                    low_prices, high_prices, i, current_price, current_type
                )
            else:
                # 尋找下一個高點
                next_point = self._find_next_high(
                    high_prices, low_prices, i, current_price, current_type
                )
            
            if next_point is None:
                break
            
            points.append(next_point)
            current_type = next_point['type']
            current_price = next_point['price']
            current_index = next_point['index']
            i = current_index + 1
        
        return points
    
    def _find_first_point(self, high_prices: np.ndarray, low_prices: np.ndarray, 
                         close_prices: np.ndarray) -> Optional[Dict[str, Any]]:
        """尋找第一個轉折點"""
        n = len(high_prices)
        
        # 在前depth個bar中尋找極值
        search_range = min(self.depth, n)
        
        # 尋找最高點和最低點
        high_idx = np.argmax(high_prices[:search_range])
        low_idx = np.argmin(low_prices[:search_range])
        
        # 比較哪個更極端
        high_extreme = high_prices[high_idx]
        low_extreme = low_prices[low_idx]
        
        # 計算相對變動
        mid_price = (high_extreme + low_extreme) / 2
        high_deviation = abs(high_extreme - mid_price) / mid_price * 100
        low_deviation = abs(low_extreme - mid_price) / mid_price * 100
        
        if high_deviation > low_deviation:
            return {
                'index': high_idx,
                'price': high_extreme,
                'type': 'high',
                'strength': high_deviation
            }
        else:
            return {
                'index': low_idx,
                'price': low_extreme,
                'type': 'low',
                'strength': low_deviation
            }
    
    def _find_next_high(self, high_prices: np.ndarray, low_prices: np.ndarray,
                       start_idx: int, last_price: float, last_type: str) -> Optional[Dict[str, Any]]:
        """尋找下一個高點"""
        n = len(high_prices)
        
        # 計算最小價格變動
        min_change = last_price * self.deviation / 100
        
        # 尋找高點
        for i in range(start_idx, n):
            current_high = high_prices[i]
            
            # 檢查是否超過最小變動
            if current_high > last_price + min_change:
                # 確認這是局部最高點
                if self._is_local_high(high_prices, i):
                    return {
                        'index': i,
                        'price': current_high,
                        'type': 'high',
                        'strength': abs(current_high - last_price) / last_price * 100
                    }
        
        return None
    
    def _find_next_low(self, low_prices: np.ndarray, high_prices: np.ndarray,
                      start_idx: int, last_price: float, last_type: str) -> Optional[Dict[str, Any]]:
        """尋找下一個低點"""
        n = len(low_prices)
        
        # 計算最小價格變動
        min_change = last_price * self.deviation / 100
        
        # 尋找低點
        for i in range(start_idx, n):
            current_low = low_prices[i]
            
            # 檢查是否超過最小變動
            if current_low < last_price - min_change:
                # 確認這是局部最低點
                if self._is_local_low(low_prices, i):
                    return {
                        'index': i,
                        'price': current_low,
                        'type': 'low',
                        'strength': abs(current_low - last_price) / last_price * 100
                    }
        
        return None
    
    def _is_local_high(self, high_prices: np.ndarray, idx: int) -> bool:
        """檢查是否為局部最高點"""
        n = len(high_prices)
        current_high = high_prices[idx]
        
        # 檢查前後depth個bar
        start = max(0, idx - self.depth)
        end = min(n, idx + self.depth + 1)
        
        for i in range(start, end):
            if i != idx and high_prices[i] >= current_high:
                return False
        
        return True
    
    def _is_local_low(self, low_prices: np.ndarray, idx: int) -> bool:
        """檢查是否為局部最低點"""
        n = len(low_prices)
        current_low = low_prices[idx]
        
        # 檢查前後depth個bar
        start = max(0, idx - self.depth)
        end = min(n, idx + self.depth + 1)
        
        for i in range(start, end):
            if i != idx and low_prices[i] <= current_low:
                return False
        
        return True
    
    def _calculate_swing_info(self, df: pd.DataFrame, zigzag_points: List[Dict[str, Any]]) -> pd.DataFrame:
        """計算波段資訊"""
        if len(zigzag_points) < 2:
            return df
        
        # 添加波段資訊欄位
        df['swing_high'] = np.nan
        df['swing_low'] = np.nan
        df['swing_range'] = np.nan
        df['swing_duration'] = np.nan
        df['swing_direction'] = None  # 'up', 'down'
        
        # 計算波段資訊
        for i in range(len(zigzag_points) - 1):
            current_point = zigzag_points[i]
            next_point = zigzag_points[i + 1]
            
            current_idx = current_point['index']
            next_idx = next_point['index']
            
            # 波段範圍
            if current_point['type'] == 'low' and next_point['type'] == 'high':
                # 上升波段
                df.loc[current_idx:next_idx, 'swing_direction'] = 'up'
                df.loc[current_idx:next_idx, 'swing_low'] = current_point['price']
                df.loc[current_idx:next_idx, 'swing_high'] = next_point['price']
                df.loc[current_idx:next_idx, 'swing_range'] = next_point['price'] - current_point['price']
                
            elif current_point['type'] == 'high' and next_point['type'] == 'low':
                # 下降波段
                df.loc[current_idx:next_idx, 'swing_direction'] = 'down'
                df.loc[current_idx:next_idx, 'swing_high'] = current_point['price']
                df.loc[current_idx:next_idx, 'swing_low'] = next_point['price']
                df.loc[current_idx:next_idx, 'swing_range'] = current_point['price'] - next_point['price']
            
            # 波段持續時間
            duration = next_idx - current_idx
            df.loc[current_idx:next_idx, 'swing_duration'] = duration
        
        return df
    
    def get_swing_points(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        獲取波段轉折點
        
        Args:
            df: 包含ZigZag計算結果的DataFrame
            
        Returns:
            轉折點列表
        """
        swing_points = []
        
        # 找出所有ZigZag點
        zigzag_mask = df['zigzag_price'].notna()
        zigzag_data = df[zigzag_mask]
        
        for idx, row in zigzag_data.iterrows():
            swing_points.append({
                'timestamp': idx,
                'price': row['zigzag_price'],
                'type': row['zigzag_type'],
                'strength': row['zigzag_strength'],
                'swing_number': row['zigzag_swing']
            })
        
        return swing_points
    
    def get_swing_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        獲取波段統計資訊
        
        Args:
            df: 包含ZigZag計算結果的DataFrame
            
        Returns:
            統計資訊字典
        """
        swing_points = self.get_swing_points(df)
        
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
            duration = (next_point['timestamp'] - current['timestamp']).total_seconds() / 3600  # 小時
            
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
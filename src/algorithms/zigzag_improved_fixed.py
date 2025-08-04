"""
修復版本的改進ZigZag演算法實現

解決SettingWithCopyWarning和切片問題，針對長期數據優化的ZigZag演算法
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging

try:
    from .base import BaseAlgorithm
except ImportError:
    from base import BaseAlgorithm

# 設定日誌
logger = logging.getLogger(__name__)


class ImprovedZigZagAlgorithmFixed(BaseAlgorithm):
    """修復版本的改進ZigZag波段識別演算法"""
    
    def __init__(self, deviation: float = 2.0, depth: int = 15, min_swing_bars: int = 3):
        """
        初始化修復版本的改進ZigZag演算法
        
        Args:
            deviation: 最小價格變動百分比 (預設2%，降低要求)
            depth: 回溯深度，用於確認轉折點 (預設15個bar)
            min_swing_bars: 最小波段持續時間 (預設3個bar，降低要求)
        """
        super().__init__(
            name="ImprovedZigZagFixed",
            description="修復版本的針對長期數據優化的轉折點識別算法"
        )
        
        self.deviation = deviation
        self.depth = depth
        self.min_swing_bars = min_swing_bars
        
        # 設定參數
        self.set_parameters(
            deviation=deviation,
            depth=depth,
            min_swing_bars=min_swing_bars
        )
    
    def calculate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        執行修復版本的改進ZigZag演算法計算
        
        Args:
            df: 包含OHLCV資料的DataFrame
            **kwargs: 可選參數
            
        Returns:
            包含ZigZag結果的DataFrame
        """
        # 更新參數
        if kwargs:
            self.set_parameters(**kwargs)
            self.deviation = self.parameters.get('deviation', self.deviation)
            self.depth = self.parameters.get('depth', self.depth)
            self.min_swing_bars = self.parameters.get('min_swing_bars', self.min_swing_bars)
        
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
            result_df['zigzag_type'] = None
            result_df['zigzag_strength'] = np.nan
            result_df['zigzag_swing'] = np.nan
            
            # 填充ZigZag點
            swing_count = 0
            for i, point in enumerate(zigzag_points):
                idx = point['index']
                result_df.loc[idx, 'zigzag_price'] = point['price']
                result_df.loc[idx, 'zigzag_type'] = point['type']
                result_df.loc[idx, 'zigzag_strength'] = point['strength']
                result_df.loc[idx, 'zigzag_swing'] = swing_count
                swing_count += 1
            
            # 計算波段資訊（修復版本）
            result_df = self._calculate_swing_info_fixed(result_df, zigzag_points)
            
            logger.info(f"修復版本改進ZigZag計算完成，找到 {len(zigzag_points)} 個轉折點")
            
            return result_df
            
        except Exception as e:
            logger.error(f"修復版本改進ZigZag計算失敗: {e}")
            raise
    
    def _calculate_zigzag_points(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        計算改進的ZigZag轉折點
        """
        points = []
        n = len(df)
        
        if n < 3:
            return points
        
        # 初始化
        high_prices = df['high'].values
        low_prices = df['low'].values
        close_prices = df['close'].values
        
        # 尋找第一個轉折點（使用更長的搜索範圍）
        first_point = self._find_first_point_improved(high_prices, low_prices, close_prices)
        if first_point is None:
            return points
        
        points.append(first_point)
        current_type = first_point['type']
        current_price = first_point['price']
        current_index = first_point['index']
        
        # 尋找後續轉折點
        i = current_index + self.min_swing_bars  # 確保最小波段持續時間
        while i < n - self.min_swing_bars:
            if current_type == 'high':
                # 尋找下一個低點
                next_point = self._find_next_low_improved(
                    low_prices, high_prices, i, current_price, current_type
                )
            else:
                # 尋找下一個高點
                next_point = self._find_next_high_improved(
                    high_prices, low_prices, i, current_price, current_type
                )
            
            if next_point is None:
                break
            
            points.append(next_point)
            current_type = next_point['type']
            current_price = next_point['price']
            current_index = next_point['index']
            i = current_index + self.min_swing_bars
        
        return points
    
    def _find_first_point_improved(self, high_prices: np.ndarray, low_prices: np.ndarray, 
                                 close_prices: np.ndarray) -> Optional[Dict[str, Any]]:
        """改進的第一個轉折點尋找"""
        n = len(high_prices)
        
        # 使用更長的搜索範圍，搜索前1/3的數據
        search_range = min(max(self.depth * 3, 100), n // 3)
        
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
    
    def _find_next_high_improved(self, high_prices: np.ndarray, low_prices: np.ndarray,
                               start_idx: int, last_price: float, last_type: str) -> Optional[Dict[str, Any]]:
        """改進的下一個高點尋找"""
        n = len(high_prices)
        
        # 計算最小價格變動
        min_change = last_price * self.deviation / 100
        
        # 尋找高點
        for i in range(start_idx, n - self.min_swing_bars):
            current_high = high_prices[i]
            
            # 檢查是否超過最小變動
            if current_high > last_price + min_change:
                # 使用改進的局部最高點檢查
                if self._is_local_high_improved(high_prices, i):
                    return {
                        'index': i,
                        'price': current_high,
                        'type': 'high',
                        'strength': abs(current_high - last_price) / last_price * 100
                    }
        
        return None
    
    def _find_next_low_improved(self, low_prices: np.ndarray, high_prices: np.ndarray,
                              start_idx: int, last_price: float, last_type: str) -> Optional[Dict[str, Any]]:
        """改進的下一個低點尋找"""
        n = len(low_prices)
        
        # 計算最小價格變動
        min_change = last_price * self.deviation / 100
        
        # 尋找低點
        for i in range(start_idx, n - self.min_swing_bars):
            current_low = low_prices[i]
            
            # 檢查是否超過最小變動
            if current_low < last_price - min_change:
                # 使用改進的局部最低點檢查
                if self._is_local_low_improved(low_prices, i):
                    return {
                        'index': i,
                        'price': current_low,
                        'type': 'low',
                        'strength': abs(current_low - last_price) / last_price * 100
                    }
        
        return None
    
    def _is_local_high_improved(self, high_prices: np.ndarray, idx: int) -> bool:
        """改進的局部最高點檢查"""
        n = len(high_prices)
        current_high = high_prices[idx]
        
        # 使用更寬鬆的檢查範圍
        check_range = min(self.depth, 8)  # 最多檢查前後8個bar
        
        # 檢查前後check_range個bar
        start = max(0, idx - check_range)
        end = min(n, idx + check_range + 1)
        
        # 計算在檢查範圍內有多少個bar比當前bar高
        higher_count = 0
        for i in range(start, end):
            if i != idx and high_prices[i] >= current_high:
                higher_count += 1
        
        # 如果只有少數bar比當前bar高，則認為是局部最高點
        return higher_count <= 3  # 允許最多3個bar比當前bar高
    
    def _is_local_low_improved(self, low_prices: np.ndarray, idx: int) -> bool:
        """改進的局部最低點檢查"""
        n = len(low_prices)
        current_low = low_prices[idx]
        
        # 使用更寬鬆的檢查範圍
        check_range = min(self.depth, 8)  # 最多檢查前後8個bar
        
        # 檢查前後check_range個bar
        start = max(0, idx - check_range)
        end = min(n, idx + check_range + 1)
        
        # 計算在檢查範圍內有多少個bar比當前bar低
        lower_count = 0
        for i in range(start, end):
            if i != idx and low_prices[i] <= current_low:
                lower_count += 1
        
        # 如果只有少數bar比當前bar低，則認為是局部最低點
        return lower_count <= 3  # 允許最多3個bar比當前bar低
    
    def _calculate_swing_info_fixed(self, df: pd.DataFrame, zigzag_points: List[Dict[str, Any]]) -> pd.DataFrame:
        """修復版本的波段資訊計算，避免SettingWithCopyWarning"""
        if len(zigzag_points) < 2:
            return df
        
        # 創建新的DataFrame避免鏈式索引警告
        result_df = df.copy()
        
        # 添加波段資訊欄位
        result_df['swing_high'] = np.nan
        result_df['swing_low'] = np.nan
        result_df['swing_range'] = np.nan
        result_df['swing_duration'] = np.nan
        result_df['swing_direction'] = None
        
        # 計算波段資訊 - 使用iloc而不是loc切片
        for i in range(len(zigzag_points) - 1):
            current_point = zigzag_points[i]
            next_point = zigzag_points[i + 1]
            
            current_idx = current_point['index']
            next_idx = next_point['index']
            
            # 確保索引在有效範圍內
            if current_idx >= len(result_df) or next_idx >= len(result_df):
                continue
            
            # 波段範圍
            if current_point['type'] == 'low' and next_point['type'] == 'high':
                # 上升波段 - 使用iloc避免切片警告
                for j in range(current_idx, next_idx + 1):
                    if j < len(result_df):
                        result_df.iloc[j, result_df.columns.get_loc('swing_direction')] = 'up'
                        result_df.iloc[j, result_df.columns.get_loc('swing_low')] = current_point['price']
                        result_df.iloc[j, result_df.columns.get_loc('swing_high')] = next_point['price']
                        result_df.iloc[j, result_df.columns.get_loc('swing_range')] = next_point['price'] - current_point['price']
                
            elif current_point['type'] == 'high' and next_point['type'] == 'low':
                # 下降波段 - 使用iloc避免切片警告
                for j in range(current_idx, next_idx + 1):
                    if j < len(result_df):
                        result_df.iloc[j, result_df.columns.get_loc('swing_direction')] = 'down'
                        result_df.iloc[j, result_df.columns.get_loc('swing_high')] = current_point['price']
                        result_df.iloc[j, result_df.columns.get_loc('swing_low')] = next_point['price']
                        result_df.iloc[j, result_df.columns.get_loc('swing_range')] = current_point['price'] - next_point['price']
            
            # 波段持續時間
            duration = next_idx - current_idx
            for j in range(current_idx, next_idx + 1):
                if j < len(result_df):
                    result_df.iloc[j, result_df.columns.get_loc('swing_duration')] = duration
        
        return result_df
    
    def get_swing_points(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """獲取波段轉折點"""
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
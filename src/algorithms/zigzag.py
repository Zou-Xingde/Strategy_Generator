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
    
    def __init__(self, deviation: float = 3.0, depth: int = 12, min_swing_bars: int = 2):
        """
        初始化ZigZag演算法
        
        Args:
            deviation: 最小價格變動百分比 (預設3%)
            depth: 回溯深度，用於確認轉折點 (預設12個bar)
            min_swing_bars: 最小波段持續時間 (預設2個bar)
        """
        super().__init__(
            name="ZigZag",
            description="改進的轉折點識別算法，支援更多參數控制"
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
            self.min_swing_bars = self.parameters.get('min_swing_bars', self.min_swing_bars)
        
        # 🔍 監控日誌 - 參數資訊
        logger.info(f"🎯 ZigZag計算開始 - 參數: deviation={self.deviation}%, depth={self.depth}, min_swing_bars={self.min_swing_bars}")
        logger.info(f"📊 輸入資料: {len(df)} 筆K線，日期範圍: {df.index[0]} ~ {df.index[-1]}")
        logger.info(f"💰 價格範圍: 最高={df['high'].max():.5f}, 最低={df['low'].min():.5f}")
        
        # 驗證資料
        if not self.validate_data(df):
            raise ValueError("輸入資料格式無效")
        
        try:
            # 複製資料避免修改原始資料
            result_df = df.copy()
            
            # 計算ZigZag點
            logger.info("🔍 開始尋找ZigZag轉折點...")
            zigzag_points = self._calculate_zigzag_points(result_df)
            
            # 🔍 監控日誌 - 轉折點詳情
            logger.info(f"✅ 找到 {len(zigzag_points)} 個轉折點")
            for i, point in enumerate(zigzag_points[:10]):  # 只記錄前10個避免日誌過長
                logger.info(f"   轉折點 {i+1}: index={point['index']}, price={point['price']:.5f}, type={point['type']}, strength={point['strength']:.2f}%")
            if len(zigzag_points) > 10:
                logger.info(f"   ... 還有 {len(zigzag_points)-10} 個轉折點")
            
            # 添加ZigZag相關欄位
            result_df['zigzag_price'] = np.nan
            result_df['zigzag_type'] = None  # 'high', 'low', None
            result_df['zigzag_strength'] = np.nan  # 轉折點強度
            result_df['zigzag_swing'] = np.nan  # 波段編號
            
            # 填充ZigZag點
            swing_count = 0
            for i, point in enumerate(zigzag_points):
                idx = point['index']
                # 使用 iloc 來避免索引類型問題
                result_df.iloc[idx, result_df.columns.get_loc('zigzag_price')] = point['price']
                result_df.iloc[idx, result_df.columns.get_loc('zigzag_type')] = point['type']
                result_df.iloc[idx, result_df.columns.get_loc('zigzag_strength')] = point['strength']
                result_df.iloc[idx, result_df.columns.get_loc('zigzag_swing')] = swing_count
                swing_count += 1
            
            # 計算波段資訊
            result_df = self._calculate_swing_info(result_df, zigzag_points)
            
            logger.info(f"🎉 ZigZag計算完成，總共生成 {len(zigzag_points)} 個轉折點")
            
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
        
        logger.info(f"🔍 開始計算ZigZag轉折點，資料長度: {n}")
        
        if n < 3:
            logger.warning(f"⚠️ 資料量太少 ({n} 筆)，無法計算ZigZag")
            return points
        
        # 初始化
        high_prices = df['high'].values
        low_prices = df['low'].values
        close_prices = df['close'].values
        
        # 計算最小變動金額 (用於日誌)
        avg_price = np.mean(close_prices)
        min_change_amount = avg_price * self.deviation / 100
        logger.info(f"💰 平均價格: {avg_price:.5f}, 最小變動量: {min_change_amount:.5f} ({self.deviation}%)")
        
        # 尋找第一個轉折點
        logger.info("🔍 尋找第一個轉折點...")
        first_point = self._find_first_point(high_prices, low_prices, close_prices)
        if first_point is None:
            logger.warning("⚠️ 找不到第一個轉折點")
            return points
        
        logger.info(f"✅ 第一個轉折點: index={first_point['index']}, price={first_point['price']:.5f}, type={first_point['type']}")
        points.append(first_point)
        current_type = first_point['type']  # 'high' 或 'low'
        current_price = first_point['price']
        current_index = first_point['index']
        
        # 尋找後續轉折點
        search_start = current_index + max(1, self.min_swing_bars)
        logger.info(f"🔄 開始循環尋找轉折點，起始位置: {search_start}, 最小波段: {self.min_swing_bars}")
        
        # 修改循環條件：允許搜尋到更接近末尾的位置
        loop_end_condition = n - 1  # 改為搜尋到倒數第二個位置
        logger.info(f"📊 數據總長度: {n}, 循環終止條件: i < {loop_end_condition}")
        
        i = search_start
        iteration_count = 0
        consecutive_failures = 0  # 連續失敗次數
        while i < loop_end_condition:
            iteration_count += 1
            
            if iteration_count % 50 == 0:  # 降低頻率，更頻繁記錄
                logger.info(f"🔄 迭代 {iteration_count}，當前位置 {i}/{n}，連續失敗: {consecutive_failures}")
            
            if current_type == 'high':
                # 尋找下一個低點
                next_point = self._find_next_low(
                    low_prices, high_prices, i, current_price, current_type
                )
                search_type = "低點"
            else:
                # 尋找下一個高點
                next_point = self._find_next_high(
                    high_prices, low_prices, i, current_price, current_type
                )
                search_type = "高點"
            
            if next_point is None:
                consecutive_failures += 1
                if consecutive_failures % 20 == 0:  # 每20次失敗記錄一次
                    logger.warning(f"🔍 位置 {i}: 未找到 {search_type}，連續失敗 {consecutive_failures} 次")
                
                # 如果連續失敗較多次，嘗試動態調整 deviation
                if consecutive_failures > 30:  # 降低觸發門檻：從100降到30
                    logger.warning(f"⚠️ 連續失敗 {consecutive_failures} 次，嘗試動態調整搜尋策略")
                    
                    # 嘗試較寬鬆的 deviation (降低到 1.5%)
                    reduced_deviation = max(1.5, self.deviation * 0.5)
                    logger.info(f"🔧 嘗試降低 deviation 從 {self.deviation}% 到 {reduced_deviation}%")
                    
                    # 臨時保存原始 deviation
                    original_deviation = self.deviation
                    self.deviation = reduced_deviation
                    
                    # 重新嘗試搜尋
                    if current_type == 'high':
                        next_point = self._find_next_low(
                            low_prices, high_prices, i, current_price, current_type
                        )
                    else:
                        next_point = self._find_next_high(
                            high_prices, low_prices, i, current_price, current_type
                        )
                    
                    # 恢復原始 deviation
                    self.deviation = original_deviation
                    
                    if next_point is not None:
                        logger.info(f"✅ 使用降低的 deviation={reduced_deviation}% 成功找到轉折點！")
                        consecutive_failures = 0  # 重置失敗計數器
                        # 繼續處理找到的轉折點
                    else:
                        # 如果還是找不到，嘗試更寬鬆的條件 (1.0%)
                        if consecutive_failures > 50:  # 降低觸發門檻：從150降到50
                            ultra_reduced_deviation = 1.0
                            logger.info(f"🔧 嘗試極寬鬆 deviation {ultra_reduced_deviation}%")
                            
                            self.deviation = ultra_reduced_deviation
                            
                            if current_type == 'high':
                                next_point = self._find_next_low(
                                    low_prices, high_prices, i, current_price, current_type
                                )
                            else:
                                next_point = self._find_next_high(
                                    high_prices, low_prices, i, current_price, current_type
                                )
                            
                            self.deviation = original_deviation
                            
                            if next_point is not None:
                                logger.info(f"✅ 使用極寬鬆 deviation={ultra_reduced_deviation}% 找到轉折點！")
                                consecutive_failures = 0
                
                # 防止無限迴圈
                if consecutive_failures > 200:
                    logger.error(f"❌ 連續失敗超過200次，強制退出防止無限迴圈")
                    break
                
                # 如果沒有找到，繼續下一個位置
                if next_point is None:
                    i += 1
                    continue
            
            consecutive_failures = 0  # 重置失敗計數器
            
            # 檢查波段持續時間
            duration = next_point['index'] - current_index
            if duration >= self.min_swing_bars:
                logger.info(f"✅ 找到 {search_type}: index={next_point['index']}, price={next_point['price']:.5f}, duration={duration}")
                points.append(next_point)
                current_type = next_point['type']
                current_price = next_point['price']
                current_index = next_point['index']
                i = current_index + max(1, self.min_swing_bars)
                logger.info(f"🎯 更新搜尋位置: 從 {next_point['index']} 跳到 {i}")
            else:
                logger.info(f"⏭️ 波段太短 ({duration} < {self.min_swing_bars})，跳過，繼續從 {i+1}")
                i += 1
        
        logger.info(f"🏁 轉折點搜尋完成，總迭代次數: {iteration_count}，找到轉折點: {len(points)}")
        logger.info(f"📊 最終搜尋位置: {i}，數據總長度: {n}，終止條件: {n - max(1, self.min_swing_bars)}")
        if len(points) > 0:
            logger.info(f"🔚 最後轉折點: index={points[-1]['index']}, 剩餘數據: {n - points[-1]['index']} 條")
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
        target_price = last_price + min_change
        
        logger.debug(f"🔍 尋找高點: 起始位置={start_idx}, 上次價格={last_price:.5f}, 目標價格>{target_price:.5f}")
        
        # 診斷：記錄搜尋範圍內的價格情況
        if start_idx < n:
            search_end = min(start_idx + 50, n)  # 檢查接下來50個位置
            max_price_in_range = np.max(high_prices[start_idx:search_end])
            logger.info(f"🔍 診斷 - 搜尋範圍 [{start_idx}, {search_end}): 最高價={max_price_in_range:.5f}, 目標>{target_price:.5f}, 差距={max_price_in_range-target_price:.5f}")
        
        # 尋找高點
        candidates_found = 0
        for i in range(start_idx, n):
            current_high = high_prices[i]
            
            # 檢查是否超過最小變動
            if current_high > target_price:
                candidates_found += 1
                logger.debug(f"   候選高點 {candidates_found}: index={i}, price={current_high:.5f} (變動={(current_high-last_price)/last_price*100:.2f}%)")
                
                # 確認這是局部最高點
                if self._is_local_high(high_prices, i):
                    strength = abs(current_high - last_price) / last_price * 100
                    logger.debug(f"   ✅ 確認為局部高點: index={i}, price={current_high:.5f}, strength={strength:.2f}%")
                    return {
                        'index': i,
                        'price': current_high,
                        'type': 'high',
                        'strength': strength
                    }
                else:
                    logger.debug(f"   ❌ 非局部高點，繼續搜尋")
        
        logger.debug(f"🔍 高點搜尋結束: 找到 {candidates_found} 個候選點，但無有效局部高點")
        
        # 如果找不到符合deviation的點，嘗試放寬條件
        if candidates_found == 0 and start_idx < n:
            # 檢查是否是deviation設定過嚴
            search_end = min(start_idx + 100, n)
            price_range = high_prices[start_idx:search_end]
            if len(price_range) > 0:
                actual_max = np.max(price_range)
                actual_change_pct = abs(actual_max - last_price) / last_price * 100
                logger.warning(f"⚠️ 未找到符合{self.deviation}%變動的高點，實際最大變動僅{actual_change_pct:.2f}%")
        
        return None
    
    def _find_next_low(self, low_prices: np.ndarray, high_prices: np.ndarray,
                      start_idx: int, last_price: float, last_type: str) -> Optional[Dict[str, Any]]:
        """尋找下一個低點"""
        n = len(low_prices)
        
        # 計算最小價格變動
        min_change = last_price * self.deviation / 100
        target_price = last_price - min_change
        
        logger.debug(f"🔍 尋找低點: 起始位置={start_idx}, 上次價格={last_price:.5f}, 目標價格<{target_price:.5f}")
        
        # 診斷：記錄搜尋範圍內的價格情況
        if start_idx < n:
            search_end = min(start_idx + 50, n)  # 檢查接下來50個位置
            min_price_in_range = np.min(low_prices[start_idx:search_end])
            logger.info(f"🔍 診斷 - 搜尋範圍 [{start_idx}, {search_end}): 最低價={min_price_in_range:.5f}, 目標<{target_price:.5f}, 差距={target_price-min_price_in_range:.5f}")
        
        # 尋找低點
        candidates_found = 0
        for i in range(start_idx, n):
            current_low = low_prices[i]
            
            # 檢查是否超過最小變動
            if current_low < target_price:
                candidates_found += 1
                logger.debug(f"   候選低點 {candidates_found}: index={i}, price={current_low:.5f} (變動={(current_low-last_price)/last_price*100:.2f}%)")
                
                # 確認這是局部最低點
                if self._is_local_low(low_prices, i):
                    strength = abs(current_low - last_price) / last_price * 100
                    logger.debug(f"   ✅ 確認為局部低點: index={i}, price={current_low:.5f}, strength={strength:.2f}%")
                    return {
                        'index': i,
                        'price': current_low,
                        'type': 'low',
                        'strength': strength
                    }
                else:
                    logger.debug(f"   ❌ 非局部低點，繼續搜尋")
        
        logger.debug(f"🔍 低點搜尋結束: 找到 {candidates_found} 個候選點，但無有效局部低點")
        
        # 如果找不到符合deviation的點，嘗試放寬條件
        if candidates_found == 0 and start_idx < n:
            # 檢查是否是deviation設定過嚴
            search_end = min(start_idx + 100, n)
            price_range = low_prices[start_idx:search_end]
            if len(price_range) > 0:
                actual_min = np.min(price_range)
                actual_change_pct = abs(actual_min - last_price) / last_price * 100
                logger.warning(f"⚠️ 未找到符合{self.deviation}%變動的低點，實際最大變動僅{actual_change_pct:.2f}%")
        
        return None
    
    def _is_local_high(self, high_prices: np.ndarray, idx: int) -> bool:
        """檢查是否為局部最高點"""
        n = len(high_prices)
        current_high = high_prices[idx]
        
        # 檢查前後depth個bar
        start = max(0, idx - self.depth)
        end = min(n, idx + self.depth + 1)
        
        logger.debug(f"   🔍 檢查局部高點: index={idx}, price={current_high:.5f}, 範圍=[{start}, {end})")
        
        competing_highs = 0
        for i in range(start, end):
            if i != idx and high_prices[i] >= current_high:
                competing_highs += 1
                logger.debug(f"      競爭高點: index={i}, price={high_prices[i]:.5f}")
        
        is_local = competing_highs == 0
        logger.debug(f"   📊 局部高點判斷: {is_local} (競爭高點數量: {competing_highs})")
        return is_local
    
    def _is_local_low(self, low_prices: np.ndarray, idx: int) -> bool:
        """檢查是否為局部最低點"""
        n = len(low_prices)
        current_low = low_prices[idx]
        
        # 檢查前後depth個bar
        start = max(0, idx - self.depth)
        end = min(n, idx + self.depth + 1)
        
        logger.debug(f"   🔍 檢查局部低點: index={idx}, price={current_low:.5f}, 範圍=[{start}, {end})")
        
        competing_lows = 0
        for i in range(start, end):
            if i != idx and low_prices[i] <= current_low:
                competing_lows += 1
                logger.debug(f"      競爭低點: index={i}, price={low_prices[i]:.5f}")
        
        is_local = competing_lows == 0
        logger.debug(f"   📊 局部低點判斷: {is_local} (競爭低點數量: {competing_lows})")
        return is_local
    
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
            
            # 波段範圍 - 使用 iloc 避免索引問題
            if current_point['type'] == 'low' and next_point['type'] == 'high':
                # 上升波段
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_direction')] = 'up'
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_low')] = current_point['price']
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_high')] = next_point['price']
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_range')] = next_point['price'] - current_point['price']
                
            elif current_point['type'] == 'high' and next_point['type'] == 'low':
                # 下降波段
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_direction')] = 'down'
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_high')] = current_point['price']
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_low')] = next_point['price']
                df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_range')] = current_point['price'] - next_point['price']
            
            # 波段持續時間
            duration = next_idx - current_idx
            df.iloc[current_idx:next_idx+1, df.columns.get_loc('swing_duration')] = duration
        
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
            # 確保timestamp是正確的時間戳格式
            timestamp = idx
            if isinstance(timestamp, (int, float)) and timestamp < 1000000:  # 可能是數字索引
                # 如果是數字索引，嘗試從DataFrame的索引獲取對應的時間戳
                if timestamp < len(df):
                    timestamp = df.index[timestamp]
                else:
                    continue  # 跳過無效的索引
            
            swing_points.append({
                'timestamp': timestamp,
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
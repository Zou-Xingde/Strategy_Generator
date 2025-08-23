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
from datetime import datetime, timedelta

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import DUCKDB_PATH, TIMEFRAMES
from config.version_control import get_algorithm_parameters, get_version_description
from src.database.connection import DuckDBConnection
from src.utils.timeframe import normalize_timeframe
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
                               algorithm_name: str = 'zigzag', limit: Optional[int] = None, **algorithm_params):
        """
        處理特定交易品種和時間週期的波段資料（整體處理模式）
        
        Args:
            symbol: 交易品種
            timeframe: 時間週期
            algorithm_name: 演算法名稱
            limit: 限制處理的資料筆數
            **algorithm_params: 演算法參數
        """
        try:
            # 參數正規化與預設
            symbol = (symbol or '').strip()
            orig_timeframe = (timeframe or '').strip()
            timeframe = normalize_timeframe(orig_timeframe)
            algorithm_name = (algorithm_name or 'zigzag').strip().lower()
            algorithm_params = algorithm_params if isinstance(algorithm_params, dict) else {}

            # timeframe 驗證（訊息回報原始輸入，便於排查）
            if timeframe not in TIMEFRAMES:
                msg = f"Unsupported timeframe: {orig_timeframe}"
                logger.error("input-error: %s", msg)
                raise ValueError(msg)

            logger.info(f"🚀 開始處理 {symbol} {timeframe} 的波段資料，使用 {algorithm_name} 演算法")
            logger.info(f"📝 演算法參數: {algorithm_params}")

            # 取得與合併演算法參數（defaults 覆蓋為基底，使用者參數覆蓋 defaults）
            try:
                default_params = get_algorithm_parameters(algorithm_name)
                default_params = default_params if isinstance(default_params, dict) else {}
            except Exception as _e:  # 極端情況防呆
                default_params = {}
            algo_params = {**default_params, **(algorithm_params or {})}
            logger.info(f"🔧 最終使用參數: {algo_params}")

            # 獲取蠟燭圖資料（使用正規化後 timeframe）
            with DuckDBConnection(self.db_path) as db:
                logger.info(f"📊 開始獲取 {symbol} {timeframe} 的K線資料...")
                candlestick_df = db.get_candlestick_data(symbol, timeframe, limit=limit)

                if candlestick_df is None or getattr(candlestick_df, 'empty', True):
                    msg = f"No data for symbol={symbol} timeframe={orig_timeframe}"
                    logger.error("input-error: %s", msg)
                    raise ValueError(msg)

                total_records = len(candlestick_df)
                logger.info(f"✅ 獲取到 {total_records} 筆蠟燭圖資料")
                logger.info(f"📅 日期範圍: {candlestick_df.index[0]} ~ {candlestick_df.index[-1]}")
                logger.info(f"💰 價格範圍: {candlestick_df['low'].min():.5f} ~ {candlestick_df['high'].max():.5f}")

                # 執行演算法計算
                algorithm = self.algorithms.get(algorithm_name)
                if not algorithm:
                    msg = f"不支援的演算法: {algorithm_name}"
                    logger.error("input-error: %s", msg)
                    raise ValueError(msg)

                logger.info(f"🎯 使用演算法: {algorithm.name}")
                
                # 設定演算法參數
                if algo_params:
                    logger.info(f"🔧 設定演算法參數: {algo_params}")
                    algorithm.set_parameters(**algo_params)
                
                # � 始終使用整體處理模式以確保連續性
                logger.info(f"🎯 使用整體處理模式 (資料量: {total_records})")
                num_batches = 1
                logger.info("🔍 開始執行演算法...")
                result_df = algorithm.calculate(candlestick_df)
                
                # 收集波段點  
                all_swing_points = algorithm.get_swing_points(result_df)
                total_swing_points = len(all_swing_points)
                
                logger.info(f"✅ 整體處理完成，找到 {total_swing_points} 個波段點")
                
                # 合併所有資料並存儲到資料庫
                if all_swing_points:
                    # 創建包含所有波段點的DataFrame
                    combined_df = self._create_combined_swing_df(candlestick_df, all_swing_points)
                    
                    # 獲取演算法參數
                    algorithm_parameters = algorithm.get_parameters() or {}
                    
                    # 生成版本描述
                    version_description = get_version_description()
                    
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
    
    def process_symbol_timeframe_by_date_range(self, symbol: str, timeframe: str, 
                                           algorithm_name: str = 'zigzag', 
                                           start_date: Optional[str] = None, 
                                           end_date: Optional[str] = None,
                                           **algorithm_params):
        """
        根據日期範圍處理特定交易品種和時間週期的波段資料（整體處理模式）
        
        Args:
            symbol: 交易品種
            timeframe: 時間週期
            algorithm_name: 演算法名稱
            start_date: 開始日期 (格式: 'YYYY-MM-DD')，如果為None則使用最早的數據
            end_date: 結束日期 (格式: 'YYYY-MM-DD')，如果為None則使用最新的數據
            **algorithm_params: 演算法參數
        """
        try:
            # 參數正規化與預設
            symbol = (symbol or '').strip()
            orig_timeframe = (timeframe or '').strip()
            timeframe = normalize_timeframe(orig_timeframe)
            algorithm_name = (algorithm_name or 'zigzag').strip().lower()
            algorithm_params = algorithm_params if isinstance(algorithm_params, dict) else {}

            # timeframe 驗證
            if timeframe not in TIMEFRAMES:
                msg = f"Unsupported timeframe: {orig_timeframe}"
                logger.error("input-error: %s", msg)
                raise ValueError(msg)

            logger.info(f"開始處理 {symbol} {timeframe} 的波段資料，日期範圍: {start_date or '最早'} 到 {end_date or '最新'}")

            # 取得與合併演算法參數
            try:
                default_params = get_algorithm_parameters(algorithm_name)
                default_params = default_params if isinstance(default_params, dict) else {}
            except Exception as _e:
                default_params = {}
            algo_params = {**default_params, **(algorithm_params or {})}
            # 獲取蠟燭圖資料
            with DuckDBConnection(self.db_path) as db:
                candlestick_df = db.get_candlestick_data(symbol, timeframe, start_date=start_date, end_date=end_date)

                if candlestick_df is None or getattr(candlestick_df, 'empty', True):
                    msg = f"No data for symbol={symbol} timeframe={orig_timeframe} in date range {start_date} to {end_date}"
                    logger.error("input-error: %s", msg)
                    raise ValueError(msg)

                total_records = len(candlestick_df)
                logger.info(f"獲取到 {total_records} 筆蠟燭圖資料")

                # 執行演算法計算
                algorithm = self.algorithms.get(algorithm_name)
                if not algorithm:
                    msg = f"不支援的演算法: {algorithm_name}"
                    logger.error("input-error: %s", msg)
                    raise ValueError(msg)

                # 設定演算法參數
                if algo_params:
                    algorithm.set_parameters(**algo_params)
                
                # � 始終使用整體處理模式以確保連續性
                logger.info(f"🎯 使用整體處理模式 (資料量: {total_records})")
                num_batches = 1
                
                # 計算波段
                result_df = algorithm.calculate(candlestick_df)
                
                # 收集波段點
                all_swing_points = algorithm.get_swing_points(result_df)
                total_swing_points = len(all_swing_points)
                
                logger.info(f"🎯 整體處理完成，找到 {total_swing_points} 個波段點")
                
                # 合併所有資料並存儲到資料庫
                if all_swing_points:
                    # 創建包含所有波段點的DataFrame
                    combined_df = self._create_combined_swing_df(candlestick_df, all_swing_points)
                    
                    # 獲取演算法參數
                    algorithm_parameters = algorithm.get_parameters() or {}
                    
                    # 生成版本描述
                    version_description = get_version_description()
                    
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
                    'date_range': f"{start_date or '最早'} 到 {end_date or '最新'}",
                    'total_records': total_records,
                    'swing_points': total_swing_points,
                    'statistics': stats,
                    'batches_processed': num_batches
                }
                
        except Exception as e:
            logger.error(f"處理波段資料失敗: {e}")
            raise
    
    def process_all_timeframes_by_date_range(self, symbol: str, 
                                         algorithm_name: str = 'zigzag', 
                                         timeframes: Optional[List[str]] = None, 
                                         start_date: Optional[str] = None, 
                                         end_date: Optional[str] = None,
                                         **algorithm_params):
        """
        針對多個時間週期，依日期範圍生成波段資料。

        Args:
            symbol: 交易品種
            algorithm_name: 使用演算法（預設 zigzag）
            timeframes: 要處理的時間週期清單，None 則使用設定檔中的全部 key 順序
            start_date: 起始日期（YYYY-MM-DD）
            end_date: 結束日期（YYYY-MM-DD）
            **algorithm_params: 演算法參數覆寫

        Returns:
            Dict 包含每個 timeframe 的處理結果
        """
        tfs = list(timeframes) if timeframes else list(TIMEFRAMES.keys())
        results: Dict[str, Any] = {}
        for tf in tfs:
            try:
                res = self.process_symbol_timeframe_by_date_range(
                    symbol=symbol,
                    timeframe=tf,
                    algorithm_name=algorithm_name,
                    start_date=start_date,
                    end_date=end_date,
                    **algorithm_params,
                )
                results[tf] = {"ok": True, "result": res}
            except Exception as e:
                logger.exception("process timeframe failed: %s %s", symbol, tf)
                results[tf] = {"ok": False, "error": str(e)}
        return {
            "symbol": symbol,
            "algorithm": algorithm_name,
            "start_date": start_date,
            "end_date": end_date,
            "timeframes": tfs,
            "results": results,
        }

    # -------------------------- internal helpers --------------------------
    def _create_combined_swing_df(self, candles: pd.DataFrame, swing_points: List[Dict[str, Any]]) -> pd.DataFrame:
        """將所有批次的 swing points 合併到同一個 K 線 DataFrame 上。

        candles: 來源 K 線（index 為 timestamp，含 open/high/low/close/volume）
        swing_points: 由演算法回傳的轉折點列表（含 timestamp/price/type/strength/...）
        """
        if candles is None or getattr(candles, 'empty', True):
            return pd.DataFrame()

        df = candles.copy()
        # 確保時間排序
        try:
            df = df.sort_index()
        except Exception:
            pass

        # 預先建立欄位
        df['zigzag_price'] = np.nan
        df['zigzag_type'] = None
        df['zigzag_strength'] = np.nan
        df['zigzag_swing'] = np.nan
        df['swing_high'] = np.nan
        df['swing_low'] = np.nan
        df['swing_range'] = np.nan
        df['swing_duration'] = np.nan
        df['swing_direction'] = None

        if not swing_points:
            return df

        # 依時間排序轉折點
        def _ts_key(v: Any) -> float:
            ts = v.get('timestamp') if isinstance(v, dict) else None
            try:
                # pandas Timestamp -> ns since epoch
                if isinstance(ts, pd.Timestamp):
                    return float(ts.value)
                # datetime -> seconds since epoch
                if hasattr(ts, 'timestamp'):
                    return float(ts.timestamp())
                # numeric
                if isinstance(ts, (int, np.integer)):
                    return float(int(ts))
                if isinstance(ts, (float, np.floating)):
                    return float(ts)
            except Exception:
                pass
            return float('-inf')

        pts = sorted([p for p in swing_points if isinstance(p, dict)], key=_ts_key)

        # 填入 zigzag_* 於對應 timestamp 列
        for i, p in enumerate(pts):
            ts = p.get('timestamp')
            if ts is None:
                continue
            if ts not in df.index:
                # 若為整數或浮點索引（極少見），盡量轉為位置
                try:
                    df.loc[ts, 'zigzag_price'] = p.get('price')
                    df.loc[ts, 'zigzag_type'] = p.get('type')
                    df.loc[ts, 'zigzag_strength'] = p.get('strength')
                    df.loc[ts, 'zigzag_swing'] = i
                    continue
                except Exception:
                    # 找最近的 timestamp（保守做法：略過）
                    continue
            df.loc[ts, 'zigzag_price'] = p.get('price')
            df.loc[ts, 'zigzag_type'] = p.get('type')
            df.loc[ts, 'zigzag_strength'] = p.get('strength')
            df.loc[ts, 'zigzag_swing'] = i

        # 依相鄰轉折點填入 swing_* 區間資訊
        for i in range(len(pts) - 1):
            cur = pts[i]
            nxt = pts[i + 1]
            t1 = cur.get('timestamp')
            t2 = nxt.get('timestamp')
            p1 = cur.get('price')
            p2 = nxt.get('price')
            typ1 = cur.get('type')
            if t1 is None or t2 is None or t1 not in df.index or t2 not in df.index:
                continue
            # 方向與高低界
            if typ1 == 'low':
                # 上升段
                seg_dir = 'up'
                hi = p2
                lo = p1
                rng = (p2 - p1) if (p1 is not None and p2 is not None) else np.nan
            elif typ1 == 'high':
                # 下降段
                seg_dir = 'down'
                hi = p1
                lo = p2
                rng = (p1 - p2) if (p1 is not None and p2 is not None) else np.nan
            else:
                seg_dir = None
                hi = np.nan
                lo = np.nan
                rng = np.nan

            # 區間賦值（包含端點）
            try:
                df.loc[t1:t2, 'swing_direction'] = seg_dir
                df.loc[t1:t2, 'swing_high'] = hi
                df.loc[t1:t2, 'swing_low'] = lo
                df.loc[t1:t2, 'swing_range'] = rng
                # duration 以 bar 數量估算
                # 取得切片長度
                try:
                    seg_len = len(df.loc[t1:t2])
                except Exception:
                    seg_len = np.nan
                df.loc[t1:t2, 'swing_duration'] = seg_len
            except Exception:
                # 若區間賦值例外就略過該段（保守處理）
                continue

        return df

    def _calculate_combined_statistics(self, swing_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """根據轉折點列表計算簡要統計。"""
        n = len(swing_points or [])
        if n < 2:
            return {
                'total_swings': 0,
                'avg_swing_range': 0,
                'avg_swing_duration': 0,
                'max_swing_range': 0,
                'min_swing_range': 0,
                'swing_ranges': [],
                'swing_durations': [],
            }

        # 依時間排序
        def _norm_key(p: Dict[str, Any]) -> float:
            ts = p.get('timestamp')
            try:
                if isinstance(ts, pd.Timestamp):
                    return float(ts.value)
                if hasattr(ts, 'timestamp'):
                    return float(ts.timestamp())
                if isinstance(ts, (int, np.integer)):
                    return float(int(ts))
                if isinstance(ts, (float, np.floating)):
                    return float(ts)
            except Exception:
                pass
            return float('-inf')
        pts = sorted([p for p in swing_points if isinstance(p, dict) and p.get('timestamp') is not None], key=_norm_key)
        ranges: List[float] = []
        durations: List[float] = []
        for i in range(len(pts) - 1):
            a = pts[i]
            b = pts[i + 1]
            pa = a.get('price')
            pb = b.get('price')
            ta = a.get('timestamp')
            tb = b.get('timestamp')
            try:
                rng = float(abs(pb - pa)) if pa is not None and pb is not None else 0.0
            except Exception:
                rng = 0.0
            try:
                dur = (tb - ta).total_seconds() / 3600.0  # 以小時
            except Exception:
                dur = 0.0
            ranges.append(rng)
            durations.append(dur)

        return {
            'total_swings': max(0, len(pts) - 1),
            'avg_swing_range': float(np.mean(ranges)) if ranges else 0.0,
            'avg_swing_duration': float(np.mean(durations)) if durations else 0.0,
            'max_swing_range': float(np.max(ranges)) if ranges else 0.0,
            'min_swing_range': float(np.min(ranges)) if ranges else 0.0,
            'swing_ranges': ranges,
            'swing_durations': durations,
        }

    # ===== 已移除所有分批處理邏輯，只保留整體處理模式 =====

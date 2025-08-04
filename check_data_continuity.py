#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
檢查資料庫中K線數據的連貫性
查找跳空和缺失的數據
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def check_data_continuity(symbol='XAUUSD', timeframe='D1', limit=1000):
    """檢查數據連貫性"""
    
    print(f"🔍 檢查 {symbol} {timeframe} 數據連貫性...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
            # 查詢數據
            query = f"""
            SELECT 
                timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM candlestick_data_new 
            WHERE symbol = '{symbol}' 
            AND timeframe = '{timeframe}'
            ORDER BY timestamp
            LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print(f"❌ 沒有找到 {symbol} {timeframe} 的數據")
                return
            
            print(f"📊 找到 {len(df)} 條數據")
            print(f"📅 時間範圍: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
            
            # 檢查時間間隔
            df = df.sort_values('timestamp')
            df['next_timestamp'] = df['timestamp'].shift(-1)
            df['time_gap'] = (df['next_timestamp'] - df['timestamp']).dt.total_seconds()
            
            # 根據時間週期計算預期的間隔
            timeframe_seconds = {
                'M1': 60,
                'M5': 300,
                'M15': 900,
                'M30': 1800,
                'H1': 3600,
                'H4': 14400,
                'D1': 86400,
                'W1': 604800,
                'MN': 2592000
            }
            
            expected_gap = timeframe_seconds.get(timeframe, 86400)
            
            # 查找異常間隔
            abnormal_gaps = df[df['time_gap'] > expected_gap * 1.5].copy()
            
            if not abnormal_gaps.empty:
                print(f"\n⚠️  發現 {len(abnormal_gaps)} 個異常時間間隔:")
                print("=" * 80)
                
                for idx, row in abnormal_gaps.iterrows():
                    if pd.notna(row['next_timestamp']):
                        gap_days = row['time_gap'] / 86400
                        print(f"📍 時間: {row['timestamp']}")
                        print(f"   下一個時間: {row['next_timestamp']}")
                        print(f"   間隔: {gap_days:.1f} 天 (預期: {expected_gap/86400:.1f} 天)")
                        print(f"   價格: O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")
                        print("-" * 40)
            else:
                print("✅ 時間間隔正常，沒有發現異常跳空")
            
            # 檢查價格跳空
            print(f"\n💰 檢查價格跳空...")
            
            # 計算價格跳空
            df['prev_close'] = df['close'].shift(1)
            df['price_gap'] = df['open'] - df['prev_close']
            df['gap_percent'] = (df['price_gap'] / df['prev_close'] * 100).abs()
            
            # 查找大跳空 (>5%)
            large_gaps = df[df['gap_percent'] > 5].copy()
            
            if not large_gaps.empty:
                print(f"⚠️  發現 {len(large_gaps)} 個大跳空 (>5%):")
                print("=" * 80)
                
                for idx, row in large_gaps.iterrows():
                    if pd.notna(row['prev_close']):
                        print(f"📍 時間: {row['timestamp']}")
                        print(f"   前收: {row['prev_close']:.2f}")
                        print(f"   開盤: {row['open']:.2f}")
                        print(f"   跳空: {row['price_gap']:.2f} ({row['gap_percent']:.2f}%)")
                        print(f"   當日: H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")
                        print("-" * 40)
            else:
                print("✅ 沒有發現大跳空 (>5%)")
            
            # 檢查數據密度
            print(f"\n📈 數據密度分析...")
            
            total_days = (df['timestamp'].max() - df['timestamp'].min()).days
            data_density = len(df) / total_days if total_days > 0 else 0
            
            print(f"   總天數: {total_days} 天")
            print(f"   數據條數: {len(df)} 條")
            print(f"   數據密度: {data_density:.2f} 條/天")
            
            # 檢查是否有連續缺失
            print(f"\n🔍 檢查連續缺失...")
            
            # 生成完整的時間序列
            start_date = df['timestamp'].min()
            end_date = df['timestamp'].max()
            
            if timeframe == 'D1':
                date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            elif timeframe == 'H1':
                date_range = pd.date_range(start=start_date, end=end_date, freq='H')
            elif timeframe == 'H4':
                date_range = pd.date_range(start=start_date, end=end_date, freq='4H')
            else:
                date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # 找出缺失的日期
            existing_dates = set(df['timestamp'].dt.date)
            expected_dates = set(date_range.date)
            missing_dates = expected_dates - existing_dates
            
            if missing_dates:
                print(f"⚠️  發現 {len(missing_dates)} 個缺失的日期:")
                print("=" * 80)
                
                missing_list = sorted(list(missing_dates))
                for i, missing_date in enumerate(missing_list[:20]):  # 只顯示前20個
                    print(f"   {missing_date}")
                
                if len(missing_list) > 20:
                    print(f"   ... 還有 {len(missing_list) - 20} 個缺失日期")
            else:
                print("✅ 沒有發現缺失的日期")
            
            # 檢查數據質量
            print(f"\n🔬 數據質量檢查...")
            
            # 檢查異常價格
            df['price_range'] = df['high'] - df['low']
            df['range_percent'] = (df['price_range'] / df['low'] * 100)
            
            # 查找異常價格範圍 (>20%)
            abnormal_ranges = df[df['range_percent'] > 20].copy()
            
            if not abnormal_ranges.empty:
                print(f"⚠️  發現 {len(abnormal_ranges)} 個異常價格範圍 (>20%):")
                print("=" * 80)
                
                for idx, row in abnormal_ranges.iterrows():
                    print(f"📍 時間: {row['timestamp']}")
                    print(f"   價格範圍: {row['price_range']:.2f} ({row['range_percent']:.2f}%)")
                    print(f"   價格: O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")
                    print("-" * 40)
            else:
                print("✅ 價格範圍正常")
            
            # 檢查零成交量
            zero_volume = df[df['volume'] == 0]
            if not zero_volume.empty:
                print(f"⚠️  發現 {len(zero_volume)} 條零成交量數據")
            else:
                print("✅ 沒有零成交量數據")
            
            print(f"\n📋 檢查完成!")
            
    except Exception as e:
        print(f"❌ 檢查失敗: {e}")
        import traceback
        traceback.print_exc()

def check_multiple_symbols():
    """檢查多個品種的數據"""
    
    symbols = ['XAUUSD', 'US30', 'US100']
    timeframes = ['D1', 'H4', 'H1']
    
    for symbol in symbols:
        for timeframe in timeframes:
            print(f"\n{'='*60}")
            check_data_continuity(symbol, timeframe, 1000)
            print(f"{'='*60}")

if __name__ == "__main__":
    print("🔍 K線數據連貫性檢查工具")
    print("=" * 60)
    
    # 檢查單一品種
    check_data_continuity('XAUUSD', 'D1', 2000)
    
    # 檢查多個品種
    # check_multiple_symbols() 
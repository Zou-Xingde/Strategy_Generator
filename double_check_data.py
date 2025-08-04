#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Double Check 資料驗證腳本
從資料庫讀取 M1~H4 時間週期的資料與 D1 資料進行比對
時間範圍: 2024/07/09~2024/08/14
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def get_timeframe_data_from_db(start_date, end_date, timeframe):
    """從資料庫獲取指定時間週期的資料"""
    
    print(f"🗄️ 從資料庫獲取 {timeframe} 資料...")
    
    with DuckDBConnection(str(DUCKDB_PATH)) as db:
        symbol = "EXUSA30IDXUSD"
        
        # 獲取timeframe資料
        df = db.get_candlestick_data(symbol, timeframe)
        
        if len(df) > 0:
            # 篩選時間範圍
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            print(f"✅ {timeframe}: 獲取 {len(df)} 筆資料")
            return df
        else:
            print(f"⚠️ {timeframe}: 沒有找到資料")
            return None

def get_d1_data_from_db(start_date, end_date):
    """從資料庫獲取D1資料"""
    
    print("🗄️ 從資料庫獲取D1資料...")
    
    with DuckDBConnection(str(DUCKDB_PATH)) as db:
        symbol = "EXUSA30IDXUSD"
        timeframe = "D1"
        
        # 獲取D1資料
        df = db.get_candlestick_data(symbol, timeframe)
        
        if len(df) > 0:
            # 篩選時間範圍
            df = df[(df.index >= start_date) & (df.index <= end_date)]
            print(f"✅ D1: 獲取 {len(df)} 筆資料")
            return df
        else:
            print("⚠️ 沒有找到D1資料")
            return None

def compare_data(timeframe_data, d1_data, timeframe):
    """比對資料"""
    
    print(f"\n🔍 比對 {timeframe} 與 D1 資料...")
    
    if timeframe_data is None or d1_data is None:
        print("❌ 無法比對，資料缺失")
        return
    
    # 將timeframe資料按日期分組，計算每日的OHLC
    timeframe_data = timeframe_data.reset_index()
    timeframe_data['date'] = timeframe_data['timestamp'].dt.date
    
    daily_ohlc = timeframe_data.groupby('date').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    
    daily_ohlc['date'] = pd.to_datetime(daily_ohlc['date'])
    daily_ohlc = daily_ohlc.set_index('date')
    
    # 準備D1資料
    d1_compare = d1_data.copy()
    
    # 找出共同的日期
    common_dates = daily_ohlc.index.intersection(d1_compare.index)
    
    if len(common_dates) == 0:
        print("⚠️ 沒有共同的日期可以比對")
        return
    
    print(f"📅 比對日期數量: {len(common_dates)}")
    
    # 比對結果
    comparison_results = []
    
    for date in sorted(common_dates):
        tf_data = daily_ohlc.loc[date]
        d1_data_row = d1_compare.loc[date]
        
        # 計算差異
        open_diff = abs(tf_data['open'] - d1_data_row['open'])
        high_diff = abs(tf_data['high'] - d1_data_row['high'])
        low_diff = abs(tf_data['low'] - d1_data_row['low'])
        close_diff = abs(tf_data['close'] - d1_data_row['close'])
        
        comparison_results.append({
            'date': date,
            'timeframe': timeframe,
            'tf_open': tf_data['open'],
            'tf_high': tf_data['high'],
            'tf_low': tf_data['low'],
            'tf_close': tf_data['close'],
            'd1_open': d1_data_row['open'],
            'd1_high': d1_data_row['high'],
            'd1_low': d1_data_row['low'],
            'd1_close': d1_data_row['close'],
            'open_diff': open_diff,
            'high_diff': high_diff,
            'low_diff': low_diff,
            'close_diff': close_diff,
            'match': (open_diff < 0.01 and high_diff < 0.01 and low_diff < 0.01 and close_diff < 0.01)
        })
    
    # 轉換為DataFrame
    comparison_df = pd.DataFrame(comparison_results)
    
    # 統計結果
    total_days = len(comparison_df)
    matched_days = len(comparison_df[comparison_df['match'] == True])
    match_rate = (matched_days / total_days) * 100 if total_days > 0 else 0
    
    print(f"📊 {timeframe} vs D1 比對結果:")
    print(f"   總天數: {total_days}")
    print(f"   匹配天數: {matched_days}")
    print(f"   匹配率: {match_rate:.2f}%")
    
    # 顯示詳細比對結果
    print(f"\n📋 {timeframe} vs D1 詳細比對:")
    for _, row in comparison_df.iterrows():
        status = "✅" if row['match'] else "❌"
        print(f"{status} {row['date'].strftime('%Y-%m-%d')}: "
              f"O:{row['tf_open']:.2f}/{row['d1_open']:.2f} "
              f"H:{row['tf_high']:.2f}/{row['d1_high']:.2f} "
              f"L:{row['tf_low']:.2f}/{row['d1_low']:.2f} "
              f"C:{row['tf_close']:.2f}/{row['d1_close']:.2f}")
    
    return comparison_df

def main():
    """主函數"""
    
    print("🔍 Double Check 資料驗證開始...")
    
    # 設定時間範圍
    start_date = datetime(2024, 7, 9)
    end_date = datetime(2024, 8, 14)
    
    print(f"📅 驗證時間範圍: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    
    # 獲取D1資料
    d1_data = get_d1_data_from_db(start_date, end_date)
    
    if d1_data is None:
        print("❌ 無法獲取D1資料，驗證終止")
        return
    
    # 要驗證的時間週期
    timeframes = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4']
    
    all_results = []
    
    for timeframe in timeframes:
        print(f"\n{'='*60}")
        print(f"🔍 驗證 {timeframe} 時間週期")
        print(f"{'='*60}")
        
        # 從資料庫獲取timeframe資料
        tf_data = get_timeframe_data_from_db(start_date, end_date, timeframe)
        
        # 比對資料
        result = compare_data(tf_data, d1_data, timeframe)
        
        if result is not None:
            all_results.append(result)
    
    # 總結報告
    print(f"\n{'='*60}")
    print("📊 驗證總結報告")
    print(f"{'='*60}")
    
    for result in all_results:
        if len(result) > 0:
            timeframe = result['timeframe'].iloc[0]
            total_days = len(result)
            matched_days = len(result[result['match'] == True])
            match_rate = (matched_days / total_days) * 100
            
            print(f"{timeframe}: {matched_days}/{total_days} 天匹配 ({match_rate:.1f}%)")
    
    print(f"\n✅ Double Check 驗證完成！")

if __name__ == "__main__":
    main() 
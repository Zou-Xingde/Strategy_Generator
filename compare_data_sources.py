#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
比對資料庫數據和前端API數據
找出數據差異和問題所在
"""

import sys
import os
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH
from src.database.connection import DuckDBConnection

def get_database_data(symbol='XAUUSD', timeframe='D1', limit=100):
    """從資料庫獲取數據"""
    print(f"📊 從資料庫獲取 {symbol} {timeframe} 數據...")
    
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            conn = db.conn
            
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
            ORDER BY timestamp DESC
            LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print(f"❌ 資料庫中沒有找到 {symbol} {timeframe} 的數據")
                return None
            
            print(f"✅ 從資料庫獲取到 {len(df)} 條數據")
            return df
            
    except Exception as e:
        print(f"❌ 從資料庫獲取數據失敗: {e}")
        return None

def get_api_data(symbol='XAUUSD', timeframe='D1', limit=100):
    """從API獲取數據"""
    print(f"🌐 從API獲取 {symbol} {timeframe} 數據...")
    
    try:
        url = f"http://127.0.0.1:8050/api/candlestick/{symbol}/{timeframe}?limit={limit}"
        print(f"API URL: {url}")
        
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ API請求失敗: {response.status_code}")
            return None
        
        data = response.json()
        
        if 'error' in data:
            print(f"❌ API返回錯誤: {data['error']}")
            return None
        
        if 'data' not in data or not data['data']:
            print(f"❌ API返回空數據")
            return None
        
        # 轉換為DataFrame
        df = pd.DataFrame(data['data'])
        
        if df.empty:
            print(f"❌ API返回的數據為空")
            return None
        
        print(f"✅ 從API獲取到 {len(df)} 條數據")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API請求異常: {e}")
        return None
    except Exception as e:
        print(f"❌ 處理API數據失敗: {e}")
        return None

def compare_data_sources(symbol='XAUUSD', timeframe='D1', limit=100):
    """比對資料庫和API數據"""
    print(f"\n🔍 比對 {symbol} {timeframe} 數據來源...")
    print("=" * 80)
    
    # 獲取資料庫數據
    db_df = get_database_data(symbol, timeframe, limit)
    if db_df is None:
        return
    
    # 獲取API數據
    api_df = get_api_data(symbol, timeframe, limit)
    if api_df is None:
        return
    
    print(f"\n📋 數據概況:")
    print(f"   資料庫數據: {len(db_df)} 條")
    print(f"   API數據: {len(api_df)} 條")
    
    # 標準化數據格式
    print(f"\n🔄 標準化數據格式...")
    
    # 資料庫數據標準化
    db_df_clean = db_df.copy()
    db_df_clean['timestamp'] = pd.to_datetime(db_df_clean['timestamp'])
    db_df_clean = db_df_clean.sort_values('timestamp', ascending=False).reset_index(drop=True)
    
    # API數據標準化
    api_df_clean = api_df.copy()
    api_df_clean['timestamp'] = pd.to_datetime(api_df_clean['timestamp'])
    api_df_clean = api_df_clean.sort_values('timestamp', ascending=False).reset_index(drop=True)
    
    # 確保列名一致
    column_mapping = {
        'open': 'open',
        'high': 'high', 
        'low': 'low',
        'close': 'close',
        'volume': 'volume'
    }
    
    # 選擇要比較的列
    db_compare = db_df_clean[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
    api_compare = api_df_clean[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
    
    print(f"✅ 數據格式標準化完成")
    
    # 比對數據數量
    print(f"\n📊 數據數量比對:")
    print(f"   資料庫: {len(db_compare)} 條")
    print(f"   API: {len(api_compare)} 條")
    print(f"   差異: {abs(len(db_compare) - len(api_compare))} 條")
    
    # 比對時間範圍
    print(f"\n📅 時間範圍比對:")
    print(f"   資料庫: {db_compare['timestamp'].min()} 到 {db_compare['timestamp'].max()}")
    print(f"   API: {api_compare['timestamp'].min()} 到 {api_compare['timestamp'].max()}")
    
    # 找出共同的時間戳
    db_timestamps = set(db_compare['timestamp'])
    api_timestamps = set(api_compare['timestamp'])
    common_timestamps = db_timestamps.intersection(api_timestamps)
    
    print(f"\n🔗 共同時間戳: {len(common_timestamps)} 個")
    
    # 比對共同時間戳的數據
    if common_timestamps:
        print(f"\n🔍 比對共同時間戳的數據...")
        
        # 篩選共同時間戳的數據
        db_common = db_compare[db_compare['timestamp'].isin(common_timestamps)].copy()
        api_common = api_compare[api_compare['timestamp'].isin(common_timestamps)].copy()
        
        # 按時間戳排序
        db_common = db_common.sort_values('timestamp').reset_index(drop=True)
        api_common = api_common.sort_values('timestamp').reset_index(drop=True)
        
        # 比對價格數據
        price_columns = ['open', 'high', 'low', 'close']
        differences = []
        
        for col in price_columns:
            # 計算差異
            diff = (db_common[col] - api_common[col]).abs()
            max_diff = diff.max()
            mean_diff = diff.mean()
            
            differences.append({
                'column': col,
                'max_diff': max_diff,
                'mean_diff': mean_diff,
                'max_diff_idx': diff.idxmax() if max_diff > 0 else None
            })
            
            print(f"   {col}: 最大差異={max_diff:.6f}, 平均差異={mean_diff:.6f}")
        
        # 找出最大差異的數據點
        max_diff_overall = max(differences, key=lambda x: x['max_diff'])
        if max_diff_overall['max_diff'] > 0.001:  # 如果差異大於0.001
            idx = max_diff_overall['max_diff_idx']
            if idx is not None:
                print(f"\n⚠️  發現最大差異:")
                print(f"   時間: {db_common.loc[idx, 'timestamp']}")
                print(f"   欄位: {max_diff_overall['column']}")
                print(f"   資料庫值: {db_common.loc[idx, max_diff_overall['column']]:.6f}")
                print(f"   API值: {api_common.loc[idx, max_diff_overall['column']]:.6f}")
                print(f"   差異: {max_diff_overall['max_diff']:.6f}")
        
        # 檢查是否有完全不同的數據
        print(f"\n🔍 檢查數據一致性...")
        
        # 檢查是否有完全不同的數據點
        different_data = []
        for i in range(min(len(db_common), len(api_common))):
            db_row = db_common.iloc[i]
            api_row = api_common.iloc[i]
            
            if db_row['timestamp'] != api_row['timestamp']:
                different_data.append({
                    'index': i,
                    'db_timestamp': db_row['timestamp'],
                    'api_timestamp': api_row['timestamp'],
                    'type': 'timestamp_mismatch'
                })
                continue
            
            # 檢查價格差異
            for col in price_columns:
                if abs(db_row[col] - api_row[col]) > 0.001:
                    different_data.append({
                        'index': i,
                        'timestamp': db_row['timestamp'],
                        'column': col,
                        'db_value': db_row[col],
                        'api_value': api_row[col],
                        'difference': abs(db_row[col] - api_row[col]),
                        'type': 'price_difference'
                    })
        
        if different_data:
            print(f"⚠️  發現 {len(different_data)} 個不同的數據點:")
            for diff in different_data[:10]:  # 只顯示前10個
                if diff['type'] == 'timestamp_mismatch':
                    print(f"   索引 {diff['index']}: 時間戳不匹配")
                    print(f"     資料庫: {diff['db_timestamp']}")
                    print(f"     API: {diff['api_timestamp']}")
                else:
                    print(f"   索引 {diff['index']}: {diff['column']} 差異")
                    print(f"     時間: {diff['timestamp']}")
                    print(f"     資料庫: {diff['db_value']:.6f}")
                    print(f"     API: {diff['api_value']:.6f}")
                    print(f"     差異: {diff['difference']:.6f}")
                print()
        else:
            print("✅ 共同時間戳的數據完全一致")
    
    # 檢查獨有的數據
    db_only = db_timestamps - api_timestamps
    api_only = api_timestamps - db_timestamps
    
    print(f"\n📈 獨有數據分析:")
    print(f"   僅資料庫有: {len(db_only)} 個時間戳")
    print(f"   僅API有: {len(api_only)} 個時間戳")
    
    if db_only:
        print(f"   資料庫獨有時間戳 (前5個):")
        for ts in sorted(list(db_only))[:5]:
            print(f"     {ts}")
    
    if api_only:
        print(f"   API獨有時間戳 (前5個):")
        for ts in sorted(list(api_only))[:5]:
            print(f"     {ts}")
    
    print(f"\n📋 比對完成!")

def check_frontend_processing():
    """檢查前端數據處理邏輯"""
    print(f"\n🔍 檢查前端數據處理邏輯...")
    print("=" * 80)
    
    # 獲取API原始數據
    api_df = get_api_data('XAUUSD', 'D1', 50)
    if api_df is None:
        return
    
    print(f"\n📊 API原始數據樣本 (前5條):")
    print(api_df.head().to_string())
    
    # 模擬前端處理邏輯
    print(f"\n🔄 模擬前端處理邏輯...")
    
    # 1. 檢查數據驗證
    print(f"1. 數據驗證檢查:")
    invalid_data = []
    for idx, row in api_df.iterrows():
        if pd.isna(row['timestamp']) or pd.isna(row['open']) or pd.isna(row['high']) or pd.isna(row['low']) or pd.isna(row['close']):
            invalid_data.append(idx)
    
    if invalid_data:
        print(f"   ⚠️  發現 {len(invalid_data)} 條無效數據")
    else:
        print(f"   ✅ 所有數據都有效")
    
    # 2. 檢查時間戳格式
    print(f"2. 時間戳格式檢查:")
    try:
        api_df['timestamp_parsed'] = pd.to_datetime(api_df['timestamp'])
        print(f"   ✅ 時間戳解析成功")
    except Exception as e:
        print(f"   ❌ 時間戳解析失敗: {e}")
    
    # 3. 檢查數據排序
    print(f"3. 數據排序檢查:")
    api_df_sorted = api_df.sort_values('timestamp')
    is_sorted = api_df_sorted.equals(api_df)
    print(f"   {'✅' if is_sorted else '⚠️'} 數據{'已' if is_sorted else '未'}排序")
    
    # 4. 檢查數據去重
    print(f"4. 數據去重檢查:")
    duplicates = api_df.duplicated(subset=['timestamp']).sum()
    print(f"   {'⚠️' if duplicates > 0 else '✅'} 發現 {duplicates} 個重複時間戳")
    
    # 5. 檢查價格邏輯
    print(f"5. 價格邏輯檢查:")
    invalid_prices = []
    for idx, row in api_df.iterrows():
        if row['high'] < max(row['open'], row['close']) or row['low'] > min(row['open'], row['close']):
            invalid_prices.append(idx)
    
    if invalid_prices:
        print(f"   ⚠️  發現 {len(invalid_prices)} 條價格邏輯錯誤")
    else:
        print(f"   ✅ 價格邏輯正確")

if __name__ == "__main__":
    print("🔍 數據來源比對工具")
    print("=" * 60)
    
    # 比對數據來源
    compare_data_sources('XAUUSD', 'D1', 100)
    
    # 檢查前端處理邏輯
    check_frontend_processing() 
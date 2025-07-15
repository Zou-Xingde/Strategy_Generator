#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速樣本資料處理腳本
先處理一小部分資料讓用戶能快速看到圖表效果
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import TICK_CSV_PATH, TIMEFRAMES, DUCKDB_PATH
from src.database.connection import DuckDBConnection

def quick_process_sample(sample_size=50000):
    """
    快速處理樣本資料
    
    Args:
        sample_size: 要處理的資料筆數
    """
    
    print(f"🚀 開始處理前 {sample_size:,} 筆Tick資料...")
    
    try:
        # 讀取樣本資料
        print("📖 正在讀取CSV文件...")
        df = pd.read_csv(TICK_CSV_PATH, nrows=sample_size)
        print(f"✅ 成功讀取 {len(df):,} 筆資料")
        
        # 標準化資料
        print("🔧 正在標準化資料...")
        df['timestamp'] = pd.to_datetime(df['DateTime'], format='%Y%m%d %H:%M:%S.%f')
        df['bid'] = df['Bid'].astype(float)
        df['ask'] = df['Ask'].astype(float)
        df['volume'] = df['Volume'].astype(int)
        
        # 計算中間價
        df['price'] = (df['bid'] + df['ask']) / 2
        
        # 移除異常值
        df = df[(df['bid'] > 0) & (df['ask'] > 0)]
        df = df.dropna()
        
        print(f"✅ 資料清洗完成，剩餘 {len(df):,} 筆資料")
        
        # 設置時間戳為索引
        df = df.set_index('timestamp')
        
        # 連接資料庫
        print("🗄️ 正在連接資料庫...")
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            symbol = "EXUSA30IDXUSD"
            
            # 為每個時間週期生成蠟燭圖資料
            for timeframe, pandas_tf in TIMEFRAMES.items():
                print(f"📊 正在生成 {timeframe} 蠟燭圖資料...")
                
                try:
                    # 重新採樣生成蠟燭圖
                    candlestick = df['price'].resample(pandas_tf).agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last'
                    })
                    
                    # 添加成交量
                    volume = df['volume'].resample(pandas_tf).sum()
                    candlestick['volume'] = volume
                    
                    # 移除空值
                    candlestick = candlestick.dropna()
                    
                    if len(candlestick) > 0:
                        # 重置索引
                        candlestick = candlestick.reset_index()
                        
                        # 插入到資料庫
                        db.insert_candlestick_data(candlestick, timeframe, symbol)
                        
                        print(f"✅ {timeframe}: 生成 {len(candlestick)} 筆蠟燭圖資料")
                    else:
                        print(f"⚠️ {timeframe}: 沒有生成資料")
                        
                except Exception as e:
                    print(f"❌ {timeframe}: 處理失敗 - {e}")
                    continue
        
        print("🎉 樣本資料處理完成！")
        print("📊 現在可以啟動前端查看圖表了")
        print("💡 運行: python start_frontend.py")
        
    except Exception as e:
        print(f"❌ 處理過程中發生錯誤: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_process_sample() 
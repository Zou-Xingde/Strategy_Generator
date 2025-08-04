#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
D1數據處理腳本
使用Tick數據分批處理生成D1時間週期的蠟燭圖資料並更新資料庫
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import time

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import TICK_CSV_PATH, DUCKDB_PATH, CHUNK_SIZE
from src.database.connection import DuckDBConnection

def process_d1_data():
    """
    使用Tick數據分批處理生成D1數據並更新資料庫
    """
    
    print("🚀 開始分批處理D1數據...")
    print(f"📁 使用Tick數據文件: {TICK_CSV_PATH}")
    print(f"⏰ 時間週期: D1")
    print(f"📦 分批大小: {CHUNK_SIZE:,} 筆")
    
    start_time = time.time()
    
    try:
        # 檢查Tick文件是否存在
        if not os.path.exists(TICK_CSV_PATH):
            print(f"❌ Tick文件不存在: {TICK_CSV_PATH}")
            return
        
        # 初始化D1資料容器
        d1_data = []
        total_processed = 0
        
        print("📖 開始分批讀取Tick CSV文件...")
        
        # 分批讀取CSV
        chunk_iter = pd.read_csv(TICK_CSV_PATH, chunksize=CHUNK_SIZE)
        
        for chunk_num, chunk in enumerate(chunk_iter):
            chunk_start = time.time()
            
            # 標準化Tick資料
            chunk['timestamp'] = pd.to_datetime(chunk['DateTime'], format='%Y%m%d %H:%M:%S.%f')
            chunk['bid'] = chunk['Bid'].astype(float)
            chunk['ask'] = chunk['Ask'].astype(float)
            chunk['volume'] = chunk['Volume'].astype(int)
            
            # 計算中間價
            chunk['price'] = (chunk['bid'] + chunk['ask']) / 2
            
            # 移除異常值
            chunk = chunk[(chunk['bid'] > 0) & (chunk['ask'] > 0)]
            chunk = chunk.dropna()
            
            if len(chunk) > 0:
                # 設置時間戳為索引
                chunk = chunk.set_index('timestamp')
                
                # 生成D1蠟燭圖資料
                candlestick = chunk['price'].resample('1D').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last'
                })
                
                # 添加成交量
                volume = chunk['volume'].resample('1D').sum()
                candlestick['volume'] = volume
                
                # 移除空值並添加到容器
                candlestick = candlestick.dropna()
                if len(candlestick) > 0:
                    candlestick = candlestick.reset_index()
                    d1_data.append(candlestick)
            
            total_processed += len(chunk)
            chunk_time = time.time() - chunk_start
            
            # 每處理10個批次顯示進度
            if (chunk_num + 1) % 10 == 0:
                print(f"📊 已處理 {chunk_num + 1} 批次，共 {total_processed:,} 筆資料，耗時 {chunk_time:.2f}秒")
        
        print(f"✅ 分批處理完成，共處理 {total_processed:,} 筆Tick資料")
        
        if len(d1_data) > 0:
            # 合併所有D1資料
            print("🔗 正在合併D1資料...")
            final_d1 = pd.concat(d1_data, ignore_index=True)
            
            # 按日期分組並重新計算OHLCV
            print("📊 正在重新計算D1 OHLCV...")
            final_d1 = final_d1.groupby(final_d1['timestamp'].dt.date).agg({
                'timestamp': 'first',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).reset_index(drop=True)
            
            # 轉換日期回時間戳
            final_d1['timestamp'] = pd.to_datetime(final_d1['timestamp'])
            
            print(f"✅ 成功生成 {len(final_d1):,} 筆D1蠟燭圖資料")
            
            # 連接資料庫
            print("🗄️ 正在連接資料庫...")
            with DuckDBConnection(str(DUCKDB_PATH)) as db:
                symbol = "EXUSA30IDXUSD"
                timeframe = "D1"
                
                print(f"📊 正在更新 {timeframe} 蠟燭圖資料...")
                
                try:
                    # 先清除舊的D1資料
                    print("🗑️ 清除舊的D1資料...")
                    db.conn.execute(f"DELETE FROM candlestick_data WHERE symbol = '{symbol}' AND timeframe = '{timeframe}'")
                    
                    # 插入新的D1資料
                    db.insert_candlestick_data(final_d1, timeframe, symbol)
                    
                    print(f"✅ {timeframe}: 成功更新 {len(final_d1):,} 筆蠟燭圖資料")
                    
                    # 顯示資料範圍
                    start_date = final_d1['timestamp'].min()
                    end_date = final_d1['timestamp'].max()
                    print(f"📅 資料範圍: {start_date} 到 {end_date}")
                    
                    # 顯示一些統計資訊
                    print(f"📈 最高價: {final_d1['high'].max():.2f}")
                    print(f"📉 最低價: {final_d1['low'].min():.2f}")
                    print(f"📊 平均成交量: {final_d1['volume'].mean():,.0f}")
                    
                except Exception as e:
                    print(f"❌ {timeframe}: 處理失敗 - {e}")
                    import traceback
                    traceback.print_exc()
        else:
            print("⚠️ 沒有生成有效的D1資料")
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print("🎉 D1數據處理完成！")
        print(f"⏱️ 總處理時間: {processing_time:.2f} 秒")
        print(f"📊 平均處理速度: {total_processed/processing_time:,.0f} 筆/秒")
        print("📊 現在可以重新啟動前端查看更新的圖表了")
        
    except Exception as e:
        print(f"❌ 處理過程中發生錯誤: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    process_d1_data() 
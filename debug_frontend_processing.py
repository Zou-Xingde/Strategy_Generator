#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
模擬前端JavaScript數據處理邏輯
找出數據處理過程中的問題
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

def get_api_data(symbol='XAUUSD', timeframe='D1', limit=100):
    """從API獲取數據"""
    try:
        url = f"http://127.0.0.1:8050/api/candlestick/{symbol}/{timeframe}?limit={limit}"
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        
        if 'error' in data or 'data' not in data or not data['data']:
            return None
        
        return data['data']
        
    except Exception as e:
        print(f"❌ 獲取API數據失敗: {e}")
        return None

def simulate_frontend_processing(raw_data):
    """模擬前端JavaScript的數據處理邏輯"""
    print(f"🔄 模擬前端數據處理邏輯...")
    print(f"   原始數據: {len(raw_data)} 條")
    
    # 1. 模擬 processDataBatchFast 方法
    print(f"\n1. 模擬 processDataBatchFast 處理...")
    
    result = []
    seen = set()  # 用於快速去重
    
    for i, item in enumerate(raw_data):
        # 快速驗證
        if not item or not item.get('timestamp') or \
           item.get('open') is None or item.get('high') is None or \
           item.get('low') is None or item.get('close') is None:
            continue
        
        # 快速去重
        if item['timestamp'] in seen:
            continue
        seen.add(item['timestamp'])
        
        # 快速轉換
        try:
            timestamp = datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00'))
            if timestamp is None:
                continue
        except:
            continue
        
        result.append({
            'time': int(timestamp.timestamp()),
            'open': float(item['open']),
            'high': float(item['high']),
            'low': float(item['low']),
            'close': float(item['close'])
        })
    
    print(f"   處理後數據: {len(result)} 條")
    print(f"   過濾掉: {len(raw_data) - len(result)} 條")
    
    # 2. 模擬 getInitialRenderData 方法
    print(f"\n2. 模擬 getInitialRenderData 處理...")
    
    full_data = result
    max_initial_bars = 5000  # 最多顯示5000個bar
    
    if len(full_data) <= max_initial_bars:
        initial_render_data = full_data
        print(f"   數據量 <= {max_initial_bars}，返回完整數據")
    else:
        # 顯示最新的數據
        initial_render_data = full_data[-max_initial_bars:]
        print(f"   數據量 > {max_initial_bars}，返回最新 {len(initial_render_data)} 條")
    
    print(f"   初始渲染數據: {len(initial_render_data)} 條")
    
    # 3. 檢查數據排序
    print(f"\n3. 檢查數據排序...")
    
    # 按時間排序
    initial_render_data.sort(key=lambda x: x['time'])
    print(f"   數據已按時間排序")
    
    # 4. 檢查時間間隔
    print(f"\n4. 檢查時間間隔...")
    
    time_gaps = []
    for i in range(1, len(initial_render_data)):
        gap = initial_render_data[i]['time'] - initial_render_data[i-1]['time']
        time_gaps.append(gap)
    
    if time_gaps:
        expected_gap = 86400  # 1天 = 86400秒
        abnormal_gaps = [gap for gap in time_gaps if gap > expected_gap * 1.5]
        
        print(f"   總間隔數: {len(time_gaps)}")
        print(f"   預期間隔: {expected_gap} 秒 (1天)")
        print(f"   平均間隔: {sum(time_gaps) / len(time_gaps):.0f} 秒")
        print(f"   最大間隔: {max(time_gaps)} 秒")
        print(f"   異常間隔: {len(abnormal_gaps)} 個")
        
        if abnormal_gaps:
            print(f"   異常間隔詳情:")
            for i, gap in enumerate(abnormal_gaps[:5]):
                print(f"     {gap} 秒 ({gap/86400:.1f} 天)")
    else:
        print(f"   只有1條數據，無法計算間隔")
    
    # 5. 檢查價格跳空
    print(f"\n5. 檢查價格跳空...")
    
    price_gaps = []
    for i in range(1, len(initial_render_data)):
        prev_close = initial_render_data[i-1]['close']
        curr_open = initial_render_data[i]['open']
        gap = abs(curr_open - prev_close)
        gap_percent = (gap / prev_close * 100) if prev_close != 0 else 0
        price_gaps.append(gap_percent)
    
    if price_gaps:
        large_gaps = [gap for gap in price_gaps if gap > 5]  # >5%
        
        print(f"   總跳空數: {len(price_gaps)}")
        print(f"   平均跳空: {sum(price_gaps) / len(price_gaps):.2f}%")
        print(f"   最大跳空: {max(price_gaps):.2f}%")
        print(f"   大跳空(>5%): {len(large_gaps)} 個")
        
        if large_gaps:
            print(f"   大跳空詳情:")
            for i, gap in enumerate(large_gaps[:5]):
                print(f"     {gap:.2f}%")
    else:
        print(f"   只有1條數據，無法計算跳空")
    
    # 6. 檢查數據完整性
    print(f"\n6. 檢查數據完整性...")
    
    # 檢查是否有缺失的時間點
    if len(initial_render_data) > 1:
        start_time = initial_render_data[0]['time']
        end_time = initial_render_data[-1]['time']
        expected_days = (end_time - start_time) / 86400
        actual_days = len(initial_render_data)
        
        print(f"   時間範圍: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
        print(f"   預期天數: {expected_days:.1f} 天")
        print(f"   實際數據: {actual_days} 條")
        print(f"   數據密度: {actual_days / expected_days:.2f} 條/天")
        
        if actual_days / expected_days < 0.8:
            print(f"   ⚠️  數據密度偏低，可能有缺失")
        else:
            print(f"   ✅ 數據密度正常")
    
    return initial_render_data

def check_chart_rendering_data(processed_data):
    """檢查圖表渲染數據"""
    print(f"\n7. 檢查圖表渲染數據...")
    
    if not processed_data:
        print(f"   ❌ 沒有處理後的數據")
        return
    
    # 檢查數據格式是否符合LightweightCharts要求
    print(f"   數據格式檢查:")
    
    valid_count = 0
    invalid_count = 0
    
    for item in processed_data:
        # 檢查必要欄位
        if all(key in item for key in ['time', 'open', 'high', 'low', 'close']):
            # 檢查數據類型
            if (isinstance(item['time'], int) and 
                isinstance(item['open'], (int, float)) and
                isinstance(item['high'], (int, float)) and
                isinstance(item['low'], (int, float)) and
                isinstance(item['close'], (int, float))):
                valid_count += 1
            else:
                invalid_count += 1
        else:
            invalid_count += 1
    
    print(f"   有效數據: {valid_count} 條")
    print(f"   無效數據: {invalid_count} 條")
    
    if invalid_count > 0:
        print(f"   ⚠️  發現無效數據格式")
    else:
        print(f"   ✅ 所有數據格式正確")
    
    # 檢查時間戳範圍
    if processed_data:
        times = [item['time'] for item in processed_data]
        min_time = min(times)
        max_time = max(times)
        
        print(f"   時間戳範圍: {min_time} 到 {max_time}")
        print(f"   對應日期: {datetime.fromtimestamp(min_time)} 到 {datetime.fromtimestamp(max_time)}")
        
        # 檢查時間戳是否合理
        current_time = datetime.now().timestamp()
        if max_time > current_time:
            print(f"   ⚠️  發現未來時間戳")
        else:
            print(f"   ✅ 時間戳範圍合理")

def main():
    print("🔍 前端數據處理邏輯調試工具")
    print("=" * 60)
    
    # 獲取API數據
    raw_data = get_api_data('XAUUSD', 'D1', 200)
    if not raw_data:
        print("❌ 無法獲取API數據")
        return
    
    print(f"📊 獲取到 {len(raw_data)} 條原始數據")
    
    # 模擬前端處理
    processed_data = simulate_frontend_processing(raw_data)
    
    # 檢查圖表渲染數據
    check_chart_rendering_data(processed_data)
    
    print(f"\n📋 調試完成!")

if __name__ == "__main__":
    main() 
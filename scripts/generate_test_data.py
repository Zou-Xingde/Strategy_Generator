#!/usr/bin/env python3
"""
测试数据生成脚本 - 生成模拟的蜡烛图数据
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DuckDBConnection
from config.settings import DUCKDB_PATH

def generate_test_data():
    """生成测试数据"""
    print("🔧 开始生成测试数据...")
    
    symbol = "EXUSA30IDXUSD"
    base_price = 44000.0
    
    # 生成不同时间框架的数据
    timeframes = {
        'D1': {'days': 365, 'interval': timedelta(days=1)},
        'H4': {'days': 90, 'interval': timedelta(hours=4)},
        'H1': {'days': 30, 'interval': timedelta(hours=1)},
        'M30': {'days': 7, 'interval': timedelta(minutes=30)},
        'M15': {'days': 3, 'interval': timedelta(minutes=15)},
        'M5': {'days': 1, 'interval': timedelta(minutes=5)},
        'M1': {'days': 1, 'interval': timedelta(minutes=1)}
    }
    
    with DuckDBConnection(str(DUCKDB_PATH)) as db:
        total_records = 0
        
        for timeframe, config in timeframes.items():
            print(f"📊 生成 {timeframe} 数据...")
            
            # 计算数据点数量
            if timeframe == 'D1':
                num_points = config['days']
            elif timeframe == 'H4':
                num_points = config['days'] * 6
            elif timeframe == 'H1':
                num_points = config['days'] * 24
            elif timeframe == 'M30':
                num_points = config['days'] * 24 * 2
            elif timeframe == 'M15':
                num_points = config['days'] * 24 * 4
            elif timeframe == 'M5':
                num_points = config['days'] * 24 * 12
            elif timeframe == 'M1':
                num_points = config['days'] * 24 * 60
            
            # 生成时间序列
            end_date = datetime.now()
            start_date = end_date - timedelta(days=config['days'])
            
            timestamps = []
            current_date = start_date
            while current_date <= end_date and len(timestamps) < num_points:
                timestamps.append(current_date)
                current_date += config['interval']
            
            # 生成价格数据
            data = []
            current_price = base_price
            
            for i, timestamp in enumerate(timestamps):
                # 生成随机价格变动
                price_change = np.random.normal(0, 50)  # 正态分布的价格变动
                current_price += price_change
                
                # 确保价格为正数
                current_price = max(current_price, 10000)
                
                # 生成OHLC数据
                high = current_price + abs(np.random.normal(0, 20))
                low = current_price - abs(np.random.normal(0, 20))
                open_price = current_price + np.random.normal(0, 10)
                close_price = current_price + np.random.normal(0, 10)
                
                # 确保OHLC逻辑正确
                high = max(high, open_price, close_price)
                low = min(low, open_price, close_price)
                
                # 生成成交量
                volume = int(np.random.uniform(1000, 10000))
                
                data.append({
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': timestamp,
                    'open': round(open_price, 2),
                    'high': round(high, 2),
                    'low': round(low, 2),
                    'close': round(close_price, 2),
                    'volume': volume
                })
            
            # 创建DataFrame
            df = pd.DataFrame(data)
            
            # 插入数据库
            db.insert_candlestick_data(df, timeframe, symbol)
            
            print(f"✅ {timeframe}: 生成了 {len(data)} 条记录")
            total_records += len(data)
        
        print(f"\n🎉 数据生成完成！总共生成了 {total_records} 条记录")

if __name__ == "__main__":
    generate_test_data() 
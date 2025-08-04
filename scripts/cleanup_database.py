#!/usr/bin/env python3
"""
数据库清理脚本 - 清理重复的蜡烛图数据
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import DuckDBConnection
from config.settings import DUCKDB_PATH

def cleanup_database():
    """清理数据库中的重复数据"""
    print("🧹 开始清理数据库重复数据...")
    
    symbol = "EXUSA30IDXUSD"
    timeframes = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1', 'W1', 'MN']
    
    with DuckDBConnection(str(DUCKDB_PATH)) as db:
        total_cleaned = 0
        
        for timeframe in timeframes:
            try:
                # 检查重复数据数量
                duplicate_count = db.get_duplicate_count(symbol, timeframe)
                
                if duplicate_count > 0:
                    print(f"📊 {timeframe}: 发现 {duplicate_count} 条重复数据")
                    
                    # 清理重复数据
                    cleaned_count = db.cleanup_duplicate_data(symbol, timeframe)
                    total_cleaned += cleaned_count
                    
                    print(f"✅ {timeframe}: 已清理 {cleaned_count} 条重复数据")
                else:
                    print(f"✅ {timeframe}: 无重复数据")
                    
            except Exception as e:
                print(f"❌ {timeframe}: 清理失败 - {e}")
        
        print(f"\n🎉 清理完成！总共清理了 {total_cleaned} 条重复数据")

if __name__ == "__main__":
    cleanup_database() 
#!/usr/bin/env python
"""
根據日期範圍生成波段數據

這個腳本用於根據指定的日期範圍，為特定交易品種和時間週期生成波段數據。
可以處理單一時間週期或所有可用的時間週期。
"""

import argparse
import logging
from datetime import datetime, timedelta
from src.data_processing.swing_processor import SwingProcessor

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """解析命令行參數"""
    parser = argparse.ArgumentParser(description='根據日期範圍生成波段數據')
    
    parser.add_argument('--symbol', type=str, required=True, help='交易品種 (必填)')
    parser.add_argument('--timeframe', type=str, help='時間週期 (不指定則處理所有時間週期)')
    parser.add_argument('--algorithm', type=str, default='zigzag', help='演算法名稱 (預設: zigzag)')
    parser.add_argument('--deviation', type=float, default=5.0, help='ZigZag最小變動百分比 (預設: 5.0)')
    parser.add_argument('--depth', type=int, default=12, help='ZigZag回溯深度 (預設: 12)')
    parser.add_argument('--start-date', type=str, help='開始日期 (格式: YYYY-MM-DD，不指定則使用最早的數據)')
    parser.add_argument('--end-date', type=str, help='結束日期 (格式: YYYY-MM-DD，不指定則使用最新的數據)')
    parser.add_argument('--batch-size', type=int, default=10000, help='每批處理的資料筆數 (預設: 10000)')
    parser.add_argument('--years', type=int, help='要處理的年數 (從結束日期往前推算，優先於 start-date)')
    
    return parser.parse_args()

def main():
    """主函數"""
    args = parse_args()
    
    # 設定演算法參數
    algo_params = {
        'deviation': args.deviation,
        'depth': args.depth
    }
    
    # 處理日期範圍
    end_date = args.end_date
    start_date = args.start_date
    
    # 如果指定了處理的年數，計算開始日期
    if args.years:
        if end_date:
            end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_datetime = datetime.now()
            end_date = end_datetime.strftime('%Y-%m-%d')
        
        start_datetime = end_datetime - timedelta(days=365 * args.years)
        start_date = start_datetime.strftime('%Y-%m-%d')
        logger.info(f"根據指定的 {args.years} 年計算日期範圍: {start_date} 到 {end_date}")
    
    # 初始化處理器
    processor = SwingProcessor()
    
    try:
        if args.timeframe:
            # 處理單一時間週期
            logger.info(f"開始處理 {args.symbol} {args.timeframe} 的波段資料")
            logger.info(f"日期範圍: {start_date or '最早'} 到 {end_date or '最新'}")
            
            result = processor.process_symbol_timeframe_by_date_range(
                args.symbol, args.timeframe, args.algorithm,
                start_date=start_date, end_date=end_date,
                batch_size=args.batch_size, **algo_params
            )
            
            logger.info(f"處理完成: {result}")
        else:
            # 處理所有時間週期
            logger.info(f"開始處理 {args.symbol} 的所有時間週期波段資料")
            logger.info(f"日期範圍: {start_date or '最早'} 到 {end_date or '最新'}")
            
            results = processor.process_all_timeframes_by_date_range(
                args.symbol, args.algorithm,
                start_date=start_date, end_date=end_date,
                batch_size=args.batch_size, **algo_params
            )
            
            logger.info(f"處理完成，成功處理 {len(results)} 個時間週期")
            for result in results:
                logger.info(f"  - {result['timeframe']}: {result['swing_points']} 個波段點")
            
    except Exception as e:
        logger.error(f"處理過程中發生錯誤: {e}")
        raise

if __name__ == "__main__":
    main()
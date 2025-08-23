"""
這個檔案包含需要添加到 src/data_processing/swing_processor.py 的新方法
請將以下方法複製到 SwingProcessor 類中
"""

# 添加到 SwingProcessor 類中的新方法

def process_symbol_timeframe_by_date_range(self, symbol: str, timeframe: str, 
                                        algorithm_name: str = 'zigzag', 
                                        start_date: Optional[str] = None, 
                                        end_date: Optional[str] = None,
                                        batch_size: int = 10000, 
                                        **algorithm_params):
    """
    根據日期範圍處理特定交易品種和時間週期的波段資料
    
    Args:
        symbol: 交易品種
        timeframe: 時間週期
        algorithm_name: 演算法名稱
        start_date: 開始日期 (格式: 'YYYY-MM-DD')，如果為None則使用最早的數據
        end_date: 結束日期 (格式: 'YYYY-MM-DD')，如果為None則使用最新的數據
        batch_size: 每批處理的資料筆數
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
            
            # 分批處理
            all_swing_points = []
            total_swing_points = 0
            
            # 計算需要多少批次
            num_batches = (total_records + batch_size - 1) // batch_size
            logger.info(f"將分 {num_batches} 批處理資料")
            
            for batch_num in range(num_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, total_records)
                
                logger.info(f"處理批次 {batch_num + 1}/{num_batches}: 索引 {start_idx} 到 {end_idx}")
                
                # 獲取當前批次的資料
                batch_df = candlestick_df.iloc[start_idx:end_idx].copy()
                
                # 計算波段
                result_df = algorithm.calculate(batch_df)
                
                # 收集波段點
                batch_swing_points = algorithm.get_swing_points(result_df)
                all_swing_points.extend(batch_swing_points)
                total_swing_points += len(batch_swing_points)
                
                logger.info(f"批次 {batch_num + 1} 完成，找到 {len(batch_swing_points)} 個波段點")
            
            # 合併所有批次的結果並存儲到資料庫
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
                                      batch_size: int = 10000, 
                                      **algorithm_params):
    """
    根據日期範圍處理所有時間週期的波段資料
    
    Args:
        symbol: 交易品種
        algorithm_name: 演算法名稱
        timeframes: 要處理的時間週期列表，如果為None則處理所有可用時間週期
        start_date: 開始日期 (格式: 'YYYY-MM-DD')，如果為None則使用最早的數據
        end_date: 結束日期 (格式: 'YYYY-MM-DD')，如果為None則使用最新的數據
        batch_size: 每批處理的資料筆數
        **algorithm_params: 演算法參數
    """
    try:
        logger.info(f"開始處理 {symbol} 的所有時間週期波段資料，日期範圍: {start_date or '最早'} 到 {end_date or '最新'}")
        
        with DuckDBConnection(self.db_path) as db:
            # 獲取可用的時間週期
            if timeframes is None:
                available_timeframes = db.get_available_timeframes(symbol)
            else:
                available_timeframes = [tf for tf in timeframes if tf in TIMEFRAMES]
            
            if not available_timeframes:
                logger.warning(f"沒有找到 {symbol} 的可用時間週期")
                return []
            
            results = []
            
            for timeframe in available_timeframes:
                try:
                    result = self.process_symbol_timeframe_by_date_range(
                        symbol, timeframe, algorithm_name, 
                        start_date, end_date, batch_size, 
                        **algorithm_params
                    )
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"處理 {symbol} {timeframe} 失敗: {e}")
                    continue
            
            logger.info(f"完成處理 {symbol} 的波段資料，成功處理 {len(results)} 個時間週期")
            return results
            
    except Exception as e:
        logger.error(f"處理所有時間週期失敗: {e}")
        raise

# 添加到 main 函數中
# 在 main 函數結尾前添加以下代碼:

"""
parser.add_argument('--start-date', type=str, help='開始日期 (格式: YYYY-MM-DD)')
parser.add_argument('--end-date', type=str, help='結束日期 (格式: YYYY-MM-DD)')
parser.add_argument('--use-date-range', action='store_true', help='使用日期範圍處理資料')

args = parser.parse_args()

# ... 原有代碼 ...

if args.use_date_range:
    if args.timeframe:
        # 處理單一時間週期，使用日期範圍
        result = processor.process_symbol_timeframe_by_date_range(
            args.symbol, args.timeframe, args.algorithm,
            start_date=args.start_date, end_date=args.end_date,
            batch_size=args.batch_size, 
            deviation=args.deviation, depth=args.depth
        )
        print(f"處理結果: {result}")
    else:
        # 處理所有時間週期，使用日期範圍
        results = processor.process_all_timeframes_by_date_range(
            args.symbol, args.algorithm,
            start_date=args.start_date, end_date=args.end_date,
            batch_size=args.batch_size,
            deviation=args.deviation, depth=args.depth
        )
        print(f"處理結果: {results}")
else:
    # 原有的處理邏輯
"""
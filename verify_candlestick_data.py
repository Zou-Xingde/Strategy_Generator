#!/usr/bin/env python3
"""
驗證原始 Tick 資料與蠟燭圖資料的一致性
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# 設定
DUCKDB_PATH = "database/market_data.duckdb"

# 商品配置
SYMBOLS_CONFIG = {
    "XAUUSD": {
        "name": "黃金",
        "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorXAUUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
    },
    "US30": {
        "name": "道瓊斯",
        "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorUSA30IDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
    },
    "US100": {
        "name": "納斯達克",
        "tick_file": r"D:\project\策略產生器\data\Strategy_GeneratorUSATECHIDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
    }
}

# 時間框架配置
TIMEFRAMES = {
    "M1": "1min",
    "M5": "5min", 
    "M15": "15min",
    "M30": "30min",
    "H1": "1h",
    "H4": "4h",
    "D1": "1D"
}

def clear_screen():
    """清屏"""
    os.system('cls' if os.name == 'nt' else 'clear')

def load_tick_sample(file_path: str, sample_size: int = 100000) -> pd.DataFrame:
    """載入 Tick 資料樣本"""
    try:
        print(f"📊 載入 Tick 資料樣本 ({sample_size:,} 筆)...")
        
        # 讀取樣本資料
        df = pd.read_csv(file_path, nrows=sample_size)
        df = df.rename(columns={"DateTime": "timestamp"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
        df = df.dropna(subset=["timestamp"])
        df = df.sort_values("timestamp")
        
        # 計算中價
        df["mid_price"] = (df["Bid"] + df["Ask"]) / 2
        df["volume"] = df.get("Volume", 1000000)
        
        print(f"✅ 成功載入 {len(df):,} 筆有效 Tick 資料")
        print(f"   時間範圍: {df['timestamp'].min()} 到 {df['timestamp'].max()}")
        print(f"   價格範圍: {df['mid_price'].min():.4f} 到 {df['mid_price'].max():.4f}")
        
        return df
        
    except Exception as e:
        print(f"❌ 載入 Tick 資料失敗: {e}")
        return pd.DataFrame()

def generate_test_candlesticks(tick_df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """從 Tick 資料生成測試用蠟燭圖"""
    try:
        if tick_df.empty:
            return pd.DataFrame()
        
        # 設定索引
        tick_df_indexed = tick_df.set_index("timestamp")
        
        # 重新取樣
        pandas_tf = TIMEFRAMES[timeframe]
        ohlc_data = tick_df_indexed["mid_price"].resample(pandas_tf).ohlc()
        volume_data = tick_df_indexed["volume"].resample(pandas_tf).sum()
        
        # 合併資料
        candlestick_df = pd.DataFrame({
            "timestamp": ohlc_data.index,
            "open": ohlc_data["open"],
            "high": ohlc_data["high"], 
            "low": ohlc_data["low"],
            "close": ohlc_data["close"],
            "volume": volume_data
        })
        
        # 移除無效資料
        candlestick_df = candlestick_df.dropna()
        
        return candlestick_df
        
    except Exception as e:
        print(f"❌ 生成測試蠟燭圖失敗: {e}")
        return pd.DataFrame()

def load_db_candlesticks(conn, symbol: str, timeframe: str, start_time, end_time) -> pd.DataFrame:
    """從資料庫載入蠟燭圖資料"""
    try:
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM v_candlestick_latest
            WHERE symbol = ? AND timeframe = ?
            AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
        """
        
        result = conn.execute(query, [symbol, timeframe, start_time, end_time]).fetchdf()
        
        if not result.empty:
            result["timestamp"] = pd.to_datetime(result["timestamp"])
        
        return result
        
    except Exception as e:
        print(f"❌ 載入資料庫蠟燭圖失敗: {e}")
        return pd.DataFrame()

def compare_candlesticks(test_df: pd.DataFrame, db_df: pd.DataFrame, symbol: str, timeframe: str) -> Dict:
    """比較測試生成的蠟燭圖與資料庫中的蠟燭圖"""
    print(f"\n🔍 比較 {symbol} {timeframe} 蠟燭圖資料...")
    
    result = {
        "symbol": symbol,
        "timeframe": timeframe,
        "test_count": len(test_df),
        "db_count": len(db_df),
        "match_count": 0,
        "price_differences": [],
        "volume_differences": [],
        "success": False
    }
    
    if test_df.empty and db_df.empty:
        print("   ⚠️ 兩邊都沒有資料")
        result["success"] = True
        return result
    
    if test_df.empty:
        print("   ❌ 測試資料為空")
        return result
        
    if db_df.empty:
        print("   ❌ 資料庫資料為空")
        return result
    
    print(f"   測試資料: {len(test_df):,} 根")
    print(f"   資料庫資料: {len(db_df):,} 根")
    
    # 合併資料進行比較
    test_df_indexed = test_df.set_index("timestamp")
    db_df_indexed = db_df.set_index("timestamp")
    
    # 找出共同的時間戳
    common_timestamps = test_df_indexed.index.intersection(db_df_indexed.index)
    
    if len(common_timestamps) == 0:
        print("   ❌ 沒有共同的時間戳")
        return result
    
    print(f"   共同時間戳: {len(common_timestamps):,} 個")
    
    # 比較價格
    price_matches = 0
    volume_matches = 0
    
    for ts in common_timestamps[:min(1000, len(common_timestamps))]:  # 限制比較數量
        test_row = test_df_indexed.loc[ts]
        db_row = db_df_indexed.loc[ts]
        
        # 價格比較（允許小誤差）
        price_tolerance = 0.0001
        price_match = (
            abs(test_row["open"] - db_row["open"]) <= price_tolerance and
            abs(test_row["high"] - db_row["high"]) <= price_tolerance and
            abs(test_row["low"] - db_row["low"]) <= price_tolerance and
            abs(test_row["close"] - db_row["close"]) <= price_tolerance
        )
        
        if price_match:
            price_matches += 1
        else:
            result["price_differences"].append({
                "timestamp": ts,
                "test": [test_row["open"], test_row["high"], test_row["low"], test_row["close"]],
                "db": [db_row["open"], db_row["high"], db_row["low"], db_row["close"]]
            })
        
        # 成交量比較（允許較大誤差）
        volume_tolerance = 0.01  # 1%
        if test_row["volume"] > 0:
            volume_diff = abs(test_row["volume"] - db_row["volume"]) / test_row["volume"]
            if volume_diff <= volume_tolerance:
                volume_matches += 1
            else:
                result["volume_differences"].append({
                    "timestamp": ts,
                    "test": test_row["volume"],
                    "db": db_row["volume"],
                    "diff_pct": volume_diff * 100
                })
    
    compared_count = min(1000, len(common_timestamps))
    price_match_rate = price_matches / compared_count if compared_count > 0 else 0
    volume_match_rate = volume_matches / compared_count if compared_count > 0 else 0
    
    result["match_count"] = compared_count
    result["price_match_rate"] = price_match_rate
    result["volume_match_rate"] = volume_match_rate
    
    print(f"   比較樣本: {compared_count:,} 根")
    print(f"   價格匹配率: {price_match_rate*100:.1f}%")
    print(f"   成交量匹配率: {volume_match_rate*100:.1f}%")
    
    # 顯示部分差異樣本
    if len(result["price_differences"]) > 0:
        print(f"   價格差異樣本 (前3個):")
        for i, diff in enumerate(result["price_differences"][:3]):
            print(f"     {diff['timestamp']}: 測試{diff['test']} vs 資料庫{diff['db']}")
    
    if len(result["volume_differences"]) > 0:
        print(f"   成交量差異樣本 (前3個):")
        for i, diff in enumerate(result["volume_differences"][:3]):
            print(f"     {diff['timestamp']}: 測試{diff['test']:.0f} vs 資料庫{diff['db']:.0f} (差異{diff['diff_pct']:.1f}%)")
    
    # 判斷是否成功
    result["success"] = price_match_rate >= 0.95 and volume_match_rate >= 0.90
    
    if result["success"]:
        print("   ✅ 資料一致性驗證通過")
    else:
        print("   ❌ 資料一致性驗證失敗")
    
    return result

def verify_symbol(symbol: str, config: Dict, conn) -> Dict:
    """驗證單個商品的資料"""
    print(f"\n{'='*60}")
    print(f"🎯 驗證 {config['name']} ({symbol})")
    print(f"{'='*60}")
    
    # 載入 Tick 資料樣本
    tick_df = load_tick_sample(config['tick_file'])
    
    if tick_df.empty:
        return {"symbol": symbol, "success": False, "error": "無法載入 Tick 資料"}
    
    # 獲取時間範圍
    start_time = tick_df['timestamp'].min()
    end_time = tick_df['timestamp'].max()
    
    results = []
    
    # 驗證每個時間框架
    for timeframe in ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]:
        print(f"\n🔸 驗證 {timeframe} 時間框架...")
        
        # 生成測試蠟燭圖
        test_candlesticks = generate_test_candlesticks(tick_df, timeframe)
        
        # 載入資料庫蠟燭圖
        db_candlesticks = load_db_candlesticks(conn, symbol, timeframe, start_time, end_time)
        
        # 比較資料
        comparison_result = compare_candlesticks(test_candlesticks, db_candlesticks, symbol, timeframe)
        results.append(comparison_result)
    
    # 計算總體成功率
    successful_timeframes = sum(1 for r in results if r["success"])
    total_timeframes = len(results)
    success_rate = successful_timeframes / total_timeframes if total_timeframes > 0 else 0
    
    print(f"\n📊 {symbol} 驗證結果:")
    print(f"   成功時間框架: {successful_timeframes}/{total_timeframes}")
    print(f"   成功率: {success_rate*100:.1f}%")
    
    return {
        "symbol": symbol,
        "success": success_rate >= 0.8,  # 80% 時間框架成功
        "success_rate": success_rate,
        "timeframe_results": results
    }

def main():
    """主函數"""
    clear_screen()
    print("🔍 驗證原始 Tick 資料與蠟燭圖資料一致性")
    print("=" * 60)
    
    # 連接資料庫
    print("\n🔌 連接資料庫...")
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        print("✅ 資料庫連接成功")
    except Exception as e:
        print(f"❌ 資料庫連接失敗: {e}")
        return
    
    # 驗證每個商品
    all_results = []
    
    for symbol, config in SYMBOLS_CONFIG.items():
        result = verify_symbol(symbol, config, conn)
        all_results.append(result)
    
    # 關閉資料庫連接
    conn.close()
    
    # 顯示總結報告
    print(f"\n{'='*60}")
    print("📋 驗證總結報告")
    print(f"{'='*60}")
    
    overall_success = True
    
    for result in all_results:
        symbol = result["symbol"]
        config = SYMBOLS_CONFIG[symbol]
        success = result.get("success", False)
        
        print(f"\n🔸 {config['name']} ({symbol}):")
        
        if "error" in result:
            print(f"   ❌ 錯誤: {result['error']}")
            overall_success = False
        else:
            success_rate = result.get("success_rate", 0)
            print(f"   狀態: {'✅ 通過' if success else '❌ 失敗'}")
            print(f"   成功率: {success_rate*100:.1f}%")
            
            if not success:
                overall_success = False
            
            # 顯示時間框架詳情
            for tf_result in result.get("timeframe_results", []):
                tf = tf_result["timeframe"]
                tf_success = tf_result["success"]
                test_count = tf_result["test_count"]
                db_count = tf_result["db_count"]
                
                status = "✅" if tf_success else "❌"
                print(f"     {tf}: {status} (測試:{test_count:,} vs 資料庫:{db_count:,})")
    
    print(f"\n🎯 整體驗證結果: {'✅ 全部通過' if overall_success else '❌ 部分失敗'}")
    
    if overall_success:
        print("💡 所有商品的蠟燭圖資料與原始 Tick 資料一致！")
    else:
        print("⚠️ 部分商品的蠟燭圖資料需要重新處理")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
import duckdb

conn = duckdb.connect('database/market_data.duckdb', read_only=True)

print("🔍 檢查當前重建進度...")
print("=" * 40)

# 檢查重建進度
try:
    result = conn.execute("""
        SELECT symbol, timeframe, COUNT(*) as count 
        FROM candlestick_data_new 
        WHERE data_source = 'final_rebuild'
        GROUP BY symbol, timeframe 
        ORDER BY symbol, timeframe
    """).fetchall()
    
    if result:
        print("📊 當前進度:")
        symbol_totals = {}
        for symbol, timeframe, count in result:
            if symbol not in symbol_totals:
                symbol_totals[symbol] = 0
            symbol_totals[symbol] += count
            print(f"  {symbol} {timeframe}: {count:,} 根")
        
        print("\n📈 各商品小計:")
        total_all = 0
        for symbol, total in symbol_totals.items():
            symbol_name = {
                "XAUUSD": "黃金",
                "US30": "道瓊斯", 
                "US100": "納斯達克"
            }.get(symbol, symbol)
            print(f"  {symbol_name} ({symbol}): {total:,} 根")
            total_all += total
        
        print(f"\n🎯 總計: {total_all:,} 根蠟燭")
    else:
        print("⚠️ 尚未有重建資料")

except Exception as e:
    print(f"❌ 查詢失敗: {e}")

# 檢查資料庫檔案大小
import os
if os.path.exists('database/market_data.duckdb'):
    size_mb = os.path.getsize('database/market_data.duckdb') / 1024 / 1024
    print(f"\n💾 資料庫檔案大小: {size_mb:.1f} MB")

conn.close()
print("\n✅ 檢查完成")
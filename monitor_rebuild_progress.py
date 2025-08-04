#!/usr/bin/env python3
"""
監控重建進度的腳本
"""

import duckdb
import time
import os
import subprocess
from datetime import datetime

def clear_screen():
    """清屏"""
    os.system('cls' if os.name == 'nt' else 'clear')

def get_python_processes():
    """獲取 Python 進程"""
    try:
        result = subprocess.run(
            ['powershell', '-Command', 'Get-Process python -ErrorAction SilentlyContinue | Select-Object Id,ProcessName,StartTime,CPU,WorkingSet | ConvertTo-Json'],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            import json
            processes = json.loads(result.stdout)
            if isinstance(processes, dict):
                processes = [processes]
            return processes
    except:
        pass
    return []

def get_db_progress():
    """獲取資料庫進度"""
    try:
        conn = duckdb.connect("database/market_data.duckdb", read_only=True)
        
        # 查詢各商品進度
        result = conn.execute("""
            SELECT symbol, timeframe, COUNT(*) as count 
            FROM candlestick_data_new 
            WHERE data_source = 'final_rebuild'
            GROUP BY symbol, timeframe 
            ORDER BY symbol, timeframe
        """).fetchall()
        
        conn.close()
        return result
    except Exception as e:
        return None

def get_db_file_info():
    """獲取資料庫檔案資訊"""
    db_path = "database/market_data.duckdb"
    if not os.path.exists(db_path):
        return None
    
    stat = os.stat(db_path)
    return {
        "size_mb": stat.st_size / 1024 / 1024,
        "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%H:%M:%S")
    }

def main():
    """主監控迴圈"""
    print("🔍 開始監控重建進度...")
    print("按 Ctrl+C 停止監控")
    print()
    
    last_db_size = 0
    last_check_time = time.time()
    
    while True:
        try:
            current_time = time.time()
            
            clear_screen()
            print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            # 檢查 Python 進程
            processes = get_python_processes()
            rebuild_running = False
            
            if processes:
                for proc in processes:
                    if isinstance(proc, dict):
                        pid = proc.get('Id', 'N/A')
                        name = proc.get('ProcessName', 'N/A')
                        cpu = proc.get('CPU', 0)
                        memory_mb = proc.get('WorkingSet', 0) / 1024 / 1024
                        start_time = proc.get('StartTime', 'N/A')
                        
                        print(f"🐍 Python 進程 (PID: {pid}):")
                        print(f"   CPU: {cpu:.1f}% | 記憶體: {memory_mb:.1f} MB")
                        if start_time != 'N/A':
                            print(f"   開始時間: {start_time}")
                        
                        rebuild_running = True
            
            if not rebuild_running:
                print("❌ 沒有 Python 重建進程運行")
            
            print()
            
            # 檢查資料庫檔案
            db_info = get_db_file_info()
            if db_info:
                print(f"💾 資料庫檔案:")
                print(f"   大小: {db_info['size_mb']:.1f} MB")
                print(f"   最後修改: {db_info['modified']}")
                
                # 計算增長速度
                if last_db_size > 0:
                    time_diff = current_time - last_check_time
                    size_diff = db_info['size_mb'] - last_db_size
                    if time_diff > 0:
                        growth_rate = size_diff / time_diff * 60  # MB/min
                        print(f"   增長速度: {growth_rate:.2f} MB/min")
                
                last_db_size = db_info['size_mb']
                last_check_time = current_time
            else:
                print("❌ 資料庫檔案不存在")
            
            print()
            
            # 檢查重建進度
            progress = get_db_progress()
            if progress:
                print("📊 重建進度:")
                
                # 按商品分組
                symbol_data = {}
                for symbol, timeframe, count in progress:
                    if symbol not in symbol_data:
                        symbol_data[symbol] = {}
                    symbol_data[symbol][timeframe] = count
                
                total_candles = 0
                for symbol, timeframes in symbol_data.items():
                    symbol_total = sum(timeframes.values())
                    total_candles += symbol_total
                    
                    symbol_name = {
                        "XAUUSD": "黃金",
                        "US30": "道瓊斯", 
                        "US100": "納斯達克"
                    }.get(symbol, symbol)
                    
                    print(f"   🔸 {symbol_name} ({symbol}): {symbol_total:,} 根")
                    for tf, count in sorted(timeframes.items()):
                        print(f"     {tf}: {count:,}")
                
                print(f"\n   🎯 總計: {total_candles:,} 根蠟燭")
                
                # 檢查是否完成
                expected_symbols = ["XAUUSD", "US30", "US100"]
                completed_symbols = len(symbol_data)
                
                if completed_symbols >= len(expected_symbols) and total_candles > 100000:
                    print(f"\n🎉 重建可能已完成！({completed_symbols}/{len(expected_symbols)} 商品)")
                else:
                    print(f"\n⏳ 進行中... ({completed_symbols}/{len(expected_symbols)} 商品)")
                    
            else:
                print("📊 進度: 正在初始化或寫入中...")
            
            print()
            print("按 Ctrl+C 停止監控")
            print("-" * 60)
            
            # 等待10秒
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n👋 監控已停止")
            break
        except Exception as e:
            print(f"\n⚠️ 監控出錯: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
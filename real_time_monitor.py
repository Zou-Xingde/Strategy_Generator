#!/usr/bin/env python3
"""
實時監控腳本 - 安全監控資料庫重建進度
"""
import time
import os
import subprocess
from pathlib import Path
from datetime import datetime

def get_python_process_info():
    """獲取 Python 進程資訊"""
    try:
        result = subprocess.run(
            ['powershell', '-Command', 'Get-Process python -ErrorAction SilentlyContinue | Select-Object Id,CPU,WorkingSet,StartTime | ConvertTo-Json'],
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

def get_db_file_info():
    """獲取資料庫檔案資訊"""
    db_path = Path("database/market_data.duckdb")
    if not db_path.exists():
        return None
    
    stat = db_path.stat()
    return {
        "size_mb": stat.st_size / 1024 / 1024,
        "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%H:%M:%S"),
        "exists": True
    }

def try_read_db_progress():
    """嘗試讀取資料庫進度（只讀模式）"""
    try:
        import duckdb
        conn = duckdb.connect("database/market_data.duckdb", read_only=True)
        result = conn.execute("""
            SELECT symbol, COUNT(*) as count 
            FROM candlestick_data_new 
            GROUP BY symbol 
            ORDER BY symbol
        """).fetchall()
        conn.close()
        return result
    except Exception as e:
        return None

def monitor_realtime():
    """實時監控"""
    print("🔍 實時監控資料庫重建進度")
    print("=" * 60)
    print("按 Ctrl+C 停止監控")
    print()
    
    last_db_size = 0
    last_check_time = time.time()
    
    while True:
        try:
            current_time = time.time()
            
            # 獲取 Python 進程資訊
            processes = get_python_process_info()
            
            # 獲取資料庫檔案資訊
            db_info = get_db_file_info()
            
            # 清屏並顯示當前時間
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            # 顯示進程狀態
            if processes:
                for proc in processes:
                    if isinstance(proc, dict):
                        pid = proc.get('Id', 'N/A')
                        cpu = proc.get('CPU', 0)
                        memory_mb = proc.get('WorkingSet', 0) / 1024 / 1024
                        start_time = proc.get('StartTime', 'N/A')
                        
                        print(f"🐍 Python 重建進程:")
                        print(f"   PID: {pid}")
                        print(f"   CPU: {cpu:.1f}%")
                        print(f"   記憶體: {memory_mb:.1f} MB")
                        print(f"   開始時間: {start_time}")
                        
                        # 計算運行時間
                        if start_time != 'N/A':
                            try:
                                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                                runtime = datetime.now() - start_dt.replace(tzinfo=None)
                                print(f"   運行時間: {runtime}")
                            except:
                                pass
            else:
                print("❌ 沒有 Python 重建進程運行")
            
            print()
            
            # 顯示資料庫檔案狀態
            if db_info:
                print(f"💾 資料庫檔案:")
                print(f"   大小: {db_info['size_mb']:.1f} MB")
                print(f"   最後修改: {db_info['modified']}")
                
                # 計算檔案增長速度
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
            
            # 嘗試讀取資料庫進度
            progress = try_read_db_progress()
            if progress:
                print("📊 目前進度:")
                total = 0
                for symbol, count in progress:
                    print(f"   {symbol}: {count:,} 根 K 線")
                    total += count
                print(f"   總計: {total:,} 根 K 線")
                
                # 檢查是否完成
                if len(progress) >= 3:
                    print("\n🎉 所有商品處理完成！")
                    break
            else:
                print("📊 進度: 資料庫正在寫入中...")
            
            print()
            print("按 Ctrl+C 停止監控")
            print("-" * 60)
            
            # 等待 5 秒
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\n👋 監控已停止")
            break
        except Exception as e:
            print(f"\n⚠️ 監控出錯: {e}")
            time.sleep(5)

if __name__ == "__main__":
    monitor_realtime() 
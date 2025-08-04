#!/usr/bin/env python3
"""
進度監控腳本 - 安全監控資料庫重建進度
"""
import time
import os
from pathlib import Path
import psutil

def check_python_processes():
    """檢查正在運行的 Python 進程"""
    python_procs = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info', 'cmdline']):
        try:
            if 'python' in proc.info['name'].lower():
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'rebuild_database' in cmdline:
                    python_procs.append({
                        'pid': proc.info['pid'],
                        'cpu': proc.info['cpu_percent'],
                        'memory_mb': proc.info['memory_info'].rss / 1024 / 1024,
                        'cmdline': cmdline
                    })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return python_procs

def check_db_file_status():
    """檢查資料庫檔案狀態"""
    db_path = Path("database/market_data.duckdb")
    if not db_path.exists():
        return {"exists": False}
    
    stat = db_path.stat()
    return {
        "exists": True,
        "size_mb": stat.st_size / 1024 / 1024,
        "modified": time.ctime(stat.st_mtime)
    }

def monitor_progress():
    """監控重建進度"""
    print("🔍 資料庫重建進度監控")
    print("=" * 50)
    
    while True:
        # 檢查 Python 進程
        procs = check_python_processes()
        print(f"\n⏰ {time.strftime('%H:%M:%S')}")
        
        if procs:
            for proc in procs:
                print(f"🐍 Python 進程運行中:")
                print(f"   PID: {proc['pid']}")
                print(f"   CPU: {proc['cpu']:.1f}%")
                print(f"   記憶體: {proc['memory_mb']:.1f} MB")
                print(f"   命令: {proc['cmdline']}")
        else:
            print("✅ 沒有重建進程運行")
        
        # 檢查資料庫檔案
        db_status = check_db_file_status()
        if db_status["exists"]:
            print(f"💾 資料庫檔案: {db_status['size_mb']:.1f} MB")
            print(f"📅 最後修改: {db_status['modified']}")
        else:
            print("❌ 資料庫檔案不存在")
        
        # 如果沒有進程運行，嘗試檢查資料庫內容
        if not procs:
            try:
                import duckdb
                conn = duckdb.connect("database/market_data.duckdb", read_only=True)
                result = conn.execute("""
                    SELECT symbol, COUNT(*) as count 
                    FROM candlestick_data_new 
                    GROUP BY symbol 
                    ORDER BY symbol
                """).fetchall()
                
                print("📊 最終結果:")
                total = 0
                for symbol, count in result:
                    print(f"   {symbol}: {count:,} 根 K 線")
                    total += count
                print(f"   總計: {total:,} 根 K 線")
                conn.close()
                
                if total > 0:
                    print("\n🎉 重建完成！")
                    break
                    
            except Exception as e:
                print(f"⚠️  無法讀取資料庫: {e}")
        
        print("-" * 30)
        time.sleep(15)  # 每 15 秒檢查一次

if __name__ == "__main__":
    try:
        monitor_progress()
    except KeyboardInterrupt:
        print("\n👋 監控已停止") 
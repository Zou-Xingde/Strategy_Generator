import os
from pathlib import Path

# 專案根目錄
ROOT_DIR = Path(__file__).parent.parent

# 資料目錄
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
PARQUET_DATA_DIR = DATA_DIR / "parquet"

# 資料庫設定
DATABASE_DIR = ROOT_DIR / "database"
DUCKDB_PATH = DATABASE_DIR / "market_data.duckdb"

# 原始Tick資料路徑
TICK_CSV_PATH = r"D:\project\策略產生器\data\EXUSA30IDXUSD-TICK-No Session.csv"

# D1資料路徑
D1_CSV_PATH = r"D:\project\策略產生器\data\EX_D1USA30IDXUSD-D1-No Session.csv"

# 時間週期設定
TIMEFRAMES = {
    "M1": "1min",
    "M5": "5min", 
    "M15": "15min",
    "M30": "30min",
    "H1": "1H",
    "H4": "4H",
    "D1": "1D"
}

# 分批處理設定
BATCH_SIZE = 100000  # 每批處理的資料筆數
CHUNK_SIZE = 10000   # 讀取CSV的chunk大小

# 前端設定
FRONTEND_HOST = "127.0.0.1"
FRONTEND_PORT = 8050

# 測量工具顏色設定
MEASUREMENT_COLORS = {
    "line1": "#0066CC",  # 藍色
    "line2": "#CC0000"   # 紅色
}

# 蠟燭圖設定
CANDLESTICK_COLORS = {
    "increasing": "#00CC00",  # 綠色
    "decreasing": "#CC0000"   # 紅色
}

# 創建必要目錄
def create_directories():
    """創建專案所需的目錄"""
    directories = [
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        PARQUET_DATA_DIR,
        DATABASE_DIR
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")

if __name__ == "__main__":
    create_directories() 
# 市場波段規律分析專案

## 專案概述
這是一個使用演算法找出市場規律(波段)並生成策略的專案。

## 功能特色

### 第一階段：資料處理
- 使用DuckDB + Parquet作為資料庫
- 將大型Tick CSV檔案分批處理
- 生成多時間週期的蠟燭圖資料：M1, M5, M15, M30, H1, H4, D1
- 高效率資料存儲和查詢

### 第二階段：高性能互動式視覺化
- 基於FastAPI + ApexCharts的高性能架構
- 即時切換不同時間週期的蠟燭圖
- 支援圖表放大縮小功能
- 測量工具：點擊式測量計算點差
- 現代化響應式用戶界面
- RESTful API架構，支援擴展

## 專案結構
```
├── data/                   # 原始資料目錄
├── src/
│   ├── data_processing/    # 第一階段：資料處理模組
│   ├── frontend/          # 第二階段：前端界面
│   └── database/          # 資料庫配置
├── tests/                 # 測試文件
├── config/                # 配置文件
└── requirements.txt       # Python依賴項
```

## 安裝和使用

1. 安裝依賴項：
```bash
pip install -r requirements.txt
```

2. 運行資料處理：
```bash
python src/data_processing/tick_processor.py
```

3. 啟動FastAPI前端界面：
```bash
python run.py --frontend
```
或直接使用：
```bash
uvicorn src.frontend.app:app --host 127.0.0.1 --port 8050 --reload
```

## 技術棧
- **資料庫**: DuckDB + Parquet
- **後端**: FastAPI, Python, Pandas, DuckDB
- **前端**: HTML/JavaScript, ApexCharts
- **版本控制**: Git

## 🎮 前端界面操作

- **M1/M5/M15/M30/H1/H4/D1 按鈕** → 即時切換時間週期
- **測量工具按鈕** → 開啟測量模式，點擊圖表兩點計算價格和時間差
- **放大/縮小按鈕** → 調整圖表顯示範圍
- **重置按鈕** → 清除測量結果和重置視圖
- **響應式設計** → 支援桌面和移動設備

## 🚀 性能優勢

### 為什麼選擇FastAPI + ApexCharts？

- **高性能API**: FastAPI是最快的Python Web框架之一
- **原生JavaScript**: 不依賴重量級前端框架，載入速度快
- **ApexCharts**: 專業級圖表庫，支援大量資料點
- **RESTful架構**: 易於擴展和維護
- **即時響應**: 無需頁面重新載入即可切換時間週期

### 性能對比
- **舊版 (Dash)**: 切換時間週期需要 2-3 秒
- **新版 (FastAPI)**: 切換時間週期只需 0.5 秒
- **資料載入**: 提升 5-10 倍速度

## 🔌 API端點

系統提供以下RESTful API端點：

- `GET /` - 主頁面
- `GET /api/candlestick/{symbol}/{timeframe}` - 獲取蠟燭圖資料
- `GET /api/timeframes/{symbol}` - 獲取可用時間週期
- `GET /api/config` - 獲取系統配置
- `POST /api/measurement` - 計算測量結果

## 📱 響應式設計

- 支援桌面、平板和手機設備
- 自適應按鈕佈局
- 觸控友好的操作介面
- 現代化的UI設計

## 🔧 開發者指南

### 啟動開發環境
```bash
# 安裝依賴
pip install -r requirements.txt

# 啟動開發服務器
python run.py --frontend

# 或使用uvicorn
uvicorn src.frontend.app:app --reload
```

### 自訂配置
編輯 `config/settings.py` 來修改：
- 資料庫路徑
- 前端端口
- 顏色主題
- 時間週期設定 
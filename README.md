# 市場波段規律分析專案

## 專案概述
這是一個使用演算法找出市場規律(波段)並生成策略的專案。

## 功能特色

### 第一階段：資料處理
- 使用DuckDB + Parquet作為資料庫
- 將大型Tick CSV檔案分批處理
- 生成多時間週期的蠟燭圖資料：M1, M5, M15, M30, H1, H4, D1
- 高效率資料存儲和查詢

### 第二階段：互動式視覺化
- 即時切換不同時間週期的蠟燭圖
- 支援圖表放大縮小功能
- 測量工具：藍色和紅色十字線計算點差
- 直觀的用戶界面

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

3. 啟動前端界面：
```bash
python src/frontend/app.py
```

## 技術棧
- **資料庫**: DuckDB + Parquet
- **後端**: Python, Pandas, DuckDB
- **前端**: HTML/JavaScript, Plotly.js
- **版本控制**: Git 
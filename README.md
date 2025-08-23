# Market Swing Strategy Generator

一個高性能的市場波段分析系統，使用 Python FastAPI 和 TradingView Lightweight Charts 構建。

## 🚀 功能特色

- **多時間框架支持**: M1, M5, M15, M30, H1, H4, D1, W1, MN
- **多品種支持**: XAUUSD (黃金), US30 (道瓊斯), US100 (納斯達克)
- **智能波段識別**: 使用 ZigZag 算法自動識別市場波段
- **高性能圖表**: 基於 TradingView Lightweight Charts 的專業級圖表
- **實時數據處理**: 支持大量歷史數據的快速處理和顯示
- **測量工具**: 內建價格和時間測量功能
- **響應式設計**: 現代化的 Web 界面，支持全屏顯示

## 📊 技術架構

### 後端技術
- **Python 3.8+**: 主要開發語言
- **FastAPI**: 高性能 Web 框架
- **DuckDB**: 高性能列式數據庫
- **Pandas**: 數據處理和分析
- **NumPy**: 數值計算

### 前端技術
- **HTML5/CSS3**: 現代化 Web 標準
- **JavaScript ES6+**: 前端邏輯
- **TradingView Lightweight Charts**: 專業級圖表庫
- **響應式設計**: 適配各種設備

### 數據處理
- **ZigZag 算法**: 智能波段識別
- **虛擬化渲染**: 支持大量數據的高性能顯示
- **智能緩存**: 優化數據加載和渲染性能

## 🛠️ 快速啟動

### 啟動指令
```bash
# 啟動前端服務
python run.py --frontend
```

### 訪問地址
- **舊頁面**: http://127.0.0.1:8050
- **波段分析頁面**: http://127.0.0.1:8050/swing

## 🛠️ 安裝和運行

### 環境要求
- Python 3.8 或更高版本
- Git

### 安裝步驟

1. **克隆專案**
```bash
git clone https://github.com/Zou-Xingde/Strategy_Generator.git
cd Strategy_Generator
```

2. **創建虛擬環境**
```bash
python -m venv .venv
```

3. **激活虛擬環境**
```bash
# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate
```

4. **安裝依賴**
```bash
pip install -r requirements.txt
```

5. **啟動應用**
```bash
python run.py --frontend
```

6. **訪問應用**
- 主頁面: http://127.0.0.1:8050
- 波段分析頁面: http://127.0.0.1:8050/swing

## 📁 專案結構

```
market_swing_cursor/
├── src/                          # 源代碼目錄
│   ├── algorithms/               # 算法實現
│   │   ├── zigzag.py            # ZigZag 算法
│   │   └── zigzag_improved.py   # 改進版算法
│   ├── database/                 # 數據庫相關
│   │   └── connection.py        # 數據庫連接
│   ├── data_processing/          # 數據處理
│   │   └── swing_processor.py   # 波段處理器
│   └── frontend/                 # 前端代碼
│       ├── app.py               # FastAPI 應用
│       ├── static/              # 靜態文件
│       │   ├── app.js          # 主要 JavaScript
│       │   └── style.css       # 樣式文件
│       └── templates/           # HTML 模板
│           └── index.html      # 主頁面
├── config/                       # 配置文件
│   └── settings.py              # 應用設置
├── database/                     # 數據庫文件
├── scripts/                      # 工具腳本
├── requirements.txt              # Python 依賴
├── run.py                       # 主啟動腳本
└── README.md                    # 專案說明
```

## 🎯 主要功能

### 1. 波段分析
- 自動識別市場高點和低點
- 計算波段強度和持續時間
- 支持多種時間框架的波段分析

### 2. 圖表功能
- 專業級 K線圖顯示
- 波段連接線和標記點
- 縮放和平移功能
- 十字線和測量工具

### 3. 數據管理
- 支持大量歷史數據
- 智能數據緩存和虛擬化
- 高性能數據處理

### 4. 用戶界面
- 現代化響應式設計
- 直觀的操作界面
- 支持全屏顯示

## 🔧 配置說明

### 數據庫配置
在 `config/settings.py` 中配置數據庫路徑：
```python
DUCKDB_PATH = Path("database/market_data.duckdb")
```

### 前端配置
在 `src/frontend/app.py` 中配置服務器設置：
```python
FRONTEND_HOST = "127.0.0.1"
FRONTEND_PORT = 8050
```

## 📈 使用指南

### 基本操作
1. **選擇品種**: 點擊品種下拉選單選擇交易品種
2. **選擇時間框架**: 點擊時間框架按鈕切換週期
3. **顯示波段**: 點擊 "📈" 按鈕顯示波段分析
4. **測量工具**: 點擊 "📏" 按鈕啟用測量功能
5. **縮放控制**: 使用 "+" "-" 按鈕或滾輪縮放圖表

### 高級功能
- **波段列表**: 點擊 "📋" 查看詳細波段信息
- **全屏顯示**: 點擊 "⛶" 進入全屏模式
- **十字線**: 點擊 "✚" 啟用十字線功能

## 🐛 故障排除

### 常見問題

1. **前端無法顯示波段**
   - 檢查數據庫中是否有對應的波段數據
   - 確認算法名稱設置正確（預設為 `zigzag_fixed`）

2. **圖表顯示異常**
   - 清除瀏覽器緩存
   - 檢查 JavaScript 控制台錯誤信息

3. **數據加載緩慢**
   - 檢查數據庫文件大小
   - 考慮優化數據查詢

### 日誌查看
應用運行時會在終端顯示詳細日誌，包括：
- 數據庫連接狀態
- API 請求信息
- 錯誤和警告信息

## 🤝 貢獻指南

歡迎提交 Issue 和 Pull Request！

### 開發環境設置
1. Fork 專案
2. 創建功能分支
3. 提交更改
4. 發起 Pull Request

### 代碼規範
- 使用 Python 3.8+ 語法
- 遵循 PEP 8 代碼風格
- 添加適當的註釋和文檔

## 📄 許可證

本專案採用 MIT 許可證 - 詳見 [LICENSE](LICENSE) 文件

## 📞 聯繫方式

- GitHub: [@Zou-Xingde](https://github.com/Zou-Xingde)
- 專案地址: https://github.com/Zou-Xingde/Strategy_Generator

## 🎉 更新日誌

### v2.0.0 (2025-08-23)
- ✅ 修復ZigZag算法長時間範圍處理問題
- ✅ 實現動態deviation調整策略 (3% → 1.5% → 1.0%)
- ✅ 移除批處理邏輯干擾，統一使用整體處理模式
- ✅ 優化循環終止條件，確保完整數據覆蓋
- ✅ 增強診斷和故障恢復機制
- ✅ 修復WebSocket消息處理和彈出窗口功能
- ✅ 完善前端UI交互體驗

### v1.0.0 (2025-01-15)
- ✅ 修復波段數據顯示問題
- ✅ 改進時間戳處理邏輯
- ✅ 優化前端性能
- ✅ 添加完整的錯誤處理
- ✅ 改進用戶界面體驗

---

**注意**: 本專案僅供學習和研究使用，不構成投資建議。請謹慎使用並自負風險。 

## Cleanup 清理紀錄

- 本專案已進行清理，移除舊版/不再使用的「波段生成/重建與處理」、「資料重建與遷移（資料庫/資料）」與「測試」相關腳本，避免與目前僅保留的前端展示與核心庫衝突。
- 前端仍維持以下檔案：`src/frontend/app.py`、`src/frontend/templates/index.html`、`src/frontend/static/app.js`、`src/frontend/static/style.css`，以及啟動腳本 `start_frontend.py`。
- 詳細清單請參考 `CLEANUP_REPORT.md`。

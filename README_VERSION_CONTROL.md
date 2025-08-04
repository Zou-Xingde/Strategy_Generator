# 版本控制資料庫架構 - 總結

## 🎯 設計目標

根據您的需求，我們重新設計了資料庫架構，實現了以下核心目標：

### 1. 資料分類管理
- **靜態資料**：蠟燭圖等價格資料（不常變動）
- **動態資料**：演算法計算結果（會因參數調整而變化）

### 2. 版本控制機制
- 支援多版本並存，**不覆寫舊資料**
- 演算法結果與參數組合綁定
- 自動版本雜湊生成和追蹤

### 3. 歷史追溯能力
- 支援 A/B 測試和回溯分析
- 自動歸檔機制
- 長期歷史資料保留

## 🏗️ 架構概覽

### 資料庫表結構

```
📊 靜態資料表
├── candlestick_data (蠟燭圖) - 支援版本控制
└── tick_data (Tick資料) - 支援版本控制

🔄 動態資料表  
├── algorithm_versions (演算法版本控制)
├── swing_data (波段資料) - 支援多版本
├── algorithm_statistics (統計結果) - 支援多版本
└── swing_data_archive (歷史歸檔)

📈 索引優化
├── 複合索引支援多維度查詢
├── 版本雜湊索引加速版本查找
└── 時間戳索引支援時間範圍查詢
```

## 🔧 核心功能

### 1. 演算法版本管理

```python
# 註冊新版本
version_hash = db.register_algorithm_version(
    "zigzag", 
    {"deviation": 5.0, "depth": 12},
    "zigzag_standard", 
    "標準參數版本"
)

# 設定活躍版本
db.set_active_version("zigzag", version_hash)

# 查詢版本列表
versions = db.get_algorithm_versions("zigzag")
```

### 2. 資料版本控制

```python
# 插入不同版本的蠟燭圖資料
db.insert_candlestick_data(df, "M5", "EXUSA30IDXUSD", "v1.0", "csv")
db.insert_candlestick_data(df_v2, "M5", "EXUSA30IDXUSD", "v1.1", "api")

# 查詢指定版本資料
df_v1 = db.get_candlestick_data("EXUSA30IDXUSD", "M5", "v1.0")
df_v2 = db.get_candlestick_data("EXUSA30IDXUSD", "M5", "v1.1")
```

### 3. 波段處理與版本綁定

```python
# 使用不同參數計算波段（自動生成不同版本）
processor = SwingProcessor()

# 標準參數版本
result1 = processor.process_symbol_timeframe(
    "EXUSA30IDXUSD", "M5", "zigzag", 
    batch_size=10000, deviation=5.0, depth=12
)

# 敏感參數版本
result2 = processor.process_symbol_timeframe(
    "EXUSA30IDXUSD", "M5", "zigzag", 
    batch_size=10000, deviation=3.0, depth=8
)
```

### 4. 版本比較與追溯

```python
# 比較不同版本的統計結果
stats1 = db.get_algorithm_statistics("EXUSA30IDXUSD", "M5", "zigzag", version_hash1)
stats2 = db.get_algorithm_statistics("EXUSA30IDXUSD", "M5", "zigzag", version_hash2)

# 版本比較工具
comparison = compare_algorithm_versions("zigzag", version1, version2)
```

## 📊 資料流程

### 1. 靜態資料更新流程
```
CSV/API → 驗證資料 → 版本標識 → 插入新版本 → 可選：設定活躍版本
```

### 2. 演算法結果更新流程
```
蠟燭圖資料 → 演算法計算 → 參數雜湊 → 版本註冊 → 結果存儲 → 統計計算
```

### 3. 版本切換流程
```
查詢現有版本 → 比較參數 → 註冊新版本 → 設定活躍版本 → 清理舊版本（可選）
```

## 🔄 分批處理機制

### 1. 大資料集處理
```python
# 支援分批處理避免記憶體問題
result = processor.process_symbol_timeframe(
    "EXUSA30IDXUSD", "M5", "zigzag", 
    batch_size=10000,  # 每批處理10000筆
    deviation=5.0, depth=12
)
```

### 2. 進度追蹤
- 批次處理進度顯示
- 記憶體使用監控
- 錯誤處理和恢復

## 📈 效能優化

### 1. 索引策略
- **複合索引**：支援多維度查詢
- **版本雜湊索引**：加速版本查找
- **時間戳索引**：支援時間範圍查詢

### 2. 查詢優化
- 自動使用活躍版本
- 支援指定版本查詢
- 統計資料預計算

### 3. 歸檔策略
- 自動歸檔舊資料
- 可設定保留期限
- 支援手動歸檔

## 🚀 擴展性設計

### 1. 新演算法支援
```python
# 只需實現 BaseAlgorithm 介面
class NewAlgorithm(BaseAlgorithm):
    def calculate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        # 實現演算法邏輯
        pass
    
    def get_swing_points(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        # 實現波段點提取
        pass
```

### 2. 新資料類型支援
- 新增資料表只需遵循版本控制模式
- 支援自定義版本策略
- 可擴展歸檔機制

## 📋 配置管理

### 1. 版本控制配置 (`config/version_control.py`)
```python
# 預定義演算法版本
ZIGZAG_VERSIONS = {
    "zigzag_v1_deviation_5_depth_12": {
        "description": "ZigZag演算法 v1 - 標準參數",
        "parameters": {"deviation": 5.0, "depth": 12},
        "is_active": True
    },
    "zigzag_v1_deviation_3_depth_8": {
        "description": "ZigZag演算法 v1 - 敏感參數",
        "parameters": {"deviation": 3.0, "depth": 8},
        "is_active": False
    }
}
```

### 2. 歸檔策略配置
```python
ARCHIVE_RETENTION_DAYS = {
    "swing_data": 365 * 2,     # 波段資料保留2年
    "statistics": 365 * 5,     # 統計資料保留5年
    "candlestick": 365 * 1     # 蠟燭圖資料保留1年
}
```

## 🧪 測試驗證

### 1. 測試腳本
```bash
# 執行版本控制架構測試
python test_version_control.py
```

### 2. 測試覆蓋範圍
- ✅ 基本版本控制功能
- ✅ 資料版本控制
- ✅ 帶版本控制的波段處理
- ✅ 版本比較功能
- ✅ 歸檔功能

## 📚 文檔結構

### 1. 架構文檔
- `docs/database_architecture.md` - 詳細架構設計
- `README_VERSION_CONTROL.md` - 本總結文檔

### 2. 配置檔案
- `config/version_control.py` - 版本控制配置
- `config/settings.py` - 基本設定

### 3. 核心程式碼
- `src/database/connection.py` - 資料庫連接和版本控制
- `src/data_processing/swing_processor.py` - 波段處理器
- `src/algorithms/` - 演算法實作

## 🎉 主要優勢

### 1. 資料完整性
- 不覆寫舊資料，保留完整歷史
- 版本雜湊確保資料一致性
- 支援資料驗證和檢查

### 2. 可追溯性
- 完整的版本歷史記錄
- 支援 A/B 測試和比較
- 自動歸檔和清理

### 3. 可擴展性
- 模組化設計，易於擴展
- 支援新演算法和資料類型
- 可擴展為分散式架構

### 4. 效能優化
- 分批處理避免記憶體問題
- 索引優化支援高效查詢
- 統計資料預計算

## 🔮 未來擴展

### 1. 分散式支援
- 多資料庫實例
- 讀寫分離
- 負載平衡

### 2. 進階分析
- 版本效能分析
- 自動參數優化
- 機器學習整合

### 3. 監控與告警
- 資料品質監控
- 效能指標追蹤
- 異常檢測和告警

這個新的架構設計完全滿足了您的需求，提供了強大的版本控制、歷史追溯和分批處理能力，同時保持了良好的擴展性和效能。 
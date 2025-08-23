from fastapi import FastAPI, HTTPException, Request
import logging
from fastapi.responses import HTMLResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta
import json
from typing import Optional, List, Dict
import uvicorn
from pathlib import Path
from starlette.responses import HTMLResponse, RedirectResponse
from fastapi import APIRouter
from fastapi.responses import RedirectResponse

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    DUCKDB_PATH, TIMEFRAMES, FRONTEND_HOST, FRONTEND_PORT,
    MEASUREMENT_COLORS, CANDLESTICK_COLORS
)
from src.database.connection import DuckDBConnection
from src.utils.timeframe import normalize_timeframe

# 創建FastAPI應用
app = FastAPI(title="市場波段規律分析系統", version="1.0.0")

# API路由
read_api = APIRouter()

@read_api.get("/api/candles")
async def api_candles(symbol: str, timeframe: str, limit: Optional[int] = 500):
    tf = normalize_timeframe(timeframe)
    if tf not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail="不支援的時間週期")
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            df = db.get_candlestick_data(symbol, tf, limit=limit)
            candles: List[Dict] = []
            if not df.empty:
                # If limit is very large (>= 999999), return all data; otherwise apply tail limit
                if limit and limit >= 999999:
                    use_df = df  # Use all data
                else:
                    use_df = df.tail(limit or 500)
                
                for ts, row in use_df.iterrows():
                    ms = int(ts.timestamp() * 1000) if hasattr(ts, "timestamp") else None
                    candles.append({
                        "ts": ms,
                        "open": float(row.get("open")),
                        "high": float(row.get("high")),
                        "low": float(row.get("low")),
                        "close": float(row.get("close")),
                        "volume": int(row.get("volume")) if pd.notna(row.get("volume")) else 0,
                    })
            return {"candles": candles}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@read_api.get("/api/swings")
async def api_swings(
    symbol: str,
    timeframe: str,
    algo: str = "zigzag",
    limit: Optional[int] = 500,
    order: str = "asc",
):
    tf = normalize_timeframe(timeframe)
    if tf not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail="不支援的時間週期")
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order 必須為 asc 或 desc")
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            # latest batch by created_at
            q = f"""
            WITH latest AS (
                SELECT MAX(created_at) AS created_at
                FROM swing_data
                WHERE symbol = ? AND timeframe = ? AND algorithm_name = ?
            )
            SELECT timestamp, zigzag_price, created_at, version_hash
            FROM swing_data
            WHERE symbol = ? AND timeframe = ? AND algorithm_name = ?
              AND created_at = (SELECT created_at FROM latest)
            ORDER BY timestamp {('DESC' if order == 'desc' else 'ASC')}
            """
            params = [symbol, tf, algo, symbol, tf, algo]
            df = db.conn.execute(q, params).fetchdf()
            if df.empty:
                return {"legs": []}

            # gather pivots
            pivots: List[Dict] = []
            batch_created_at = None
            version_hash = None
            for _, row in df.iterrows():
                price = row["zigzag_price"]
                if pd.notna(price):
                    ts = row["timestamp"]
                    ms = int(ts.timestamp() * 1000) if hasattr(ts, "timestamp") else None
                    pivots.append({"t": ms, "p": float(price)})
                if batch_created_at is None and pd.notna(row.get("created_at")):
                    cat = row["created_at"]
                    batch_created_at = int(cat.timestamp() * 1000) if hasattr(cat, "timestamp") else None
                if version_hash is None and row.get("version_hash") is not None:
                    version_hash = str(row["version_hash"]) 

            # build legs
            legs: List[Dict] = []
            for i in range(len(pivots) - 1):
                legs.append({
                    "t1": pivots[i]["t"],
                    "p1": pivots[i]["p"],
                    "t2": pivots[i + 1]["t"],
                    "p2": pivots[i + 1]["p"],
                })

            # apply limit after order - if limit is very large, don't apply limit
            if isinstance(limit, int) and limit > 0 and limit < 999999:
                legs = legs[:limit] if order == "asc" else legs[:limit]

            return {
                "legs": legs,
                "created_at": batch_created_at,
                "version_hash": version_hash,
                "order": order,
                "count": len(legs),
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CORS（同域下無害，跨域時可使用）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 靜態文件和模板
static_dir = Path(__file__).parent / "static"
templates_dir = Path(__file__).parent / "templates"
algorithms_static_dir = static_dir / "algorithms"

# 創建目錄（如果不存在）
static_dir.mkdir(exist_ok=True)
templates_dir.mkdir(exist_ok=True)
algorithms_static_dir.mkdir(exist_ok=True)

# 模板引擎
templates = Jinja2Templates(directory=str(templates_dir))

# 先掛新的進度通道路由（共用 InMemoryProgressStore）
try:
    from src.backend.swing_progress_router import router as swing_progress_router
    app.include_router(swing_progress_router)
except Exception as e:
    logging.getLogger(__name__).exception("Failed to include swing progress router: %s", e)

# 後掛其餘 legacy API（已移除重複路徑）
try:
    from src.frontend.routes.swing import router as swing_router
    app.include_router(swing_router)
except Exception as e:
    logging.getLogger(__name__).exception("Failed to include swing routes: %s", e)

# 在路由之後掛載靜態資源，避免覆蓋 API/WS
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.include_router(read_api)

# 新的波段測試頁面 - 使用新的響應式模板架構
@app.get("/swing/", include_in_schema=False)
async def swing_page(request: Request):
    """波段生成器頁面 - 使用新的響應式RWD模板"""
    import time
    cache_buster = int(time.time())  # 使用當前時間戳作為版本號
    return templates.TemplateResponse("swing_new.html", {
        "request": request,
        "cache_buster": cache_buster
    })

# 添加測試頁面路由
@app.get("/test/", include_in_schema=False)
async def test_page(request: Request):
    """測試頁面 - 基本圖表功能測試"""
    return templates.TemplateResponse("test.html", {"request": request})

# 連接測試頁面
@app.get("/connection-test/", include_in_schema=False)
async def connection_test_page(request: Request):
    """連接測試頁面"""
    return templates.TemplateResponse("connection_test.html", {"request": request})

# 提供演算法清單檔案（保持舊前端請求路徑 /algorithms/index.json 可用，避免再掛一個 StaticFiles 在根目錄）
@app.get("/algorithms/index.json", include_in_schema=False)
async def algorithms_index():
    target = algorithms_static_dir / "index.json"
    return FileResponse(str(target))

# 全局變數
current_symbol = "XAUUSD"  # 預設為 XAUUSD

class CandlestickData:
    """蠟燭圖資料類別"""
    
    @staticmethod
    def get_data(symbol: str, timeframe: str, start_date: Optional[str] = None, 
                 end_date: Optional[str] = None, limit: Optional[int] = None) -> Dict:
        """獲取蠟燭圖資料"""
        try:
            with DuckDBConnection(str(DUCKDB_PATH)) as db:
                # 根據時間框架設定不同的預設限制
                if limit is None:
                    if timeframe in ['M1', 'M5']:
                        limit = 500  # 短時間框架限制更多資料
                    elif timeframe in ['M15', 'M30']:
                        limit = 300
                    elif timeframe in ['H1', 'H4']:
                        limit = 200
                    else:  # D1
                        limit = 100  # 日線限制較少資料
                
                df = db.get_candlestick_data(symbol, timeframe, start_date, end_date, limit)
                
                if df.empty:
                    return {"data": [], "message": "暫無資料"}
                
                # 轉換為前端所需格式
                data = []
                for timestamp, row in df.iterrows():
                    data.append({
                        "timestamp": timestamp.isoformat(),
                        "open": float(row['open']),
                        "high": float(row['high']),
                        "low": float(row['low']),
                        "close": float(row['close']),
                        "volume": int(row['volume'])
                    })
                
                return {
                    "data": data,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "count": len(data),
                    "limit": limit
                }
                
        except Exception as e:
            return {"error": str(e), "data": []}

    @staticmethod
    def get_smart_data(symbol: str, timeframe: str, days: int = 30) -> Dict:
        """智能獲取資料 - 根據天數動態調整資料量"""
        try:
            with DuckDBConnection(str(DUCKDB_PATH)) as db:
                # 計算開始日期
                from datetime import datetime, timedelta
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                # 根據天數和時間框架計算合適的限制
                if timeframe == 'D1':
                    limit = min(days + 10, 200)  # 日線最多200筆
                elif timeframe == 'H4':
                    limit = min(days * 6 + 20, 500)  # 4小時線
                elif timeframe == 'H1':
                    limit = min(days * 24 + 50, 1000)  # 1小時線
                elif timeframe == 'M30':
                    limit = min(days * 48 + 100, 1500)  # 30分鐘線
                elif timeframe == 'M15':
                    limit = min(days * 96 + 200, 2000)  # 15分鐘線
                elif timeframe == 'M5':
                    limit = min(days * 288 + 500, 3000)  # 5分鐘線
                else:  # M1
                    limit = min(days * 1440 + 1000, 5000)  # 1分鐘線
                
                df = db.get_candlestick_data(
                    symbol, timeframe, 
                    start_date.strftime('%Y-%m-%d'), 
                    end_date.strftime('%Y-%m-%d'), 
                    limit
                )
                
                if df.empty:
                    return {"data": [], "message": "暫無資料"}
                
                # 轉換為前端所需格式
                data = []
                for timestamp, row in df.iterrows():
                    data.append({
                        "timestamp": timestamp.isoformat(),
                        "open": float(row['open']),
                        "high": float(row['high']),
                        "low": float(row['low']),
                        "close": float(row['close']),
                        "volume": int(row['volume'])
                    })
                
                return {
                    "data": data,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "count": len(data),
                    "limit": limit,
                    "days": days
                }
                
        except Exception as e:
            return {"error": str(e), "data": []}

@app.get("/", include_in_schema=False)
async def index():
    """回前端首頁檔案"""
    return FileResponse(str(templates_dir / "index.html"))


@app.get("/favicon.ico")
async def favicon() -> Response:
    # Return a tiny transparent icon to suppress 404 noise
    # 1x1 transparent PNG (base64)
    data = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
        b"\x00\x00\x00\x0bIDAT\x08\x1dc\x00\x01\x00\x00\x05\x00\x01\x0d\n\x2d\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    return Response(content=data, media_type="image/png")



# Remove SPA fallback and healthz; keep only /, /swing/, /docs, and /api/*


# Remove debug endpoints and factory indirection

@app.get("/api/candlestick/{symbol}/{timeframe}")
async def get_candlestick_data(
    symbol: str, 
    timeframe: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None
):
    """獲取蠟燭圖資料API"""
    
    # 先正規化（接受 1h/4h/1d 與 H1/H4/D1 等別名）
    orig_tf = timeframe
    tf = normalize_timeframe(timeframe)

    # 驗證正規化後的 timeframe；錯誤訊息可使用原始輸入便於排查
    if tf not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail="不支援的時間週期")
    
    data = CandlestickData.get_data(symbol, tf, start_date, end_date, limit)
    # 若資料為空，回 404（不回傳假資料）
    if not data.get("data"):
        raise HTTPException(status_code=404, detail=f"不支援或無資料（symbol={symbol}, timeframe={orig_tf}）")
    
    if "error" in data:
        raise HTTPException(status_code=500, detail=data["error"])
    
    return data

@app.get("/api/candlestick/{symbol}/{timeframe}/smart")
async def get_smart_candlestick_data(
    symbol: str, 
    timeframe: str,
    days: int = 30
):
    """智能獲取蠟燭圖資料API - 根據天數動態調整資料量"""
    
    if timeframe not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail="不支援的時間週期")
    
    if days < 1 or days > 365:
        raise HTTPException(status_code=400, detail="天數必須在 1-365 之間")
    
    data = CandlestickData.get_smart_data(symbol, timeframe, days)
    
    if "error" in data:
        raise HTTPException(status_code=500, detail=data["error"])
    
    return data

@app.get("/api/timeframes/{symbol}")
async def get_available_timeframes(symbol: str):
    """獲取可用時間週期"""
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            timeframes = db.get_available_timeframes(symbol)
            return {"timeframes": timeframes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/config")
async def get_config():
    """獲取前端配置"""
    return {
        "timeframes": TIMEFRAMES,
        "colors": {
            "measurement": MEASUREMENT_COLORS,
            "candlestick": CANDLESTICK_COLORS
        },
        "symbol": current_symbol
    }

@app.post("/api/measurement")
async def calculate_measurement(data: Dict):
    """計算測量結果"""
    try:
        point1 = data["point1"]
        point2 = data["point2"]
        
        # 計算價格差異
        price_diff = abs(point2["y"] - point1["y"])
        
        # 計算時間差異
        time1 = datetime.fromisoformat(point1["x"].replace("Z", "+00:00"))
        time2 = datetime.fromisoformat(point2["x"].replace("Z", "+00:00"))
        time_diff = abs(time2 - time1)
        
        return {
            "price_diff": price_diff,
            "time_diff": str(time_diff),
            "point1": point1,
            "point2": point2
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/swing/{symbol}/{timeframe}")
async def get_swing_data(
    symbol: str, 
    timeframe: str,
    algorithm: str = "zigzag",
    limit: Optional[int] = None
):
    """獲取波段資料"""
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            # 設定預設限制
            if limit is None:
                if timeframe in ['M1', 'M5']:
                    limit = 50
                elif timeframe in ['M15', 'M30']:
                    limit = 30
                elif timeframe in ['H1', 'H4']:
                    limit = 20
                else:  # D1
                    limit = 15
            
            # 查詢波段資料
            query = """
            SELECT id, symbol, timeframe, algorithm_name, version_hash,
                   timestamp, zigzag_price, zigzag_type, zigzag_strength,
                   zigzag_swing, swing_high, swing_low, swing_range,
                   swing_duration, swing_direction, created_at
            FROM swing_data 
            WHERE symbol = ? 
              AND timeframe = ?
              AND algorithm_name = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
            params = [symbol, timeframe, algorithm, limit]
            df = db.conn.execute(query, params).fetchdf()
            
            if df.empty:
                return {"data": [], "message": "暫無波段資料"}
            
            # 轉換為前端所需格式
            data = []
            for _, row in df.iterrows():
                data.append({
                    "id": int(row['id']),
                    "symbol": row['symbol'],
                    "timeframe": row['timeframe'],
                    "algorithm_name": row['algorithm_name'],
                    "version_hash": row['version_hash'],
                    "timestamp": row['timestamp'].isoformat() if pd.notna(row['timestamp']) else None,
                    "zigzag_price": float(row['zigzag_price']) if pd.notna(row['zigzag_price']) else None,
                    "zigzag_type": row['zigzag_type'],
                    "zigzag_strength": float(row['zigzag_strength']) if pd.notna(row['zigzag_strength']) else None,
                    "zigzag_swing": int(row['zigzag_swing']) if pd.notna(row['zigzag_swing']) else None,
                    "swing_high": float(row['swing_high']) if pd.notna(row['swing_high']) else None,
                    "swing_low": float(row['swing_low']) if pd.notna(row['swing_low']) else None,
                    "swing_range": float(row['swing_range']) if pd.notna(row['swing_range']) else None,
                    "swing_duration": int(row['swing_duration']) if pd.notna(row['swing_duration']) else None,
                    "swing_direction": row['swing_direction'],
                    "created_at": row['created_at'].isoformat() if pd.notna(row['created_at']) else None
                })
            
            return {
                "data": data,
                "symbol": symbol,
                "timeframe": timeframe,
                "algorithm": algorithm,
                "count": len(data),
                "limit": limit
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/swing/{symbol}/{timeframe}/statistics")
async def get_swing_statistics(
    symbol: str, 
    timeframe: str,
    algorithm: str = "zigzag"
):
    """獲取波段統計資訊"""
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            # 查詢統計資訊
            query = """
            SELECT
                COUNT(*) as total_swings,
                AVG(zigzag_strength) as avg_strength,
                MAX(zigzag_strength) as max_strength,
                MIN(zigzag_strength) as min_strength,
                AVG(swing_range) as avg_range,
                MAX(swing_range) as max_range,
                MIN(swing_range) as min_range,
                AVG(swing_duration) as avg_duration,
                MAX(swing_duration) as max_duration,
                MIN(swing_duration) as min_duration,
                COUNT(CASE WHEN zigzag_type = 'high' THEN 1 END) as high_points,
                COUNT(CASE WHEN zigzag_type = 'low' THEN 1 END) as low_points
            FROM swing_data 
            WHERE symbol = ? 
              AND timeframe = ?
              AND algorithm_name = ?
            """
            params = [symbol, timeframe, algorithm]
            result = db.conn.execute(query, params).fetchone()
            
            if not result:
                return {"message": "暫無統計資料"}
            
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "algorithm": algorithm,
                "statistics": {
                    "total_swings": int(result[0]) if result[0] else 0,
                    "avg_strength": float(result[1]) if result[1] else 0,
                    "max_strength": float(result[2]) if result[2] else 0,
                    "min_strength": float(result[3]) if result[3] else 0,
                    "avg_range": float(result[4]) if result[4] else 0,
                    "max_range": float(result[5]) if result[5] else 0,
                    "min_range": float(result[6]) if result[6] else 0,
                    "avg_duration": float(result[7]) if result[7] else 0,
                    "max_duration": int(result[8]) if result[8] else 0,
                    "min_duration": int(result[9]) if result[9] else 0,
                    "high_points": int(result[10]) if result[10] else 0,
                    "low_points": int(result[11]) if result[11] else 0
                }
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def create_static_files():
    """創建靜態文件"""
    
    # 創建CSS文件
    css_content = """
    body {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        margin: 0;
        padding: 0;
        background-color: #f8f9fa;
    }

    .container {
        max-width: 1200px;
        margin: 0 auto;
        padding: 20px;
    }

    h1 {
        text-align: center;
        color: #333;
        margin-bottom: 30px;
    }

    .controls {
        display: flex;
        justify-content: center;
        gap: 10px;
        margin-bottom: 20px;
        flex-wrap: wrap;
    }

    .btn {
        padding: 10px 20px;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        font-size: 14px;
        transition: all 0.3s ease;
    }

    .btn-primary {
        background-color: #007bff;
        color: white;
    }

    .btn-primary:hover {
        background-color: #0056b3;
    }

    .btn-primary.active {
        background-color: #0056b3;
        box-shadow: 0 0 0 2px rgba(0, 123, 255, 0.5);
    }

    .btn-info {
        background-color: #17a2b8;
        color: white;
    }

    .btn-info:hover {
        background-color: #138496;
    }

    .btn-warning {
        background-color: #ffc107;
        color: #212529;
    }

    .btn-warning:hover {
        background-color: #e0a800;
    }

    .btn-success {
        background-color: #28a745;
        color: white;
    }

    .btn-success:hover {
        background-color: #218838;
    }

    .chart-container {
        position: relative;
        height: 600px;
        background: white;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }

    .info-panel {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }

    .info-row {
        display: flex;
        justify-content: space-between;
        gap: 20px;
        flex-wrap: wrap;
    }

    .info-item {
        flex: 1;
        min-width: 200px;
    }

    .info-item h6 {
        color: #007bff;
        margin-bottom: 10px;
    }

    .info-item p {
        margin: 5px 0;
        color: #666;
    }

    .measurement-result {
        background: #e9ecef;
        border: 1px solid #dee2e6;
        border-radius: 5px;
        padding: 15px;
        margin-top: 10px;
    }

    .measurement-result h5 {
        color: #17a2b8;
        margin-bottom: 10px;
    }

    .loading {
        text-align: center;
        padding: 50px;
        color: #666;
    }

    .error {
        background: #f8d7da;
        color: #721c24;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }

    @media (max-width: 768px) {
        .controls {
            flex-direction: column;
            align-items: center;
        }
        
        .info-row {
            flex-direction: column;
        }
        
        .btn {
            width: 100%;
            max-width: 200px;
        }
    }
    """
    
    with open(static_dir / "style.css", "w", encoding="utf-8") as f:
        f.write(css_content)
    
    # 創建JavaScript文件
    js_content = """
    class CandlestickChart {
        constructor() {
            this.chart = null;
            this.currentTimeframe = 'D1';
            this.symbol = 'EXUSA30IDXUSD';
            this.measurementMode = false;
            this.measurementPoints = [];
            this.config = {};
            
            this.init();
        }
        
        async init() {
            await this.loadConfig();
            await this.loadChart();
            this.setupEventListeners();
        }
        
        async loadConfig() {
            try {
                const response = await fetch('/api/config');
                this.config = await response.json();
                this.symbol = this.config.symbol;
            } catch (error) {
                console.error('載入配置失敗:', error);
            }
        }
        
        async loadChart(timeframe = 'D1') {
            this.currentTimeframe = timeframe;
            
            try {
                const response = await fetch(`/api/candlestick/${this.symbol}/${timeframe}`);
                const data = await response.json();
                
                if (data.error) {
                    throw new Error(data.error);
                }
                
                this.renderChart(data.data);
                this.updateInfo(data);
                
            } catch (error) {
                console.error('載入圖表失敗:', error);
                this.showError('載入圖表資料失敗: ' + error.message);
            }
        }
        
        renderChart(data) {
            if (!data || data.length === 0) {
                this.showError('暫無資料');
                return;
            }
            
            // 準備資料
            const timestamps = data.map(d => d.timestamp);
            const ohlcData = data.map(d => [
                new Date(d.timestamp).getTime(),
                d.open,
                d.high,
                d.low,
                d.close
            ]);
            
            const volumeData = data.map(d => [
                new Date(d.timestamp).getTime(),
                d.volume
            ]);
            
            // 圖表配置
            const options = {
                chart: {
                    type: 'candlestick',
                    height: 600,
                    toolbar: {
                        show: true
                    },
                    events: {
                        click: (event, chartContext, config) => {
                            if (this.measurementMode) {
                                this.addMeasurementPoint(event, config);
                            }
                        }
                    }
                },
                title: {
                    text: `${this.symbol} - ${this.currentTimeframe} 蠟燭圖`,
                    align: 'center'
                },
                xaxis: {
                    type: 'datetime',
                    labels: {
                        datetimeFormatter: {
                            year: 'yyyy',
                            month: 'MMM \'yy',
                            day: 'dd MMM',
                            hour: 'HH:mm'
                        }
                    }
                },
                yaxis: [
                    {
                        title: {
                            text: '價格'
                        },
                        tooltip: {
                            enabled: true
                        }
                    },
                    {
                        opposite: true,
                        title: {
                            text: '成交量'
                        },
                        max: Math.max(...volumeData.map(d => d[1])) * 4
                    }
                ],
                tooltip: {
                    shared: true,
                    custom: function({seriesIndex, dataPointIndex, w}) {
                        const data = w.globals.initialSeries[seriesIndex].data[dataPointIndex];
                        if (seriesIndex === 0) {
                            return `<div class="tooltip">
                                <strong>${new Date(data.x).toLocaleString()}</strong><br>
                                開: ${data.y[0]}<br>
                                高: ${data.y[1]}<br>
                                低: ${data.y[2]}<br>
                                收: ${data.y[3]}
                            </div>`;
                        }
                        return '';
                    }
                }
            };
            
            // 創建圖表
            if (this.chart) {
                this.chart.destroy();
            }
            
            this.chart = new ApexCharts(document.querySelector("#chart"), {
                ...options,
                series: [
                    {
                        name: '蠟燭圖',
                        type: 'candlestick',
                        data: ohlcData
                    },
                    {
                        name: '成交量',
                        type: 'column',
                        data: volumeData,
                        yAxisIndex: 1
                    }
                ]
            });
            
            this.chart.render();
        }
        
        setupEventListeners() {
            // 時間週期按鈕
            document.querySelectorAll('.timeframe-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    document.querySelectorAll('.timeframe-btn').forEach(b => b.classList.remove('active'));
                    e.target.classList.add('active');
                    this.loadChart(e.target.dataset.timeframe);
                });
            });
            
            // 測量工具按鈕
            document.getElementById('measureBtn').addEventListener('click', () => {
                this.toggleMeasurementMode();
            });
            
            // 重置按鈕
            document.getElementById('resetBtn').addEventListener('click', () => {
                this.resetMeasurement();
            });
            
            // 縮放按鈕
            document.getElementById('zoomInBtn').addEventListener('click', () => {
                if (this.chart) {
                    this.chart.zoomIn();
                }
            });
            
            document.getElementById('zoomOutBtn').addEventListener('click', () => {
                if (this.chart) {
                    this.chart.zoomOut();
                }
            });
        }
        
        toggleMeasurementMode() {
            this.measurementMode = !this.measurementMode;
            const btn = document.getElementById('measureBtn');
            
            if (this.measurementMode) {
                btn.textContent = '取消測量';
                btn.classList.add('active');
                this.measurementPoints = [];
                this.updateMeasurementResult('測量模式已開啟，請點擊圖表上的兩個點');
            } else {
                btn.textContent = '測量工具';
                btn.classList.remove('active');
                this.resetMeasurement();
            }
        }
        
        addMeasurementPoint(event, config) {
            if (this.measurementPoints.length >= 2) {
                return;
            }
            
            const point = {
                x: new Date(config.w.globals.seriesX[0][config.dataPointIndex]).toISOString(),
                y: config.w.globals.seriesY[0][config.dataPointIndex]
            };
            
            this.measurementPoints.push(point);
            
            if (this.measurementPoints.length === 2) {
                this.calculateMeasurement();
            } else {
                this.updateMeasurementResult(`已選擇第 ${this.measurementPoints.length} 個點`);
            }
        }
        
        async calculateMeasurement() {
            try {
                const response = await fetch('/api/measurement', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        point1: this.measurementPoints[0],
                        point2: this.measurementPoints[1]
                    })
                });
                
                const result = await response.json();
                this.displayMeasurementResult(result);
                
            } catch (error) {
                console.error('計算測量失敗:', error);
                this.updateMeasurementResult('計算測量失敗');
            }
        }
        
        displayMeasurementResult(result) {
            const html = `
                <div class="measurement-result">
                    <h5>測量結果:</h5>
                    <p><strong>價格差異:</strong> ${result.price_diff.toFixed(5)}</p>
                    <p><strong>時間差異:</strong> ${result.time_diff}</p>
                    <p><strong>點1:</strong> ${new Date(result.point1.x).toLocaleString()} @ ${result.point1.y.toFixed(5)}</p>
                    <p><strong>點2:</strong> ${new Date(result.point2.x).toLocaleString()} @ ${result.point2.y.toFixed(5)}</p>
                </div>
            `;
            
            document.getElementById('measurementResult').innerHTML = html;
            this.measurementMode = false;
            document.getElementById('measureBtn').textContent = '測量工具';
            document.getElementById('measureBtn').classList.remove('active');
        }
        
        resetMeasurement() {
            this.measurementPoints = [];
            this.measurementMode = false;
            document.getElementById('measureBtn').textContent = '測量工具';
            document.getElementById('measureBtn').classList.remove('active');
            document.getElementById('measurementResult').innerHTML = '';
        }
        
        updateInfo(data) {
            if (!data.data || data.data.length === 0) {
                return;
            }
            
            const latest = data.data[data.data.length - 1];
            
            document.getElementById('currentSymbol').textContent = data.symbol;
            document.getElementById('currentTimeframe').textContent = data.timeframe;
            document.getElementById('dataCount').textContent = data.count;
            
            document.getElementById('latestOpen').textContent = latest.open.toFixed(5);
            document.getElementById('latestHigh').textContent = latest.high.toFixed(5);
            document.getElementById('latestLow').textContent = latest.low.toFixed(5);
            document.getElementById('latestClose').textContent = latest.close.toFixed(5);
            
            document.getElementById('latestVolume').textContent = latest.volume.toLocaleString();
            document.getElementById('latestTime').textContent = new Date(latest.timestamp).toLocaleString();
        }
        
        updateMeasurementResult(message) {
            document.getElementById('measurementResult').innerHTML = `<p>${message}</p>`;
        }
        
        showError(message) {
            const errorDiv = document.createElement('div');
            errorDiv.className = 'error';
            errorDiv.textContent = message;
            
            const container = document.querySelector('.container');
            container.insertBefore(errorDiv, container.firstChild);
            
            setTimeout(() => {
                errorDiv.remove();
            }, 5000);
        }
    }
    
    // 初始化應用
    document.addEventListener('DOMContentLoaded', () => {
        new CandlestickChart();
    });
    """
    
    with open(static_dir / "app.js", "w", encoding="utf-8") as f:
        f.write(js_content)

def create_template():
    """創建HTML模板"""
    html_content = """
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>市場波段規律分析系統</title>
        <link rel="stylesheet" href="/static/style.css">
        <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    </head>
    <body>
        <div class="container">
            <h1>市場波段規律分析系統</h1>
            
            <!-- 控制按鈕 -->
            <div class="controls">
                <!-- 時間週期按鈕 -->
                <button class="btn btn-primary timeframe-btn" data-timeframe="M1">M1</button>
                <button class="btn btn-primary timeframe-btn" data-timeframe="M5">M5</button>
                <button class="btn btn-primary timeframe-btn" data-timeframe="M15">M15</button>
                <button class="btn btn-primary timeframe-btn" data-timeframe="M30">M30</button>
                <button class="btn btn-primary timeframe-btn" data-timeframe="H1">H1</button>
                <button class="btn btn-primary timeframe-btn" data-timeframe="H4">H4</button>
                <button class="btn btn-primary timeframe-btn active" data-timeframe="D1">D1</button>
                
                <!-- 工具按鈕 -->
                <button class="btn btn-info" id="measureBtn">測量工具</button>
                <button class="btn btn-warning" id="resetBtn">重置</button>
                <button class="btn btn-success" id="zoomInBtn">放大</button>
                <button class="btn btn-success" id="zoomOutBtn">縮小</button>
            </div>
            
            <!-- 圖表區域 -->
            <div class="chart-container">
                <div id="chart"></div>
            </div>
            
            <!-- 資訊面板 -->
            <div class="info-panel">
                <div class="info-row">
                    <div class="info-item">
                        <h6>當前資訊</h6>
                        <p>交易品種: <span id="currentSymbol">-</span></p>
                        <p>時間週期: <span id="currentTimeframe">-</span></p>
                        <p>資料筆數: <span id="dataCount">-</span></p>
                    </div>
                    
                    <div class="info-item">
                        <h6>最新價格</h6>
                        <p>開盤: <span id="latestOpen">-</span></p>
                        <p>最高: <span id="latestHigh">-</span></p>
                        <p>最低: <span id="latestLow">-</span></p>
                        <p>收盤: <span id="latestClose">-</span></p>
                    </div>
                    
                    <div class="info-item">
                        <h6>成交量</h6>
                        <p>成交量: <span id="latestVolume">-</span></p>
                        <p>時間: <span id="latestTime">-</span></p>
                    </div>
                </div>
            </div>
            
            <!-- 測量結果 -->
            <div id="measurementResult"></div>
        </div>
        
        <script src="/static/app.js"></script>
    </body>
    </html>
    """
    
    with open(templates_dir / "index.html", "w", encoding="utf-8") as f:
        f.write(html_content)

# 創建靜態文件和模板
# create_static_files()  # 已手動創建，註釋掉避免覆蓋
# create_template()      # 已手動創建，註釋掉避免覆蓋

if __name__ == "__main__":
    print("🚀 啟動市場波段規律分析系統...")
    print(f"📊 系統將在 http://{FRONTEND_HOST}:{FRONTEND_PORT} 啟動")
    print("✨ 使用FastAPI + ApexCharts 提供高性能體驗")
    
    uvicorn.run(
        "src.frontend.app:app",
        host=FRONTEND_HOST,
        port=FRONTEND_PORT,
        reload=True,
        log_level="info"
    ) 
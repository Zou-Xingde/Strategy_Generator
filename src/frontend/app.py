from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta
import json
from typing import Optional, List, Dict
import uvicorn
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    DUCKDB_PATH, TIMEFRAMES, FRONTEND_HOST, FRONTEND_PORT,
    MEASUREMENT_COLORS, CANDLESTICK_COLORS
)
from src.database.connection import DuckDBConnection

# 創建FastAPI應用
app = FastAPI(title="市場波段規律分析系統", version="1.0.0")

# 靜態文件和模板
static_dir = Path(__file__).parent / "static"
templates_dir = Path(__file__).parent / "templates"

# 創建目錄（如果不存在）
static_dir.mkdir(exist_ok=True)
templates_dir.mkdir(exist_ok=True)

# 掛載靜態文件
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# 模板引擎
templates = Jinja2Templates(directory=str(templates_dir))

# 全局變數
current_symbol = "EXUSA30IDXUSD"

class CandlestickData:
    """蠟燭圖資料類別"""
    
    @staticmethod
    def get_data(symbol: str, timeframe: str, start_date: Optional[str] = None, 
                 end_date: Optional[str] = None) -> Dict:
        """獲取蠟燭圖資料"""
        try:
            with DuckDBConnection(str(DUCKDB_PATH)) as db:
                df = db.get_candlestick_data(symbol, timeframe, start_date, end_date)
                
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
                    "count": len(data)
                }
                
        except Exception as e:
            return {"error": str(e), "data": []}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """主頁面"""
    return templates.TemplateResponse("index.html", {"request": request})



@app.get("/api/candlestick/{symbol}/{timeframe}")
async def get_candlestick_data(
    symbol: str, 
    timeframe: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """獲取蠟燭圖資料API"""
    
    if timeframe not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail="不支援的時間週期")
    
    data = CandlestickData.get_data(symbol, timeframe, start_date, end_date)
    
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
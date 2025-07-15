/**
 * 專業級 TradingView 蠟燭圖交易系統
 * 使用 TradingView Lightweight Charts 提供頂級金融圖表體驗
 */

class ProfessionalTradingChart {
    constructor() {
        this.chart = null;
        this.candlestickSeries = null;
        this.volumeSeries = null;
        this.currentTimeframe = 'D1';
        this.symbol = 'EXUSA30IDXUSD';
        this.measurementMode = false;
        this.measurementPoints = [];
        this.config = {};
        this.container = document.getElementById('chart');
        
        this.init();
    }

    async init() {
        await this.loadConfig();
        this.createChart();
        await this.loadData();
        this.setupEventListeners();
        this.updateActiveTimeframe();
    }

    async loadConfig() {
        try {
            const response = await fetch('/api/config');
            if (response.ok) {
                this.config = await response.json();
            }
        } catch (error) {
            console.warn('載入配置失敗，使用預設配置:', error);
        }
    }

    createChart() {
        // 創建專業的 TradingView 圖表
        this.chart = LightweightCharts.createChart(this.container, {
            width: this.container.clientWidth,
            height: 600,
            layout: {
                background: {
                    type: 'solid',
                    color: '#1a1a2e'
                },
                textColor: '#e8e8e8',
                fontSize: 12,
                fontFamily: 'Segoe UI, Arial, sans-serif'
            },
            grid: {
                vertLines: {
                    color: 'rgba(42, 46, 57, 0.6)',
                    style: 1,
                    visible: true
                },
                horzLines: {
                    color: 'rgba(42, 46, 57, 0.6)',
                    style: 1,
                    visible: true
                }
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    color: '#00d4aa',
                    width: 1,
                    style: 3,
                    labelBackgroundColor: '#00d4aa'
                },
                horzLine: {
                    color: '#00d4aa',
                    width: 1,
                    style: 3,
                    labelBackgroundColor: '#00d4aa'
                }
            },
            rightPriceScale: {
                borderColor: 'rgba(42, 46, 57, 0.8)',
                textColor: '#e8e8e8',
                entireTextOnly: false,
                visible: true,
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.15
                }
            },
            timeScale: {
                borderColor: 'rgba(42, 46, 57, 0.8)',
                textColor: '#e8e8e8',
                timeVisible: true,
                secondsVisible: false,
                rightOffset: 5,
                barSpacing: 8,
                fixLeftEdge: false,
                lockVisibleTimeRangeOnResize: true,
                rightBarStaysOnScroll: true,
                borderVisible: true,
                visible: true
            }
        });

        // 創建蠟燭圖系列
        this.candlestickSeries = this.chart.addCandlestickSeries({
            upColor: '#00ff88',
            downColor: '#ff4757',
            borderDownColor: '#ff4757',
            borderUpColor: '#00ff88',
            wickDownColor: '#ff4757',
            wickUpColor: '#00ff88',
            borderVisible: true,
            wickVisible: true,
            priceLineVisible: true,
            lastValueVisible: true,
            priceFormat: {
                type: 'price',
                precision: 2,
                minMove: 0.01
            }
        });

        // 創建成交量系列
        this.volumeSeries = this.chart.addHistogramSeries({
            color: 'rgba(0, 212, 170, 0.4)',
            priceFormat: {
                type: 'volume'
            },
            priceScaleId: 'volume',
            scaleMargins: {
                top: 0.8,
                bottom: 0
            }
        });

        // 設置成交量軸
        this.chart.priceScale('volume').applyOptions({
            scaleMargins: {
                top: 0.8,
                bottom: 0
            }
        });

        // 響應式處理
        this.setupResizeObserver();
        
        // 添加圖表事件監聽
        this.setupChartEvents();
    }

    async loadData(timeframe = null) {
        if (timeframe) {
            this.currentTimeframe = timeframe;
        }

        try {
            this.showLoading();
            
            const response = await fetch(`/api/candlestick/${this.symbol}/${this.currentTimeframe}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }

            this.renderData(data.data);
            this.updateInfoPanel(data);
            this.hideLoading();

        } catch (error) {
            console.error('載入數據失敗:', error);
            this.showError('載入圖表數據失敗: ' + error.message);
            this.hideLoading();
        }
    }

    renderData(data) {
        if (!data || data.length === 0) {
            this.showError('沒有可用的數據');
            return;
        }

        // 轉換數據格式為 TradingView 格式
        const candlestickData = data.map(item => ({
            time: new Date(item.timestamp).getTime() / 1000, // 轉換為秒級時間戳
            open: parseFloat(item.open),
            high: parseFloat(item.high),
            low: parseFloat(item.low),
            close: parseFloat(item.close)
        }));

        const volumeData = data.map(item => ({
            time: new Date(item.timestamp).getTime() / 1000,
            value: parseFloat(item.volume || 0),
            color: parseFloat(item.close) >= parseFloat(item.open) 
                ? 'rgba(0, 255, 136, 0.6)' 
                : 'rgba(255, 71, 87, 0.6)'
        }));

        // 設置數據
        this.candlestickSeries.setData(candlestickData);
        this.volumeSeries.setData(volumeData);

        // 自適應視圖
        this.chart.timeScale().fitContent();
    }

    updateInfoPanel(data) {
        if (!data.data || data.data.length === 0) return;

        const latest = data.data[data.data.length - 1];
        
        // 更新基本資訊
        document.getElementById('currentSymbol').textContent = this.symbol;
        document.getElementById('currentTimeframe').textContent = this.currentTimeframe;
        document.getElementById('dataCount').textContent = data.data.length.toLocaleString();

        // 更新價格資訊
        document.getElementById('latestOpen').textContent = parseFloat(latest.open).toFixed(2);
        document.getElementById('latestHigh').textContent = parseFloat(latest.high).toFixed(2);
        document.getElementById('latestLow').textContent = parseFloat(latest.low).toFixed(2);
        document.getElementById('latestClose').textContent = parseFloat(latest.close).toFixed(2);

        // 更新成交量和時間
        document.getElementById('latestVolume').textContent = (latest.volume || 0).toLocaleString();
        document.getElementById('latestTime').textContent = new Date(latest.timestamp).toLocaleString('zh-TW');

        // 價格顏色處理
        const isUp = parseFloat(latest.close) >= parseFloat(latest.open);
        const closeElement = document.getElementById('latestClose');
        closeElement.style.color = isUp ? '#00ff88' : '#ff4757';
    }

    setupEventListeners() {
        // 時間框架按鈕
        document.querySelectorAll('.btn-timeframe').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const timeframe = e.target.dataset.timeframe;
                this.switchTimeframe(timeframe);
            });
        });

        // 工具按鈕
        document.getElementById('measureBtn')?.addEventListener('click', () => {
            this.toggleMeasurementMode();
        });

        document.getElementById('fitBtn')?.addEventListener('click', () => {
            this.chart.timeScale().fitContent();
        });

        document.getElementById('resetBtn')?.addEventListener('click', () => {
            this.resetChart();
        });
    }

    setupChartEvents() {
        // 滑鼠移動事件 - 顯示十字線資訊
        this.chart.subscribeCrosshairMove((param) => {
            if (!param.time || !param.point) return;

            const data = param.seriesData.get(this.candlestickSeries);
            if (data) {
                this.updateCrosshairInfo(data, param.time);
            }
        });

        // 點擊事件 - 測量工具
        this.chart.subscribeClick((param) => {
            if (this.measurementMode && param.time && param.point) {
                this.addMeasurementPoint(param);
            }
        });
    }

    updateCrosshairInfo(data, time) {
        // 這裡可以添加十字線資訊顯示邏輯
        // 例如顯示當前點的詳細價格資訊
    }

    switchTimeframe(timeframe) {
        // 更新按鈕狀態
        document.querySelectorAll('.btn-timeframe').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.timeframe === timeframe);
        });

        // 載入新數據
        this.loadData(timeframe);
    }

    updateActiveTimeframe() {
        document.querySelectorAll('.btn-timeframe').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.timeframe === this.currentTimeframe);
        });
    }

    toggleMeasurementMode() {
        this.measurementMode = !this.measurementMode;
        const btn = document.getElementById('measureBtn');
        
        if (this.measurementMode) {
            btn.style.background = 'rgba(255, 123, 67, 0.3)';
            btn.textContent = '📏 測量中...';
            this.showMeasurementPanel();
        } else {
            btn.style.background = 'rgba(255, 123, 67, 0.1)';
            btn.textContent = '📏 測量工具';
            this.clearMeasurements();
        }
    }

    addMeasurementPoint(param) {
        this.measurementPoints.push({
            time: param.time,
            price: param.point.y,
            timestamp: new Date(param.time * 1000)
        });

        if (this.measurementPoints.length === 2) {
            this.calculateMeasurement();
            this.measurementPoints = [];
        }
    }

    calculateMeasurement() {
        if (this.measurementPoints.length !== 2) return;

        const [point1, point2] = this.measurementPoints;
        const priceDiff = Math.abs(point2.price - point1.price);
        const timeDiff = Math.abs(point2.time - point1.time);
        
        const result = `
            <div class="measurement-result">
                <h6>📏 測量結果</h6>
                <p><strong>價格差異:</strong> ${priceDiff.toFixed(2)}</p>
                <p><strong>時間差異:</strong> ${this.formatTimeDiff(timeDiff)}</p>
                <p><strong>起點:</strong> ${point1.timestamp.toLocaleString('zh-TW')}</p>
                <p><strong>終點:</strong> ${point2.timestamp.toLocaleString('zh-TW')}</p>
            </div>
        `;

        document.getElementById('measurementResult').innerHTML = result;
    }

    formatTimeDiff(seconds) {
        const days = Math.floor(seconds / 86400);
        const hours = Math.floor((seconds % 86400) / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        
        if (days > 0) return `${days}天 ${hours}小時`;
        if (hours > 0) return `${hours}小時 ${minutes}分鐘`;
        return `${minutes}分鐘`;
    }

    showMeasurementPanel() {
        const panel = document.getElementById('measurementPanel');
        if (panel) {
            panel.classList.remove('hidden');
        }
    }

    clearMeasurements() {
        this.measurementPoints = [];
        document.getElementById('measurementResult').innerHTML = '';
        const panel = document.getElementById('measurementPanel');
        if (panel) {
            panel.classList.add('hidden');
        }
    }

    resetChart() {
        this.chart.timeScale().fitContent();
        this.clearMeasurements();
        this.measurementMode = false;
        document.getElementById('measureBtn').textContent = '📏 測量工具';
    }

    setupResizeObserver() {
        const resizeObserver = new ResizeObserver(entries => {
            for (let entry of entries) {
                const { width, height } = entry.contentRect;
                this.chart.applyOptions({
                    width: width,
                    height: height || 600
                });
            }
        });

        resizeObserver.observe(this.container);
    }

    showLoading() {
        this.container.innerHTML = '<div class="loading">載入專業圖表中...</div>';
    }

    hideLoading() {
        // Loading 會在創建圖表時自動清除
    }

    showError(message) {
        this.container.innerHTML = `
            <div style="
                display: flex; 
                align-items: center; 
                justify-content: center; 
                height: 400px; 
                color: #ff4757;
                font-size: 1.2rem;
                text-align: center;
                flex-direction: column;
            ">
                <div style="font-size: 3rem; margin-bottom: 20px;">⚠️</div>
                <div>${message}</div>
                <button onclick="location.reload()" style="
                    margin-top: 20px;
                    padding: 10px 20px;
                    background: rgba(255, 123, 67, 0.2);
                    border: 1px solid #ff7b43;
                    border-radius: 8px;
                    color: #ff7b43;
                    cursor: pointer;
                ">重新載入</button>
            </div>
        `;
    }
}

// 初始化應用
document.addEventListener('DOMContentLoaded', () => {
    console.log('🚀 啟動專業 TradingView 圖表系統...');
    new ProfessionalTradingChart();
});

// 全局錯誤處理
window.addEventListener('error', (event) => {
    console.error('全局錯誤:', event.error);
});

window.addEventListener('unhandledrejection', (event) => {
    console.error('未處理的 Promise 拒絕:', event.reason);
}); 
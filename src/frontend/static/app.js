
    class CandlestickChart {
        constructor() {
            this.mainChart = null;
            this.indicatorChart = null;
            this.candlestickSeries = null;
            this.volumeSeries = null;
            this.rsiSeries = null;
            this.rsiUpperLine = null;
            this.rsiLowerLine = null;
            this.currentTimeframe = 'D1';
            this.symbol = 'EXUSA30IDXUSD';
            this.measurementMode = false;
            this.measurementPoints = [];
            this.config = {};
            
            console.log('CandlestickChart constructor called');
            this.init();
        }
        
        async init() {
            console.log('Initializing CandlestickChart...');
            try {
                await this.loadConfig();
                this.initChart();
                this.setupEventListeners();
                await this.loadChart();
                console.log('CandlestickChart initialization completed');
            } catch (error) {
                console.error('Failed to initialize CandlestickChart:', error);
            }
        }
        
        async loadConfig() {
            try {
                console.log('Loading config...');
                const response = await fetch('/api/config');
                this.config = await response.json();
                this.symbol = this.config.symbol || 'EXUSA30IDXUSD';
                console.log('Config loaded:', this.config);
            } catch (error) {
                console.error('載入配置失敗:', error);
            }
        }
        
        initChart() {
            console.log('Initializing charts...');
            const mainChartContainer = document.querySelector("#main-chart");
            const indicatorChartContainer = document.querySelector("#indicator-chart");
            
            if (!mainChartContainer) {
                console.error('Main chart container not found!');
                return;
            }
            
            if (!indicatorChartContainer) {
                console.error('Indicator chart container not found!');
                return;
            }
            
            console.log('Chart containers found, creating charts...');
            
            // 清除舊圖表
            if (this.mainChart) {
                try {
                    this.mainChart.remove();
                } catch (e) {
                    console.warn('Error removing main chart:', e);
                }
            }
            if (this.indicatorChart) {
                try {
                    this.indicatorChart.remove();
                } catch (e) {
                    console.warn('Error removing indicator chart:', e);
                }
            }
            
            // 檢查容器尺寸
            const mainWidth = mainChartContainer.offsetWidth || 800;
            const mainHeight = mainChartContainer.offsetHeight || 400;
            const indicatorWidth = indicatorChartContainer.offsetWidth || 800;
            const indicatorHeight = indicatorChartContainer.offsetHeight || 100;
            
            console.log('Main chart size:', mainWidth, 'x', mainHeight);
            console.log('Indicator chart size:', indicatorWidth, 'x', indicatorHeight);
            
            try {
                // 創建主圖表 - MT5風格配置
                this.mainChart = LightweightCharts.createChart(mainChartContainer, {
                    width: mainWidth,
                    height: mainHeight,
                    layout: {
                        backgroundColor: '#000000',
                        textColor: '#ffffff',
                    },
                    grid: {
                        vertLines: {
                            color: '#1a1a1a',
                        },
                        horzLines: {
                            color: '#1a1a1a',
                        },
                    },
                    crosshair: {
                        mode: LightweightCharts.CrosshairMode.Normal,
                    },
                    rightPriceScale: {
                        borderColor: '#333333',
                        textColor: '#cccccc',
                    },
                    timeScale: {
                        borderColor: '#333333',
                        textColor: '#cccccc',
                        timeVisible: true,
                        secondsVisible: false,
                    },
                });
                
                console.log('Main chart created successfully');
                
                // 創建指標圖表 - RSI
                this.indicatorChart = LightweightCharts.createChart(indicatorChartContainer, {
                    width: indicatorWidth,
                    height: indicatorHeight,
                    layout: {
                        backgroundColor: '#000000',
                        textColor: '#ffffff',
                    },
                    grid: {
                        vertLines: {
                            color: '#1a1a1a',
                        },
                        horzLines: {
                            color: '#1a1a1a',
                        },
                    },
                    crosshair: {
                        mode: LightweightCharts.CrosshairMode.Normal,
                    },
                    rightPriceScale: {
                        borderColor: '#333333',
                        textColor: '#cccccc',
                    },
                    timeScale: {
                        borderColor: '#333333',
                        textColor: '#cccccc',
                        timeVisible: true,
                        secondsVisible: false,
                    },
                });
                
                console.log('Indicator chart created successfully');
                
                // 主圖表：添加蠟燭圖系列
                this.candlestickSeries = this.mainChart.addCandlestickSeries({
                    upColor: '#00cc00',
                    downColor: '#ff4444',
                    borderVisible: false,
                    wickUpColor: '#00cc00',
                    wickDownColor: '#ff4444',
                });
                
                console.log('Candlestick series added');
                
                // 主圖表：添加成交量系列
                this.volumeSeries = this.mainChart.addHistogramSeries({
                    color: '#0099ff',
                    priceFormat: {
                        type: 'volume',
                        minMove: 0.01,
                        precision: 2,
                    },
                    priceScaleId: 'left',
                    scaleMargins: {
                        top: 0.85,  // 蠟燭圖佔據頂部85%空間
                        bottom: 0,  // 成交量佔據底部15%空間
                    },
                });
                
                console.log('Volume series added');
                
                // 配置主圖表左側價格軸（成交量）
                this.mainChart.priceScale('left').applyOptions({
                    borderColor: '#333333',
                    textColor: '#cccccc',
                    scaleMargins: {
                        top: 0.85,
                        bottom: 0,
                    },
                });
                
                // 指標圖表：添加RSI線圖系列
                this.rsiSeries = this.indicatorChart.addLineSeries({
                    color: '#ffaa00',
                    lineWidth: 1,
                    priceFormat: {
                        type: 'price',
                        precision: 2,
                        minMove: 0.01,
                    },
                });
                
                console.log('RSI series added');
                
                // 添加RSI超買超賣線
                this.rsiUpperLine = this.indicatorChart.addLineSeries({
                    color: '#ff4444',
                    lineWidth: 1,
                    lineStyle: 2, // 虛線
                    priceFormat: {
                        type: 'price',
                        precision: 2,
                        minMove: 0.01,
                    },
                });
                
                this.rsiLowerLine = this.indicatorChart.addLineSeries({
                    color: '#ff4444',
                    lineWidth: 1,
                    lineStyle: 2, // 虛線
                    priceFormat: {
                        type: 'price',
                        precision: 2,
                        minMove: 0.01,
                    },
                });
                
                console.log('RSI lines added');
                
                // 響應式處理
                this.handleResize();
                window.addEventListener('resize', () => this.handleResize());
                
                console.log('Chart initialization complete');
                
            } catch (error) {
                console.error('Error creating charts:', error);
            }
        }
        
        handleResize() {
            if (this.mainChart) {
                const mainChartContainer = document.querySelector("#main-chart");
                if (mainChartContainer) {
                    this.mainChart.applyOptions({
                        width: mainChartContainer.offsetWidth,
                        height: mainChartContainer.offsetHeight,
                    });
                }
            }
            
            if (this.indicatorChart) {
                const indicatorChartContainer = document.querySelector("#indicator-chart");
                if (indicatorChartContainer) {
                    this.indicatorChart.applyOptions({
                        width: indicatorChartContainer.offsetWidth,
                        height: indicatorChartContainer.offsetHeight,
                    });
                }
            }
        }
        
        async loadChart(timeframe = 'D1') {
            this.currentTimeframe = timeframe;
            
            try {
                console.log(`載入 ${this.symbol} ${timeframe} 數據...`);
                
                const response = await fetch(`/api/candlestick/${this.symbol}/${timeframe}`);
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const data = await response.json();
                
                if (data.error) {
                    throw new Error(data.error);
                }
                
                if (!data.data || data.data.length === 0) {
                    console.error('API返回空數據');
                    return;
                }
                
                console.log(`收到 ${data.data.length} 條數據`);
                console.log('數據樣本:', data.data.slice(0, 3));
                
                // 檢查圖表是否已初始化
                if (!this.candlestickSeries || !this.volumeSeries || !this.rsiSeries) {
                    console.error('圖表系列未正確初始化');
                    this.initChart(); // 嘗試重新初始化
                    if (!this.candlestickSeries || !this.volumeSeries || !this.rsiSeries) {
                        console.error('重新初始化失敗');
                        return;
                    }
                }
                
                // 先去除重複的時間戳，保留最後一條記錄
                const uniqueData = [];
                const timeMap = new Map();
                
                data.data.forEach(item => {
                    if (item && item.timestamp) {
                        const timeKey = item.timestamp;
                        // 總是用最新的數據覆蓋（保留最後一條）
                        timeMap.set(timeKey, item);
                    }
                });
                
                // 將去重後的數據轉為數組並按時間排序
                const deduplicatedData = Array.from(timeMap.values()).sort((a, b) => {
                    return new Date(a.timestamp) - new Date(b.timestamp);
                });
                
                console.log(`去重後: ${deduplicatedData.length} 條數據`);
                
                // 轉換數據格式，添加數據驗證
                const candlestickData = deduplicatedData
                    .filter(item => {
                        // 檢查所有必需字段是否存在且不為 null
                        const hasValidData = item && 
                               item.timestamp && 
                               item.open !== null && item.open !== undefined &&
                               item.high !== null && item.high !== undefined &&
                               item.low !== null && item.low !== undefined &&
                               item.close !== null && item.close !== undefined;
                        
                        if (!hasValidData) {
                            console.warn('發現無效數據點:', item);
                        }
                        return hasValidData;
                    })
                    .map(item => {
                        const timestamp = new Date(item.timestamp);
                        if (isNaN(timestamp.getTime())) {
                            console.warn('無效時間戳:', item.timestamp);
                            return null;
                        }
                        
                        const open = parseFloat(item.open);
                        const high = parseFloat(item.high);
                        const low = parseFloat(item.low);
                        const close = parseFloat(item.close);
                        
                        if (isNaN(open) || isNaN(high) || isNaN(low) || isNaN(close)) {
                            console.warn('無效價格數據:', item);
                            return null;
                        }
                        
                        return {
                            time: Math.floor(timestamp.getTime() / 1000),
                            open: open,
                            high: high,
                            low: low,
                            close: close
                        };
                    })
                    .filter(item => item !== null);
                
                // 確保蠟燭圖數據按時間排序
                candlestickData.sort((a, b) => a.time - b.time);
                
                const volumeData = deduplicatedData
                    .filter(item => {
                        return item && 
                               item.timestamp && 
                               item.volume !== null && item.volume !== undefined &&
                               item.close !== null && item.close !== undefined &&
                               item.open !== null && item.open !== undefined;
                    })
                    .map(item => {
                        const timestamp = new Date(item.timestamp);
                        const volume = parseFloat(item.volume);
                        const close = parseFloat(item.close);
                        const open = parseFloat(item.open);
                        
                        if (isNaN(timestamp.getTime()) || isNaN(volume) || isNaN(close) || isNaN(open)) {
                            return null;
                        }
                        
                        return {
                            time: Math.floor(timestamp.getTime() / 1000),
                            value: volume / 1000000, // 將成交量縮放為百萬為單位
                            color: close >= open ? '#00cc00' : '#ff4444'
                        };
                    })
                    .filter(item => item !== null);
                
                // 確保成交量數據按時間排序
                volumeData.sort((a, b) => a.time - b.time);
                
                console.log(`處理後的蠟燭圖數據: ${candlestickData.length} 條`);
                console.log(`處理後的成交量數據: ${volumeData.length} 條`);
                
                if (candlestickData.length === 0) {
                    console.error('沒有有效的蠟燭圖數據');
                    return;
                }
                
                // 計算RSI數據
                const rsiData = this.calculateRSI(candlestickData, 7);
                
                // 創建RSI超買超賣線數據
                const rsiUpperData = candlestickData.map(item => ({
                    time: item.time,
                    value: 70
                }));
                
                const rsiLowerData = candlestickData.map(item => ({
                    time: item.time,
                    value: 30
                }));
                
                // 設置數據
                try {
                    console.log('設置蠟燭圖數據...');
                    this.candlestickSeries.setData(candlestickData);
                    
                    console.log('設置成交量數據...');
                    this.volumeSeries.setData(volumeData);
                    
                    console.log('設置RSI數據...');
                    this.rsiSeries.setData(rsiData);
                    this.rsiUpperLine.setData(rsiUpperData);
                    this.rsiLowerLine.setData(rsiLowerData);
                    
                    // 自適應顯示範圍
                    this.mainChart.timeScale().fitContent();
                    this.indicatorChart.timeScale().fitContent();
                    
                    // 更新最新數據顯示 - 只有在有有效數據時才更新
                    const validData = data.data.filter(item => 
                        item && item.timestamp && 
                        item.open !== null && item.close !== null
                    );
                    
                    if (validData.length > 0) {
                        this.updateLatestInfo(validData[validData.length - 1]);
                    }
                    
                    console.log('圖表數據載入完成');
                } catch (error) {
                    console.error('設置圖表數據時發生錯誤:', error);
                    console.error('錯誤詳情:', error.stack);
                }
                    
            } catch (error) {
                console.error('載入圖表數據失敗:', error);
            }
        }
        
        calculateRSI(candlestickData, period = 14) {
            const rsiData = [];
            
            if (candlestickData.length < period + 1) {
                return rsiData;
            }
            
            const gains = [];
            const losses = [];
            
            // 計算第一個週期的平均收益和損失
            for (let i = 1; i <= period; i++) {
                const change = candlestickData[i].close - candlestickData[i - 1].close;
                if (change > 0) {
                    gains.push(change);
                    losses.push(0);
                } else {
                    gains.push(0);
                    losses.push(Math.abs(change));
                }
            }
            
            let avgGain = gains.reduce((sum, gain) => sum + gain, 0) / period;
            let avgLoss = losses.reduce((sum, loss) => sum + loss, 0) / period;
            
            // 計算第一個RSI值
            let rs = avgGain / (avgLoss === 0 ? 1 : avgLoss);
            let rsi = 100 - (100 / (1 + rs));
            
            rsiData.push({
                time: candlestickData[period].time,
                value: rsi
            });
            
            // 計算後續的RSI值
            for (let i = period + 1; i < candlestickData.length; i++) {
                const change = candlestickData[i].close - candlestickData[i - 1].close;
                const currentGain = change > 0 ? change : 0;
                const currentLoss = change < 0 ? Math.abs(change) : 0;
                
                // 使用Wilder's smoothing方法
                avgGain = ((avgGain * (period - 1)) + currentGain) / period;
                avgLoss = ((avgLoss * (period - 1)) + currentLoss) / period;
                
                rs = avgGain / (avgLoss === 0 ? 1 : avgLoss);
                rsi = 100 - (100 / (1 + rs));
                
                rsiData.push({
                    time: candlestickData[i].time,
                    value: rsi
                });
            }
            
            return rsiData;
        }
        
        updateLatestInfo(latestData) {
            if (!latestData) return;
            
            // 在MT5風格布局中，價格信息已經在交易面板中顯示
            // 這裡只需要更新圖表標題
            const chartTitle = document.querySelector('.chart-title');
            if (chartTitle) {
                const symbol = 'US30Cash';
                const timeframe = this.currentTimeframe || 'D1';
                chartTitle.textContent = `${symbol},${timeframe}`;
            }
            
            // 更新交易面板中的價格
            const bidElement = document.querySelector('.bid');
            const askElement = document.querySelector('.ask');
            
            if (bidElement && askElement) {
                const close = parseFloat(latestData.close);
                bidElement.textContent = close.toFixed(2);
                askElement.textContent = (close + 2).toFixed(2); // 模擬買賣價差
            }
            
            // 更新RSI顯示
            const rsiData = this.calculateRSI([{close: latestData.close}], 7);
            if (rsiData.length > 0) {
                const indicatorHeader = document.querySelector('.indicator-header span');
                if (indicatorHeader) {
                    indicatorHeader.textContent = `RSI(7) ${rsiData[rsiData.length - 1].value.toFixed(2)}`;
                }
            }
        }
        
        setupEventListeners() {
            // 時間週期按鈕
            document.querySelectorAll('.tf-btn').forEach(btn => {
                btn.addEventListener('click', async (e) => {
                    const timeframe = e.target.dataset.timeframe;
                    
                    // 更新活動狀態
                    document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
                    e.target.classList.add('active');
                    
                    await this.loadChart(timeframe);
                });
            });
            
            // 圖表控制按鈕
            const chartBtns = document.querySelectorAll('.chart-btn');
            chartBtns.forEach((btn, index) => {
                btn.addEventListener('click', () => {
                    switch(index) {
                        case 0: // 放大
                            if (this.mainChart) {
                                this.mainChart.timeScale().zoomIn();
                            }
                            break;
                        case 1: // 縮小
                            if (this.mainChart) {
                                this.mainChart.timeScale().zoomOut();
                            }
                            break;
                        case 2: // 重置
                            this.resetChart();
                            break;
                        case 3: // 設置
                            console.log('設置按鈕被點擊');
                            break;
                    }
                });
            });
        }
        
        resetChart() {
            if (this.mainChart) {
                this.mainChart.timeScale().fitContent();
            }
            if (this.indicatorChart) {
                this.indicatorChart.timeScale().fitContent();
            }
            this.measurementPoints = [];
            this.measurementMode = false;
            
            console.log('圖表已重置');
        }
    }
    
    // 初始化應用
    document.addEventListener('DOMContentLoaded', () => {
        console.log('初始化蠟燭圖應用...');
        window.chartApp = new CandlestickChart();
    });
    
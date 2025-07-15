
    class CandlestickChart {
        constructor() {
            this.mainChart = null;
            this.candlestickSeries = null;
            this.currentTimeframe = 'D1';
            this.symbol = 'EXUSA30IDXUSD';
            this.measurementMode = false;
            this.measurementPoints = [];
            this.measurementLines = []; // 儲存測量線
            this.config = {};
            this.dataCount = 0; // 記錄數據總數
            
            console.log('Market Swing CandlestickChart constructor called');
            this.init();
        }
        
        async init() {
            console.log('Initializing Market Swing Chart...');
            try {
                await this.loadConfig();
                
                // 等待DOM完全載入
                setTimeout(() => {
                    this.initChart();
                    this.setupEventListeners();
                    this.loadChart();
                }, 200);
                
                console.log('Market Swing Chart initialization started');
            } catch (error) {
                console.error('Failed to initialize Market Swing Chart:', error);
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
            console.log('Initializing Market Swing chart...');
            const mainChartContainer = document.querySelector("#main-chart");
            
            if (!mainChartContainer) {
                console.error('Main chart container not found!');
                return;
            }
            
            console.log('Chart container found, creating chart...');
            
            // 清除舊圖表
            if (this.mainChart) {
                try {
                    this.mainChart.remove();
                } catch (e) {
                    console.warn('Error removing main chart:', e);
                }
            }
            
            // 檢查容器尺寸
            const mainWidth = mainChartContainer.offsetWidth || 800;
            const mainHeight = mainChartContainer.offsetHeight || 400;
            
            console.log('Main chart size:', mainWidth, 'x', mainHeight);
            
            try {
                // 強制設置容器尺寸
                if (mainWidth <= 0 || mainHeight <= 0) {
                    mainChartContainer.style.width = '100%';
                    mainChartContainer.style.height = 'calc(100vh - 80px)';
                    
                    // 重新獲取尺寸
                    setTimeout(() => {
                        const newWidth = mainChartContainer.offsetWidth || 1200;
                        const newHeight = mainChartContainer.offsetHeight || 600;
                        console.log('重新計算尺寸:', newWidth, 'x', newHeight);
                        
                        this.createChart(mainChartContainer, newWidth, newHeight);
                    }, 100);
                    return;
                }
                
                this.createChart(mainChartContainer, mainWidth, mainHeight);
                
            } catch (error) {
                console.error('Error creating charts:', error);
            }
        }
        
        createChart(container, width, height) {
            try {
                // 創建主圖表 - Market Swing 乾淨風格配置
                this.mainChart = LightweightCharts.createChart(container, {
                    width: width,
                    height: height,
                    layout: {
                        backgroundColor: '#000000',
                        textColor: '#ffffff',
                    },
                    grid: {
                        vertLines: {
                            visible: false,
                        },
                        horzLines: {
                            visible: false,
                        },
                    },
                    crosshair: {
                        mode: LightweightCharts.CrosshairMode.Hidden,
                    },
                    rightPriceScale: {
                        borderVisible: false,
                        textColor: '#cccccc',
                    },
                    timeScale: {
                        borderVisible: false,
                        textColor: '#cccccc',
                        timeVisible: true,
                        secondsVisible: false,
                    },
                    handleScroll: {
                        mouseWheel: true,
                        pressedMouseMove: true,
                    },
                    handleScale: {
                        axisPressedMouseMove: true,
                        mouseWheel: true,
                        pinch: true,
                    },
                });
                
                console.log('Main chart created successfully');
                
                // 添加蠟燭圖系列 - Market Swing 風格
                this.candlestickSeries = this.mainChart.addCandlestickSeries({
                    upColor: '#00cc00',
                    downColor: '#ff4444',
                    borderVisible: false,
                    wickUpColor: '#00cc00',
                    wickDownColor: '#ff4444',
                });
                
                console.log('Candlestick series added - Market Swing style');
                
                // 響應式處理
                this.handleResize();
                window.addEventListener('resize', () => this.handleResize());
                
                // 添加圖表點擊事件處理
                this.mainChart.subscribeClick((param) => {
                    console.log('圖表被點擊！param:', param);
                    console.log('測量模式狀態:', this.measurementMode);
                    console.log('param.time存在:', !!param.time);
                    
                    if (this.measurementMode) {
                        console.log('測量模式已啟動，處理測量點擊');
                        this.handleMeasurementClick(param);
                    } else {
                        console.log('測量模式未啟動，忽略點擊');
                    }
                });
                
                // 添加滑鼠中鍵事件支持
                container.addEventListener('mousedown', (event) => {
                    if (event.button === 1) { // 滑鼠中鍵
                        event.preventDefault();
                        this.toggleMeasurementMode();
                    }
                });
                
                console.log('Chart initialization complete');
                
            } catch (error) {
                console.error('Error in createChart:', error);
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
                if (!this.candlestickSeries) {
                    console.error('圖表系列未正確初始化');
                    this.initChart(); // 嘗試重新初始化
                    if (!this.candlestickSeries) {
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
                
                console.log(`處理後的蠟燭圖數據: ${candlestickData.length} 條`);
                
                if (candlestickData.length === 0) {
                    console.error('沒有有效的蠟燭圖數據');
                    return;
                }
                
                // 設置數據
                try {
                    console.log('設置Market Swing蠟燭圖數據...');
                    this.candlestickSeries.setData(candlestickData);
                    
                    // 記錄數據總數
                    this.dataCount = candlestickData.length;
                    console.log('數據總數已記錄:', this.dataCount);
                    
                    // 自動縮放到適當比例 - 像TradingView和MT5一樣
                    setTimeout(() => {
                        this.mainChart.timeScale().fitContent();
                        console.log('自動縮放完成');
                    }, 100);
                    
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
        

        
        updateLatestInfo(latestData) {
            if (!latestData) return;
            
            // 更新圖表標題
            const chartTitle = document.querySelector('.chart-title');
            if (chartTitle) {
                const symbol = 'US30Cash';
                const timeframe = this.currentTimeframe || 'D1';
                chartTitle.textContent = `${symbol},${timeframe}`;
            }
            
            // 更新價格信息顯示
            const close = parseFloat(latestData.close);
            this.updatePriceInfo(close.toFixed(2), new Date(latestData.timestamp).toLocaleString());
            
            console.log('Market Swing - 最新數據已更新:', close);
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
            
            // 工具按鈕事件監聽器
            this.setupToolButtons();
            
        }
        
        setupToolButtons() {
            console.log('正在設置工具按鈕...');
            
            // 放大按鈕 - 同時縮放時間軸和價格軸
            const zoomInBtn = document.getElementById('zoom-in');
            console.log('放大按鈕找到:', !!zoomInBtn);
            if (zoomInBtn) {
                zoomInBtn.addEventListener('click', () => {
                    console.log('放大按鈕被點擊');
                    if (this.mainChart) {
                        try {
                            this.performZoom(0.8); // 縮小到80%，即放大
                        } catch (error) {
                            console.error('放大失敗:', error);
                        }
                    } else {
                        console.error('mainChart未初始化');
                    }
                });
            }
            
            // 縮小按鈕 - 同時縮放時間軸和價格軸
            const zoomOutBtn = document.getElementById('zoom-out');
            console.log('縮小按鈕找到:', !!zoomOutBtn);
            if (zoomOutBtn) {
                zoomOutBtn.addEventListener('click', () => {
                    console.log('縮小按鈕被點擊');
                    if (this.mainChart) {
                        try {
                            this.performZoom(1.25); // 擴大到125%，即縮小
                        } catch (error) {
                            console.error('縮小失敗:', error);
                        }
                    } else {
                        console.error('mainChart未初始化');
                    }
                });
            }
            
            // 重置縮放按鈕
            const zoomResetBtn = document.getElementById('zoom-reset');
            if (zoomResetBtn) {
                zoomResetBtn.addEventListener('click', () => {
                    this.resetChart();
                });
            }
            
            // 十字線按鈕
            const crosshairBtn = document.getElementById('crosshair');
            if (crosshairBtn) {
                crosshairBtn.addEventListener('click', () => {
                    this.toggleCrosshair(crosshairBtn);
                });
            }
            
            // 測量工具按鈕 - 修復功能
            const measureBtn = document.getElementById('measure');
            console.log('主應用測量按鈕查找結果:', !!measureBtn);
            if (measureBtn) {
                // 清除之前可能存在的事件監聽器
                measureBtn.removeEventListener('click', this.measurementClickHandler);
                
                // 創建綁定的事件處理器
                this.measurementClickHandler = () => {
                    console.log('主應用測量按鈕被點擊！');
                    this.toggleMeasurement(measureBtn);
                };
                
                measureBtn.addEventListener('click', this.measurementClickHandler);
                console.log('主應用測量按鈕事件監聽器已設置');
            } else {
                console.error('主應用找不到測量按鈕元素 #measure');
            }
            
            // 全屏按鈕
            const fullscreenBtn = document.getElementById('fullscreen');
            if (fullscreenBtn) {
                fullscreenBtn.addEventListener('click', () => {
                    this.toggleFullscreen();
                });
            }
        }
        
        resetChart() {
            if (this.mainChart) {
                this.mainChart.timeScale().fitContent();
            }
            this.clearMeasurementLines();
            this.measurementMode = false;
            
            // 重置測量按鈕狀態
            const button = document.getElementById('measure');
            if (button && button.classList.contains('active')) {
                button.classList.remove('active');
            }
            
            console.log('Market Swing圖表已重置');
        }
        
        toggleCrosshair(button) {
            const isActive = button.classList.toggle('active');
            
            if (this.mainChart) {
                const crosshairMode = isActive ? 1 : 0; // 1 = Normal, 0 = Hidden
                this.mainChart.applyOptions({
                    crosshair: {
                        mode: crosshairMode
                    }
                });
            }
            
            console.log('十字線已', isActive ? '開啟' : '關閉');
        }
        
        toggleMeasurement(button) {
            console.log('toggleMeasurement被調用，當前模式:', this.measurementMode);
            this.measurementMode = button.classList.toggle('active');
            console.log('切換後模式:', this.measurementMode);
            
            if (!this.measurementMode) {
                this.measurementPoints = [];
                this.clearMeasurementLines();
                console.log('測量模式已關閉');
                button.style.backgroundColor = '';
            } else {
                console.log('測量模式已開啟，點擊圖表設置第一個測量點');
                button.style.backgroundColor = '#0066ff';
            }
        }
        
        toggleMeasurementMode() {
            const button = document.getElementById('measure');
            if (button) {
                this.toggleMeasurement(button);
            }
        }
        
        handleMeasurementClick(param) {
            console.log('測量點擊事件被觸發，param:', param);
            console.log('測量模式狀態:', this.measurementMode);
            console.log('param對象的keys:', Object.keys(param || {}));
            
            if (!param || !param.time) {
                console.log('沒有時間數據，嘗試使用邏輯位置');
                
                // 如果沒有時間數據，嘗試從邏輯位置獲取
                if (param && param.logical !== undefined) {
                    console.log('使用邏輯位置:', param.logical);
                    // 使用邏輯位置作為時間值
                    param.time = param.logical;
                } else {
                    console.log('完全沒有位置數據，退出');
                    return;
                }
            }
            
            console.log('candlestickSeries存在:', !!this.candlestickSeries);
            console.log('param.seriesData存在:', !!param.seriesData);
            
            let price = null;
            if (param.seriesData && this.candlestickSeries) {
                price = param.seriesData.get(this.candlestickSeries);
                console.log('從seriesData獲取的價格:', price);
            }
            
            // 如果無法從seriesData獲取價格，嘗試其他方法
            if (!price) {
                console.log('無法從seriesData獲取價格，嘗試其他方法');
                
                // 嘗試使用點擊位置的價格
                if (param.point && param.point.y !== undefined) {
                    // 從點擊的Y座標估算價格
                    const priceScale = this.mainChart.priceScale('right');
                    if (priceScale && priceScale.coordinateToPrice) {
                        const estimatedPrice = priceScale.coordinateToPrice(param.point.y);
                        console.log('從座標估算的價格:', estimatedPrice);
                        price = { close: estimatedPrice };
                    }
                }
                
                // 如果還是沒有價格，使用當前最新價格
                if (!price && this.latestPrice) {
                    console.log('使用最新價格:', this.latestPrice);
                    price = { close: this.latestPrice };
                }
                
                if (!price) {
                    console.log('完全無法獲取價格數據，退出');
                    return;
                }
            }
            
            const priceValue = price.close || price.value;
            const point = {
                time: param.time,
                price: priceValue
            };
            
            console.log('有效測量點:', point);
            
            // 清除之前的測量標記（如果存在）
            if (this.measurementPoints.length >= 2) {
                console.log('清除之前的測量點');
                this.clearMeasurementLines();
                this.measurementPoints = [];
            }
            
            this.measurementPoints.push(point);
            console.log('當前測量點數量:', this.measurementPoints.length);
            
            // 創建十字標記
            const markerColor = this.measurementPoints.length === 1 ? '#0066ff' : '#ff4444'; // 藍色和紅色
            const markerText = this.measurementPoints.length === 1 ? '▲' : '▼';
            
            // 創建標記數據
            const marker = {
                time: param.time,
                position: 'inBar',
                color: markerColor,
                shape: 'arrowUp',
                text: markerText,
                size: 1
            };
            
            // 將標記添加到series
            this.measurementLines.push(marker);
            
            // 更新所有標記
            this.candlestickSeries.setMarkers(this.measurementLines);
            console.log('標記已設置，總標記數:', this.measurementLines.length);
            
            if (this.measurementPoints.length === 1) {
                console.log('第一個測量點設置完成（藍色三角），點擊設置第二個測量點');
            } else if (this.measurementPoints.length === 2) {
                console.log('第二個測量點設置完成（紅色三角），正在計算結果...');
                setTimeout(() => {
                    this.calculateMeasurement();
                }, 100);
            }
        }
        
        calculateMeasurement() {
            if (this.measurementPoints.length !== 2) return;
            
            const [point1, point2] = this.measurementPoints;
            const priceDiff = Math.abs(point2.price - point1.price);
            const timeDiff = Math.abs(point2.time - point1.time);
            
            // 計算價格變化百分比
            const priceChangePercent = ((point2.price - point1.price) / point1.price * 100).toFixed(2);
            
            // 時間差轉換
            const timeDiffSeconds = timeDiff;
            const timeDiffHours = (timeDiffSeconds / 3600).toFixed(2);
            
            // 判斷方向
            const direction = point2.price > point1.price ? '上漲' : '下跌';
            const directionSymbol = point2.price > point1.price ? '📈' : '📉';
            
            console.log('測量結果:', {
                價格差: priceDiff.toFixed(2),
                價格變化: `${priceChangePercent}%`,
                時間差: `${timeDiffHours}小時`,
                方向: direction
            });
            
            // 顯示測量結果
            const result = confirm(`${directionSymbol} 測量結果:\n` +
                `價格差: ${priceDiff.toFixed(2)}\n` +
                `變化率: ${priceChangePercent}% (${direction})\n` +
                `時間差: ${timeDiffHours}小時\n\n` +
                `點擊確定清除測量線，點擊取消保留測量線`);
            
            if (result) {
                this.clearMeasurementLines();
                // 關閉測量模式
                const button = document.getElementById('measure');
                if (button && button.classList.contains('active')) {
                    button.classList.remove('active');
                    this.measurementMode = false;
                }
            }
        }
        
        // 實用的縮放方法 - 時間軸縮放 + 自動價格軸調整
        performZoom(scaleFactor) {
            console.log(`執行縮放，比例: ${scaleFactor}`);
            
            const timeScale = this.mainChart.timeScale();
            
            try {
                // 獲取當前時間軸範圍
                const logicalRange = timeScale.getVisibleLogicalRange();
                if (!logicalRange) {
                    console.log('無法獲取時間軸範圍');
                    return;
                }
                
                console.log('當前時間範圍:', logicalRange);
                
                // 計算新的時間軸範圍
                const currentTimeRange = logicalRange.to - logicalRange.from;
                const newTimeRange = currentTimeRange * scaleFactor;
                const timeCenter = (logicalRange.from + logicalRange.to) / 2;
                
                // 確保時間軸不會縮放過小或過大
                if (scaleFactor < 1 && newTimeRange < 2) {
                    console.log('已達到最大放大限制');
                    return;
                }
                
                const fullRange = this.dataCount || 50;
                if (scaleFactor > 1 && newTimeRange > fullRange) {
                    console.log('已達到最大縮小限制');
                    return;
                }
                
                // 設置新的時間軸範圍
                const newLogicalRange = {
                    from: Math.max(0, timeCenter - newTimeRange / 2),
                    to: Math.min(fullRange, timeCenter + newTimeRange / 2)
                };
                
                console.log('新時間範圍:', newLogicalRange);
                
                // 設置時間軸範圍
                timeScale.setVisibleLogicalRange(newLogicalRange);
                
                // 讓價格軸自動調整以適應可見數據
                setTimeout(() => {
                    try {
                        const priceScale = this.mainChart.priceScale('right');
                        priceScale.setAutoScale(true);
                        console.log('價格軸自動縮放已啟用');
                    } catch (e) {
                        console.log('價格軸自動縮放設置失敗:', e);
                    }
                }, 100);
                
                console.log('縮放完成');
                
            } catch (error) {
                console.error('縮放失敗:', error);
            }
        }
        
        clearMeasurementLines() {
            // 清除測量標記
            try {
                this.candlestickSeries.setMarkers([]);
            } catch (e) {
                console.warn('清除測量標記時出錯:', e);
            }
            this.measurementLines = [];
            this.measurementPoints = [];
            console.log('測量標記已清除');
        }
        
        toggleFullscreen() {
            const chartArea = document.querySelector('.chart-area');
            
            if (!document.fullscreenElement) {
                chartArea.requestFullscreen().catch(err => {
                    console.error('無法進入全屏模式:', err);
                });
            } else {
                document.exitFullscreen();
            }
        }
        
        // 更新價格和時間信息
        updatePriceInfo(price, time) {
            // 保存最新價格供測量功能使用
            this.latestPrice = parseFloat(price);
            
            const priceInfo = document.getElementById('price-info');
            const timeInfo = document.getElementById('time-info');
            
            if (priceInfo) {
                priceInfo.textContent = `價格: ${price}`;
            }
            
            if (timeInfo) {
                timeInfo.textContent = time;
            }
        }
        
        // 更新狀態欄信息
        updateStatusBar(dataCount, latency) {
            const dataCountElement = document.getElementById('data-count');
            const latencyElement = document.getElementById('latency');
            
            if (dataCountElement) {
                dataCountElement.textContent = `數據量: ${dataCount}筆`;
            }
            
            if (latencyElement) {
                latencyElement.textContent = `延遲: ${latency}ms`;
            }
        }
    }
    
    // 初始化應用
    document.addEventListener('DOMContentLoaded', () => {
        console.log('DOM載入完成，正在初始化Market Swing圖表...');
        
        // 多重檢查確保正確初始化
        function initializeChart() {
            const mainChartContainer = document.querySelector("#main-chart");
            
            if (!mainChartContainer) {
                console.warn('圖表容器未找到，2秒後重試...');
                setTimeout(initializeChart, 2000);
                return;
            }
            
            console.log('圖表容器已找到，創建Market Swing應用...');
            window.chartApp = new CandlestickChart();
        }
        
        // 立即嘗試初始化，如果失敗則重試
        try {
            initializeChart();
        } catch (error) {
            console.error('初始化圖表失敗:', error);
            setTimeout(initializeChart, 1000);
        }
    });

    // 確保TradingView庫已載入
    if (typeof LightweightCharts === 'undefined') {
        console.error('TradingView Lightweight Charts 庫未載入！');
    } else {
        console.log('TradingView Lightweight Charts 庫已就緒');
    }    // 測量按鈕測試 - 立即執行
    document.addEventListener('DOMContentLoaded', () => {
        console.log('=== 測量按鈕測試開始 ===');
        setTimeout(() => {
            const measureBtn = document.getElementById('measure');
            console.log('直接查找測量按鈕:', measureBtn);
            console.log('按鈕是否存在:', !!measureBtn);
            
            if (measureBtn) {
                console.log('按鈕文本:', measureBtn.textContent);
                console.log('按鈕標題:', measureBtn.title);
                
                // 添加測試點擊事件
                measureBtn.addEventListener('click', () => {
                    console.log('🎯 測量按鈕測試點擊成功！');
                    alert('測量按鈕測試成功！');
                });
                
                console.log('測試事件監聽器已添加');
            } else {
                console.error('❌ 測量按鈕未找到！');
            }
        }, 500);
    });


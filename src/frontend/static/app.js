
    class CandlestickChart {
        constructor() {
            this.mainChart = null;
            this.candlestickSeries = null;
            this.measurementLineSeries = null; // 測量連接線系列
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
                        backgroundColor: '#2a2a2a',
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
                        mode: LightweightCharts.CrosshairMode.Normal,
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
                
                // 添加測量連接線系列
                this.measurementLineSeries = this.mainChart.addLineSeries({
                    color: '#ffffff',
                    lineWidth: 2,
                    lineStyle: LightweightCharts.LineStyle.Dashed,
                    crosshairMarkerVisible: false,
                    lastValueVisible: false,
                    priceLineVisible: false,
                });
                
                console.log('Candlestick series added - Market Swing style');
                console.log('Measurement line series added');
                
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
                
                // 根據時間週期調整數據限制
                let dataLimit = 10000; // 默認限制
                if (timeframe === 'M1') dataLimit = 5000;  // 1分鐘數據較多，限制更嚴格
                if (timeframe === 'M5') dataLimit = 8000;
                if (timeframe === 'M15') dataLimit = 10000;
                if (timeframe === 'M30') dataLimit = 12000;
                if (timeframe === 'H1') dataLimit = 15000;
                if (timeframe === 'H4') dataLimit = 20000;
                if (timeframe === 'D1') dataLimit = 25000;
                if (timeframe === 'W1') dataLimit = 30000;
                if (timeframe === 'MN') dataLimit = 50000;
                
                const response = await fetch(`/api/candlestick/${this.symbol}/${timeframe}?limit=${dataLimit}`);
                
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
                
                console.log(`收到 ${data.data.length} 條數據 (限制: ${dataLimit})`);
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
                
                // 優化數據處理 - 限制處理的數據量
                const maxProcessData = Math.min(data.data.length, dataLimit);
                const dataToProcess = data.data.slice(-maxProcessData); // 取最新的數據
                
                console.log(`處理 ${dataToProcess.length} 條數據 (從總共 ${data.data.length} 條中)`);
                
                // 先去除重複的時間戳，保留最後一條記錄
                const uniqueData = [];
                const timeMap = new Map();
                
                dataToProcess.forEach(item => {
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
                        console.log('初始自動縮放完成');
                        
                        // 自動放大11個階段 - 使用更直接的方法
                        setTimeout(() => {
                            const timeScale = this.mainChart.timeScale();
                            const logicalRange = timeScale.getVisibleLogicalRange();
                            
                            if (logicalRange) {
                                // 計算放大11次後的範圍
                                const currentTimeRange = logicalRange.to - logicalRange.from;
                                const zoomedTimeRange = currentTimeRange * Math.pow(0.8, 11); // 0.8的11次方
                                const timeCenter = (logicalRange.from + logicalRange.to) / 2;
                                
                                const newLogicalRange = {
                                    from: Math.max(0, timeCenter - zoomedTimeRange / 2),
                                    to: Math.min(this.dataCount || 50, timeCenter + zoomedTimeRange / 2)
                                };
                                
                                timeScale.setVisibleLogicalRange(newLogicalRange);
                                console.log('自動放大11個階段完成');
                            }
                        }, 300);
                    }, 100);
                    
                    // 更新最新數據顯示 - 只有在有有效數據時才更新
                    const validData = dataToProcess.filter(item => 
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
            
            // 詳細調試座標信息
            if (param && param.point) {
                console.log('點擊座標詳情:', {
                    'X座標': param.point.x,
                    'Y座標': param.point.y,
                    '座標類型': typeof param.point.x + ', ' + typeof param.point.y
                });
            }
            
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
            let timeValue = param.time;
            
            // 優先從seriesData獲取價格
            if (param.seriesData && this.candlestickSeries) {
                price = param.seriesData.get(this.candlestickSeries);
                console.log('從seriesData獲取的價格:', price);
            }
            
            // 嘗試從seriesPrices獲取價格
            if (!price && param.seriesPrices && param.seriesPrices.size > 0) {
                console.log('嘗試從seriesPrices獲取價格');
                console.log('seriesPrices內容:', param.seriesPrices);
                
                // 遍歷所有系列數據
                for (const [series, data] of param.seriesPrices) {
                    console.log('系列:', series);
                    console.log('數據:', data);
                    
                    if (series === this.candlestickSeries && data) {
                        price = data;
                        console.log('✅ 從seriesPrices獲取到價格:', data);
                        break;
                    }
                }
            }
            
            // 如果無法從seriesData獲取價格，嘗試從點擊位置獲取
            if (!price && param.point && param.point.y !== undefined) {
                console.log('嘗試從點擊位置獲取價格');
                console.log('Y座標值:', param.point.y);
                
                // 嘗試從可見範圍估算價格
                const priceScale = this.mainChart.priceScale('right');
                console.log('價格軸對象:', priceScale);
                
                if (priceScale && typeof priceScale.getVisibleRange === 'function') {
                    console.log('使用可見範圍估算');
                    const range = priceScale.getVisibleRange();
                    if (range) {
                        const totalHeight = this.mainChart.height();
                        const yRatio = param.point.y / totalHeight;
                        const estimatedPrice = range.minValue + (range.maxValue - range.minValue) * (1 - yRatio);
                        
                        console.log('可見範圍估算:', {
                            '範圍': range,
                            '總高度': totalHeight,
                            'Y比例': yRatio,
                            '估算價格': estimatedPrice
                        });
                        
                        // 驗證估算價格是否合理（基於數據庫中的價格範圍）
                        if (estimatedPrice && !isNaN(estimatedPrice) && estimatedPrice > 10000 && estimatedPrice < 13000) {
                            price = { close: estimatedPrice };
                            console.log('✅ 使用可見範圍估算價格:', estimatedPrice);
                        } else {
                            console.log('❌ 估算價格超出合理範圍，跳過');
                            console.log('價格範圍檢查:', {
                                '有值': !!estimatedPrice,
                                '非NaN': !isNaN(estimatedPrice),
                                '大於10000': estimatedPrice > 10000,
                                '小於13000': estimatedPrice < 13000,
                                '實際值': estimatedPrice
                            });
                        }
                    } else {
                        console.log('❌ 無法獲取可見範圍');
                    }
                } else {
                    console.log('❌ 價格軸或getVisibleRange方法不存在');
                }
            }
            
            // 最後的備用方案：使用當前最新價格
            if (!price && this.latestPrice) {
                console.log('使用最新價格作為備用:', this.latestPrice);
                price = { close: this.latestPrice };
            }
            
            if (!price) {
                console.log('完全無法獲取價格數據，退出');
                return;
            }
            
            const priceValue = price.close || price.value;
            const point = {
                time: timeValue,
                price: priceValue
            };
            
            console.log('有效測量點:', point);
            
            // 檢查是否與之前的測量點價格相同
            if (this.measurementPoints.length > 0) {
                const lastPoint = this.measurementPoints[this.measurementPoints.length - 1];
                console.log('與前一個測量點比較:', {
                    '前一個價格': lastPoint.price,
                    '當前價格': priceValue,
                    '價格相同': lastPoint.price === priceValue,
                    '時間相同': lastPoint.time === timeValue
                });
            }
            
            // 清除之前的測量標記（如果存在）
            if (this.measurementPoints.length >= 2) {
                console.log('清除之前的測量點');
                this.clearMeasurementLines();
                this.measurementPoints = [];
            }
            
            this.measurementPoints.push(point);
            console.log('當前測量點數量:', this.measurementPoints.length);
            
            // 創建測量點標記 - 十字樣式
            const markerColor = this.measurementPoints.length === 1 ? '#00aaff' : '#ff6600'; // 亮藍色和橙色
            const markerText = this.measurementPoints.length === 1 ? '✚' : '✚'; // 使用十字
            
            // 創建標記數據
            const marker = {
                time: timeValue,
                position: 'inBar',
                color: markerColor,
                shape: 'cross',
                text: markerText,
                size: 1
            };
            
            // 將標記添加到series
            this.measurementLines.push(marker);
            
            // 如果有兩個測量點，添加連接線
            if (this.measurementPoints.length === 2) {
                this.addConnectionLine();
            }
            
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
            
            console.log('測量點1:', point1);
            console.log('測量點2:', point2);
            
            // 計算價格差異（絕對值）
            const priceDiff = Math.abs(point2.price - point1.price);
            
            // 計算價格變化（從第一點到第二點，保留正負號）
            const priceChange = point2.price - point1.price;
            let priceChangePercent;
            
            // 確保基準價格不為0，避免除零錯誤
            if (point1.price !== 0) {
                priceChangePercent = ((priceChange / point1.price) * 100).toFixed(2);
            } else {
                priceChangePercent = '0.00';
            }
            
            console.log('價格計算調試:', {
                '第一點價格': point1.price,
                '第二點價格': point2.price,
                '價格變化': priceChange,
                '變化率': priceChangePercent
            });
            
            // 正確處理時間差
            let timeDiffText = '0天0時0分';
            try {
                // 統一時間處理：將所有時間轉換為Date對象進行計算
                let time1, time2;
                
                if (typeof point1.time === 'number' && typeof point2.time === 'number') {
                    // 檢查是否為Unix時間戳（通常大於1000000000）
                    if (point1.time > 1000000000 && point2.time > 1000000000) {
                        // Unix時間戳（秒），轉換為毫秒
                        time1 = new Date(point1.time * 1000);
                        time2 = new Date(point2.time * 1000);
                        
                        console.log('時間計算調試 (Unix時間戳):', {
                            '時間1原始值': point1.time,
                            '時間2原始值': point2.time,
                            '時間1轉換後': time1.toISOString(),
                            '時間2轉換後': time2.toISOString()
                        });
                    } else {
                        // 邏輯位置，需要根據時間週期計算
                        const timeDiff = Math.abs(point2.time - point1.time);
                        const timeframeMultiplier = this.getTimeframeMultiplier();
                        const timeDiffMs = timeDiff * timeframeMultiplier;
                        
                        // 使用當前時間作為基準，向前推算
                        const now = new Date();
                        time1 = new Date(now.getTime() - timeDiffMs);
                        time2 = now;
                        
                        console.log('時間計算調試 (邏輯位置):', {
                            '邏輯時間差': timeDiff,
                            '時間週期倍數': timeframeMultiplier,
                            '總毫秒差': timeDiffMs,
                            '時間1': time1.toISOString(),
                            '時間2': time2.toISOString()
                        });
                    }
                } else {
                    // 如果時間是字符串，直接解析
                    time1 = new Date(point1.time);
                    time2 = new Date(point2.time);
                    
                    console.log('時間計算調試 (字符串時間):', {
                        '時間1原始值': point1.time,
                        '時間2原始值': point2.time,
                        '時間1解析後': time1.toISOString(),
                        '時間2解析後': time2.toISOString()
                    });
                }
                
                // 驗證時間對象是否有效
                if (isNaN(time1.getTime()) || isNaN(time2.getTime())) {
                    throw new Error('無效的時間值');
                }
                
                // 計算時間差（毫秒）
                const timeDiffMs = Math.abs(time2.getTime() - time1.getTime());
                
                // 計算天、時、分
                const days = Math.floor(timeDiffMs / (24 * 60 * 60 * 1000));
                const hours = Math.floor((timeDiffMs % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000));
                const minutes = Math.floor((timeDiffMs % (60 * 60 * 1000)) / (60 * 1000));
                
                timeDiffText = `${days}天${hours}時${minutes}分`;
                
                console.log('最終時間計算結果:', {
                    '時間差(毫秒)': timeDiffMs,
                    '天': days,
                    '時': hours,
                    '分': minutes,
                    '最終格式': timeDiffText
                });
                
            } catch (error) {
                console.error('時間差計算錯誤:', error);
                timeDiffText = '計算錯誤';
            }
            
            // 判斷方向（從第一點到第二點）
            let direction, directionSymbol;
            if (priceChange > 0) {
                direction = '上漲';
                directionSymbol = '📈';
            } else if (priceChange < 0) {
                direction = '下跌';  
                directionSymbol = '📉';
            } else {
                direction = '持平';
                directionSymbol = '⚊';
            }
            
            console.log('方向計算調試:', {
                '價格變化值': priceChange,
                '方向': direction,
                '符號': directionSymbol
            });
            
            console.log('測量結果總結:', {
                價格差: priceDiff.toFixed(2),
                價格變化率: `${priceChangePercent}%`,
                時間差: timeDiffText,
                方向: `${directionSymbol} ${direction}`,
                原始價格變化: priceChange
            });
            
            // 顯示測量結果在懸浮視窗中
            this.showMeasurementPopup(priceDiff, priceChangePercent, timeDiffText, direction, directionSymbol);
        }
        
        getTimeframeMultiplier() {
            // 根據當前時間週期返回毫秒倍數
            const multipliers = {
                'M1': 60 * 1000,           // 1分鐘
                'M5': 5 * 60 * 1000,       // 5分鐘
                'M15': 15 * 60 * 1000,     // 15分鐘
                'M30': 30 * 60 * 1000,     // 30分鐘
                'H1': 60 * 60 * 1000,      // 1小時
                'H4': 4 * 60 * 60 * 1000,  // 4小時
                'D1': 24 * 60 * 60 * 1000, // 1天
                'W1': 7 * 24 * 60 * 60 * 1000, // 1週
                'MN': 30 * 24 * 60 * 60 * 1000 // 1月
            };
            return multipliers[this.currentTimeframe] || 24 * 60 * 60 * 1000; // 默認1天
        }
        
        showMeasurementPopup(priceDiff, priceChangePercent, timeDiffText, direction, directionSymbol) {
            // 更新懸浮視窗內容
            document.getElementById('measurement-price-diff').textContent = priceDiff.toFixed(2);
            
            // 確保百分比正確顯示正負號
            const percentageText = parseFloat(priceChangePercent) >= 0 ? `+${priceChangePercent}%` : `${priceChangePercent}%`;
            document.getElementById('measurement-change-percent').textContent = percentageText;
            
            document.getElementById('measurement-time-diff').textContent = timeDiffText;
            document.getElementById('measurement-direction').textContent = `${directionSymbol} ${direction}`;
            
            console.log('懸浮視窗顯示內容:', {
                '價格差': priceDiff.toFixed(2),
                '變化率顯示': percentageText,
                '時間差': timeDiffText,
                '方向顯示': `${directionSymbol} ${direction}`
            });
            
            // 顯示懸浮視窗
            const popup = document.getElementById('measurement-popup');
            popup.style.display = 'block';
            
            // 設置全局變數供按鈕使用
            window.currentMeasurementChart = this;
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
        
        addConnectionLine() {
            if (this.measurementPoints.length !== 2) return;
            
            const [point1, point2] = this.measurementPoints;
            
            // 創建連接線數據
            const lineData = [
                { time: point1.time, value: point1.price },
                { time: point2.time, value: point2.price }
            ];
            
            // 設置連接線數據
            this.measurementLineSeries.setData(lineData);
            
            console.log('連接線已添加');
        }
        
        clearMeasurementLines() {
            // 清除測量標記
            try {
                this.candlestickSeries.setMarkers([]);
            } catch (e) {
                console.warn('清除測量標記時出錯:', e);
            }
            
            // 清除連接線
            if (this.measurementLineSeries) {
                this.measurementLineSeries.setData([]);
            }
            
            this.measurementLines = [];
            this.measurementPoints = [];
            console.log('測量標記和連接線已清除');
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
    }
    
    // 懸浮視窗控制函數
    function closeMeasurementPopup() {
        const popup = document.getElementById('measurement-popup');
        popup.style.display = 'none';
    }
    
    function clearMeasurementAndClose() {
        if (window.currentMeasurementChart) {
            window.currentMeasurementChart.clearMeasurementLines();
            // 關閉測量模式
            const button = document.getElementById('measure');
            if (button && button.classList.contains('active')) {
                button.classList.remove('active');
                window.currentMeasurementChart.measurementMode = false;
            }
        }
        closeMeasurementPopup();
    }
    
    function keepMeasurementAndClose() {
        // 只關閉懸浮視窗，保留測量線
        closeMeasurementPopup();
    }
    

    




    class CandlestickChart {
        constructor() {
            this.mainChart = null;
            this.candlestickSeries = null;
            this.measurementLineSeries = null; // 測量連接線系列
            this.currentTimeframe = 'D1';
            this.symbol = 'XAUUSD'; // 預設為 XAUUSD
            this.currentAlgorithm = 'zigzag'; // 預設演算法
            this.measurementMode = false;
            this.measurementPoints = [];
            this.measurementLines = []; // 儲存測量線
            this.swingLines = []; // 儲存波段連接線
            this.swingPoints = []; // 儲存波段點
            this.config = {};
            this.dataCount = 0; // 記錄數據總數
            
            // 性能優化設置
            this.dataCache = new Map(); // 數據緩存
            this.lastLoadTime = 0; // 上次載入時間
            this.loadingPromise = null; // 防止重複載入

            
            // MT5風格真正的虛擬化渲染設置
            this.fullDataCache = new Map(); // 完整數據緩存
            this.renderCache = new Map(); // 渲染數據緩存
            this.visibleRange = { from: 0, to: 1000 }; // 可見範圍
            this.currentBarSpacing = 3; // 當前bar間距
            this.isLoadingMore = false; // 是否正在加載更多數據
            
            // MT5風格動態渲染參數
            this.renderThresholds = {
                minBarSpacing: 0.5,  // 最小bar間距
                maxBarSpacing: 20,   // 最大bar間距
                maxVisibleBars: 10000 // 最大可見bar數量
            };
            
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
                    
                    // 確保下拉選單初始狀態是關閉的
                    this.forceCloseDropdowns();
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
                this.symbol = this.config.symbol || 'XAUUSD';
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
                // 創建主圖表 - MT5風格高性能配置
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
                        autoScale: true,
                        scaleMargins: {
                            top: 0.1,
                            bottom: 0.1,
                        },
                    },
                    timeScale: {
                        borderVisible: false,
                        textColor: '#cccccc',
                        timeVisible: true,
                        secondsVisible: false,
                        rightOffset: 5,
                        barSpacing: 3, // 優化渲染性能
                        minBarSpacing: 1,
                        rightBarStaysOnScroll: true,
                    },
                    handleScroll: {
                        mouseWheel: false, // 禁用默認滾輪行為，使用自定義處理
                        pressedMouseMove: true,
                        horzTouchDrag: true,
                        vertTouchDrag: true,
                    },
                    handleScale: {
                        axisPressedMouseMove: true,
                        mouseWheel: false,  // 禁用滾輪縮放，使用自定義滾輪移動
                        pinch: true,
                        axisDoubleClickReset: true,
                    },
                    // 性能優化設置
                    watermark: {
                        visible: false,
                    },
                    overlayPriceScales: {
                        borderVisible: false,
                    },
                    // 啟用高性能模式
                    localization: {
                        timeFormatter: (time) => {
                            return new Date(time * 1000).toLocaleDateString();
                        },
                    },
                });
                
                console.log('Main chart created successfully');
                
                // 添加蠟燭圖系列 - MT5風格高性能配置
                this.candlestickSeries = this.mainChart.addCandlestickSeries({
                    upColor: '#00cc00',
                    downColor: '#ff4444',
                    borderVisible: false,
                    wickUpColor: '#00cc00',
                    wickDownColor: '#ff4444',
                });
                
                // 添加縮放事件監聽器 - MT5風格動態數據加載
                this.mainChart.timeScale().subscribeVisibleTimeRangeChange((timeRange) => {
                    if (timeRange) {
                        this.handleTimeRangeChange(timeRange);
                    }
                });
                
                // 添加縮放事件監聽器
                this.mainChart.timeScale().subscribeVisibleLogicalRangeChange((logicalRange) => {
                    if (logicalRange) {
                        this.handleLogicalRangeChange(logicalRange);
                    }
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
                
                // 添加自定義滾輪事件處理器 - 滾輪用於左右移動圖表
                container.addEventListener('wheel', (event) => {
                    event.preventDefault(); // 阻止默認的滾輪行為
                    
                    if (this.mainChart) {
                        const timeScale = this.mainChart.timeScale();
                        const logicalRange = timeScale.getVisibleLogicalRange();
                        
                        if (logicalRange) {
                            // 計算移動距離 - 根據滾輪方向和當前可見範圍調整
                            const visibleRange = logicalRange.to - logicalRange.from;
                            const moveDistance = Math.max(1, Math.floor(visibleRange * 0.1)); // 移動10%的可見範圍
                            
                            // 根據滾輪方向決定移動方向
                            const direction = event.deltaY > 0 ? -1 : 1; // 向下滾動向左移動，向上滾動向右移動
                            
                            // 計算新的時間範圍
                            const newFrom = Math.max(0, logicalRange.from + (direction * moveDistance));
                            const newTo = logicalRange.to + (direction * moveDistance);
                            
                            // 設置新的可見範圍
                            timeScale.setVisibleLogicalRange({
                                from: newFrom,
                                to: newTo
                            });
                        }
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
        
        // MT5風格數據加載策略 - 平衡性能和完整性
        getDataStrategy(timeframe) {
            // 平衡策略：載入適量數據，確保性能
            const strategies = {
                'M1': { 
                    name: 'MT5平衡模式', 
                    limit: 100000,  // 載入10萬條數據
                    virtualRender: true,
                    fullDataMode: true
                },
                'M5': { 
                    name: 'MT5平衡模式', 
                    limit: 50000,   // 載入5萬條數據
                    virtualRender: true,
                    fullDataMode: true
                },
                'M15': { 
                    name: 'MT5平衡模式', 
                    limit: 30000,
                    virtualRender: true,
                    fullDataMode: true
                },
                'M30': { 
                    name: 'MT5平衡模式', 
                    limit: 20000,
                    virtualRender: true,
                    fullDataMode: true
                },
                'H1': { 
                    name: 'MT5平衡模式', 
                    limit: 15000,
                    virtualRender: true,
                    fullDataMode: true
                },
                'H4': { 
                    name: 'MT5平衡模式', 
                    limit: 10000,
                    virtualRender: true,
                    fullDataMode: true
                },
                'D1': { 
                    name: 'MT5平衡模式', 
                    limit: 8000,
                    virtualRender: true,
                    fullDataMode: true
                },
                'W1': { 
                    name: 'MT5平衡模式', 
                    limit: 5000,
                    virtualRender: true,
                    fullDataMode: true
                },
                'MN': { 
                    name: 'MT5平衡模式', 
                    limit: 2000,
                    virtualRender: true,
                    fullDataMode: true
                }
            };
            
            return strategies[timeframe] || strategies['D1'];
        }
        
        // MT5風格真正的虛擬化數據處理
        processDataIntelligently(rawData, strategy) {
            console.log(`MT5風格處理數據: ${rawData.length} 條原始數據`);
            
            const cacheKey = `${this.symbol}_${this.currentTimeframe}`;
            
            // 處理並存儲完整數據
            const fullData = this.processDataBatchFast(rawData);
            this.fullDataCache.set(cacheKey, fullData);
            
            console.log(`存儲完整數據: ${fullData.length} 條`);
            
            // MT5風格：初始顯示時使用智能初始化視圖
            const initialRenderData = this.getInitialRenderData(fullData, strategy);
            
            // 緩存結果
            this.dataCache.set(cacheKey, initialRenderData);
            return initialRenderData;
        }
        
        // 獲取初始渲染數據
        getInitialRenderData(fullData, strategy) {
            // 返回完整數據，不進行截斷，避免數據缺失
            console.log(`返回完整數據: ${fullData.length} 條`);
            return fullData;
        }
        
        // 創建虛擬化數據 - 修復數據遺漏問題
        createVirtualizedData(data, strategy) {
            const levels = {};
            
            // 為每個縮放級別創建不同精度的數據
            Object.keys(strategy.dataLevels).forEach(level => {
                const levelConfig = strategy.dataLevels[level];
                
                if (data.length <= levelConfig.renderPoints) {
                    // 如果數據量小於目標點數，直接使用所有數據
                    levels[level] = data;
                } else {
                    // 使用智能聚合而不是簡單抽樣
                    levels[level] = this.aggregateDataIntelligently(data, levelConfig.renderPoints);
                }
            });
            
            return {
                fullData: data,
                levels: levels,
                strategy: strategy
            };
        }
        
        // 智能數據聚合 - 避免數據遺漏
        aggregateDataIntelligently(data, targetPoints) {
            if (data.length <= targetPoints) {
                return data;
            }
            
            const result = [];
            const step = data.length / targetPoints;
            
            for (let i = 0; i < targetPoints; i++) {
                const startIndex = Math.floor(i * step);
                const endIndex = Math.min(Math.floor((i + 1) * step), data.length);
                
                if (startIndex >= data.length) break;
                
                // 如果這個區間只有一個數據點，直接使用
                if (endIndex - startIndex === 1) {
                    result.push(data[startIndex]);
                } else {
                    // 如果有多個數據點，進行聚合
                    const segment = data.slice(startIndex, endIndex);
                    const aggregated = this.aggregateSegment(segment);
                    result.push(aggregated);
                }
            }
            
            return result;
        }
        
        // 聚合數據段
        aggregateSegment(segment) {
            if (segment.length === 0) return null;
            if (segment.length === 1) return segment[0];
            
            // 按時間排序
            segment.sort((a, b) => a.time - b.time);
            
            const first = segment[0];
            const last = segment[segment.length - 1];
            
            // 聚合OHLC數據
            const high = Math.max(...segment.map(d => d.high));
            const low = Math.min(...segment.map(d => d.low));
            
            return {
                time: first.time, // 使用第一個時間點
                open: first.open,
                high: high,
                low: low,
                close: last.close
            };
        }
        
        // 獲取當前縮放級別的數據
        getCurrentLevelData(virtualizedData, strategy) {
            const levelConfig = strategy.dataLevels[this.currentZoomLevel];
            if (!levelConfig) {
                return virtualizedData.fullData.slice(0, strategy.maxDisplayPoints);
            }
            
            return virtualizedData.levels[this.currentZoomLevel] || virtualizedData.fullData.slice(0, levelConfig.renderPoints);
        }
        
        // 動態調整縮放級別
        adjustZoomLevel(barSpacing) {
            let newLevel = 'normal';
            
            if (barSpacing < 2) {
                newLevel = 'detail'; // 詳細模式
            } else if (barSpacing > 10) {
                newLevel = 'overview'; // 概覽模式
            } else {
                newLevel = 'normal'; // 正常模式
            }
            
            if (newLevel !== this.currentZoomLevel) {
                this.currentZoomLevel = newLevel;
                console.log(`切換到縮放級別: ${newLevel}`);
                this.updateChartWithCurrentLevel();
            }
        }
        
        // 更新圖表數據
        updateChartWithCurrentLevel() {
            const cacheKey = `${this.symbol}_${this.currentTimeframe}`;
            const virtualizedData = this.virtualData.get(cacheKey);
            
            if (virtualizedData) {
                const currentData = this.getCurrentLevelData(virtualizedData, virtualizedData.strategy);
                if (this.candlestickSeries) {
                    this.candlestickSeries.setData(currentData);
                }
            }
        }
        
        // MT5風格處理時間範圍變化
        handleTimeRangeChange(timeRange) {
            if (!timeRange) return;
            
            // 避免在縮放操作時頻繁更新
            if (this.isZooming) return;
            
            const cacheKey = `${this.symbol}_${this.currentTimeframe}`;
            const fullData = this.fullDataCache.get(cacheKey);
            
            if (!fullData || fullData.length === 0) return;
            
            // 計算當前可見的時間範圍
            const timeSpan = timeRange.to - timeRange.from;
            const barSpacing = this.mainChart.timeScale().options().barSpacing || 3;
            this.currentBarSpacing = barSpacing;
            
            // 暫時禁用動態更新，避免數據缺失
            // 讓 LightweightCharts 庫自己處理數據渲染
            console.log(`時間範圍變化: ${timeSpan.toFixed(2)} 時間單位, bar間距: ${barSpacing.toFixed(2)}`);
        }
        
        // MT5風格獲取時間範圍內的渲染數據
        getRenderDataForTimeRange(fullData, timeRange, barSpacing) {
            // 找到時間範圍內的數據
            const rangeData = fullData.filter(item => 
                item.time >= timeRange.from && item.time <= timeRange.to
            );
            
            // 根據bar間距決定數據密度
            let renderData = rangeData;
            
            // MT5風格智能抽樣：當數據過多且bar間距小時進行智能聚合
            if (barSpacing < 1 && rangeData.length > this.renderThresholds.maxVisibleBars) {
                // 極小間距時，使用時間段聚合
                const targetPoints = Math.min(rangeData.length, this.renderThresholds.maxVisibleBars);
                renderData = this.intelligentTimeAggregation(rangeData, targetPoints);
            } else if (barSpacing < 2 && rangeData.length > this.renderThresholds.maxVisibleBars * 2) {
                // 小間距時，輕度聚合
                const targetPoints = Math.min(rangeData.length, this.renderThresholds.maxVisibleBars * 1.5);
                renderData = this.intelligentTimeAggregation(rangeData, targetPoints);
            }
            // 其他情況直接顯示所有數據
            
            return renderData;
        }
        
        // 智能時間聚合
        intelligentTimeAggregation(data, targetPoints) {
            if (data.length <= targetPoints) {
                return data;
            }
            
            const result = [];
            const step = data.length / targetPoints;
            
            for (let i = 0; i < targetPoints; i++) {
                const startIndex = Math.floor(i * step);
                const endIndex = Math.min(Math.floor((i + 1) * step), data.length);
                
                if (startIndex >= data.length) break;
                
                if (endIndex - startIndex === 1) {
                    // 單一數據點直接使用
                    result.push(data[startIndex]);
                } else {
                    // 多個數據點進行OHLC聚合
                    const segment = data.slice(startIndex, endIndex);
                    const aggregated = this.aggregateOHLC(segment);
                    result.push(aggregated);
                }
            }
            
            return result;
        }
        
        // OHLC聚合
        aggregateOHLC(segment) {
            if (segment.length === 0) return null;
            if (segment.length === 1) return segment[0];
            
            // 按時間排序確保正確性
            segment.sort((a, b) => a.time - b.time);
            
            const first = segment[0];
            const last = segment[segment.length - 1];
            
            return {
                time: first.time,
                open: first.open,
                high: Math.max(...segment.map(d => d.high)),
                low: Math.min(...segment.map(d => d.low)),
                close: last.close
            };
        }
        
        // 處理邏輯範圍變化
        handleLogicalRangeChange(logicalRange) {
            this.visibleRange = logicalRange;
            
            // 更新狀態欄顯示當前可見範圍
            const visibleBars = logicalRange.to - logicalRange.from;
            this.updateStatusBar(
                `${visibleBars.toFixed(0)} 個可見bar`, 
                `範圍: ${logicalRange.from.toFixed(0)} - ${logicalRange.to.toFixed(0)}`
            );
        }
        
        // 檢查並加載更多數據
        async checkAndLoadMoreData(timeRange) {
            if (this.isLoadingMore) return;
            
            const cacheKey = `${this.symbol}_${this.currentTimeframe}`;
            const virtualizedData = this.virtualData.get(cacheKey);
            
            if (!virtualizedData) return;
            
            // 檢查是否需要加載更早的數據
            const currentData = virtualizedData.fullData;
            const earliestTime = currentData[0]?.time;
            const latestTime = currentData[currentData.length - 1]?.time;
            
            // 如果用戶滾動到數據邊緣，加載更多數據
            if (timeRange.from < earliestTime + 86400 * 7) { // 7天前
                await this.loadMoreHistoricalData();
            }
        }
        
        // 加載更多歷史數據
        async loadMoreHistoricalData() {
            if (this.isLoadingMore) return;
            
            this.isLoadingMore = true;
            console.log('加載更多歷史數據...');
            
            try {
                const cacheKey = `${this.symbol}_${this.currentTimeframe}`;
                const virtualizedData = this.virtualData.get(cacheKey);
                
                if (!virtualizedData) return;
                
                const currentData = virtualizedData.fullData;
                const earliestTime = currentData[0]?.time;
                
                // 計算需要加載的時間範圍
                const daysToLoad = 30; // 每次加載30天
                const startTime = earliestTime - (daysToLoad * 86400);
                
                // 這裡可以實現動態加載更多數據的邏輯
                // 由於API限制，我們暫時只使用已加載的數據
                console.log(`需要加載 ${startTime} 之前的數據`);
                
            } catch (error) {
                console.error('加載更多數據失敗:', error);
            } finally {
                this.isLoadingMore = false;
            }
        }
        
        // 數據去重 (保留作為備用)
        deduplicateData(data) {
                const timeMap = new Map();
                
            data.forEach(item => {
                    if (item && item.timestamp) {
                        const timeKey = item.timestamp;
                        timeMap.set(timeKey, item);
                    }
                });
                
            return Array.from(timeMap.values()).sort((a, b) => {
                    return new Date(a.timestamp) - new Date(b.timestamp);
                });
        }
        
        // 分塊處理數據 (保留作為備用)
        processDataInChunks(data, strategy) {
            console.log(`開始分塊處理 ${data.length} 條數據`);
            
            const chunks = [];
            const chunkSize = strategy.chunkSize;
            
            for (let i = 0; i < data.length; i += chunkSize) {
                const chunk = data.slice(i, i + chunkSize);
                chunks.push(chunk);
            }
            
            console.log(`分成 ${chunks.length} 個塊，每塊 ${chunkSize} 條數據`);
            
            // 處理第一個塊（最新的數據）
            const firstChunk = this.processDataBatch(chunks[0]);
            
            // 如果數據量仍然很大，進行抽樣
            if (firstChunk.length > strategy.maxDisplayPoints) {
                console.log(`進行數據抽樣: ${firstChunk.length} -> ${strategy.maxDisplayPoints}`);
                return this.sampleData(firstChunk, strategy.maxDisplayPoints);
            }
            
            return firstChunk;
        }
        
        // 極速批量處理數據
        processDataBatchFast(data) {
            const result = [];
            
            for (let i = 0; i < data.length; i++) {
                const item = data[i];
                
                // 快速驗證
                if (!item || !item.timestamp || 
                    item.open == null || item.high == null || 
                    item.low == null || item.close == null) {
                    continue;
                }
                
                // 快速轉換
                const timestamp = new Date(item.timestamp);
                if (isNaN(timestamp.getTime())) {
                    continue;
                }
                
                result.push({
                    time: Math.floor(timestamp.getTime() / 1000),
                    open: +item.open,
                    high: +item.high,
                    low: +item.low,
                    close: +item.close
                });
            }
            
            // 按時間排序並去重（保留最新的數據）
            const timeMap = new Map();
            result.forEach(item => {
                const existing = timeMap.get(item.time);
                if (!existing || item.time > existing.time) {
                    timeMap.set(item.time, item);
                }
            });
            
            const finalResult = Array.from(timeMap.values()).sort((a, b) => a.time - b.time);
            console.log(`處理完成: ${data.length} -> ${finalResult.length} 條有效數據`);
            
            return finalResult;
        }
        
        // 批量處理數據 (保留作為備用)
        processDataBatch(data) {
            return data
                    .filter(item => {
                        const hasValidData = item && 
                               item.timestamp && 
                               item.open !== null && item.open !== undefined &&
                               item.high !== null && item.high !== undefined &&
                               item.low !== null && item.low !== undefined &&
                               item.close !== null && item.close !== undefined;
                        
                        return hasValidData;
                    })
                    .map(item => {
                        const timestamp = new Date(item.timestamp);
                        if (isNaN(timestamp.getTime())) {
                            return null;
                        }
                        
                        return {
                            time: Math.floor(timestamp.getTime() / 1000),
                        open: parseFloat(item.open),
                        high: parseFloat(item.high),
                        low: parseFloat(item.low),
                        close: parseFloat(item.close)
                        };
                    })
                    .filter(item => item !== null);
        }
        
        // 數據抽樣
        sampleData(data, targetCount) {
            if (data.length <= targetCount) {
                return data;
            }
            
            const step = Math.ceil(data.length / targetCount);
            const sampled = [];
            
            for (let i = 0; i < data.length; i += step) {
                sampled.push(data[i]);
                if (sampled.length >= targetCount) break;
            }
            
            console.log(`抽樣完成: ${data.length} -> ${sampled.length}`);
            return sampled;
        }
        
        async loadChart(timeframe = 'D1') {
            this.currentTimeframe = timeframe;
            
            try {
                console.log(`載入 ${this.symbol} ${timeframe} 數據...`);
                
                // 智能數據加載策略 - 平衡完整性和性能
                const dataStrategy = this.getDataStrategy(timeframe);
                console.log(`使用數據策略: ${dataStrategy.name}`);
                
                // 顯示載入狀態
                this.updateStatusBar('載入中...', '正在獲取數據');
                
                const response = await fetch(`/api/candlestick/${this.symbol}/${timeframe}?limit=${dataStrategy.limit}`);
                
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
                
                console.log(`收到 ${data.data.length} 條數據 (策略: ${dataStrategy.name})`);
                
                // 檢查圖表是否已初始化
                if (!this.candlestickSeries) {
                    console.error('圖表系列未正確初始化');
                    this.initChart(); // 嘗試重新初始化
                    if (!this.candlestickSeries) {
                        console.error('重新初始化失敗');
                        return;
                    }
                }
                
                // 智能數據處理
                const candlestickData = this.processDataIntelligently(data.data, dataStrategy);
                
                console.log(`處理完成: ${candlestickData.length} 條有效數據`);
                
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
                    if (candlestickData.length > 0) {
                        const latestData = candlestickData[candlestickData.length - 1];
                        this.updateLatestInfo({
                            timestamp: new Date(latestData.time * 1000).toISOString(),
                            close: latestData.close
                        });
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
                const symbol = this.symbol || 'XAUUSD';
                const timeframe = this.currentTimeframe || 'D1';
                chartTitle.textContent = `${symbol},${timeframe}`;
            }
            
            // 更新價格信息顯示
            const close = parseFloat(latestData.close);
            this.updatePriceInfo(close.toFixed(2), new Date(latestData.timestamp).toLocaleString());
            
            console.log('Market Swing - 最新數據已更新:', close);
        }
        
        setupEventListeners() {
            // 品種選擇器事件
            this.setupSymbolSelector();
            
            // 演算法選擇器事件
            this.setupAlgorithmSelector();
            
            // 統一的點擊外部關閉下拉選單事件
            document.addEventListener('click', (e) => {
                // 關閉品種下拉選單
                const symbolBtn = document.getElementById('symbol-dropdown');
                const symbolDropdown = document.getElementById('symbol-dropdown-menu');
                if (symbolBtn && symbolDropdown && !symbolBtn.contains(e.target) && !symbolDropdown.contains(e.target)) {
                    symbolBtn.closest('.symbol-selector').classList.remove('active');
                }
                
                // 關閉演算法下拉選單
                const algorithmBtn = document.getElementById('algorithm-dropdown');
                const algorithmDropdown = document.getElementById('algorithm-dropdown-menu');
                if (algorithmBtn && algorithmDropdown && !algorithmBtn.contains(e.target) && !algorithmDropdown.contains(e.target)) {
                    algorithmBtn.closest('.algorithm-selector').classList.remove('active');
                }
            });
            
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
        
        setupSymbolSelector() {
            console.log('正在設置品種選擇器...');
            
            const symbolBtn = document.getElementById('symbol-dropdown');
            const symbolDropdown = document.getElementById('symbol-dropdown-menu');
            const currentSymbolSpan = document.getElementById('current-symbol');
            
            if (symbolBtn && symbolDropdown) {
                // 點擊品種按鈕切換下拉選單
                symbolBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const selector = symbolBtn.closest('.symbol-selector');
                    selector.classList.toggle('active');
                });
                
                // 點擊選項切換品種
                symbolDropdown.addEventListener('click', async (e) => {
                    const option = e.target.closest('.symbol-option');
                    if (option) {
                        const newSymbol = option.dataset.symbol;
                        await this.switchSymbol(newSymbol);
                        
                        // 更新選中狀態
                        symbolDropdown.querySelectorAll('.symbol-option').forEach(opt => {
                            opt.classList.remove('selected');
                        });
                        option.classList.add('selected');
                        
                        // 關閉下拉選單
                        symbolBtn.closest('.symbol-selector').classList.remove('active');
                    }
                });
                
                // 初始化當前品種顯示
                this.updateSymbolDisplay();
                
                // 確保初始狀態下拉選單是關閉的
                symbolBtn.closest('.symbol-selector').classList.remove('active');
            }
        }
        
        async switchSymbol(newSymbol) {
            console.log('切換品種:', newSymbol);
            this.symbol = newSymbol;
            this.updateSymbolDisplay();
            
            // 重新載入圖表
            await this.loadChart(this.currentTimeframe);
        }
        
        setupAlgorithmSelector() {
            console.log('正在設置演算法選擇器...');
            
            const algorithmBtn = document.getElementById('algorithm-dropdown');
            const algorithmDropdown = document.getElementById('algorithm-dropdown-menu');
            const currentAlgorithmSpan = document.getElementById('current-algorithm');
            
            if (algorithmBtn && algorithmDropdown) {
                // 點擊演算法按鈕切換下拉選單
                algorithmBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const selector = algorithmBtn.closest('.algorithm-selector');
                    selector.classList.toggle('active');
                });
                
                // 點擊選項切換演算法
                algorithmDropdown.addEventListener('click', async (e) => {
                    const option = e.target.closest('.algorithm-option');
                    if (option) {
                        const newAlgorithm = option.dataset.algorithm;
                        await this.switchAlgorithm(newAlgorithm);
                        
                        // 更新選中狀態
                        algorithmDropdown.querySelectorAll('.algorithm-option').forEach(opt => {
                            opt.classList.remove('selected');
                        });
                        option.classList.add('selected');
                        
                        // 關閉下拉選單
                        algorithmBtn.closest('.algorithm-selector').classList.remove('active');
                    }
                });
                
                // 初始化當前演算法顯示
                this.updateAlgorithmDisplay();
                
                // 確保初始狀態下拉選單是關閉的
                algorithmBtn.closest('.algorithm-selector').classList.remove('active');
            }
        }
        
        async switchAlgorithm(newAlgorithm) {
            console.log('切換演算法:', newAlgorithm);
            this.currentAlgorithm = newAlgorithm;
            this.updateAlgorithmDisplay();
            
            // 清除現有的波段顯示
            this.clearSwingLines();
            
            // 顯示提示信息
            console.log(`已切換到 ${newAlgorithm} 演算法，請點擊"顯示波段"按鈕查看新的波段分析`);
        }
        
        updateAlgorithmDisplay() {
            const currentAlgorithmSpan = document.getElementById('current-algorithm');
            if (currentAlgorithmSpan) {
                // 將演算法名稱轉換為顯示名稱
                const displayNames = {
                    'zigzag': 'ZigZag',
                    'fractal': 'Fractal',
                    'pivot': 'Pivot'
                };
                currentAlgorithmSpan.textContent = displayNames[this.currentAlgorithm] || this.currentAlgorithm;
            }
            
            // 更新選中狀態
            const algorithmDropdown = document.getElementById('algorithm-dropdown-menu');
            if (algorithmDropdown) {
                algorithmDropdown.querySelectorAll('.algorithm-option').forEach(option => {
                    if (option.dataset.algorithm === this.currentAlgorithm) {
                        option.classList.add('selected');
                    } else {
                        option.classList.remove('selected');
                    }
                });
            }
        }

        
        updateSymbolDisplay() {
            const currentSymbolSpan = document.getElementById('current-symbol');
            if (currentSymbolSpan) {
                currentSymbolSpan.textContent = this.symbol;
            }
            
            // 更新選中狀態
            const symbolDropdown = document.getElementById('symbol-dropdown-menu');
            if (symbolDropdown) {
                symbolDropdown.querySelectorAll('.symbol-option').forEach(option => {
                    if (option.dataset.symbol === this.symbol) {
                        option.classList.add('selected');
                    } else {
                        option.classList.remove('selected');
                    }
                });
            }
        }
        
        forceCloseDropdowns() {
            console.log('強制關閉所有下拉選單...');
            
            // 關閉演算法下拉選單
            const algorithmSelector = document.querySelector('.algorithm-selector');
            if (algorithmSelector) {
                algorithmSelector.classList.remove('active');
                console.log('已關閉演算法下拉選單');
            }
            
            // 關閉品種下拉選單
            const symbolSelector = document.querySelector('.symbol-selector');
            if (symbolSelector) {
                symbolSelector.classList.remove('active');
                console.log('已關閉品種下拉選單');
            }
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
            
            // 顯示波段按鈕
            const showSwingBtn = document.getElementById('show-swing');
            console.log('查找顯示波段按鈕:', showSwingBtn);
            console.log('所有按鈕元素:', document.querySelectorAll('button'));
            
            if (showSwingBtn) {
                console.log('找到顯示波段按鈕，開始設置事件監聽器');
                
                // 清除之前可能存在的事件監聽器
                if (this.swingClickHandler) {
                    showSwingBtn.removeEventListener('click', this.swingClickHandler);
                    console.log('清除舊的事件監聽器');
                }
                
                // 創建綁定的事件處理器
                this.swingClickHandler = async () => {
                    console.log('=== 顯示波段按鈕被點擊 ===');
                    console.log('this 對象:', this);
                    console.log('showSwingBtn 元素:', showSwingBtn);
                    
                    // 添加載入狀態
                    showSwingBtn.textContent = '⏳';
                    showSwingBtn.title = '載入中...';
                    showSwingBtn.disabled = true;
                    
                    try {
                        console.log('開始調用 showSwingData...');
                    await this.showSwingData();
                        console.log('showSwingData 調用完成');
                        
                        // 更新按鈕狀態
                        showSwingBtn.textContent = '📈';
                        showSwingBtn.title = '波段已顯示';
                        showSwingBtn.classList.add('active');
                        
                        // 3秒後恢復原始狀態
                        setTimeout(() => {
                            showSwingBtn.title = '顯示波段';
                            showSwingBtn.classList.remove('active');
                        }, 3000);
                        
                    } catch (error) {
                        console.error('顯示波段失敗:', error);
                        showSwingBtn.textContent = '❌';
                        showSwingBtn.title = '載入失敗';
                        
                        // 2秒後恢復原始狀態
                        setTimeout(() => {
                            showSwingBtn.textContent = '📈';
                            showSwingBtn.title = '顯示波段';
                        }, 2000);
                    } finally {
                        showSwingBtn.disabled = false;
                    }
                };
                
                showSwingBtn.addEventListener('click', this.swingClickHandler);
                console.log('顯示波段按鈕事件監聽器已設置');
                
                // 測試按鈕是否可點擊
                console.log('按鈕可點擊狀態:', !showSwingBtn.disabled);
                console.log('按鈕樣式:', showSwingBtn.style.cssText);
            } else {
                console.error('找不到顯示波段按鈕元素 #show-swing');
                console.error('頁面中的所有按鈕:', Array.from(document.querySelectorAll('button')).map(btn => ({
                    id: btn.id,
                    className: btn.className,
                    textContent: btn.textContent
                })));
            }
            
            // 清除波段按鈕
            const clearSwingBtn = document.getElementById('clear-swing');
            if (clearSwingBtn) {
                // 清除之前可能存在的事件監聽器
                clearSwingBtn.removeEventListener('click', this.clearSwingClickHandler);
                
                // 創建綁定的事件處理器
                this.clearSwingClickHandler = () => {
                    console.log('清除波段按鈕被點擊');
                    
                    // 添加視覺反饋
                    clearSwingBtn.textContent = '🗑️';
                    clearSwingBtn.title = '清除中...';
                    
                    this.clearSwingLines();
                    this.clearSwingStatus();
                    
                    // 清除顯示波段按鈕的活動狀態
                    const showSwingBtn = document.getElementById('show-swing');
                    if (showSwingBtn) {
                        showSwingBtn.classList.remove('active');
                        showSwingBtn.title = '顯示波段';
                    }
                    
                    // 恢復原始狀態
                    setTimeout(() => {
                        clearSwingBtn.title = '清除波段';
                    }, 1000);
                };
                
                clearSwingBtn.addEventListener('click', this.clearSwingClickHandler);
                console.log('清除波段按鈕事件監聽器已設置');
            } else {
                console.error('找不到清除波段按鈕元素 #clear-swing');
            }
            
            // 波段列表按鈕
            const swingListBtn = document.getElementById('swing-list');
            if (swingListBtn) {
                // 清除之前可能存在的事件監聽器
                swingListBtn.removeEventListener('click', this.swingListClickHandler);
                
                // 創建綁定的事件處理器
                this.swingListClickHandler = async () => {
                    console.log('波段列表按鈕被點擊');
                    
                    // 添加載入狀態
                    swingListBtn.textContent = '⏳';
                    swingListBtn.title = '載入中...';
                    swingListBtn.disabled = true;
                    
                    try {
                        await this.showSwingList();
                        
                        // 更新按鈕狀態
                        swingListBtn.textContent = '📋';
                        swingListBtn.title = '波段列表已顯示';
                        
                    } catch (error) {
                        console.error('顯示波段列表失敗:', error);
                        swingListBtn.textContent = '❌';
                        swingListBtn.title = '載入失敗';
                        
                        // 2秒後恢復原始狀態
                        setTimeout(() => {
                            swingListBtn.textContent = '📋';
                            swingListBtn.title = '波段列表';
                        }, 2000);
                    } finally {
                        swingListBtn.disabled = false;
                    }
                };
                
                swingListBtn.addEventListener('click', this.swingListClickHandler);
                console.log('波段列表按鈕事件監聽器已設置');
                            } else {
                    console.error('找不到波段列表按鈕元素 #swing-list');
                }
                

                
                
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
            
            // 設置縮放狀態，避免虛擬化干擾
            this.isZooming = true;
            
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
            } finally {
                // 延遲重置縮放狀態，避免立即觸發虛擬化更新
                setTimeout(() => {
                    this.isZooming = false;
                }, 500);
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
        
        async showSwingData() {
            try {
                console.log('=== 開始載入波段資料 ===');
                console.log('當前品種:', this.symbol);
                console.log('當前時間週期:', this.currentTimeframe);
                console.log('當前演算法:', this.currentAlgorithm);
                console.log('mainChart 存在:', !!this.mainChart);
                
                // 從API獲取波段資料
                const apiUrl = `/api/swing/${this.symbol}/${this.currentTimeframe}?algorithm=${this.currentAlgorithm}`;
                console.log('API URL:', apiUrl);
                
                const response = await fetch(apiUrl);
                console.log('API響應狀態:', response.status);
                
                if (!response.ok) {
                    throw new Error(`API響應錯誤: ${response.status} ${response.statusText}`);
                }
                
                const swingData = await response.json();
                console.log('API響應數據類型:', typeof swingData);
                console.log('API響應數據:', swingData);
                
                if (swingData.error) {
                    console.error('獲取波段資料失敗:', swingData.error);
                    throw new Error(swingData.error);
                }
                
                if (!swingData.data || swingData.data.length === 0) {
                    console.log('沒有找到波段資料');
                    throw new Error('沒有找到波段資料');
                }
                
                console.log('波段資料數量:', swingData.data.length);
                console.log('波段資料樣本:', swingData.data[0]);
                
                // 清除現有的波段線
                console.log('清除現有波段線...');
                this.clearSwingLines();
                
                // 顯示波段點和連接線
                console.log('開始顯示波段點...');
                this.displaySwingPoints(swingData.data);
                
                // 更新狀態欄顯示波段統計
                console.log('更新狀態欄...');
                this.updateSwingStatus(swingData.data);
                
                console.log('=== 波段資料載入完成 ===');
                
            } catch (error) {
                console.error('顯示波段資料失敗:', error);
                console.error('錯誤詳情:', error.message);
                console.error('錯誤堆疊:', error.stack);
                throw error; // 重新拋出錯誤以便按鈕處理
            }
        }
        
        displaySwingPoints(swingData) {
            console.log('displaySwingPoints 被調用');
            console.log('mainChart 存在:', !!this.mainChart);
            console.log('swingData 存在:', !!swingData);
            console.log('swingData 長度:', swingData ? swingData.length : 0);
            
            if (!this.mainChart || !swingData || swingData.length === 0) {
                console.log('條件檢查失敗，退出 displaySwingPoints');
                return;
            }
            
            try {
                console.log('開始處理波段數據...');
                // 按時間排序
                const sortedData = swingData.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
                console.log('排序後的數據數量:', sortedData.length);
                
                // 創建波段點數據
                const swingPoints = sortedData
                    .filter(point => {
                        // 過濾掉沒有有效時間戳的數據點
                        if (!point.timestamp) {
                            console.log('跳過無效時間戳的波段點:', point);
                            return false;
                        }
                        return true;
                    })
                    .map(point => {
                        console.log('處理波段點:', point);
                        
                        // 處理時間戳 - 支持多種格式
                        let timestamp;
                        if (typeof point.timestamp === 'string') {
                            // 處理ISO字符串格式，如 "2003-07-28T00:00:00"
                            timestamp = new Date(point.timestamp);
                            console.log('字符串時間戳:', point.timestamp, '轉換為:', timestamp);
                        } else if (typeof point.timestamp === 'number') {
                            timestamp = new Date(point.timestamp * 1000); // 如果是Unix時間戳
                            console.log('數字時間戳:', point.timestamp, '轉換為:', timestamp);
                        } else {
                            // 如果是其他格式，嘗試直接轉換
                            timestamp = new Date(point.timestamp);
                            console.log('其他格式時間戳:', point.timestamp, '轉換為:', timestamp);
                        }
                        
                        // 驗證時間戳是否有效
                        if (isNaN(timestamp.getTime())) {
                            console.log('無效時間戳，跳過:', point.timestamp);
                            return null;
                        }
                        
                        const timeValue = Math.floor(timestamp.getTime() / 1000);
                        const priceValue = parseFloat(point.zigzag_price);
                        
                        console.log('最終波段點:', {
                            time: timeValue,
                            value: priceValue,
                            type: point.zigzag_type,
                            originalTimestamp: point.timestamp
                        });
                        
                        return {
                            time: timeValue,
                            value: priceValue,
                            type: point.zigzag_type
                        };
                    })
                    .filter(point => point !== null); // 過濾掉無效的數據點
                
                // 添加波段點標記 - 使用藍色虛線來創建明顯的標記點
                swingPoints.forEach((point, index) => {
                    console.log(`添加波段點 ${index + 1}: ${point.type} at ${point.time} (${new Date(point.time * 1000)})`);
                    
                    // 使用線條系列來創建明顯的藍色虛線標記點
                    const markerSeries = this.mainChart.addLineSeries({
                        color: '#0066ff', // 藍色
                        lineWidth: 4, // 適中的線條寬度
                        lineStyle: LightweightCharts.LineStyle.Dashed, // 虛線
                        crosshairMarkerVisible: false,
                        lastValueVisible: false,
                        priceLineVisible: false,
                        pointMarkersVisible: true, // 顯示點標記
                        pointMarkersSize: 8, // 點標記大小
                    });
                    
                    // 創建標記點數據 - 使用兩個點來創建一個可見的線段
                    const markerData = [
                        { time: point.time - 1, value: point.value }, // 前一個時間點
                        { time: point.time, value: point.value },     // 當前時間點
                        { time: point.time + 1, value: point.value }  // 後一個時間點
                    ];
                    
                    console.log(`設置標記數據:`, markerData);
                    markerSeries.setData(markerData);
                    
                    // 添加標記到波段點列表
                    this.swingPoints.push(markerSeries);
                    console.log(`波段點 ${index + 1} 已添加到圖表`);
                });
                
                // 創建連接線
                for (let i = 0; i < swingPoints.length - 1; i++) {
                    const currentPoint = swingPoints[i];
                    const nextPoint = swingPoints[i + 1];
                    
                    console.log(`添加連接線 ${i + 1}: ${currentPoint.time} -> ${nextPoint.time}`);
                    
                    // 創建連接線系列 - 使用藍色虛線
                    const lineSeries = this.mainChart.addLineSeries({
                        color: '#0066ff', // 藍色
                        lineWidth: 3, // 適中的線條寬度
                        lineStyle: LightweightCharts.LineStyle.Dashed, // 虛線
                        crosshairMarkerVisible: false,
                        lastValueVisible: false,
                        priceLineVisible: false,
                    });
                    
                    // 設置連接線數據
                    const lineData = [
                        { time: currentPoint.time, value: currentPoint.value },
                        { time: nextPoint.time, value: nextPoint.value }
                    ];
                    
                    lineSeries.setData(lineData);
                    
                    // 添加連接線到波段線列表
                    this.swingLines.push(lineSeries);
                    console.log(`連接線 ${i + 1} 已添加到圖表`);
                }
                
                console.log(`成功顯示 ${swingPoints.length} 個波段點和 ${this.swingLines.length} 條連接線`);
                
            } catch (error) {
                console.error('顯示波段點失敗:', error);
            }
        }
        
        getSwingLineColor(type1, type2) {
            // 根據波段方向決定顏色
            if (type1 === 'low' && type2 === 'high') {
                return '#00cc00'; // 上升波段 - 綠色
            } else if (type1 === 'high' && type2 === 'low') {
                return '#ff4444'; // 下降波段 - 紅色
            } else {
                return '#888888'; // 默認灰色
            }
        }
        
        clearSwingLines() {
            // 清除所有波段點
            this.swingPoints.forEach(point => {
                if (point && this.mainChart) {
                    try {
                        this.mainChart.removeSeries(point);
                    } catch (e) {
                        console.warn('Error removing swing point:', e);
                    }
                }
            });
            this.swingPoints = [];
            
            // 清除所有波段連接線
            this.swingLines.forEach(line => {
                if (line && this.mainChart) {
                    try {
                        this.mainChart.removeSeries(line);
                    } catch (e) {
                        console.warn('Error removing swing line:', e);
                    }
                }
            });
            this.swingLines = [];
            
            // 清除狀態欄的波段信息
            this.clearSwingStatus();
            
            console.log('已清除所有波段顯示');
        }
        
        updateSwingStatus(swingData) {
            if (!swingData || swingData.length === 0) return;
            
            const highPoints = swingData.filter(point => point.zigzag_type === 'high').length;
            const lowPoints = swingData.filter(point => point.zigzag_type === 'low').length;
            const totalSwings = swingData.length;
            
            // 更新狀態欄
            const statusRight = document.querySelector('.status-right');
            if (statusRight) {
                const swingInfo = document.getElementById('swing-info');
                if (!swingInfo) {
                    // 創建波段信息元素
                    const swingInfoElement = document.createElement('span');
                    swingInfoElement.id = 'swing-info';
                    swingInfoElement.innerHTML = `波段: ${totalSwings}個 (高:${highPoints} 低:${lowPoints})`;
                    swingInfoElement.style.color = '#00cc00';
                    swingInfoElement.style.fontWeight = 'bold';
                    
                    // 插入到狀態欄
                    const separator = document.createElement('span');
                    separator.className = 'separator';
                    separator.textContent = '|';
                    
                    statusRight.insertBefore(separator, statusRight.firstChild);
                    statusRight.insertBefore(swingInfoElement, statusRight.firstChild);
                } else {
                    swingInfo.innerHTML = `波段: ${totalSwings}個 (高:${highPoints} 低:${lowPoints})`;
                }
            }
        }
        
        clearSwingStatus() {
            const swingInfo = document.getElementById('swing-info');
            if (swingInfo) {
                const separator = swingInfo.previousElementSibling;
                if (separator && separator.className === 'separator') {
                    separator.remove();
                }
                swingInfo.remove();
            }
        }
        
        async showSwingList() {
            try {
                console.log('=== 開始載入波段列表 ===');
                console.log('當前品種:', this.symbol);
                console.log('當前時間週期:', this.currentTimeframe);
                console.log('當前演算法:', this.currentAlgorithm);
                
                // 從API獲取波段資料
                const apiUrl = `/api/swing/${this.symbol}/${this.currentTimeframe}?algorithm=${this.currentAlgorithm}`;
                console.log('API URL:', apiUrl);
                
                const response = await fetch(apiUrl);
                console.log('API響應狀態:', response.status);
                
                if (!response.ok) {
                    throw new Error(`API響應錯誤: ${response.status} ${response.statusText}`);
                }
                
                const swingData = await response.json();
                console.log('API響應數據:', swingData);
                
                if (swingData.error) {
                    console.error('獲取波段資料失敗:', swingData.error);
                    throw new Error(swingData.error);
                }
                
                if (!swingData.data || swingData.data.length === 0) {
                    console.log('沒有找到波段資料');
                    throw new Error('沒有找到波段資料');
                }
                
                console.log('波段資料數量:', swingData.data.length);
                
                // 生成波段列表
                this.generateSwingList(swingData.data);
                
                // 顯示彈出視窗
                this.showSwingListPopup();
                
                console.log('=== 波段列表載入完成 ===');
                
            } catch (error) {
                console.error('顯示波段列表失敗:', error);
                console.error('錯誤詳情:', error.message);
                console.error('錯誤堆疊:', error.stack);
                throw error;
            }
        }
        
        generateSwingList(swingData) {
            console.log('生成波段列表...');
            
            // 過濾掉無效時間戳的數據
            const validData = swingData.filter(point => {
                if (!point.timestamp) {
                    console.log('跳過無效時間戳的波段點:', point);
                    return false;
                }
                const timestamp = new Date(point.timestamp);
                if (isNaN(timestamp.getTime())) {
                    console.log('跳過無效時間戳格式:', point.timestamp);
                    return false;
                }
                return true;
            });
            
            console.log(`有效數據數量: ${validData.length} / ${swingData.length}`);
            
            // 按時間排序
            const sortedData = validData.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
            console.log('排序後的數據數量:', sortedData.length);
            
            // 生成波段對
            const swingPairs = [];
            for (let i = 0; i < sortedData.length - 1; i++) {
                const currentPoint = sortedData[i];
                const nextPoint = sortedData[i + 1];
                
                // 計算價差
                const priceDiff = parseFloat(nextPoint.zigzag_price) - parseFloat(currentPoint.zigzag_price);
                
                // 計算時差
                const currentTime = new Date(currentPoint.timestamp);
                const nextTime = new Date(nextPoint.timestamp);
                const timeDiffMs = nextTime.getTime() - currentTime.getTime();
                const timeDiffDays = Math.floor(timeDiffMs / (1000 * 60 * 60 * 24));
                
                swingPairs.push({
                    startTime: currentPoint.timestamp,
                    startPrice: parseFloat(currentPoint.zigzag_price),
                    endTime: nextPoint.timestamp,
                    endPrice: parseFloat(nextPoint.zigzag_price),
                    priceDiff: priceDiff,
                    timeDiff: timeDiffDays,
                    startType: currentPoint.zigzag_type,
                    endType: nextPoint.zigzag_type
                });
            }
            
            console.log('生成的波段對數量:', swingPairs.length);
            
            // 更新HTML
            const swingListBody = document.getElementById('swing-list-body');
            if (swingListBody) {
                swingListBody.innerHTML = '';
                
                swingPairs.forEach((pair, index) => {
                    const row = document.createElement('div');
                    row.className = 'swing-list-row';
                    
                    // 格式化時間
                    const startTimeFormatted = new Date(pair.startTime).toLocaleDateString('zh-TW');
                    const endTimeFormatted = new Date(pair.endTime).toLocaleDateString('zh-TW');
                    
                    // 格式化價格
                    const startPriceFormatted = pair.startPrice.toFixed(2);
                    const endPriceFormatted = pair.endPrice.toFixed(2);
                    
                    // 格式化價差
                    const priceDiffFormatted = pair.priceDiff > 0 ? 
                        `+${pair.priceDiff.toFixed(2)}` : 
                        pair.priceDiff.toFixed(2);
                    
                    // 格式化時差
                    const timeDiffFormatted = `${pair.timeDiff}天`;
                    
                    row.innerHTML = `
                        <div class="swing-list-cell">${startTimeFormatted}</div>
                        <div class="swing-list-cell">${startPriceFormatted}</div>
                        <div class="swing-list-cell">${endTimeFormatted}</div>
                        <div class="swing-list-cell">${endPriceFormatted}</div>
                        <div class="swing-list-cell">${priceDiffFormatted}</div>
                        <div class="swing-list-cell">${timeDiffFormatted}</div>
                    `;
                    
                    swingListBody.appendChild(row);
                });
                
                console.log('波段列表HTML已更新');
            } else {
                console.error('找不到波段列表主體元素 #swing-list-body');
            }
        }
        
        showSwingListPopup() {
            const popup = document.getElementById('swing-list-popup');
            if (popup) {
                popup.style.display = 'block';
                console.log('波段列表彈出視窗已顯示');
            } else {
                console.error('找不到波段列表彈出視窗元素 #swing-list-popup');
            }
        }
        
        hideSwingListPopup() {
            const popup = document.getElementById('swing-list-popup');
            if (popup) {
                popup.style.display = 'none';
                console.log('波段列表彈出視窗已隱藏');
            }
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
        
        // 立即關閉所有下拉選單
        function closeAllDropdowns() {
            const algorithmSelectors = document.querySelectorAll('.algorithm-selector');
            const symbolSelectors = document.querySelectorAll('.symbol-selector');
            
            algorithmSelectors.forEach(selector => {
                selector.classList.remove('active');
            });
            
            symbolSelectors.forEach(selector => {
                selector.classList.remove('active');
            });
            
            console.log('已關閉所有下拉選單');
        }
        
        // 立即執行一次
        closeAllDropdowns();
        
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
    
    // 關閉波段列表彈出視窗
    function closeSwingListPopup() {
        const popup = document.getElementById('swing-list-popup');
        if (popup) {
            popup.style.display = 'none';
            console.log('波段列表彈出視窗已關閉');
        }
    }

console.log('=== Swing Chart v3.0 載入 ===');

class SwingChart {
    constructor() {
        console.log('SwingChart 構造函數開始');
        
        // 初始化狀態
        this.chart = null;
        this.candlestickSeries = null;
        this.algoParamsState = {
            deviation: 12,
            depth: 12,
            backstep: 3
        };
        
        this.initializeChart();
        this.setupEventListeners();
        
        console.log('SwingChart 構造函數完成');
    }

    initializeChart() {
        console.log('開始初始化圖表...');
        
        const chartContainer = document.getElementById('main-chart');
        if (!chartContainer) {
            console.error('找不到圖表容器');
            return;
        }

        try {
            // 創建圖表 - 你要求的設置
            this.chart = LightweightCharts.createChart(chartContainer, {
                width: chartContainer.clientWidth,
                height: chartContainer.clientHeight || 400,
                layout: {
                    backgroundColor: '#1e1e1e',
                    textColor: '#d1d4dc',
                },
                grid: {
                    vertLines: { color: '#2B2B43' },
                    horzLines: { color: '#2B2B43' }
                },
                timeScale: {
                    timeVisible: true,
                    secondsVisible: false,
                    rightOffset: 0, // 你要求的：移除右邊距
                },
                crosshair: {
                    mode: LightweightCharts.CrosshairMode.Normal,
                },
            });

            // 創建K線系列
            this.candlestickSeries = this.chart.addCandlestickSeries({
                upColor: '#26a69a',
                downColor: '#ef5350',
                borderVisible: false,
                wickUpColor: '#26a69a',
                wickDownColor: '#ef5350',
            });

            console.log('圖表創建成功');

            // 載入測試數據
            this.loadTestData();
            
            // 你要求的：縮小3個階段的縮放
            setTimeout(() => {
                const zoom = Math.pow(1.2, 3); // 縮小3階段
                this.chart.timeScale().fitContent();
                console.log('縮放調整完成');
            }, 100);

        } catch (error) {
            console.error('圖表初始化錯誤:', error);
        }
    }

    loadTestData() {
        const testData = [
            { time: '2024-01-01', open: 2000, high: 2050, low: 1980, close: 2020 },
            { time: '2024-01-02', open: 2020, high: 2070, low: 2010, close: 2060 },
            { time: '2024-01-03', open: 2060, high: 2080, low: 2040, close: 2070 },
            { time: '2024-01-04', open: 2070, high: 2090, low: 2050, close: 2080 },
            { time: '2024-01-05', open: 2080, high: 2100, low: 2060, close: 2090 },
            { time: '2024-01-06', open: 2090, high: 2110, low: 2070, close: 2100 },
            { time: '2024-01-07', open: 2100, high: 2120, low: 2080, close: 2110 },
            { time: '2024-01-08', open: 2110, high: 2130, low: 2090, close: 2120 },
        ];

        this.candlestickSeries.setData(testData);
        
        // 你要求的：定位到最後一根K棒
        setTimeout(() => {
            this.chart.timeScale().scrollToRealTime();
        }, 200);
        
        console.log('測試數據載入完成');
    }

    setupEventListeners() {
        console.log('設置事件監聽器...');
        
        // 你要求的：參數按鈕事件
        const paramsBtn = document.getElementById('swing-params');
        if (paramsBtn) {
            paramsBtn.addEventListener('click', () => {
                console.log('參數按鈕被點擊');
                this.showParamsPopup();
            });
        }

        // 其他按鈕事件可以在這裡添加
        console.log('事件監聽器設置完成');
    }

    // 你要求的：參數設置彈出視窗
    showParamsPopup() {
        console.log('顯示參數設置彈出視窗');
        
        const popup = document.getElementById('params-popup');
        const form = document.getElementById('params-form');
        
        if (!popup || !form) {
            console.error('找不到參數彈出視窗元素');
            return;
        }

        // 清空表單
        form.innerHTML = '';

        // 創建ZigZag參數
        const params = [
            {name: 'deviation', label: 'Deviation', value: this.algoParamsState.deviation, description: '偏差值，控制波段的敏感度'},
            {name: 'depth', label: 'Depth', value: this.algoParamsState.depth, description: '深度值，影響波段識別的範圍'},
            {name: 'backstep', label: 'Backstep', value: this.algoParamsState.backstep, description: '回退步數，防止波段過於頻繁'}
        ];

        params.forEach(param => {
            const group = document.createElement('div');
            group.className = 'params-form-group';
            
            const label = document.createElement('label');
            label.textContent = param.label;
            
            const input = document.createElement('input');
            input.type = 'number';
            input.id = `param-${param.name}`;
            input.value = param.value;
            input.min = '1';
            input.step = '1';
            
            const desc = document.createElement('div');
            desc.className = 'param-description';
            desc.textContent = param.description;
            
            group.appendChild(label);
            group.appendChild(input);
            group.appendChild(desc);
            
            form.appendChild(group);
        });

        // 顯示彈出視窗
        popup.style.display = 'block';
        console.log('參數彈出視窗已顯示');
    }
}

// 你要求的：參數彈窗相關函數
function closeParamsPopup() {
    const popup = document.getElementById('params-popup');
    if (popup) {
        popup.style.display = 'none';
        console.log('參數彈出視窗已關閉');
    }
}

function confirmParams() {
    console.log('確認參數設置');
    
    if (window.swingChart) {
        // 收集參數值
        const form = document.getElementById('params-form');
        const inputs = form.querySelectorAll('input');
        
        inputs.forEach(input => {
            const paramName = input.id.replace('param-', '');
            const value = parseInt(input.value);
            window.swingChart.algoParamsState[paramName] = value;
        });
        
        console.log('參數已更新:', window.swingChart.algoParamsState);
    }
    
    closeParamsPopup();
}

// 其他必要的函數
function closeMeasurementPopup() {
    const popup = document.getElementById('measurement-popup');
    if (popup) popup.style.display = 'none';
}

function clearMeasurementAndClose() {
    closeMeasurementPopup();
}

function keepMeasurementAndClose() {
    closeMeasurementPopup();
}

function closeSwingListPopup() {
    const popup = document.getElementById('swing-list-popup');
    if (popup) popup.style.display = 'none';
}

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM 載入完成，開始創建 SwingChart...');
    
    try {
        window.swingChart = new SwingChart();
        console.log('SwingChart 創建成功');
        
        // 更新狀態
        const statusElement = document.getElementById('generation-status');
        if (statusElement) {
            statusElement.textContent = '就緒';
        }
        
    } catch (error) {
        console.error('創建 SwingChart 時發生錯誤:', error);
    }
});

console.log('swing.js v3.0 載入完成');
    constructor() {
        this.mainChart = null;
        this.candlestickSeries = null;
        this.swingLineSeries = null;
        this.measurementLineSeries = null;
        this.currentTimeframe = 'D1';
        this.symbol = 'XAUUSD';
        this.currentAlgorithm = 'zigzag';
        this.algoSpec = null;
        this.algoParamsState = {};
        this.algorithmsIndex = [];
        this.measurementMode = false;
        this.measurementPoints = [];
        this.measurementLines = [];
        this.swingLines = [];
        this.swingVisible = false;
        this.swingTask = { ws: null, taskId: null, running: false };
        this.dataCount = 0;
        this.config = {};
        this.lastLoadTime = 0;
        this.loadingPromise = null;
        
        console.log('SwingChart constructor called');
        this.init();
    }
    
    async init() {
        console.log('Initializing Swing Chart...');
        try {
            // 載入配置和演算法列表
            await this.loadConfig();
            
            // 等待DOM完全載入
            setTimeout(() => {
                // 初始化圖表
                this.initChart();
                
                // 設置事件監聽器
                this.setupEventListeners();
                
                // 載入初始數據
                this.loadChart();
                
                // 設置UI引用
                this._setupSwingTaskUIRefs();
                
                // 確保下拉選單初始狀態是關閉的
                this.forceCloseDropdowns();
                
                // 更新時間顯示
                this.updateTimeDisplay();
                setInterval(() => this.updateTimeDisplay(), 1000);
                
                console.log('Swing Chart initialized successfully');
            }, 200);
            
        } catch (error) {
            console.error('Failed to initialize chart:', error);
        }
    }
    
    async loadConfig() {
        try {
            console.log('Loading config...');
            const response = await fetch('/api/config');
            this.config = await response.json();
            this.symbol = this.config.symbol || 'XAUUSD';
            console.log('Config loaded:', this.config);
            // 同步載入演算法清單
            await this.loadAlgorithmsIndex();
        } catch (error) {
            console.error('載入配置失敗:', error);
        }
    }

    async loadAlgorithmsIndex() {
        try {
            const resp = await fetch('/algorithms/index.json');
            const list = await resp.json();
            this.algorithmsIndex = Array.isArray(list) ? list : [];
            // 預設選第一個
            if (this.algorithmsIndex.length && !this.algoSpec) {
                this.algoSpec = this.algorithmsIndex[0];
                this.currentAlgorithm = this.algoSpec.value;
                this.algoParamsState = this._defaultParamsFromSpec(this.algoSpec);
            }
            this.renderAlgorithmSelectorDynamic();
        } catch (e) {
            console.warn('載入演算法清單失敗，使用舊有下拉:', e);
        }
    }

    _defaultParamsFromSpec(spec) {
        const state = {};
        if (spec && Array.isArray(spec.params)) {
            spec.params.forEach(p => {
                state[p.name] = p.default;
            });
        }
        return state;
    }
    
    _setupSwingTaskUIRefs() {
        this.$progress = document.getElementById('swing-progress');
        this.$progressFill = document.getElementById('swing-progress-fill');
        this.$progressText = document.getElementById('swing-progress-text');
        this.$log = document.getElementById('swing-log');
        // 初始化為隱藏
        if (this.$progress) this.$progress.style.display = 'none';
        if (this.$log) this.$log.style.display = 'none';
    }
    
    forceCloseDropdowns() {
        console.log('強制關閉所有下拉選單...');
        
        // 關閉演算法下拉選單
        const algorithmSelector = document.querySelector('.algorithm-selector');
        if (algorithmSelector) {
            algorithmSelector.classList.remove('active');
        }
        
        // 關閉品種下拉選單
        const symbolSelector = document.querySelector('.symbol-selector');
        if (symbolSelector) {
            symbolSelector.classList.remove('active');
        }
    }
    
    initChart() {
        const container = document.getElementById('main-chart');
        if (!container) {
            console.error('Chart container not found');
            return;
        }
        
        // 創建圖表
        this.mainChart = LightweightCharts.createChart(container, {
            width: container.clientWidth,
            height: container.clientHeight,
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
                rightOffset: 0,
                barSpacing: 3,
                minBarSpacing: 1,
                rightBarStaysOnScroll: true,
            },
            handleScroll: {
                mouseWheel: false,
                pressedMouseMove: true,
                horzTouchDrag: true,
                vertTouchDrag: true,
            },
            handleScale: {
                axisPressedMouseMove: true,
                mouseWheel: false,
                pinch: true,
                axisDoubleClickReset: true,
            },
            watermark: {
                visible: false,
            },
            overlayPriceScales: {
                borderVisible: false,
            },
        });
        
        // 創建K線系列
        this.candlestickSeries = this.mainChart.addCandlestickSeries({
            upColor: '#00cc00',
            downColor: '#ff4444',
            borderVisible: false,
            wickUpColor: '#00cc00',
            wickDownColor: '#ff4444',
        });
        
        // 創建波段線系列
        this.swingLineSeries = this.mainChart.addLineSeries({
            color: '#ff4444',
            lineWidth: 2,
            visible: false,
        });
        
        // 創建測量線系列
        this.measurementLineSeries = this.mainChart.addLineSeries({
            color: '#ffffff',
            lineWidth: 2,
            lineStyle: LightweightCharts.LineStyle.Dashed,
            crosshairMarkerVisible: false,
            lastValueVisible: false,
            priceLineVisible: false,
        });
        
        // 監聽圖表點擊事件
        this.mainChart.subscribeClick((param) => {
            console.log('圖表被點擊！param:', param);
            console.log('測量模式狀態:', this.measurementMode);
            
            if (this.measurementMode) {
                console.log('測量模式已啟動，處理測量點擊');
                this.handleMeasurementClick(param);
            } else {
                console.log('測量模式未啟動，忽略點擊');
            }
        });
        
        // 監聽十字線移動
        this.mainChart.subscribeCrosshairMove((param) => {
            this.updatePriceInfo(param);
        });
        
        // 處理窗口大小變化
        window.addEventListener('resize', () => {
            this.mainChart.applyOptions({
                width: container.clientWidth,
                height: container.clientHeight,
            });
        });
        
        // 添加自定義滾輪事件處理器
        container.addEventListener('wheel', (event) => {
            event.preventDefault();
            
            if (this.mainChart) {
                const timeScale = this.mainChart.timeScale();
                const logicalRange = timeScale.getVisibleLogicalRange();
                
                if (logicalRange) {
                    const visibleRange = logicalRange.to - logicalRange.from;
                    const moveDistance = Math.max(1, Math.floor(visibleRange * 0.1));
                    const direction = event.deltaY > 0 ? -1 : 1;
                    
                    const newFrom = Math.max(0, logicalRange.from + (direction * moveDistance));
                    const newTo = logicalRange.to + (direction * moveDistance);
                    
                    timeScale.setVisibleLogicalRange({
                        from: newFrom,
                        to: newTo
                    });
                }
            }
        });
        
        // 添加滑鼠中鍵事件支持
        container.addEventListener('mousedown', (event) => {
            if (event.button === 1) {
                event.preventDefault();
                this.toggleMeasurementMode();
            }
        });
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
        // 舊實作保留，但會被動態渲染覆蓋
        this.renderAlgorithmSelectorDynamic();
    }

    renderAlgorithmSelectorDynamic() {
        const container = document.querySelector('.algorithm-selector');
        if (!container) return;
        const btn = container.querySelector('#algorithm-dropdown');
        const menu = container.querySelector('#algorithm-dropdown-menu');
        const labelSpan = container.querySelector('#current-algorithm');
        if (!btn || !menu || !labelSpan) return;

        // 以原有 dropdown 容器，清空並注入動態內容
        menu.innerHTML = '';
        (this.algorithmsIndex || []).forEach(spec => {
            const div = document.createElement('div');
            div.className = 'algorithm-option';
            div.dataset.algorithm = spec.value;
            div.textContent = spec.label || spec.value;
            if (this.algoSpec && this.algoSpec.value === spec.value) {
                div.classList.add('selected');
            }
            div.addEventListener('click', () => {
                this.algoSpec = spec;
                this.currentAlgorithm = spec.value;
                this.algoParamsState = this._defaultParamsFromSpec(spec);
                this.renderAlgorithmSelectorDynamic();
            });
            menu.appendChild(div);
        });

        // 標籤
        labelSpan.textContent = (this.algoSpec && this.algoSpec.label) ? this.algoSpec.label : (this.currentAlgorithm || 'ZigZag');

        // 參數區塊（選單下方）
        let paramsHost = container.querySelector('.algorithm-params');
        if (!paramsHost) {
            paramsHost = document.createElement('div');
            paramsHost.className = 'algorithm-params';
            paramsHost.style.marginTop = '6px';
            paramsHost.style.display = 'grid';
            paramsHost.style.gridTemplateColumns = 'repeat(auto-fit, minmax(120px, 1fr))';
            paramsHost.style.gap = '6px';
            container.appendChild(paramsHost);
        }
        paramsHost.innerHTML = '';
        if (this.algoSpec && Array.isArray(this.algoSpec.params) && this.algoSpec.params.length) {
            this.algoSpec.params.forEach(p => {
                const wrap = document.createElement('label');
                wrap.style.color = '#ddd';
                wrap.style.fontSize = '12px';
                wrap.textContent = p.label || p.name;
                const input = document.createElement('input');
                input.type = p.type === 'number' ? 'number' : 'text';
                if (p.step != null) input.step = String(p.step);
                if (p.min != null) input.min = String(p.min);
                input.value = this.algoParamsState[p.name] ?? p.default ?? '';
                input.style.marginLeft = '6px';
                input.addEventListener('input', () => {
                    const v = input.type === 'number' ? Number(input.value) : input.value;
                    this.algoParamsState[p.name] = v;
                });
                wrap.appendChild(input);
                paramsHost.appendChild(wrap);
            });
            paramsHost.style.display = 'grid';
        } else {
            paramsHost.style.display = 'none';
        }

        // 點擊開關下拉
        btn.onclick = (e) => {
            e.stopPropagation();
            container.classList.toggle('active');
        };
    }
    
    async switchAlgorithm(newAlgorithm) {
        console.log('切換演算法:', newAlgorithm);
        // 兼容: 若從舊的 dataset.algorithm 來源
        this.currentAlgorithm = newAlgorithm;
        // 嘗試從列表中找到 spec
        if (this.algorithmsIndex) {
            const found = this.algorithmsIndex.find(x => x.value === newAlgorithm);
            if (found) {
                this.algoSpec = found;
                this.algoParamsState = this._defaultParamsFromSpec(found);
            }
        }
        this.renderAlgorithmSelectorDynamic();
        
        // 清除現有的波段顯示
        this.clearSwingLines();
        
        // 顯示提示信息
        console.log(`已切換到 ${newAlgorithm} 演算法，請點擊"顯示波段"按鈕查看新的波段分析`);
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
    
    clearSwingLines() {
        // 清除波段線
        if (this.swingLineSeries) {
            this.swingLineSeries.setData([]);
        }
        this.swingLines = [];
    }
    
    setupToolButtons() {
        console.log('正在設置工具按鈕...');
        
        // 生成波段按鈕
        const generateBtn = document.getElementById('generate-swing');
        if (generateBtn) {
            generateBtn.addEventListener('click', async () => {
                console.log('生成波段按鈕被點擊');
                
                // 檢查日期範圍
                const startDate = document.getElementById('start-date').value;
                const endDate = document.getElementById('end-date').value;
                
                if (!startDate || !endDate) {
                    this.showNotification('請選擇日期範圍', 'warning');
                    return;
                }
                
                if (new Date(startDate) >= new Date(endDate)) {
                    this.showNotification('結束日期必須晚於開始日期', 'error');
                    return;
                }
                
                // 添加載入狀態
                generateBtn.textContent = '⏳';
                generateBtn.disabled = true;
                
                try {
                    await this.startSwingGeneration();
                    
                    // 更新按鈕狀態
                    generateBtn.textContent = '✅';
                    
                    // 3秒後恢復原始狀態
                    setTimeout(() => {
                        generateBtn.textContent = '🚀';
                    }, 3000);
                    
                } catch (error) {
                    console.error('生成波段失敗:', error);
                    generateBtn.textContent = '❌ 生成失敗';
                    
                    // 2秒後恢復原始狀態
                    setTimeout(() => {
                        generateBtn.textContent = '🚀';
                    }, 2000);
                } finally {
                    generateBtn.disabled = false;
                }
            });
        }
        
        // 顯示/隱藏波段按鈕
        const showSwingBtn = document.getElementById('show-swing');
        if (showSwingBtn) {
            showSwingBtn.addEventListener('click', () => {
                this.toggleSwingVisibility();
            });
        }
        
        // 參數設置按鈕
        const paramsBtn = document.getElementById('swing-params');
        if (paramsBtn) {
            paramsBtn.addEventListener('click', () => {
                this.showParamsPopup();
            });
        }
        
        // 清除波段按鈕
        const clearSwingBtn = document.getElementById('clear-swing');
        if (clearSwingBtn) {
            clearSwingBtn.addEventListener('click', () => {
                this.clearSwings();
                this.clearSwingStatus();
                this.stopSwingGeneration();
            });
        }
        
        // 測量工具按鈕
        const measureBtn = document.getElementById('measure');
        if (measureBtn) {
            measureBtn.addEventListener('click', () => {
                this.toggleMeasurementMode();
            });
        }
        
        // 縮放控制按鈕
        const zoomInBtn = document.getElementById('zoom-in');
        const zoomOutBtn = document.getElementById('zoom-out');
        const zoomResetBtn = document.getElementById('zoom-reset');
        
        if (zoomInBtn) zoomInBtn.addEventListener('click', () => this.performZoom(0.8));
        if (zoomOutBtn) zoomOutBtn.addEventListener('click', () => this.performZoom(1.25));
        if (zoomResetBtn) zoomResetBtn.addEventListener('click', () => this.resetChart());
        
        // 十字線按鈕
        const crosshairBtn = document.getElementById('crosshair');
        if (crosshairBtn) {
            crosshairBtn.addEventListener('click', () => {
                this.toggleCrosshair(crosshairBtn);
            });
        }

        // 波段列表按鈕
        const swingListBtn = document.getElementById('swing-list');
        if (swingListBtn) {
            swingListBtn.addEventListener('click', async () => {
                console.log('波段列表按鈕被點擊');
                
                // 添加載入狀態
                const originalText = swingListBtn.textContent;
                const originalTitle = swingListBtn.title;
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
                        swingListBtn.textContent = originalText;
                        swingListBtn.title = originalTitle;
                    }, 2000);
                } finally {
                    swingListBtn.disabled = false;
                }
            });
        }

        // 全屏按鈕
        const fullscreenBtn = document.getElementById('fullscreen');
        if (fullscreenBtn) {
            fullscreenBtn.addEventListener('click', () => {
                this.toggleFullscreen();
            });
        }

        // 清除日期按鈕
        const clearDatesBtn = document.getElementById('clear-dates');
        if (clearDatesBtn) {
            clearDatesBtn.addEventListener('click', () => {
                document.getElementById('start-date').value = '';
                document.getElementById('end-date').value = '';
                this.showNotification('日期範圍已清除', 'success');
            });
        }
    }
    
    async loadChart(timeframe = 'D1') {
        this.currentTimeframe = timeframe;
        
        try {
            console.log(`載入 ${this.symbol} ${timeframe} 數據...`);
            
            // 顯示載入狀態
            this.updateStatusBar('載入中...', '正在獲取數據');
            
            const response = await fetch(`/api/candlestick/${this.symbol}/${timeframe}?limit=999999`);
            
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

            // 檢查圖表是否已初始化
            if (!this.candlestickSeries) {
                console.error('圖表系列未正確初始化');
                this.initChart(); // 嘗試重新初始化
                if (!this.candlestickSeries) {
                    console.error('重新初始化失敗');
                    return;
                }
            }

            // 處理數據
            const candlestickData = this.processDataBatchFast(data.data);

            console.log(`處理完成: ${candlestickData.length} 條有效數據`);

            // 確保蠟燭圖數據按時間排序
            candlestickData.sort((a, b) => a.time - b.time);

            if (candlestickData.length === 0) {
                console.error('沒有有效的蠟燭圖數據');
                return;
            }

            // 設置數據
            try {
                console.log('設置蠟燭圖數據...');
                this.candlestickSeries.setData(candlestickData);
                
                // 記錄數據總數
                this.dataCount = candlestickData.length;
                console.log('數據總數已記錄:', this.dataCount);
                
                // 自動縮放到適當比例
                setTimeout(() => {
                    this.mainChart.timeScale().fitContent();
                    console.log('初始自動縮放完成');
                    
                    // 縮小3個階段，並定位到最後一根K棒
                    setTimeout(() => {
                        const timeScale = this.mainChart.timeScale();
                        const logicalRange = timeScale.getVisibleLogicalRange();
                        
                        if (logicalRange && this.dataCount > 0) {
                            // 縮小3個階段 (使用1.2的3次方來縮小)
                            const currentTimeRange = logicalRange.to - logicalRange.from;
                            const zoomedTimeRange = currentTimeRange * Math.pow(1.2, 3);
                            
                            // 設定右邊界為最後一根K棒，不留白
                            const rightBoundary = this.dataCount - 1;
                            const leftBoundary = Math.max(0, rightBoundary - zoomedTimeRange);
                            
                            const newLogicalRange = {
                                from: leftBoundary,
                                to: rightBoundary
                            };
                            
                            timeScale.setVisibleLogicalRange(newLogicalRange);
                            console.log('縮小3個階段並定位到最後一根K棒完成');
                        }
                    }, 300);
                }, 100);
                
                // 更新最新數據顯示
                if (candlestickData.length > 0) {
                    const latestData = candlestickData[candlestickData.length - 1];
                    this.updateLatestInfo({
                        timestamp: new Date(latestData.time * 1000).toISOString(),
                        close: latestData.close
                    });
                }
                
                // 更新數據計數顯示
                this.updateDataCount();
                
                // 更新圖表標題
                this.updateChartTitle();
                
                console.log('圖表數據載入完成');
            } catch (error) {
                console.error('設置圖表數據時發生錯誤:', error);
            }
                
        } catch (error) {
            console.error('載入圖表數據失敗:', error);
        }
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
        
        console.log('最新數據已更新:', close);
    }
    
    updateStatusBar(left, right) {
        // 更新狀態欄顯示
        const statusLeft = document.getElementById('connection-status');
        const statusRight = document.getElementById('data-count');
        
        if (statusLeft && left) statusLeft.textContent = left;
        if (statusRight && right) statusRight.textContent = right;
    }
    
    // Swing generation lifecycle
    async startSwingGeneration() {
        if (this.swingTask.running) {
            return;
        }
        this._showProgress(0);
        this._appendLog('[ui] start swing generation');

        // 獲取日期範圍
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;

        const payload = {
            symbol: this.symbol,
            timeframe: this.currentTimeframe || 'D1',
            algo: (this.algoSpec && this.algoSpec.value) ? this.algoSpec.value : (this.currentAlgorithm || 'zigzag'),
            params: { ...(this.algoParamsState || {}) },
            start_date: startDate,
            end_date: endDate
        };
        const resp = await fetch('/swing/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!resp.ok) {
            const msg = await resp.text();
            this._appendLog(`[error] start failed: ${msg}`);
            throw new Error('start failed');
        }
        const data = await resp.json();
        const taskId = data.task_id;
        this.swingTask.taskId = taskId;
        this.swingTask.running = true;

        this._openSwingWS(taskId);
    }

    stopSwingGeneration() {
        try {
            if (this.swingTask.ws) {
                this.swingTask.ws.close();
            }
        } catch (e) {}
        this.swingTask.ws = null;
        this.swingTask.taskId = null;
        this.swingTask.running = false;
        this._hideProgress();
        this._clearLog();
    }
    
    clearSwingStatus() {
        this._hideProgress();
        this._clearLog();
        document.getElementById('generation-status').textContent = '就緒';
    }

    _openSwingWS(taskId) {
        const proto = location.protocol === 'https:' ? 'wss' : 'ws';
        const url = `${proto}://${location.host}/swing/progress/${taskId}`;
        let connectDelay = 150; // ms: avoid race between POST creation and WS subscribe
        setTimeout(() => {
            const ws = new WebSocket(url);
            this.swingTask.ws = ws;

            ws.onopen = () => {
                this._appendLog('[ws] connected');
            };
            ws.onmessage = (ev) => {
                // Debug raw WS payload
                try { console.log("WS message:", ev.data); } catch (_) {}
                try {
                    const msg = JSON.parse(ev.data);
                    if (msg.type === 'log') {
                        this._appendLog(msg.line);
                    } else if (msg.type === 'log_batch') {
                        (msg.lines || []).forEach(line => this._appendLog(line));
                    } else if (msg.type === 'progress') {
                        const p = Number(msg.percent || 0);
                        this._showProgress(p);
                    } else if (msg.type === 'done') {
                        this._showProgress(100);
                        setTimeout(() => this._hideProgress(), 1000);
                        // 刷新圖表資料
                        this.loadChart(this.currentTimeframe);
                    }
                } catch (e) {
                    this._appendLog(`[ws error] ${String(e)}`);
                }
            };
            ws.onclose = (ev) => {
                this._appendLog('[ws] closed');
                try { console.log('WS closed', ev.code, ev.reason); } catch (_) {}
            };
            ws.onerror = () => {
                this._appendLog('[ws] error; falling back to HTTP polling');
                // Fallback polling if WS fails (e.g., proxy or browser policy)
                try { this._startProgressPolling(taskId); } catch (_) {}
            };
        }, connectDelay);
    }

    _startProgressPolling(taskId) {
        if (this._pollTimer) clearInterval(this._pollTimer);
        this._pollTimer = setInterval(async () => {
            try {
                const r = await fetch(`/swing/progress/${taskId}/snapshot`);
                if (!r.ok) return;
                const j = await r.json();
                (j.lines || []).forEach(line => this._appendLog(line));
                this._showProgress(Number(j.percent || 0));
                if (j.done) {
                    clearInterval(this._pollTimer);
                    this._pollTimer = null;
                    this._showProgress(100);
                    setTimeout(() => this._hideProgress(), 1000);
                    this.loadChart(this.currentTimeframe);
                }
            } catch (_) { /* ignore */ }
        }, 500);
    }

    _showProgress(percent) {
        if (!this.$progress || !this.$progressFill || !this.$progressText) return;
        this.$progress.style.display = 'flex';
        const p = Math.max(0, Math.min(100, Number(percent) || 0));
        this.$progressFill.style.width = `${p.toFixed(1)}%`;
        this.$progressText.textContent = `${p.toFixed(1)}%`;
    }

    _hideProgress() {
        if (!this.$progress) return;
        this.$progress.style.display = 'none';
        if (this.$progressFill) this.$progressFill.style.width = '0%';
        if (this.$progressText) this.$progressText.textContent = '0%';
    }

    _appendLog(line) {
        if (!this.$log) return;
        this.$log.style.display = 'block';
        this.$log.textContent += (this.$log.textContent ? '\n' : '') + String(line);
        this.$log.scrollTop = this.$log.scrollHeight;
    }

    _clearLog() {
        if (!this.$log) return;
        this.$log.textContent = '';
        this.$log.style.display = 'none';
    }
    
    toggleSwingVisibility() {
        this.swingVisible = !this.swingVisible;
        this.swingLineSeries.applyOptions({ visible: this.swingVisible });
        
        const btn = document.getElementById('show-swing');
        if (btn) {
            btn.style.backgroundColor = this.swingVisible ? '#4caf50' : '';
        }
    }
    
    clearSwings() {
        this.swingLineSeries.setData([]);
        this.swingVisible = false;
        this.swingLineSeries.applyOptions({ visible: false });
        
        const btn = document.getElementById('show-swing');
        if (btn) {
            btn.style.backgroundColor = '';
        }
    }
    
    updateChartTitle() {
        const titleElement = document.getElementById('chart-title');
        if (titleElement) {
            titleElement.textContent = `${this.symbol}, ${this.currentTimeframe}`;
        }
    }
    
    updateDataCount() {
        const countElement = document.getElementById('data-count');
        if (countElement) {
            countElement.textContent = `數據量: ${this.dataCount}筆`;
        }
    }
    
    updatePriceInfo(param) {
        if (!param || !param.time) return;
        
        const priceInfo = document.getElementById('price-info');
        const timeInfo = document.getElementById('time-info');
        
        const candleData = param.seriesPrices && param.seriesPrices.get(this.candlestickSeries);
        if (candleData && priceInfo) {
            priceInfo.textContent = `價格: ${candleData.close?.toFixed(5) || '-'}`;
        }
        
        if (timeInfo) {
            const date = new Date(param.time * 1000);
            timeInfo.textContent = date.toLocaleString('zh-TW');
        }
    }
    
    updateTimeDisplay() {
        const timeElement = document.getElementById('current-time');
        if (timeElement) {
            const now = new Date();
            const year = now.getFullYear();
            const month = String(now.getMonth() + 1).padStart(2, '0');
            const day = String(now.getDate()).padStart(2, '0');
            const hours = String(now.getHours()).padStart(2, '0');
            const minutes = String(now.getMinutes()).padStart(2, '0');
            const seconds = String(now.getSeconds()).padStart(2, '0');
            
            timeElement.textContent = `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
        }
    }
    
    performZoom(factor) {
        if (this.mainChart) {
            const timeScale = this.mainChart.timeScale();
            const logicalRange = timeScale.getVisibleLogicalRange();
            
            if (logicalRange) {
                const center = (logicalRange.from + logicalRange.to) / 2;
                const currentRange = logicalRange.to - logicalRange.from;
                const newRange = currentRange * factor;
                
                const newFrom = Math.max(0, center - newRange / 2);
                const newTo = Math.min(this.dataCount || 1000, center + newRange / 2);
                
                timeScale.setVisibleLogicalRange({
                    from: newFrom,
                    to: newTo
                });
            }
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
        
        console.log('圖表已重置');
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
    
    toggleMeasurementMode() {
        this.measurementMode = !this.measurementMode;
        const button = document.getElementById('measure');
        
        if (button) {
            button.style.backgroundColor = this.measurementMode ? '#ffaa00' : '';
            button.classList.toggle('active', this.measurementMode);
        }
        
        if (!this.measurementMode) {
            this.clearMeasurement();
        }
        
        console.log('測量模式:', this.measurementMode ? '開啟' : '關閉');
    }
    
    handleMeasurementClick(param) {
        console.log('測量點擊事件被觸發，param:', param);
        
        if (!param || !param.time) {
            console.log('沒有時間數據，嘗試使用邏輯位置');
            
            if (param && param.logical !== undefined) {
                console.log('使用邏輯位置:', param.logical);
                param.time = param.logical;
            } else {
                console.log('完全沒有位置數據，退出');
                return;
            }
        }
        
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
            
            for (const [series, data] of param.seriesPrices) {
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
            
            const priceScale = this.mainChart.priceScale('right');
            if (priceScale && typeof priceScale.getVisibleRange === 'function') {
                const range = priceScale.getVisibleRange();
                if (range) {
                    const totalHeight = this.mainChart.height();
                    const yRatio = param.point.y / totalHeight;
                    const estimatedPrice = range.minValue + (range.maxValue - range.minValue) * (1 - yRatio);
                    
                    if (estimatedPrice && !isNaN(estimatedPrice) && estimatedPrice > 10000 && estimatedPrice < 13000) {
                        price = { close: estimatedPrice };
                        console.log('✅ 使用可見範圍估算價格:', estimatedPrice);
                    }
                }
            }
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
        
        // 清除之前的測量標記（如果存在）
        if (this.measurementPoints.length >= 2) {
            console.log('清除之前的測量點');
            this.clearMeasurementLines();
            this.measurementPoints = [];
        }
        
        this.measurementPoints.push(point);
        console.log('當前測量點數量:', this.measurementPoints.length);
        
        // 創建測量點標記
        const markerColor = this.measurementPoints.length === 1 ? '#00aaff' : '#ff6600';
        const marker = {
            time: timeValue,
            position: 'inBar',
            color: markerColor,
            shape: 'cross',
            text: '✚',
            size: 1
        };
        
        this.measurementLines.push(marker);
        
        // 如果有兩個測量點，添加連接線
        if (this.measurementPoints.length === 2) {
            this.addConnectionLine();
        }
        
        // 更新所有標記
        this.candlestickSeries.setMarkers(this.measurementLines);
        console.log('標記已設置，總標記數:', this.measurementLines.length);
        
        if (this.measurementPoints.length === 1) {
            console.log('第一個測量點設置完成，點擊設置第二個測量點');
        } else if (this.measurementPoints.length === 2) {
            console.log('第二個測量點設置完成，正在計算結果...');
            setTimeout(() => {
                this.calculateMeasurement();
            }, 100);
        }
    }
    
    addConnectionLine() {
        if (this.measurementPoints.length === 2) {
            const lineData = [
                { time: this.measurementPoints[0].time, value: this.measurementPoints[0].price },
                { time: this.measurementPoints[1].time, value: this.measurementPoints[1].price }
            ];
            this.measurementLineSeries.setData(lineData);
        }
    }
    
    calculateMeasurement() {
        if (this.measurementPoints.length !== 2) return;
        
        const [point1, point2] = this.measurementPoints;
        
        // 計算價格差異
        const priceDiff = Math.abs(point2.price - point1.price);
        const priceChange = point2.price - point1.price;
        
        let priceChangePercent;
        if (point1.price !== 0) {
            priceChangePercent = ((priceChange / point1.price) * 100).toFixed(2);
        } else {
            priceChangePercent = '0.00';
        }
        
        // 計算時間差
        let timeDiffText = '0天0時0分';
        try {
            let time1, time2;
            
            if (typeof point1.time === 'number' && typeof point2.time === 'number') {
                if (point1.time > 1000000000 && point2.time > 1000000000) {
                    time1 = new Date(point1.time * 1000);
                    time2 = new Date(point2.time * 1000);
                } else {
                    const timeDiff = Math.abs(point2.time - point1.time);
                    const timeframeMultiplier = this.getTimeframeMultiplier();
                    const timeDiffMs = timeDiff * timeframeMultiplier;
                    const now = new Date();
                    time1 = new Date(now.getTime() - timeDiffMs);
                    time2 = now;
                }
            } else {
                time1 = new Date(point1.time);
                time2 = new Date(point2.time);
            }
            
            if (isNaN(time1.getTime()) || isNaN(time2.getTime())) {
                throw new Error('無效的時間值');
            }
            
            const timeDiffMs = Math.abs(time2.getTime() - time1.getTime());
            const days = Math.floor(timeDiffMs / (24 * 60 * 60 * 1000));
            const hours = Math.floor((timeDiffMs % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000));
            const minutes = Math.floor((timeDiffMs % (60 * 60 * 1000)) / (60 * 1000));
            
            timeDiffText = `${days}天${hours}時${minutes}分`;
            
        } catch (error) {
            console.error('時間差計算錯誤:', error);
            timeDiffText = '計算錯誤';
        }
        
        // 判斷方向
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
        
        // 顯示測量結果
        this.showMeasurementPopup(priceDiff, priceChangePercent, timeDiffText, direction, directionSymbol);
    }
    
    getTimeframeMultiplier() {
        const multipliers = {
            'M1': 60 * 1000,
            'M5': 5 * 60 * 1000,
            'M15': 15 * 60 * 1000,
            'M30': 30 * 60 * 1000,
            'H1': 60 * 60 * 1000,
            'H4': 4 * 60 * 60 * 1000,
            'D1': 24 * 60 * 60 * 1000,
            'W1': 7 * 24 * 60 * 60 * 1000,
            'MN': 30 * 24 * 60 * 60 * 1000
        };
        return multipliers[this.currentTimeframe] || 24 * 60 * 60 * 1000;
    }
    
    showMeasurementPopup(priceDiff, priceChangePercent, timeDiffText, direction, directionSymbol) {
        // 更新懸浮視窗內容
        document.getElementById('measurement-price-diff').textContent = priceDiff.toFixed(2);
        
        const percentageText = parseFloat(priceChangePercent) >= 0 ? `+${priceChangePercent}%` : `${priceChangePercent}%`;
        document.getElementById('measurement-change-percent').textContent = percentageText;
        
        document.getElementById('measurement-time-diff').textContent = timeDiffText;
        document.getElementById('measurement-direction').textContent = `${directionSymbol} ${direction}`;
        
        // 顯示懸浮視窗
        const popup = document.getElementById('measurement-popup');
        popup.style.display = 'block';
    }
    
    clearMeasurement() {
        this.measurementPoints = [];
        this.measurementLines = [];
        this.measurementLineSeries.setData([]);
        this.candlestickSeries.setMarkers([]);
    }
    
    clearMeasurementLines() {
        this.clearMeasurement();
    }

    // 波段列表功能
    async showSwingList() {
        try {
            console.log('開始顯示波段列表');
            
            // 檢查是否有波段數據
            if (!this.swingData || this.swingData.length === 0) {
                console.log('沒有波段數據，顯示空列表');
                this.updateSwingListHTML([]);
                this.showSwingListPopup();
                return;
            }
            
            // 創建波段對
            const swingPairs = [];
            for (let i = 0; i < this.swingData.length - 1; i += 2) {
                const start = this.swingData[i];
                const end = this.swingData[i + 1];
                
                if (start && end) {
                    const startTime = new Date(start.time * 1000);
                    const endTime = new Date(end.time * 1000);
                    const priceDiff = end.value - start.value;
                    const timeDiff = Math.floor((endTime - startTime) / (1000 * 60 * 60 * 24));
                    
                    swingPairs.push({
                        startTime: startTime,
                        startPrice: start.value,
                        endTime: endTime,
                        endPrice: end.value,
                        priceDiff: priceDiff,
                        timeDiff: timeDiff
                    });
                }
            }
            
            console.log('生成的波段對數量:', swingPairs.length);
            
            // 更新HTML並顯示彈出視窗
            this.updateSwingListHTML(swingPairs);
            this.showSwingListPopup();
            
        } catch (error) {
            console.error('顯示波段列表失敗:', error);
            throw error;
        }
    }
    
    updateSwingListHTML(swingPairs) {
        const swingListBody = document.getElementById('swing-list-body');
        if (swingListBody) {
            swingListBody.innerHTML = '';
            
            swingPairs.forEach((pair, index) => {
                const row = document.createElement('div');
                row.className = 'swing-list-row';
                
                // 格式化時間
                const startTimeFormatted = pair.startTime.toLocaleDateString('zh-TW');
                const endTimeFormatted = pair.endTime.toLocaleDateString('zh-TW');
                
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
    
    // 全屏功能
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

    // 通知功能
    showNotification(message, type = 'info') {
        // 移除已存在的通知
        const existingNotification = document.querySelector('.notification');
        if (existingNotification) {
            existingNotification.remove();
        }

        // 創建通知元素
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.textContent = message;

        // 添加到頁面頂部
        document.body.appendChild(notification);

        // 動畫顯示
        setTimeout(() => {
            notification.classList.add('show');
        }, 10);

        // 3秒後自動移除
        setTimeout(() => {
            notification.classList.remove('show');
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.remove();
                }
            }, 300);
        }, 3000);
    }
    showParamsPopup() {
        // 檢查當前算法並顯示對應參數
        const paramsForm = document.getElementById('params-form');
        if (!paramsForm) return;
        
        // 清空之前的內容
        paramsForm.innerHTML = '';
        
        if (this.algoSpec && Array.isArray(this.algoSpec.params)) {
            this.algoSpec.params.forEach(param => {
                const group = document.createElement('div');
                group.className = 'params-form-group';
                
                const label = document.createElement('label');
                label.textContent = param.label || param.name;
                
                const input = document.createElement('input');
                input.type = param.type || 'number';
                input.id = `param-${param.name}`;
                input.value = this.algoParamsState[param.name] || param.default || '';
                input.step = param.step || '0.01';
                if (param.min !== undefined) input.min = param.min;
                if (param.max !== undefined) input.max = param.max;
                
                group.appendChild(label);
                group.appendChild(input);
                
                if (param.description) {
                    const desc = document.createElement('div');
                    desc.className = 'param-description';
                    desc.textContent = param.description;
                    group.appendChild(desc);
                }
                
                paramsForm.appendChild(group);
            });
        } else {
            // 如果沒有動態參數，顯示默認ZigZag參數
            const defaultParams = [
                {name: 'deviation', label: 'Deviation', type: 'number', default: 12, step: '1', min: '1', description: '偏差值，控制波段的敏感度'},
                {name: 'depth', label: 'Depth', type: 'number', default: 12, step: '1', min: '1', description: '深度值，影響波段識別的範圍'},
                {name: 'backstep', label: 'Backstep', type: 'number', default: 3, step: '1', min: '1', description: '回退步數，防止波段過於頻繁'}
            ];
            
            defaultParams.forEach(param => {
                const group = document.createElement('div');
                group.className = 'params-form-group';
                
                const label = document.createElement('label');
                label.textContent = param.label;
                
                const input = document.createElement('input');
                input.type = param.type;
                input.id = `param-${param.name}`;
                input.value = this.algoParamsState[param.name] || param.default;
                input.step = param.step;
                input.min = param.min;
                
                group.appendChild(label);
                group.appendChild(input);
                
                const desc = document.createElement('div');
                desc.className = 'param-description';
                desc.textContent = param.description;
                group.appendChild(desc);
                
                paramsForm.appendChild(group);
            });
        }
        
        // 顯示彈出視窗
        document.getElementById('params-popup').style.display = 'block';
    }
}

// 參數彈窗相關函數
function closeParamsPopup() {
    document.getElementById('params-popup').style.display = 'none';
}

function confirmParams() {
    if (window.swingChart) {
        // 收集所有參數值
        const paramsForm = document.getElementById('params-form');
        const inputs = paramsForm.querySelectorAll('input');
        const newParams = {};
        
        inputs.forEach(input => {
            const paramName = input.id.replace('param-', '');
            const value = input.type === 'number' ? parseFloat(input.value) : input.value;
            newParams[paramName] = value;
        });
        
        // 更新參數狀態
        window.swingChart.algoParamsState = { ...window.swingChart.algoParamsState, ...newParams };
        
        // 顯示成功通知
        window.swingChart.showNotification('參數已更新', 'success');
        
        console.log('參數已更新:', newParams);
    }
    closeParamsPopup();
}

// 測量彈窗相關函數
function closeMeasurementPopup() {
    document.getElementById('measurement-popup').style.display = 'none';
}

function clearMeasurementAndClose() {
    if (window.swingChart) {
        window.swingChart.clearMeasurement();
    }
    closeMeasurementPopup();
}

function keepMeasurementAndClose() {
    closeMeasurementPopup();
}

// 波段列表彈窗相關函數
function closeSwingListPopup() {
    if (window.swingChart) {
        window.swingChart.hideSwingListPopup();
    }
}

// 初始化應用
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, creating SwingChart...');
    window.swingChart = new SwingChart();
});

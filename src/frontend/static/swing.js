// SwingChart v5.0 - 2025/08/11 04:30:00 
// RWD響應式版本 - 支持移動端和完整工具按鈕

class SwingChart {
    constructor() {
        console.log('🚀 SwingChart v5.0 constructor - RWD版本');
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
                
                // 設置UI引用
                this._setupSwingTaskUIRefs();
                
                // 設置事件監聽器 (在UI引用之後)
                this.setupEventListeners();
                
                // 載入初始數據
                this.loadChart();
                
                // 確保下拉選單初始狀態是關閉的
                this.forceCloseDropdowns();
                
                // 更新時間顯示
                this.updateTimeDisplay();
                setInterval(() => this.updateTimeDisplay(), 1000);
                
                console.log('Swing Chart initialized successfully');
            }, 500); // 增加延遲確保DOM完全載入
            
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
        const container = document.getElementById('chart');
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
        
        // 工具欄群組功能
        this.setupToolbarGroups();
        
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
        
        // 移動端支持
        this.setupMobileMenu();
        
        // 響應式支持
        window.addEventListener('resize', () => {
            if (this.mainChart) {
                setTimeout(() => {
                    const container = document.getElementById('chart');
                    if (container) {
                        this.mainChart.applyOptions({
                            width: container.clientWidth,
                            height: container.clientHeight
                        });
                    }
                }, 100);
            }
        });
    }
    
    setupMobileMenu() {
        const mobileMenuToggle = document.getElementById('mobile-menu-toggle');
        const mainToolbar = document.querySelector('.main-toolbar');
        
        if (mobileMenuToggle && mainToolbar) {
            mobileMenuToggle.addEventListener('click', () => {
                mainToolbar.classList.toggle('mobile-open');
                mobileMenuToggle.classList.toggle('active');
            });
        }
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

        // 參數區塊移除：改為僅使用彈窗參數設定（避免工具列出現 Deviation(%) 等欄位）
        const existedParamsHost = container.querySelector('.algorithm-params');
        if (existedParamsHost) {
            existedParamsHost.remove();
        }
        // 小提示：點擊參數按鈕開啟彈窗
        // 保留當前 algo 的預設參數在 this.algoParamsState 中，供彈窗使用

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
    
    setupToolbarGroups() {
        console.log('設置工具欄群組功能...');
        
        // 下拉選單控制
        this.setupDropdowns();
        
        // 時間框架控制
        this.setupTimeframes();
        
        // 參數彈窗控制
        this.setupParamsPopup();
        
        // 測量彈窗控制
        this.setupMeasurementPopup();
    }
    
    setupDropdowns() {
        const qs = (sel) => document.querySelector(sel);
        const qsa = (sel) => Array.from(document.querySelectorAll(sel));
        
        const setExpanded = (container, button, expanded) => {
            if (container) container.setAttribute('aria-expanded', expanded ? 'true' : 'false');
            if (button) button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        };
        
        const closeAllDropdowns = () => {
            qsa('.symbol-selector, .algorithm-selector').forEach((s) => 
                setExpanded(s, s.querySelector('button'), false));
        };
        
        // 品種下拉選單
        const symbolSelector = qs('.symbol-selector');
        const symbolBtn = qs('#symbol-dropdown');
        if (symbolBtn) {
            symbolBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                const open = symbolSelector.getAttribute('aria-expanded') === 'true';
                closeAllDropdowns();
                setExpanded(symbolSelector, symbolBtn, !open);
            });
        }
        
        // 演算法下拉選單
        const algoSelector = qs('.algorithm-selector');
        const algoBtn = qs('#algorithm-dropdown');
        if (algoBtn) {
            algoBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                const open = algoSelector.getAttribute('aria-expanded') === 'true';
                closeAllDropdowns();
                setExpanded(algoSelector, algoBtn, !open);
            });
        }
        
        // 點擊外部關閉
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.symbol-selector, .algorithm-selector')) {
                closeAllDropdowns();
            }
        });
        
        // ESC 關閉
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') closeAllDropdowns();
        });
        
        // 選項點擊處理
        document.addEventListener('click', (e) => {
            const symbolOpt = e.target.closest('.symbol-option');
            if (symbolOpt) {
                const value = symbolOpt.dataset.symbol;
                qsa('.symbol-option').forEach(o => o.setAttribute('aria-selected', o === symbolOpt ? 'true' : 'false'));
                const label = qs('#current-symbol');
                if (label && value) {
                    label.textContent = value;
                    this.switchSymbol(value);
                }
                closeAllDropdowns();
            }
            
            const algoOpt = e.target.closest('.algorithm-option');
            if (algoOpt) {
                const value = algoOpt.dataset.algorithm;
                qsa('.algorithm-option').forEach(o => o.setAttribute('aria-selected', o === algoOpt ? 'true' : 'false'));
                const label = qs('#current-algorithm');
                if (label && value) {
                    label.textContent = value;
                    this.switchAlgorithm(value);
                }
                closeAllDropdowns();
            }
        });
    }
    
    setupTimeframes() {
        const container = document.querySelector('.timeframe-buttons');
        if (!container) return;
        
        container.addEventListener('click', (e) => {
            const btn = e.target.closest('.tf-btn');
            if (!btn) return;
            const timeframe = btn.dataset.timeframe;
            
            // 更新活動狀態
            document.querySelectorAll('.tf-btn').forEach(b => {
                b.classList.toggle('active', b === btn);
                b.setAttribute('aria-selected', b === btn ? 'true' : 'false');
            });
            
            // 載入圖表
            this.loadChart(timeframe);
        });
    }
    
    setupParamsPopup() {
        const dlg = document.querySelector('#params-popup');
        if (!dlg) return;
        
        const close = () => this.closeParams();
        
        // 關閉按鈕
        const closeBtn = document.querySelector('#close-params');
        if (closeBtn) {
            closeBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                close();
            });
        }
        
        // 點擊外部關閉
        dlg.addEventListener('click', (e) => {
            if (e.target === dlg) close();
        });
        
        // 取消/確認按鈕
        const cancelBtn = document.querySelector('#cancel-params');
        const confirmBtn = document.querySelector('#confirm-params');
        
        if (cancelBtn) {
            cancelBtn.addEventListener('click', (e) => {
                e.preventDefault();
                close();
            });
        }
        
        if (confirmBtn) {
            confirmBtn.addEventListener('click', (e) => {
                e.preventDefault();
                console.log('🔧 確認參數按鈕被點擊');
                console.log('🔧 更新前的參數狀態:', this.algoParamsState);
                
                const params = this.collectParams();
                this.algoParamsState = { ...this.algoParamsState, ...params };
                
                console.log('🔧 更新後的參數狀態:', this.algoParamsState);
                this.showNotification('參數已更新', 'success');
                close();
            });
        }
        
        // ESC 關閉
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') close();
        });
    }
    
    setupMeasurementPopup() {
        const popup = document.querySelector('#measurement-popup');
        if (!popup) return;
        
        const close = () => {
            popup.setAttribute('aria-hidden', 'true');
            popup.style.display = 'none';
        };
        
        // 關閉按鈕
        const closeBtn = document.querySelector('#close-measurement');
        const confirmBtn = document.querySelector('#confirm-measurement');
        
        if (closeBtn) {
            closeBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                close();
            });
        }
        
        if (confirmBtn) {
            confirmBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                close();
            });
        }
        
        // 點擊外部關閉
        popup.addEventListener('click', (e) => {
            if (e.target === popup) close();
        });
        
        // ESC 關閉
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && popup.getAttribute('aria-hidden') === 'false') {
                close();
            }
        });
    }
    
    collectParams() {
        const params = {};
        
        // 如果有當前演算法規格，動態收集參數
        if (this.algoSpec && this.algoSpec.params) {
            console.log('🔧 動態收集參數，當前演算法:', this.algoSpec.label);
            
            this.algoSpec.params.forEach(paramSpec => {
                const input = document.querySelector(`#${paramSpec.name}-param`);
                if (input) {
                    let value;
                    if (paramSpec.type === 'number') {
                        value = parseFloat(input.value) || paramSpec.default;
                    } else {
                        value = input.value || paramSpec.default;
                    }
                    params[paramSpec.name] = value;
                    console.log(`  📊 ${paramSpec.name}: ${value}`);
                } else {
                    // 如果找不到輸入框，使用預設值
                    params[paramSpec.name] = paramSpec.default;
                    console.log(`  📊 ${paramSpec.name}: ${paramSpec.default} (預設)`);
                }
            });
        } else {
            // 回退到硬編碼參數（向後兼容）
            console.log('🔧 使用硬編碼參數收集');
            const deviationInput = document.querySelector('#deviation-param');
            const depthInput = document.querySelector('#depth-param');
            const backstepInput = document.querySelector('#backstep-param');
            
            if (deviationInput) params.deviation = parseFloat(deviationInput.value) || 5;
            if (depthInput) params.depth = parseInt(depthInput.value) || 10;
            if (backstepInput) params.backstep = parseInt(backstepInput.value) || 3;
        }
        
        console.log('🔧 收集到的參數:', params);
        return params;
    }
    
    closeParams() {
        const dlg = document.querySelector('#params-popup');
        if (dlg) {
            dlg.setAttribute('aria-hidden', 'true');
            dlg.style.display = 'none';
        }
        document.documentElement.style.overflow = '';
        document.body.style.overscrollBehavior = '';
    }
    
    showParamsPopup() {
        console.log('🔧 顯示參數彈窗，當前演算法:', this.algoSpec);
        
        const state = this.algoParamsState || {};
        
        // 如果有當前演算法規格，動態設置參數
        if (this.algoSpec && this.algoSpec.params) {
            console.log('🔧 動態設置參數值');
            
            this.algoSpec.params.forEach(paramSpec => {
                const input = document.querySelector(`#${paramSpec.name}-param`);
                if (input) {
                    const value = state[paramSpec.name] !== undefined ? state[paramSpec.name] : paramSpec.default;
                    input.value = value;
                    console.log(`  📊 設置 ${paramSpec.name} = ${value}`);
                } else {
                    console.log(`  ⚠️ 找不到輸入框: #${paramSpec.name}-param`);
                }
            });
        } else {
            // 回退到硬編碼參數（向後兼容）
            console.log('🔧 使用硬編碼參數設置');
            const deviationInput = document.querySelector('#deviation-param');
            const depthInput = document.querySelector('#depth-param');
            const backstepInput = document.querySelector('#backstep-param');
            
            if (deviationInput && state.deviation != null) deviationInput.value = state.deviation;
            if (depthInput && state.depth != null) depthInput.value = state.depth;
            if (backstepInput && state.backstep != null) backstepInput.value = state.backstep;
        }
        
        const dlg = document.querySelector('#params-popup');
        if (dlg) {
            dlg.setAttribute('aria-hidden', 'false');
            dlg.style.display = 'flex';
            document.documentElement.style.overflow = 'hidden';
            document.body.style.overscrollBehavior = 'contain';
            console.log('🔧 參數彈窗已顯示');
        } else {
            console.error('❌ 找不到參數彈窗元素 #params-popup');
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
    
    
    async loadSwingData() {
        try {
            console.log('🔍 載入波段數據開始...');
            console.log(`📊 當前品種: ${this.symbol}, 時間框架: ${this.currentTimeframe}`);
            
            // 修復演算法名稱映射
            let algoName = (this.algoSpec && this.algoSpec.value) ? this.algoSpec.value : (this.currentAlgorithm || 'zigzag');
            
            // 如果是完整的類名，提取簡單名稱
            if (algoName.includes('algorithms.zigzag:')) {
                algoName = 'zigzag';
            } else if (algoName.includes('algorithms.')) {
                // 處理其他演算法類名
                const parts = algoName.split(':');
                if (parts.length > 1) {
                    algoName = parts[1].toLowerCase().replace('algorithm', '');
                } else {
                    algoName = algoName.split('.').pop().toLowerCase().replace('algorithm', '');
                }
            }
            
            console.log(`🧮 使用演算法: ${algoName} (原始: ${(this.algoSpec && this.algoSpec.value) ? this.algoSpec.value : (this.currentAlgorithm || 'zigzag')})`);
            
            const apiUrl = `/api/swing/${this.symbol}/${this.currentTimeframe}?algorithm=${algoName}`;
            console.log(`🌐 API 請求 URL: ${apiUrl}`);
            
            const response = await fetch(apiUrl);
            console.log(`📡 API 響應狀態: ${response.status} ${response.statusText}`);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            console.log('📋 API 響應數據:', data);
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            if (!data.data || data.data.length === 0) {
                console.log('⚠️ 沒有波段數據');
                this.showNotification('沒有找到波段數據', 'warning');
                return;
            }
            
            console.log(`✅ 載入了 ${data.data.length} 個波段點`);
            console.log('📈 波段數據樣本:', data.data.slice(0, 3));
            
            // 轉換波段數據格式
            const swingData = data.data.map(point => ({
                time: Math.floor(new Date(point.timestamp).getTime() / 1000),
                value: parseFloat(point.zigzag_price)
            }));
            
            console.log('🔄 轉換後的波段數據樣本:', swingData.slice(0, 3));
            
            // 按時間排序
            swingData.sort((a, b) => a.time - b.time);
            console.log('📅 數據已按時間排序');
            
            // 設置波段線數據
            this.swingLineSeries.setData(swingData);
            console.log('📊 波段線數據已設置到圖表');
            
            // 設置波段線可見
            this.swingLineSeries.applyOptions({ visible: true });
            this.swingVisible = true;
            console.log('👁️ 波段線已設置為可見');
            
            // 更新顯示波段按鈕狀態
            const showSwingBtn = document.getElementById('show-swing');
            if (showSwingBtn) {
                showSwingBtn.style.backgroundColor = '#4caf50';
                console.log('🔘 顯示波段按鈕狀態已更新');
            }
            
            console.log('🎉 波段數據載入完成');
            this.showNotification(`成功載入 ${data.data.length} 個波段點`, 'success');
            
        } catch (error) {
            console.error('❌ 載入波段數據失敗:', error);
            console.error('🔍 錯誤詳情:', error.stack);
            this.showNotification('載入波段數據失敗: ' + error.message, 'error');
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
                console.log('🚀 生成波段按鈕被點擊');
                
                // 檢查日期範圍
                const startDate = document.getElementById('start-date').value;
                const endDate = document.getElementById('end-date').value;
                console.log(`📅 日期範圍: ${startDate} 到 ${endDate}`);
                
                if (!startDate || !endDate) {
                    console.log('⚠️ 日期範圍未完整設置');
                    this.showNotification('請選擇日期範圍', 'warning');
                    return;
                }
                
                if (new Date(startDate) >= new Date(endDate)) {
                    console.log('❌ 日期範圍無效');
                    this.showNotification('結束日期必須晚於開始日期', 'error');
                    return;
                }
                
                console.log('✅ 日期範圍驗證通過');
                
                // 獲取按鈕圖示元素
                const btnIcon = generateBtn.querySelector('.btn-icon');
                const originalIcon = btnIcon ? btnIcon.textContent : '🚀';
                console.log(`🔘 按鈕圖示: ${originalIcon} -> ⏳`);
                
                // 添加載入狀態
                if (btnIcon) btnIcon.textContent = '⏳';
                generateBtn.disabled = true;
                
                try {
                    console.log('🎯 開始智能分析...');
                    
                    // 檢查是否開啟智能分層
                    const smartLayersEnabled = document.getElementById('smart-layers')?.checked;
                    
                    if (smartLayersEnabled) {
                        const analysisResult = await this.performSmartAnalysis();
                        if (!analysisResult.approved) {
                            // 用戶取消了執行
                            if (btnIcon) btnIcon.textContent = originalIcon;
                            generateBtn.disabled = false;
                            return;
                        }
                        // 使用智能分析的結果更新參數
                        this.algoParamsState = { ...this.algoParamsState, ...analysisResult.optimizedParams };
                    }
                    
                    console.log('🎯 開始調用 startSwingGeneration...');
                    await this.startSwingGeneration();
                    
                    // 更新按鈕狀態為成功
                    if (btnIcon) btnIcon.textContent = '✅';
                    console.log('✅ 生成請求發送成功');
                    
                    // 3秒後恢復原始狀態
                    setTimeout(() => {
                        if (btnIcon) btnIcon.textContent = '🚀';
                        console.log('🔘 按鈕圖示已恢復');
                    }, 3000);
                    
                } catch (error) {
                    console.error('❌ 生成波段失敗:', error);
                    if (btnIcon) btnIcon.textContent = '❌';
                    
                    // 2秒後恢復原始狀態
                    setTimeout(() => {
                        if (btnIcon) btnIcon.textContent = '🚀';
                    }, 2000);
                } finally {
                    generateBtn.disabled = false;
                    console.log('🔓 按鈕已重新啟用');
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
        console.log('尋找波段列表按鈕元素:', swingListBtn);
        
        if (swingListBtn) {
            console.log('✅ 波段列表按鈕找到，設置事件監聽器');
            
            // 移除可能存在的舊事件監聽器
            const newBtn = swingListBtn.cloneNode(true);
            swingListBtn.parentNode.replaceChild(newBtn, swingListBtn);
            
            newBtn.addEventListener('click', async (event) => {
                event.preventDefault();
                event.stopPropagation();
                
                console.log('🖱️ 波段列表按鈕被點擊');
                
                // 檢查按鈕是否被禁用
                if (newBtn.disabled) {
                    console.log('⚠️ 按鈕已被禁用，忽略點擊');
                    return;
                }
                
                // 添加載入狀態
                const originalText = newBtn.textContent;
                const originalTitle = newBtn.title;
                newBtn.textContent = '⏳';
                newBtn.title = '載入中...';
                newBtn.disabled = true;
                
                try {
                    console.log('🔄 開始執行 showSwingList()');
                    await this.showSwingList();
                    console.log('✅ showSwingList() 執行成功');
                    
                    // 更新按鈕狀態
                    newBtn.textContent = '📋';
                    newBtn.title = '波段列表已顯示';
                    
                } catch (error) {
                    console.error('❌ 顯示波段列表失敗:', error);
                    newBtn.textContent = '❌';
                    newBtn.title = '載入失敗';
                    
                    // 顯示錯誤提示
                    this.showNotification(`載入波段列表失敗: ${error.message}`, 'error');
                    
                    // 2秒後恢復原始狀態
                    setTimeout(() => {
                        newBtn.textContent = originalText;
                        newBtn.title = originalTitle;
                    }, 2000);
                } finally {
                    newBtn.disabled = false;
                }
            });
        } else {
            console.error('❌ 找不到波段列表按鈕元素 #swing-list');
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
            console.log('⚠️ 波段生成任務已在運行中');
            return;
        }
        
        console.log('🚀 開始波段生成任務');
        this._showProgress(0);
        this._appendLog('[ui] start swing generation');

        // 獲取日期範圍
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        console.log(`📅 使用日期範圍: ${startDate} 到 ${endDate}`);

        const payload = {
            symbol: this.symbol,
            timeframe: this.currentTimeframe || 'D1',
            algo: (this.algoSpec && this.algoSpec.value) ? this.algoSpec.value : (this.currentAlgorithm || 'zigzag'),
            params: { ...(this.algoParamsState || {}) },
            start_date: startDate,
            end_date: endDate
        };
        
        console.log('📦 API 請求 payload:', payload);
        console.log(`🔧 當前演算法規格:`, this.algoSpec);
        console.log(`🔧 當前參數狀態:`, this.algoParamsState);
        
        // 🚨 ZigZag算法特殊處理警告
        if (payload.algo && payload.algo.toLowerCase() === 'zigzag') {
            console.warn('🚨 ZigZag算法將使用連續處理模式以保持波段完整性');
        }
        
        const resp = await fetch('/swing/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        console.log(`📡 API 響應狀態: ${resp.status} ${resp.statusText}`);
        
        if (!resp.ok) {
            const msg = await resp.text();
            console.error('❌ API 請求失敗:', msg);
            this._appendLog(`[error] start failed: ${msg}`);
            throw new Error('start failed');
        }
        
        const data = await resp.json();
        console.log('📋 API 響應數據:', data);
        
        const taskId = data.task_id;
        this.swingTask.taskId = taskId;
        this.swingTask.running = true;
        
        console.log(`🆔 任務 ID: ${taskId}`);
        console.log('🔌 準備建立 WebSocket 連接...');

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
        
        // 重置生成按鈕狀態
        const generateBtn = document.getElementById('generate-swing');
        if (generateBtn) {
            const btnIcon = generateBtn.querySelector('.btn-icon');
            if (btnIcon) btnIcon.textContent = '🚀';
            generateBtn.disabled = false;
        }
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
                    console.log('📥 WebSocket 消息解析:', msg);
                    
                    // 統一處理進度更新
                    if (typeof msg.percent !== 'undefined') {
                        const p = Number(msg.percent || 0);
                        this._showProgress(p);
                    }
                    
                    // 統一處理日誌
                    if (msg.logs && Array.isArray(msg.logs) && msg.logs.length > 0) {
                        msg.logs.forEach(line => this._appendLog(line));
                    }
                    
                    // 處理舊格式消息
                    if (msg.type === 'log') {
                        this._appendLog(msg.line);
                    } else if (msg.type === 'log_batch') {
                        (msg.lines || []).forEach(line => this._appendLog(line));
                    } else if (msg.type === 'progress') {
                        const p = Number(msg.percent || 0);
                        this._showProgress(p);
                    } else if (msg.type === 'done') {
                        console.log('🎉 WebSocket 收到 type=done 事件');
                        this._handleTaskCompletion();
                    }
                    // 處理新格式消息 (stage-based)
                    else if (msg.stage) {
                        console.log(`📋 WebSocket 收到 stage=${msg.stage} 事件`);
                        
                        if (msg.stage === 'started') {
                            console.log('🚀 任務開始');
                            // 開始事件已經在上面統一處理了進度和日誌
                        } else if (msg.stage === 'done') {
                            console.log('🎉 任務完成');
                            this._handleTaskCompletion();
                        } else {
                            console.log(`⚙️ 任務階段: ${msg.stage}, 進度: ${msg.percent}%`);
                        }
                    } 
                    // 處理沒有stage的進度消息
                    else if (!msg.stage && typeof msg.percent === 'number') {
                        console.log(`⚙️ 進度更新: ${msg.percent}%`);
                        // 這些是中間進度消息，已經在上面統一處理了進度和日誌
                    }
                    // 處理未識別的消息格式
                    else if (!msg.type && !msg.stage && typeof msg.percent !== 'number') {
                        console.log('❓ 未處理的 WebSocket 消息格式:', msg);
                    }
                } catch (e) {
                    this._appendLog(`[ws error] ${String(e)}`);
                    console.error('❌ WebSocket 消息解析錯誤:', e);
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
                    console.log('🎉 HTTP 輪詢收到 done 事件');
                    clearInterval(this._pollTimer);
                    this._pollTimer = null;
                    this._showProgress(100);
                    this.swingTask.running = false;
                    setTimeout(() => this._hideProgress(), 1000);
                    
                    // 波段生成完成，載入波段數據
                    console.log('🔄 開始載入波段數據...');
                    this.loadSwingData();
                    
                    // 顯示成功訊息
                    this.showNotification('波段生成完成', 'success');
                }
            } catch (_) { /* ignore */ }
        }, 500);
    }

    _handleTaskCompletion() {
        console.log('🎯 處理任務完成事件');
        this._showProgress(100);
        this.swingTask.running = false;
        
        setTimeout(() => this._hideProgress(), 1000);
        
        // 波段生成完成，載入波段數據
        console.log('🔄 開始載入波段數據...');
        this.loadSwingData();
        
        // 顯示成功訊息
        this.showNotification('波段生成完成', 'success');
    }

    _showProgress(percent) {
        if (!this.$progress || !this.$progressFill || !this.$progressText) return;
        this.$progress.style.display = 'flex';
        const p = Math.max(0, Math.min(100, Number(percent) || 0));
        this.$progressFill.style.width = `${p.toFixed(1)}%`;
        this.$progressText.textContent = `${p.toFixed(1)}%`;
        
        // 更新進度信息
        const progressInfo = document.getElementById('progress-info');
        const currentStage = document.getElementById('current-stage');
        const stageProgress = document.getElementById('stage-progress');
        
        if (progressInfo && currentStage && stageProgress) {
            if (p <= 20) {
                progressInfo.textContent = '系統分析: 正在分析數據特性並選擇最佳處理策略...';
                currentStage.textContent = '🔍 正在分析數據特性...';
                stageProgress.textContent = '• 載入數據樣本並分析波動性';
            } else if (p <= 40) {
                progressInfo.textContent = '智能計算: 根據數據特性計算最佳分層參數...';
                currentStage.textContent = '⚙️ 計算最佳處理參數...';
                stageProgress.textContent = '• 智能選擇分層策略';
            } else if (p <= 70) {
                progressInfo.textContent = '主要波段: 執行第一層主要波段識別...';
                currentStage.textContent = '📊 執行主要波段計算...';
                stageProgress.textContent = '• 識別關鍵轉折點';
            } else if (p <= 90) {
                progressInfo.textContent = '細化處理: 執行第二層波段細化...';
                currentStage.textContent = '🔧 執行波段細化處理...';
                stageProgress.textContent = '• 優化波段精度';
            } else if (p < 100) {
                progressInfo.textContent = '結果合併: 整合所有層級的計算結果...';
                currentStage.textContent = '✅ 合併結果並存儲...';
                stageProgress.textContent = '• 最終結果處理中';
            } else {
                progressInfo.textContent = '🎉 波段生成完成！正在準備顯示結果...';
                currentStage.textContent = '🎉 波段生成完成！';
                stageProgress.textContent = '• 準備顯示結果';
            }
        }
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
        
        // 找到內容區域（第二個div）
        const logContent = this.$log.children[1];
        if (logContent) {
            logContent.textContent += (logContent.textContent ? '\n' : '') + String(line);
            logContent.scrollTop = logContent.scrollHeight;
        }
    }

    _clearLog() {
        if (!this.$log) return;
        const logContent = this.$log.children[1];
        if (logContent) {
            logContent.textContent = '';
        }
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
            
            // 修復演算法名稱映射
            let algoName = (this.algoSpec && this.algoSpec.value) ? this.algoSpec.value : (this.currentAlgorithm || 'zigzag');
            
            // 如果是完整的類名，提取簡單名稱
            if (algoName.includes('algorithms.zigzag:')) {
                algoName = 'zigzag';
            } else if (algoName.includes('algorithms.')) {
                // 處理其他演算法類名
                const parts = algoName.split(':');
                if (parts.length > 1) {
                    algoName = parts[1].toLowerCase().replace('algorithm', '');
                } else {
                    algoName = algoName.split('.').pop().toLowerCase().replace('algorithm', '');
                }
            }
            
            console.log(`🧮 波段列表使用演算法: ${algoName}`);
            
            const response = await fetch(`/api/swing/${this.symbol}/${this.currentTimeframe}?algorithm=${algoName}`);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            if (!data.data || data.data.length === 0) {
                console.log('沒有波段數據，顯示空列表');
                this.updateSwingListHTML([]);
                this.showSwingListPopup();
                return;
            }
            
            console.log(`獲取了 ${data.data.length} 個波段點`);
            
            // 創建波段對
            const swingPairs = [];
            for (let i = 0; i < data.data.length - 1; i += 2) {
                const start = data.data[i];
                const end = data.data[i + 1];
                
                if (start && end) {
                    const startTime = new Date(start.timestamp);
                    const endTime = new Date(end.timestamp);
                    const priceDiff = parseFloat(end.zigzag_price) - parseFloat(start.zigzag_price);
                    const timeDiff = Math.floor((endTime - startTime) / (1000 * 60 * 60 * 24));
                    
                    swingPairs.push({
                        startTime: startTime,
                        startPrice: parseFloat(start.zigzag_price),
                        endTime: endTime,
                        endPrice: parseFloat(end.zigzag_price),
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
        console.log('🔍 開始顯示波段列表彈出視窗...');
        
        // 強制等待DOM完全載入
        const waitForDOM = () => {
            return new Promise((resolve) => {
                if (document.readyState === 'complete') {
                    resolve();
                } else {
                    window.addEventListener('load', resolve);
                }
            });
        };
        
        waitForDOM().then(() => {
            // 嘗試多種方式查找popup元素
            let popup = document.getElementById('swing-list-popup');
            
            if (!popup) {
                // 如果找不到，嘗試用querySelector
                popup = document.querySelector('#swing-list-popup');
            }
            
            if (!popup) {
                // 如果還是找不到，嘗試用class查找
                popup = document.querySelector('.swing-list-popup');
            }
            
            console.log('📦 查找彈出視窗元素:', popup);
            
            if (popup) {
                // 檢查當前樣式
                const currentDisplay = popup.style.display;
                const computedStyle = window.getComputedStyle(popup);
                
                console.log('📋 彈出視窗當前狀態:');
                console.log('  - 元素存在:', !!popup);
                console.log('  - style.display (before):', currentDisplay);
                console.log('  - computedStyle.display (before):', computedStyle.display);
                console.log('  - computedStyle.visibility:', computedStyle.visibility);
                console.log('  - computedStyle.opacity:', computedStyle.opacity);
                console.log('  - computedStyle.zIndex:', computedStyle.zIndex);
                console.log('  - offsetWidth:', popup.offsetWidth);
                console.log('  - offsetHeight:', popup.offsetHeight);
                
                // 設置顯示
                popup.style.display = 'block';
                popup.style.visibility = 'visible';
                popup.style.opacity = '1';
                popup.style.zIndex = '1000';
                
                // 檢查設置後的狀態
                const newComputedStyle = window.getComputedStyle(popup);
                console.log('📋 設置後的彈出視窗狀態:');
                console.log('  - style.display (after):', popup.style.display);
                console.log('  - computedStyle.display (after):', newComputedStyle.display);
                console.log('  - computedStyle.visibility (after):', newComputedStyle.visibility);
                console.log('  - computedStyle.opacity (after):', newComputedStyle.opacity);
                console.log('  - offsetWidth (after):', popup.offsetWidth);
                console.log('  - offsetHeight (after):', popup.offsetHeight);
                
                console.log('✅ 波段列表彈出視窗顯示指令已執行');
            } else {
                console.error('❌ 找不到波段列表彈出視窗元素 #swing-list-popup');
                
                // 嘗試查找所有可能的相關元素
                const allPopups = document.querySelectorAll('[id*="popup"], [class*="popup"]');
                console.log('🔍 頁面上所有包含 popup 的元素:', allPopups);
                
                const allSwingList = document.querySelectorAll('[id*="swing-list"], [class*="swing-list"]');
                console.log('🔍 頁面上所有包含 swing-list 的元素:', allSwingList);
            }
        });
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

    // 智能分析功能
    async performSmartAnalysis() {
        return new Promise((resolve) => {
            const analysisWindow = document.getElementById('analysis-status-window');
            const steps = {
                'step-data-load': { icon: '🔄', text: '載入數據樣本...', status: '⏳' },
                'step-data-analysis': { icon: '📊', text: '分析數據特性...', status: '⏱️' },
                'step-param-calc': { icon: '⚙️', text: '計算最佳參數...', status: '⏱️' },
                'step-strategy-select': { icon: '🎯', text: '選擇處理策略...', status: '⏱️' }
            };

            // 顯示分析窗口
            analysisWindow.style.display = 'block';

            // 重置所有步驟狀態
            Object.keys(steps).forEach(stepId => {
                const stepEl = document.getElementById(stepId);
                if (stepEl) {
                    stepEl.querySelector('.step-status').textContent = steps[stepId].status;
                }
            });

            // 隱藏結果和操作區域
            document.getElementById('analysis-result').style.display = 'none';
            document.getElementById('analysis-actions').style.display = 'none';

            // 模擬分析過程
            this.simulateAnalysisSteps(steps).then((analysisData) => {
                // 顯示分析結果
                this.displayAnalysisResults(analysisData);
                
                // 設置確認按鈕事件
                const confirmBtn = document.getElementById('confirm-analysis-and-execute');
                const newConfirmBtn = confirmBtn.cloneNode(true);
                confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
                
                newConfirmBtn.addEventListener('click', () => {
                    analysisWindow.style.display = 'none';
                    resolve({ approved: true, optimizedParams: analysisData.optimizedParams });
                });

                // 設置取消事件
                analysisWindow.querySelector('button[onclick*="analysis-status-window"]').onclick = () => {
                    analysisWindow.style.display = 'none';
                    resolve({ approved: false });
                };
            });
        });
    }

    async simulateAnalysisSteps(steps) {
        const stepIds = Object.keys(steps);
        
        for (let i = 0; i < stepIds.length; i++) {
            const stepId = stepIds[i];
            const stepEl = document.getElementById(stepId);
            
            // 設置當前步驟為進行中
            if (stepEl) {
                stepEl.querySelector('.step-status').textContent = '🔄';
            }

            // 模擬處理時間
            await new Promise(resolve => setTimeout(resolve, 800 + Math.random() * 400));

            // 設置當前步驟為完成
            if (stepEl) {
                stepEl.querySelector('.step-status').textContent = '✅';
            }
        }

        // 生成分析結果
        return this.generateAnalysisData();
    }

    generateAnalysisData() {
        const userParams = this.algoParamsState || {};
        const userDeviation = parseFloat(userParams.deviation || 3.0);
        const userDepth = parseInt(userParams.depth || 12);
        const userMinBars = parseInt(userParams.min_swing_bars || 2);

        // 模擬數據分析
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        const daysDiff = Math.ceil((new Date(endDate) - new Date(startDate)) / (1000 * 60 * 60 * 24));
        const estimatedRecords = Math.floor(daysDiff * 24 * 12); // 假設5分鐘K線

        // 智能計算最佳參數
        let strategy = 'single';
        let layer1Params = { deviation: userDeviation, depth: userDepth, min_swing_bars: userMinBars };
        let layer2Params = null;
        let estimatedTime = '2-4秒';
        let expectedSwings = '15-25個';

        if (estimatedRecords > 50000) {
            strategy = 'dual-layer';
            layer1Params = {
                deviation: Math.max(userDeviation * 1.5, 4.0),
                depth: Math.max(userDepth, 15),
                min_swing_bars: Math.max(userMinBars * 2, 4)
            };
            layer2Params = {
                deviation: userDeviation,
                depth: userDepth,
                min_swing_bars: userMinBars
            };
            estimatedTime = '5-8秒';
            expectedSwings = '25-50個';
        }

        if (estimatedRecords > 200000) {
            strategy = 'triple-layer';
            estimatedTime = '8-12秒';
            expectedSwings = '30-60個';
        }

        return {
            dataOverview: {
                timeRange: `${startDate} ~ ${endDate}`,
                estimatedRecords: estimatedRecords.toLocaleString(),
                volatilityLevel: estimatedRecords > 100000 ? '中高' : '中等',
                marketType: '震盪偏多'
            },
            strategy: {
                type: strategy,
                layer1: layer1Params,
                layer2: layer2Params,
                description: strategy === 'single' ? '單層處理' : strategy === 'dual-layer' ? '雙層分層處理' : '三層分層處理'
            },
            performance: {
                estimatedTime,
                memoryUsage: Math.ceil(estimatedRecords / 1000) + 'MB',
                expectedSwings
            },
            optimizedParams: layer2Params || layer1Params
        };
    }

    displayAnalysisResults(data) {
        // 顯示數據概況
        document.getElementById('data-overview').innerHTML = `
            • 時間範圍: ${data.dataOverview.timeRange}<br>
            • 資料筆數: ${data.dataOverview.estimatedRecords}筆<br>
            • 波動特性: ${data.dataOverview.volatilityLevel} (${data.dataOverview.marketType})
        `;

        // 顯示計算策略
        let strategyHtml = `• 處理方式: ${data.strategy.description}<br>`;
        strategyHtml += `• 主要波段: ${data.strategy.layer1.deviation}%, ${data.strategy.layer1.depth}深度, ${data.strategy.layer1.min_swing_bars}條<br>`;
        if (data.strategy.layer2) {
            strategyHtml += `• 細化波段: ${data.strategy.layer2.deviation}%, ${data.strategy.layer2.depth}深度, ${data.strategy.layer2.min_swing_bars}條<br>`;
        }
        document.getElementById('calculation-strategy').innerHTML = strategyHtml;

        // 顯示效能預估
        document.getElementById('performance-estimation').innerHTML = `
            • 預估時間: ${data.performance.estimatedTime}<br>
            • 記憶體使用: ~${data.performance.memoryUsage}<br>
            • 預期波段: ${data.performance.expectedSwings}
        `;

        // 顯示結果和操作區域
        document.getElementById('analysis-result').style.display = 'block';
        document.getElementById('analysis-actions').style.display = 'block';
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
        
        // 設置智能分析事件監聽
        const smartLayersCheckbox = document.getElementById('smart-layers');
        const smartAnalysisSection = document.getElementById('smart-analysis-section');
        const refreshAnalysisBtn = document.getElementById('refresh-analysis');
        
        if (smartLayersCheckbox && smartAnalysisSection) {
            // 智能分層勾選狀態變化
            smartLayersCheckbox.addEventListener('change', () => {
                if (smartLayersCheckbox.checked) {
                    smartAnalysisSection.style.display = 'block';
                    this.triggerSmartAnalysisPreview();
                } else {
                    smartAnalysisSection.style.display = 'none';
                }
            });
            
            // 重新分析按鈕
            if (refreshAnalysisBtn) {
                refreshAnalysisBtn.addEventListener('click', () => {
                    this.triggerSmartAnalysisPreview();
                });
            }
            
            // 如果已經勾選，立即顯示分析結果
            if (smartLayersCheckbox.checked) {
                smartAnalysisSection.style.display = 'block';
                this.triggerSmartAnalysisPreview();
            }
        }
    }
    
    triggerSmartAnalysisPreview() {
        const analysisContent = document.getElementById('analysis-content');
        if (!analysisContent) return;
        
        // 顯示載入狀態
        analysisContent.innerHTML = '<div class="analysis-loading">正在分析數據特性...</div>';
        
        // 模擬分析過程
        setTimeout(() => {
            const analysisData = this.generateAnalysisData();
            analysisContent.innerHTML = `
                <div style="font-size: 13px; line-height: 1.6;">
                    <div style="margin-bottom: 12px;">
                        <strong>📊 數據概況:</strong><br>
                        • 時間範圍: ${analysisData.dataOverview.timeRange}<br>
                        • 資料筆數: ${analysisData.dataOverview.estimatedRecords}筆<br>
                        • 波動特性: ${analysisData.dataOverview.volatilityLevel}
                    </div>
                    <div style="margin-bottom: 12px;">
                        <strong>⚙️ 建議策略:</strong><br>
                        • ${analysisData.strategy.description}<br>
                        • 主要參數: ${analysisData.strategy.layer1.deviation}%, ${analysisData.strategy.layer1.depth}深度, ${analysisData.strategy.layer1.min_swing_bars}條
                        ${analysisData.strategy.layer2 ? `<br>• 細化參數: ${analysisData.strategy.layer2.deviation}%, ${analysisData.strategy.layer2.depth}深度, ${analysisData.strategy.layer2.min_swing_bars}條` : ''}
                    </div>
                    <div>
                        <strong>⏱️ 預估效能:</strong><br>
                        • 處理時間: ${analysisData.performance.estimatedTime}<br>
                        • 記憶體使用: ${analysisData.performance.memoryUsage}<br>
                        • 預期波段: ${analysisData.performance.expectedSwings}
                    </div>
                </div>
            `;
        }, 1500);
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

// 手動觸發波段列表顯示（備用方法）
function showSwingListManual() {
    console.log('🔧 手動觸發波段列表顯示');
    if (window.swingChart) {
        window.swingChart.showSwingList().catch(error => {
            console.error('手動顯示波段列表失敗:', error);
        });
    } else {
        console.error('swingChart 實例不存在');
    }
}

// 直接測試彈出視窗顯示
function testPopupDisplay() {
    console.log('🧪 測試彈出視窗顯示');
    
    const popup = document.getElementById('swing-list-popup');
    console.log('彈出視窗元素:', popup);
    
    if (popup) {
        // 強制顯示
        popup.style.display = 'block';
        popup.style.visibility = 'visible';
        popup.style.opacity = '1';
        popup.style.position = 'fixed';
        popup.style.top = '50%';
        popup.style.left = '50%';
        popup.style.transform = 'translate(-50%, -50%)';
        popup.style.zIndex = '9999';
        popup.style.background = 'red'; // 臨時顏色便於識別
        popup.style.border = '5px solid yellow';
        popup.style.width = '400px';
        popup.style.height = '300px';
        
        console.log('✅ 彈出視窗強制顯示完成');
        console.log('位置和樣式:', {
            display: popup.style.display,
            visibility: popup.style.visibility,
            opacity: popup.style.opacity,
            zIndex: popup.style.zIndex,
            top: popup.style.top,
            left: popup.style.left
        });
    } else {
        console.error('❌ 找不到彈出視窗元素');
    }
}

// 響應式處理函數
function handleResize() {
    if (window.swingChart && window.swingChart.chart) {
        // 延遲執行以避免頻繁觸發
        clearTimeout(window.resizeTimeout);
        window.resizeTimeout = setTimeout(() => {
            console.log('Window resized, updating chart...');
            window.swingChart.chart.timeScale().fitContent();
            
            // 檢查是否需要關閉移動端選單
            const mainToolbar = document.querySelector('.main-toolbar');
            if (window.innerWidth > 480 && mainToolbar) {
                mainToolbar.classList.remove('mobile-open');
            }
        }, 150);
    }
}

// 處理屏幕方向變化
function handleOrientationChange() {
    setTimeout(() => {
        handleResize();
    }, 500); // 等待方向變化完成
}

// 初始化應用
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, creating SwingChart...');
    window.swingChart = new SwingChart();
    
    // 添加響應式事件監聽器
    window.addEventListener('resize', handleResize);
    window.addEventListener('orientationchange', handleOrientationChange);
    
    // 初始化完成後記錄
    console.log('RWD事件監聽器已註冊');
});

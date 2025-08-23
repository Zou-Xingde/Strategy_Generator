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

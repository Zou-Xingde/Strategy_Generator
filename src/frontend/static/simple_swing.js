console.log('=== 最簡單的 Swing 測試 ===');

// 測試 LightweightCharts 是否可用
if (typeof LightweightCharts === 'undefined') {
    console.error('❌ LightweightCharts 未載入');
    document.body.innerHTML = '<div style="color: red; font-size: 24px; text-align: center; margin-top: 100px;">❌ LightweightCharts 庫未載入！</div>';
} else {
    console.log('✅ LightweightCharts 已載入');
}

// 簡化的 SwingChart 類
class SimpleSwingChart {
    constructor() {
        console.log('🎯 SimpleSwingChart 構造函數被調用');
        this.initializeChart();
    }

    initializeChart() {
        console.log('📊 開始初始化圖表...');
        
        const chartContainer = document.getElementById('main-chart');
        if (!chartContainer) {
            console.error('❌ 找不到圖表容器 #main-chart');
            return;
        }
        
        console.log('✅ 找到圖表容器');
        
        try {
            // 創建圖表
            this.chart = LightweightCharts.createChart(chartContainer, {
                width: chartContainer.clientWidth,
                height: chartContainer.clientHeight || 400,
                layout: {
                    backgroundColor: '#1e1e1e',
                    textColor: '#d1d4dc',
                },
                timeScale: {
                    rightOffset: 0, // 移除右邊距
                },
            });
            
            console.log('✅ 圖表對象創建成功');
            
            // 創建K線系列
            this.candlestickSeries = this.chart.addCandlestickSeries({
                upColor: '#26a69a',
                downColor: '#ef5350',
                borderVisible: false,
                wickUpColor: '#26a69a',
                wickDownColor: '#ef5350',
            });
            
            console.log('✅ K線系列創建成功');
            
            // 添加測試數據
            const testData = [
                { time: '2023-01-01', open: 2000, high: 2050, low: 1980, close: 2020 },
                { time: '2023-01-02', open: 2020, high: 2070, low: 2010, close: 2060 },
                { time: '2023-01-03', open: 2060, high: 2080, low: 2040, close: 2070 },
                { time: '2023-01-04', open: 2070, high: 2090, low: 2050, close: 2080 },
                { time: '2023-01-05', open: 2080, high: 2100, low: 2060, close: 2090 },
            ];
            
            this.candlestickSeries.setData(testData);
            console.log('✅ 測試數據添加成功');
            
            // 調整縮放（縮小3個階段）
            const zoom = Math.pow(1.2, 3); // 3階段縮小
            this.chart.timeScale().setVisibleLogicalRange({
                from: 0,
                to: testData.length * zoom
            });
            
            console.log('✅ 縮放調整完成');
            console.log('🎉 圖表初始化完全成功！');
            
            // 移除載入中顯示
            const statusElement = document.getElementById('generation-status');
            if (statusElement) {
                statusElement.textContent = '圖表載入成功';
            }
            
        } catch (error) {
            console.error('❌ 圖表初始化錯誤:', error);
            document.body.innerHTML += `<div style="color: red; margin: 20px;">錯誤: ${error.message}</div>`;
        }
    }
}

// DOM 載入完成後初始化
document.addEventListener('DOMContentLoaded', () => {
    console.log('🚀 DOM 載入完成，開始創建圖表...');
    try {
        window.swingChart = new SimpleSwingChart();
    } catch (error) {
        console.error('❌ 創建圖表時發生錯誤:', error);
    }
});

console.log('📄 simple_swing.js 載入完成');

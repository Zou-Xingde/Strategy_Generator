console.log('swing_test.js loaded');

class SwingChart {
    constructor() {
        console.log('SwingChart constructor called');
        this.initializeChart();
    }

    initializeChart() {
        console.log('Initializing basic chart...');
        
        // 获取图表容器
        const chartContainer = document.getElementById('main-chart');
        if (!chartContainer) {
            console.error('Chart container not found!');
            return;
        }

        try {
            console.log('Creating LightweightCharts chart...');
            
            // 创建图表
            this.chart = LightweightCharts.createChart(chartContainer, {
                width: chartContainer.clientWidth,
                height: chartContainer.clientHeight,
                layout: {
                    backgroundColor: '#1e1e1e',
                    textColor: '#d1d4dc',
                },
                grid: {
                    vertLines: {
                        color: '#2B2B43',
                    },
                    horzLines: {
                        color: '#2B2B43',
                    },
                },
                timeScale: {
                    timeVisible: true,
                    secondsVisible: false,
                    rightOffset: 0,
                },
            });

            console.log('Chart created successfully');

            // 创建K线系列
            this.candlestickSeries = this.chart.addCandlestickSeries({
                upColor: '#26a69a',
                downColor: '#ef5350',
                borderVisible: false,
                wickUpColor: '#26a69a',
                wickDownColor: '#ef5350',
            });

            console.log('Candlestick series added');

            // 添加一些测试数据
            const testData = [
                { time: '2023-01-01', open: 2000, high: 2050, low: 1980, close: 2020 },
                { time: '2023-01-02', open: 2020, high: 2070, low: 2010, close: 2060 },
                { time: '2023-01-03', open: 2060, high: 2080, low: 2040, close: 2070 },
                { time: '2023-01-04', open: 2070, high: 2090, low: 2050, close: 2080 },
                { time: '2023-01-05', open: 2080, high: 2100, low: 2060, close: 2090 },
            ];

            this.candlestickSeries.setData(testData);
            console.log('Test data added');

            // 设置缩放
            this.chart.timeScale().fitContent();
            console.log('Chart initialization completed');

        } catch (error) {
            console.error('Error creating chart:', error);
        }
    }
}

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, creating test SwingChart...');
    window.swingChart = new SwingChart();
});

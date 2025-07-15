
    class CandlestickChart {
        constructor() {
            this.chart = null;
            this.currentTimeframe = 'D1';
            this.symbol = 'EXUSA30IDXUSD';
            this.measurementMode = false;
            this.measurementPoints = [];
            this.config = {};
            
            this.init();
        }
        
        async init() {
            await this.loadConfig();
            await this.loadChart();
            this.setupEventListeners();
        }
        
        async loadConfig() {
            try {
                const response = await fetch('/api/config');
                this.config = await response.json();
                this.symbol = this.config.symbol;
            } catch (error) {
                console.error('載入配置失敗:', error);
            }
        }
        
        async loadChart(timeframe = 'D1') {
            this.currentTimeframe = timeframe;
            
            try {
                const response = await fetch(`/api/candlestick/${this.symbol}/${timeframe}`);
                const data = await response.json();
                
                if (data.error) {
                    throw new Error(data.error);
                }
                
                this.renderChart(data.data);
                this.updateInfo(data);
                
            } catch (error) {
                console.error('載入圖表失敗:', error);
                this.showError('載入圖表資料失敗: ' + error.message);
            }
        }
        
        renderChart(data) {
            if (!data || data.length === 0) {
                this.showError('暫無資料');
                return;
            }
            
            // 準備資料
            const timestamps = data.map(d => d.timestamp);
            const ohlcData = data.map(d => [
                new Date(d.timestamp).getTime(),
                d.open,
                d.high,
                d.low,
                d.close
            ]);
            
            const volumeData = data.map(d => [
                new Date(d.timestamp).getTime(),
                d.volume
            ]);
            
            // 圖表配置
            const options = {
                chart: {
                    type: 'candlestick',
                    height: 600,
                    toolbar: {
                        show: true
                    },
                    events: {
                        click: (event, chartContext, config) => {
                            if (this.measurementMode) {
                                this.addMeasurementPoint(event, config);
                            }
                        }
                    }
                },
                title: {
                    text: `${this.symbol} - ${this.currentTimeframe} 蠟燭圖`,
                    align: 'center'
                },
                xaxis: {
                    type: 'datetime',
                    labels: {
                        datetimeFormatter: {
                            year: 'yyyy',
                            month: 'MMM 'yy',
                            day: 'dd MMM',
                            hour: 'HH:mm'
                        }
                    }
                },
                yaxis: [
                    {
                        title: {
                            text: '價格'
                        },
                        tooltip: {
                            enabled: true
                        }
                    },
                    {
                        opposite: true,
                        title: {
                            text: '成交量'
                        },
                        max: Math.max(...volumeData.map(d => d[1])) * 4
                    }
                ],
                tooltip: {
                    shared: true,
                    custom: function({seriesIndex, dataPointIndex, w}) {
                        const data = w.globals.initialSeries[seriesIndex].data[dataPointIndex];
                        if (seriesIndex === 0) {
                            return `<div class="tooltip">
                                <strong>${new Date(data.x).toLocaleString()}</strong><br>
                                開: ${data.y[0]}<br>
                                高: ${data.y[1]}<br>
                                低: ${data.y[2]}<br>
                                收: ${data.y[3]}
                            </div>`;
                        }
                        return '';
                    }
                }
            };
            
            // 創建圖表
            if (this.chart) {
                this.chart.destroy();
            }
            
            this.chart = new ApexCharts(document.querySelector("#chart"), {
                ...options,
                series: [
                    {
                        name: '蠟燭圖',
                        type: 'candlestick',
                        data: ohlcData
                    },
                    {
                        name: '成交量',
                        type: 'column',
                        data: volumeData,
                        yAxisIndex: 1
                    }
                ]
            });
            
            this.chart.render();
        }
        
        setupEventListeners() {
            // 時間週期按鈕
            document.querySelectorAll('.timeframe-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    document.querySelectorAll('.timeframe-btn').forEach(b => b.classList.remove('active'));
                    e.target.classList.add('active');
                    this.loadChart(e.target.dataset.timeframe);
                });
            });
            
            // 測量工具按鈕
            document.getElementById('measureBtn').addEventListener('click', () => {
                this.toggleMeasurementMode();
            });
            
            // 重置按鈕
            document.getElementById('resetBtn').addEventListener('click', () => {
                this.resetMeasurement();
            });
            
            // 縮放按鈕
            document.getElementById('zoomInBtn').addEventListener('click', () => {
                if (this.chart) {
                    this.chart.zoomIn();
                }
            });
            
            document.getElementById('zoomOutBtn').addEventListener('click', () => {
                if (this.chart) {
                    this.chart.zoomOut();
                }
            });
        }
        
        toggleMeasurementMode() {
            this.measurementMode = !this.measurementMode;
            const btn = document.getElementById('measureBtn');
            
            if (this.measurementMode) {
                btn.textContent = '取消測量';
                btn.classList.add('active');
                this.measurementPoints = [];
                this.updateMeasurementResult('測量模式已開啟，請點擊圖表上的兩個點');
            } else {
                btn.textContent = '測量工具';
                btn.classList.remove('active');
                this.resetMeasurement();
            }
        }
        
        addMeasurementPoint(event, config) {
            if (this.measurementPoints.length >= 2) {
                return;
            }
            
            const point = {
                x: new Date(config.w.globals.seriesX[0][config.dataPointIndex]).toISOString(),
                y: config.w.globals.seriesY[0][config.dataPointIndex]
            };
            
            this.measurementPoints.push(point);
            
            if (this.measurementPoints.length === 2) {
                this.calculateMeasurement();
            } else {
                this.updateMeasurementResult(`已選擇第 ${this.measurementPoints.length} 個點`);
            }
        }
        
        async calculateMeasurement() {
            try {
                const response = await fetch('/api/measurement', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        point1: this.measurementPoints[0],
                        point2: this.measurementPoints[1]
                    })
                });
                
                const result = await response.json();
                this.displayMeasurementResult(result);
                
            } catch (error) {
                console.error('計算測量失敗:', error);
                this.updateMeasurementResult('計算測量失敗');
            }
        }
        
        displayMeasurementResult(result) {
            const html = `
                <div class="measurement-result">
                    <h5>測量結果:</h5>
                    <p><strong>價格差異:</strong> ${result.price_diff.toFixed(5)}</p>
                    <p><strong>時間差異:</strong> ${result.time_diff}</p>
                    <p><strong>點1:</strong> ${new Date(result.point1.x).toLocaleString()} @ ${result.point1.y.toFixed(5)}</p>
                    <p><strong>點2:</strong> ${new Date(result.point2.x).toLocaleString()} @ ${result.point2.y.toFixed(5)}</p>
                </div>
            `;
            
            document.getElementById('measurementResult').innerHTML = html;
            this.measurementMode = false;
            document.getElementById('measureBtn').textContent = '測量工具';
            document.getElementById('measureBtn').classList.remove('active');
        }
        
        resetMeasurement() {
            this.measurementPoints = [];
            this.measurementMode = false;
            document.getElementById('measureBtn').textContent = '測量工具';
            document.getElementById('measureBtn').classList.remove('active');
            document.getElementById('measurementResult').innerHTML = '';
        }
        
        updateInfo(data) {
            if (!data.data || data.data.length === 0) {
                return;
            }
            
            const latest = data.data[data.data.length - 1];
            
            document.getElementById('currentSymbol').textContent = data.symbol;
            document.getElementById('currentTimeframe').textContent = data.timeframe;
            document.getElementById('dataCount').textContent = data.count;
            
            document.getElementById('latestOpen').textContent = latest.open.toFixed(5);
            document.getElementById('latestHigh').textContent = latest.high.toFixed(5);
            document.getElementById('latestLow').textContent = latest.low.toFixed(5);
            document.getElementById('latestClose').textContent = latest.close.toFixed(5);
            
            document.getElementById('latestVolume').textContent = latest.volume.toLocaleString();
            document.getElementById('latestTime').textContent = new Date(latest.timestamp).toLocaleString();
        }
        
        updateMeasurementResult(message) {
            document.getElementById('measurementResult').innerHTML = `<p>${message}</p>`;
        }
        
        showError(message) {
            const errorDiv = document.createElement('div');
            errorDiv.className = 'error';
            errorDiv.textContent = message;
            
            const container = document.querySelector('.container');
            container.insertBefore(errorDiv, container.firstChild);
            
            setTimeout(() => {
                errorDiv.remove();
            }, 5000);
        }
    }
    
    // 初始化應用
    document.addEventListener('DOMContentLoaded', () => {
        new CandlestickChart();
    });
    
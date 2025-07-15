import dash
from dash import dcc, html, Input, Output, State, callback_context
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, timedelta
import json
import dash_bootstrap_components as dbc

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    DUCKDB_PATH, TIMEFRAMES, FRONTEND_HOST, FRONTEND_PORT,
    MEASUREMENT_COLORS, CANDLESTICK_COLORS
)
from src.database.connection import DuckDBConnection

# 初始化Dash應用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "市場波段規律分析系統"

# 全局變數
current_symbol = "EXUSA30IDXUSD"  # 默認交易品種
measurement_mode = False
measurement_points = []

# 應用布局
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("市場波段規律分析系統", className="text-center mb-4"),
            html.Hr()
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            # 時間週期選擇按鈕
            dbc.ButtonGroup([
                dbc.Button("M1", id="btn-M1", color="primary", outline=True),
                dbc.Button("M5", id="btn-M5", color="primary", outline=True),
                dbc.Button("M15", id="btn-M15", color="primary", outline=True),
                dbc.Button("M30", id="btn-M30", color="primary", outline=True),
                dbc.Button("H1", id="btn-H1", color="primary", outline=True),
                dbc.Button("H4", id="btn-H4", color="primary", outline=True),
                dbc.Button("D1", id="btn-D1", color="primary", outline=True, active=True),
            ], className="mb-3"),
            
            # 工具按鈕
            dbc.ButtonGroup([
                dbc.Button("測量工具", id="btn-measure", color="info", outline=True),
                dbc.Button("重置", id="btn-reset", color="warning", outline=True),
                dbc.Button("放大", id="btn-zoom-in", color="success", outline=True),
                dbc.Button("縮小", id="btn-zoom-out", color="success", outline=True),
            ], className="mb-3 ms-3"),
            
        ], width=12),
    ]),
    
    dbc.Row([
        dbc.Col([
            # 蠟燭圖顯示區域
            dcc.Graph(
                id="candlestick-chart",
                style={"height": "70vh"},
                config={
                    'displayModeBar': True,
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': 'candlestick_chart',
                        'height': 800,
                        'width': 1200,
                        'scale': 1
                    }
                }
            )
        ], width=12),
    ]),
    
    dbc.Row([
        dbc.Col([
            # 資訊顯示區域
            html.Div(id="info-display", className="mt-3"),
            
            # 測量結果顯示
            html.Div(id="measurement-result", className="mt-3"),
            
        ], width=12),
    ]),
    
    # 隱藏的數據存儲
    dcc.Store(id="current-timeframe", data="D1"),
    dcc.Store(id="chart-data", data={}),
    dcc.Store(id="measurement-data", data={"points": [], "mode": False}),
    dcc.Store(id="zoom-range", data={}),
    
], fluid=True)

def get_candlestick_data(symbol: str, timeframe: str) -> pd.DataFrame:
    """從資料庫獲取蠟燭圖資料"""
    try:
        with DuckDBConnection(str(DUCKDB_PATH)) as db:
            df = db.get_candlestick_data(symbol, timeframe)
            return df
    except Exception as e:
        print(f"Error getting candlestick data: {e}")
        return pd.DataFrame()

def create_candlestick_chart(df: pd.DataFrame, timeframe: str, measurement_points: list = None, zoom_range: dict = None):
    """創建蠟燭圖"""
    if df.empty:
        return go.Figure().add_annotation(
            text="暫無資料",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=20)
        )
    
    fig = go.Figure()
    
    # 添加蠟燭圖
    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name=f"{timeframe} 蠟燭圖",
        increasing_line_color=CANDLESTICK_COLORS['increasing'],
        decreasing_line_color=CANDLESTICK_COLORS['decreasing'],
        increasing_fillcolor=CANDLESTICK_COLORS['increasing'],
        decreasing_fillcolor=CANDLESTICK_COLORS['decreasing'],
    ))
    
    # 添加成交量（子圖）
    if 'volume' in df.columns:
        fig.add_trace(go.Bar(
            x=df.index,
            y=df['volume'],
            name="成交量",
            yaxis="y2",
            marker_color="rgba(0, 0, 255, 0.3)"
        ))
    
    # 添加測量線
    if measurement_points and len(measurement_points) >= 2:
        # 藍色線（第一個點）
        fig.add_shape(
            type="line",
            x0=measurement_points[0]['x'], y0=df['low'].min(),
            x1=measurement_points[0]['x'], y1=df['high'].max(),
            line=dict(color=MEASUREMENT_COLORS['line1'], width=2, dash="dash"),
            name="測量線1"
        )
        
        # 紅色線（第二個點）
        fig.add_shape(
            type="line",
            x0=measurement_points[1]['x'], y0=df['low'].min(),
            x1=measurement_points[1]['x'], y1=df['high'].max(),
            line=dict(color=MEASUREMENT_COLORS['line2'], width=2, dash="dash"),
            name="測量線2"
        )
    
    # 設置圖表布局
    fig.update_layout(
        title=f"{current_symbol} - {timeframe} 蠟燭圖",
        xaxis_title="時間",
        yaxis_title="價格",
        xaxis_rangeslider_visible=False,
        height=600,
        template="plotly_white",
        showlegend=True,
        yaxis2=dict(
            title="成交量",
            overlaying="y",
            side="right",
            range=[0, df['volume'].max() * 4] if 'volume' in df.columns else None
        )
    )
    
    # 設置縮放範圍
    if zoom_range:
        fig.update_layout(
            xaxis=dict(range=zoom_range.get('x', [df.index.min(), df.index.max()])),
            yaxis=dict(range=zoom_range.get('y', [df['low'].min(), df['high'].max()]))
        )
    
    return fig

# 時間週期切換回調
@app.callback(
    [Output("current-timeframe", "data"),
     Output("chart-data", "data"),
     Output("candlestick-chart", "figure")],
    [Input("btn-M1", "n_clicks"),
     Input("btn-M5", "n_clicks"),
     Input("btn-M15", "n_clicks"),
     Input("btn-M30", "n_clicks"),
     Input("btn-H1", "n_clicks"),
     Input("btn-H4", "n_clicks"),
     Input("btn-D1", "n_clicks")],
    [State("current-timeframe", "data"),
     State("measurement-data", "data"),
     State("zoom-range", "data")]
)
def update_timeframe(m1, m5, m15, m30, h1, h4, d1, current_tf, measurement_data, zoom_range):
    # 確定觸發的按鈕
    ctx = callback_context
    if not ctx.triggered:
        timeframe = "D1"
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        timeframe = button_id.split('-')[1]
    
    # 獲取資料
    df = get_candlestick_data(current_symbol, timeframe)
    
    # 創建圖表
    fig = create_candlestick_chart(
        df, timeframe, 
        measurement_data.get('points', []) if measurement_data else [], 
        zoom_range if zoom_range else {}
    )
    
    # 將DataFrame轉換為字典以存儲
    chart_data = df.to_dict('records') if not df.empty else {}
    
    return timeframe, chart_data, fig

# 測量工具回調
@app.callback(
    [Output("measurement-data", "data"),
     Output("measurement-result", "children")],
    [Input("btn-measure", "n_clicks"),
     Input("btn-reset", "n_clicks"),
     Input("candlestick-chart", "clickData")],
    [State("measurement-data", "data"),
     State("chart-data", "data")]
)
def handle_measurement(measure_clicks, reset_clicks, click_data, measurement_data, chart_data):
    ctx = callback_context
    if not ctx.triggered:
        return measurement_data, ""
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if trigger_id == "btn-measure":
        # 切換測量模式
        measurement_data['mode'] = not measurement_data.get('mode', False)
        measurement_data['points'] = []
        return measurement_data, "測量模式已開啟，請點擊圖表上的兩個點" if measurement_data['mode'] else ""
    
    elif trigger_id == "btn-reset":
        # 重置測量
        measurement_data['mode'] = False
        measurement_data['points'] = []
        return measurement_data, ""
    
    elif trigger_id == "candlestick-chart" and click_data and measurement_data.get('mode', False):
        # 添加測量點
        points = measurement_data.get('points', [])
        
        if len(points) < 2:
            point = {
                'x': click_data['points'][0]['x'],
                'y': click_data['points'][0]['y']
            }
            points.append(point)
            measurement_data['points'] = points
            
            if len(points) == 2:
                # 計算點差
                x1, y1 = points[0]['x'], points[0]['y']
                x2, y2 = points[1]['x'], points[1]['y']
                
                # 計算價格差異
                price_diff = abs(y2 - y1)
                
                # 計算時間差異
                time1 = pd.to_datetime(x1)
                time2 = pd.to_datetime(x2)
                time_diff = abs(time2 - time1)
                
                result = html.Div([
                    html.H5("測量結果:", className="text-info"),
                    html.P(f"價格差異: {price_diff:.5f}"),
                    html.P(f"時間差異: {time_diff}"),
                    html.P(f"點1: {x1} @ {y1:.5f}"),
                    html.P(f"點2: {x2} @ {y2:.5f}"),
                ], className="border p-3 mt-2")
                
                measurement_data['mode'] = False  # 測量完成後關閉模式
                return measurement_data, result
        
        return measurement_data, f"已選擇 {len(points)} 個點，{'請選擇第二個點' if len(points) == 1 else '測量完成'}"
    
    return measurement_data, ""

# 縮放控制回調
@app.callback(
    [Output("zoom-range", "data"),
     Output("candlestick-chart", "figure", allow_duplicate=True)],
    [Input("btn-zoom-in", "n_clicks"),
     Input("btn-zoom-out", "n_clicks")],
    [State("candlestick-chart", "relayoutData"),
     State("current-timeframe", "data"),
     State("chart-data", "data"),
     State("measurement-data", "data"),
     State("zoom-range", "data")],
    prevent_initial_call=True
)
def handle_zoom(zoom_in, zoom_out, relayout_data, timeframe, chart_data, measurement_data, zoom_range):
    ctx = callback_context
    if not ctx.triggered or not chart_data:
        return zoom_range, dash.no_update
    
    df = pd.DataFrame(chart_data)
    if df.empty:
        return zoom_range, dash.no_update
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if trigger_id == "btn-zoom-in":
        # 放大：縮小時間範圍
        current_range = zoom_range.get('x', [df.index.min(), df.index.max()])
        time_span = pd.to_datetime(current_range[1]) - pd.to_datetime(current_range[0])
        new_span = time_span * 0.5  # 縮小到一半
        
        center = pd.to_datetime(current_range[0]) + time_span / 2
        new_start = center - new_span / 2
        new_end = center + new_span / 2
        
        zoom_range['x'] = [new_start, new_end]
        
    elif trigger_id == "btn-zoom-out":
        # 縮小：擴大時間範圍
        current_range = zoom_range.get('x', [df.index.min(), df.index.max()])
        time_span = pd.to_datetime(current_range[1]) - pd.to_datetime(current_range[0])
        new_span = time_span * 2  # 擴大到兩倍
        
        center = pd.to_datetime(current_range[0]) + time_span / 2
        new_start = max(center - new_span / 2, df.index.min())
        new_end = min(center + new_span / 2, df.index.max())
        
        zoom_range['x'] = [new_start, new_end]
    
    # 重新創建圖表
    fig = create_candlestick_chart(
        df, timeframe,
        measurement_data.get('points', []),
        zoom_range
    )
    
    return zoom_range, fig

# 資訊顯示回調
@app.callback(
    Output("info-display", "children"),
    [Input("current-timeframe", "data"),
     Input("chart-data", "data")]
)
def update_info(timeframe, chart_data):
    if not chart_data:
        return html.Div("暫無資料", className="text-muted")
    
    df = pd.DataFrame(chart_data)
    if df.empty:
        return html.Div("暫無資料", className="text-muted")
    
    latest_record = df.iloc[-1]
    
    return dbc.Row([
        dbc.Col([
            html.H6("當前資訊:", className="text-primary"),
            html.P(f"交易品種: {current_symbol}"),
            html.P(f"時間週期: {timeframe}"),
            html.P(f"資料筆數: {len(df)}"),
        ], width=4),
        dbc.Col([
            html.H6("最新價格:", className="text-primary"),
            html.P(f"開盤: {latest_record.get('open', 'N/A'):.5f}"),
            html.P(f"最高: {latest_record.get('high', 'N/A'):.5f}"),
            html.P(f"最低: {latest_record.get('low', 'N/A'):.5f}"),
            html.P(f"收盤: {latest_record.get('close', 'N/A'):.5f}"),
        ], width=4),
        dbc.Col([
            html.H6("成交量:", className="text-primary"),
            html.P(f"成交量: {latest_record.get('volume', 'N/A')}"),
            html.P(f"時間: {latest_record.get('timestamp', 'N/A')}"),
        ], width=4),
    ])

if __name__ == "__main__":
    app.run_server(
        debug=True,
        host=FRONTEND_HOST,
        port=FRONTEND_PORT
    ) 
import duckdb

def check_m5_candles():
    try:
        conn = duckdb.connect('database/market_data.duckdb')
        
        print("=== 檢查 M5 K線數據 ===")
        
        # 檢查 M5 K線數據數量
        count_result = conn.execute('''
            SELECT COUNT(*) 
            FROM candlestick_data_new 
            WHERE symbol = 'XAUUSD' 
            AND timeframe = 'M5'
        ''').fetchall()
        print(f"XAUUSD M5 K線數據: {count_result[0][0]} 條")
        
        # 檢查時間範圍
        if count_result[0][0] > 0:
            time_range = conn.execute('''
                SELECT 
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM candlestick_data_new 
                WHERE symbol = 'XAUUSD' 
                AND timeframe = 'M5'
            ''').fetchall()
            
            print(f"時間範圍: {time_range[0][0]} 到 {time_range[0][1]}")
        
        conn.close()
        
    except Exception as e:
        print(f"錯誤: {e}")

if __name__ == "__main__":
    check_m5_candles() 
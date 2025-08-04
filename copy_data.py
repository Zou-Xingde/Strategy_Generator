import duckdb

def copy_candlestick_data():
    """從備份資料庫複製蠟燭圖資料到新資料庫"""
    try:
        # 連接新資料庫
        dst_conn = duckdb.connect('database/market_data.duckdb')
        
        # 直接複製資料
        print("正在複製蠟燭圖資料...")
        dst_conn.execute("""
            INSERT INTO candlestick_data 
            SELECT * FROM read_duckdb('database/market_data_backup_20250722_130926.duckdb', 'candlestick_data')
        """)
        
        # 檢查複製結果
        result = dst_conn.execute("SELECT COUNT(*) FROM candlestick_data").fetchone()
        count = result[0] if result else 0
        print(f"成功複製 {count} 筆蠟燭圖資料")
        
        dst_conn.close()
        
    except Exception as e:
        print(f"複製資料時發生錯誤: {e}")

if __name__ == "__main__":
    copy_candlestick_data() 
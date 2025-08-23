import duckdb
con = duckdb.connect(r'database\market_data.duckdb')
print('SYMBOL x TF counts =>')
print(con.execute('SELECT symbol, timeframe, COUNT(*) AS n FROM candlestick_data GROUP BY 1,2 ORDER BY 1,2').fetchall())
print('Your 3 symbols TFs =>')
print(con.execute("SELECT symbol, LIST(DISTINCT timeframe) FROM candlestick_data WHERE symbol IN ('XAUUSD','US30','US100') GROUP BY 1 ORDER BY 1").fetchall())

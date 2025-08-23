import duckdb
con = duckdb.connect(r'database\market_data.duckdb')
print('SYMBOL x TF in v_candlestick_latest =>')
print(con.execute('SELECT symbol, timeframe, COUNT(*) AS n FROM v_candlestick_latest GROUP BY 1,2 ORDER BY 1,2').fetchall())

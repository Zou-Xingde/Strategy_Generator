import duckdb
con = duckdb.connect(r"database\market_data.duckdb")
con.execute("""
  DELETE FROM algorithm_statistics
  WHERE symbol='XAUUSD' AND timeframe='H4' AND algorithm_name='zigzag'
""")
print("deleted old H4 stats (if any)")

import duckdb
con = duckdb.connect(r"database\market_data.duckdb")
q = """
  SELECT symbol, timeframe, algorithm_name, total_swings,
         avg_swing_range, avg_swing_duration, max_swing_range, min_swing_range,
         created_at
  FROM algorithm_statistics
  WHERE symbol='XAUUSD' AND timeframe='H4' AND algorithm_name='zigzag'
  ORDER BY created_at DESC LIMIT 1
"""
print(con.execute(q).fetchall())

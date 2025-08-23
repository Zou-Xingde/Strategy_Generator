import duckdb

db = r'database\market_data.duckdb'
con = duckdb.connect(db)

print('Before schema:')
print(con.execute("PRAGMA table_info('algorithm_statistics')").fetchall())

con.execute("""
    CREATE TABLE algorithm_statistics_new AS
    SELECT
      symbol,
      timeframe,
      algorithm,
      run_id,
      total_swings,
      CAST(avg_swing_range   AS DECIMAL(18,5)) AS avg_swing_range,
      CAST(avg_swing_duration AS DECIMAL(18,5)) AS avg_swing_duration,
      CAST(max_swing_range   AS DECIMAL(18,5)) AS max_swing_range,
      CAST(min_swing_range   AS DECIMAL(18,5)) AS min_swing_range
    FROM algorithm_statistics
""")

con.execute("DROP TABLE algorithm_statistics")
con.execute("ALTER TABLE algorithm_statistics_new RENAME TO algorithm_statistics")

print('After schema:')
print(con.execute("PRAGMA table_info('algorithm_statistics')").fetchall())

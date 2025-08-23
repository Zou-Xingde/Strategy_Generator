import duckdb
con = duckdb.connect(r'database\market_data.duckdb')

print('Before:')
print(con.execute("PRAGMA table_info('algorithm_statistics')").fetchall())

# 把四個 DECIMAL(10,5) 放大到 DECIMAL(18,5)
cols = ['avg_swing_range','avg_swing_duration','max_swing_range','min_swing_range']
for c in cols:
    con.execute(f"ALTER TABLE algorithm_statistics ALTER COLUMN {c} TYPE DECIMAL(18,5)")

print('After:')
print(con.execute("PRAGMA table_info('algorithm_statistics')").fetchall())

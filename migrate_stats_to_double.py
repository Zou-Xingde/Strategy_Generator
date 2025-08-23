import duckdb
con = duckdb.connect(r'database\market_data.duckdb')

cols = ['avg_swing_range','avg_swing_duration','max_swing_range','min_swing_range']
for c in cols:
    con.execute(f"ALTER TABLE algorithm_statistics ALTER COLUMN {c} TYPE DOUBLE")

print('OK -> columns set to DOUBLE')
print(con.execute("PRAGMA table_info('algorithm_statistics')").fetchall())

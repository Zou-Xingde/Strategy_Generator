import duckdb

con = duckdb.connect('database/market_data.duckdb')

print('--- TABLES ---')
tables = [row[0] for row in con.execute('SHOW TABLES').fetchall()]
for t in tables:
    print(f'===== {t} =====')
    try:
        df = con.execute(f'SELECT * FROM {t} LIMIT 10').fetchdf()
        print(df.to_string(index=False))
    except Exception as e:
        print(f'Error: {e}')
    print('\n')
con.close()
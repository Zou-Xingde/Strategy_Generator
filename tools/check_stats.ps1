param(
  [string]$DB = "database\market_data.duckdb",
  [string]$Symbol = "XAUUSD",
  [string]$Algo = "zigzag",
  [string[]]$TFs = @("H1","H4","D1")
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"

$env:DB     = $DB
$env:SYMBOL = $Symbol
$env:TFS    = ($TFs -join ",")
$env:ALGO   = $Algo

$py = @'
import os, duckdb
db = os.environ["DB"]
symbol = os.environ["SYMBOL"]
tfs = os.environ.get("TFS","")
algo = os.environ.get("ALGO","zigzag")
tfs_list = [x for x in tfs.split(",") if x] if tfs else []

con = duckdb.connect(db)
if not tfs_list:
    print("no TFs provided")
else:
    placeholders = ",".join(["?"]*len(tfs_list))
    q = f"""
      SELECT *
      FROM (
        SELECT symbol, timeframe, algorithm_name, total_swings,
               avg_swing_range, avg_swing_duration, max_swing_range, min_swing_range,
               created_at,
               ROW_NUMBER() OVER (PARTITION BY timeframe ORDER BY created_at DESC) AS rn
        FROM algorithm_statistics
        WHERE symbol = ? AND algorithm_name = ? AND timeframe IN ({placeholders})
      ) t
      WHERE rn = 1
      ORDER BY timeframe;
    """
    rows = con.execute(q, [symbol, algo] + tfs_list).fetchall()
    for r in rows: print(r)
'@

$py | & python -

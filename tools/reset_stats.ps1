param(
  [string]$DB = "database\market_data.duckdb",
  [string]$Symbol = "XAUUSD",
  [string[]]$TFs = @("1h","4h","1d"),
  [string]$Algo = "zigzag"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"

function Normalize-Tf([object]$tf) {
  if ($tf -is [TimeSpan]) {
    if ($tf.Days -ge 1 -and $tf.Hours -eq 0 -and $tf.Minutes -eq 0) { return "D$($tf.Days)" }
    if ($tf.TotalHours -ge 1 -and $tf.Minutes -eq 0) { return "H$([int]$tf.TotalHours)" }
    return "$tf"
  }
  $s = "$tf"
  switch -Regex ($s.ToUpper()) {
    '^(1H|H1)$' { 'H1'; break }
    '^(4H|H4)$' { 'H4'; break }
    '^(1D|D1)$' { 'D1'; break }
    default { $s.ToUpper() }
  }
}

$normTFs = $TFs | ForEach-Object { Normalize-Tf $_ }
Write-Host ("Deleting {0} {1} {2} ..." -f $Symbol, ($normTFs -join ","), $Algo) -ForegroundColor Cyan

# 透過環境變數傳入 Python，避免轉義與引號問題
$env:DB     = $DB
$env:SYMBOL = $Symbol
$env:TFS    = ($normTFs -join ",")
$env:ALGO   = $Algo

$py = @'
import os, duckdb
db = os.environ["DB"]
symbol = os.environ["SYMBOL"]
tfs = os.environ["TFS"].split(",") if os.environ.get("TFS") else []
algo = os.environ.get("ALGO","zigzag")
con = duckdb.connect(db)

if tfs:
    placeholders = ",".join(["?"]*len(tfs))
    cnt = con.execute(
        f"SELECT COUNT(*) FROM algorithm_statistics WHERE symbol=? AND algorithm_name=? AND timeframe IN ({placeholders})",
        [symbol, algo] + tfs
    ).fetchone()[0]
    con.execute(
        f"DELETE FROM algorithm_statistics WHERE symbol=? AND algorithm_name=? AND timeframe IN ({placeholders})",
        [symbol, algo] + tfs
    )
else:
    cnt = con.execute(
        "SELECT COUNT(*) FROM algorithm_statistics WHERE symbol=? AND algorithm_name=?",
        [symbol, algo]
    ).fetchone()[0]
    con.execute(
        "DELETE FROM algorithm_statistics WHERE symbol=? AND algorithm_name=?",
        [symbol, algo]
    )

print(f"deleted {cnt} rows for {symbol} {tfs or 'ALL TFs'} {algo}")
'@

$py | & python -




param(
  [string]$Base = "http://127.0.0.1:8051",
  [string]$Symbol = "XAUUSD",
  [string[]]$Timeframes = @("1h","4h","1d"),
  [string]$Algo = "zigzag",
  [hashtable]$Params = @{ deviation = 0.5; depth = 10 },
  [int]$PollMs = 700,
  [int]$MaxPoll = 180
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"
function Normalize-Tf([object]$tf) {
  $s = "$tf"; $s = $s.Trim()
  if ($tf -is [TimeSpan]) {
    if ($tf.Days -ge 1 -and $tf.Hours -eq 0 -and $tf.Minutes -eq 0) { return "$($tf.Days)d" }
    if ($tf.TotalHours -ge 1 -and $tf.Minutes -eq 0) { return "$([int]$tf.TotalHours)h" }
    return "$s"
  }
  switch -Regex ($s.ToLower()) {
    '^(h?1|1h)$' { '1h'; break }
    '^(h?4|4h)$' { '4h'; break }
    '^(d?1|1d)$' { '1d'; break }
    default { $s }
  }
}
function Start-Swing { param([object]$TfRaw)
  $Tf = Normalize-Tf $TfRaw
  $taskId = "tf-$Tf"
  $body = @{ taskId=$taskId; symbol=$Symbol; timeframe=$Tf; algo=$Algo; params=$Params } | ConvertTo-Json -Depth 10
  $r = Invoke-RestMethod -Method Post -Uri "$Base/swing/generate" -Body $body -ContentType "application/json"
  if (-not $r.taskId) { throw "No taskId in response for TF=$Tf" }
  $tid = $r.taskId
  $tries = 0; do { Start-Sleep -Milliseconds $PollMs; $snap = Invoke-RestMethod -Method Get -Uri "$Base/swing/progress/$tid/snapshot"; $tries++ } until ($snap.done -or $tries -ge $MaxPoll)
  $allLines = @(); if ($snap.logs){$allLines+=$snap.logs}; if($snap.lines){$allLines+=$snap.lines}
  $summaryLine = $allLines | Where-Object { $_ -match '\[swing\] summary:' } | Select-Object -Last 1
  $ok = $false; if ($summaryLine -and ($summaryLine -match 'completed\s+(\d+)/failed\s+(\d+)')) { $ok = ([int]$Matches[2] -eq 0) }
  [pscustomobject]@{ timeframe=$Tf; taskId=$tid; done=$snap.done; ok=$ok; summary=$summaryLine }
}
$results = foreach ($tf in $Timeframes) { Start-Swing -TfRaw $tf }
$results | Format-Table -AutoSize


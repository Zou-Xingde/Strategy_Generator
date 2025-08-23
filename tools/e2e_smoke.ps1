param(
  [string]$Base = "http://127.0.0.1:8051",
  [string]$Symbol = "XAUUSD",
  [string[]]$TFs = @('1h','4h','1d')
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"

Write-Host "Preflight Dash..." -ForegroundColor Yellow
irm "$Base/swing/_dash-layout" -TimeoutSec 5 | Out-Null
irm "$Base/swing/_dash-dependencies" -TimeoutSec 5 | Out-Null

Write-Host "Reset stats..." -ForegroundColor Yellow
& .\tools\reset_stats.ps1 -Symbol $Symbol -TFs $TFs | Write-Host

Write-Host "Generate swings..." -ForegroundColor Yellow
& .\tools\swing_batch.ps1 -Base $Base -Symbol $Symbol -Timeframes $TFs | Write-Host

Write-Host "Check APIs..." -ForegroundColor Yellow
irm "$Base/api/candles?symbol=$Symbol&timeframe=4h&limit=50" -TimeoutSec 5 | Out-Null
irm "$Base/api/swings?symbol=$Symbol&timeframe=H4&algo=zigzag" -TimeoutSec 5 | Out-Null

Write-Host "E2E smoke PASS" -ForegroundColor Green




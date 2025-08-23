param(
  [int]$Port = 8051
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
chcp 65001 | Out-Null
$env:PYTHONIOENCODING = "utf-8"

Write-Host "Stopping old uvicorn/python..." -ForegroundColor Yellow
Get-Process -Name python,uvicorn -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Milliseconds 300

$env:PYTHONPATH = (Get-Location).Path
Write-Host "Starting server on port $Port ..." -ForegroundColor Cyan
& .\.venv\Scripts\python -m uvicorn src.frontend.app:app --host 127.0.0.1 --port $Port --reload |
  Tee-Object -Variable serverOut | Out-Null

# Preflight
for($i=0;$i -lt 20;$i++){
  try{ irm "http://127.0.0.1:$Port/swing/_dash-layout" -TimeoutSec 2 | Out-Null; break }catch{ Start-Sleep -Milliseconds 500 }
}
if($i -ge 20){ Write-Error "Server didn't respond on /swing/_dash-layout"; exit 1 }

Start-Process "http://127.0.0.1:$Port/swing/"
Write-Host "Server ready." -ForegroundColor Green




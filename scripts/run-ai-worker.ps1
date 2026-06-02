$ErrorActionPreference = "Stop"

Write-Host "Starting Python LangGraph AI worker..." -ForegroundColor Green

$ServicePath = Join-Path $PSScriptRoot "..\services\ai-worker"
Set-Location $ServicePath

if (!(Test-Path ".venv")) {
    Write-Host "Creating Python virtual environment..."
    python -m venv .venv
}

Write-Host "Installing dependencies..."
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host "Running Python LangGraph AI worker..."
.\.venv\Scripts\python.exe ai_worker.py
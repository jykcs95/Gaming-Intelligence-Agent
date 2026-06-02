$ErrorActionPreference = "Stop"

Write-Host "Starting Steam ingestion service..." -ForegroundColor Green

Set-Location "$PSScriptRoot\..\services\steam-ingestion"

if (!(Test-Path ".venv")) {
    Write-Host "Creating Python virtual environment..."
    python -m venv .venv
}

.\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host "Running Steam ingestion service..."
.\.venv\Scripts\python.exe steam_ingestion.py
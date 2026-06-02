$ErrorActionPreference = "Stop"

Write-Host "Starting Discord alert consumer..." -ForegroundColor Green

$ServicePath = Join-Path $PSScriptRoot "..\services\discord-alert-consumer"
Set-Location $ServicePath

if (!(Test-Path ".venv")) {
    Write-Host "Creating Python virtual environment..."
    python -m venv .venv
}

Write-Host "Installing dependencies..."
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host "Running Discord alert consumer..."
.\.venv\Scripts\python.exe discord_alert_consumer.py
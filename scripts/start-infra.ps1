$ErrorActionPreference = "Stop"

Write-Host "Starting Docker infrastructure..." -ForegroundColor Green

$InfraPath = Join-Path $PSScriptRoot "..\infrastructure"
Set-Location $InfraPath

docker compose up -d

Write-Host ""
Write-Host "Infrastructure status:" -ForegroundColor Cyan
docker compose ps
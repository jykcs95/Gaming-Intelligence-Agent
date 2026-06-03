$ErrorActionPreference = "Stop"

Write-Host "Stoping Docker infrastructure..." -ForegroundColor Green

$InfraPath = Join-Path $PSScriptRoot "..\infrastructure\docker"
Set-Location $InfraPath

docker down

Write-Host ""
Write-Host "Infrastructure status:" -ForegroundColor Cyan
docker compose ps
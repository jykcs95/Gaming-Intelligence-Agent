$ErrorActionPreference = "Stop"

Write-Host "Starting Spring Boot processor service..." -ForegroundColor Green

$ServicePath = Join-Path $PSScriptRoot "..\services\processor-service"
Set-Location $ServicePath

mvn spring-boot:run
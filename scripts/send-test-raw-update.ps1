$ErrorActionPreference = "Stop"

$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$gid = "demo-full-pipeline-test-$timestamp"

Write-Host "Sending full-pipeline raw update test message..." -ForegroundColor Green
Write-Host "GID: $gid" -ForegroundColor Cyan

$json = @{
    gid = $gid
    app_id = 730
    title = "Critical security update and exploit fix"
    url = "https://store.steampowered.com/news/app/730/view/$gid"
    author = "Valve"
    contents = "This update fixes a critical security exploit that negatively impacts matchmaking and player safety. Immediate mitigation required."
    date = 1717178400
    published_at = "2026-05-31T18:00:00Z"
} | ConvertTo-Json -Compress

Write-Host ""
Write-Host "Payload:" -ForegroundColor Yellow
Write-Host $json

Write-Host ""
Write-Host "Publishing to Kafka topic raw_updates..."

$json | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server kafka:9092 `
    --topic raw_updates

Write-Host ""
Write-Host "Message sent." -ForegroundColor Green
Write-Host "Check these services:" -ForegroundColor Cyan
Write-Host "- Spring Boot logs"
Write-Host "- Python AI worker logs"
Write-Host "- Discord alert consumer logs"
Write-Host "- Discord channel"
Write-Host ""
Write-Host "Verify API:"
Write-Host "curl http://localhost:8080/api/alerts/$gid"
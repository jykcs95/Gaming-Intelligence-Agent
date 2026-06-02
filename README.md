## Observability Demo

### Prometheus Metrics

Spring Boot exposes metrics at:

http://localhost:8080/actuator/prometheus

Useful custom metrics:

- raw_updates_consumed_total
- raw_updates_processed_success_total
- raw_updates_duplicate_skipped_total
- raw_updates_processed_failure_total
- processed_updates_consumed_total
- ai_analysis_success_total
- ai_analysis_failure_total
- ai_analysis_messages_consumed_total
- alerts_created_total
- alerts_skipped_total
- alerts_published_total
- alerts_generated_total
- alerts_generated_by_severity_total
- alerts_database_total
- alerts_database_by_severity

### DLQ Demo

Bad Kafka messages are routed to DLQ topics.

Raw update DLQ topic:

raw_updates_dlq

Example bad message:

```json
{
  "gid": "dlq-test-001",
  "appId": 730,
  "title": "Bad DLQ test message",
  "url": "https://store.steampowered.com/news/app/730/view/dlq-test-001",
  "author": "Valve",
  "contents": "This message intentionally uses appId instead of app_id so it should fail validation or processing.",
  "date": 1717178400,
  "published_at": "2026-05-31T18:00:00Z"
}
```

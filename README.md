# Event-Driven Gaming Intelligence Agent

An event-driven backend system that ingests gaming update events, processes them through Kafka, analyzes them with a local Ollama LLM, generates alerts, persists results in PostgreSQL, and exposes REST APIs, metrics, and dashboards.

This project demonstrates backend engineering, distributed systems, event-driven architecture, AI application integration, observability, and production-style error handling.

---

## Tech Stack

- Java 21
- Spring Boot
- Spring Kafka
- Apache Kafka
- PostgreSQL
- pgvector
- Hibernate JSONB mapping
- Ollama
- Prometheus
- Grafana
- Swagger / OpenAPI
- Docker Compose
- JUnit 5
- AssertJ

---

## Architecture

```text
raw_updates
  -> SteamUpdateConsumer
  -> SteamUpdateProcessingService
  -> steam_events table
  -> processed_message_ledger table
  -> ProcessedUpdateProducer
  -> processed_updates topic
  -> ProcessedUpdateConsumer
  -> OllamaClientService
  -> AiAnalysisService
  -> ai_analysis table
  -> AiAnalysisProducer
  -> ai_analysis topic
  -> AiAnalysisConsumer
  -> AlertRuleService
  -> AlertPersistenceService
  -> alerts table
  -> AlertProducer
  -> alerts topic
```

### Main Features

- Kafka-based event-driven pipeline
- Idempotent message processing
- AI analysis using local Ollama
- Alert generation based on rule evaluation
- PostgreSQL persistence
- JSONB support for raw payloads and generated analysis fields
- REST API for querying alerts
- Swagger/OpenAPI documentation
- Retry and DLQ handling
- Prometheus metrics
- Grafana dashboard
- Unit-tested alert rule logic

### Kafka Topics

- `raw_updates`
- `raw_updates_dlq`
- `processed_updates`
- `processed_updates_dlq`
- `ai_analysis`
- `alerts`

**Kafka listener setup:**

- Internal Docker listener: `kafka:9092`
- Host listener: `localhost:29092`

### PostgreSQL Tables

- `steam_events`
- `processed_message_ledger`
- `ai_analysis`
- `embeddings`
- `alerts`

**Database:** `gaming_ai`  
The `pgvector` extension is enabled.

---

## Running the Project

### 1. Start Infrastructure

From the project root:

```bash
docker compose up -d
```

This starts:

- Kafka
- Kafdrop
- PostgreSQL + pgvector
- Prometheus
- Grafana
- Ollama

### 2. Start Spring Boot Processor Service

```bash
cd services/processor-service
./mvnw spring-boot:run
```

On Windows PowerShell:

```powershell
cd services/processor-service
.\mvnw.cmd spring-boot:run
```

---

## Valid Kafka Test Message

Send this one-line JSON to the `raw_updates` topic:

```json
{
  "gid": "readme-demo-test-001",
  "app_id": 730,
  "title": "Critical security update and exploit fix",
  "url": "https://steampowered.com",
  "author": "Valve",
  "contents": "This update fixes a critical security exploit that negatively impacts matchmaking and player safety. Immediate mitigation required.",
  "date": 1717178400,
  "published_at": "2026-05-31T18:00:00Z"
}
```

> **Important:** `SteamUpdateMessage` expects `snake_case` fields: `app_id`, `published_at`. Do not use `appId` or `publishedAt`.

### Kafka Console Producer Example

Command:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic raw_updates
```

Then paste the one-line JSON message.

---

## Alert Rules

Alert rules are evaluated by `AlertRuleService`.

- `importanceScore >= 8` -> `HIGH_IMPORTANCE_SCORE`
- `updateType = security` -> `SECURITY_UPDATE`
- `sentiment = negative` and `confidence >= 0.75` -> `NEGATIVE_SENTIMENT_HIGH_CONFIDENCE`
- `updateType = balance_change` and `importanceScore >= 7` -> `IMPORTANT_BALANCE_CHANGE`

**Severity logic:**

- `security` or `importanceScore >= 9` -> critical
- `negative high confidence` -> high
- `important balance change` -> medium
- otherwise -> high

---

## REST API

Base URL: `http://localhost:8080`

**Available endpoints:**

- `GET /api/alerts`
- `GET /api/alerts?severity=medium&limit=10`
- `GET /api/alerts/high-priority`
- `GET /api/alerts/{gid}`
- `GET /api/alerts/severity/{severity}`
- `GET /api/alerts/summary`

**Example:**

```bash
curl "http://localhost:8080/api/alerts/high-priority"
```

### Swagger / OpenAPI

- **Swagger UI:** `http://localhost:8080/swagger-ui/index.html`
- **OpenAPI JSON:** `http://localhost:8080/v3/api-docs`

---

## Observability

### Endpoints & Interfaces

- **Spring Boot Prometheus Endpoint:** `http://localhost:8080/actuator/prometheus`
- **Prometheus:** `http://localhost:9090`
- **Grafana:** `http://localhost:3000` (Main dashboard: _Gaming Intelligence Pipeline_)

### Custom Metrics

- **Raw update metrics:** `raw_updates_consumed_total`, `raw_updates_processed_success_total`, `raw_updates_duplicate_skipped_total`, `raw_updates_processed_failure_total`
- **AI analysis metrics:** `processed_updates_consumed_total`, `ai_analysis_success_total`, `ai_analysis_failure_total`
- **Alert generation metrics:** `ai_analysis_messages_consumed_total`, `alerts_created_total`, `alerts_skipped_total`, `alerts_published_total`, `alert_generation_failure_total`
- **Alert persistence metrics:** `alerts_generated_total`, `alerts_generated_by_severity_total`
- **Database-backed alert gauges:** `alerts_database`, `alerts_database_by_severity`

> **Note:** Counter metrics reset when the Spring Boot app restarts. Database-backed gauges do not reset to zero because they are calculated from PostgreSQL.

### Prometheus Scrape Config

Prometheus should scrape both itself and the Spring Boot processor service.

Example `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "processor-service"
    metrics_path: "/actuator/prometheus"
    static_configs:
      - targets: ["host.docker.internal:8080"]
```

_`host.docker.internal:8080` is used because Prometheus runs in Docker while Spring Boot runs on the host machine._

### Grafana Provisioning

Grafana provisioning files should live under the existing monitoring directory.

Example structure:

```text
infrastructure/monitoring/grafana/provisioning/datasources
infrastructure/monitoring/grafana/provisioning/dashboards
infrastructure/monitoring/grafana/dashboards
```

**Prometheus datasource provisioning:**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

**Dashboard provider:**

```yaml
apiVersion: 1
providers:
  - name: "Gaming Intelligence Dashboards"
    orgId: 1
    folder: "Gaming Intelligence"
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/dashboards/json
```

_When exporting dashboards from Grafana, set only the top-level dashboard id to null. Do not change panel IDs._

---

## DLQ Demo

Bad Kafka messages are routed to DLQ topics.

- **Raw update DLQ topic:** `raw_updates_dlq`

**Example bad message:**

```json
{
  "gid": "readme-dlq-test-001",
  "appId": 730,
  "title": "Bad DLQ test message",
  "url": "https://steampowered.com",
  "author": "Valve",
  "contents": "This message intentionally uses appId instead of app_id so it should fail validation or processing.",
  "date": 1717178400,
  "published_at": "2026-05-31T18:00:00Z"
}
```

This message is intentionally wrong because it uses `appId` instead of `app_id`.

- **Check DLQ in Kafdrop:** `http://localhost:9000`
- **Or use Kafka CLI:**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic raw_updates_dlq \
  --from-beginning \
  --max-messages 5
```

---

## Useful Demo Flow

1. **Start infrastructure:** `docker compose up -d`
2. **Start Spring Boot:** `cd services/processor-service && .\mvnw.cmd spring-boot:run`
3. **Send a good Kafka message:**
   ```json
   {
     "gid": "demo-good-001",
     "app_id": 730,
     "title": "Critical security update and exploit fix",
     "url": "https://steampowered.com",
     "author": "Valve",
     "contents": "This update fixes a critical security exploit that negatively impacts matchmaking and player safety. Immediate mitigation required.",
     "date": 1717178400,
     "published_at": "2026-05-31T18:00:00Z"
   }
   ```
4. **Confirm alert API:** `curl "http://localhost:8080/api/alerts/high-priority"`
5. **Open Swagger:** `http://localhost:8080/swagger-ui/index.html`
6. **Open Grafana:** `http://localhost:3000` (Dashboard: _Gaming Intelligence Pipeline_)
7. **Send a bad Kafka message:**
   ```json
   {
     "gid": "demo-dlq-001",
     "appId": 730,
     "title": "Bad DLQ test message",
     "url": "https://steampowered.com",
     "author": "Valve",
     "contents": "This message intentionally uses appId instead of app_id so it should fail validation or processing.",
     "date": 1717178400,
     "published_at": "2026-05-31T18:00:00Z"
   }
   ```
8. **Confirm DLQ in Kafdrop:** `http://localhost:9000` (Topic: `raw_updates_dlq`)

---

## Local Run Scripts

From the project root:

```powershell
.\scripts\start-infra.ps1

Start the Spring Boot processor:

.\scripts\run-processor-service.ps1

Start the Steam ingestion service:

.\scripts\run-steam-ingestion.ps1

Start the Discord alert consumer:

.\scripts\run-discord-alert-consumer.ps1

Stop infrastructure:

.\scripts\stop-infra.ps1

If PowerShell blocks script execution for the current terminal session:

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

---

## Running Tests

From `services/processor-service`:

```bash
./mvnw test
```

On Windows PowerShell:

```powershell
.\mvnw.cmd test
```

---

## Test the scripts

Open separate PowerShell windows for long-running services.

Terminal 1:

```powershell
cd ..\gaming-intelligence-agent
.\scripts\start-infra.ps1

Terminal 2:

cd ..\gaming-intelligence-agent
.\scripts\run-processor-service.ps1

Terminal 3:

cd ..\gaming-intelligence-agent
.\scripts\run-discord-alert-consumer.ps1

Terminal 4:

cd ..\gaming-intelligence-agent
.\scripts\run-steam-ingestion.ps1

Then test full pipeline with a new gid:

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server kafka:9092 `
  --topic raw_updates
```

---

## Current Completed Features

- Kafka topic initialization
- Kafka retry and DLQ handling
- Raw update ingestion
- Idempotent message processing

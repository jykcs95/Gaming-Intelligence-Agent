# Event-Driven Gaming Intelligence Agent

A local-first, zero-cost, event-driven intelligence pipeline for monitoring Steam game updates, analyzing them with local AI, generating high-priority alerts, exposing REST APIs, and visualizing system metrics.

This project is designed as a portfolio-grade backend/distributed-systems project showcasing:

- Event-driven architecture
- Apache Kafka message pipelines
- Java 21 / Spring Boot microservice development
- Python service workers
- Local Ollama AI inference
- LangGraph orchestration
- Pydantic validation
- PostgreSQL persistence
- pgvector-ready database architecture
- Idempotent processing
- Retry/DLQ resiliency
- Prometheus metrics
- Grafana dashboards
- Swagger/OpenAPI documentation
- Discord live alert delivery

---

## Architecture Overview

Current implemented flow:

```text
Steam Web API / Manual Kafka Test
        |
        v
Python Steam Ingestion Service
        |
        v
Kafka topic: raw_updates
        |
        v
Spring Boot Processor Service
        |
        v
PostgreSQL:
- steam_events
- processed_message_ledger
        |
        v
Kafka topic: processed_updates
        |
        v
Python LangGraph AI Worker
        |
        v
Kafka topic: ai_analysis
        |
        v
Spring Boot Alert Generation
        |
        v
PostgreSQL:
- ai_analysis
- alerts
        |
        v
Kafka topic: alerts
        |
        v
Python Discord Alert Consumer
        |
        v
Discord Webhook

The project originally targeted separate Python ingestion, AI orchestration, and live alerting services. The current implementation includes those Python services while keeping alert generation, APIs, persistence, and resiliency in Spring Boot.

Tech Stack
Backend
Java 21
Spring Boot 3.3+
Spring Kafka
Spring Data JPA
Spring Boot Actuator
Micrometer
Swagger/OpenAPI
Python Services
Python 3
kafka-python
requests
prometheus-client
python-dotenv
LangGraph
Pydantic
Infrastructure
Docker Compose
Apache Kafka in KRaft mode
PostgreSQL
pgvector
Prometheus
Grafana
Ollama
AI
Local Ollama
Model: llama3.2:latest
Python LangGraph AI worker
Pydantic structured output validation
Kafka Topics

Core topics:

raw_updates
raw_updates_retry
raw_updates_dlq
processed_updates
processed_updates_dlq
ai_analysis
alerts

Recommended topic settings:

partitions: 3
replication factor: 1

Kafka listeners:

Docker internal listener: kafka:9092
Host listener: localhost:29092

Use kafka:9092 from inside Docker containers.

Use localhost:29092 from Windows/PowerShell or local Python/Spring Boot services.

Database Tables

Current PostgreSQL database:

gaming_ai

Current tables:

steam_events
processed_message_ledger
ai_analysis
embeddings
alerts

PostgreSQL has pgvector enabled for future semantic search and embedding features.

Services
1. Spring Boot Processor Service

Path:

services/processor-service

Responsibilities:

Consume raw_updates
Validate Steam update messages
Persist raw Steam events
Deduplicate messages using Postgres ledger
Publish processed updates to processed_updates
Consume ai_analysis
Persist AI analysis
Apply alert rules
Persist alerts
Publish alerts to alerts
Expose alert REST APIs
Expose Actuator/Prometheus metrics
Expose Swagger/OpenAPI documentation

Package root:

com.gamingintel.processor_service
2. Python Steam Ingestion Service

Path:

services/steam-ingestion

Responsibilities:

Poll Steam Web API for AppID 730
Publish valid raw update messages to Kafka topic raw_updates
Preserve expected snake_case fields:
gid
app_id
title
url
author
contents
date
published_at
Expose Prometheus metrics on port 8000

Metrics:

steam_api_latency_seconds
ingestion_loop_duration_seconds
steam_api_errors_total
steam_ingestion_kafka_publish_success_total
steam_ingestion_kafka_publish_failure_total
steam_updates_seen_total
3. Python LangGraph AI Worker

Path:

services/ai-worker

Responsibilities:

Consume processed updates from Kafka topic processed_updates
Build AI analysis prompt
Call local Ollama
Validate structured output with Pydantic
Publish AI analysis messages to Kafka topic ai_analysis
Expose Prometheus metrics on port 8002

Metrics:

python_ai_processed_updates_consumed_total
python_ai_analysis_success_total
python_ai_analysis_failure_total
python_ai_analysis_published_total
python_ai_worker_errors_total
python_ai_llm_inference_latency_seconds
python_ai_worker_message_duration_seconds

When using the Python AI worker, disable the Spring Boot AI consumer:

app:
  ai:
    spring-consumer-enabled: false
4. Python Discord Alert Consumer

Path:

services/discord-alert-consumer

Responsibilities:

Consume alerts from Kafka topic alerts
Send critical/high/medium alerts to Discord webhook
Support DRY_RUN=true mode for local testing
Expose Prometheus metrics on port 8001

Metrics:

discord_alert_messages_consumed_total
discord_alert_send_success_total
discord_alert_send_failure_total
discord_alert_consumer_errors_total
discord_webhook_latency_seconds
Alert Rules

Current alert rule conditions:

importanceScore >= 8
-> HIGH_IMPORTANCE_SCORE

updateType = security
-> SECURITY_UPDATE

sentiment = negative and confidence >= 0.75
-> NEGATIVE_SENTIMENT_HIGH_CONFIDENCE

updateType = balance_change and importanceScore >= 7
-> IMPORTANT_BALANCE_CHANGE

Severity rules:

security or importanceScore >= 9
-> critical

negative high confidence
-> high

important balance change
-> medium

otherwise
-> high
REST APIs

Base path:

/api/alerts

Implemented endpoints:

GET /api/alerts
GET /api/alerts?severity=medium&limit=10
GET /api/alerts/{gid}
GET /api/alerts/severity/{severity}
GET /api/alerts/summary
GET /api/alerts/high-priority

Important controller route order:

/summary
/high-priority
/severity/{severity}
/{gid}

Specific routes must be declared before the catch-all /{gid} route.

Useful URLs
Swagger UI:
http://localhost:8080/swagger-ui/index.html

OpenAPI JSON:
http://localhost:8080/v3/api-docs

Spring Boot Prometheus metrics:
http://localhost:8080/actuator/prometheus

Steam ingestion metrics:
http://localhost:8000

Discord alert consumer metrics:
http://localhost:8001

Python AI worker metrics:
http://localhost:8002

Prometheus:
http://localhost:9090

Grafana:
http://localhost:3000

Kafdrop:
http://localhost:9000

Default Grafana login is usually:

username: admin
password: admin
Environment Files

Do not commit real .env files.

Commit only .env.example files.

Recommended .gitignore entries:

# Python
.venv/
__pycache__/
*.pyc

# Local env/secrets
.env
*.env

# Steam ingestion local state
services/steam-ingestion/.steam_ingestion_seen_gids.json
Example Environment Files
Steam Ingestion

Path:

services/steam-ingestion/.env.example

Example:

STEAM_APP_ID=730
POLL_INTERVAL_SECONDS=300
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
RAW_UPDATES_TOPIC=raw_updates
PROMETHEUS_PORT=8000
STEAM_NEWS_COUNT=20
STEAM_NEWS_MAX_LENGTH=5000
AI Worker

Path:

services/ai-worker/.env.example

Example:

KAFKA_BOOTSTRAP_SERVERS=localhost:29092
PROCESSED_UPDATES_TOPIC=processed_updates
AI_ANALYSIS_TOPIC=ai_analysis
CONSUMER_GROUP_ID=python-langgraph-ai-worker
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2:latest
PROMETHEUS_PORT=8002
Discord Alert Consumer

Path:

services/discord-alert-consumer/.env.example

Example:

KAFKA_BOOTSTRAP_SERVERS=localhost:29092
ALERTS_TOPIC=alerts
CONSUMER_GROUP_ID=discord-alert-consumer
PROMETHEUS_PORT=8001
DRY_RUN=true
DISCORD_WEBHOOK_URL=

For real Discord delivery:

DRY_RUN=false
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your-real-webhook

Never commit the real webhook URL.

Local Run Scripts

Run all commands from the project root:

cd C:\Users\Ji\Projects\gaming-intelligence-agent

If PowerShell blocks script execution for the current terminal session:

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
Start Docker Infrastructure
.\scripts\start-infra.ps1

This starts:

Kafka
Kafdrop
PostgreSQL
Prometheus
Grafana
Ollama
Start Spring Boot Processor Service
.\scripts\run-processor-service.ps1
Start Python LangGraph AI Worker
.\scripts\run-ai-worker.ps1
Start Python Steam Ingestion Service
.\scripts\run-steam-ingestion.ps1
Start Python Discord Alert Consumer
.\scripts\run-discord-alert-consumer.ps1
Stop Docker Infrastructure
.\scripts\stop-infra.ps1
Manual Local Startup Without Scripts
Start Infrastructure
cd C:\Users\Ji\Projects\gaming-intelligence-agent\infrastructure

docker compose up -d
docker compose ps
Start Spring Boot
cd C:\Users\Ji\Projects\gaming-intelligence-agent\services\processor-service

mvn spring-boot:run
Start AI Worker
cd C:\Users\Ji\Projects\gaming-intelligence-agent\services\ai-worker

python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe ai_worker.py
Start Discord Alert Consumer
cd C:\Users\Ji\Projects\gaming-intelligence-agent\services\discord-alert-consumer

python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe discord_alert_consumer.py
Start Steam Ingestion
cd C:\Users\Ji\Projects\gaming-intelligence-agent\services\steam-ingestion

python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe steam_ingestion.py
Kafka Test Messages

Important:

Use one-line JSON only.

The Kafka console producer sends one Kafka message per line. Pretty-printed JSON will break the payload into multiple bad messages.

Full Pipeline Test

Send this to:

raw_updates

Command:

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server kafka:9092 `
  --topic raw_updates

Paste as one line:

{"gid":"readme-full-pipeline-test-001","app_id":730,"title":"Critical security update and exploit fix","url":"https://store.steampowered.com/news/app/730/view/readme-full-pipeline-test-001","author":"Valve","contents":"This update fixes a critical security exploit that negatively impacts matchmaking and player safety. Immediate mitigation required.","date":1717178400,"published_at":"2026-05-31T18:00:00Z"}

Expected flow:

raw_updates
-> Spring Boot processor
-> processed_updates
-> Python LangGraph AI worker
-> ai_analysis
-> Spring Boot alert generation
-> alerts
-> Python Discord consumer
-> Discord

Use a new unique gid for every test.

Direct Discord Consumer Test

Send this to:

alerts

Command:

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server kafka:9092 `
  --topic alerts

Paste as one line:

{"alertId":"readme-discord-test-001","gid":"readme-discord-test-001","severity":"critical","importanceScore":9,"sentiment":"negative","confidence":0.92,"updateType":"security","summary":"Testing real Discord webhook delivery from the Gaming Intelligence Agent.","keyPoints":["SECURITY_UPDATE","HIGH_IMPORTANCE_SCORE"],"source":"manual-kafka-test","createdAt":"2026-06-02T18:00:00Z"}

Expected flow:

alerts
-> Python Discord consumer
-> Discord
Message Formats
Raw Steam Update Message

Topic:

raw_updates

Expected JSON:

{
  "gid": "unique-test-id",
  "app_id": 730,
  "title": "Critical security update and exploit fix",
  "url": "https://store.steampowered.com/news/app/730/view/unique-test-id",
  "author": "Valve",
  "contents": "This update fixes a critical security exploit that negatively impacts matchmaking and player safety. Immediate mitigation required.",
  "date": 1717178400,
  "published_at": "2026-05-31T18:00:00Z"
}

Important:

Use app_id, not appId.
Use published_at, not publishedAt.
AI Analysis Message

Topic:

ai_analysis

Expected JSON:

{
  "gid": "unique-test-id",
  "summary": "Critical exploit fix detected.",
  "sentiment": "negative",
  "confidence": 0.92,
  "importanceScore": 9,
  "updateType": "security",
  "keyPoints": [
    "SECURITY_UPDATE",
    "HIGH_IMPORTANCE_SCORE"
  ],
  "createdAt": "2026-06-02T18:00:00Z",
  "source": "python-langgraph-ai-worker"
}

The Spring Boot consumer supports both camelCase and snake_case aliases for AI worker compatibility.

Alert Message

Topic:

alerts

Expected JSON:

{
  "alertId": "alert-id",
  "gid": "unique-test-id",
  "severity": "critical",
  "importanceScore": 9,
  "sentiment": "negative",
  "confidence": 0.92,
  "updateType": "security",
  "summary": "Critical exploit fix detected.",
  "keyPoints": [
    "SECURITY_UPDATE",
    "HIGH_IMPORTANCE_SCORE"
  ],
  "source": "alert-rule-service",
  "createdAt": "2026-06-02T18:00:00Z"
}
Prometheus Configuration

Prometheus should scrape:

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "processor-service"
    metrics_path: "/actuator/prometheus"
    static_configs:
      - targets: ["host.docker.internal:8080"]

  - job_name: "steam-ingestion"
    static_configs:
      - targets: ["host.docker.internal:8000"]

  - job_name: "discord-alert-consumer"
    static_configs:
      - targets: ["host.docker.internal:8001"]

  - job_name: "python-ai-worker"
    static_configs:
      - targets: ["host.docker.internal:8002"]

Verify targets:

http://localhost:9090/targets

Expected targets:

prometheus
processor-service
steam-ingestion
discord-alert-consumer
python-ai-worker
Grafana Notes

Grafana runs at:

http://localhost:3000

The Prometheus datasource should be provisioned as:

apiVersion: 1

datasources:
  - name: Prometheus
    uid: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true

Dashboard datasource references should use:

{
  "type": "prometheus",
  "uid": "Prometheus"
}

If Grafana fails with:

Datasource provisioning error: data source not found

then the Grafana Docker volume may contain stale datasource state.

For local development only, reset Grafana volume:

cd C:\Users\Ji\Projects\gaming-intelligence-agent\infrastructure

docker compose down
docker volume ls
docker volume rm infrastructure_grafana_data
docker compose up -d

The exact volume name may differ. Check docker volume ls.

Resiliency Features

Implemented or partially implemented resiliency patterns:

Kafka-based asynchronous processing
Topic partitioning
DLQ topics
Retry/DLQ configuration
ErrorHandlingDeserializer for safer Kafka deserialization
Idempotent consumer behavior using Postgres ledger
Alert persistence idempotency by gid
Local-first operation
Prometheus observability
DB-backed gauges for durable alert counts

Important development note:

Kafka DLQ topics can be cleaned in local development by deleting and recreating topics. Do not use that approach in production.

Local Development Troubleshooting
Kafka Pretty JSON Problem

Bad:

{
  "gid": "test"
}

When pasted into kafka-console-producer, this creates multiple messages.

Good:

{"gid":"test","app_id":730,"title":"Test","url":"https://example.com","author":"Valve","contents":"Test contents","date":1717178400,"published_at":"2026-05-31T18:00:00Z"}

Always paste one-line JSON.

Raw Update vs Alert Topic

Raw Steam update messages go to:

raw_updates

Alert messages go to:

alerts

If testing Discord directly, send alert-shaped JSON to alerts.

If testing the whole pipeline, send Steam update-shaped JSON to raw_updates.

Ollama localhost vs 127.0.0.1

On this local setup, localhost and 127.0.0.1 previously pointed to different Ollama endpoints.

The working endpoint used by the project is:

http://localhost:11434

Model:

llama3.2:latest

Check models:

curl http://localhost:11434/api/tags
Python Virtual Environment Activation Issue

If this fails:

.\.venv\Scripts\Activate.ps1

Use the venv Python directly:

.\.venv\Scripts\python.exe your_script.py

Or allow script execution for the current PowerShell session:

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
Discord Webhook Does Not Send

Check:

DRY_RUN=false
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your-real-webhook

Make sure the webhook URL is for the exact Discord channel.

Direct PowerShell test:

$webhookUrl = "https://discord.com/api/webhooks/..."

$body = @{
  content = "Test message from PowerShell webhook"
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri $webhookUrl `
  -Method Post `
  -ContentType "application/json" `
  -Body $body

A successful Discord webhook request usually returns HTTP 204 No Content.

Common Verification Commands
Docker Status
cd C:\Users\Ji\Projects\gaming-intelligence-agent\infrastructure

docker compose ps
Grafana Logs
docker compose logs grafana --tail=100
Prometheus Logs
docker compose logs prometheus --tail=100
Kafka Topic List
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --list
Consume Raw Updates
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic raw_updates `
  --from-beginning
Consume Processed Updates
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic processed_updates `
  --from-beginning
Consume AI Analysis
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic ai_analysis `
  --from-beginning
Consume Alerts
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic alerts `
  --from-beginning
Project Status

Implemented:

Docker Kafka infrastructure
PostgreSQL + pgvector
Spring Boot processor service
Kafka topic initialization
Kafka retry/DLQ configuration
Raw update ingestion pipeline
Idempotent processing ledger
AI analysis persistence
Alert generation
Alert persistence
Alert APIs
Swagger/OpenAPI
Prometheus metrics
Grafana dashboard support
Python Steam ingestion service
Python Discord alert consumer
Python LangGraph AI worker
Pydantic AI output validation
Local Ollama inference
PowerShell run scripts

Future improvements:

Add automated integration tests
Add CI workflow
Add Dockerfiles for Python services
Add docker-compose profiles for local demo modes
Add semantic search APIs using embeddings and pgvector
Add richer Grafana dashboards
Add alert severity trend panels
Add DLQ replay tooling
Add README screenshots/GIFs
Add architecture diagram image
Portfolio Summary

This project demonstrates a local-first event-driven architecture with multiple independently running services, Kafka-based communication, Postgres persistence, local AI inference, live alert delivery, and full observability.

It is designed to highlight backend engineering skills across:

Distributed systems
Message brokers
Microservices
Reliability engineering
Observability
AI application engineering
API design
Data persistence
Developer tooling
```

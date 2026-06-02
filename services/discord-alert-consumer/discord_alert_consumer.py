import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()
# -----------------------------
# Configuration
# -----------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "discord-alert-consumer")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8001"))
HTTP_TIMEOUT_SECONDS = 15


# -----------------------------
# Logging
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [discord-alert-consumer] %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------
# Prometheus Metrics
# -----------------------------

discord_alert_messages_consumed_total = Counter(
    "discord_alert_messages_consumed_total",
    "Total number of alert messages consumed from Kafka",
)

discord_alert_send_success_total = Counter(
    "discord_alert_send_success_total",
    "Total number of alerts successfully sent to Discord",
)

discord_alert_send_failure_total = Counter(
    "discord_alert_send_failure_total",
    "Total number of failed Discord alert send attempts",
)

discord_alert_consumer_errors_total = Counter(
    "discord_alert_consumer_errors_total",
    "Total number of Discord alert consumer errors",
)

discord_webhook_latency_seconds = Histogram(
    "discord_webhook_latency_seconds",
    "Latency of Discord webhook requests in seconds",
)


# -----------------------------
# Shutdown Handling
# -----------------------------

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping Discord alert consumer...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# -----------------------------
# Helpers
# -----------------------------

def get_value(alert: Dict[str, Any], camel_case_key: str, snake_case_key: str, default: Any = None) -> Any:
    """
    Supports both Java-style camelCase and Python-style snake_case payloads.
    Example:
    - importanceScore
    - importance_score
    """
    if camel_case_key in alert:
        return alert.get(camel_case_key)

    if snake_case_key in alert:
        return alert.get(snake_case_key)

    return default


def normalize_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "alert_id": get_value(alert, "alertId", "alert_id", "unknown-alert-id"),
        "gid": alert.get("gid", "unknown-gid"),
        "severity": str(alert.get("severity", "unknown")).lower(),
        "importance_score": get_value(alert, "importanceScore", "importance_score", 0),
        "sentiment": alert.get("sentiment", "unknown"),
        "confidence": alert.get("confidence", 0),
        "update_type": get_value(alert, "updateType", "update_type", "unknown"),
        "summary": alert.get("summary", ""),
        "key_points": get_value(alert, "keyPoints", "key_points", []),
        "source": alert.get("source", "unknown"),
        "created_at": get_value(alert, "createdAt", "created_at", "unknown"),
    }


def severity_icon(severity: str) -> str:
    severity = severity.lower()

    if severity == "critical":
        return "🚨"

    if severity == "high":
        return "⚠️"

    if severity == "medium":
        return "🟡"

    return "ℹ️"


def should_send_to_discord(alert: Dict[str, Any]) -> bool:
    """
    Only send useful alerts to Discord.

    Current behavior:
    - critical: send
    - high: send
    - medium: send
    - low/unknown: skip

    You can tighten this later if Discord gets noisy.
    """
    severity = str(alert.get("severity", "")).lower()
    return severity in {"critical", "high", "medium"}


def build_discord_payload(alert: Dict[str, Any]) -> Dict[str, Any]:
    icon = severity_icon(alert["severity"])

    key_points = alert.get("key_points") or []
    if isinstance(key_points, list):
        key_points_text = "\n".join(f"- {point}" for point in key_points)
    else:
        key_points_text = str(key_points)

    content = (
        f"{icon} **Gaming Intelligence Alert**\n\n"
        f"**Severity:** {alert['severity'].upper()}\n"
        f"**GID:** `{alert['gid']}`\n"
        f"**Update Type:** `{alert['update_type']}`\n"
        f"**Importance Score:** `{alert['importance_score']}`\n"
        f"**Sentiment:** `{alert['sentiment']}`\n"
        f"**Confidence:** `{alert['confidence']}`\n\n"
        f"**Summary:**\n{alert['summary'] or 'No summary provided.'}\n"
    )

    if key_points_text:
        content += f"\n**Key Points:**\n{key_points_text}\n"

    content += f"\n**Source:** `{alert['source']}`"

    return {
        "content": content
    }


def send_to_discord(alert: Dict[str, Any]) -> None:
    if DRY_RUN:
        logger.info("DRY_RUN=true. Discord message would be:")
        logger.info(json.dumps(build_discord_payload(alert), indent=2))
        discord_alert_send_success_total.inc()
        return

    if not DISCORD_WEBHOOK_URL:
        raise ValueError("DISCORD_WEBHOOK_URL is required when DRY_RUN=false")

    payload = build_discord_payload(alert)

    logger.info("Sending alert to Discord webhook URL present=%s", bool(DISCORD_WEBHOOK_URL))

    with discord_webhook_latency_seconds.time():
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json=payload,
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    logger.info("Discord webhook response status=%s body=%s", response.status_code, response.text)

    response.raise_for_status()
    discord_alert_send_success_total.inc()


def create_consumer() -> KafkaConsumer:
    logger.info("Connecting to Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Consuming topic: %s", ALERTS_TOPIC)
    logger.info("Consumer group: %s", CONSUMER_GROUP_ID)

    return KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
    )


def handle_alert_message(raw_alert: Dict[str, Any]) -> None:
    logger.info("RAW ALERT RECEIVED: %s", json.dumps(raw_alert, indent=2))
    discord_alert_messages_consumed_total.inc()

    alert = normalize_alert(raw_alert)

    logger.info(
        "Consumed alert gid=%s severity=%s update_type=%s",
        alert["gid"],
        alert["severity"],
        alert["update_type"],
    )

    if not should_send_to_discord(alert):
        logger.info(
            "Skipping Discord send for gid=%s severity=%s",
            alert["gid"],
            alert["severity"],
        )
        return

    send_to_discord(alert)

    logger.info(
        "Discord alert handled successfully gid=%s severity=%s",
        alert["gid"],
        alert["severity"],
    )


def main() -> int:
    logger.info("Starting Discord alert consumer")
    logger.info("Prometheus metrics port: %s", PROMETHEUS_PORT)
    logger.info("DRY_RUN: %s", DRY_RUN)

    start_http_server(PROMETHEUS_PORT)

    consumer = create_consumer()

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)

            for _, messages in records.items():
                for message in messages:
                    try:
                        handle_alert_message(message.value)
                    except Exception as exc:
                        discord_alert_send_failure_total.inc()
                        discord_alert_consumer_errors_total.inc()
                        logger.exception("Failed to handle alert message: %s", exc)

            time.sleep(0.1)

    finally:
        consumer.close()
        logger.info("Discord alert consumer stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
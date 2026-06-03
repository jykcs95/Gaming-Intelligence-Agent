import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server


# -----------------------------
# Env loading
# -----------------------------

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent

load_dotenv(BASE_DIR / ".env")


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

SENT_ALERTS_STATE_FILE = Path(
    os.getenv("SENT_ALERTS_STATE_FILE", str(BASE_DIR / ".discord_sent_alerts.json"))
)

WATCHLIST_FILE = Path(
    os.getenv(
        "WATCHLIST_FILE",
        str(PROJECT_ROOT / "services" / "steam-ingestion" / "watchlist.json"),
    )
)

ENABLE_PERSONALIZED_FILTERING = (
    os.getenv("ENABLE_PERSONALIZED_FILTERING", "true").lower() == "true"
)

ALWAYS_SEND_SECURITY_OR_CRITICAL = (
    os.getenv("ALWAYS_SEND_SECURITY_OR_CRITICAL", "true").lower() == "true"
)


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

discord_alert_duplicates_skipped_total = Counter(
    "discord_alert_duplicates_skipped_total",
    "Total number of duplicate Discord alerts skipped",
)

discord_alert_personalized_filter_skipped_total = Counter(
    "discord_alert_personalized_filter_skipped_total",
    "Total number of Discord alerts skipped by personalized keyword filtering",
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
# State / Dedupe Helpers
# -----------------------------

def load_sent_alert_keys() -> Set[str]:
    if not SENT_ALERTS_STATE_FILE.exists():
        return set()

    try:
        with SENT_ALERTS_STATE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, list):
            return {str(item) for item in data}

        if isinstance(data, dict) and isinstance(data.get("sent_alert_keys"), list):
            return {str(item) for item in data["sent_alert_keys"]}

        logger.warning("Sent alerts state file has unexpected format. Ignoring existing state.")
        return set()

    except Exception as exc:
        logger.warning("Could not load sent alerts state file %s: %s", SENT_ALERTS_STATE_FILE, exc)
        return set()


def save_sent_alert_keys(sent_alert_keys: Set[str]) -> None:
    try:
        SENT_ALERTS_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

        temp_file = SENT_ALERTS_STATE_FILE.with_suffix(".tmp")
        payload = {
            "sent_alert_keys": sorted(sent_alert_keys),
            "updated_at_epoch_seconds": int(time.time()),
        }

        with temp_file.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)

        temp_file.replace(SENT_ALERTS_STATE_FILE)

    except Exception as exc:
        logger.warning("Could not save sent alerts state file %s: %s", SENT_ALERTS_STATE_FILE, exc)


# -----------------------------
# Watchlist / Personalized Filtering Helpers
# -----------------------------

def load_watchlist_keywords() -> Dict[int, List[str]]:
    if not WATCHLIST_FILE.exists():
        logger.warning("Watchlist file not found: %s", WATCHLIST_FILE)
        return {}

    try:
        with WATCHLIST_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)

        games = data.get("games", [])
        keywords_by_app_id: Dict[int, List[str]] = {}

        for game in games:
            if not game.get("enabled", True):
                continue

            app_id = game.get("app_id")
            keywords = game.get("alert_keywords", [])

            if app_id is None:
                continue

            keywords_by_app_id[int(app_id)] = [
                str(keyword).strip().lower()
                for keyword in keywords
                if str(keyword).strip()
            ]

        return keywords_by_app_id

    except Exception as exc:
        discord_alert_consumer_errors_total.inc()
        logger.warning("Could not load watchlist file %s: %s", WATCHLIST_FILE, exc)
        return {}


def normalize_list(value: Any) -> List[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]

    if isinstance(value, str):
        return [value.strip().lower()] if value.strip() else []

    normalized = str(value).strip().lower()
    return [normalized] if normalized else []


def alert_search_text(alert: Dict[str, Any]) -> str:
    key_points = alert.get("key_points", [])
    key_points_text = " ".join(normalize_list(key_points))

    parts = [
        str(alert.get("summary", "")),
        str(alert.get("sentiment", "")),
        str(alert.get("update_type", "")),
        key_points_text,
        str(alert.get("source", "")),
    ]

    return " ".join(parts).lower()


def is_security_or_critical(alert: Dict[str, Any]) -> bool:
    severity = str(alert.get("severity", "")).lower()
    update_type = str(alert.get("update_type", "")).lower()
    key_points = normalize_list(alert.get("key_points", []))

    return (
        severity == "critical"
        or update_type == "security"
        or "security_update" in key_points
    )


def matches_personal_keywords(
    alert: Dict[str, Any],
    keywords_by_app_id: Dict[int, List[str]],
) -> bool:
    app_id = alert.get("app_id")

    try:
        app_id = int(app_id) if app_id is not None else None
    except ValueError:
        app_id = None

    watchlist_keywords = keywords_by_app_id.get(app_id, []) if app_id is not None else []

    if not watchlist_keywords:
        logger.info(
            "No personalized keywords found for app_id=%s. Allowing alert.",
            app_id,
        )
        return True

    search_text = alert_search_text(alert)

    for keyword in watchlist_keywords:
        if keyword in search_text:
            logger.info("Alert matched personalized keyword=%s", keyword)
            return True

    logger.info(
        "Alert did not match personalized keywords app_id=%s keywords=%s",
        app_id,
        watchlist_keywords,
    )
    return False


# -----------------------------
# Alert Helpers
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
        "app_id": get_value(alert, "appId", "app_id", None),
        "game_name": get_value(alert, "gameName", "game_name", "Unknown Game"),
        "url": alert.get("url", ""),
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


def alert_dedupe_key(alert: Dict[str, Any]) -> str:
    """
    Prefer alert_id when available.
    Fallback to gid because your alert persistence is based around one alert per gid.
    """
    alert_id = str(alert.get("alert_id", "")).strip()
    gid = str(alert.get("gid", "")).strip()

    if alert_id and alert_id != "unknown-alert-id":
        return f"alert_id:{alert_id}"

    return f"gid:{gid}"


def severity_icon(severity: str) -> str:
    severity = severity.lower()

    if severity == "critical":
        return "🚨"

    if severity == "high":
        return "⚠️"

    if severity == "medium":
        return "🟡"

    return "ℹ️"


def should_send_to_discord(
    alert: Dict[str, Any],
    keywords_by_app_id: Dict[int, List[str]],
) -> bool:
    severity = str(alert.get("severity", "")).lower()

    if severity not in {"critical", "high", "medium"}:
        logger.info(
            "Skipping Discord send for gid=%s severity=%s",
            alert["gid"],
            severity,
        )
        return False

    if not ENABLE_PERSONALIZED_FILTERING:
        return True

    if ALWAYS_SEND_SECURITY_OR_CRITICAL and is_security_or_critical(alert):
        logger.info(
            "Alert is security/critical. Bypassing personalized keyword filter gid=%s",
            alert["gid"],
        )
        return True

    if matches_personal_keywords(alert, keywords_by_app_id):
        return True

    discord_alert_personalized_filter_skipped_total.inc()
    logger.info(
        "Skipping Discord send because alert did not match personalized keywords gid=%s app_id=%s game=%s",
        alert["gid"],
        alert["app_id"],
        alert["game_name"],
    )
    return False


def build_discord_payload(alert: Dict[str, Any]) -> Dict[str, Any]:
    icon = severity_icon(alert["severity"])

    key_points = alert.get("key_points") or []
    if isinstance(key_points, list):
        key_points_text = "\n".join(f"- {point}" for point in key_points)
    else:
        key_points_text = str(key_points)

    content = (
        f"{icon} **Gaming Intelligence Alert**\n\n"
        f"**Game:** {alert['game_name']}\n"
        f"**Severity:** {alert['severity'].upper()}\n"
        f"**GID:** `{alert['gid']}`\n"
        f"**Update Type:** `{alert['update_type']}`\n"
        f"**Importance Score:** `{alert['importance_score']}`\n"
        f"**Sentiment:** `{alert['sentiment']}`\n"
        f"**Confidence:** `{alert['confidence']}`\n\n"
        f"**Summary:**\n{alert['summary'] or 'No summary provided.'}\n"
    )

    if alert.get("url"):
        content += f"\n**Steam Update:** {alert['url']}\n"

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


# -----------------------------
# Kafka
# -----------------------------

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


def handle_alert_message(
    raw_alert: Dict[str, Any],
    sent_alert_keys: Set[str],
    keywords_by_app_id: Dict[int, List[str]],
) -> None:
    logger.info("RAW ALERT RECEIVED: %s", json.dumps(raw_alert, indent=2))

    discord_alert_messages_consumed_total.inc()

    alert = normalize_alert(raw_alert)
    dedupe_key = alert_dedupe_key(alert)

    if dedupe_key in sent_alert_keys:
        discord_alert_duplicates_skipped_total.inc()
        logger.info(
            "Skipping duplicate Discord alert dedupe_key=%s gid=%s severity=%s",
            dedupe_key,
            alert["gid"],
            alert["severity"],
        )
        return

    logger.info(
        "Consumed alert gid=%s game=%s severity=%s update_type=%s",
        alert["gid"],
        alert["game_name"],
        alert["severity"],
        alert["update_type"],
    )

    if not should_send_to_discord(alert, keywords_by_app_id):
        return

    send_to_discord(alert)

    sent_alert_keys.add(dedupe_key)
    save_sent_alert_keys(sent_alert_keys)

    logger.info(
        "Discord alert handled successfully gid=%s game=%s severity=%s",
        alert["gid"],
        alert["game_name"],
        alert["severity"],
    )


def main() -> int:
    logger.info("Starting Discord alert consumer")
    logger.info("Prometheus metrics port: %s", PROMETHEUS_PORT)
    logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Alerts topic: %s", ALERTS_TOPIC)
    logger.info("Consumer group: %s", CONSUMER_GROUP_ID)
    logger.info("DRY_RUN: %s", DRY_RUN)
    logger.info("DISCORD_WEBHOOK_URL present: %s", bool(DISCORD_WEBHOOK_URL))
    logger.info("Sent alerts state file: %s", SENT_ALERTS_STATE_FILE)
    logger.info("Watchlist file: %s", WATCHLIST_FILE)
    logger.info("Personalized filtering enabled: %s", ENABLE_PERSONALIZED_FILTERING)
    logger.info("Always send security/critical: %s", ALWAYS_SEND_SECURITY_OR_CRITICAL)

    start_http_server(PROMETHEUS_PORT)

    sent_alert_keys = load_sent_alert_keys()
    keywords_by_app_id = load_watchlist_keywords()

    logger.info("Loaded %s previously sent Discord alert keys", len(sent_alert_keys))
    logger.info("Loaded personalized keywords for %s games", len(keywords_by_app_id))

    consumer = create_consumer()

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in records.items():
                logger.info("Received %s records from %s", len(messages), topic_partition)

                for message in messages:
                    try:
                        handle_alert_message(
                            message.value,
                            sent_alert_keys,
                            keywords_by_app_id,
                        )
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
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

import requests
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server


# -----------------------------
# Configuration
# -----------------------------

APP_ID = int(os.getenv("STEAM_APP_ID", "730"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
RAW_UPDATES_TOPIC = os.getenv("RAW_UPDATES_TOPIC", "raw_updates")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))

STEAM_NEWS_URL = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"

STATE_FILE = Path(os.getenv("STEAM_INGESTION_STATE_FILE", ".steam_ingestion_seen_gids.json"))

HTTP_TIMEOUT_SECONDS = 15
NEWS_COUNT = int(os.getenv("STEAM_NEWS_COUNT", "20"))
MAX_LENGTH = int(os.getenv("STEAM_NEWS_MAX_LENGTH", "5000"))


# -----------------------------
# Logging
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [steam-ingestion] %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------
# Prometheus Metrics
# -----------------------------

steam_api_latency_seconds = Histogram(
    "steam_api_latency_seconds",
    "Latency of Steam Web API requests in seconds",
)

ingestion_loop_duration_seconds = Histogram(
    "ingestion_loop_duration_seconds",
    "Duration of one Steam ingestion polling loop in seconds",
)

steam_api_errors_total = Counter(
    "steam_api_errors_total",
    "Total number of Steam Web API errors",
)

steam_ingestion_kafka_publish_success_total = Counter(
    "steam_ingestion_kafka_publish_success_total",
    "Total number of successfully published Steam update messages",
)

steam_ingestion_kafka_publish_failure_total = Counter(
    "steam_ingestion_kafka_publish_failure_total",
    "Total number of failed Kafka publish attempts from Steam ingestion",
)

steam_updates_seen_total = Counter(
    "steam_updates_seen_total",
    "Total number of Steam updates seen by the ingestion service",
)


# -----------------------------
# Shutdown Handling
# -----------------------------

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping ingestion loop...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# -----------------------------
# Helpers
# -----------------------------

def load_seen_gids() -> Set[str]:
    if not STATE_FILE.exists():
        return set()

    try:
        with STATE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data, list):
            logger.warning("State file is not a list. Ignoring existing state.")
            return set()

        return {str(item) for item in data}

    except Exception as exc:
        logger.warning("Could not load state file %s: %s", STATE_FILE, exc)
        return set()


def save_seen_gids(seen_gids: Set[str]) -> None:
    try:
        with STATE_FILE.open("w", encoding="utf-8") as file:
            json.dump(sorted(seen_gids), file, indent=2)
    except Exception as exc:
        logger.warning("Could not save state file %s: %s", STATE_FILE, exc)


def strip_markup(value: str) -> str:
    """
    Steam news contents can include HTML/BBCode-like markup.
    Keep this lightweight because the processor/AI can still handle raw text,
    but remove the noisiest tags for better summaries.
    """
    if not value:
        return ""

    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\[[^\]]+\]", " ", value)
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def unix_to_iso_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_steam_news() -> List[Dict[str, Any]]:
    params = {
        "appid": APP_ID,
        "count": NEWS_COUNT,
        "maxlength": MAX_LENGTH,
        "format": "json",
    }

    with steam_api_latency_seconds.time():
        response = requests.get(
            STEAM_NEWS_URL,
            params=params,
            timeout=HTTP_TIMEOUT_SECONDS,
        )

    response.raise_for_status()
    payload = response.json()

    appnews = payload.get("appnews", {})
    news_items = appnews.get("newsitems", [])

    if not isinstance(news_items, list):
        logger.warning("Unexpected Steam API response shape: newsitems is not a list")
        return []

    return news_items


def normalize_news_item(item: Dict[str, Any]) -> Dict[str, Any]:
    gid = str(item.get("gid", "")).strip()

    date_value = item.get("date")
    if date_value is None:
        date_value = int(time.time())

    date_value = int(date_value)

    return {
        "gid": gid,
        "app_id": APP_ID,
        "title": str(item.get("title", "")).strip(),
        "url": str(item.get("url", "")).strip(),
        "author": str(item.get("author", "Valve")).strip() or "Valve",
        "contents": strip_markup(str(item.get("contents", ""))),
        "date": date_value,
        "published_at": unix_to_iso_utc(date_value),
    }


def create_kafka_producer() -> KafkaProducer:
    logger.info("Connecting to Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        retries=3,
        linger_ms=10,
        acks="all",
    )


def publish_update(producer: KafkaProducer, message: Dict[str, Any]) -> None:
    gid = message["gid"]

    future = producer.send(
        RAW_UPDATES_TOPIC,
        key=gid,
        value=message,
    )

    future.get(timeout=15)
    producer.flush(timeout=15)


def run_once(producer: KafkaProducer, seen_gids: Set[str]) -> int:
    published_count = 0

    with ingestion_loop_duration_seconds.time():
        logger.info("Polling Steam news for app_id=%s", APP_ID)

        try:
            news_items = fetch_steam_news()
        except Exception as exc:
            steam_api_errors_total.inc()
            logger.exception("Steam API request failed: %s", exc)
            return 0

        # Oldest first so downstream processing is chronological.
        normalized_messages = [
            normalize_news_item(item)
            for item in news_items
        ]

        normalized_messages = [
            message
            for message in normalized_messages
            if message.get("gid")
        ]

        normalized_messages.sort(key=lambda message: message["date"])

        logger.info("Steam API returned %s usable news items", len(normalized_messages))

        for message in normalized_messages:
            gid = message["gid"]
            steam_updates_seen_total.inc()

            if gid in seen_gids:
                logger.info("Skipping already-seen Steam update gid=%s", gid)
                continue

            try:
                publish_update(producer, message)
                steam_ingestion_kafka_publish_success_total.inc()
                seen_gids.add(gid)
                published_count += 1

                logger.info(
                    "Published Steam update to Kafka topic=%s gid=%s title=%s",
                    RAW_UPDATES_TOPIC,
                    gid,
                    message["title"],
                )

            except Exception as exc:
                steam_ingestion_kafka_publish_failure_total.inc()
                logger.exception("Failed to publish Steam update gid=%s: %s", gid, exc)

        save_seen_gids(seen_gids)

    return published_count


def main() -> int:
    logger.info("Starting Steam ingestion service")
    logger.info("Prometheus metrics port: %s", PROMETHEUS_PORT)
    logger.info("Poll interval seconds: %s", POLL_INTERVAL_SECONDS)
    logger.info("Raw updates topic: %s", RAW_UPDATES_TOPIC)

    start_http_server(PROMETHEUS_PORT)

    seen_gids = load_seen_gids()
    logger.info("Loaded %s previously seen gids", len(seen_gids))

    producer = create_kafka_producer()

    while running:
        published_count = run_once(producer, seen_gids)

        logger.info("Ingestion loop complete. Published %s new updates.", published_count)

        if not running:
            break

        logger.info("Sleeping for %s seconds", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)

    producer.close(timeout=15)
    logger.info("Steam ingestion service stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
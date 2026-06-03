import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server


# -----------------------------
# Env loading
# -----------------------------

load_dotenv(Path(__file__).resolve().parent / ".env")


# -----------------------------
# Paths
# -----------------------------

BASE_DIR = Path(__file__).resolve().parent
WATCHLIST_FILE = Path(os.getenv("WATCHLIST_FILE", BASE_DIR / "watchlist.json"))
STATE_FILE = Path(os.getenv("STEAM_INGESTION_STATE_FILE", BASE_DIR / ".steam_ingestion_seen_gids.json"))


# -----------------------------
# Configuration
# -----------------------------

DEFAULT_APP_ID = int(os.getenv("STEAM_APP_ID", "730"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
RAW_UPDATES_TOPIC = os.getenv("RAW_UPDATES_TOPIC", "raw_updates")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))

STEAM_NEWS_URL = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"

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
    ["app_id", "game_name"],
)

ingestion_loop_duration_seconds = Histogram(
    "ingestion_loop_duration_seconds",
    "Duration of one Steam ingestion polling loop in seconds",
)

steam_api_errors_total = Counter(
    "steam_api_errors_total",
    "Total number of Steam Web API errors",
    ["app_id", "game_name"],
)

steam_ingestion_kafka_publish_success_total = Counter(
    "steam_ingestion_kafka_publish_success_total",
    "Total number of successfully published Steam update messages",
    ["app_id", "game_name"],
)

steam_ingestion_kafka_publish_failure_total = Counter(
    "steam_ingestion_kafka_publish_failure_total",
    "Total number of failed Kafka publish attempts from Steam ingestion",
    ["app_id", "game_name"],
)

steam_updates_seen_total = Counter(
    "steam_updates_seen_total",
    "Total number of Steam updates seen by the ingestion service",
    ["app_id", "game_name"],
)

steam_watchlist_games_enabled = Counter(
    "steam_watchlist_games_enabled_total",
    "Total number of enabled games loaded from the Steam watchlist",
)


# -----------------------------
# Models
# -----------------------------

@dataclass(frozen=True)
class WatchedGame:
    app_id: int
    name: str
    enabled: bool
    alert_keywords: List[str]


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
# Watchlist
# -----------------------------

def load_watchlist() -> List[WatchedGame]:
    if not WATCHLIST_FILE.exists():
        logger.warning(
            "Watchlist file not found at %s. Falling back to STEAM_APP_ID=%s.",
            WATCHLIST_FILE,
            DEFAULT_APP_ID,
        )

        return [
            WatchedGame(
                app_id=DEFAULT_APP_ID,
                name=f"Steam App {DEFAULT_APP_ID}",
                enabled=True,
                alert_keywords=[],
            )
        ]

    with WATCHLIST_FILE.open("r", encoding="utf-8") as file:
        data = json.load(file)

    games = data.get("games", [])

    if not isinstance(games, list):
        raise ValueError("watchlist.json must contain a top-level 'games' array")

    watched_games: List[WatchedGame] = []

    for game in games:
        if not isinstance(game, dict):
            logger.warning("Skipping invalid watchlist entry because it is not an object: %s", game)
            continue

        app_id = int(game["app_id"])
        name = str(game.get("name", f"Steam App {app_id}")).strip()
        enabled = bool(game.get("enabled", True))
        alert_keywords = game.get("alert_keywords", [])

        if not isinstance(alert_keywords, list):
            alert_keywords = []

        watched_games.append(
            WatchedGame(
                app_id=app_id,
                name=name,
                enabled=enabled,
                alert_keywords=[str(keyword).strip() for keyword in alert_keywords if str(keyword).strip()],
            )
        )

    enabled_games = [game for game in watched_games if game.enabled]

    logger.info("Loaded %s games from watchlist, %s enabled", len(watched_games), len(enabled_games))

    for game in enabled_games:
        steam_watchlist_games_enabled.inc()
        logger.info("Watching game app_id=%s name=%s", game.app_id, game.name)

    return enabled_games


# -----------------------------
# State Helpers
# -----------------------------

def state_key(app_id: int, gid: str) -> str:
    return f"{app_id}:{gid}"


def load_seen_gids() -> Set[str]:
    if not STATE_FILE.exists():
        return set()

    try:
        with STATE_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data, list):
            logger.warning("State file is not a list. Ignoring existing state.")
            return set()

        return set(str(item) for item in data)

    except Exception as exc:
        logger.warning("Could not load state file %s: %s", STATE_FILE, exc)
        return set()


def save_seen_gids(seen_gids: Set[str]) -> None:
    try:
        with STATE_FILE.open("w", encoding="utf-8") as file:
            json.dump(sorted(seen_gids), file, indent=2)
    except Exception as exc:
        logger.warning("Could not save state file %s: %s", STATE_FILE, exc)


# -----------------------------
# Helpers
# -----------------------------

def strip_markup(value: str) -> str:
    if not value:
        return ""

    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\[[^\]]+\]", " ", value)
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def unix_to_iso_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_steam_news(game: WatchedGame) -> List[Dict[str, Any]]:
    params = {
        "appid": game.app_id,
        "count": NEWS_COUNT,
        "maxlength": MAX_LENGTH,
        "format": "json",
    }

    with steam_api_latency_seconds.labels(str(game.app_id), game.name).time():
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
        logger.warning(
            "Unexpected Steam API response shape for app_id=%s name=%s",
            game.app_id,
            game.name,
        )
        return []

    return news_items


def normalize_news_item(game: WatchedGame, item: Dict[str, Any]) -> Dict[str, Any]:
    gid = str(item.get("gid", "")).strip()

    date_value = item.get("date")
    if date_value is None:
        date_value = int(time.time())

    date_value = int(date_value)

    return {
        "gid": gid,
        "app_id": game.app_id,
        "game_name": game.name,
        "alert_keywords": game.alert_keywords,
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


def process_game(producer: KafkaProducer, seen_gids: Set[str], game: WatchedGame) -> int:
    published_count = 0

    logger.info("Polling Steam news for app_id=%s name=%s", game.app_id, game.name)

    try:
        news_items = fetch_steam_news(game)
    except Exception as exc:
        steam_api_errors_total.labels(str(game.app_id), game.name).inc()
        logger.exception(
            "Steam API request failed for app_id=%s name=%s: %s",
            game.app_id,
            game.name,
            exc,
        )
        return 0

    normalized_messages = [
        normalize_news_item(game, item)
        for item in news_items
    ]

    normalized_messages = [
        message
        for message in normalized_messages
        if message.get("gid")
    ]

    normalized_messages.sort(key=lambda message: message["date"])

    logger.info(
        "Steam API returned %s usable news items for app_id=%s name=%s",
        len(normalized_messages),
        game.app_id,
        game.name,
    )

    for message in normalized_messages:
        gid = message["gid"]
        app_id = message["app_id"]
        game_name = message["game_name"]
        key = state_key(app_id, gid)

        steam_updates_seen_total.labels(str(app_id), game_name).inc()

        if key in seen_gids:
            logger.info("Skipping already-seen Steam update app_id=%s gid=%s", app_id, gid)
            continue

        try:
            publish_update(producer, message)
            steam_ingestion_kafka_publish_success_total.labels(str(app_id), game_name).inc()

            # Only mark as seen after Kafka publish succeeds.
            seen_gids.add(key)
            published_count += 1

            logger.info(
                "Published Steam update topic=%s app_id=%s game=%s gid=%s title=%s",
                RAW_UPDATES_TOPIC,
                app_id,
                game_name,
                gid,
                message["title"],
            )

        except Exception as exc:
            steam_ingestion_kafka_publish_failure_total.labels(str(app_id), game_name).inc()
            logger.exception(
                "Failed to publish Steam update app_id=%s gid=%s: %s",
                app_id,
                gid,
                exc,
            )

    return published_count


def run_once(producer: KafkaProducer, seen_gids: Set[str], watched_games: List[WatchedGame]) -> int:
    total_published = 0

    with ingestion_loop_duration_seconds.time():
        for game in watched_games:
            if not running:
                break

            total_published += process_game(producer, seen_gids, game)

        save_seen_gids(seen_gids)

    return total_published


def main() -> int:
    logger.info("Starting Steam ingestion service")
    logger.info("Prometheus metrics port: %s", PROMETHEUS_PORT)
    logger.info("Poll interval seconds: %s", POLL_INTERVAL_SECONDS)
    logger.info("Raw updates topic: %s", RAW_UPDATES_TOPIC)
    logger.info("Watchlist file: %s", WATCHLIST_FILE)
    logger.info("State file: %s", STATE_FILE)

    start_http_server(PROMETHEUS_PORT)

    watched_games = load_watchlist()

    if not watched_games:
        logger.error("No enabled games found in watchlist. Exiting.")
        return 1

    seen_gids = load_seen_gids()
    logger.info("Loaded %s previously seen app_id:gid keys", len(seen_gids))

    producer = create_kafka_producer()

    while running:
        published_count = run_once(producer, seen_gids, watched_games)

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
import json
import logging
import os
import time
import uuid
from datetime import datetime

import requests

from kafka import KafkaProducer

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    start_http_server
)

STEAM_APP_ID = 730

STEAM_URL = (
    "https://api.steampowered.com/"
    "ISteamNews/GetNewsForApp/v2/"
)

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "localhost:9092"
)

TOPIC = "raw_updates"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

###########################################
# Metrics
###########################################

api_latency = Histogram(
    "steam_api_latency_seconds",
    "Steam API latency"
)

loop_duration = Histogram(
    "steam_ingestor_loop_duration_seconds",
    "Loop duration"
)

error_counter = Counter(
    "steam_ingestor_errors_total",
    "Total errors"
)

events_published = Counter(
    "steam_events_published_total",
    "Published events"
)

last_poll_timestamp = Gauge(
    "steam_last_poll_timestamp",
    "Last successful poll"
)

###########################################
# Kafka
###########################################

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v:
        json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

###########################################
# Poll Steam
###########################################

@api_latency.time()
def fetch_news():

    params = {
        "appid": STEAM_APP_ID,
        "count": 20,
        "maxlength": 5000
    }

    response = requests.get(
        STEAM_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


def publish_news(items):

    for item in items:

        gid = item.get("gid")

        if not gid:
            gid = str(uuid.uuid4())

        payload = {
            "gid": gid,
            "app_id": STEAM_APP_ID,
            "title": item.get("title"),
            "url": item.get("url"),
            "author": item.get("author"),
            "contents": item.get("contents"),
            "date": item.get("date"),
            "published_at": datetime.timezone.utc().isoformat()
        }

        producer.send(
            TOPIC,
            key=gid.encode(),
            value=payload
        )

        events_published.inc()

    producer.flush()


@loop_duration.time()
def run_cycle():

    data = fetch_news()

    news_items = data["appnews"]["newsitems"]

    publish_news(news_items)

    last_poll_timestamp.set(time.time())

    logger.info(
        "Published %s events",
        len(news_items)
    )


def main():

    logger.info(
        "Starting Steam Ingestor..."
    )

    start_http_server(8000)

    while True:

        try:

            run_cycle()

        except Exception as ex:

            error_counter.inc()

            logger.exception(
                "Poll failed: %s",
                ex
            )

        time.sleep(300)


if __name__ == "__main__":
    main()
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from langgraph.graph import END, START, StateGraph
from prometheus_client import Counter, Histogram, start_http_server
from pydantic import BaseModel, Field, ValidationError, field_validator


# -----------------------------
# Env loading
# -----------------------------

load_dotenv(Path(__file__).resolve().parent / ".env")


# -----------------------------
# Configuration
# -----------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
PROCESSED_UPDATES_TOPIC = os.getenv("PROCESSED_UPDATES_TOPIC", "processed_updates")
AI_ANALYSIS_TOPIC = os.getenv("AI_ANALYSIS_TOPIC", "ai_analysis")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "python-langgraph-ai-worker")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")

PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8002"))
HTTP_TIMEOUT_SECONDS = 120


# -----------------------------
# Logging
# -----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [ai-worker] %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------
# Prometheus Metrics
# -----------------------------

processed_updates_consumed_total = Counter(
    "python_ai_processed_updates_consumed_total",
    "Total number of processed update messages consumed by the Python AI worker",
)

ai_analysis_success_total = Counter(
    "python_ai_analysis_success_total",
    "Total number of successful AI analyses from the Python AI worker",
)

ai_analysis_failure_total = Counter(
    "python_ai_analysis_failure_total",
    "Total number of failed AI analyses from the Python AI worker",
)

ai_analysis_published_total = Counter(
    "python_ai_analysis_published_total",
    "Total number of AI analysis messages published by the Python AI worker",
)

ai_worker_errors_total = Counter(
    "python_ai_worker_errors_total",
    "Total number of Python AI worker errors",
)

llm_inference_latency_seconds = Histogram(
    "python_ai_llm_inference_latency_seconds",
    "Ollama LLM inference latency in seconds",
)

ai_worker_message_duration_seconds = Histogram(
    "python_ai_worker_message_duration_seconds",
    "Total time to process one message in the Python AI worker",
)


# -----------------------------
# Shutdown Handling
# -----------------------------

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping AI worker...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# -----------------------------
# Pydantic Models
# -----------------------------

class ProcessedSteamUpdate(BaseModel):
    gid: str
    app_id: int = Field(default=730, alias="appId")
    game_name: str = Field(default="Unknown Game", alias="gameName")
    title: str = ""
    url: str = ""
    author: str = ""
    contents: str = ""
    date: Optional[int] = None
    published_at: Optional[str] = Field(default=None, alias="publishedAt")
    alert_keywords: List[str] = Field(default_factory=list, alias="alertKeywords")

    model_config = {
        "populate_by_name": True
    }


class AiAnalysisOutput(BaseModel):
    summary: str
    sentiment: str
    confidence: float
    importance_score: int
    update_type: str
    key_points: List[str]

    @field_validator("sentiment")
    @classmethod
    def validate_sentiment(cls, value: str) -> str:
        allowed = {"positive", "neutral", "negative"}
        normalized = value.lower().strip()

        if normalized not in allowed:
            return "neutral"

        return normalized

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @field_validator("importance_score")
    @classmethod
    def validate_importance_score(cls, value: int) -> int:
        return max(1, min(10, int(value)))

    @field_validator("update_type")
    @classmethod
    def validate_update_type(cls, value: str) -> str:
        allowed = {
            "security",
            "balance_change",
            "bug_fix",
            "content_update",
            "performance",
            "server",
            "other",
        }

        normalized = value.lower().strip()

        if normalized not in allowed:
            return "other"

        return normalized


class AiWorkerState(TypedDict):
    update: ProcessedSteamUpdate
    prompt: str
    raw_llm_response: str
    analysis: AiAnalysisOutput
    message: Dict[str, Any]


# -----------------------------
# Helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_prompt(update: ProcessedSteamUpdate) -> str:
    return f"""
You are an AI analyst for a gaming intelligence pipeline.

Analyze this Steam update for Counter-Strike 2.

Return ONLY valid JSON. Do not include markdown. Do not include explanations outside JSON.

JSON schema:
{{
  "summary": "short plain-English summary",
  "sentiment": "positive | neutral | negative",
  "confidence": 0.0,
  "importance_score": 1,
  "update_type": "security | balance_change | bug_fix | content_update | performance | server | other",
  "key_points": ["short point 1", "short point 2"]
}}

Scoring rules:
- importance_score must be an integer from 1 to 10.
- Use security when the update mentions exploit, vulnerability, cheating, account safety, mitigation, attack, or critical security.
- Use balance_change for weapon tuning, matchmaking balance, economy changes, recoil, ranked changes, or gameplay balancing.
- Use bug_fix for crash fixes, UI fixes, minor fixes, and stability patches.
- Use performance for FPS, latency, memory, optimization, or rendering improvements.
- Use server for backend, matchmaking server, networking, uptime, or service reliability changes.
- Use content_update for maps, cosmetics, events, missions, or new game content.
- Use other only if none fit.

Steam update:
game_name: {update.game_name}
gid: {update.gid}
app_id: {update.app_id}
title: {update.title}
author: {update.author}
published_at: {update.published_at}
url: {update.url}
alert_keywords:{",".join(update.alert_keywords) if update.alert_keywords else "none"}

contents:
{update.contents}
""".strip()


def extract_json_object(raw_text: str) -> Dict[str, Any]:
    """
    Ollama JSON mode usually returns clean JSON, but this fallback protects us
    from occasional extra text.
    """
    text = raw_text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No JSON object found in LLM response: {raw_text}")

    return json.loads(text[start:end + 1])


def call_ollama(prompt: str) -> str:
    url = f"{OLLAMA_BASE_URL}/api/generate"

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0
        }
    }

    with llm_inference_latency_seconds.time():
        response = requests.post(url, json=payload, timeout=HTTP_TIMEOUT_SECONDS)

    response.raise_for_status()
    data = response.json()

    return data.get("response", "")


def to_kafka_ai_analysis_message(update: ProcessedSteamUpdate, analysis: AiAnalysisOutput) -> Dict[str, Any]:
    """
    Publish camelCase fields so Spring Boot AiAnalysisMessage can consume them cleanly.
    """
    return {
        "gid": update.gid,
        "appId": update.app_id,
        "gameName": update.game_name,
        "url": update.url,
        "summary": analysis.summary,
        "sentiment": analysis.sentiment,
        "confidence": analysis.confidence,
        "importanceScore": analysis.importance_score,
        "updateType": analysis.update_type,
        "keyPoints": analysis.key_points,
        "createdAt": utc_now_iso(),
        "source": "python-langgraph-ai-worker",
    }


# -----------------------------
# LangGraph Nodes
# -----------------------------

def build_prompt_node(state: AiWorkerState) -> AiWorkerState:
    update = state["update"]
    state["prompt"] = build_prompt(update)
    return state


def analyze_with_ollama_node(state: AiWorkerState) -> AiWorkerState:
    raw_response = call_ollama(state["prompt"])
    state["raw_llm_response"] = raw_response

    parsed = extract_json_object(raw_response)
    state["analysis"] = AiAnalysisOutput.model_validate(parsed)

    return state


def build_message_node(state: AiWorkerState) -> AiWorkerState:
    state["message"] = to_kafka_ai_analysis_message(
        update=state["update"],
        analysis=state["analysis"],
    )

    return state


def build_graph():
    graph = StateGraph(AiWorkerState)

    graph.add_node("build_prompt", build_prompt_node)
    graph.add_node("analyze_with_ollama", analyze_with_ollama_node)
    graph.add_node("build_message", build_message_node)

    graph.add_edge(START, "build_prompt")
    graph.add_edge("build_prompt", "analyze_with_ollama")
    graph.add_edge("analyze_with_ollama", "build_message")
    graph.add_edge("build_message", END)

    return graph.compile()


# -----------------------------
# Kafka
# -----------------------------

def create_consumer() -> KafkaConsumer:
    logger.info("Connecting consumer to Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Consuming topic: %s", PROCESSED_UPDATES_TOPIC)
    logger.info("Consumer group: %s", CONSUMER_GROUP_ID)

    return KafkaConsumer(
        PROCESSED_UPDATES_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
    )


def create_producer() -> KafkaProducer:
    logger.info("Connecting producer to Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        retries=3,
        linger_ms=10,
        acks="all",
    )


def publish_ai_analysis(producer: KafkaProducer, message: Dict[str, Any]) -> None:
    gid = message["gid"]

    future = producer.send(
        AI_ANALYSIS_TOPIC,
        key=gid,
        value=message,
    )

    future.get(timeout=15)
    producer.flush(timeout=15)

    ai_analysis_published_total.inc()


def handle_processed_update(raw_message: Dict[str, Any], graph, producer: KafkaProducer) -> None:
    with ai_worker_message_duration_seconds.time():
        processed_updates_consumed_total.inc()

        update = ProcessedSteamUpdate.model_validate(raw_message)

        logger.info(
            "Processing update with Python LangGraph AI worker gid=%s title=%s",
            update.gid,
            update.title,
        )

        initial_state: AiWorkerState = {
            "update": update,
            "prompt": "",
            "raw_llm_response": "",
            "analysis": None,
            "message": {},
        }

        final_state = graph.invoke(initial_state)
        ai_message = final_state["message"]

        publish_ai_analysis(producer, ai_message)

        ai_analysis_success_total.inc()

        logger.info(
            "Published AI analysis gid=%s updateType=%s importanceScore=%s",
            ai_message["gid"],
            ai_message["updateType"],
            ai_message["importanceScore"],
        )


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    logger.info("Starting Python LangGraph AI worker")
    logger.info("Prometheus metrics port: %s", PROMETHEUS_PORT)
    logger.info("Ollama base URL: %s", OLLAMA_BASE_URL)
    logger.info("Ollama model: %s", OLLAMA_MODEL)
    logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Processed updates topic: %s", PROCESSED_UPDATES_TOPIC)
    logger.info("AI analysis topic: %s", AI_ANALYSIS_TOPIC)

    start_http_server(PROMETHEUS_PORT)

    graph = build_graph()
    consumer = create_consumer()
    producer = create_producer()

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)

            for _, messages in records.items():
                for message in messages:
                    try:
                        handle_processed_update(message.value, graph, producer)
                    except ValidationError as exc:
                        ai_analysis_failure_total.inc()
                        ai_worker_errors_total.inc()
                        logger.exception("Validation failed for processed update: %s", exc)
                    except Exception as exc:
                        ai_analysis_failure_total.inc()
                        ai_worker_errors_total.inc()
                        logger.exception("Failed to process message: %s", exc)

            time.sleep(0.1)

    finally:
        consumer.close()
        producer.close(timeout=15)
        logger.info("Python LangGraph AI worker stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
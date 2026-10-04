"""Prometheus instrumentation for the worker."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger("sap_agent_telemetry")

# Outcomes: success | failed | cancelled | awaiting_review | skipped_duplicate | dlq
BATCH_EVENTS_TOTAL = Counter(
    "langgraph_batch_events_total",
    "Processed Kafka events by topic and outcome",
    ["topic", "outcome"],
)

BATCH_PROCESSING_DURATION = Histogram(
    "langgraph_batch_processing_seconds",
    "Wall-clock time of one graph segment (start -> next human checkpoint, or resume -> end). "
    "Human think-time is NOT included because the graph is suspended between segments.",
    ["topic"],
    buckets=(0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600),
)

AGENT_COST_USD_TOTAL = Counter(
    "langgraph_agent_cost_usd_total",
    "Cumulative USD spend on LLM calls",
    ["model"],
)

LLM_TOKENS_TOTAL = Counter(
    "langgraph_llm_tokens_total",
    "LLM tokens consumed",
    ["model", "direction"],
)

SAP_COMMITS_TOTAL = Counter(
    "langgraph_sap_po_commits_total",
    "SAP purchase order commit attempts by outcome",
    ["outcome"],  # created | already_exists | failed | ambiguous
)

LAST_LOOP_HEARTBEAT = Gauge(
    "langgraph_worker_last_heartbeat_timestamp_seconds",
    "Unix time of the last consume-loop iteration",
)


@contextmanager
def track_processing_time(topic: str) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        BATCH_PROCESSING_DURATION.labels(topic=topic).observe(time.perf_counter() - start)


def record_outcome(topic: str, outcome: str) -> None:
    BATCH_EVENTS_TOTAL.labels(topic=topic, outcome=outcome).inc()


def record_llm_usage(model: str, input_tokens: int, output_tokens: int, usd_cost: float) -> None:
    LLM_TOKENS_TOTAL.labels(model=model, direction="input").inc(max(0, input_tokens))
    LLM_TOKENS_TOTAL.labels(model=model, direction="output").inc(max(0, output_tokens))
    if usd_cost > 0:
        AGENT_COST_USD_TOTAL.labels(model=model).inc(usd_cost)


def record_sap_commit(outcome: str) -> None:
    SAP_COMMITS_TOTAL.labels(outcome=outcome).inc()


def heartbeat() -> None:
    LAST_LOOP_HEARTBEAT.set_to_current_time()


def start_prometheus_server(port: int) -> None:
    """Start the /metrics endpoint. Fails fast: a worker without metrics is not observable."""
    logger.info("Starting Prometheus exporter", extra={"port": port})
    start_http_server(port)

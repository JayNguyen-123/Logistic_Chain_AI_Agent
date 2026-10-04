"""Kafka worker entry point.

Consumes two topics:
  * KAFKA_TOPIC_RECONCILIATION  - new batches -> start a graph thread, run until the human gate
  * KAFKA_TOPIC_HUMAN_DECISIONS - reviewer decisions -> update_state + resume the paused thread

Fixes vs. original:
  * Graceful SIGTERM handling (Kubernetes never sends KeyboardInterrupt).
  * Poison-pill safe: JSON/schema errors no longer kill the iterator (the deserializer ran
    inside the consumer iterator); bad messages go to a real DLQ topic (the original only
    logged "Routed to DLQ").
  * Offsets are committed only after the outcome is durable (graph checkpoint written
    and/or DLQ publish acknowledged). If the DLQ publish fails we do NOT commit, and the
    process exits so the message is redelivered.
  * Redelivery-safe: a batch whose thread already exists is skipped instead of restarted.
  * Paused-for-review is reported as AWAITING_REVIEW instead of being counted as a failure.
  * `auto_offset_reset="earliest"` so batches published before the group's first start
    are not silently skipped.
  * max_poll_interval_ms sized for slow LLM/SAP calls; one record per poll.
  * PagerDuty is actually invoked on terminal failures.
"""
from __future__ import annotations

import json
import logging
import signal
import sys
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from src.config import AppSettings, get_settings
from src.contracts import BatchJobMessage, HumanDecisionMessage
from src.graph.pipeline import HUMAN_GATE, compile_app
from src.logging_config import configure_logging
from src.telemetry.health import HealthState, start_health_server
from src.telemetry.metrics import heartbeat, record_outcome, start_prometheus_server, track_processing_time
from src.telemetry.pd_alerter import resolve_pagerduty_incident, trigger_pagerduty_incident

logger = logging.getLogger("sap_kafka_consumer_engine")


class PermanentMessageError(Exception):
    """Message can never succeed (bad JSON / schema). Route to DLQ, do not retry."""


def _kafka_security_kwargs(s: AppSettings) -> Dict[str, Any]:
    kw: Dict[str, Any] = {"security_protocol": s.KAFKA_SECURITY_PROTOCOL}
    if s.KAFKA_SSL_CAFILE:
        kw["ssl_cafile"] = s.KAFKA_SSL_CAFILE
    if s.KAFKA_SASL_MECHANISM:
        kw["sasl_mechanism"] = s.KAFKA_SASL_MECHANISM
        kw["sasl_plain_username"] = s.KAFKA_SASL_USERNAME
        kw["sasl_plain_password"] = s.KAFKA_SASL_PASSWORD.get_secret_value() if s.KAFKA_SASL_PASSWORD else None
    return kw


def thread_config(batch_id: str) -> Dict[str, Any]:
    return {"configurable": {"thread_id": f"batch_run_{batch_id}"}}


class Worker:
    def __init__(self, app, consumer, producer, settings: AppSettings, health: HealthState) -> None:
        self.app = app
        self.consumer = consumer
        self.producer = producer
        self.s = settings
        self.health = health
        self._stopping = False

    # ------------------------------------------------------------ lifecycle
    def request_stop(self, signum=None, _frame=None) -> None:
        logger.info("Shutdown requested; finishing in-flight message", extra={"signal": signum})
        self._stopping = True
        self.health.set_ready(False)

    def run(self) -> None:
        self.health.set_ready(True)
        while not self._stopping:
            self.health.tick()
            heartbeat()
            batch = self.consumer.poll(timeout_ms=1000, max_records=1)
            for _tp, records in batch.items():
                for record in records:
                    self.handle_record(record)
                    # Commit only after handle_record returns (outcome durable).
                    self.consumer.commit()
                    self.health.tick()

    # ------------------------------------------------------------ dispatch
    def handle_record(self, record) -> None:
        topic = record.topic
        try:
            try:
                raw = json.loads(record.value.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise PermanentMessageError(f"undecodable payload: {exc}") from exc

            with track_processing_time(topic):
                if topic == self.s.KAFKA_TOPIC_RECONCILIATION:
                    self.handle_new_batch(raw, topic)
                elif topic == self.s.KAFKA_TOPIC_HUMAN_DECISIONS:
                    self.handle_human_decision(raw, topic)
                else:
                    raise PermanentMessageError(f"unexpected topic {topic}")

        except PermanentMessageError as exc:
            logger.error("Permanent message error -> DLQ", extra={"topic": topic, "offset": record.offset, "error": str(exc)})
            self.publish_dlq(record, str(exc))
            record_outcome(topic, "dlq")
        except Exception as exc:  # unexpected bug / infra failure inside the graph
            batch_id = self._safe_batch_id(record)
            logger.exception("Unhandled error processing message", extra={"topic": topic, "batch_id": batch_id})
            self.publish_dlq(record, f"{type(exc).__name__}: {exc}")
            record_outcome(topic, "dlq")
            trigger_pagerduty_incident(batch_id or "unknown", f"Worker exception on batch {batch_id}: {type(exc).__name__}",
                                       [str(exc)])

    # ------------------------------------------------------------ handlers
    def handle_new_batch(self, raw: Dict[str, Any], topic: str) -> None:
        try:
            msg = BatchJobMessage.model_validate(raw)
        except ValidationError as exc:
            raise PermanentMessageError(f"invalid batch message: {exc.errors(include_url=False)}") from exc

        cfg = thread_config(msg.batch_id)
        if self.app.get_state(cfg).values:
            logger.info("Duplicate delivery - thread already exists, skipping", extra={"batch_id": msg.batch_id})
            record_outcome(topic, "skipped_duplicate")
            return

        initial_state = {
            "batch_id": msg.batch_id,
            "purchase_order_draft": msg.purchase_order_draft.model_dump(exclude_none=True),
            "target_material_id": msg.target_material_id or "",
            "execution_status": "PROCESSING",
        }
        logger.info("Starting batch", extra={"batch_id": msg.batch_id, "items": len(msg.purchase_order_draft.items)})
        self.app.invoke(initial_state, config=cfg)
        self._report(msg.batch_id, topic)

    def handle_human_decision(self, raw: Dict[str, Any], topic: str) -> None:
        try:
            msg = HumanDecisionMessage.model_validate(raw)
        except ValidationError as exc:
            raise PermanentMessageError(f"invalid decision message: {exc.errors(include_url=False)}") from exc

        cfg = thread_config(msg.batch_id)
        snapshot = self.app.get_state(cfg)
        if not snapshot.values:
            raise PermanentMessageError(f"no workflow found for batch {msg.batch_id}")
        if HUMAN_GATE not in (snapshot.next or ()):
            # Stale or duplicate decision (workflow already moved on / finished).
            logger.warning("Decision ignored: workflow not awaiting review",
                           extra={"batch_id": msg.batch_id, "next": list(snapshot.next or ())})
            record_outcome(topic, "skipped_duplicate")
            return

        update = {"human_action": msg.human_action, "reviewer_id": msg.reviewer_id, "human_feedback": msg.human_feedback}
        if msg.target_material_id is not None:
            update["target_material_id"] = msg.target_material_id
        self.app.update_state(cfg, update)
        logger.info("Resuming batch with reviewer decision",
                    extra={"batch_id": msg.batch_id, "action": msg.human_action, "reviewer": msg.reviewer_id})
        self.app.invoke(None, config=cfg)
        self._report(msg.batch_id, topic)

    # ------------------------------------------------------------ outcomes
    def _report(self, batch_id: str, topic: str) -> None:
        snapshot = self.app.get_state(thread_config(batch_id))
        values = snapshot.values
        status = values.get("execution_status")

        if HUMAN_GATE in (snapshot.next or ()):
            record_outcome(topic, "awaiting_review")
            self.publish_review_request(batch_id, values)
            return
        if status == "SUCCESS":
            logger.info("Batch committed to SAP", extra={"batch_id": batch_id, "po": values.get("sap_po_numbers")})
            record_outcome(topic, "success")
            resolve_pagerduty_incident(batch_id)
        elif status == "CANCELLED":
            logger.info("Batch cancelled", extra={"batch_id": batch_id})
            record_outcome(topic, "cancelled")
        else:
            logger.error("Batch failed", extra={"batch_id": batch_id, "status": status})
            record_outcome(topic, "failed")
            errors = [a.get("message", "") for a in values.get("anomalies_detected", [])]
            trigger_pagerduty_incident(batch_id, f"SAP reconciliation batch {batch_id} failed: {values.get('last_action_taken', '')}",
                                       errors, len(values.get("purchase_order_draft", {}).get("items", [])))

    def publish_review_request(self, batch_id: str, values: Dict[str, Any]) -> None:
        offer = values.get("recommended_offer")
        event = {
            "batch_id": batch_id,
            "review_status": values.get("review_status"),
            "iteration_count": values.get("iteration_count"),
            "purchase_order_draft": values.get("purchase_order_draft"),
            "recommended_offer": offer.model_dump() if hasattr(offer, "model_dump") else offer,
            "anomalies_detected": values.get("anomalies_detected", [])[-20:],
            "last_action_taken": values.get("last_action_taken"),
        }
        self.producer.send(self.s.KAFKA_TOPIC_REVIEW_REQUESTS, key=batch_id.encode(),
                           value=json.dumps(event, default=str).encode()).get(timeout=30)

    def publish_dlq(self, record, reason: str) -> None:
        headers = [("dlq_reason", reason[:1000].encode()), ("source_topic", record.topic.encode()),
                   ("source_partition", str(record.partition).encode()), ("source_offset", str(record.offset).encode())]
        try:
            self.producer.send(self.s.KAFKA_TOPIC_DLQ, key=record.key, value=record.value, headers=headers).get(timeout=30)
        except Exception:
            # Can't park the message safely -> do not commit; crash so it is redelivered.
            logger.critical("DLQ publish failed; exiting without committing offset", exc_info=True)
            raise SystemExit(2) from None

    @staticmethod
    def _safe_batch_id(record) -> str | None:
        try:
            return str(json.loads(record.value.decode("utf-8")).get("batch_id"))
        except Exception:
            return None


def main() -> None:
    s = get_settings()
    configure_logging(s.LOG_LEVEL, s.LOG_JSON)
    start_prometheus_server(s.METRICS_PORT)
    health = HealthState(stale_after_seconds=s.LIVENESS_STALE_SECONDS)
    start_health_server(health, s.HEALTH_PORT)

    app = compile_app()
    security = _kafka_security_kwargs(s)
    consumer = KafkaConsumer(
        *s.consumed_topics,
        bootstrap_servers=s.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=s.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=1,
        max_poll_interval_ms=s.KAFKA_MAX_POLL_INTERVAL_MS,
        client_id="langgraph-supply-chain-worker",
        **security,
    )
    producer = KafkaProducer(
        bootstrap_servers=s.KAFKA_BOOTSTRAP_SERVERS.split(","),
        acks="all",
        retries=5,
        linger_ms=5,
        **security,
    )
    worker = Worker(app, consumer, producer, s, health)
    signal.signal(signal.SIGTERM, worker.request_stop)
    signal.signal(signal.SIGINT, worker.request_stop)

    logger.info("Worker started", extra={"topics": s.consumed_topics, "environment": s.ENVIRONMENT})
    try:
        worker.run()
    finally:
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        finally:
            consumer.close()  # leaves the group cleanly -> fast rebalance
        logger.info("Worker stopped cleanly")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        logging.getLogger("startup").critical("Fatal error during startup", exc_info=True)
        sys.exit(1)

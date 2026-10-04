"""Kafka worker behaviour with fake app / producer (no broker needed)."""
import json
from collections import namedtuple
from types import SimpleNamespace

import pytest

pytest.importorskip("kafka")

from src import main as main_mod  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.telemetry.health import HealthState  # noqa: E402

Record = namedtuple("Record", "topic partition offset key value")


class FakeFuture:
    def __init__(self, fail=False):
        self.fail = fail

    def get(self, timeout=None):
        if self.fail:
            raise RuntimeError("broker down")


class FakeProducer:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def send(self, topic, key=None, value=None, headers=None):
        self.sent.append((topic, key, value, headers))
        return FakeFuture(self.fail)


class FakeApp:
    def __init__(self):
        self.threads = {}
        self.invocations = []

    def get_state(self, cfg):
        t = self.threads.get(cfg["configurable"]["thread_id"])
        return SimpleNamespace(values=t["values"] if t else {}, next=t["next"] if t else ())

    def invoke(self, inp, config):
        tid = config["configurable"]["thread_id"]
        self.invocations.append((tid, inp))
        if inp is not None:
            self.threads[tid] = {"values": {**inp, "execution_status": "AWAITING_REVIEW"}, "next": ("human_gate",)}
        else:
            vals = self.threads[tid]["values"]
            done = vals.get("human_action") == "APPROVE"
            vals["execution_status"] = "SUCCESS" if done else "AWAITING_REVIEW"
            self.threads[tid]["next"] = () if done else ("human_gate",)

    def update_state(self, cfg, values):
        self.threads[cfg["configurable"]["thread_id"]]["values"].update(values)


@pytest.fixture
def worker(monkeypatch):
    monkeypatch.setattr(main_mod, "trigger_pagerduty_incident", lambda *a, **k: True)
    monkeypatch.setattr(main_mod, "resolve_pagerduty_incident", lambda *a, **k: True)
    w = main_mod.Worker(FakeApp(), consumer=None, producer=FakeProducer(), settings=get_settings(), health=HealthState(60))
    return w


def rec(topic, payload, offset=0):
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return Record(topic, 0, offset, b"k", value)


def test_new_batch_pauses_and_emits_review_request(worker, draft):
    s = get_settings()
    worker.handle_record(rec(s.KAFKA_TOPIC_RECONCILIATION, {"batch_id": "B1", "purchase_order_draft": draft}))
    assert worker.producer.sent[0][0] == s.KAFKA_TOPIC_REVIEW_REQUESTS


def test_duplicate_batch_is_not_restarted(worker, draft):
    s = get_settings()
    msg = rec(s.KAFKA_TOPIC_RECONCILIATION, {"batch_id": "B1", "purchase_order_draft": draft})
    worker.handle_record(msg)
    worker.handle_record(msg)
    assert len(worker.app.invocations) == 1


def test_decision_resumes_thread(worker, draft):
    s = get_settings()
    worker.handle_record(rec(s.KAFKA_TOPIC_RECONCILIATION, {"batch_id": "B1", "purchase_order_draft": draft}))
    worker.handle_record(rec(s.KAFKA_TOPIC_HUMAN_DECISIONS, {"batch_id": "B1", "human_action": "APPROVE", "reviewer_id": "alice"}))
    assert worker.app.invocations[-1] == ("batch_run_B1", None)
    assert worker.app.get_state(main_mod.thread_config("B1")).values["execution_status"] == "SUCCESS"


def test_poison_pill_goes_to_dlq(worker):
    s = get_settings()
    worker.handle_record(rec(s.KAFKA_TOPIC_RECONCILIATION, b"{not json"))
    worker.handle_record(rec(s.KAFKA_TOPIC_RECONCILIATION, {"batch_id": "B2", "purchase_order_draft": {"items": []}}))
    worker.handle_record(rec(s.KAFKA_TOPIC_HUMAN_DECISIONS, {"batch_id": "UNKNOWN", "human_action": "APPROVE", "reviewer_id": "a"}))
    assert [t for t, *_ in worker.producer.sent] == [s.KAFKA_TOPIC_DLQ] * 3


def test_dlq_failure_exits_without_commit(worker):
    worker.producer = FakeProducer(fail=True)
    with pytest.raises(SystemExit):
        worker.handle_record(rec(get_settings().KAFKA_TOPIC_RECONCILIATION, b"garbage"))


def test_production_requires_durable_checkpointer(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("CHECKPOINT_DATABASE_URL", raising=False)
    get_settings.cache_clear()
    with pytest.raises(Exception, match="CHECKPOINT_DATABASE_URL"):
        get_settings()

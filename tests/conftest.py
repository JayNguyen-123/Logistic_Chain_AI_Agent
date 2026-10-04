"""Shared fixtures. Unit tests are fully offline: SAP, OpenAI, PagerDuty and Kafka are faked."""
import os

# Must be set before any src.* import resolves settings.
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")
os.environ.setdefault("SAP_CLIENT_SECRET", "test-sap-secret")
os.environ.setdefault("PAGERDUTY_INTEGRATION_KEY", "test-pd-key")
os.environ.setdefault("LOG_JSON", "false")

import copy  # noqa: E402

import pytest  # noqa: E402

from src.config import get_settings  # noqa: E402

BASE_DRAFT = {
    "company_code": "1010",
    "vendor_id": "VEND-001",
    "items": [
        {"material_id": "MAT-102", "quantity": 100, "plant": "1010", "baseline_contract_price": 150.0},
        {"material_id": "MAT-509", "quantity": 10, "plant": "1010", "baseline_contract_price": 45.0},
    ],
}


@pytest.fixture(autouse=True)
def _fresh_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def draft():
    return copy.deepcopy(BASE_DRAFT)


class FakeSAPClient:
    """Records calls; responses are configured per test."""

    def __init__(self):
        self.get_responses = {}   # entity-set substring -> response dict or Exception
        self.post_response = {"PurchaseOrder": "4500000001"}
        self.post_error = None
        self.posts = []
        self.gets = []

    def get(self, path, params=None, correlation_id=None):
        self.gets.append((path, params))
        for key, resp in self.get_responses.items():
            if key in path:
                if isinstance(resp, Exception):
                    raise resp
                return resp(params) if callable(resp) else resp
        return {"results": []}

    def post(self, service_root, entity_set, payload, correlation_id=None):
        self.posts.append(payload)
        if self.post_error:
            raise self.post_error
        return self.post_response


@pytest.fixture
def fake_sap(monkeypatch):
    client = FakeSAPClient()
    for mod in (
        "src.graph.nodes.sap_integration",
        "src.graph.nodes.sub_agents.inventory",
        "src.graph.nodes.sub_agents.logistics",
    ):
        monkeypatch.setattr(f"{mod}.get_sap_client", lambda: client)
    return client

"""PagerDuty Events API v2 integration (deduplicated per batch)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import get_settings

logger = logging.getLogger("sap_agent_pagerduty")

_session = requests.Session()
# Events v2 enqueue is idempotent per dedup_key, so retrying POST on 429/5xx is safe here.
_session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"POST"}),
            respect_retry_after_header=True,
        )
    ),
)


def _dedup_key(batch_id: str) -> str:
    return f"sap-agent-batch-{batch_id}"


def _send(event: Dict[str, Any]) -> bool:
    settings = get_settings()
    event["routing_key"] = settings.PAGERDUTY_INTEGRATION_KEY.get_secret_value()
    try:
        resp = _session.post(settings.PAGERDUTY_EVENTS_URL, json=event, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        # Never log the routing key / request body.
        logger.error("PagerDuty event delivery failed", extra={"error": type(exc).__name__, "dedup_key": event.get("dedup_key")})
        return False


def trigger_pagerduty_incident(
    batch_id: str,
    summary: str,
    error_history: List[str] | None = None,
    items_impacted: int = 0,
    severity: str = "critical",
) -> bool:
    error_history = error_history or []
    detailed_log = "\n".join(e[:500] for e in error_history[-3:]) or "No failure traces logged."
    event = {
        "event_action": "trigger",
        "dedup_key": _dedup_key(batch_id),
        "payload": {
            "summary": summary[:1024],  # PD hard limit
            "source": "langgraph-cluster-worker-pool",
            "severity": severity,
            "component": "SAP-S4HANA-Integration-Engine",
            "group": "Logistics-AI-Agents",
            "class": "Multi-Agent-State-Failure",
            # Only metadata - no line-item business data leaves the cluster.
            "custom_details": {
                "batch_id": batch_id,
                "terminal_error_trace": detailed_log,
                "items_impacted_count": items_impacted,
                "environment": get_settings().ENVIRONMENT,
            },
        },
    }
    ok = _send(event)
    if ok:
        logger.info("PagerDuty incident triggered", extra={"batch_id": batch_id})
    return ok


def resolve_pagerduty_incident(batch_id: str) -> bool:
    return _send({"event_action": "resolve", "dedup_key": _dedup_key(batch_id)})

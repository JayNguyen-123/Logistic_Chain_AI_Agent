"""Shared SAP S/4HANA OData v2 client.

Fixes over the original per-node `requests` calls:
  * Uses SAP_BASE_URL (the original code built SAP URLs from KAFKA_BOOTSTRAP_SERVERS).
  * Real OAuth2 client-credentials flow with token caching (the original sent the raw
    client secret as a Bearer token, which SAP rejects).
  * X-CSRF-Token fetch for modifying requests (required by SAP Gateway).
  * Connection pooling + bounded retries for *idempotent* reads only. POSTs are never
    blindly retried (a timed-out POST may already have created a document).
  * OData key/filter literals are escaped to prevent query injection via material IDs.

NOTE: Entity-set and field names follow the public SAP API Business Hub definitions for
API_PURCHASEORDER_PROCESS_SRV, API_MATERIAL_STOCK_SRV and API_INFORECORD_PROCESS_SRV.
Verify them against your S/4HANA release before go-live.
"""
from __future__ import annotations

import logging
import threading
import time
from functools import lru_cache
from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import get_settings

logger = logging.getLogger("sap_client")

PO_SERVICE = "/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV"
STOCK_SERVICE = "/sap/opu/odata/sap/API_MATERIAL_STOCK_SRV"
INFO_RECORD_SERVICE = "/sap/opu/odata/sap/API_INFORECORD_PROCESS_SRV"


class SAPError(Exception):
    """Raised for any non-success SAP interaction."""

    def __init__(self, message: str, status_code: int | None = None, ambiguous: bool = False):
        super().__init__(message)
        self.status_code = status_code
        # True when we cannot know whether a write was applied (timeout / connection reset mid-POST)
        self.ambiguous = ambiguous


def odata_literal(value: str) -> str:
    """Escape a string for use inside an OData v2 single-quoted literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _extract_sap_error(resp: requests.Response) -> str:
    try:
        return resp.json()["error"]["message"]["value"]
    except Exception:
        return (resp.text or "")[:500]


class SAPClient:
    def __init__(self) -> None:
        s = get_settings()
        self._base_url = s.SAP_BASE_URL.rstrip("/")
        self._token_url = s.SAP_TOKEN_URL
        self._client_id = s.SAP_CLIENT_ID
        self._client_secret = s.SAP_CLIENT_SECRET
        self._timeout = s.SAP_TIMEOUT_SECONDS
        self._verify = s.SAP_VERIFY_TLS

        self._session = requests.Session()
        read_retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),  # never auto-retry POST
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=read_retry, pool_connections=4, pool_maxsize=8)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._lock = threading.Lock()

    # ---------------- auth ----------------
    def _access_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._token_expiry - 60:
                return self._token
            try:
                resp = self._session.post(
                    self._token_url,
                    data={"grant_type": "client_credentials"},
                    auth=(self._client_id, self._client_secret.get_secret_value()),
                    timeout=self._timeout,
                    verify=self._verify,
                )
            except requests.RequestException as exc:
                raise SAPError(f"OAuth token request failed: {exc}") from exc
            if resp.status_code != 200:
                raise SAPError(f"OAuth token request rejected: HTTP {resp.status_code}", resp.status_code)
            body = resp.json()
            self._token = body["access_token"]
            self._token_expiry = time.time() + int(body.get("expires_in", 600))
            return self._token

    def _headers(self, correlation_id: str | None) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._access_token()}",
            "Accept": "application/json",
        }
        if correlation_id:
            headers["X-Correlation-ID"] = correlation_id
        return headers

    # ---------------- reads ----------------
    def get(self, path: str, params: Dict[str, str] | None = None, correlation_id: str | None = None) -> Any:
        url = f"{self._base_url}{path}"
        query = {"$format": "json", **(params or {})}
        try:
            resp = self._session.get(
                url, params=query, headers=self._headers(correlation_id), timeout=self._timeout, verify=self._verify
            )
        except requests.RequestException as exc:
            raise SAPError(f"GET {path} failed: {exc}") from exc
        if resp.status_code >= 400:
            raise SAPError(f"GET {path} -> HTTP {resp.status_code}: {_extract_sap_error(resp)}", resp.status_code)
        return resp.json()["d"]

    # ---------------- writes ----------------
    def _csrf_token(self, service_root: str, correlation_id: str | None) -> str | None:
        headers = self._headers(correlation_id)
        headers["X-CSRF-Token"] = "Fetch"
        try:
            resp = self._session.get(
                f"{self._base_url}{service_root}/", headers=headers, timeout=self._timeout, verify=self._verify
            )
        except requests.RequestException as exc:
            raise SAPError(f"CSRF token fetch failed: {exc}") from exc
        if resp.status_code >= 400:
            raise SAPError(f"CSRF token fetch -> HTTP {resp.status_code}", resp.status_code)
        return resp.headers.get("x-csrf-token")

    def post(self, service_root: str, entity_set: str, payload: Dict[str, Any], correlation_id: str | None = None) -> Any:
        token = self._csrf_token(service_root, correlation_id)
        headers = self._headers(correlation_id)
        headers["Content-Type"] = "application/json"
        if token:
            headers["X-CSRF-Token"] = token
        url = f"{self._base_url}{service_root}/{entity_set}"
        try:
            resp = self._session.post(url, json=payload, headers=headers, timeout=self._timeout, verify=self._verify)
        except (requests.Timeout, requests.ConnectionError) as exc:
            # The request may have reached SAP; caller must reconcile before retrying.
            raise SAPError(f"POST {entity_set} outcome unknown: {exc}", ambiguous=True) from exc
        except requests.RequestException as exc:
            raise SAPError(f"POST {entity_set} failed: {exc}") from exc
        if resp.status_code >= 500:
            raise SAPError(
                f"POST {entity_set} -> HTTP {resp.status_code}: {_extract_sap_error(resp)}",
                resp.status_code,
                ambiguous=resp.status_code in (502, 504),
            )
        if resp.status_code >= 400:
            raise SAPError(f"POST {entity_set} -> HTTP {resp.status_code}: {_extract_sap_error(resp)}", resp.status_code)
        return resp.json()["d"]


@lru_cache(maxsize=1)
def get_sap_client() -> SAPClient:
    return SAPClient()


__all__ = [
    "SAPClient",
    "SAPError",
    "get_sap_client",
    "odata_literal",
    "PO_SERVICE",
    "STOCK_SERVICE",
    "INFO_RECORD_SERVICE",
]

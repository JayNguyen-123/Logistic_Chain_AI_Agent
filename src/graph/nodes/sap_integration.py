"""Final commit node: creates SAP purchase order(s) for an approved draft.

Fixes vs. original:
  * Endpoint built from SAP_BASE_URL (original used KAFKA_BOOTSTRAP_SERVERS).
  * OAuth access token + CSRF token (original sent the client secret as a Bearer token).
  * Real idempotency: each PO carries a deterministic reference
    (CorrespncInternalReference, CHAR 12) and we look it up before creating. A Kafka
    redelivery or a retry after a timeout can no longer create duplicate POs. The
    original only claimed idempotency in a comment.
  * Ambiguous outcomes (timeout mid-POST) are reconciled by lookup, never blindly retried.
  * Approval is bound to the exact draft that was approved (hash check).
  * Item-level vendor overrides (accepted vendor swaps) are split into one PO per supplier.
  * Pydantic v2 config (`class Config` is v1 style), positive quantity, 4-char plant.
  * `PurchaseOrderHeaderText` is not a property of A_PurchaseOrder and was removed.
"""
from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field

from src.config import get_settings
from src.graph.state import ParallelSupplyChainState, draft_fingerprint
from src.integrations.sap_client import PO_SERVICE, SAPError, get_sap_client, odata_literal
from src.telemetry.metrics import record_sap_commit

logger = logging.getLogger("sap_integration_node")


class SAPLineItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    purchase_order_item: str = Field(..., alias="PurchaseOrderItem")
    material_id: str = Field(..., alias="Material", min_length=1, max_length=40)
    quantity: str = Field(..., alias="OrderQuantity")  # Edm.Decimal is serialized as a string in OData v2
    plant: str = Field(..., alias="Plant", pattern=r"^[A-Z0-9]{4}$")
    storage_location: str | None = Field(default=None, alias="StorageLocation", max_length=4)
    net_price: str | None = Field(default=None, alias="NetPriceAmount")


def idempotency_reference(batch_id: str, vendor_id: str) -> str:
    """Deterministic 12-char reference that fits CorrespncInternalReference (UNSEZ, CHAR 12)."""
    return hashlib.sha256(f"{batch_id}|{vendor_id}".encode()).hexdigest()[:12].upper()


def _build_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for n, item in enumerate(items, start=1):
        qty = item["quantity"]
        if not isinstance(qty, (int, float)) or qty <= 0:
            raise ValueError(f"invalid quantity {qty!r} for {item.get('material_id')}")
        line = SAPLineItem(
            PurchaseOrderItem=str(n * 10).zfill(5),
            Material=item["material_id"],
            OrderQuantity=str(qty),
            Plant=str(item["plant"]).upper(),
            StorageLocation=item.get("storage_location"),
            NetPriceAmount=str(item["net_price"]) if item.get("net_price") is not None else None,
        )
        out.append(line.model_dump(by_alias=True, exclude_none=True))
    return out


def _find_existing_po(ref: str, batch_id: str) -> str | None:
    data = get_sap_client().get(
        f"{PO_SERVICE}/A_PurchaseOrder",
        params={"$filter": f"CorrespncInternalReference eq {odata_literal(ref)}", "$select": "PurchaseOrder", "$top": "1"},
        correlation_id=batch_id,
    )
    rows = data.get("results", [])
    return rows[0]["PurchaseOrder"] if rows else None


def _fail(message: str, anomaly_type: str, created: List[str], severity: str = "CRITICAL") -> Dict[str, Any]:
    return {
        "execution_status": "FAILED",
        "sap_po_numbers": created,
        "anomalies_detected": [{"type": anomaly_type, "severity": severity, "message": message[:500]}],
        "last_action_taken": f"SAP Integration: {message[:300]}",
        "audit_log": [f"sap: FAILED ({anomaly_type}) created_so_far={created}"],
    }


def create_sap_purchase_order(state: ParallelSupplyChainState) -> Dict[str, Any]:
    batch_id, draft = state.batch_id, state.purchase_order_draft
    s = get_settings()

    # 1. Authorization guardrails
    if state.review_status != "APPROVED" or not state.approved_draft_hash:
        logger.critical("Commit reached without approval", extra={"batch_id": batch_id})
        return _fail("Blocked: explicit human approval missing.", "UNAUTHORIZED_COMMIT", [])
    if state.approved_draft_hash != draft_fingerprint(draft):
        logger.critical("Draft changed after approval", extra={"batch_id": batch_id})
        return _fail("Blocked: draft was modified after approval.", "APPROVAL_DRAFT_MISMATCH", [])

    # 2. Group items by supplier (item-level override from an accepted vendor swap)
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in draft.get("items", []):
        groups[item.get("vendor_id") or draft.get("vendor_id")].append(item)
    if not groups or None in groups:
        return _fail("Draft has items without a supplier.", "SCHEMA_MISMATCH", [])

    try:
        payloads = {
            vendor: {
                "PurchaseOrderType": draft.get("po_type", s.SAP_DEFAULT_PURCHASE_ORDER_TYPE),
                "Supplier": vendor,
                "CompanyCode": draft.get("company_code", "1010"),
                "PurchasingOrganization": draft.get("purch_org", "1010"),
                "PurchasingGroup": draft.get("purch_group", "001"),
                "CorrespncInternalReference": idempotency_reference(batch_id, vendor),
                "to_PurchaseOrderItem": _build_items(items),
            }
            for vendor, items in sorted(groups.items())
        }
    except (ValueError, KeyError) as exc:
        logger.error("Local schema validation failed", extra={"batch_id": batch_id, "error": str(exc)})
        return _fail(f"Local schema validation failed: {exc}", "SCHEMA_MISMATCH", [])

    # 3. Idempotent create, one PO per supplier
    created: List[str] = []
    client = get_sap_client()
    for vendor, payload in payloads.items():
        ref = payload["CorrespncInternalReference"]
        try:
            existing = _find_existing_po(ref, batch_id)
            if existing:
                logger.info("PO already exists - skipping create", extra={"batch_id": batch_id, "po": existing, "vendor": vendor})
                record_sap_commit("already_exists")
                created.append(existing)
                continue
            result = client.post(PO_SERVICE, "A_PurchaseOrder", payload, correlation_id=batch_id)
            created.append(result["PurchaseOrder"])
            record_sap_commit("created")
            logger.info("PO created", extra={"batch_id": batch_id, "po": result["PurchaseOrder"], "vendor": vendor})
        except SAPError as exc:
            if exc.ambiguous:
                # Reconcile once: did the timed-out POST actually land?
                try:
                    existing = _find_existing_po(ref, batch_id)
                except SAPError:
                    existing = None
                if existing:
                    record_sap_commit("created")
                    created.append(existing)
                    continue
                record_sap_commit("ambiguous")
                return _fail(f"Outcome unknown for supplier {vendor} ({exc}); manual reconciliation required "
                             f"(reference {ref}).", "SAP_AMBIGUOUS_COMMIT", created)
            record_sap_commit("failed")
            return _fail(f"SAP rejected PO for supplier {vendor}: {exc}", "SAP_APPLICATION_ERROR", created)

    return {
        "execution_status": "SUCCESS",
        "sap_po_numbers": created,
        "last_action_taken": f"SAP Integration: purchase order(s) {', '.join(created)} created.",
        "audit_log": [f"sap: SUCCESS {created}"],
    }

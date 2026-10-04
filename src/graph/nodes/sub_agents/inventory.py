"""Parallel branch: on-hand stock assessment for the material under review.

Fixes vs. original:
  * No more fabricated fallback data. The original returned 250 units from "VEND-901" at a
    hard-coded $142.50 whenever SAP was unreachable, which would have shown invented
    supply options to the approver. Failures are now surfaced as anomalies.
  * Correct base URL (SAP_BASE_URL, not the Kafka brokers) and escaped OData literals.
  * Writes only reducer channels (`branch_findings`, `anomalies_detected`, `audit_log`) so
    it can run concurrently with the logistics branch without InvalidUpdateError.
  * Produces a *finding*, not a vendor offer: stock on hand is not a supplier quote.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from src.graph.state import ParallelSupplyChainState
from src.integrations.sap_client import STOCK_SERVICE, SAPError, get_sap_client, odata_literal

logger = logging.getLogger("inventory_branch_agent")

UNRESTRICTED_STOCK_TYPE = "01"


def fetch_unrestricted_stock(material_id: str, plant: str, correlation_id: str) -> float:
    flt = (
        f"Material eq {odata_literal(material_id)} and Plant eq {odata_literal(plant)} "
        f"and InventoryStockType eq {odata_literal(UNRESTRICTED_STOCK_TYPE)}"
    )
    data = get_sap_client().get(
        f"{STOCK_SERVICE}/A_MatlStkInAcctMod",
        params={"$filter": flt, "$select": "MatlWrhsStkQtyInMatlBaseUnit"},
        correlation_id=correlation_id,
    )
    return sum(float(r.get("MatlWrhsStkQtyInMatlBaseUnit") or 0) for r in data.get("results", []))


def inventory_branch_agent(state: ParallelSupplyChainState) -> Dict[str, Any]:
    batch_id, mat_id, rnd = state.batch_id, state.target_material_id, state.iteration_count
    item = next((i for i in state.purchase_order_draft.get("items", []) if i.get("material_id") == mat_id), None)
    if not mat_id or item is None:
        return {"anomalies_detected": [{"type": "INVENTORY_MISSING_MAT", "severity": "WARNING", "iteration": rnd,
                                        "message": "Inventory branch skipped: target material not in draft."}]}

    plant = item.get("plant", "")
    try:
        on_hand = fetch_unrestricted_stock(mat_id, plant, correlation_id=batch_id)
    except SAPError as exc:
        logger.warning("Stock lookup failed", extra={"batch_id": batch_id, "material_id": mat_id, "error": str(exc)})
        return {
            "anomalies_detected": [{"type": "SAP_LOOKUP_FAILED", "severity": "WARNING", "source": "inventory",
                                    "material_id": mat_id, "message": str(exc)[:300], "iteration": rnd}],
            "audit_log": [f"[{rnd}] inventory: stock lookup failed for {mat_id}"],
        }

    required = float(item.get("quantity") or 0)
    finding = {
        "source": "inventory",
        "review_round": rnd,
        "material_id": mat_id,
        "plant": plant,
        "unrestricted_on_hand": on_hand,
        "required_quantity": required,
        "covers_requirement": on_hand >= required > 0,
    }
    logger.info("Stock assessed", extra={"batch_id": batch_id, **finding})
    return {
        "branch_findings": [finding],
        "audit_log": [f"[{rnd}] inventory: {mat_id}@{plant} on-hand={on_hand} required={required}"],
    }

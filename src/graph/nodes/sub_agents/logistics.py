"""Parallel branch: discover alternative suppliers from SAP Purchasing Info Records.

Fixes vs. original:
  * The alternate vendor was hard-coded ("VEND-404") and price/lead-time fell back to
    invented values on any error. Candidates now come from real info records for the
    material + plant, excluding the current supplier; errors become anomalies.
  * Confidence is no longer a hard-coded constant; scoring happens in the consolidator,
    where all offers can be compared.
  * Only reducer channels are written (safe in parallel).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.graph.state import AlternativeVendorOffer, ParallelSupplyChainState
from src.integrations.sap_client import INFO_RECORD_SERVICE, SAPError, get_sap_client, odata_literal

logger = logging.getLogger("logistics_branch_agent")


def fetch_info_record_offers(material_id: str, plant: str, exclude_vendor: str, correlation_id: str, rnd: int) -> List[AlternativeVendorOffer]:
    data = get_sap_client().get(
        f"{INFO_RECORD_SERVICE}/A_PurchasingInfoRecord",
        params={
            "$filter": f"Material eq {odata_literal(material_id)} and IsDeleted eq false",
            "$expand": "to_PurgInfoRecdOrgPlantData",
            "$top": "50",
        },
        correlation_id=correlation_id,
    )
    offers: List[AlternativeVendorOffer] = []
    for rec in data.get("results", []):
        supplier = rec.get("Supplier")
        if not supplier or supplier == exclude_vendor:
            continue
        org_rows = (rec.get("to_PurgInfoRecdOrgPlantData") or {}).get("results", [])
        # Prefer the row for our plant, else a plant-independent row.
        row = next((r for r in org_rows if r.get("Plant") == plant), None) or next(
            (r for r in org_rows if not r.get("Plant")), None
        )
        if not row or row.get("NetPriceAmount") in (None, ""):
            continue
        price_unit = float(row.get("MaterialPriceUnitQty") or 1) or 1.0
        offers.append(
            AlternativeVendorOffer(
                vendor_id=supplier,
                material_id=material_id,
                unit_price=round(float(row["NetPriceAmount"]) / price_unit, 4),
                currency=row.get("DocumentCurrency") or "USD",
                estimated_delivery_days=int(row.get("MaterialPlannedDeliveryDurn") or 0),
                agent_source="Logistics_Branch",
                review_round=rnd,
            )
        )
    return offers


def logistics_branch_agent(state: ParallelSupplyChainState) -> Dict[str, Any]:
    batch_id, mat_id, rnd = state.batch_id, state.target_material_id, state.iteration_count
    draft = state.purchase_order_draft
    item = next((i for i in draft.get("items", []) if i.get("material_id") == mat_id), None)
    if not mat_id or item is None:
        return {"anomalies_detected": [{"type": "LOGISTICS_MISSING_MAT", "severity": "WARNING", "iteration": rnd,
                                        "message": "Logistics branch skipped: target material not in draft."}]}

    current_vendor = item.get("vendor_id") or draft.get("vendor_id", "")
    try:
        offers = fetch_info_record_offers(mat_id, item.get("plant", ""), current_vendor, batch_id, rnd)
    except SAPError as exc:
        logger.warning("Info record lookup failed", extra={"batch_id": batch_id, "material_id": mat_id, "error": str(exc)})
        return {
            "anomalies_detected": [{"type": "SAP_LOOKUP_FAILED", "severity": "WARNING", "source": "logistics",
                                    "material_id": mat_id, "message": str(exc)[:300], "iteration": rnd}],
            "audit_log": [f"[{rnd}] logistics: info record lookup failed for {mat_id}"],
        }

    logger.info("Alternative suppliers found", extra={"batch_id": batch_id, "material_id": mat_id, "count": len(offers)})
    return {
        "candidate_offers": offers,
        "audit_log": [f"[{rnd}] logistics: {len(offers)} alternative supplier(s) for {mat_id}"],
    }

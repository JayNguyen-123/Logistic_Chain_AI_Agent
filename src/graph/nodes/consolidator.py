"""Fan-in node: scores this round's alternative offers and flags price anomalies.

This module was imported by the original pipeline but never existed (ImportError at
startup). Scoring is deterministic so it is auditable and testable.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.config import get_settings
from src.graph.state import AlternativeVendorOffer, ParallelSupplyChainState

logger = logging.getLogger("consolidation_evaluator")

# Score weights: price matters most, then lead time. Tune with procurement.
W_PRICE, W_LEAD = 0.7, 0.3


def score_offers(offers: List[AlternativeVendorOffer]) -> List[AlternativeVendorOffer]:
    """Min-max normalise price and lead time across offers -> confidence_score in [0, 1]."""
    if not offers:
        return []
    prices = [o.unit_price for o in offers]
    leads = [o.estimated_delivery_days for o in offers]
    p_min, p_span = min(prices), (max(prices) - min(prices)) or 1.0
    l_min, l_span = min(leads), (max(leads) - min(leads)) or 1.0
    scored = []
    for o in offers:
        price_score = 1 - (o.unit_price - p_min) / p_span
        lead_score = 1 - (o.estimated_delivery_days - l_min) / l_span
        scored.append(o.model_copy(update={"confidence_score": round(W_PRICE * price_score + W_LEAD * lead_score, 4)}))
    return sorted(scored, key=lambda o: (-o.confidence_score, o.unit_price, o.estimated_delivery_days))


def consolidation_evaluator(state: ParallelSupplyChainState) -> Dict[str, Any]:
    rnd, mat_id = state.iteration_count, state.target_material_id
    threshold = get_settings().PRICE_SPIKE_THRESHOLD

    item = next((i for i in state.purchase_order_draft.get("items", []) if i.get("material_id") == mat_id), {})
    baseline: float | None = item.get("baseline_contract_price")

    offers = [o for o in state.candidate_offers if o.review_round == rnd and o.material_id == mat_id]
    anomalies: List[Dict[str, Any]] = []
    viable: List[AlternativeVendorOffer] = []

    for offer in offers:
        if baseline and offer.unit_price > baseline * (1 + threshold):
            anomalies.append({
                "type": "PRICE_SPIKE", "severity": "CRITICAL", "iteration": rnd,
                "material_id": mat_id, "vendor_id": offer.vendor_id,
                "message": f"Offer {offer.unit_price:.2f} exceeds contract baseline {baseline:.2f} by more than {threshold:.0%}.",
            })
        else:
            viable.append(offer)

    ranked = score_offers(viable)
    recommended = ranked[0] if ranked else None

    stock = next((f for f in state.branch_findings if f.get("review_round") == rnd and f.get("source") == "inventory"), None)
    if stock and stock.get("covers_requirement"):
        anomalies.append({"type": "STOCK_ON_HAND", "severity": "INFO", "iteration": rnd, "material_id": mat_id,
                          "message": f"{stock['unrestricted_on_hand']} units already on hand at plant {stock['plant']}; "
                                     "consider a stock transfer instead of a new PO line."})
    if not ranked:
        anomalies.append({"type": "NO_VIABLE_ALTERNATIVE", "severity": "WARNING", "iteration": rnd, "material_id": mat_id,
                          "message": "No alternative supplier passed the price guardrail."})

    summary = (f"recommended {recommended.vendor_id} @ {recommended.unit_price} ({recommended.estimated_delivery_days}d)"
               if recommended else "no recommendation")
    logger.info("Consolidated offers", extra={"batch_id": state.batch_id, "offers": len(offers),
                                              "viable": len(ranked), "material_id": mat_id})
    return {
        "recommended_offer": recommended,
        "anomalies_detected": anomalies,
        "execution_status": "AWAITING_REVIEW",
        "review_status": "PENDING",
        "last_action_taken": f"Consolidator: {len(offers)} offer(s), {len(ranked)} viable; {summary}.",
        "audit_log": [f"[{rnd}] consolidator: {summary}"],
    }

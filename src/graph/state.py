"""Central LangGraph state schema.

Key fixes vs. the original:
  * Every key that nodes / main.py write is declared (`execution_status` was missing, so
    the worker could never observe SUCCESS).
  * Parallel branches no longer write the same non-reducer key in one super-step
    (`last_action_taken` from two branches raises InvalidUpdateError). Branch audit
    messages go to the additive `audit_log` channel instead.
  * `candidate_offers` can be reset between vendor-swap rounds (plain operator.add kept
    stale offers forever).
  * The cost reducer accepts dicts as well as models (checkpoint round-trips / JSON).
"""
from __future__ import annotations

import hashlib
import json
import operator
from typing import Annotated, Any, Dict, List, Literal, Union

from pydantic import BaseModel, Field

HumanAction = Literal["PENDING", "APPROVE", "AMEND", "SWAP_VENDOR", "ACCEPT_OFFER", "CANCEL"]
ReviewStatus = Literal["PENDING", "APPROVED", "AMENDED", "REJECTED"]
ExecutionStatus = Literal["PROCESSING", "AWAITING_REVIEW", "SUCCESS", "FAILED", "CANCELLED"]
GateRoute = Literal["await_review", "sourcing", "fan_out", "commit", "cancel"]


# ---------------------------------------------------------------- sub-schemas
class ExecutionCost(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    usd_cost: float = 0.0


def reduce_cost(
    current: Union[ExecutionCost, Dict[str, Any]] | None,
    update: Union[ExecutionCost, Dict[str, Any]] | None,
) -> ExecutionCost:
    cur = ExecutionCost.model_validate(current or {})
    if update is None:
        return cur
    new = ExecutionCost.model_validate(update)
    return ExecutionCost(
        input_tokens=cur.input_tokens + new.input_tokens,
        output_tokens=cur.output_tokens + new.output_tokens,
        usd_cost=round(cur.usd_cost + new.usd_cost, 6),
    )


class AlternativeVendorOffer(BaseModel):
    vendor_id: str = Field(..., description="SAP Supplier (LIFNR)")
    material_id: str = Field(..., description="SAP Material (MATNR)")
    unit_price: float = Field(..., ge=0)
    currency: str = "USD"
    estimated_delivery_days: int = Field(..., ge=0)
    confidence_score: float = Field(default=0.0, ge=0, le=1)
    agent_source: str
    review_round: int = Field(default=0, description="iteration_count when this offer was sourced")


def reduce_offers(
    current: List[AlternativeVendorOffer] | None,
    update: List[AlternativeVendorOffer] | None,
) -> List[AlternativeVendorOffer]:
    """Additive reducer that supports an explicit reset: a node returns `None` to clear."""
    if update is None:
        return []
    return list(current or []) + list(update)


def draft_fingerprint(draft: Dict[str, Any]) -> str:
    """Stable hash of a PO draft; binds a human approval to the exact draft they saw."""
    return hashlib.sha256(json.dumps(draft, sort_keys=True, default=str).encode()).hexdigest()


# ---------------------------------------------------------------- graph state
class ParallelSupplyChainState(BaseModel):
    # Identity
    batch_id: str = Field(..., min_length=1)
    target_material_id: str | None = ""

    # Transactional data
    purchase_order_draft: Dict[str, Any] = Field(default_factory=dict)

    # Human-in-the-loop plane (written by the dashboard via graph.update_state)
    human_action: HumanAction = "PENDING"
    human_feedback: str | None = None
    reviewer_id: str | None = None
    review_status: ReviewStatus = "PENDING"
    approved_draft_hash: str | None = None

    # Control plane
    execution_status: ExecutionStatus = "PROCESSING"
    next_route: GateRoute = "await_review"
    iteration_count: int = 0
    last_action_taken: str = ""  # written ONLY by sequential nodes
    recommended_offer: AlternativeVendorOffer | None = None
    sap_po_numbers: List[str] = Field(default_factory=list)

    # Reducer channels (safe for concurrent writes from parallel branches)
    candidate_offers: Annotated[List[AlternativeVendorOffer], reduce_offers] = Field(default_factory=list)
    anomalies_detected: Annotated[List[Dict[str, Any]], operator.add] = Field(default_factory=list)
    branch_findings: Annotated[List[Dict[str, Any]], operator.add] = Field(default_factory=list)
    audit_log: Annotated[List[str], operator.add] = Field(default_factory=list)
    total_run_cost: Annotated[ExecutionCost, reduce_cost] = Field(default_factory=ExecutionCost)

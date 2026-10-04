"""Human-in-the-loop gate.

The graph is compiled with `interrupt_before=["human_gate"]`, so execution pauses right
before this node. The dashboard (via the human-decisions Kafka topic) writes
`human_action` / `human_feedback` / `target_material_id` / `reviewer_id` with
`graph.update_state(...)` and resumes. This node then validates the decision, records an
audit entry, *consumes* the action (resets it to PENDING so a resume without a new
decision can never replay an old one) and sets `next_route` for the router.

Original defects fixed:
  * This node existed but was never wired into the graph.
  * The interrupt was placed before the entry node, so the routing logic ran on
    `PENDING` and cancelled every batch on its first pass.
  * Actions were never consumed -> resuming with a stale SWAP_VENDOR looped forever.
  * No iteration ceiling was enforced (iteration_count was write-only).
"""
from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List

from src.config import get_settings
from src.graph.state import ParallelSupplyChainState, draft_fingerprint

logger = logging.getLogger("sap_agent_checkpoint_gate")


def _draft_problems(draft: Dict[str, Any]) -> List[str]:
    problems = []
    items = draft.get("items") or []
    if not items:
        problems.append("draft has no line items")
    for idx, item in enumerate(items):
        vendor = item.get("vendor_id") or draft.get("vendor_id")
        if not vendor:
            problems.append(f"item {idx} ({item.get('material_id')}) has no vendor")
        if not item.get("plant"):
            problems.append(f"item {idx} ({item.get('material_id')}) has no plant")
        qty = item.get("quantity")
        if not isinstance(qty, (int, float)) or qty <= 0:
            problems.append(f"item {idx} ({item.get('material_id')}) has invalid quantity {qty!r}")
    return problems


def _bounce(state: ParallelSupplyChainState, reason: str, iteration: int) -> Dict[str, Any]:
    """Reject the decision and pause again for a new one."""
    logger.warning("Gate rejected human decision", extra={"batch_id": state.batch_id, "reason": reason})
    return {
        "human_action": "PENDING",
        "review_status": "AMENDED",
        "execution_status": "AWAITING_REVIEW",
        "next_route": "await_review",
        "iteration_count": iteration,
        "anomalies_detected": [{"type": "GATE_VALIDATION", "severity": "WARNING", "message": reason, "iteration": iteration}],
        "last_action_taken": f"Human Gate: decision rejected - {reason}",
        "audit_log": [f"[{iteration}] gate: rejected {state.human_action} by {state.reviewer_id or 'unknown'}: {reason}"],
    }


def human_checkpoint_verification_gate(state: ParallelSupplyChainState) -> Dict[str, Any]:
    action = state.human_action
    reviewer = state.reviewer_id or "unknown"

    # Resumed without a new decision (e.g. duplicate resume) -> just pause again.
    if action == "PENDING":
        return {"next_route": "await_review", "execution_status": "AWAITING_REVIEW",
                "last_action_taken": "Human Gate: awaiting decision."}

    iteration = state.iteration_count + 1
    max_iter = get_settings().MAX_REVIEW_ITERATIONS
    logger.info("Gate processing decision", extra={"batch_id": state.batch_id, "action": action,
                                                   "reviewer": reviewer, "iteration": iteration})

    if action == "CANCEL":
        return {
            "human_action": "PENDING", "review_status": "REJECTED", "next_route": "cancel",
            "iteration_count": iteration,
            "last_action_taken": f"Human Gate: cancelled by {reviewer}. Reason: {state.human_feedback or 'none given'}",
            "audit_log": [f"[{iteration}] gate: CANCEL by {reviewer}"],
        }

    # Loop guard applies to every action that would do more work (APPROVE is always allowed).
    if action != "APPROVE" and iteration > max_iter:
        return {
            "human_action": "PENDING", "review_status": "REJECTED", "next_route": "cancel",
            "iteration_count": iteration,
            "anomalies_detected": [{"type": "MAX_ITERATIONS_EXCEEDED", "severity": "CRITICAL", "iteration": iteration,
                                    "message": f"Review loop exceeded {max_iter} iterations."}],
            "last_action_taken": "Human Gate: iteration ceiling reached; workflow terminated.",
            "audit_log": [f"[{iteration}] gate: loop ceiling hit"],
        }

    if action == "APPROVE":
        problems = _draft_problems(state.purchase_order_draft)
        if problems:
            return _bounce(state, "; ".join(problems), iteration)
        return {
            "human_action": "PENDING", "human_feedback": None,
            "review_status": "APPROVED", "next_route": "commit",
            "approved_draft_hash": draft_fingerprint(state.purchase_order_draft),
            "iteration_count": iteration,
            "last_action_taken": f"Human Gate: draft approved by {reviewer}.",
            "audit_log": [f"[{iteration}] gate: APPROVE by {reviewer}"],
        }

    if action == "AMEND":
        if not (state.human_feedback or "").strip():
            return _bounce(state, "AMEND requires human_feedback text", iteration)
        return {
            "human_action": "PENDING", "review_status": "AMENDED", "next_route": "sourcing",
            "execution_status": "PROCESSING", "iteration_count": iteration,
            "last_action_taken": f"Human Gate: amendment requested by {reviewer}.",
            "audit_log": [f"[{iteration}] gate: AMEND by {reviewer}"],
        }

    if action == "SWAP_VENDOR":
        mat = state.target_material_id
        known = {i.get("material_id") for i in state.purchase_order_draft.get("items", [])}
        if not mat:
            return _bounce(state, "SWAP_VENDOR requires target_material_id", iteration)
        if mat not in known:
            return _bounce(state, f"target_material_id {mat} is not in the draft", iteration)
        return {
            "human_action": "PENDING", "human_feedback": None,
            "review_status": "AMENDED", "next_route": "fan_out",
            "execution_status": "PROCESSING", "iteration_count": iteration,
            "candidate_offers": None,      # reset reducer -> fresh round
            "recommended_offer": None,
            "last_action_taken": f"Human Gate: vendor search started for {mat} by {reviewer}.",
            "audit_log": [f"[{iteration}] gate: SWAP_VENDOR {mat} by {reviewer}"],
        }

    if action == "ACCEPT_OFFER":
        offer = state.recommended_offer
        if offer is None:
            return _bounce(state, "ACCEPT_OFFER requires a recommended_offer from a previous SWAP_VENDOR round", iteration)
        draft = copy.deepcopy(state.purchase_order_draft)
        for item in draft.get("items", []):
            if item.get("material_id") == offer.material_id:
                # Item-level vendor override; SAP commit splits into one PO per supplier.
                item["vendor_id"] = offer.vendor_id
                item["net_price"] = offer.unit_price
                item["currency"] = offer.currency
                item["last_revised_reason"] = f"Vendor swap accepted by {reviewer}"
        return {
            "human_action": "PENDING", "review_status": "AMENDED", "next_route": "await_review",
            "execution_status": "AWAITING_REVIEW", "iteration_count": iteration,
            "purchase_order_draft": draft, "recommended_offer": None, "approved_draft_hash": None,
            "last_action_taken": f"Human Gate: offer from {offer.vendor_id} applied to {offer.material_id}; awaiting final approval.",
            "audit_log": [f"[{iteration}] gate: ACCEPT_OFFER {offer.vendor_id}/{offer.material_id} by {reviewer}"],
        }

    return _bounce(state, f"unsupported action {action}", iteration)  # defensive; Literal should prevent this


def route_after_gate(state: ParallelSupplyChainState):
    """Conditional edge: maps the gate's decision onto graph nodes (list = parallel fan-out)."""
    return {
        "commit": "execute_sap_commit",
        "sourcing": "sourcing_subgraph",
        "fan_out": ["inventory_branch", "logistics_branch"],
        "cancel": "cancel_workflow",
        "await_review": "human_gate",
    }[state.next_route]

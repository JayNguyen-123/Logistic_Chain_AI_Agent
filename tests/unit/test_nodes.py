"""Offline unit tests for graph nodes, reducers and contracts."""
import pytest
from pydantic import ValidationError

from src.contracts import BatchJobMessage, HumanDecisionMessage
from src.graph.nodes import checkpoint_gate as gate
from src.graph.nodes.consolidator import consolidation_evaluator, score_offers
from src.graph.nodes.sap_integration import create_sap_purchase_order, idempotency_reference
from src.graph.nodes.sub_agents import sourcing
from src.graph.nodes.sub_agents.inventory import inventory_branch_agent
from src.graph.nodes.sub_agents.logistics import logistics_branch_agent
from src.graph.state import (
    AlternativeVendorOffer,
    ExecutionCost,
    draft_fingerprint,
    reduce_cost,
    reduce_offers,
)
from src.graph.state import (
    ParallelSupplyChainState as S,
)
from src.integrations.sap_client import SAPError, odata_literal


def offer(vendor, price, days, rnd=1, mat="MAT-102"):
    return AlternativeVendorOffer(vendor_id=vendor, material_id=mat, unit_price=price,
                                  estimated_delivery_days=days, agent_source="t", review_round=rnd)


# ---------------------------------------------------------------- reducers
def test_cost_reducer_accepts_dicts_and_models():
    out = reduce_cost({"input_tokens": 10, "output_tokens": 5, "usd_cost": 0.1}, ExecutionCost(input_tokens=1, usd_cost=0.05))
    assert (out.input_tokens, out.output_tokens, out.usd_cost) == (11, 5, 0.15)
    assert reduce_cost(None, None).usd_cost == 0


def test_offer_reducer_appends_and_resets():
    assert len(reduce_offers([offer("A", 1, 1)], [offer("B", 1, 1)])) == 2
    assert reduce_offers([offer("A", 1, 1)], None) == []


def test_odata_literal_escapes_quotes():
    assert odata_literal("MAT'X") == "'MAT''X'"


# ---------------------------------------------------------------- contracts
def test_batch_contract_rejects_bad_payloads(draft):
    BatchJobMessage.model_validate({"batch_id": "90001", "purchase_order_draft": draft})
    with pytest.raises(ValidationError):
        BatchJobMessage.model_validate({"batch_id": "90001", "purchase_order_draft": {**draft, "items": []}})
    with pytest.raises(ValidationError):
        BatchJobMessage.model_validate({"batch_id": "bad id; drop", "purchase_order_draft": draft})
    with pytest.raises(ValidationError):
        HumanDecisionMessage.model_validate({"batch_id": "1", "human_action": "NUKE", "reviewer_id": "x"})


# ---------------------------------------------------------------- gate
def test_gate_pending_pauses_without_counting(draft):
    out = gate.human_checkpoint_verification_gate(S(batch_id="1", purchase_order_draft=draft))
    assert out["next_route"] == "await_review" and "iteration_count" not in out


def test_gate_approve_binds_hash_and_consumes_action(draft):
    st = S(batch_id="1", purchase_order_draft=draft, human_action="APPROVE", reviewer_id="alice")
    out = gate.human_checkpoint_verification_gate(st)
    assert out["next_route"] == "commit"
    assert out["review_status"] == "APPROVED"
    assert out["human_action"] == "PENDING"
    assert out["approved_draft_hash"] == draft_fingerprint(draft)


def test_gate_approve_rejects_incomplete_draft():
    st = S(batch_id="1", purchase_order_draft={"items": [{"material_id": "M", "quantity": 0, "plant": "1010"}]},
           human_action="APPROVE")
    out = gate.human_checkpoint_verification_gate(st)
    assert out["next_route"] == "await_review"
    assert out["anomalies_detected"][0]["type"] == "GATE_VALIDATION"


def test_gate_swap_requires_known_material(draft):
    out = gate.human_checkpoint_verification_gate(S(batch_id="1", purchase_order_draft=draft, human_action="SWAP_VENDOR"))
    assert out["next_route"] == "await_review" and out["review_status"] == "AMENDED"
    out = gate.human_checkpoint_verification_gate(
        S(batch_id="1", purchase_order_draft=draft, human_action="SWAP_VENDOR", target_material_id="MAT-102"))
    assert out["next_route"] == "fan_out" and out["candidate_offers"] is None


def test_gate_amend_requires_feedback(draft):
    out = gate.human_checkpoint_verification_gate(S(batch_id="1", purchase_order_draft=draft, human_action="AMEND"))
    assert out["next_route"] == "await_review"


def test_gate_enforces_iteration_ceiling(draft):
    st = S(batch_id="1", purchase_order_draft=draft, human_action="AMEND", human_feedback="x", iteration_count=5)
    out = gate.human_checkpoint_verification_gate(st)
    assert out["next_route"] == "cancel"
    assert out["anomalies_detected"][0]["type"] == "MAX_ITERATIONS_EXCEEDED"


def test_gate_accept_offer_sets_item_vendor_and_requires_reapproval(draft):
    st = S(batch_id="1", purchase_order_draft=draft, human_action="ACCEPT_OFFER", recommended_offer=offer("VEND-777", 120, 5))
    out = gate.human_checkpoint_verification_gate(st)
    item = next(i for i in out["purchase_order_draft"]["items"] if i["material_id"] == "MAT-102")
    assert item["vendor_id"] == "VEND-777" and item["net_price"] == 120
    assert out["next_route"] == "await_review" and out["approved_draft_hash"] is None
    assert st.purchase_order_draft["items"][0].get("vendor_id") is None  # original not mutated


def test_router_maps_routes():
    assert gate.route_after_gate(S(batch_id="1", next_route="fan_out")) == ["inventory_branch", "logistics_branch"]
    assert gate.route_after_gate(S(batch_id="1", next_route="commit")) == "execute_sap_commit"
    assert gate.route_after_gate(S(batch_id="1", next_route="await_review")) == "human_gate"


# ---------------------------------------------------------------- sourcing
def adj(mat, action, value, reason="r"):
    return sourcing.SingleItemAdjustment(target_material_id=mat, action=action, adjustment_value=value, reasoning=reason)


def test_apply_adjustments_is_pure_and_validates(draft):
    new, anomalies, applied = sourcing.apply_adjustments(draft, [
        adj("MAT-102", "ADJUST_QUANTITY", "25 units"),
        adj("MAT-509", "CHANGE_PLANT", "1020"),
        adj("MAT-509", "ADJUST_QUANTITY", "-3"),
        adj("MAT-999", "REMOVE_ITEM", ""),
        adj("MAT-102", "CHANGE_PLANT", "Berlin"),
    ], iteration=1)
    items = {i["material_id"]: i for i in new["items"]}
    assert items["MAT-102"]["quantity"] == 25 and items["MAT-102"]["plant"] == "1010"
    assert items["MAT-509"]["plant"] == "1020" and items["MAT-509"]["quantity"] == 10
    assert applied == 2
    assert sorted(a["type"] for a in anomalies) == ["INVALID_ADJUSTMENT", "INVALID_ADJUSTMENT", "UNKNOWN_MATERIAL"]
    assert draft["items"][0]["quantity"] == 100  # input untouched


def test_remove_all_items_flags_empty_draft(draft):
    new, anomalies, _ = sourcing.apply_adjustments(draft, [adj("MAT-102", "REMOVE_ITEM", ""), adj("MAT-509", "REMOVE_ITEM", "")], 1)
    assert new["items"] == [] and anomalies[-1]["type"] == "EMPTY_DRAFT"


def test_sourcing_initial_pass_has_no_llm_call(draft, monkeypatch):
    monkeypatch.setattr(sourcing, "extract_adjustments", lambda *a: pytest.fail("LLM must not be called"))
    out = sourcing.sourcing_agent_multi(S(batch_id="1", purchase_order_draft=draft))
    assert out["execution_status"] == "AWAITING_REVIEW"


def test_sourcing_consumes_feedback_and_tracks_cost(draft, monkeypatch):
    extracted = sourcing.MultiItemIntentExtraction(adjustments=[adj("MAT-102", "ADJUST_QUANTITY", "25")])
    monkeypatch.setattr(sourcing, "extract_adjustments", lambda items, fb: (extracted, ExecutionCost(input_tokens=100, usd_cost=0.01)))
    out = sourcing.sourcing_agent_multi(S(batch_id="1", purchase_order_draft=draft, human_feedback="cut MAT-102 to 25", iteration_count=1))
    assert out["human_feedback"] is None
    assert out["purchase_order_draft"]["items"][0]["quantity"] == 25
    assert out["total_run_cost"].usd_cost == 0.01
    assert out["approved_draft_hash"] is None


def test_sourcing_llm_failure_is_contained(draft, monkeypatch):
    def boom(*a):
        raise TimeoutError("openai down")
    monkeypatch.setattr(sourcing, "extract_adjustments", boom)
    out = sourcing.sourcing_agent_multi(S(batch_id="1", purchase_order_draft=draft, human_feedback="x"))
    assert out["anomalies_detected"][0]["type"] == "PARSE_FAILURE"
    assert "purchase_order_draft" not in out


# ---------------------------------------------------------------- branches
def test_branches_do_not_write_shared_scalar_keys(draft, fake_sap):
    fake_sap.get_responses["A_MatlStkInAcctMod"] = {"results": [{"MatlWrhsStkQtyInMatlBaseUnit": "60"}, {"MatlWrhsStkQtyInMatlBaseUnit": "50"}]}
    fake_sap.get_responses["A_PurchasingInfoRecord"] = {"results": [
        {"Supplier": "VEND-001", "to_PurgInfoRecdOrgPlantData": {"results": [{"Plant": "1010", "NetPriceAmount": "99"}]}},
        {"Supplier": "VEND-404", "to_PurgInfoRecdOrgPlantData": {"results": [
            {"Plant": "1010", "NetPriceAmount": "1100", "MaterialPriceUnitQty": "10", "MaterialPlannedDeliveryDurn": "28"}]}},
        {"Supplier": "VEND-500", "to_PurgInfoRecdOrgPlantData": {"results": []}},
    ]}
    st = S(batch_id="1", purchase_order_draft=draft, target_material_id="MAT-102", iteration_count=2)
    inv, log = inventory_branch_agent(st), logistics_branch_agent(st)
    shared = (set(inv) & set(log)) - {"anomalies_detected", "audit_log", "candidate_offers", "branch_findings"}
    assert not shared, f"parallel branches both write non-reducer keys: {shared}"
    assert inv["branch_findings"][0]["unrestricted_on_hand"] == 110 and inv["branch_findings"][0]["covers_requirement"]
    offers = log["candidate_offers"]
    assert [o.vendor_id for o in offers] == ["VEND-404"]  # current vendor excluded, no-price row skipped
    assert offers[0].unit_price == 110 and offers[0].estimated_delivery_days == 28 and offers[0].review_round == 2
    assert "'MAT-102'" in fake_sap.gets[0][1]["$filter"]


def test_branch_sap_failure_never_fabricates_offers(draft, fake_sap):
    fake_sap.get_responses["A_PurchasingInfoRecord"] = SAPError("timeout")
    fake_sap.get_responses["A_MatlStkInAcctMod"] = SAPError("timeout")
    st = S(batch_id="1", purchase_order_draft=draft, target_material_id="MAT-102")
    log, inv = logistics_branch_agent(st), inventory_branch_agent(st)
    assert "candidate_offers" not in log and "branch_findings" not in inv
    assert log["anomalies_detected"][0]["type"] == inv["anomalies_detected"][0]["type"] == "SAP_LOOKUP_FAILED"


# ---------------------------------------------------------------- consolidator
def test_score_offers_ranks_cheaper_faster_first():
    ranked = score_offers([offer("SLOW", 100, 30), offer("BEST", 100, 3), offer("PRICEY", 140, 3)])
    assert ranked[0].vendor_id == "BEST" and ranked[-1].vendor_id in {"SLOW", "PRICEY"}


def test_consolidator_flags_price_spike_and_ignores_stale_rounds():
    draft = {"vendor_id": "V1", "items": [{"material_id": "MAT-102", "quantity": 50, "plant": "1010", "baseline_contract_price": 100.0}]}
    st = S(batch_id="90003", purchase_order_draft=draft, target_material_id="MAT-102", iteration_count=2,
           candidate_offers=[offer("STALE", 50, 1, rnd=1), offer("SPIKE", 130, 5, rnd=2), offer("OK", 110, 10, rnd=2)])
    out = consolidation_evaluator(st)
    assert out["recommended_offer"].vendor_id == "OK"
    spike = [a for a in out["anomalies_detected"] if a["type"] == "PRICE_SPIKE"]
    assert spike and spike[0]["severity"] == "CRITICAL" and spike[0]["vendor_id"] == "SPIKE"


# ---------------------------------------------------------------- SAP commit
def approved_state(draft, **kw):
    return S(batch_id="B-1", purchase_order_draft=draft, review_status="APPROVED",
             approved_draft_hash=draft_fingerprint(draft), **kw)


def test_commit_blocks_without_approval_or_after_change(draft, fake_sap):
    assert create_sap_purchase_order(S(batch_id="1", purchase_order_draft=draft))["execution_status"] == "FAILED"
    st = approved_state(draft)
    st.purchase_order_draft["items"][0]["quantity"] = 9999
    out = create_sap_purchase_order(st)
    assert out["anomalies_detected"][0]["type"] == "APPROVAL_DRAFT_MISMATCH"
    assert fake_sap.posts == []


def test_commit_creates_po_with_idempotency_reference(draft, fake_sap):
    out = create_sap_purchase_order(approved_state(draft))
    assert out["execution_status"] == "SUCCESS" and out["sap_po_numbers"] == ["4500000001"]
    payload = fake_sap.posts[0]
    assert payload["CorrespncInternalReference"] == idempotency_reference("B-1", "VEND-001")
    assert len(payload["CorrespncInternalReference"]) == 12
    assert payload["to_PurchaseOrderItem"][0] == {"PurchaseOrderItem": "00010", "Material": "MAT-102", "OrderQuantity": "100", "Plant": "1010"}


def test_commit_is_idempotent_on_redelivery(draft, fake_sap):
    fake_sap.get_responses["A_PurchaseOrder"] = {"results": [{"PurchaseOrder": "4500000042"}]}
    out = create_sap_purchase_order(approved_state(draft))
    assert out["sap_po_numbers"] == ["4500000042"] and fake_sap.posts == []


def test_commit_splits_po_per_vendor(draft, fake_sap):
    draft["items"][1]["vendor_id"] = "VEND-777"
    out = create_sap_purchase_order(approved_state(draft))
    assert out["execution_status"] == "SUCCESS"
    assert sorted(p["Supplier"] for p in fake_sap.posts) == ["VEND-001", "VEND-777"]


def test_commit_ambiguous_timeout_reconciles_by_lookup(draft, fake_sap):
    calls = {"n": 0}

    def lookup(params):
        calls["n"] += 1
        return {"results": [{"PurchaseOrder": "4500000077"}]} if calls["n"] > 1 else {"results": []}

    fake_sap.get_responses["A_PurchaseOrder"] = lookup
    fake_sap.post_error = SAPError("read timeout", ambiguous=True)
    out = create_sap_purchase_order(approved_state(draft))
    assert out["execution_status"] == "SUCCESS" and out["sap_po_numbers"] == ["4500000077"]


def test_commit_ambiguous_unresolved_requires_manual_reconciliation(draft, fake_sap):
    fake_sap.post_error = SAPError("read timeout", ambiguous=True)
    out = create_sap_purchase_order(approved_state(draft))
    assert out["anomalies_detected"][0]["type"] == "SAP_AMBIGUOUS_COMMIT"
    assert len(fake_sap.posts) == 1  # never blindly re-POSTed


def test_commit_rejects_bad_plant(draft, fake_sap):
    draft["items"][0]["plant"] = "berlin-1"
    out = create_sap_purchase_order(approved_state(draft))
    assert out["anomalies_detected"][0]["type"] == "SCHEMA_MISMATCH" and fake_sap.posts == []

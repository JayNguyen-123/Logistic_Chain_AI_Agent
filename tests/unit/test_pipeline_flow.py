"""End-to-end graph flow on a real LangGraph runtime (MemorySaver), externals faked.

Exercises the things that only show up at graph level: the interrupt position, parallel
fan-out/fan-in without InvalidUpdateError, reducer resets, and resume semantics.
"""
import pytest

pytest.importorskip("langgraph.checkpoint.memory")

from langgraph.checkpoint.memory import MemorySaver  # noqa: E402

from src.graph.nodes.sub_agents import sourcing  # noqa: E402
from src.graph.pipeline import HUMAN_GATE, compile_app  # noqa: E402
from src.graph.state import ExecutionCost  # noqa: E402


@pytest.fixture
def app():
    return compile_app(MemorySaver())


def cfg(bid):
    return {"configurable": {"thread_id": f"t-{bid}"}}


def start(app, bid, draft, **extra):
    app.invoke({"batch_id": bid, "purchase_order_draft": draft, **extra}, config=cfg(bid))
    return app.get_state(cfg(bid))


def decide(app, bid, **update):
    app.update_state(cfg(bid), {"reviewer_id": "tester", **update})
    app.invoke(None, config=cfg(bid))
    return app.get_state(cfg(bid))


def test_initial_run_pauses_at_human_gate(app, draft):
    snap = start(app, "90001", draft)
    assert snap.next == (HUMAN_GATE,)
    assert snap.values["execution_status"] == "AWAITING_REVIEW"
    assert snap.values["anomalies_detected"] == []


def test_resume_without_decision_pauses_again(app, draft):
    start(app, "r1", draft)
    app.invoke(None, config=cfg("r1"))
    assert app.get_state(cfg("r1")).next == (HUMAN_GATE,)


def test_amend_then_approve_commits(app, draft, fake_sap, monkeypatch):
    extracted = sourcing.MultiItemIntentExtraction(adjustments=[
        sourcing.SingleItemAdjustment(target_material_id="MAT-102", action="ADJUST_QUANTITY", adjustment_value="25", reasoning="r"),
        sourcing.SingleItemAdjustment(target_material_id="MAT-509", action="CHANGE_PLANT", adjustment_value="1020", reasoning="r"),
    ])
    monkeypatch.setattr(sourcing, "extract_adjustments", lambda *a: (extracted, ExecutionCost(input_tokens=500, usd_cost=0.002)))
    start(app, "90002", draft)
    snap = decide(app, "90002", human_action="AMEND", human_feedback="Cut MAT-102 to 25 and move MAT-509 to 1020")
    items = {i["material_id"]: i for i in snap.values["purchase_order_draft"]["items"]}
    assert items["MAT-102"]["quantity"] == 25 and items["MAT-509"]["plant"] == "1020"
    assert snap.next == (HUMAN_GATE,) and snap.values["human_feedback"] is None

    snap = decide(app, "90002", human_action="APPROVE")
    assert snap.next == ()
    assert snap.values["execution_status"] == "SUCCESS"
    assert snap.values["total_run_cost"].usd_cost == pytest.approx(0.002)


def test_swap_vendor_fans_out_and_flags_price_spike(app, fake_sap):
    draft = {"vendor_id": "VEND-001", "items": [{"material_id": "MAT-102", "quantity": 50, "plant": "1010", "baseline_contract_price": 100.0}]}
    fake_sap.get_responses["A_PurchasingInfoRecord"] = {"results": [
        {"Supplier": "VEND-404", "to_PurgInfoRecdOrgPlantData": {"results": [{"Plant": "1010", "NetPriceAmount": "130", "MaterialPlannedDeliveryDurn": "28"}]}},
        {"Supplier": "VEND-500", "to_PurgInfoRecdOrgPlantData": {"results": [{"Plant": "1010", "NetPriceAmount": "105", "MaterialPlannedDeliveryDurn": "7"}]}},
    ]}
    start(app, "90003", draft)
    snap = decide(app, "90003", human_action="SWAP_VENDOR", target_material_id="MAT-102")
    assert snap.next == (HUMAN_GATE,)
    assert any(a["type"] == "PRICE_SPIKE" and a["severity"] == "CRITICAL" for a in snap.values["anomalies_detected"])
    assert snap.values["recommended_offer"].vendor_id == "VEND-500"

    # Second round must not see first-round offers (reducer reset)
    snap = decide(app, "90003", human_action="SWAP_VENDOR", target_material_id="MAT-102")
    assert len(snap.values["candidate_offers"]) == 2

    snap = decide(app, "90003", human_action="ACCEPT_OFFER")
    assert snap.values["purchase_order_draft"]["items"][0]["vendor_id"] == "VEND-500"
    snap = decide(app, "90003", human_action="APPROVE")
    assert snap.values["execution_status"] == "SUCCESS"
    assert fake_sap.posts[-1]["Supplier"] == "VEND-500"


def test_swap_without_material_is_bounced(app, draft):
    start(app, "90004", draft)
    snap = decide(app, "90004", human_action="SWAP_VENDOR")
    assert snap.next == (HUMAN_GATE,)
    assert snap.values["review_status"] == "AMENDED"
    assert snap.values["anomalies_detected"][-1]["type"] == "GATE_VALIDATION"


def test_cancel_terminates(app, draft):
    start(app, "c1", draft)
    snap = decide(app, "c1", human_action="CANCEL", human_feedback="not needed")
    assert snap.next == () and snap.values["execution_status"] == "CANCELLED"

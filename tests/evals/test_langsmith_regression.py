"""LangSmith regression evals against the golden dataset (real LLM, faked SAP).

Run explicitly:  pytest -m evals
Skipped automatically when LANGSMITH_API_KEY is absent (the original hard-failed the
whole test session, including unit tests, when the key was missing).

Fixes vs. original:
  * `assert_sap_compliance` was referenced but never defined (NameError).
  * The target invoked the graph once; with the human gate the run paused before any
    decision was applied, so every reference output was unreachable. The target now
    replays the human decision exactly like the Kafka worker does.
  * Results are read via the ExperimentResults row dict API.
  * Dataset is upserted (examples were only created when the dataset was missing).
"""
import json
import os
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.evals

if not os.environ.get("LANGSMITH_API_KEY"):
    pytest.skip("LANGSMITH_API_KEY not set", allow_module_level=True)

from langgraph.checkpoint.memory import MemorySaver  # noqa: E402
from langsmith import Client  # noqa: E402
from langsmith.evaluation import evaluate  # noqa: E402

from src.graph.pipeline import compile_app  # noqa: E402

DATASET_NAME = "Production_Supply_Chain_Golden_Dataset"
DATASET_PATH = Path(__file__).resolve().parents[1] / "mock_dataset.json"
COST_CEILING_USD = 0.50


def _to_jsonable(values):
    return json.loads(json.dumps(values, default=lambda o: o.model_dump() if hasattr(o, "model_dump") else str(o)))


def run_scenario(inputs: dict) -> dict:
    app = compile_app(MemorySaver())
    cfg = {"configurable": {"thread_id": f"eval-{inputs['batch_id']}-{uuid.uuid4().hex[:8]}"}}
    app.invoke({"batch_id": inputs["batch_id"], "purchase_order_draft": inputs["purchase_order_draft"]}, config=cfg)
    decision = inputs.get("human_decision")
    if decision:
        app.update_state(cfg, {"reviewer_id": "langsmith-eval", **decision})
        app.invoke(None, config=cfg)
    return _to_jsonable(app.get_state(cfg).values)


def sap_draft_compliance(run, example) -> dict:
    draft = (run.outputs or {}).get("purchase_order_draft", {})
    reference = (example.outputs or {}).get("purchase_order_draft", {})
    run_items = {i["material_id"]: i for i in draft.get("items", [])}
    for ref in reference.get("items", []):
        got = run_items.get(ref["material_id"])
        if got is None:
            return {"key": "sap_draft_compliance", "score": 0, "comment": f"{ref['material_id']} missing"}
        for field in ("quantity", "plant"):
            if field in ref and got.get(field) != ref[field]:
                return {"key": "sap_draft_compliance", "score": 0, "comment": f"{field} mismatch on {ref['material_id']}"}
    return {"key": "sap_draft_compliance", "score": 1}


def gateway_guardrails(run, example) -> dict:
    out, ref = run.outputs or {}, example.outputs or {}
    for field in ("review_status", "execution_status"):
        if field in ref and out.get(field) != ref[field]:
            return {"key": "gateway_guardrails", "score": 0, "comment": f"{field}: expected {ref[field]}, got {out.get(field)}"}
    got_types = {(a.get("type"), a.get("severity")) for a in out.get("anomalies_detected", [])}
    for expected in ref.get("anomalies_detected", []):
        if (expected["type"], expected.get("severity")) not in got_types:
            return {"key": "gateway_guardrails", "score": 0, "comment": f"missing anomaly {expected}"}
    if ref.get("anomalies_detected") == [] and out.get("anomalies_detected"):
        return {"key": "gateway_guardrails", "score": 0, "comment": "unexpected anomalies"}
    return {"key": "gateway_guardrails", "score": 1}


def cost_ceiling(run, example) -> dict:
    usd = ((run.outputs or {}).get("total_run_cost") or {}).get("usd_cost", 0.0)
    return {"key": "cost_efficiency_ceiling", "score": int(usd < COST_CEILING_USD), "comment": f"${usd:.4f}"}


def _sync_dataset(client: Client):
    cases = json.loads(DATASET_PATH.read_text())
    if client.has_dataset(dataset_name=DATASET_NAME):
        ds = client.read_dataset(dataset_name=DATASET_NAME)
        for ex in client.list_examples(dataset_id=ds.id):
            client.delete_example(ex.id)
    else:
        ds = client.create_dataset(dataset_name=DATASET_NAME, description="SAP Supply Chain golden scenarios")
    for case in cases:
        client.create_example(inputs=case["inputs"], outputs=case["reference_outputs"], dataset_id=ds.id,
                              metadata={"case_id": case["id"]})


def test_orchestrator_regression_pipeline(fake_sap):
    fake_sap.get_responses["A_PurchasingInfoRecord"] = {"results": [
        {"Supplier": "VEND-404", "to_PurgInfoRecdOrgPlantData": {"results": [{"Plant": "1010", "NetPriceAmount": "130", "MaterialPlannedDeliveryDurn": "28"}]}},
    ]}
    client = Client()
    _sync_dataset(client)
    results = evaluate(
        run_scenario,
        data=DATASET_NAME,
        evaluators=[sap_draft_compliance, gateway_guardrails, cost_ceiling],
        experiment_prefix="ci-multi-agent-pipeline",
        max_concurrency=2,
    )
    failures = []
    for row in results:
        for ev in row["evaluation_results"]["results"]:
            if ev.score != 1:
                failures.append(f"{row['example'].metadata.get('case_id')}: {ev.key} -> {ev.comment}")
    assert not failures, "\n".join(failures)

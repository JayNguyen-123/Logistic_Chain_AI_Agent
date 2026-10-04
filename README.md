# LogiChain AI Agent

An AI-assisted purchase-order reconciliation worker for SAP S/4HANA. It takes nightly purchase-order drafts from Kafka, prepares them for a human reviewer, and acts on the reviewer's decisions:

- applies plain-English edits;
- researches alternative suppliers;
- creates the purchase orders in SAP, but only after explicit approval.

Built on LangGraph, with a durable Postgres checkpointer. It runs on AWS EKS and is autoscaled from zero by KEDA.

---

## How it works

```
                   ┌───────────────────── LogiChain worker (src/main.py) ─────────────────────┐
 Kafka: batches ──►│ sourcing ──► [pause] human_gate ──┬─ APPROVE ────► SAP commit ──► END     │
                   │     ▲              ▲              ├─ AMEND ──────► sourcing (LLM edits)  │
 Kafka: decisions ►│     └──────────────┤              ├─ SWAP_VENDOR ► inventory ┐           │
                   │                    │              │                logistics ┴► consolidate
 Kafka: review  ◄──│                    └──────────────┤                         (back to gate)│
   requests        │                                   ├─ ACCEPT_OFFER (apply recommendation) │
 Kafka: DLQ     ◄──│                                   └─ CANCEL ─────► END                    │
                   └──────────────────────────────────────────────────────────────────────────┘
                        │ Postgres (paused state)   │ SAP OData   │ OpenAI   │ Prometheus / PagerDuty
```

1. **A batch arrives** on the reconciliation topic. Each message is validated against `src/contracts.py`, and a graph thread named `batch_run_<batch_id>` is started.
2. **The sourcing node** stages the draft. The graph then **pauses before `human_gate`** and publishes a review request.
3. **The reviewer decides** in a dashboard, which sends a message on the human-decisions topic. The worker calls `update_state` and resumes the thread.
4. **The human gate** validates the decision, records it in the audit log, consumes it, and routes the batch:

   | Action | Effect |
   |---|---|
   | `APPROVE` | Locks the approval to a SHA-256 hash of the draft, then commits to SAP. |
   | `AMEND` + feedback | The LLM extracts structured edits. They are validated and applied deterministically, and the draft returns for review. |
   | `SWAP_VENDOR` + material | The inventory and logistics branches run in parallel. The consolidator scores the offers, flags anything more than 15% over the contract price, and recommends one. |
   | `ACCEPT_OFFER` | Applies the recommended supplier to that line. A fresh approval is still required. |
   | `CANCEL` | Ends the workflow. |

5. **The SAP commit** groups lines by supplier and creates one purchase order per supplier. Each order carries an idempotency reference that is looked up before every create, so the commit is safe to retry.

## Safety guarantees

- **No posting without approval.** A posting needs an approved draft whose hash matches the current draft.
- **No duplicate purchase orders.** A deterministic `CorrespncInternalReference` is checked before every create. If a POST times out, the commit reconciles by looking up that reference and never re-POSTs.
- **No invented data.** If SAP lookups fail, the worker raises anomalies. It never falls back to made-up stock or prices.
- **No replay loops.** Each decision is consumed once, and `MAX_REVIEW_ITERATIONS` caps the review cycles.
- **Limited LLM reach.** The LLM sees only material, quantity and plant. It can only edit materials already in the draft, and its output passes through a closed action set.
- **Durable Kafka handling.** Offsets are committed only after the outcome is durable. Poison messages go to the DLQ. If the DLQ publish fails, the offset is not committed and the worker exits.

## Repository layout

```
src/
  main.py                     Kafka worker (consume, dispatch, DLQ, SIGTERM, outcomes)
  config.py                   Typed settings (env / .env), production invariants
  contracts.py                Inbound message schemas
  logging_config.py           JSON logging
  integrations/sap_client.py  OAuth2 + CSRF OData client, read retries, literal escaping
  telemetry/                  Prometheus metrics, /livez /readyz, PagerDuty
  graph/
    state.py                  Graph state + reducers
    pipeline.py               Graph wiring + checkpointer
    nodes/                    human gate, consolidator, SAP commit, sub_agents/{sourcing,inventory,logistics}
tests/
  unit/                       Offline tests (nodes, worker, full graph flow)
  evals/                      LangSmith regression evals (opt-in)
  mock_dataset.json           Golden scenarios
k8s/                          Namespace, ServiceAccount, Deployment+PDB, ExternalSecret, KEDA
monitoring/                   Prometheus scrape config + alert rules
.github/workflows/deploy.yml  CI → build/scan/push → EKS rollout
REVIEW.md                     Production-readiness review and change log
```

## Kafka topics

| Topic (env var) | Direction | Payload |
|---|---|---|
| `KAFKA_TOPIC_RECONCILIATION` | in | `BatchJobMessage`: `batch_id`, `purchase_order_draft{vendor_id, items[]}` |
| `KAFKA_TOPIC_HUMAN_DECISIONS` | in | `HumanDecisionMessage`: `batch_id`, `human_action`, `reviewer_id`, optional `human_feedback` and `target_material_id` |
| `KAFKA_TOPIC_REVIEW_REQUESTS` | out | Draft, recommended offer, recent anomalies |
| `KAFKA_TOPIC_DLQ` | out | Original message plus `dlq_reason` and source headers |

Example batch message:

```json
{
  "batch_id": "90001",
  "purchase_order_draft": {
    "vendor_id": "VEND-001",
    "company_code": "1010",
    "items": [
      {"material_id": "MAT-102", "quantity": 100, "plant": "1010", "baseline_contract_price": 150.0}
    ]
  }
}
```

Example decision message:

```json
{"batch_id": "90001", "human_action": "AMEND", "reviewer_id": "jdoe",
 "human_feedback": "Cut MAT-102 to 25 units"}
```

## Local development

Requires Python 3.12 or later. For a local run you also need Kafka and an SAP sandbox; the OpenAI and PagerDuty keys can be dummies for tests.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env            # fill in values; ENVIRONMENT=development allows the in-memory checkpointer

ruff check src tests
pytest                          # offline unit + graph-flow tests (no network)
pytest -m evals                 # LangSmith evals; needs LANGSMITH_API_KEY and OPENAI_API_KEY

python -m src.main              # start the worker
```

Metrics are served at `:8000/metrics`. Health checks are at `:8081/livez` and `:8081/readyz`.

## Configuration

All settings are environment variables; see `src/config.py` for the full list and defaults. The key ones:

| Variable | Notes |
|---|---|
| `ENVIRONMENT` | `development`, `staging` or `production`. Outside development, the worker requires a checkpoint DB and HTTPS SAP endpoints. |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*` | Broker connection settings |
| `SAP_BASE_URL`, `SAP_TOKEN_URL`, `SAP_CLIENT_ID`, `SAP_CLIENT_SECRET` | S/4HANA OData connection, using OAuth2 client credentials |
| `OPENAI_API_KEY`, `OPENAI_MODEL` | LLM for the amend feedback; `OPENAI_*_USD_PER_MTOK` sets the cost metric |
| `PAGERDUTY_INTEGRATION_KEY` | Events API v2 routing key |
| `CHECKPOINT_DATABASE_URL` | Postgres for paused reviews (required outside development) |
| `MAX_REVIEW_ITERATIONS`, `PRICE_SPIKE_THRESHOLD`, `MAX_QUANTITY_PER_LINE` | Business guardrails |

## Deployment

1. **Lock dependencies.** CI refuses to build without the lock file:

   ```bash
   pip-compile --generate-hashes --strip-extras -o requirements.lock requirements.txt
   ```

2. **Provision:**
   - the four Kafka topics;
   - PostgreSQL;
   - AWS Secrets Manager entries (see `k8s/external-secret.yaml`);
   - an IRSA role;
   - the ECR repository.
3. **Fill in placeholders.** Replace every `<PLACEHOLDER>` in `k8s/`, and set the GitHub variables `AWS_ROLE_ARN` and `AWS_DEPLOY_ROLE_ARN`.
4. **Apply the manifests:**

   ```bash
   kubectl apply -f k8s/
   ```

5. **Push to `main`.** CI runs the tests, builds the image, scans it with Trivy, pushes the `:<git-sha>` image, and rolls out with `kubectl set image`.
6. **Load the monitoring config.** Add `monitoring/alert.rules.yml` and `monitoring/prometheus.yml` to your Prometheus setup. The Kafka lag alert needs kafka-exporter.

## Before go-live

- **SAP field names.** Verify the entity sets and field names against your S/4HANA release: `A_MatlStkInAcctMod`, `A_PurchasingInfoRecord` with `to_PurgInfoRecdOrgPlantData`, and `CorrespncInternalReference`. They are centralised in `src/integrations/sap_client.py` and in the nodes.
- **Dashboard.** Build or connect a review dashboard that consumes review requests and produces decisions.
- **Graph-flow tests.** Confirm `tests/unit/test_pipeline_flow.py` passes in CI against the locked dependency versions.

## Operations

| Alert | Meaning and first response |
|---|---|
| `HighAgentFailureRate` | More than 15% of events failed or were dead-lettered. Check the worker logs and the DLQ headers (`dlq_reason`). |
| `SAPAmbiguousCommit` | A purchase-order POST outcome is unknown. Search SAP for the logged 12-character reference before taking any action. |
| `AgentCostSpike` | LLM spend is over $150 per hour. Look for review loops or abusive feedback. |
| `KafkaConsumerLag` | There is a backlog. Check KEDA, the partition count and stuck pods. |
| `WorkerLoopStalled` | There is no consume-loop heartbeat. Liveness should restart the pod; investigate any hang. |

**Replaying a DLQ message.** Fix the cause, then re-publish the message to its source topic.

A batch whose graph thread already exists is skipped on redelivery. To resume a paused batch, send a new decision instead of replaying the batch.

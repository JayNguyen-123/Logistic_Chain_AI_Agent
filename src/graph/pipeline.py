"""Graph wiring.

    START -> sourcing_subgraph -> [interrupt] human_gate --route_after_gate-->
        commit       -> execute_sap_commit -> END
        sourcing     -> sourcing_subgraph -> human_gate ...
        fan_out      -> inventory_branch + logistics_branch -> consolidation_node -> human_gate ...
        await_review -> human_gate (pauses again)
        cancel       -> cancel_workflow -> END

Fixes vs. original:
  * Interrupt moved from the entry node to `human_gate` (originally every run paused
    before doing anything and, when resumed, the router saw PENDING and cancelled).
  * `human_gate` (checkpoint_gate.py) is actually wired in.
  * Consolidation returns to the human gate, not to the LLM sourcing node (which re-ran
    the LLM on stale feedback each loop).
  * Explicit fan-in join: consolidation waits for BOTH branches.
  * Cancel node sets a declared status (CANCELLED) instead of an undeclared key.
  * Durable Postgres checkpointer outside development. MemorySaver loses every paused
    review when the pod restarts or KEDA scales to zero, and is not shared across replicas.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from langgraph.graph import END, START, StateGraph

from src.config import get_settings
from src.graph.nodes.checkpoint_gate import human_checkpoint_verification_gate, route_after_gate
from src.graph.nodes.consolidator import consolidation_evaluator
from src.graph.nodes.sap_integration import create_sap_purchase_order
from src.graph.nodes.sub_agents.inventory import inventory_branch_agent
from src.graph.nodes.sub_agents.logistics import logistics_branch_agent
from src.graph.nodes.sub_agents.sourcing import sourcing_agent_multi
from src.graph.state import ParallelSupplyChainState

logger = logging.getLogger("pipeline")

HUMAN_GATE = "human_gate"


def log_and_terminate_workflow(state: ParallelSupplyChainState) -> Dict[str, Any]:
    return {
        "execution_status": "CANCELLED",
        "last_action_taken": "Workflow terminated (cancelled by reviewer or loop ceiling reached).",
        "audit_log": ["cancel: workflow terminated"],
    }


def build_workflow() -> StateGraph:
    g = StateGraph(ParallelSupplyChainState)
    g.add_node("sourcing_subgraph", sourcing_agent_multi)
    g.add_node(HUMAN_GATE, human_checkpoint_verification_gate)
    g.add_node("inventory_branch", inventory_branch_agent)
    g.add_node("logistics_branch", logistics_branch_agent)
    g.add_node("consolidation_node", consolidation_evaluator)
    g.add_node("execute_sap_commit", create_sap_purchase_order)
    g.add_node("cancel_workflow", log_and_terminate_workflow)

    g.add_edge(START, "sourcing_subgraph")
    g.add_edge("sourcing_subgraph", HUMAN_GATE)
    g.add_conditional_edges(
        HUMAN_GATE,
        route_after_gate,
        ["execute_sap_commit", "sourcing_subgraph", "inventory_branch", "logistics_branch", "cancel_workflow", HUMAN_GATE],
    )
    g.add_edge(["inventory_branch", "logistics_branch"], "consolidation_node")  # join
    g.add_edge("consolidation_node", HUMAN_GATE)
    g.add_edge("execute_sap_commit", END)
    g.add_edge("cancel_workflow", END)
    return g


def build_checkpointer():
    s = get_settings()
    if s.CHECKPOINT_DATABASE_URL is None:
        # Allowed only in development (enforced by AppSettings validator).
        from langgraph.checkpoint.memory import MemorySaver

        logger.warning("Using in-memory checkpointer - paused reviews will NOT survive a restart")
        return MemorySaver()

    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    pool = ConnectionPool(
        conninfo=s.CHECKPOINT_DATABASE_URL.get_secret_value(),
        min_size=1,
        max_size=4,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
        open=True,
    )
    saver = PostgresSaver(pool)
    saver.setup()  # idempotent migrations
    return saver


def compile_app(checkpointer=None):
    return build_workflow().compile(
        checkpointer=checkpointer if checkpointer is not None else build_checkpointer(),
        interrupt_before=[HUMAN_GATE],
    )

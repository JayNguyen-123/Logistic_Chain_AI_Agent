"""Inbound Kafka message contracts. Anything that fails validation goes to the DLQ."""
from __future__ import annotations

from typing import List

from pydantic import BaseModel, ConfigDict, Field

from src.graph.state import HumanAction


class DraftItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    material_id: str = Field(..., min_length=1, max_length=40)
    quantity: float = Field(..., gt=0)
    plant: str = Field(..., pattern=r"^[A-Z0-9]{4}$")
    vendor_id: str | None = None
    baseline_contract_price: float | None = Field(default=None, ge=0)


class PurchaseOrderDraft(BaseModel):
    model_config = ConfigDict(extra="allow")

    vendor_id: str = Field(..., min_length=1)
    company_code: str = "1010"
    purch_org: str = "1010"
    purch_group: str = "001"
    items: List[DraftItem] = Field(..., min_length=1)


class BatchJobMessage(BaseModel):
    """New reconciliation batch (topic: KAFKA_TOPIC_RECONCILIATION)."""

    batch_id: str = Field(..., min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    purchase_order_draft: PurchaseOrderDraft
    target_material_id: str | None = ""


class HumanDecisionMessage(BaseModel):
    """Reviewer decision from the dashboard (topic: KAFKA_TOPIC_HUMAN_DECISIONS)."""

    batch_id: str = Field(..., min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    human_action: HumanAction
    reviewer_id: str = Field(..., min_length=1)
    human_feedback: str | None = Field(default=None, max_length=4000)
    target_material_id: str | None = None

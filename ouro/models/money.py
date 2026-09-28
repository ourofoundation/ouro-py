"""Wallet models. Bitcoin amounts are in sats; USD amounts are in cents."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field

from ._base import OuroModel, Page

__all__ = [
    "BitcoinBalance",
    "BitcoinPurchase",
    "BitcoinTransaction",
    "BitcoinTransfer",
    "PendingEarnings",
    "UsageHistory",
    "UsageRecord",
    "UsdBalance",
    "UsdPurchase",
    "UsdTip",
    "UsdTransaction",
]


class BitcoinBalance(OuroModel):
    balance: int
    available: int
    escrowed: int = 0


class UsdBalance(OuroModel):
    balance_cents: int
    available_cents: int
    pending_cents: int = 0
    escrowed_cents: int = 0
    currency: str = "usd"
    onboarded: bool = False
    transfers_enabled: bool = False
    payouts_enabled: bool = False
    payouts_available_soon: bool = False
    requirements: List[Any] = Field(default_factory=list)
    last_updated: Optional[datetime] = None


class BitcoinTransaction(OuroModel):
    id: UUID
    type: str
    value: int
    status: str
    user_id: Optional[UUID] = None
    wallet_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    action_id: Optional[UUID] = None
    is_external: bool = False
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class UsdTransaction(OuroModel):
    id: UUID
    type: str
    amount_cents: int
    currency: str = "usd"
    status: str
    user_id: Optional[UUID] = None
    recipient_user_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class BitcoinTransfer(OuroModel):
    """A Spark network transfer."""

    id: str
    status: Optional[str] = None
    total_value: Optional[int] = Field(default=None, alias="totalValue")


class UsdTip(OuroModel):
    payment_intent_id: str
    transfer_id: Optional[str] = None
    amount_cents: int
    recipient_amount_cents: Optional[int] = None
    platform_fee_cents: Optional[int] = None


class BitcoinPurchase(OuroModel):
    transfer: BitcoinTransfer
    platform_fee_transfer: Optional[BitcoinTransfer] = None
    platform_fee_sats: int = 0


class UsdPurchase(OuroModel):
    payment_intent_id: str
    price_cents: int
    platform_fee_cents: int = 0
    total_cents: int


class UsageRecord(OuroModel):
    """One metered charge for a pay-per-use route."""

    id: UUID
    user_id: Optional[UUID] = None
    creator_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    asset: Optional[Dict[str, Any]] = None
    action_id: Optional[UUID] = None
    quantity: float = 0
    cost_unit: Optional[str] = None
    unit_cost_cents: int = 0
    total_cents: int = 0
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


class UsageSummary(OuroModel):
    record_count: int = 0
    total_cents: int = 0


class UsageHistory(Page[UsageRecord]):
    summary: UsageSummary = Field(default_factory=UsageSummary)


class PendingEarnings(OuroModel):
    total_pending_cents: int = 0
    in_progress_cents: int = 0
    total_paid_out_cents: int = 0
    assets: List[Dict[str, Any]] = Field(default_factory=list)

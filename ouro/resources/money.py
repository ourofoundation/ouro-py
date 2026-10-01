from __future__ import annotations

import logging
from typing import List, Literal, Optional, Union, overload

from ouro._resource import SyncAPIResource
from ouro.models import (
    BitcoinBalance,
    BitcoinPurchase,
    BitcoinTransaction,
    BitcoinTransfer,
    Page,
    PendingEarnings,
    UsageHistory,
    UsdBalance,
    UsdPurchase,
    UsdTip,
    UsdTransaction,
)

log: logging.Logger = logging.getLogger(__name__)

__all__ = ["Money"]

Currency = Literal["btc", "usd"]
VALID_CURRENCIES = ("btc", "usd")


def _validate_currency(currency: str) -> str:
    currency = currency.lower()
    if currency not in VALID_CURRENCIES:
        raise ValueError(f"currency must be one of {VALID_CURRENCIES}, got '{currency}'")
    return currency


class Money(SyncAPIResource):
    @overload
    def get_balance(self, currency: Literal["btc"] = "btc") -> BitcoinBalance: ...
    @overload
    def get_balance(self, currency: Literal["usd"]) -> UsdBalance: ...

    def get_balance(self, currency: Currency = "btc") -> Union[BitcoinBalance, UsdBalance]:
        """Get wallet balance: sats for ``"btc"``, cents for ``"usd"``."""
        if _validate_currency(currency) == "btc":
            request = self.client.get("/wallet/balance")
            return self._parse(BitcoinBalance, self._handle_response(request))
        request = self.client.get("/stripe/wallet/balance")
        return self._parse(UsdBalance, self._handle_response(request))

    @overload
    def get_transactions(self, currency: Literal["btc"] = "btc") -> List[BitcoinTransaction]: ...
    @overload
    def get_transactions(
        self,
        currency: Literal["usd"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        type: Optional[str] = None,
    ) -> Page[UsdTransaction]: ...

    def get_transactions(
        self,
        currency: Currency = "btc",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        type: Optional[str] = None,
    ) -> Union[List[BitcoinTransaction], Page[UsdTransaction]]:
        """Get transaction history.

        Bitcoin history is returned whole; USD history is paginated and can be
        filtered with ``limit``, ``offset``, and ``type``.
        """
        if _validate_currency(currency) == "btc":
            request = self.client.get("/wallet/transactions")
            return self._parse_list(BitcoinTransaction, self._handle_response(request))

        params = {"limit": limit, "offset": offset, "type": type}
        request = self.client.get(
            "/stripe/wallet/transactions",
            params={k: v for k, v in params.items() if v is not None},
        )
        return self._page(Page[UsdTransaction], self._handle_response(request, raw=True))

    @overload
    def unlock_asset(
        self, asset_type: str, asset_id: str, currency: Literal["btc"] = "btc"
    ) -> BitcoinPurchase: ...
    @overload
    def unlock_asset(
        self, asset_type: str, asset_id: str, currency: Literal["usd"]
    ) -> UsdPurchase: ...

    def unlock_asset(
        self,
        asset_type: str,
        asset_id: str,
        currency: Currency = "btc",
    ) -> Union[BitcoinPurchase, UsdPurchase]:
        """Unlock (purchase) a paid asset.

        Args:
            asset_type: The type of asset (e.g. "post", "file", "dataset").
            asset_id: The asset's UUID.
            currency: "btc" or "usd". An asset can be sold in both, each at
                its own price (``price_sats`` / ``price_usd``); pick one it
                is sold in.
        """
        payload = {"assetType": asset_type, "assetId": asset_id}
        if _validate_currency(currency) == "btc":
            request = self.client.post("/wallet/purchase-asset", json=payload)
            return self._parse(BitcoinPurchase, self._handle_response(request))
        request = self.client.post("/stripe/wallet/purchase-asset", json=payload)
        return self._parse(UsdPurchase, self._handle_response(request))

    @overload
    def send(
        self, recipient_id: str, amount: int, currency: Literal["btc"] = "btc", message: None = None
    ) -> BitcoinTransfer: ...
    @overload
    def send(
        self, recipient_id: str, amount: int, currency: Literal["usd"], message: Optional[str] = None
    ) -> UsdTip: ...

    def send(
        self,
        recipient_id: str,
        amount: int,
        currency: Currency = "btc",
        message: Optional[str] = None,
    ) -> Union[BitcoinTransfer, UsdTip]:
        """Send money to another Ouro user.

        Args:
            recipient_id: The recipient's user UUID.
            amount: Amount in sats (BTC) or cents (USD).
            currency: "btc" or "usd".
            message: Optional message (USD tips only).
        """
        if _validate_currency(currency) == "btc":
            payload = {"recipientId": recipient_id, "amount": amount}
            request = self.client.post("/wallet/send-sats", json=payload)
            return self._parse(BitcoinTransfer, self._handle_response(request))

        payload = {"recipientId": recipient_id, "amountCents": amount}
        if message is not None:
            payload["message"] = message
        request = self.client.post("/stripe/wallet/tip", json=payload)
        return self._parse(UsdTip, self._handle_response(request))

    def get_deposit_address(self) -> str:
        """Get a Bitcoin L1 deposit address for receiving funds."""
        request = self.client.get("/wallet/deposit-address")
        return self._handle_response(request) or ""

    def get_usage_history(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        asset_id: Optional[str] = None,
        role: Optional[str] = None,
    ) -> UsageHistory:
        """Get a page of usage-based billing records, with a summary.

        Args:
            limit: Max number of records.
            offset: Pagination offset.
            asset_id: Filter by asset ID.
            role: "consumer" or "creator".
        """
        params = {"limit": limit, "offset": offset, "assetId": asset_id, "role": role}
        request = self.client.get(
            "/stripe/usage/history",
            params={k: v for k, v in params.items() if v is not None},
        )
        body = self._handle_response(request, raw=True) or {}
        history = body.get("data") or {}
        return self._page(
            UsageHistory,
            {"data": history.get("records"), "pagination": body.get("pagination")},
            summary=history.get("summary") or {},
        )

    def get_pending_earnings(self) -> PendingEarnings:
        """Get pending creator earnings (USD)."""
        request = self.client.get("/stripe/wallet/pending-earnings")
        return self._parse(PendingEarnings, self._handle_response(request))

    def add_funds(self) -> str:
        """Returns instructions for adding USD funds.

        USD top-ups must be done through the Ouro web app.
        """
        return (
            "To add USD funds to your wallet, visit https://ouro.foundation "
            "and use the wallet top-up feature in your account settings."
        )

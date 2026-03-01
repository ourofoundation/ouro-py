from __future__ import annotations

import logging
from typing import Optional, Union

from ouro._resource import SyncAPIResource

log: logging.Logger = logging.getLogger(__name__)

__all__ = ["Money"]

VALID_CURRENCIES = ("btc", "usd")


def _validate_currency(currency: str) -> str:
    currency = currency.lower()
    if currency not in VALID_CURRENCIES:
        raise ValueError(f"currency must be one of {VALID_CURRENCIES}, got '{currency}'")
    return currency


class Money(SyncAPIResource):
    def get_balance(self, currency: str = "btc") -> dict:
        """Get wallet balance.

        Args:
            currency: "btc" (returns sats) or "usd" (returns cents).
        """
        currency = _validate_currency(currency)

        if currency == "btc":
            request = self.client.get("/wallet/balance")
        else:
            request = self.client.get("/stripe/wallet/balance")

        return self._handle_response(request) or {}

    def get_transactions(
        self,
        currency: str = "btc",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        type: Optional[str] = None,
        with_pagination: bool = False,
    ) -> Union[list, dict]:
        """Get transaction history.

        Args:
            currency: "btc" or "usd".
            limit: Max number of transactions (USD only).
            offset: Pagination offset (USD only).
            type: Filter by transaction type (USD only).
        """
        currency = _validate_currency(currency)

        if currency == "btc":
            request = self.client.get("/wallet/transactions")
        else:
            params = {}
            if limit is not None:
                params["limit"] = limit
            if offset is not None:
                params["offset"] = offset
            if type is not None:
                params["type"] = type
            request = self.client.get("/stripe/wallet/transactions", params=params)

        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": [], "pagination": None}
            result["data"] = result.get("data") or []
            return result
        return self._handle_response(request) or []

    def unlock_asset(
        self,
        asset_type: str,
        asset_id: str,
        currency: str = "btc",
    ) -> dict:
        """Unlock (purchase) a paid asset.

        Args:
            asset_type: The type of asset (e.g. "post", "file", "dataset").
            asset_id: The asset's UUID.
            currency: "btc" or "usd".
        """
        currency = _validate_currency(currency)
        payload = {"assetType": asset_type, "assetId": asset_id}

        if currency == "btc":
            request = self.client.post("/wallet/purchase-asset", json=payload)
        else:
            request = self.client.post("/stripe/wallet/purchase-asset", json=payload)

        return self._handle_response(request) or {}

    def send(
        self,
        recipient_id: str,
        amount: int,
        currency: str = "btc",
        message: Optional[str] = None,
    ) -> dict:
        """Send money to another Ouro user.

        Args:
            recipient_id: The recipient's user UUID.
            amount: Amount in sats (BTC) or cents (USD).
            currency: "btc" or "usd".
            message: Optional message (USD tips only).
        """
        currency = _validate_currency(currency)

        if currency == "btc":
            payload = {"recipientId": recipient_id, "amount": amount}
            request = self.client.post("/wallet/send-sats", json=payload)
        else:
            payload = {"recipientId": recipient_id, "amountCents": amount}
            if message is not None:
                payload["message"] = message
            request = self.client.post("/stripe/wallet/tip", json=payload)

        return self._handle_response(request) or {}

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
        with_pagination: bool = False,
    ) -> dict:
        """Get usage-based billing history.

        Args:
            limit: Max number of records.
            offset: Pagination offset.
            asset_id: Filter by asset ID.
            role: "consumer" or "creator".
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if asset_id is not None:
            params["assetId"] = asset_id
        if role is not None:
            params["role"] = role

        request = self.client.get("/stripe/usage/history", params=params)
        if with_pagination:
            result = self._handle_response(request, with_pagination=True) or {}
            if not isinstance(result, dict):
                return {"data": {}, "pagination": None}
            result["data"] = result.get("data") or {}
            return result
        return self._handle_response(request) or {}

    def get_pending_earnings(self) -> dict:
        """Get pending creator earnings (USD)."""
        request = self.client.get("/stripe/wallet/pending-earnings")
        return self._handle_response(request) or {}

    def add_funds(self) -> str:
        """Returns instructions for adding USD funds.

        USD top-ups must be done through the Ouro web app.
        """
        return (
            "To add USD funds to your wallet, visit https://ouro.foundation "
            "and use the wallet top-up feature in your account settings."
        )

import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import Field

from ._base import OuroModel, Page
from .asset import AssetRef, UserProfile

log = logging.getLogger(__name__)

ActionStatus = Literal["queued", "in-progress", "timed-out", "success", "error"]


class ActionLog(OuroModel):
    """A log entry emitted while a route action is running."""

    id: UUID
    action_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    asset_id: Optional[UUID] = None
    event_type: Optional[str] = None
    level: Optional[str] = None
    message: Optional[str] = None
    origin: Optional[str] = None
    source: Optional[str] = None
    client: Optional[str] = None
    sdk: Optional[str] = None
    api_key_name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    user: Optional[UserProfile] = None
    asset: Optional[AssetRef] = None


class Action(OuroModel):
    """Represents an action (route execution) in the Ouro system."""

    id: UUID
    route_id: UUID
    user_id: UUID
    status: ActionStatus
    input_asset_id: Optional[UUID] = None
    output_asset_id: Optional[UUID] = None
    response: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = None
    side_effects: Optional[bool] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    # When the upstream service first responded (200 or 202). For sync routes
    # this is approximately equal to ``finished_at``; for async routes the
    # gap between ``ack_at`` and ``finished_at`` is the out-of-band wait
    # time the upstream took to complete after acknowledging the request.
    ack_at: Optional[datetime] = None
    # HTTP status code returned by the upstream service at ack time.
    upstream_status_code: Optional[int] = None
    finished_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    input_asset: Optional[AssetRef] = None
    input_assets: Optional[List[Dict[str, Any]]] = None
    output_asset: Optional[AssetRef] = None
    # Named outputs: ``[{"name": ..., "asset": {...}}]``.
    output_assets: Optional[List[Dict[str, Any]]] = None
    route: Optional[Dict[str, Any]] = None
    user: Optional[UserProfile] = None
    # Per-call billing record for monetized pay-per-use USD routes. NULL when
    # the route is free, paid in BTC, or the caller doesn't have visibility
    # into the charge. Shape: {id, total_cents, unit_cost_cents, quantity,
    # cost_unit, status, stripe_invoice_id, created_at}.
    usage_record: Optional[Dict[str, Any]] = None
    # BTC charges produced by this action — buyer "route_usage" (negative
    # sats) and seller "route_revenue" (positive sats). RLS filters to rows
    # visible to the caller, so usually 0 or 1 row. Each row has
    # {id, type, value, status, metadata, created_at}.
    btc_charges: Optional[List[Dict[str, Any]]] = None

    @property
    def is_complete(self) -> bool:
        """Check if the action has finished polling."""
        return self.status in ("success", "error", "timed-out")

    @property
    def is_pending(self) -> bool:
        """Check if the action is still queued or in progress."""
        return self.status in ("queued", "in-progress")

    @property
    def is_success(self) -> bool:
        """Check if the action completed successfully."""
        return self.status == "success"

    @property
    def is_error(self) -> bool:
        """Check if the action failed."""
        return self.status == "error"

    @property
    def is_timed_out(self) -> bool:
        """Check if the action was marked stale but may still resolve later."""
        return self.status == "timed-out"

    @property
    def final_data(self) -> Any:
        """The response payload with output assets merged in, as plain data.

        Output assets are merged into the response under their declared output
        name (e.g. ``{"report": {...}}`` for a route declaring a ``report``
        output). Legacy single-output routes merge under the asset type
        instead (e.g. ``{"dataset": {...}}``). Otherwise the raw ``response``
        is returned unchanged.
        """
        response_data = self.response
        if self.output_assets:
            if not isinstance(response_data, dict):
                response_data = (
                    {"_raw": response_data} if response_data is not None else {}
                )
            for output in self.output_assets:
                name = output.get("name")
                asset = output.get("asset") or output
                if name and isinstance(asset, dict):
                    response_data[name] = asset
        elif self.output_asset:
            if not isinstance(response_data, dict):
                response_data = (
                    {"_raw": response_data} if response_data is not None else {}
                )
            if self.output_asset.asset_type:
                response_data[self.output_asset.asset_type] = (
                    self.output_asset.model_dump(mode="json")
                )
        return response_data

    def log(
        self,
        message: str,
        *,
        level: str = "info",
        asset_id: Optional[str] = None,
    ) -> None:
        """Post a log message to this action.

        Args:
            message: The log message text.
            level: Log level — "info", "warning", or "error" (default: "info").
            asset_id: Asset ID to associate with the log.
                Defaults to this action's route_id.
        """
        ouro = self._require_client()
        payload: Dict[str, Any] = {
            "message": message,
            "level": level,
            "asset_id": asset_id or str(self.route_id),
        }
        try:
            ouro.client.post(f"/actions/{self.id}/log", json=payload)
        except Exception as e:
            log.warning(
                "Failed to post action log (action_id=%s): %s",
                self.id,
                e,
                exc_info=True,
            )

    def read_logs(
        self,
        *,
        level: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        sort_order: str = "desc",
        chronological: Optional[bool] = None,
    ) -> Page[ActionLog]:
        """Read logs for this action."""
        return self._require_client().routes.get_action_logs(
            str(self.id),
            level=level,
            limit=limit,
            offset=offset,
            sort_order=sort_order,
            chronological=chronological,
        )

    def refresh(self) -> "Action":
        """Re-fetch this action from the server and update it in place."""
        updated = self._require_client().routes.retrieve_action(str(self.id))
        for field in type(self).model_fields:
            setattr(self, field, getattr(updated, field))
        return self

    def wait(
        self,
        *,
        poll_interval: float = 1.0,
        timeout: Optional[float] = None,
    ) -> "Action":
        """
        Wait for this action to complete by polling.

        Args:
            poll_interval: Seconds between status checks (default: 1.0)
            timeout: Maximum seconds to wait (default: None = wait forever)

        Returns:
            The completed Action

        Raises:
            TimeoutError: If timeout is reached before completion
            Exception: If the action completed with an error
        """
        return self._require_client().routes.poll_action(
            str(self.id),
            poll_interval=poll_interval,
            timeout=timeout,
        )


class AssetActions(OuroModel):
    """Route actions linked to one asset, in both directions."""

    created_by: Optional[Action] = None
    as_input: List[Action] = Field(default_factory=list)
    has_more: bool = False

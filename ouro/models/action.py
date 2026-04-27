import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ouro import Ouro


ActionStatus = Literal["queued", "in-progress", "timed-out", "success", "error"]


class ActionLog(BaseModel):
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
    api_key_name: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

    # Nested objects from joins
    user: Optional[Dict[str, Any]] = None
    asset: Optional[Dict[str, Any]] = None


class Action(BaseModel):
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
    finished_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    # Nested objects from joins
    input_asset: Optional[Dict[str, Any]] = None
    input_assets: Optional[list[Dict[str, Any]]] = None
    output_asset: Optional[Dict[str, Any]] = None
    route: Optional[Dict[str, Any]] = None
    user: Optional[Dict[str, Any]] = None

    _ouro: Optional["Ouro"] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        object.__setattr__(self, "_ouro", kwargs.get("_ouro"))

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
        """Return the response payload shaped like :meth:`Routes.use` returns.

        If the action created an output asset, it's merged into the response
        under the asset-type key (e.g. ``{"dataset": {...}}``). Otherwise the
        raw ``response`` is returned unchanged. Useful for migrating callers
        from :meth:`Routes.use` (dict return) to :meth:`Routes.execute`
        (Action return) without changing downstream code.
        """
        response_data = self.response
        if self.output_asset:
            if not isinstance(response_data, dict):
                response_data = (
                    {"_raw": response_data} if response_data is not None else {}
                )
            asset_type = self.output_asset.get("asset_type")
            if asset_type:
                response_data[asset_type] = self.output_asset
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
        if not self._ouro:
            raise RuntimeError("Action object not connected to Ouro client")
        payload: Dict[str, Any] = {
            "message": message,
            "level": level,
            "asset_id": asset_id or str(self.route_id),
        }
        try:
            self._ouro.client.post(f"/actions/{self.id}/log", json=payload)
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
        with_pagination: bool = False,
    ):
        """Read logs for this action."""
        if not self._ouro:
            raise RuntimeError("Action object not connected to Ouro client")
        return self._ouro.routes.get_action_logs(
            str(self.id),
            level=level,
            limit=limit,
            offset=offset,
            sort_order=sort_order,
            chronological=chronological,
            with_pagination=with_pagination,
        )

    def refresh(self) -> "Action":
        """
        Refresh this action's data from the server.
        Returns the updated Action instance.
        """
        if not self._ouro:
            raise RuntimeError("Action object not connected to Ouro client")
        updated = self._ouro.routes.retrieve_action(str(self.id))
        # Update this instance with the new data
        for field in self.model_fields:
            if field != "_ouro":
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
        if not self._ouro:
            raise RuntimeError("Action object not connected to Ouro client")
        return self._ouro.routes.poll_action(
            str(self.id),
            poll_interval=poll_interval,
            timeout=timeout,
        )

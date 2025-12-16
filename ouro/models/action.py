from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from ouro import Ouro


ActionStatus = Literal["pending", "in-progress", "success", "error"]


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
    created_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    # Nested objects from joins
    input_asset: Optional[Dict[str, Any]] = None
    output_asset: Optional[Dict[str, Any]] = None
    user: Optional[Dict[str, Any]] = None

    _ouro: Optional["Ouro"] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        object.__setattr__(self, "_ouro", kwargs.get("_ouro"))

    @property
    def is_complete(self) -> bool:
        """Check if the action has finished (success or error)."""
        return self.status in ("success", "error")

    @property
    def is_pending(self) -> bool:
        """Check if the action is still pending or in progress."""
        return self.status in ("pending", "in-progress")

    @property
    def is_success(self) -> bool:
        """Check if the action completed successfully."""
        return self.status == "success"

    @property
    def is_error(self) -> bool:
        """Check if the action failed."""
        return self.status == "error"

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

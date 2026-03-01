from __future__ import annotations

import logging
from typing import Any, List

from ouro._resource import SyncAPIResource

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Users"]


class Users(SyncAPIResource):
    def search(
        self,
        query: str,
        **kwargs: Any,
    ) -> List[dict]:
        """Search for users."""
        request = self.client.get(
            "/users/search",
            params={"query": query, **kwargs},
        )
        return self._handle_response(request) or []

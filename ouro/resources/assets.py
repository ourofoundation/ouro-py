import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource

log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Assets"]


class Assets(SyncAPIResource):
    def __init__(self, client):
        super().__init__(client)

    def search(
        self,
        query: str,
        **kwargs,
    ) -> List[dict]:
        """
        Search for assets
        """

        request = self.client.get(
            f"/search/assets",
            params={
                "query": query,
                **kwargs,
            },
        )
        request.raise_for_status()
        response = request.json()
        if response.get("error", None):
            raise Exception(response["error"])
        return response.get("data", [])

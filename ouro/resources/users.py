import logging
from typing import List, Optional

from ouro._resource import SyncAPIResource

# from ouro.models import User


log: logging.Logger = logging.getLogger(__name__)


__all__ = ["Users"]


class Users(SyncAPIResource):
    def __init__(self, client):
        super().__init__(client)

    def search(
        self,
        query: str,
        **kwargs,
    ) -> List[dict]:
        """
        Search for users
        """

        request = self.client.get(
            f"/users/search",
            params={
                "query": query,
                **kwargs,
            },
        )
        request.raise_for_status()
        response = request.json()
        if response.get("error", None):
            raise Exception(response["error"])
        # return [User(**user) for user in response["data"]]
        return response.get("data", [])

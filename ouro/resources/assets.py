import logging
from typing import List, Union

from ouro._resource import SyncAPIResource
from ouro.models import Asset, Comment, Dataset, File, Post, Service, Route

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

    def retrieve(
        self,
        id: str,
    ) -> Union[Post, Comment, File, Dataset, Service, Route, Asset]:
        """
        Retrieve any asset by its ID, regardless of asset type.
        This method automatically determines the asset type and routes to the
        appropriate resource's retrieve method.

        Args:
            id: The asset ID to retrieve

        Returns:
            The appropriate asset model instance (Post, Comment, File, Dataset, Service, Route, or Asset)

        Raises:
            Exception: If the asset is not found or if there's an error retrieving it
        """
        try:
            # First, call the backend route to get the asset_type
            request = self.client.get(f"/assets/{id}/type")

            # Handle 404 errors gracefully
            if request.status_code == 404:
                raise Exception(f"Asset with id {id} not found")

            request.raise_for_status()
            response = request.json()

            if response.get("error"):
                raise Exception(response["error"])

            asset_type = response.get("data", {}).get("asset_type")

            if not asset_type:
                raise Exception(f"Asset with id {id} has no asset_type")

            # Route to the appropriate resource's retrieve method
            if asset_type == "post":
                return self.ouro.posts.retrieve(id)
            elif asset_type == "comment":
                return self.ouro.comments.retrieve(id)
            elif asset_type == "file":
                return self.ouro.files.retrieve(id)
            elif asset_type == "dataset":
                return self.ouro.datasets.retrieve(id)
            elif asset_type == "service":
                return self.ouro.services.retrieve(id)
            elif asset_type == "route":
                return self.ouro.routes.retrieve(id)
            else:
                # For unknown asset types (e.g., conversation, quest, blueprint, replication),
                # we need to fetch basic asset info. Since we don't have a generic endpoint,
                # we'll try to get it from the search endpoint or return a basic Asset
                log.warning(
                    f"Unknown asset type: {asset_type}. Cannot retrieve full asset details via API."
                )
                # For now, raise an error suggesting to use the specific resource if available
                raise Exception(
                    f"Asset type '{asset_type}' is not supported by the unified retrieve method. "
                    f"Please use the specific resource's retrieve method if available."
                )
        except Exception as e:
            # Re-raise the exception with more context if needed
            error_str = str(e).lower()
            if "not found" in error_str or "404" in error_str or "no rows" in error_str:
                raise Exception(f"Asset with id {id} not found") from e
            raise

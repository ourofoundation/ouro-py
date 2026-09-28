from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional, Union

from ouro._resource import SyncAPIResource, _coerce_description, _strip_none
from ouro.models import Asset, JoinRequest, Page, Team, TeamBan, TeamUnreads

if TYPE_CHECKING:
    from .content import Content

log: logging.Logger = logging.getLogger(__name__)

__all__ = ["Teams"]


class Teams(SyncAPIResource):
    def create(
        self,
        name: str,
        org_id: str,
        description: Optional[Union[str, dict, "Content"]] = None,
        visibility: Optional[str] = None,
        default_role: Optional[str] = None,
        actor_type_policy: Optional[str] = None,
        source_policy: Optional[str] = None,
        join_policy: Optional[str] = None,
        **kwargs,
    ) -> Team:
        """Create a team in an organization.

        ``name`` must be a slug (lowercase letters, numbers, and single dashes)
        and unique within the organization; otherwise the request fails with
        ``BadRequestError`` or ``ConflictError``.
        """
        team = _strip_none({
            "name": name,
            "org_id": org_id,
            "description": _coerce_description(description),
            "visibility": visibility,
            "default_role": default_role,
            "actor_type_policy": actor_type_policy,
            "source_policy": source_policy,
            "join_policy": join_policy,
            **kwargs,
        })
        request = self.client.post("/teams/create", json={"team": team})
        return self._parse(Team, self._handle_response(request))

    def update(
        self,
        id: str,
        name: Optional[str] = None,
        description: Optional[Union[str, dict, "Content"]] = None,
        visibility: Optional[str] = None,
        default_role: Optional[str] = None,
        actor_type_policy: Optional[str] = None,
        source_policy: Optional[str] = None,
        join_policy: Optional[str] = None,
        **kwargs,
    ) -> Team:
        """Update a team."""
        team = _strip_none({
            "id": id,
            "name": name,
            "description": _coerce_description(description),
            "visibility": visibility,
            "default_role": default_role,
            "actor_type_policy": actor_type_policy,
            "source_policy": source_policy,
            "join_policy": join_policy,
            **kwargs,
        })
        request = self.client.put(f"/teams/{id}", json={"team": team})
        return self._parse(Team, self._handle_response(request))

    def delete(self, id: str) -> None:
        """Delete a team (team or organization admins only).

        The team's assets move to the organization's default team; its
        memberships, bans, and join requests are removed. An organization's
        default team cannot be deleted.
        """
        request = self.client.delete(f"/teams/{id}")
        self._handle_response(request)

    def list(
        self,
        org_id: Optional[str] = None,
        joined: Optional[bool] = None,
        public_only: Optional[bool] = None,
    ) -> List[Team]:
        """List teams with optional filters.

        Args:
            org_id: Filter by organization ID.
            joined: If True, only return teams the user has joined.
            public_only: If True, only return public teams.
        """
        params = {}
        if org_id is not None:
            params["org_id"] = org_id
        if joined is not None:
            params["joined"] = str(joined).lower()
        if public_only is not None:
            params["public_only"] = str(public_only).lower()

        request = self.client.get("/teams", params=params)
        return self._parse_list(Team, self._handle_response(request))

    def retrieve(self, id: str, *, include_members: bool = False) -> Team:
        """Retrieve a team by ID with organization policy fields and metrics.

        Set ``include_members=True`` to also fetch the member roster.
        """
        params = {"include_members": "true"} if include_members else None
        request = self.client.get(f"/teams/{id}", params=params)
        return self._parse(Team, self._handle_response(request))

    def join(self, id: str) -> Team:
        """Join a team.

        On teams with ``join_policy="request"`` this submits a join request
        instead of adding membership immediately; check ``team.join_status``
        and ``team.user_join_request``. Invite-only teams and bans return an
        error.
        """
        request = self.client.post(f"/teams/{id}/join", json={})
        return self._parse(Team, self._handle_response(request))

    def list_join_requests(self, id: str) -> List[JoinRequest]:
        """List pending join requests. Admins see all; others see their own."""
        request = self.client.get(f"/teams/{id}/join-requests")
        return self._parse_list(JoinRequest, self._handle_response(request))

    def approve_join_request(self, id: str, request_id: str) -> None:
        """Approve a pending join request (team admin)."""
        request = self.client.post(
            f"/teams/{id}/join-requests/{request_id}/approve", json={}
        )
        self._handle_response(request)

    def reject_join_request(self, id: str, request_id: str) -> None:
        """Reject a pending join request (team admin)."""
        request = self.client.post(
            f"/teams/{id}/join-requests/{request_id}/reject", json={}
        )
        self._handle_response(request)

    def ban_member(
        self,
        id: str,
        user_id: str,
        reason: Optional[str] = None,
        remove_contributions: bool = False,
    ) -> None:
        """Remove a member and prevent them from rejoining (team admin).

        Pass ``remove_contributions=True`` to move their assets to the
        organization's #all team and delete their quest entries on this team.
        """
        request = self.client.post(
            f"/teams/{id}/bans",
            json=_strip_none({
                "user_id": user_id,
                "reason": reason,
                "remove_contributions": remove_contributions or None,
            }),
        )
        self._handle_response(request)

    def unban_member(self, id: str, user_id: str) -> None:
        """Lift a team ban (team admin). Does not restore membership."""
        request = self.client.delete(f"/teams/{id}/bans/{user_id}")
        self._handle_response(request)

    def list_bans(self, id: str) -> List[TeamBan]:
        """List users banned from a team (team admin)."""
        request = self.client.get(f"/teams/{id}/bans")
        return self._parse_list(TeamBan, self._handle_response(request))

    def leave(self, id: str) -> None:
        """Leave a team as the authenticated user."""
        request = self.client.get(f"/teams/{id}/leave")
        self._handle_response(request)

    def activity(
        self,
        id: str,
        offset: int = 0,
        limit: int = 20,
        asset_type: Optional[str] = None,
    ) -> Page[Asset]:
        """Get a page of a team's activity feed, newest first.

        Args:
            id: Team ID.
            offset: Zero-based pagination offset.
            limit: Number of items per page.
            asset_type: Filter by asset type (e.g. "post", "dataset", "file").
        """
        params = {"offset": offset, "limit": limit}
        if asset_type is not None:
            params["assetType"] = asset_type

        request = self.client.get(f"/teams/{id}/activity", params=params)
        return self._page(Page[Asset], self._handle_response(request, raw=True))

    def unreads(self, id: str, org_id: Optional[str] = None) -> int:
        """Get unread post count for a single team.

        Args:
            id: Team ID.
            org_id: Organization ID containing the team. If omitted, this method
                fetches the team to resolve its org automatically.
        """
        resolved_org_id = org_id or self.retrieve(id).org_id
        if not resolved_org_id:
            raise ValueError(f"Unable to resolve org_id for team '{id}'")

        request = self.client.get(
            "/teams/unreads",
            params={"org_id": str(resolved_org_id), "view_mode": "count"},
        )
        data = self._handle_response(request) or {}
        return int((data.get("unreads") or {}).get(id, 0))

    def unread_preview(self, id: str, offset: int = 0, limit: int = 20) -> TeamUnreads:
        """Get a page of unread assets in a single team.

        Args:
            id: Team ID.
            offset: Zero-based pagination offset.
            limit: Number of unread items to return.
        """
        params = {
            "view_mode": "preview",
            "team_id": id,
            "offset": offset,
            "limit": limit,
        }
        request = self.client.get("/teams/unreads", params=params)
        body = self._handle_response(request, raw=True)
        preview = body.get("data") or {}
        return self._page(
            TeamUnreads,
            {"data": preview.get("results"), "pagination": body.get("pagination")},
            team_id=preview.get("team_id", id),
            unread_count=preview.get("unread_count", 0),
        )

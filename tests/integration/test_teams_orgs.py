from __future__ import annotations

import pytest

from ouro import BadRequestError, ConflictError, NotFoundError, OuroError
from ouro.models import Organization, Team


@pytest.fixture(scope="module")
def org(ouro) -> Organization:
    return next(o for o in ouro.organizations.list() if o.membership and o.membership.role == "admin")


@pytest.fixture(scope="module")
def team(ouro, org, run_id):
    created = ouro.teams.create(
        name=f"sdk-it-{run_id}",
        org_id=str(org.id),
        description="Integration test team",
        visibility="public",
    )
    yield created
    ouro.teams.delete(str(created.id))


def test_organizations(ouro, org):
    assert isinstance(org, Organization)
    fetched = ouro.organizations.retrieve(str(org.id))
    assert fetched.id == org.id
    assert isinstance(ouro.organizations.get_context(), Organization)
    assert isinstance(ouro.organizations.list_discoverable(), list)


def test_create_and_retrieve_team(ouro, team, org):
    assert isinstance(team, Team)
    assert team.org_id == org.id
    fetched = ouro.teams.retrieve(str(team.id), include_members=True)
    assert fetched.name == team.name
    assert fetched.members and any(str(m.user_id) == str(ouro.user.id) for m in fetched.members)


def test_team_policies_resolve(ouro, team, org):
    fetched = ouro.teams.retrieve(str(team.id))
    organization = ouro.organizations.retrieve(str(org.id))
    assert fetched.source_policy or organization.source_policy
    assert fetched.actor_type_policy or organization.actor_type_policy


def test_update_team(ouro, team):
    updated = ouro.teams.update(str(team.id), description="Updated team description")
    assert "Updated team description" in updated.description["text"]


def test_team_names_must_be_slugs(ouro, org):
    with pytest.raises(BadRequestError):
        ouro.teams.create(name="Not A Slug!", org_id=str(org.id))


def test_duplicate_team_name_conflicts(ouro, team, org):
    with pytest.raises(ConflictError):
        ouro.teams.create(name=team.name, org_id=str(org.id))


def test_list_teams(ouro, team, org):
    teams = ouro.teams.list(org_id=str(org.id), joined=True)
    assert any(t.id == team.id for t in teams)


def test_publish_into_team_and_read_activity(ouro, track, team, org):
    post = track.add(
        ouro.posts.create(
            name=track.name("team-post"),
            content_markdown="Hello team",
            visibility="public",
            org_id=str(org.id),
            team_id=str(team.id),
        )
    )
    assert post.team_id == team.id
    activity = ouro.teams.activity(str(team.id), limit=10)
    assert any(item.get("id") == str(post.id) for item in activity["data"])
    in_team = ouro.posts.list(team_id=str(team.id), org_id=str(org.id), scope="all", sort="recent")
    assert any(p.id == post.id for p in in_team)


def test_unreads(ouro, team):
    assert isinstance(ouro.teams.unreads(str(team.id)), int)
    preview = ouro.teams.unread_preview(str(team.id), limit=5)
    assert "pagination" in preview


def test_join_leave_ban(ouro, other, team):
    other.teams.join(str(team.id))
    members = ouro.teams.retrieve(str(team.id), include_members=True).members
    assert any(str(m.user_id) == str(other.user.id) for m in members)

    other.teams.leave(str(team.id))
    members = ouro.teams.retrieve(str(team.id), include_members=True).members
    assert all(str(m.user_id) != str(other.user.id) for m in members)

    other.teams.join(str(team.id))
    ouro.teams.ban_member(str(team.id), str(other.user.id), reason="integration test")
    assert any(str(b.get("user_id")) == str(other.user.id) for b in ouro.teams.list_bans(str(team.id)))
    with pytest.raises(OuroError):
        other.teams.join(str(team.id))
    ouro.teams.unban_member(str(team.id), str(other.user.id))
    assert all(str(b.get("user_id")) != str(other.user.id) for b in ouro.teams.list_bans(str(team.id)))


def test_join_requests(ouro, other, org, run_id):
    gated = ouro.teams.create(name=f"sdk-it-{run_id}-gated", org_id=str(org.id), join_policy="request")
    try:
        other.teams.join(str(gated.id))
        [pending] = ouro.teams.list_join_requests(str(gated.id))
        ouro.teams.approve_join_request(str(gated.id), str(pending["id"]))
        members = ouro.teams.retrieve(str(gated.id), include_members=True).members
        assert any(str(m.user_id) == str(other.user.id) for m in members)
    finally:
        ouro.teams.delete(str(gated.id))


def test_delete_team_keeps_its_assets(ouro, other, track, org, run_id):
    doomed = ouro.teams.create(name=f"sdk-it-{run_id}-doomed", org_id=str(org.id), visibility="public")
    post = track.add(
        ouro.posts.create(
            name=track.name("orphan"),
            content_markdown="outlives its team",
            visibility="public",
            org_id=str(org.id),
            team_id=str(doomed.id),
        )
    )
    other.teams.join(str(doomed.id))
    with pytest.raises(OuroError):
        other.teams.delete(str(doomed.id))

    ouro.teams.delete(str(doomed.id))
    with pytest.raises(NotFoundError):
        ouro.teams.retrieve(str(doomed.id))
    moved = ouro.posts.retrieve(str(post.id))
    assert moved.team_id and moved.team_id != doomed.id

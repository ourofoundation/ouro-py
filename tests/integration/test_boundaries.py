"""Where assets live and who can see them: the team boundary, moves between
teams and organizations, and the context an API key runs in."""

from __future__ import annotations

import httpx
import pytest

from ouro import APIStatusError, NotFoundError, Ouro, OuroError, PermissionDeniedError

from conftest import BASE_URL, PRIMARY_KEY

GLOBAL_ORG_ID = "00000000-0000-0000-0000-000000000000"
CSV = b"a,b\n1,2\n3,4\n"


@pytest.fixture(scope="module")
def org(ouro):
    """An organization the primary user administers and the second user is outside."""
    return next(
        o
        for o in ouro.organizations.list()
        if o.membership and o.membership.role == "admin" and str(o.id) != GLOBAL_ORG_ID
    )


@pytest.fixture(scope="module")
def internal_team(ouro, org, run_id):
    team = ouro.teams.create(
        name=f"sdk-it-{run_id}-internal", org_id=str(org.id), visibility="organization"
    )
    yield team
    ouro.teams.delete(str(team.id))


@pytest.fixture(scope="module")
def public_team(ouro, org, run_id):
    team = ouro.teams.create(
        name=f"sdk-it-{run_id}-public", org_id=str(org.id), visibility="public"
    )
    yield team
    ouro.teams.delete(str(team.id))


@pytest.fixture(scope="module")
def pinned(org) -> Ouro:
    return Ouro(
        api_key=PRIMARY_KEY,
        organization=str(org.id),
        base_url=BASE_URL,
        client="ouro-py-integration",
    )


def _create(client: Ouro, kind: str, name: str, **kwargs):
    if kind == "post":
        return client.posts.create(name=name, content_markdown="Boundary test", **kwargs)
    if kind == "file":
        kwargs.setdefault("visibility", None)
        return client.files.create(
            name=name, file_content=b"boundary test\n", file_name="boundary.txt", **kwargs
        )
    kwargs.setdefault("visibility", None)
    return client.datasets.create(name=name, data=[{"a": 1, "b": 2}], **kwargs)


def _resource(client: Ouro, kind: str):
    return {"post": client.posts, "file": client.files, "dataset": client.datasets}[kind]


KINDS = ["post", "file", "dataset"]


# --- the team is the visibility boundary -------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_visibility_follows_an_internal_team(pinned, other, track, org, internal_team, kind):
    asset = track.add(
        _create(pinned, kind, track.name(f"internal-{kind}"), team_id=str(internal_team.id))
    )
    assert str(asset.org_id) == str(org.id)
    assert str(asset.team_id) == str(internal_team.id)
    assert asset.visibility == "organization"
    # Someone outside the organization can't read it, or find it.
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        _resource(other, kind).retrieve(str(asset.id))
    hits = other.assets.search(track.name(f"internal-{kind}"))
    assert all(str(hit.id) != str(asset.id) for hit in hits)


@pytest.mark.parametrize("kind", KINDS)
def test_visibility_follows_a_public_team(pinned, other, track, public_team, kind):
    asset = track.add(
        _create(pinned, kind, track.name(f"public-{kind}"), team_id=str(public_team.id))
    )
    assert asset.visibility == "public"
    assert str(_resource(other, kind).retrieve(str(asset.id)).id) == str(asset.id)


@pytest.mark.parametrize("kind", KINDS)
def test_public_asset_is_refused_in_an_internal_team(pinned, track, internal_team, kind):
    with pytest.raises(APIStatusError) as refused:
        track.add(
            _create(
                pinned,
                kind,
                track.name(f"refused-{kind}"),
                team_id=str(internal_team.id),
                visibility="public",
            )
        )
    assert 400 <= refused.value.status_code < 500


def test_monetized_asset_is_refused_in_an_internal_team(pinned, track, internal_team):
    with pytest.raises(APIStatusError) as refused:
        track.add(
            pinned.posts.create(
                name=track.name("refused-paid"),
                content_markdown="Paid",
                team_id=str(internal_team.id),
                visibility="monetized",
                monetization="pay-to-unlock",
                price_usd=1.0,
            )
        )
    assert 400 <= refused.value.status_code < 500


@pytest.mark.parametrize("kind", KINDS)
def test_internal_asset_cannot_be_made_public_in_place(pinned, track, internal_team, kind):
    asset = track.add(
        _create(pinned, kind, track.name(f"stay-{kind}"), team_id=str(internal_team.id))
    )
    with pytest.raises(APIStatusError) as refused:
        _resource(pinned, kind).update(str(asset.id), visibility="public")
    assert 400 <= refused.value.status_code < 500
    assert _resource(pinned, kind).retrieve(str(asset.id)).visibility == "organization"


@pytest.mark.parametrize("kind", KINDS)
def test_publishing_is_moving_to_a_public_team(
    pinned, other, track, internal_team, public_team, kind
):
    asset = track.add(
        _create(pinned, kind, track.name(f"publish-{kind}"), team_id=str(internal_team.id))
    )
    moved = _resource(pinned, kind).update(
        str(asset.id), team_id=str(public_team.id), visibility="public"
    )
    assert str(moved.team_id) == str(public_team.id)
    assert moved.visibility == "public"
    assert str(_resource(other, kind).retrieve(str(asset.id)).id) == str(asset.id)

    # And back: an internal team takes it out of public view again.
    back = _resource(pinned, kind).update(
        str(asset.id), team_id=str(internal_team.id), visibility="organization"
    )
    assert back.visibility == "organization"
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        _resource(other, kind).retrieve(str(asset.id))


def test_moving_to_an_internal_team_keeps_nothing_public(pinned, track, internal_team, public_team):
    post = track.add(
        pinned.posts.create(
            name=track.name("move-in"), content_markdown="x", team_id=str(public_team.id)
        )
    )
    with pytest.raises(APIStatusError) as refused:
        pinned.posts.update(str(post.id), team_id=str(internal_team.id), visibility="public")
    assert 400 <= refused.value.status_code < 500


def test_outsider_cannot_create_in_the_organization(other, track, org, internal_team, public_team):
    for team in (internal_team, public_team):
        with pytest.raises(APIStatusError) as refused:
            asset = other.posts.create(
                name=track.name("outsider"),
                content_markdown="x",
                org_id=str(org.id),
                team_id=str(team.id),
            )
            track.add(asset)
        assert 400 <= refused.value.status_code < 500


def test_private_stays_private_in_any_team(pinned, other, track, public_team):
    post = track.add(
        pinned.posts.create(
            name=track.name("private"),
            content_markdown="x",
            team_id=str(public_team.id),
            visibility="private",
        )
    )
    assert post.visibility == "private"
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        other.posts.retrieve(str(post.id))


# --- moving between organizations --------------------------------------------


def test_move_between_personal_context_and_organization(ouro, other, track, org, internal_team):
    post = track.add(
        ouro.posts.create(name=track.name("org-move"), content_markdown="x", visibility="public")
    )
    assert str(post.org_id) == GLOBAL_ORG_ID
    moved = ouro.posts.update(
        str(post.id),
        org_id=str(org.id),
        team_id=str(internal_team.id),
        visibility="organization",
    )
    assert str(moved.org_id) == str(org.id)
    assert moved.visibility == "organization"
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        other.posts.retrieve(str(post.id))


def test_pinned_client_refuses_other_organizations(pinned, track):
    with pytest.raises(OuroError):
        track.add(
            pinned.posts.create(
                name=track.name("wrong-org"), content_markdown="x", org_id=GLOBAL_ORG_ID
            )
        )


# --- the context an API key runs in ------------------------------------------


def _token(api_key: str) -> str:
    response = httpx.post(f"{BASE_URL}/users/get-token", json={"pat": api_key})
    response.raise_for_status()
    return response.json()["access_token"]


def _get(client: Ouro, path: str, org: str | None = None) -> httpx.Response:
    headers = {"Authorization": client.access_token, "X-Ouro-Client": "ouro-py-integration"}
    if org is not None:
        headers["X-Ouro-Org"] = org
    return httpx.get(f"{BASE_URL}{path}", headers=headers)


def test_unpinned_client_runs_in_the_personal_context(ouro):
    assert str(ouro.organizations.get_context().id) == GLOBAL_ORG_ID


def test_pinned_client_runs_in_its_organization(pinned, org):
    assert str(pinned.organizations.get_context().id) == str(org.id)


def test_org_header_must_be_an_organization_id(ouro):
    response = _get(ouro, "/organizations/context", org="not-a-uuid")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_org_context"


def test_org_header_requires_membership(other, org):
    response = _get(other, "/organizations/context", org=str(org.id))
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "org_context_forbidden"


def test_personal_key_cannot_name_an_organization(personal_bound, org):
    response = _get(personal_bound, "/organizations/context", org=str(org.id))
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "api_key_org_mismatch"


def test_personal_key_cannot_be_pinned_to_an_organization(org):
    from conftest import PERSONAL_BOUND_KEY

    if not PERSONAL_BOUND_KEY:
        pytest.skip("set OURO_TEST_API_KEY_PERSONAL for bound-key tests")
    with pytest.raises(OuroError):
        Ouro(api_key=PERSONAL_BOUND_KEY, organization=str(org.id), base_url=BASE_URL)


def test_personal_key_sees_only_public_work_of_an_organization(
    pinned, personal_bound, track, internal_team, public_team
):
    internal = track.add(
        pinned.posts.create(
            name=track.name("key-internal"), content_markdown="x", team_id=str(internal_team.id)
        )
    )
    public = track.add(
        pinned.posts.create(
            name=track.name("key-public"), content_markdown="x", team_id=str(public_team.id)
        )
    )
    assert str(personal_bound.organizations.get_context().id) == GLOBAL_ORG_ID
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        personal_bound.posts.retrieve(str(internal.id))
    assert str(personal_bound.posts.retrieve(str(public.id)).id) == str(public.id)
    # It can't change the organization's work either, even public work it owns.
    with pytest.raises(APIStatusError) as refused:
        personal_bound.posts.update(str(public.id), name=track.name("key-public-renamed"))
    assert 400 <= refused.value.status_code < 500


def test_personal_key_cannot_create_in_an_organization(personal_bound, track, org, public_team):
    with pytest.raises(PermissionDeniedError) as refused:
        track.add(
            personal_bound.posts.create(
                name=track.name("key-create"),
                content_markdown="x",
                org_id=str(org.id),
                team_id=str(public_team.id),
            )
        )
    assert "bound" in str(refused.value)


def test_org_key_creates_in_its_organization(org_bound, ouro, track):
    context = org_bound.organizations.get_context()
    assert str(context.id) != GLOBAL_ORG_ID
    assert org_bound.organization == str(context.id)
    post = track.add(org_bound.posts.create(name=track.name("org-key"), content_markdown="x"))
    assert str(post.org_id) == str(context.id)


def test_org_key_is_shut_out_of_the_personal_context(org_bound, ouro, track):
    private = track.add(
        ouro.posts.create(name=track.name("personal-private"), content_markdown="x", visibility="private")
    )
    public = track.add(
        ouro.posts.create(name=track.name("personal-public"), content_markdown="x", visibility="public")
    )
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        org_bound.posts.retrieve(str(private.id))
    assert str(org_bound.posts.retrieve(str(public.id)).id) == str(public.id)
    with pytest.raises(APIStatusError) as refused:
        org_bound.posts.update(str(public.id), name=track.name("personal-renamed"))
    assert 400 <= refused.value.status_code < 500


def test_org_key_cannot_move_work_out(org_bound, track):
    post = track.add(org_bound.posts.create(name=track.name("org-key-move"), content_markdown="x"))
    with pytest.raises((OuroError, APIStatusError)):
        org_bound.posts.update(str(post.id), org_id=GLOBAL_ORG_ID, visibility="public")
    assert str(org_bound.posts.retrieve(str(post.id)).org_id) != GLOBAL_ORG_ID

from __future__ import annotations

import pytest

from ouro import NotFoundError, PermissionDeniedError
from ouro.models import Dataset, File, Post


@pytest.fixture(scope="module")
def private_dataset(ouro, track):
    return track.add(
        ouro.datasets.create(
            name=track.name("asset-private"),
            visibility="private",
            data=[{"x": 1}],
            license_id="CC-BY-4.0",
            attribution={
                "originality": "derivative",
                "github_url": "https://github.com/example/project",
                "relation_type": "IsDerivedFrom",
            },
        )
    )


def test_retrieve_dispatches_by_type(ouro, track, private_dataset):
    post = track.add(ouro.posts.create(name=track.name("dispatch"), content_markdown="hi", visibility="private"))
    file = track.add(
        ouro.files.create(name=track.name("dispatch"), visibility="private", file_content=b"x", file_name="x.txt")
    )
    assert isinstance(ouro.assets.retrieve(str(private_dataset.id)), Dataset)
    assert isinstance(ouro.assets.retrieve(str(post.id)), Post)
    assert isinstance(ouro.assets.retrieve(str(file.id)), File)


def test_license_and_attribution_round_trip(ouro, private_dataset):
    fetched = ouro.datasets.retrieve(str(private_dataset.id))
    assert fetched.license_id == "CC-BY-4.0"
    assert fetched.attribution.originality == "derivative"
    assert fetched.attribution.github_url == "https://github.com/example/project"
    assert fetched.attribution.relation_type == "IsDerivedFrom"


def test_partial_attribution_update_keeps_originality(ouro, private_dataset):
    updated = ouro.datasets.update(
        str(private_dataset.id), attribution={"paper_url": "https://arxiv.org/abs/0000.00000"}
    )
    assert updated.attribution.paper_url == "https://arxiv.org/abs/0000.00000"
    assert updated.attribution.originality == "derivative"


def test_private_asset_is_invisible_until_shared(ouro, other, track):
    dataset = track.add(
        ouro.datasets.create(name=track.name("share-me"), visibility="private", data=[{"x": 1}])
    )
    with pytest.raises((NotFoundError, PermissionDeniedError)):
        other.datasets.retrieve(str(dataset.id))

    ouro.assets.share(str(dataset.id), other.user.id, role="read")
    assert other.datasets.retrieve(str(dataset.id)).id == dataset.id
    assert other.datasets.query(str(dataset.id))["x"].tolist() == [1]

    with pytest.raises((PermissionDeniedError, NotFoundError)):
        other.datasets.update(str(dataset.id), name="hijacked")
    with pytest.raises((PermissionDeniedError, NotFoundError)):
        other.datasets.delete(str(dataset.id))

    permissions = ouro.datasets.permissions(str(dataset.id))
    assert any(str(p.get("user_id") or p.get("user", {}).get("user_id")) == str(other.user.id) for p in permissions)


def test_write_share_allows_update(ouro, other, track):
    post = track.add(ouro.posts.create(name=track.name("writable"), content_markdown="v1", visibility="private"))
    ouro.assets.share(str(post.id), other.user.id, role="write")
    content = other.posts.Content()
    content.from_markdown("v2 from collaborator")
    other.posts.update(str(post.id), content=content)
    assert "collaborator" in ouro.posts.retrieve(str(post.id)).content.text


def test_search_modes(ouro, private_dataset):
    browse = ouro.assets.search(asset_type="dataset", scope="personal", sort="recent", limit=100)
    assert any(a["id"] == str(private_dataset.id) for a in browse)

    by_id = ouro.assets.search(str(private_dataset.id))
    assert by_id and by_id[0]["id"] == str(private_dataset.id)

    page = ouro.assets.search(asset_type=["dataset", "post"], scope="personal", limit=2, with_pagination=True)
    assert len(page["data"]) <= 2
    assert {"hasMore"} <= set(page["pagination"])


def test_search_treats_blank_filters_as_unset(ouro):
    assert isinstance(ouro.assets.search(asset_type="dataset", org_id="", team_id="null", limit=5), list)


def test_search_paginates_past_server_cap(ouro):
    results = ouro.assets.search(scope="all", sort="recent", limit=250)
    ids = [r["id"] for r in results]
    assert len(ids) == len(set(ids))
    assert len(ids) <= 250


def test_engagement_endpoints(ouro, private_dataset):
    asset_id = str(private_dataset.id)
    assert isinstance(ouro.assets.counts(asset_id), dict)
    assert isinstance(ouro.assets.connections(asset_id), list)
    assert isinstance(ouro.assets.tags(asset_id), list)
    assert isinstance(ouro.assets.children(asset_id), list)
    assert isinstance(ouro.assets.impact(asset_id), dict)
    actions = ouro.assets.actions(asset_id)
    assert actions["created_by"] is None and actions["as_input"] == []


def test_delete_via_assets_and_dry_run(ouro, track):
    post = track.add(ouro.posts.create(name=track.name("to-delete"), content_markdown="bye", visibility="private"))
    preview = ouro.assets.delete(str(post.id), dry_run=True)
    assert preview["dry_run"] is True and preview["asset_type"] == "post"
    ouro.assets.retrieve(str(post.id))

    ouro.assets.delete(str(post.id))
    with pytest.raises(NotFoundError):
        ouro.assets.retrieve(str(post.id))

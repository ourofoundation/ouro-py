"""Selling an asset in dollars and sats: the prices round-trip, a stranger
sees the listing and not the content, and making it free opens it again."""

from __future__ import annotations

import pytest

from ouro import APIStatusError, PermissionDeniedError

BODY = "The paid part of the post."


@pytest.fixture(scope="module")
def paid_post(ouro, track):
    return track.add(
        ouro.posts.create(
            name=track.name("paid-post"),
            content_markdown=BODY,
            visibility="monetized",
            monetization="pay-to-unlock",
            price_usd=1.5,
            price_sats=700,
        )
    )


def test_both_prices_round_trip(ouro, paid_post):
    fetched = ouro.posts.retrieve(str(paid_post.id))
    assert fetched.visibility == "monetized"
    assert fetched.monetization == "pay-to-unlock"
    assert fetched.price_usd == 1.5
    assert fetched.price_sats == 700


def test_stranger_sees_the_listing_but_not_the_content(other, paid_post):
    listing = other.posts.retrieve(str(paid_post.id))
    assert listing.price_usd == 1.5
    assert listing.price_sats == 700
    assert not (listing.content and listing.content.text)


def test_search_finds_a_paid_asset(other, paid_post):
    hits = other.assets.search(paid_post.name)
    assert any(str(hit.id) == str(paid_post.id) for hit in hits)


def test_download_link_is_refused_before_purchase(other, paid_post):
    with pytest.raises(APIStatusError) as refused:
        other.assets.create_download_url(str(paid_post.id))
    assert 400 <= refused.value.status_code < 500


def test_prices_update_independently(ouro, paid_post):
    updated = ouro.posts.update(str(paid_post.id), price_usd=2.25)
    assert updated.price_usd == 2.25
    assert updated.price_sats == 700
    updated = ouro.posts.update(str(paid_post.id), price_sats=900)
    assert updated.price_usd == 2.25
    assert updated.price_sats == 900


def test_negative_price_is_refused(ouro, paid_post):
    with pytest.raises(APIStatusError) as refused:
        ouro.posts.update(str(paid_post.id), price_usd=-1)
    assert 400 <= refused.value.status_code < 500


@pytest.mark.parametrize("kind", ["file", "dataset"])
def test_other_asset_types_take_both_prices(ouro, other, track, kind):
    pricing = dict(visibility="monetized", monetization="pay-to-unlock", price_usd=3.0, price_sats=1500)
    if kind == "file":
        asset = ouro.files.create(
            name=track.name("paid-file"), file_content=b"paid bytes\n", file_name="paid.txt", **pricing
        )
        seen = other.files.retrieve(str(track.add(asset).id), include_data=False)
    else:
        asset = ouro.datasets.create(name=track.name("paid-dataset"), data=[{"a": 1}], **pricing)
        seen = other.datasets.retrieve(str(track.add(asset).id))
        # The schema is part of the listing; the rows are what is sold.
        assert [c.name for c in other.datasets.schema(str(asset.id)) if c.name == "a"]
        with pytest.raises(PermissionDeniedError, match="Unlock"):
            other.datasets.list_rows(str(asset.id), limit=5)
        with pytest.raises(PermissionDeniedError, match="Unlock"):
            other.datasets.query(str(asset.id), sql="select * from {{table}}")
    assert (asset.price_usd, asset.price_sats) == (3.0, 1500)
    assert (seen.price_usd, seen.price_sats) == (3.0, 1500)


def test_making_it_free_opens_it(ouro, other, paid_post):
    freed = ouro.posts.update(str(paid_post.id), visibility="public", monetization="none")
    assert freed.visibility == "public"
    assert BODY in other.posts.retrieve(str(paid_post.id)).content.text

from __future__ import annotations

import pytest

from ouro.models import Page
from ouro.models._base import OuroModel


class _Item(OuroModel):
    name: str


def test_page_behaves_like_its_data():
    page = Page[_Item].model_validate(
        {"data": [{"name": "a"}, {"name": "b"}], "hasMore": True, "total": 5}
    )

    assert [item.name for item in page] == ["a", "b"]
    assert len(page) == 2
    assert page[1].name == "b"
    assert page.has_more is True
    assert page.total == 5


def test_empty_page_is_falsy():
    assert not Page[_Item]()


def test_unknown_fields_are_kept_as_attributes():
    item = _Item.model_validate({"name": "a", "added_later": 1})

    assert item.added_later == 1


def test_client_binds_through_nested_models():
    ouro = object()
    page = Page[_Item].model_validate(
        {"data": [{"name": "a"}]}, context={"ouro": ouro}
    )

    assert page._ouro is ouro
    assert page[0]._require_client() is ouro


def test_unbound_model_raises_on_client_use():
    with pytest.raises(RuntimeError, match="not connected"):
        _Item(name="a")._require_client()

from __future__ import annotations

import uuid

import pytest

import ouro as ouro_pkg
from ouro import (
    AuthenticationError,
    BadRequestError,
    NotFoundError,
    Ouro,
    OuroError,
)

@pytest.fixture
def base_url(ouro):
    return ouro.base_url


def test_public_exports():
    for name in (
        "Ouro",
        "OuroError",
        "APIStatusError",
        "NotFoundError",
        "PermissionDeniedError",
        "RouteExecutionError",
        "__version__",
    ):
        assert getattr(ouro_pkg, name) is not None


def test_authenticates_and_exposes_current_user(ouro, me):
    assert ouro.access_token
    assert str(ouro.user.id) == str(me.user_id)
    assert me.username


def test_invalid_api_key_raises_authentication_error(base_url):
    with pytest.raises(AuthenticationError):
        Ouro(api_key="not-a-real-key", base_url=base_url)


def test_missing_api_key_raises(monkeypatch, base_url):
    monkeypatch.delenv("OURO_API_KEY", raising=False)
    with pytest.raises(OuroError):
        Ouro(base_url=base_url)


def test_refresh_session_keeps_client_usable(ouro):
    before = ouro.access_token
    ouro.refresh_session()
    assert ouro.access_token
    assert ouro.users.me().user_id
    assert isinstance(before, str)


def test_all_errors_share_base_class(ouro):
    missing = str(uuid.uuid4())
    with pytest.raises(NotFoundError) as info:
        ouro.assets.retrieve(missing)
    assert isinstance(info.value, OuroError)
    assert info.value.status_code == 404


@pytest.mark.parametrize(
    "call",
    [
        lambda o, i: o.datasets.retrieve(i),
        lambda o, i: o.files.retrieve(i),
        lambda o, i: o.posts.retrieve(i),
        lambda o, i: o.quests.retrieve(i),
        lambda o, i: o.services.retrieve(i),
        lambda o, i: o.routes.retrieve(i),
    ],
    ids=["datasets", "files", "posts", "quests", "services", "routes"],
)
def test_missing_asset_raises_not_found_for_every_resource(ouro, call):
    with pytest.raises(NotFoundError):
        call(ouro, str(uuid.uuid4()))


def test_malformed_id_is_a_client_error(ouro):
    with pytest.raises(BadRequestError):
        ouro.datasets.retrieve("definitely-not-a-uuid")

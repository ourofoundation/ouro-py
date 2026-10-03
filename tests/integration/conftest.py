"""Live integration tests for ouro-py.

These run the SDK against a real Ouro backend and create (then delete) real
assets. They are skipped unless ``OURO_TEST_API_KEY`` is set.

    OURO_TEST_API_KEY=...     # primary user (owns everything the tests create)
    OURO_TEST_API_KEY_2=...   # optional second user for sharing / membership tests
    OURO_TEST_API_KEY_PERSONAL=...  # optional key of the primary user bound to the personal context
    OURO_TEST_API_KEY_ORG=...       # optional key of the primary user bound to an organization
    OURO_TEST_BASE_URL=http://localhost:8003
    pytest tests/integration

The suite refuses to run against a non-local backend unless
``OURO_TEST_ALLOW_REMOTE=1`` is set.
"""

from __future__ import annotations

import logging
import os
import uuid
from urllib.parse import urlparse

import pytest

from ouro import NotFoundError, Ouro
from ouro.models import User

from mock_service import MockService

BASE_URL = os.environ.get("OURO_TEST_BASE_URL", "http://localhost:8003")
PRIMARY_KEY = os.environ.get("OURO_TEST_API_KEY")
SECONDARY_KEY = os.environ.get("OURO_TEST_API_KEY_2")
PERSONAL_BOUND_KEY = os.environ.get("OURO_TEST_API_KEY_PERSONAL")
ORG_BOUND_KEY = os.environ.get("OURO_TEST_API_KEY_ORG")

log = logging.getLogger("ouro.integration")


def pytest_collection_modifyitems(config, items):
    reason = None
    if not PRIMARY_KEY:
        reason = "set OURO_TEST_API_KEY to run live integration tests"
    elif (
        urlparse(BASE_URL).hostname not in {"localhost", "127.0.0.1"}
        and os.environ.get("OURO_TEST_ALLOW_REMOTE") != "1"
    ):
        reason = f"refusing to create test assets on {BASE_URL}; set OURO_TEST_ALLOW_REMOTE=1"
    if reason is None:
        return
    here = os.path.dirname(__file__)
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(pytest.mark.skip(reason=reason))


class Tracker:
    """Records created asset ids so the session can delete them at the end."""

    def __init__(self, ouro: Ouro, run_id: str) -> None:
        self.ouro = ouro
        self.run_id = run_id
        self.ids: list[str] = []

    def name(self, label: str) -> str:
        return f"sdk-it-{self.run_id}-{label}"

    def add(self, asset):
        self.ids.append(str(asset.id))
        return asset

    def cleanup(self) -> list[str]:
        failures = []
        for asset_id in reversed(self.ids):
            try:
                self.ouro.assets.delete(asset_id, delete_children=True)
            except NotFoundError:
                pass
            except Exception as exc:  # report every leftover, don't stop
                failures.append(f"{asset_id}: {exc}")
        return failures


@pytest.fixture(scope="session")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="session")
def ouro() -> Ouro:
    return Ouro(api_key=PRIMARY_KEY, base_url=BASE_URL, client="ouro-py-integration")


@pytest.fixture(scope="session")
def other() -> Ouro:
    if not SECONDARY_KEY:
        pytest.skip("set OURO_TEST_API_KEY_2 for multi-user tests")
    client = Ouro(api_key=SECONDARY_KEY, base_url=BASE_URL, client="ouro-py-integration")
    return client


@pytest.fixture(scope="session")
def personal_bound() -> Ouro:
    if not PERSONAL_BOUND_KEY:
        pytest.skip("set OURO_TEST_API_KEY_PERSONAL for bound-key tests")
    return Ouro(
        api_key=PERSONAL_BOUND_KEY,
        organization="",
        base_url=BASE_URL,
        client="ouro-py-integration",
    )


@pytest.fixture(scope="session")
def org_bound() -> Ouro:
    if not ORG_BOUND_KEY:
        pytest.skip("set OURO_TEST_API_KEY_ORG for bound-key tests")
    return Ouro(api_key=ORG_BOUND_KEY, base_url=BASE_URL, client="ouro-py-integration")


@pytest.fixture(scope="session")
def me(ouro) -> User:
    return ouro.users.me()


@pytest.fixture(scope="session")
def other_me(other) -> User:
    return other.users.me()


@pytest.fixture(scope="session")
def track(ouro, run_id):
    tracker = Tracker(ouro, run_id)
    yield tracker
    failures = tracker.cleanup()
    assert not failures, "failed to clean up test assets:\n" + "\n".join(failures)


@pytest.fixture(scope="session")
def mock_service(run_id):
    service = MockService(prefix=f"sdk-it-{run_id}")
    service.start()
    yield service
    service.stop()

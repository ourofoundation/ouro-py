from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ouro.resources.services import Services


SERVICE_ID = "00000000-0000-0000-0000-000000000001"
SPEC_URL = "https://api.example.test/openapi.json"


def _service_response() -> dict:
    return {
        "id": SERVICE_ID,
        "user_id": "00000000-0000-0000-0000-000000000002",
        "org_id": "00000000-0000-0000-0000-000000000003",
        "team_id": "00000000-0000-0000-0000-000000000004",
        "visibility": "public",
        "asset_type": "service",
        "created_at": "2026-01-01T00:00:00+00:00",
        "last_updated": "2026-01-01T00:00:00+00:00",
        "name": "Example API",
        "metadata": {
            "base_url": "https://api.example.test",
            "authentication": "None",
            "spec_url": SPEC_URL,
        },
    }


def _services(spec_url: str | None = SPEC_URL) -> tuple[Services, MagicMock]:
    ouro = MagicMock()
    services = Services(ouro)
    services.retrieve = MagicMock(
        return_value=SimpleNamespace(
            name="Example API",
            metadata=SimpleNamespace(spec_url=spec_url),
        )
    )
    services._handle_response = MagicMock(return_value=_service_response())
    return services, ouro.client


def test_update_refreshes_stored_remote_spec() -> None:
    services, client = _services()

    services.update(SERVICE_ID, refresh_spec=True)

    client.put.assert_called_once_with(
        f"/services/{SERVICE_ID}/update/from-file",
        json={
            "service": {
                "id": SERVICE_ID,
                "name": "Example API",
                "metadata": {"spec_url": SPEC_URL},
            }
        },
    )
    services.retrieve.assert_called_once_with(SERVICE_ID)


def test_update_refresh_requires_stored_remote_spec() -> None:
    services, client = _services(spec_url=None)

    with pytest.raises(ValueError, match="stored spec_url"):
        services.update(SERVICE_ID, refresh_spec=True)

    client.put.assert_not_called()

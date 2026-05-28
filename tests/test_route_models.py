"""Tests for the typed route declaration models in :mod:`ouro.models.route`.

These exercise the canonical plural declarations and their compatibility
with the legacy single-asset projection that still ships on every route.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ouro.models.route import (
    RouteData,
    RouteInputAssetDeclaration,
    RouteOutputAssetDeclaration,
)


class TestRouteInputAssetDeclaration:
    def test_accepts_canonical_fields(self) -> None:
        declaration = RouteInputAssetDeclaration.model_validate(
            {
                "asset_type": "file",
                "primary": True,
                "input_filter": "atomic structure",
                "file_extensions": ["cif", "xyz"],
            }
        )
        assert declaration.asset_type == "file"
        assert declaration.primary is True
        assert declaration.input_filter == "atomic structure"
        assert declaration.file_extensions == ["cif", "xyz"]

    def test_preserves_unmodeled_metadata(self) -> None:
        declaration = RouteInputAssetDeclaration.model_validate(
            {
                "asset_type": "file",
                "visualization_hint": "scatter-3d",
            }
        )
        dumped = declaration.model_dump()
        assert dumped["visualization_hint"] == "scatter-3d"

    def test_rejects_unknown_asset_type(self) -> None:
        with pytest.raises(ValidationError):
            RouteInputAssetDeclaration.model_validate({"asset_type": "video"})

    def test_rejects_unknown_input_filter(self) -> None:
        with pytest.raises(ValidationError):
            RouteInputAssetDeclaration.model_validate(
                {"asset_type": "file", "input_filter": "binary"}
            )


class TestRouteOutputAssetDeclaration:
    def test_accepts_canonical_fields(self) -> None:
        declaration = RouteOutputAssetDeclaration.model_validate(
            {
                "asset_type": "post",
                "primary": False,
                "file_extensions": None,
            }
        )
        assert declaration.asset_type == "post"
        assert declaration.primary is False
        assert declaration.file_extensions is None


class TestRouteData:
    def test_legacy_only_route_parses(self) -> None:
        route = RouteData.model_validate(
            {
                "path": "/relax",
                "method": "POST",
                "input_type": "file",
                "input_file_extension": "cif",
                "output_type": "post",
            }
        )
        assert route.input_type == "file"
        assert route.output_type == "post"
        assert route.input_assets is None
        assert route.output_assets is None

    def test_plural_only_route_parses(self) -> None:
        route = RouteData.model_validate(
            {
                "path": "/relax",
                "method": "POST",
                "input_assets": {
                    "structure": {
                        "asset_type": "file",
                        "file_extensions": ["cif"],
                        "primary": True,
                    },
                    "training": {"asset_type": "dataset"},
                },
                "output_assets": {
                    "report": {"asset_type": "post", "primary": True},
                    "relaxed": {"asset_type": "file", "file_extensions": ["cif"]},
                },
            }
        )
        assert isinstance(route.input_assets["structure"], RouteInputAssetDeclaration)
        assert route.input_assets["structure"].file_extensions == ["cif"]
        assert route.output_assets["report"].primary is True

    def test_mixed_declarations_coexist(self) -> None:
        route = RouteData.model_validate(
            {
                "path": "/relax",
                "method": "POST",
                "input_type": "file",
                "input_file_extension": "cif",
                "input_assets": {
                    "structure": {
                        "asset_type": "file",
                        "primary": True,
                        "file_extensions": ["cif"],
                    }
                },
                "output_type": "post",
                "output_assets": {
                    "report": {"asset_type": "post", "primary": True}
                },
            }
        )
        assert route.input_type == "file"
        assert route.input_assets is not None
        assert route.input_assets["structure"].asset_type == "file"

"""Tests for the typed route declaration models in :mod:`ouro.models.route`.

These exercise the canonical plural declarations and their compatibility
with the legacy single-asset projection that still ships on every route.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ouro.models.route import (
    RouteCapabilities,
    RouteData,
    RouteInputAssetDeclaration,
    RouteOutputAssetDeclaration,
)
from ouro.utils import ouro_capabilities


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

    def test_accepts_post_and_comment_union(self) -> None:
        declaration = RouteInputAssetDeclaration.model_validate(
            {
                "asset_type": "post",
                "asset_types": ["post", "comment"],
            }
        )
        assert declaration.asset_type == "post"
        assert declaration.asset_types == ["post", "comment"]

    def test_rejects_inconsistent_legacy_projection(self) -> None:
        with pytest.raises(ValidationError):
            RouteInputAssetDeclaration.model_validate(
                {
                    "asset_type": "file",
                    "asset_types": ["post", "comment"],
                }
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

    def test_semantic_capabilities_parse(self) -> None:
        route = RouteData.model_validate(
            {
                "path": "/translate",
                "method": "POST",
                "capabilities": {
                    "text.translate.v1": {
                        "supported_languages": ["en", "es"],
                        "cache_version": "translator-1",
                        "structured_content": True,
                    },
                    "text.speech.v1": {
                        "supported_languages": ["en-US"],
                        "voices": [{"id": "alloy", "language": "en-US"}],
                        "cache_scope": "user",
                        "cache_version": "speech-2",
                        "trusted": True,
                    },
                    "speech.transcribe.v1": {
                        "supported_languages": ["*"],
                        "cache_version": "stt-1",
                    },
                },
            }
        )
        assert isinstance(route.capabilities, RouteCapabilities)
        assert route.capabilities.text_translate_v1 is not None
        assert route.capabilities.text_translate_v1.cache_scope == "none"
        assert route.capabilities.text_speech_v1 is not None
        assert route.capabilities.text_speech_v1.voices[0].id == "alloy"
        assert route.capabilities.speech_transcribe_v1 is not None
        assert route.capabilities.speech_transcribe_v1.cache_scope == "none"


def test_ouro_capabilities_decorator_uses_openapi_extension() -> None:
    decorator = ouro_capabilities(
        {
            "text.translate.v1": {
                "supported_languages": ["en", "es"],
                "cache_version": "translator-1",
            }
        }
    )

    @decorator
    def translate() -> None:
        pass

    capability = translate.ouro_fields["x-ouro-capabilities"]["text.translate.v1"]
    assert capability["cache_scope"] == "none"
    assert capability["trusted"] is False

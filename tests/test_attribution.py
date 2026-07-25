"""Tests for attribution payload helpers and Asset.attribution parsing."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from inspect import signature
from uuid import uuid4

from ouro._resource import _ensure_attribution, _optional_attribution
from ouro.models import Attribution, Citation, Service
from ouro.models.asset import Asset
from ouro.resources.comments import Comments
from ouro.resources.conversations import Conversations
from ouro.resources.datasets import Datasets
from ouro.resources.files import Files
from ouro.resources.posts import Posts
from ouro.resources.quests import Quests
from ouro.resources.routes import Routes
from ouro.resources.services import _attribution_payload, _service_metadata
from ouro.resources.services import Services


class AttributionPayloadTests(unittest.TestCase):
    def test_asset_write_methods_expose_top_level_license_and_attribution(self) -> None:
        methods = (
            Posts.create,
            Posts.update,
            Datasets.create,
            Datasets.update,
            Files.create,
            Files.update,
            Quests.create,
            Quests.update,
            Services.create,
            Services.update,
            Routes.create,
            Routes.update,
            Comments.create,
            Comments.update,
            Conversations.create,
            Conversations.update,
        )

        for method in methods:
            parameters = signature(method).parameters
            self.assertIn("license_id", parameters, method.__qualname__)
            self.assertIn("attribution", parameters, method.__qualname__)

    def test_normalizes_attribution_models_for_create_and_update(self) -> None:
        attribution = Attribution(
            originality="derivative",
            github_url="https://github.com/example/project",
        )

        expected = {
            "originality": "derivative",
            "github_url": "https://github.com/example/project",
        }
        self.assertEqual(_ensure_attribution(attribution), expected)
        self.assertEqual(_optional_attribution(attribution), expected)

    def test_update_attribution_does_not_add_create_defaults(self) -> None:
        self.assertEqual(
            _optional_attribution({"doi_url": "https://doi.org/10.1234/example"}),
            {"doi_url": "https://doi.org/10.1234/example"},
        )

    def test_service_metadata_excludes_attribution_fields(self) -> None:
        meta = _service_metadata(
            base_url="https://api.example.com",
            authentication="None",
            version="1.0",
        )
        self.assertEqual(
            meta,
            {
                "base_url": "https://api.example.com",
                "authentication": "None",
                "version": "1.0",
            },
        )

    def test_attribution_payload_strips_none(self) -> None:
        attr = _attribution_payload(
            originality="third-party",
            doi_url="https://doi.org/10.1234/foo",
            github_url=None,
            relation_type="IsSupplementTo",
        )
        self.assertEqual(
            attr,
            {
                "originality": "third-party",
                "doi_url": "https://doi.org/10.1234/foo",
                "relation_type": "IsSupplementTo",
            },
        )


class AssetAttributionParseTests(unittest.TestCase):
    def test_asset_parses_attribution_column(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        asset = Asset(
            id=uuid4(),
            user_id=uuid4(),
            org_id=uuid4(),
            team_id=uuid4(),
            visibility="public",
            asset_type="file",
            created_at=now,
            last_updated=now,
            name="demo",
            attribution={
                "originality": "original",
                "doi_url": "https://doi.org/10.1/x",
                "citation": {
                    "doi": "10.1/x",
                    "title": "Demo Paper",
                    "authors": ["Ada"],
                    "year": 2024,
                    "source": "crossref",
                },
                "relation_type": "IsSupplementTo",
            },
            metadata={"path": "x", "bucket": "files"},
        )
        self.assertIsInstance(asset.attribution, Attribution)
        assert asset.attribution is not None
        self.assertEqual(asset.attribution.doi_url, "https://doi.org/10.1/x")
        self.assertIsInstance(asset.attribution.citation, Citation)
        assert asset.attribution.citation is not None
        self.assertEqual(asset.attribution.citation.title, "Demo Paper")
        # Type-specific metadata stays separate
        self.assertEqual(asset.metadata, {"path": "x", "bucket": "files"})

    def test_service_metadata_no_longer_carries_doi(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        service = Service(
            id=uuid4(),
            user_id=uuid4(),
            org_id=uuid4(),
            team_id=uuid4(),
            visibility="public",
            asset_type="service",
            created_at=now,
            last_updated=now,
            name="svc",
            metadata={
                "base_url": "https://api.example.com",
                "authentication": "None",
            },
            attribution={
                "originality": "third-party",
                "doi_url": "https://doi.org/10.1/x",
            },
        )
        self.assertEqual(service.metadata.base_url, "https://api.example.com")
        assert service.attribution is not None
        self.assertEqual(service.attribution.doi_url, "https://doi.org/10.1/x")
        self.assertIsNone(getattr(service.metadata, "doi_url", None))


if __name__ == "__main__":
    unittest.main()

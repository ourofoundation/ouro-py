from ouro.models.asset import Asset
from ouro.models.route import RouteOutputAssetDeclaration
from ouro.utils.route_outputs import resolve_route_output_role, resolve_route_outputs


def test_output_without_a_source_is_new():
    assert resolve_route_output_role("version", has_input=False, primary=True) == {
        "role": "new",
        "source": None,
    }


def test_undeclared_roles_follow_where_the_output_lands():
    assert resolve_route_output_role(has_input=True) == {"role": "result", "source": "input"}
    assert resolve_route_output_role(has_input=False) == {
        "role": "result",
        "source": "primary_output",
    }
    assert resolve_route_output_role(has_input=True, embedded=True) == {
        "role": "supporting",
        "source": "embedding_post",
    }


def test_declared_role_wins_once_there_is_a_source():
    assert resolve_route_output_role("version", has_input=True)["role"] == "version"
    assert resolve_route_output_role("bogus", has_input=True)["role"] == "result"


def test_declarations_carry_role():
    outputs = {
        "cif": {"asset_type": "file", "primary": True, "role": "version"},
        "traj": {"asset_type": "file"},
    }
    assert [o["role"] for o in resolve_route_outputs(outputs)] == ["version", None]
    assert RouteOutputAssetDeclaration(asset_type="file", role="result").role == "result"


def test_asset_lineage_fields():
    asset = Asset.model_validate(
        {
            "id": "019f0000-0000-7000-8000-000000000001",
            "user_id": "019f0000-0000-7000-8000-000000000002",
            "org_id": "019f0000-0000-7000-8000-000000000003",
            "team_id": "019f0000-0000-7000-8000-000000000004",
            "visibility": "public",
            "asset_type": "file",
            "created_at": "2026-10-18T00:00:00Z",
            "last_updated": "2026-10-18T00:00:00Z",
            "lineage_role": "version",
            "source_id": "019f0000-0000-7000-8000-000000000005",
            "family_id": "019f0000-0000-7000-8000-000000000005",
        }
    )
    assert asset.lineage_role == "version"
    assert str(asset.family_id).endswith("5")


def test_lineage_reads_sources_and_derived():
    from types import SimpleNamespace

    import httpx

    from ouro.resources.assets import Assets

    def row(n, **extra):
        return {
            "id": f"019f0000-0000-7000-8000-00000000000{n}",
            "user_id": "019f0000-0000-7000-8000-000000000009",
            "org_id": "019f0000-0000-7000-8000-000000000009",
            "team_id": "019f0000-0000-7000-8000-000000000009",
            "visibility": "public",
            "asset_type": "file",
            "created_at": "2026-10-18T00:00:00Z",
            "last_updated": "2026-10-18T00:00:00Z",
            **extra,
        }

    body = {
        "data": {
            "asset": {
                "id": row(2)["id"],
                "asset_type": "file",
                "lineage_role": "version",
                "source_id": row(1)["id"],
                "family_id": row(1)["id"],
                "run": None,
            },
            "sources": [row(1, lineage_role="new")],
            "derived": [
                row(
                    3,
                    lineage_role="result",
                    source_id=row(2)["id"],
                    run={
                        "action_id": "019f0000-0000-7000-8000-000000000008",
                        "created_at": "2026-10-18T00:00:00Z",
                        "route": {"id": "r", "name": "Phonon dispersion"},
                    },
                )
            ],
            "truncated": False,
        },
        "error": None,
    }
    requests = []

    class Response:
        status_code = 200
        is_success = True
        headers = {}
        request = httpx.Request("GET", "https://api.example.test")

        def json(self):
            return body

    def get(path, params=None):
        requests.append(path)
        return Response()

    ouro = SimpleNamespace(client=SimpleNamespace(get=get), websocket=None)
    lineage = Assets(ouro).lineage(row(2)["id"])

    assert requests == [f"/assets/{row(2)['id']}/lineage"]
    assert lineage.asset.lineage_role == "version"
    assert lineage.sources[0].lineage_role == "new"
    assert lineage.derived[0].lineage_role == "result"
    assert lineage.derived[0].run.route["name"] == "Phonon dispersion"

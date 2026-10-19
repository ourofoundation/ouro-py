from ouro.models.route import RouteOutputAssetDeclaration
from ouro.utils.route_outputs import (
    describe_route_output_when,
    request_body_json_schema,
    resolve_route_outputs,
    route_output_when_matches,
)

REQUEST_BODY = {
    "content": {
        "application/json": {
            "schema": {
                "type": "object",
                "properties": {
                    "include_dataset": {"type": "boolean", "default": False},
                    "include_report": {"type": "boolean", "default": False},
                    "mode": {"type": "string", "enum": ["single", "batch"], "default": "single"},
                    "trajectory": {"anyOf": [{"type": "string", "enum": ["none", "full"]}, {"type": "null"}]},
                    "steps": {"type": "integer", "default": 1},
                },
            }
        }
    }
}

OUTPUTS = {
    "structure": {"asset_type": "file", "primary": True},
    "phases_dataset": {"asset_type": "dataset", "when": {"include_dataset": True}},
    "report": {"asset_type": "post", "when": {"include_report": True}},
    "summary": {"asset_type": "file", "embedded_in": "report"},
    "cifs_zip": {"asset_type": "file", "when": {"mode": ["batch"]}},
}


def test_matches_body_then_schema_default():
    assert not route_output_when_matches({"include_dataset": True}, {}, REQUEST_BODY)
    assert route_output_when_matches({"include_dataset": True}, {"include_dataset": True}, REQUEST_BODY)
    assert route_output_when_matches({"mode": ["single"]}, None, REQUEST_BODY)
    assert not route_output_when_matches({"trajectory": "full"}, {}, REQUEST_BODY)
    assert route_output_when_matches(None, {}, REQUEST_BODY)


def test_true_never_matches_one():
    assert not route_output_when_matches({"steps": True}, {}, REQUEST_BODY)
    assert route_output_when_matches({"steps": 1}, {"steps": "1"}, REQUEST_BODY)


def test_resolve_marks_active_and_embedded():
    off = {o["name"]: o for o in resolve_route_outputs(OUTPUTS, REQUEST_BODY, {})}
    assert off["structure"]["active"] and not off["report"]["active"]
    assert off["summary"]["active"] and not off["summary"]["embedded"]
    assert not off["phases_dataset"]["active"] and not off["cifs_zip"]["active"]

    on = {
        o["name"]: o
        for o in resolve_route_outputs(
            OUTPUTS, REQUEST_BODY, {"include_report": True, "include_dataset": True, "mode": "batch"}
        )
    }
    assert on["report"]["active"] and on["summary"]["embedded"]
    assert on["phases_dataset"]["active"] and on["cifs_zip"]["active"]


def test_resolve_accepts_models():
    models = {name: RouteOutputAssetDeclaration(**decl) for name, decl in OUTPUTS.items()}
    resolved = {o["name"]: o for o in resolve_route_outputs(models, REQUEST_BODY, {"include_dataset": True})}
    assert resolved["phases_dataset"]["active"]
    assert resolved["phases_dataset"]["when"] == {"include_dataset": True}


def test_describe():
    assert describe_route_output_when({"include_dataset": True}) == "include_dataset is true"
    assert (
        describe_route_output_when({"mode": ["batch", "both"], "include_report": True})
        == "mode is batch or both and include_report is true"
    )


def test_request_body_json_schema_picks_the_object_variant_in_place():
    variant = {"type": "object", "properties": {"file": {"type": "object"}}}
    body = {"content": {"application/json": {"schema": {"anyOf": [variant, {"type": "null"}]}}}}
    schema = request_body_json_schema(body)
    assert schema is variant  # the object itself, so callers can edit a copy through it
    assert request_body_json_schema({"content": {}}) is None

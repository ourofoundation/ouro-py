import base64

import pytest

from ouro.models.route import RoutePropertyDeclaration
from ouro.utils import (
    dataset_payload,
    file_payload,
    include_dataset_output,
    ouro_outputs,
    ouro_properties,
    ouro_response_charts,
)

CURVE_CHART = {
    "data": "magnetization_curve",
    "title": "Magnetization curve",
    "dataset": "curve_dataset",
    "sort": "temperature_K",
    "config": {"type": "line", "series": [{"dataKey": "order_parameter"}]},
}

OUTPUTS = {
    "report": {"asset_type": "post", "primary": True, "when": {"include_report": True}},
    "summary": {"asset_type": "file", "file_extensions": ["json"], "embedded_in": "report"},
    "curve_dataset": include_dataset_output(),
}


def _fields(*decorators):
    def endpoint():
        pass

    for decorator in decorators:
        endpoint = decorator(endpoint)
    return endpoint.ouro_fields


def test_declarations_land_as_written():
    fields = _fields(
        ouro_outputs(OUTPUTS),
        ouro_response_charts([CURVE_CHART]),
        ouro_properties(
            [{"property": "curie_temperature", "value": "Tc_K", "unit": "K", "method": None}]
        ),
    )
    assert fields["x-ouro-output-assets"]["curve_dataset"] == {
        "asset_type": "dataset",
        "when": {"include_dataset": True},
    }
    assert fields["x-ouro-response-charts"] == [CURVE_CHART]
    # None fields are left out of the spec
    assert fields["x-ouro-properties"] == [
        {"property": "curie_temperature", "value": "Tc_K", "unit": "K"}
    ]


@pytest.mark.parametrize(
    "outputs, message",
    [
        ({"cif": {"asset_type": "file", "optinal": True}}, "unknown key 'optinal'"),
        ({"cif": {"asset_type": "folder"}}, "asset_type"),
        ({"cif": {"asset_type": "file", "embedded_in": "report"}}, "is not another output"),
        ({"cif": {"asset_type": "file", "embedded_in": "cif"}}, "is not another output"),
        ({}, "give a dict"),
    ],
)
def test_outputs_reject_mistakes(outputs, message):
    with pytest.raises(ValueError, match=message):
        ouro_outputs(outputs)


def test_chart_dataset_must_be_a_dataset_output_in_either_order():
    chart = ouro_response_charts([{**CURVE_CHART, "dataset": "summary"}])
    outputs = ouro_outputs(OUTPUTS)
    with pytest.raises(ValueError, match="'summary' is not a dataset output"):
        _fields(outputs, chart)
    with pytest.raises(ValueError, match="'summary' is not a dataset output"):
        _fields(chart, outputs)
    # Without declared outputs there is nothing to check against here
    assert _fields(chart)


@pytest.mark.parametrize(
    "chart, message",
    [
        ({**CURVE_CHART, "limit": 0}, "limit"),
        ({**CURVE_CHART, "sql": "x"}, "unknown key 'sql'"),
        ({k: v for k, v in CURVE_CHART.items() if k != "config"}, "config"),
    ],
)
def test_charts_reject_mistakes(chart, message):
    with pytest.raises(ValueError, match=message):
        ouro_response_charts([chart])


@pytest.mark.parametrize(
    "declarations, message",
    [
        ([{"property": "x", "value": "y", "subject": {"asset": "z"}}], "subject"),
        ([{"property": "x", "value": "y", "requires": {"a": {"low": 1}}}], "range"),
        ([{"property": "x", "value": "y", "requires": {"a": []}}], "list needs"),
        ([{"property": "x", "value": "y"}, {"property": "x", "value": "z"}], "declared twice"),
        ([{"property": "x", "value": "y", "subject": {"output": "nope"}}], None),
    ],
)
def test_properties_reject_mistakes(declarations, message):
    if message is None:
        # An output subject is checked once outputs are declared
        with pytest.raises(ValueError, match="'nope' is not an output"):
            _fields(ouro_properties(declarations), ouro_outputs(OUTPUTS))
        return
    with pytest.raises(ValueError, match=message):
        ouro_properties(declarations)


def test_property_methods_tell_repeats_apart():
    assert _fields(
        ouro_properties(
            [
                {"property": "x", "value": "a", "method": "Monte Carlo"},
                {"property": "x", "value": "b", "method": "mean field",
                 "requires": {"overlap": {"min": 0.5}, "status": ["ok", "done"]}},
            ]
        )
    )
    # Models read from the API validate the same way
    assert RoutePropertyDeclaration.model_validate(
        {"property": "x", "value": "a", "subject": {"input": "file"}}
    )


def test_payloads():
    file = file_payload("data_NaCl", name="NaCl", description="Rock salt", extension=".CIF")
    assert file["extension"] == "cif"
    assert file["type"] == "chemical/x-cif"
    assert base64.b64decode(file["base64"]) == b"data_NaCl"
    assert "visibility" not in file
    assert file_payload(b"{}", name="a", description="b", extension="json")["type"] == (
        "application/json"
    )
    assert dataset_payload([], name="a", description="b") is None
    assert dataset_payload([{"x": 1}], name="a", description="b", visibility="public") == {
        "name": "a",
        "description": "b",
        "data": [{"x": 1}],
        "visibility": "public",
    }

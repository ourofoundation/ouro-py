import json

import pytest

pytest.importorskip("pymatgen")

from pymatgen.analysis.phase_diagram import PDEntry, PhaseDiagram
from pymatgen.core import Composition

from ouro.utils.phase_diagram import phase_diagram_to_dict


def _entry(formula: str, energy: float, entry_id: str) -> PDEntry:
    entry = PDEntry(Composition(formula), energy)
    entry.entry_id = entry_id
    return entry


@pytest.fixture
def li_fe_o():
    entries = [
        _entry("Li", -1.9, "mp-1"),
        _entry("Fe", -8.3, "mp-2"),
        _entry("O2", -9.8, "mp-3"),
        _entry("Li2O", -14.3, "mp-4"),
        _entry("FeO", -16.8, "mp-5"),
        _entry("LiFeO2", -30.0, "mp-6"),
        _entry("Li2O2", -19.0, "mp-7"),
        _entry("FeO2", -20.0, "ouro-abc"),
    ]
    return PhaseDiagram(entries), entries


def test_serializes_entries_in_element_order(li_fe_o):
    pd, _ = li_fe_o
    data = phase_diagram_to_dict(pd)

    assert data["format"] == "ouro.phase-diagram"
    assert data["version"] == 1
    assert data["elements"] == ["Li", "Fe", "O"]
    li2o = next(e for e in data["entries"] if e["formula"] == "Li2O")
    assert li2o["composition"] == pytest.approx([2 / 3, 0, 1 / 3], abs=1e-6)
    assert li2o["stable"] is True
    assert li2o["e_above_hull"] == 0
    json.dumps(data)


def test_facets_reference_stable_entries(li_fe_o):
    pd, _ = li_fe_o
    data = phase_diagram_to_dict(pd)

    assert len(data["facets"]) == len(pd.facets)
    for facet in data["facets"]:
        assert len(facet) == len(data["elements"])
        assert all(data["entries"][i]["stable"] for i in facet)


def test_cutoff_keeps_stable_and_highlighted_entries(li_fe_o):
    pd, entries = li_fe_o
    far_above = entries[-1]
    assert pd.get_e_above_hull(far_above) > 0.5

    data = phase_diagram_to_dict(pd, max_e_above_hull=0.1)
    assert "FeO2" not in {e["formula"] for e in data["entries"]}

    data = phase_diagram_to_dict(
        pd,
        max_e_above_hull=0.1,
        highlight=far_above,
        asset_ids={"ouro-abc": "11111111-1111-1111-1111-111111111111"},
    )
    highlighted = data["entries"][data["highlight"]]
    assert highlighted["formula"] == "FeO2"
    assert highlighted["asset_id"] == "11111111-1111-1111-1111-111111111111"

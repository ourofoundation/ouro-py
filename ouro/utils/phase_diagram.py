"""Serialize pymatgen phase diagrams into Ouro's ``.phasediagram`` file format.

Ouro renders ``.phasediagram`` files as interactive binary, ternary, and
quaternary phase diagrams. The file carries the thermodynamics pymatgen already
computed — formation energies, hull distances, and the hull facets themselves —
so the viewer draws exactly the hull the numbers were measured against rather
than recomputing one. The format is open and fully specified at
https://ouro.foundation/docs/developers/phase-diagram-format, so files written
by any other tool render the same way.

    import json
    from ouro.utils.phase_diagram import PHASE_DIAGRAM_EXTENSION, phase_diagram_to_dict

    data = phase_diagram_to_dict(pd, max_e_above_hull=0.2, highlight=my_entry)
    ouro.files.create(
        name="Fe-Co-Bi phase diagram",
        visibility="public",
        file_content=json.dumps(data).encode(),
        file_name=f"Fe-Co-Bi.{PHASE_DIAGRAM_EXTENSION}",
    )

Format (version 1)::

    {
      "format": "ouro.phase-diagram",
      "version": 1,
      "elements": ["Fe", "Co", "Bi"],
      "energy_model": "Orb v3",              # optional label
      "entries": [
        {
          "id": "mp-13",                     # entry_id, or null
          "formula": "Fe",
          "composition": [1.0, 0.0, 0.0],    # atomic fractions, ordered like elements
          "energy_per_atom": -8.31,
          "formation_energy_per_atom": 0.0,
          "e_above_hull": 0.0,
          "stable": true,
          "space_group": "Im-3m",            # optional Hermann–Mauguin symbol
          "asset_id": "…"                    # optional Ouro asset for this entry
        }
      ],
      "facets": [[0, 3, 5]],                 # hull simplices, as indices into entries
      "highlight": 7                         # optional entry to feature
    }

Decompositions are not stored: every entry decomposes onto the facet whose
composition simplex contains it, so readers derive them from ``facets``.

``space_group`` comes from the entry's structure when it has one (a
``ComputedStructureEntry``), otherwise from ``entry.data["space_group"]``.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

PHASE_DIAGRAM_FORMAT = "ouro.phase-diagram"
PHASE_DIAGRAM_VERSION = 1
PHASE_DIAGRAM_EXTENSION = "phasediagram"

__all__ = [
    "PHASE_DIAGRAM_EXTENSION",
    "PHASE_DIAGRAM_FORMAT",
    "PHASE_DIAGRAM_VERSION",
    "phase_diagram_to_dict",
]


def phase_diagram_to_dict(
    phase_diagram: Any,
    *,
    max_e_above_hull: Optional[float] = None,
    highlight: Any = None,
    asset_ids: Optional[Mapping[str, str]] = None,
    energy_model: Optional[str] = None,
) -> dict:
    """Serialize a ``pymatgen.analysis.phase_diagram.PhaseDiagram``.

    Args:
        phase_diagram: The pymatgen ``PhaseDiagram`` to serialize.
        max_e_above_hull: Drop unstable entries further than this (eV/atom)
            above the hull. Stable entries and ``highlight`` are always kept.
        highlight: An entry of ``phase_diagram`` to feature, usually the
            structure the diagram was built to assess.
        asset_ids: Ouro asset ids keyed by ``entry_id``, so the viewer can link
            an entry to the structure file it came from.
        energy_model: Label for the method that produced the energies.
    """
    pd = phase_diagram
    elements = list(pd.elements)
    asset_ids = asset_ids or {}

    e_above_hull = {id(entry): pd.get_e_above_hull(entry) for entry in pd.all_entries}
    kept = [
        entry
        for entry in pd.all_entries
        if entry is highlight
        or max_e_above_hull is None
        or e_above_hull[id(entry)] <= max_e_above_hull
    ]
    index = {id(entry): i for i, entry in enumerate(kept)}
    stable = {id(entry) for entry in pd.stable_entries}

    def serialize(entry: Any) -> dict:
        serialized = {
            "id": entry.entry_id,
            "formula": entry.composition.reduced_formula,
            "composition": [
                round(entry.composition.get_atomic_fraction(el), 6) for el in elements
            ],
            "energy_per_atom": round(entry.energy_per_atom, 6),
            "formation_energy_per_atom": round(pd.get_form_energy_per_atom(entry), 6),
            "e_above_hull": round(e_above_hull[id(entry)], 6),
            "stable": id(entry) in stable,
        }
        space_group = _space_group(entry)
        if space_group:
            serialized["space_group"] = space_group
        if entry.entry_id in asset_ids:
            serialized["asset_id"] = asset_ids[entry.entry_id]
        return serialized

    data = {
        "format": PHASE_DIAGRAM_FORMAT,
        "version": PHASE_DIAGRAM_VERSION,
        "elements": [el.symbol for el in elements],
        "entries": [serialize(entry) for entry in kept],
        "facets": [
            [index[id(pd.qhull_entries[vertex])] for vertex in facet]
            for facet in pd.facets
        ],
    }
    if energy_model:
        data["energy_model"] = energy_model
    if highlight is not None:
        data["highlight"] = index[id(highlight)]
    return data


def _space_group(entry: Any) -> Optional[str]:
    structure = getattr(entry, "structure", None)
    if structure is not None:
        return structure.get_space_group_info()[0]
    return getattr(entry, "data", {}).get("space_group")

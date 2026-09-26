"""Serialize phonopy results into Ouro's ``.phonons`` file format.

Ouro renders ``.phonons`` files as an interactive phonon dispersion beside the
phonon density of states, reporting whether the structure is dynamically
stable and which elements move in each mode. The format is open and fully
specified at https://ouro.foundation/docs/developers/phonon-format, so files
written by any other tool render the same way.

    import json
    from ouro.utils.phonons import PHONONS_EXTENSION, phonons_to_dict

    phonon.produce_force_constants()
    data = phonons_to_dict(phonon, force_model="Orb v3")
    ouro.files.create(
        name="NaCl phonons",
        visibility="public",
        file_content=json.dumps(data).encode(),
        file_name=f"NaCl.{PHONONS_EXTENSION}",
    )

Format (version 1)::

    {
      "format": "ouro.phonons",
      "version": 1,
      "force_model": "Orb v3",                   # optional label
      "qpoints": {
        "distances": [0.0, 0.01, ...],           # repeats at segment junctions
        "labels": [{"index": 0, "label": "Γ"}, ...]
      },
      "frequencies": [[...], ...],               # THz, [branch][q-point]; imaginary < 0
      "projections": [                           # each element's share of every mode
        {"element": "Na", "weights": [[...], ...]}
      ],
      "dos": {
        "frequencies": [...],                    # THz, ascending
        "total": [...],                          # states/THz
        "projections": [{"element": "Na", "values": [...]}]
      }
    }
"""

from __future__ import annotations

import re
from typing import Any, Optional

import numpy as np

PHONONS_FORMAT = "ouro.phonons"
PHONONS_VERSION = 1
PHONONS_EXTENSION = "phonons"

#: Frequencies below −this, in THz, are imaginary modes rather than the
#: numerical noise acoustic branches pick up near Γ. Ouro's viewer uses the same.
IMAGINARY_TOLERANCE = 0.3

__all__ = [
    "IMAGINARY_TOLERANCE",
    "PHONONS_EXTENSION",
    "PHONONS_FORMAT",
    "PHONONS_VERSION",
    "phonons_to_dict",
]

_SYMBOLS = {r"\Gamma": "Γ", "GAMMA": "Γ", "SIGMA": "Σ", "DELTA": "Δ", "LAMBDA": "Λ"}
_SUBSCRIPTS = str.maketrans("0123456789", "₀₁₂₃₄₅₆₇₈₉")


def phonons_to_dict(
    phonon: Any,
    *,
    force_model: Optional[str] = None,
    dos_mesh: float = 40.0,
) -> dict:
    """Serialize a ``phonopy.Phonopy`` whose force constants are produced.

    Runs the high-symmetry band structure and a DOS mesh on ``phonon``,
    replacing any band structure, mesh, or DOS results it already holds.

    Args:
        phonon: The ``Phonopy`` object to serialize.
        force_model: Label for the method behind the force constants.
        dos_mesh: Reciprocal-space length phonopy turns into the DOS mesh.
    """
    symbols = list(phonon.primitive.symbols)
    phonon.auto_band_structure(with_eigenvectors=True)
    band_structure = phonon.band_structure
    # (q, branch) → (branch, q)
    frequencies = np.concatenate(band_structure.frequencies).T

    data: dict = {"format": PHONONS_FORMAT, "version": PHONONS_VERSION}
    if force_model:
        data["force_model"] = force_model
    data.update(
        qpoints=_qpath(band_structure),
        frequencies=np.round(frequencies, 4).tolist(),
        projections=_mode_character(band_structure, symbols),
        dos=_dos(phonon, symbols, dos_mesh),
    )
    return data


def _readable_label(latex: str) -> str:
    r"""A phonopy path label as readers see it: ``$\mathrm{X}_{1}$`` → ``X₁``."""
    text = re.sub(r"\\mathrm\{(.*?)\}", r"\1", latex.strip("$"))
    text = re.sub(r"_\{?(\d+)\}?", lambda m: m[1].translate(_SUBSCRIPTS), text)
    for name, symbol in _SYMBOLS.items():
        text = text.replace(name, symbol)
    return text


def _qpath(band_structure: Any) -> dict:
    """Distances and labels along phonopy's path, its segments laid end to end.

    Segments share their end points, so each junction repeats a distance: the
    same point where the path is connected, a jump where it isn't.
    """
    distances = np.concatenate(band_structure.distances)
    labels = iter(band_structure.labels)
    marks = [{"index": 0, "label": _readable_label(next(labels))}]
    offset = 0
    for segment, connected in zip(
        band_structure.distances, band_structure.path_connections
    ):
        offset += len(segment)
        marks.append({"index": offset - 1, "label": _readable_label(next(labels))})
        if not connected and offset < len(distances):
            marks.append({"index": offset, "label": _readable_label(next(labels))})
    return {"distances": np.round(distances, 5).tolist(), "labels": marks}


def _by_element(symbols: list[str], per_atom: np.ndarray) -> dict[str, np.ndarray]:
    """Sum an array indexed by atom along its first axis into one per element."""
    return {
        element: per_atom[[s == element for s in symbols]].sum(axis=0)
        for element in dict.fromkeys(symbols)
    }


def _mode_character(band_structure: Any, symbols: list[str]) -> list[dict]:
    """Each element's share of every mode, from the squared eigenvector on its atoms."""
    # (q, 3·atom + xyz, branch) → (atom, branch, q)
    eigenvectors = np.concatenate(band_structure.eigenvectors)
    shares = (
        (np.abs(eigenvectors) ** 2)
        .reshape(len(eigenvectors), len(symbols), 3, -1)
        .sum(axis=2)
        .transpose(1, 2, 0)
    )
    return [
        {"element": element, "weights": np.round(weights, 2).tolist()}
        for element, weights in _by_element(symbols, shares).items()
    ]


def _dos(phonon: Any, symbols: list[str], mesh: float) -> dict:
    phonon.run_mesh(mesh, with_eigenvectors=True, is_mesh_symmetry=False)
    phonon.run_projected_dos()
    pdos = phonon.projected_dos
    return {
        "frequencies": np.round(pdos.frequency_points, 4).tolist(),
        "total": np.round(pdos.projected_dos.sum(axis=0), 4).tolist(),
        "projections": [
            {"element": element, "values": np.round(values, 4).tolist()}
            for element, values in _by_element(symbols, pdos.projected_dos).items()
        ],
    }

import json

import numpy as np
import pytest

pytest.importorskip("phonopy")
pytest.importorskip("ase")

from ase import Atoms
from ase.build import bulk
from ase.calculators.emt import EMT
from phonopy import Phonopy
from phonopy.structure.atoms import PhonopyAtoms

from ouro.utils.phonons import IMAGINARY_TOLERANCE, phonons_to_dict


def _phonon(atoms: Atoms, supercell: int) -> Phonopy:
    phonon = Phonopy(
        PhonopyAtoms(
            symbols=atoms.get_chemical_symbols(),
            cell=atoms.cell.array,
            scaled_positions=atoms.get_scaled_positions(),
        ),
        supercell_matrix=np.eye(3, dtype=int) * supercell,
        primitive_matrix="auto",
    )
    phonon.generate_displacements(distance=0.01)
    forces = []
    for cell in phonon.supercells_with_displacements:
        displaced = Atoms(
            cell.symbols,
            cell=cell.cell,
            scaled_positions=cell.scaled_positions,
            pbc=True,
        )
        displaced.calc = EMT()
        forces.append(displaced.get_forces())
    phonon.forces = forces
    phonon.produce_force_constants()
    return phonon


@pytest.fixture(scope="module")
def cu3au():
    atoms = Atoms(
        "AuCu3",
        cell=np.eye(3) * 3.75,
        scaled_positions=[[0, 0, 0], [0, 0.5, 0.5], [0.5, 0, 0.5], [0.5, 0.5, 0]],
        pbc=True,
    )
    return phonons_to_dict(_phonon(atoms, 2), force_model="EMT")


def test_writes_the_documented_shape(cu3au):
    assert cu3au["format"] == "ouro.phonons"
    assert cu3au["version"] == 1
    assert cu3au["force_model"] == "EMT"
    n_q = len(cu3au["qpoints"]["distances"])
    assert len(cu3au["frequencies"]) == 12
    assert all(len(branch) == n_q for branch in cu3au["frequencies"])
    json.dumps(cu3au)


def test_labels_the_path_and_its_jump(cu3au):
    distances = cu3au["qpoints"]["distances"]
    labels = [(m["index"], m["label"]) for m in cu3au["qpoints"]["labels"]]
    assert labels[0] == (0, "Γ")
    assert labels[-1][0] == len(distances) - 1
    # The simple cubic path jumps from X to R.
    jump = next(i for i, label in labels if label == "R" and (i - 1, "X") in labels)
    assert distances[jump] == distances[jump - 1]
    assert all("$" not in label and "\\" not in label for _, label in labels)


def test_mode_shares_sum_to_one(cu3au):
    assert [p["element"] for p in cu3au["projections"]] == ["Au", "Cu"]
    total = np.sum([p["weights"] for p in cu3au["projections"]], axis=0)
    assert np.allclose(total, 1, atol=0.02)
    # Heavy Au carries the acoustic branches at the zone boundary.
    au = np.array(cu3au["projections"][0]["weights"])
    assert au[0].max() > 0.5


def test_dos_projections_add_up_to_the_total(cu3au):
    dos = cu3au["dos"]
    assert np.all(np.diff(dos["frequencies"]) > 0)
    summed = np.sum([p["values"] for p in dos["projections"]], axis=0)
    assert np.allclose(summed, dos["total"], atol=1e-3)


def test_bcc_copper_has_imaginary_modes():
    data = phonons_to_dict(_phonon(bulk("Cu", "bcc", a=2.87), 3))
    assert "force_model" not in data
    assert min(map(min, data["frequencies"])) < -IMAGINARY_TOLERANCE

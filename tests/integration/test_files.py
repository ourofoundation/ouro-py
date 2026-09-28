from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from ouro.models import File

CIF = """data_Fe
_cell_length_a 2.87
_cell_length_b 2.87
_cell_length_c 2.87
_cell_angle_alpha 90
_cell_angle_beta 90
_cell_angle_gamma 90
_symmetry_space_group_name_H-M 'I m -3 m'
loop_
_atom_site_label
_atom_site_fract_x
_atom_site_fract_y
_atom_site_fract_z
Fe 0 0 0
"""


@pytest.fixture(scope="module")
def cif_file(ouro, track, tmp_path_factory):
    path = tmp_path_factory.mktemp("files") / "fe.cif"
    path.write_text(CIF)
    return track.add(
        ouro.files.create(
            name=track.name("fe-cif"),
            visibility="private",
            file_path=str(path),
            description="Body-centred iron",
        )
    )


def test_create_from_path(cif_file):
    assert isinstance(cif_file, File)
    assert cif_file.asset_type == "file"
    assert cif_file.metadata.extension == "cif"
    assert cif_file.metadata.size == len(CIF)
    assert cif_file.metadata.bucket == "files"


def test_retrieve_includes_signed_url(ouro, cif_file):
    fetched = ouro.files.retrieve(str(cif_file.id))
    assert fetched.data.url
    assert httpx.get(fetched.data.url).text == CIF
    assert ouro.files.retrieve(str(cif_file.id), include_data=False).data is None


def test_read_data_on_model(ouro, cif_file):
    model = ouro.files.retrieve(str(cif_file.id), include_data=False)
    assert model.read_data().url


def test_create_from_bytes_public_bucket(ouro, track):
    created = track.add(
        ouro.files.create(
            name=track.name("notes"),
            visibility="public",
            file_content=b"hello,world\n1,2\n",
            file_name="notes.csv",
        )
    )
    assert created.metadata.bucket == "public-files"
    assert created.metadata.extension == "csv"


def test_file_content_requires_file_name(ouro):
    with pytest.raises(ValueError):
        ouro.files.create(name="x", visibility="private", file_content=b"x")
    with pytest.raises(ValueError):
        ouro.files.create(name="x", visibility="private", file_path="a", file_content=b"x")


def test_update_metadata_and_content(ouro, track):
    created = track.add(
        ouro.files.create(
            name=track.name("mutable"),
            visibility="private",
            file_content=b"v1",
            file_name="data.txt",
        )
    )
    renamed = ouro.files.update(str(created.id), name=track.name("mutable-renamed"))
    assert renamed.name == track.name("mutable-renamed")

    replaced = ouro.files.update(str(created.id), file_content=b"version two", file_name="data.txt")
    assert replaced.id == created.id
    url = ouro.files.retrieve(str(created.id)).data.url
    assert httpx.get(url).content == b"version two"


def test_download_roundtrip(ouro, cif_file, tmp_path):
    result = ouro.assets.download(str(cif_file.id), output_path=str(tmp_path) + "/")
    saved = Path(result["path"])
    assert saved.parent == tmp_path.resolve()
    assert saved.suffix == ".cif"
    assert saved.read_text() == CIF
    assert result["bytes"] == len(CIF)


def test_search_by_extension(ouro, cif_file):
    results = ouro.files.search(extension=".CIF", scope="personal", limit=50, sort="recent")
    assert all(isinstance(f, File) for f in results)
    assert any(f.id == cif_file.id for f in results)
    assert all(f.metadata.extension == "cif" for f in results if f.metadata)

    page = ouro.files.search(extension="cif", scope="personal", limit=1, with_pagination=True)
    assert len(page["data"]) == 1
    assert "hasMore" in page["pagination"]


def test_list_is_search_without_pagination(ouro, cif_file):
    assert any(f.id == cif_file.id for f in ouro.files.list(scope="personal", sort="recent", limit=50))


def test_create_requires_file_data(ouro):
    with pytest.raises(ValueError):
        ouro.files.create(name="x", visibility="private")

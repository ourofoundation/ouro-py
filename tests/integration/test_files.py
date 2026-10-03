from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from ouro import NotFoundError
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
    saved = Path(result.path)
    assert saved.parent == tmp_path.resolve()
    assert saved.suffix == ".cif"
    assert saved.read_text() == CIF
    assert result.size == len(CIF)


def test_search_by_extension(ouro, cif_file):
    results = ouro.files.search(extension=".CIF", scope="personal", limit=50, sort="recent")
    assert all(isinstance(f, File) for f in results)
    assert any(f.id == cif_file.id for f in results)
    assert all(f.metadata.extension == "cif" for f in results if f.metadata)

    page = ouro.files.search(extension="cif", scope="personal", limit=1)
    assert len(page) == 1
    assert isinstance(page.has_more, bool)


def test_list_is_search_without_pagination(ouro, cif_file):
    assert any(f.id == cif_file.id for f in ouro.files.list(scope="personal", sort="recent", limit=50))


def test_create_requires_file_data(ouro):
    with pytest.raises(ValueError):
        ouro.files.create(name="x", visibility="private")


def _put(upload: dict, content: bytes) -> None:
    """Upload the way a shell would: a raw body to the signed URL."""
    response = httpx.request(
        upload["method"], upload["upload_url"], headers=upload["headers"], content=content
    )
    response.raise_for_status()


def test_create_from_signed_upload(ouro, track):
    content = b"x,y\n1,2\n"
    upload = ouro.files.create_upload_url("points.csv", visibility="private")
    assert upload["upload_id"] == f"{upload['bucket']}/{upload['path']}"
    assert upload["headers"]["content-type"] == "text/csv"

    _put(upload, content)
    created = track.add(
        ouro.files.create(
            name=track.name("signed-upload"),
            visibility="private",
            upload_id=upload["upload_id"],
            file_name="points.csv",
        )
    )
    assert created.metadata.extension == "csv"
    assert created.metadata.size == len(content)
    assert created.metadata.type == "text/csv"
    assert httpx.get(ouro.files.retrieve(str(created.id)).data.url).content == content


def test_update_from_signed_upload(ouro, cif_file):
    replacement = b"data_replaced\n"
    upload = ouro.files.create_upload_url("fe.cif")
    _put(upload, replacement)
    updated = ouro.files.update(str(cif_file.id), upload_id=upload["upload_id"], file_name="fe.cif")
    # The asset keeps its own storage path; only the bytes change.
    assert updated.metadata.path == cif_file.metadata.path
    assert httpx.get(ouro.files.retrieve(str(cif_file.id)).data.url).content == replacement


def test_signed_upload_must_be_uploaded_first(ouro):
    upload = ouro.files.create_upload_url("never.txt")
    with pytest.raises(NotFoundError):
        ouro.files.create(name="x", visibility="private", upload_id=upload["upload_id"])


def test_one_file_source_only(ouro):
    with pytest.raises(ValueError):
        ouro.files.create(name="x", visibility="private", upload_id="files/a/b.txt", file_path="a")
    with pytest.raises(ValueError):
        ouro.files.update("00000000-0000-0000-0000-000000000000", upload_id="files/a/b.txt", file_content=b"x", file_name="b.txt")


def _classification(ouro, file_id: str, *, timeout: float = 30.0):
    """A file is inspected just after its bytes land; wait for the result."""
    import time

    deadline = time.monotonic() + timeout
    while True:
        metadata = ouro.files.retrieve(file_id, include_data=False).metadata.model_dump()
        if metadata.get("classification") or time.monotonic() > deadline:
            return metadata.get("classification")
        time.sleep(1)


def test_upload_is_classified(ouro, track):
    created = track.add(
        ouro.files.create(
            name=track.name("classified-cif"),
            visibility="private",
            file_path=str(Path(__file__).parent.parent / "Fe.cif"),
        )
    )
    classification = _classification(ouro, str(created.id))
    assert classification, "the CIF was never inspected"
    assert classification["inspector"] == "cif"
    assert classification["kind"] == "crystal"

    # New bytes are inspected again rather than keeping the old answer.
    ouro.files.update(str(created.id), file_content=b"not a structure\n", file_name="Fe.cif")
    replaced = _classification(ouro, str(created.id))
    assert replaced is None or replaced["kind"] != "crystal"


def test_classification_is_never_taken_from_the_client(ouro, track):
    created = track.add(
        ouro.files.create(
            name=track.name("forged-classification"),
            visibility="private",
            file_content=b"plain text\n",
            file_name="notes.txt",
            metadata={"classification": {"kind": "forged", "inspector": "client", "version": 99}},
        )
    )
    metadata = ouro.files.retrieve(str(created.id), include_data=False).metadata.model_dump()
    assert (metadata.get("classification") or {}).get("kind") != "forged"


def test_download_link_needs_no_credentials(ouro, track):
    created = track.add(
        ouro.files.create(
            name=track.name("download"), visibility="private", file_content=CIF.encode(), file_name="fe.cif"
        )
    )
    link = ouro.assets.create_download_url(str(created.id))
    assert link["asset_type"] == "file"
    assert link["file_name"].endswith(".cif")
    assert link["expires_in"] > 0
    response = httpx.get(link["download_url"], follow_redirects=True)
    assert response.status_code == 200
    assert response.text == CIF

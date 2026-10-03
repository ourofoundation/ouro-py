from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from ouro import BadRequestError, OuroError
from ouro.models import Dataset

ROWS = [
    {"sample": "a", "score": 0.5, "count": 1, "passed": True, "measured_on": "2026-01-01"},
    {"sample": "b", "score": 0.9, "count": 2, "passed": False, "measured_on": "2026-01-02"},
    {"sample": "c", "score": None, "count": 3, "passed": True, "measured_on": "2026-01-03"},
]


@pytest.fixture(scope="module")
def dataset(ouro, track):
    return track.add(
        ouro.datasets.create(
            name=track.name("dataset"),
            visibility="private",
            data=ROWS,
            description="Integration test dataset",
        )
    )


def test_create_returns_dataset_model(dataset, me):
    assert isinstance(dataset, Dataset)
    assert dataset.asset_type == "dataset"
    assert dataset.visibility == "private"
    assert dataset.user_id == me.user_id
    assert dataset.source == "api"
    assert dataset.attribution.originality == "original"
    assert dataset.row_ingest.inserted == len(ROWS)


def test_retrieve_matches_create(ouro, dataset):
    fetched = ouro.datasets.retrieve(str(dataset.id))
    assert fetched.id == dataset.id
    assert fetched.name == dataset.name
    assert fetched.description.text.startswith("Integration test dataset")


def test_query_full_table_returns_dataframe(ouro, dataset):
    df = ouro.datasets.query(str(dataset.id))
    assert isinstance(df, pd.DataFrame)
    assert sorted(df["sample"]) == ["a", "b", "c"]
    assert {"sample", "score", "count", "passed", "measured_on"} <= set(df.columns)
    assert pd.isna(df.loc[df["sample"] == "c", "score"].iloc[0])


def test_query_paginated(ouro, dataset):
    page = ouro.datasets.list_rows(str(dataset.id), limit=2)
    assert len(page) == 2
    assert page.has_more is True
    rest = ouro.datasets.list_rows(str(dataset.id), limit=2, offset=2)
    assert len(rest) == 1 and rest.has_more is False


def test_query_sql(ouro, dataset):
    df = ouro.datasets.query(
        str(dataset.id),
        "select sample, count from {{table}} where count >= 2 order by count desc",
    )
    assert df["sample"].tolist() == ["c", "b"]


def test_query_sql_is_read_only(ouro, dataset):
    with pytest.raises(OuroError):
        ouro.datasets.query(str(dataset.id), "delete from {{table}}")
    assert len(ouro.datasets.query(str(dataset.id))) == len(ROWS)


def test_list_rows_argument_validation(ouro, dataset):
    with pytest.raises(ValueError):
        ouro.datasets.list_rows(str(dataset.id), limit=0)
    with pytest.raises(ValueError):
        ouro.datasets.list_rows(str(dataset.id), offset=-1)


def test_schema_and_stats(ouro, dataset):
    schema = {c.name: c.type for c in ouro.datasets.schema(str(dataset.id))}
    assert schema["sample"] == "text"
    assert schema["count"] == "bigint"
    assert schema["passed"] == "boolean"
    assert "double" in schema["score"]
    stats = ouro.datasets.stats(str(dataset.id))
    assert stats.count == len(ROWS)


def test_create_from_dataframe_with_timestamps_and_index(ouro, track):
    df = pd.DataFrame(
        {
            "t": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-02T12:30:00Z"]),
            "big": [2**40, 2**41],
        },
        index=pd.Index(["x", "y"], name="key"),
    )
    created = track.add(
        ouro.datasets.create(name=track.name("frame"), visibility="private", data=df)
    )
    back = ouro.datasets.query(str(created.id)).sort_values("key")
    assert back["key"].tolist() == ["x", "y"]
    assert back["big"].tolist() == [2**40, 2**41]
    assert isinstance(back["t"].iloc[0], dt.date)


def test_floats_keep_double_precision(ouro, track):
    value = 1.23456789012345
    ds = track.add(ouro.datasets.create(name=track.name("floats"), visibility="private", data=[{"f": value}]))
    assert ouro.datasets.query(str(ds.id))["f"].iloc[0] == value


def test_update_append_overwrite_upsert(ouro, track):
    ds = track.add(
        ouro.datasets.create(
            name=track.name("modes"),
            visibility="private",
            data=[{"id": 1, "v": "one"}, {"id": 2, "v": "two"}],
        )
    )
    ds_id = str(ds.id)

    appended = ouro.datasets.update(ds_id, data=[{"id": 3, "v": "three"}])
    assert appended.row_ingest.inserted == 1
    assert len(ouro.datasets.query(ds_id)) == 3

    ouro.datasets.update(ds_id, data=[{"id": 2, "v": "TWO"}], data_mode="upsert")
    rows = ouro.datasets.query(ds_id).set_index("id")["v"].to_dict()
    assert rows == {1: "one", 2: "TWO", 3: "three"}

    ouro.datasets.update(ds_id, data=[{"id": 9, "v": "only"}], data_mode="overwrite")
    assert ouro.datasets.query(ds_id)["v"].tolist() == ["only"]


def test_update_metadata_only(ouro, dataset):
    updated = ouro.datasets.update(str(dataset.id), description="Updated description")
    assert updated.description.text.startswith("Updated description")
    assert updated.name == dataset.name


def test_large_upload_is_batched(ouro, track):
    rows = [{"i": i, "payload": "x" * 200} for i in range(40_000)]
    ds = track.add(ouro.datasets.create(name=track.name("large"), visibility="private", data=rows))
    assert ouro.datasets.stats(str(ds.id)).count == len(rows)
    fetched = ouro.datasets.query(str(ds.id))
    assert sorted(fetched["i"]) == list(range(len(rows)))


def test_column_lifecycle(ouro, track):
    ds = track.add(
        ouro.datasets.create(
            name=track.name("columns"),
            visibility="private",
            data=[{"name": "a", "status": "todo"}],
        )
    )
    ds_id = str(ds.id)
    ouro.datasets.add_column(ds_id, "notes", type="text")
    ouro.datasets.update_column(ds_id, "notes", new_name="comments")
    ouro.datasets.update_column(ds_id, "status", enum_values=["todo", "done"])
    schema = {c.name: c for c in ouro.datasets.schema(ds_id)}
    assert "comments" in schema and "notes" not in schema
    assert schema["status"].semantic_type == "enum"
    assert schema["status"].enum_values == ["todo", "done"]

    with pytest.raises(OuroError):
        ouro.datasets.update(ds_id, data=[{"name": "b", "status": "bogus"}])

    ouro.datasets.drop_column(ds_id, "comments")
    assert "comments" not in {c.name for c in ouro.datasets.schema(ds_id)}


def test_enum_columns_on_create(ouro, track):
    ds = track.add(
        ouro.datasets.create(
            name=track.name("enums"),
            visibility="private",
            data=[{"state": "open"}, {"state": "closed"}],
            enum_columns={"state": ["open", "closed"]},
        )
    )
    state = next(c for c in ouro.datasets.schema(str(ds.id)) if c.name == "state")
    assert state.enum_values == ["open", "closed"]


def test_reference_columns_resolve(ouro, track, dataset):
    ds = track.add(
        ouro.datasets.create(
            name=track.name("refs"),
            visibility="private",
            data=[{"label": "source", "source_id": str(dataset.id)}],
            refs={"source_id": "dataset"},
        )
    )
    ref = next(c for c in ouro.datasets.schema(str(ds.id)) if c.name == "source_id")
    assert ref.semantic_type == "reference"

    page = ouro.datasets.list_rows(str(ds.id), limit=10, resolve_refs=True)
    resolved = page.resolved_refs["source_id"][str(dataset.id)]
    assert resolved.name == dataset.name


def test_reference_to_missing_asset_is_skipped_with_warning(ouro, track):
    import uuid

    ds = track.add(
        ouro.datasets.create(
            name=track.name("badrefs"),
            visibility="private",
            data=[{"source_id": str(uuid.uuid4())}],
            refs={"source_id": "asset"},
        )
    )
    assert ds.row_ingest.skipped == 1
    assert ds.ingest_warning


def test_views(ouro, dataset):
    view = ouro.datasets.create_view(
        str(dataset.id),
        name="Score by sample",
        sql_query="select sample, score from {{table}} order by sample",
        config={"type": "bar", "category": {"dataKey": "sample"}, "series": [{"dataKey": "score"}]},
    )
    assert any(v.id == view.id for v in ouro.datasets.list_views(str(dataset.id)))
    updated = ouro.datasets.update_view(str(dataset.id), str(view.id), name="Renamed view")
    assert updated.name == "Renamed view"
    ouro.datasets.delete_view(str(dataset.id), str(view.id))
    assert all(v.id != view.id for v in ouro.datasets.list_views(str(dataset.id)))


def test_create_validation(ouro):
    with pytest.raises(ValueError):
        ouro.datasets.create(name="x", visibility="private", data=[])
    with pytest.raises(ValueError):
        ouro.datasets.create(name="x", visibility="private", data=[{"a": 1}], refs={"b": "asset"})


def test_list_finds_dataset(ouro, dataset):
    found = ouro.datasets.list(scope="personal", limit=50, sort="recent")
    assert all(isinstance(d, Dataset) for d in found)
    assert any(d.id == dataset.id for d in found)


def test_delete_dry_run(ouro, dataset):
    summary = ouro.datasets.delete(str(dataset.id), dry_run=True)
    assert summary.dry_run is True
    assert summary.id == dataset.id
    assert ouro.datasets.retrieve(str(dataset.id)).id == dataset.id


def test_bad_visibility_is_rejected(ouro, track):
    with pytest.raises((BadRequestError, OuroError)):
        track.add(ouro.datasets.create(name=track.name("badvis"), visibility="secret", data=[{"a": 1}]))

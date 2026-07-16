#!/usr/bin/env python3
"""Collect calculated properties for every CIF on Ouro into a reference dataset.

"Calculated properties" are not stored on the file itself. They live on the
``response`` payloads of successful route actions that used the CIF as an
input (ALIGNN predictions, e_above_hull, relaxation energies, phonons, …).

Pattern for agents (ouro-py skill):

1. ``ouro.files.search(extension="cif", scope="all", limit=None)``
2. ``ouro.assets.actions(file_id, role="input", status="success")``
3. Flatten scalar fields from ``action.response`` into long-form rows
4. ``ouro.datasets.create(..., refs={"file_id": "file", "action_id": "action",
   "route_id": "route"})``

Usage::

    # Dry run (no dataset create): collect + print summary
    python examples/build_cif_properties_dataset.py --dry-run

    # Create a public dataset on #materials-science
    python examples/build_cif_properties_dataset.py \\
        --org-id 00000000-0000-0000-0000-000000000000 \\
        --team-id 01956d7e-7f02-7715-9a89-1f847181e199

Checkpointing: progress is written to ``--checkpoint`` as JSONL so a
sandbox timeout can resume without redoing finished CIFs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Optional

from ouro import Ouro

# Nested / asset payloads — keep scalars only.
_SKIP_RESPONSE_KEYS = {
    "file",
    "files",
    "dataset",
    "post",
    "html",
    "plot",
    "image",
    "decomposition",
    "input_symmetry",
    "output_symmetry",
    "same_composition_entries",
    "lowest_energy_at_composition",
    "user_contribution_ids",
    "band_structure",
    "phonon_band_structure",
}


def _log(msg: str) -> None:
    print(msg, flush=True)


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _route_name(action: Any) -> Optional[str]:
    route = getattr(action, "route", None)
    if isinstance(route, dict):
        return route.get("name")
    return None


def _extract_property_rows(
    *,
    file_id: str,
    file_name: str,
    action: Any,
) -> list[dict[str, Any]]:
    """Turn one action.response into zero or more long-form property rows."""
    response = action.response
    if not isinstance(response, dict) or not response:
        return []

    action_id = _as_str(getattr(action, "id", None))
    route_id = _as_str(getattr(action, "route_id", None))
    route = _route_name(action)
    model = response.get("model")
    unit = response.get("unit")

    base = {
        "file_id": file_id,
        "file_name": file_name,
        "action_id": action_id,
        "route_id": route_id,
        "route_name": route,
        "model": _as_str(model) if model is not None else None,
    }

    rows: list[dict[str, Any]] = []

    # ALIGNN-style: explicit property + prediction
    if "property" in response and "prediction" in response:
        pred = response.get("prediction")
        rows.append(
            {
                **base,
                "property_name": _as_str(response.get("property")),
                "value_numeric": float(pred)
                if isinstance(pred, (int, float))
                else None,
                "value_text": None
                if isinstance(pred, (int, float))
                else _as_str(pred),
                "unit": _as_str(unit) if unit is not None else None,
            }
        )
        return rows

    # Generic: one row per scalar field
    for key, value in response.items():
        if key in _SKIP_RESPONSE_KEYS or key in {"model", "unit"}:
            continue
        if isinstance(value, bool):
            rows.append(
                {
                    **base,
                    "property_name": key,
                    "value_numeric": 1.0 if value else 0.0,
                    "value_text": "true" if value else "false",
                    "unit": _as_str(unit) if unit is not None else None,
                }
            )
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            rows.append(
                {
                    **base,
                    "property_name": key,
                    "value_numeric": float(value),
                    "value_text": None,
                    "unit": _as_str(unit) if unit is not None else None,
                }
            )
        elif isinstance(value, str):
            rows.append(
                {
                    **base,
                    "property_name": key,
                    "value_numeric": None,
                    "value_text": value,
                    "unit": _as_str(unit) if unit is not None else None,
                }
            )
        # dict / list / None → skipped
    return rows


def _load_done_ids(checkpoint: Path) -> set[str]:
    done: set[str] = set()
    if not checkpoint.exists():
        return done
    with checkpoint.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            fid = row.get("_file_id")
            if fid:
                done.add(fid)
    return done


def _append_checkpoint(checkpoint: Path, file_id: str, rows: list[dict]) -> None:
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    with checkpoint.open("a") as fh:
        fh.write(
            json.dumps({"_file_id": file_id, "rows": rows}, default=str) + "\n"
        )


def _read_all_rows(checkpoint: Path) -> list[dict]:
    rows: list[dict] = []
    if not checkpoint.exists():
        return rows
    with checkpoint.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            rows.extend(payload.get("rows") or [])
    return rows


def _process_file(ouro: Ouro, file_obj: Any) -> tuple[str, list[dict]]:
    file_id = str(file_obj.id)
    file_name = getattr(file_obj, "name", None) or file_id
    try:
        bundle = ouro.assets.actions(
            file_id, role="input", status="success", include_response=True
        )
    except Exception as exc:  # noqa: BLE001 — resume-friendly
        print(f"  ! actions failed for {file_id}: {exc}", file=sys.stderr, flush=True)
        return file_id, []

    actions = bundle.get("as_input") or []
    rows: list[dict] = []
    for action in actions:
        rows.extend(
            _extract_property_rows(
                file_id=file_id, file_name=file_name, action=action
            )
        )
    return file_id, rows


def collect_properties(
    ouro: Ouro,
    *,
    checkpoint: Path,
    workers: int = 16,
    limit: Optional[int] = None,
) -> list[dict]:
    search_limit = limit  # None → fetch all (SDK paginates internally)
    _log(
        f"Searching for CIF files (extension=cif, scope=all, "
        f"limit={search_limit!r})…"
    )
    t0 = time.time()
    cifs = ouro.files.search(extension="cif", scope="all", limit=search_limit)
    _log(f"  found {len(cifs)} CIFs in {time.time() - t0:.1f}s")

    done = _load_done_ids(checkpoint)
    todo = [f for f in cifs if str(f.id) not in done]
    _log(f"  already checkpointed: {len(done)}; remaining: {len(todo)}")

    # One client is shared across threads; httpx Client is generally fine for
    # concurrent GETs with a connection pool, but we keep workers modest.
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process_file, ouro, f): f for f in todo}
        for fut in as_completed(futures):
            file_id, rows = fut.result()
            _append_checkpoint(checkpoint, file_id, rows)
            completed += 1
            if completed % 100 == 0 or completed == len(todo):
                _log(
                    f"  processed {completed}/{len(todo)} "
                    f"({len(rows)} props on last)"
                )

    all_rows = _read_all_rows(checkpoint)
    _log(
        f"Collected {len(all_rows)} property rows across "
        f"{len(done) + len(todo)} CIFs"
    )
    return all_rows


def create_dataset(
    ouro: Ouro,
    rows: list[dict],
    *,
    org_id: str,
    team_id: str,
    name: str,
    visibility: str,
    description: str,
    batch_size: int = 200,
) -> Any:
    """Create a ref-typed dataset, uploading rows in small batches.

    Backend ref validation resolves ids with PostgREST ``.in(...)`` per batch.
    Large batches can exceed the filter URL limit and mark every id as missing,
    skipping the whole batch. Keep batches modest when many unique action/file
    ids are present.
    """
    if not rows:
        raise SystemExit("No property rows to upload — nothing to create.")

    skip_props = {
        "cache_key",
        "collinear_cache_key",
        "tb2j_scf_dir",
        "tb2j_nscf_dir",
        "status",
        "method",
    }
    filtered = [
        row
        for row in rows
        if row.get("property_name") not in skip_props
        and not str(row.get("property_name") or "").startswith("tb2j_")
    ]
    _log(
        f"Creating dataset {name!r}: {len(filtered)} rows "
        f"(filtered {len(rows) - len(filtered)} noise rows), "
        f"batch_size={batch_size}"
    )

    refs = {
        "file_id": "file",
        "action_id": "action",
        "route_id": "route",
    }
    first, rest = filtered[:batch_size], filtered[batch_size:]
    dataset = ouro.datasets.create(
        name=name,
        visibility=visibility,
        description=description,
        data=first,
        org_id=org_id,
        team_id=team_id,
        refs=refs,
    )
    _log(f"  dataset id: {dataset.id}")
    _log(f"  first batch ingest: {getattr(dataset, 'row_ingest', None)}")
    warning = getattr(dataset, "ingest_warning", None)
    if warning:
        _log(f"  first batch warning: {warning}")

    inserted = int((getattr(dataset, "row_ingest", None) or {}).get("inserted") or 0)
    skipped = int((getattr(dataset, "row_ingest", None) or {}).get("skipped") or 0)

    for i in range(0, len(rest), batch_size):
        chunk = rest[i : i + batch_size]
        updated = None
        last_exc: Exception | None = None
        for attempt in range(1, 6):
            try:
                updated = ouro.datasets.update(
                    str(dataset.id),
                    data=chunk,
                    data_mode="append",
                )
                break
            except Exception as exc:  # noqa: BLE001 — retry transient pool/timeouts
                last_exc = exc
                wait = min(2**attempt, 30)
                _log(
                    f"  append failed at offset {i} "
                    f"(attempt {attempt}/5): {exc}; sleeping {wait}s"
                )
                time.sleep(wait)
        if updated is None:
            raise RuntimeError(
                f"Failed to append rows at offset {i} after retries"
            ) from last_exc

        ingest = getattr(updated, "row_ingest", None) or {}
        inserted += int(ingest.get("inserted") or 0)
        skipped += int(ingest.get("skipped") or 0)
        if (i // batch_size + 1) % 10 == 0 or i + batch_size >= len(rest):
            _log(
                f"  appended {min(i + batch_size, len(rest))}/{len(rest)} "
                f"(running inserted={inserted} skipped={skipped})"
            )
        warn = getattr(updated, "ingest_warning", None)
        if warn:
            _log(f"  append warning: {warn}")

    _log(f"  final ingest totals: inserted={inserted} skipped={skipped}")
    return dataset


def summarize(rows: Iterable[dict]) -> None:
    from collections import Counter

    props = Counter()
    routes = Counter()
    files = set()
    for row in rows:
        props[row.get("property_name")] += 1
        routes[row.get("route_name")] += 1
        files.add(row.get("file_id"))
    _log(f"Unique files with properties: {len(files)}")
    _log("Top properties:")
    for name, n in props.most_common(15):
        _log(f"  {n:6d}  {name}")
    _log("Top routes:")
    for name, n in routes.most_common(10):
        _log(f"  {n:6d}  {name}")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("cif_properties_checkpoint.jsonl"),
        help="JSONL checkpoint path (resume-safe)",
    )
    p.add_argument("--workers", type=int, default=16)
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of CIFs (for testing)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect + summarize only; do not create a dataset",
    )
    p.add_argument(
        "--from-checkpoint",
        action="store_true",
        help="Skip CIF/action collection; build dataset from existing checkpoint",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Row upload batch size (keep small when using many refs)",
    )
    p.add_argument("--org-id", default=os.environ.get("OURO_ORG_ID"))
    p.add_argument("--team-id", default=os.environ.get("OURO_TEAM_ID"))
    p.add_argument(
        "--name",
        default="CIF calculated properties",
    )
    p.add_argument("--visibility", default="public")
    p.add_argument(
        "--description",
        default=(
            "Long-form calculated properties extracted from successful route "
            "actions that used each CIF as an input. Columns file_id / "
            "action_id / route_id are Ouro references."
        ),
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    ouro = Ouro()

    if args.from_checkpoint:
        if not args.checkpoint.exists():
            raise SystemExit(f"Checkpoint not found: {args.checkpoint}")
        _log(f"Loading rows from checkpoint {args.checkpoint}…")
        rows = _read_all_rows(args.checkpoint)
        _log(f"  loaded {len(rows)} rows")
    else:
        rows = collect_properties(
            ouro,
            checkpoint=args.checkpoint,
            workers=args.workers,
            limit=args.limit,
        )
    summarize(rows)

    if args.dry_run:
        _log("Dry run — skipping dataset create.")
        return 0

    if not args.org_id or not args.team_id:
        raise SystemExit(
            "Pass --org-id and --team-id (or set OURO_ORG_ID / OURO_TEAM_ID) "
            "to create the dataset."
        )

    dataset = create_dataset(
        ouro,
        rows,
        org_id=args.org_id,
        team_id=args.team_id,
        name=args.name,
        visibility=args.visibility,
        description=args.description,
        batch_size=args.batch_size,
    )
    schema = ouro.datasets.schema(str(dataset.id))
    _log("Schema semantic types:")
    for col in schema:
        _log(
            f"  {col.get('column_name') or col.get('name')}: "
            f"type={col.get('data_type') or col.get('type')} "
            f"semantic={col.get('semantic_type')} "
            f"ref_kind={col.get('ref_kind')} "
            f"asset_type={col.get('asset_type')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

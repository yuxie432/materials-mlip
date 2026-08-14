"""Targeted recovery of the vasprun ``parameters`` (resolved/effective) block — metadata-only.

The first Zenodo harvest parsed ``vasprun.xml`` before :func:`parse._calc_parameters` began
storing VASP's RESOLVED ``parameters`` block, so the ``pymatgen.Vasprun`` calcs carry only the
user ``incar``: a tag left at default is absent, integer tags with no stable default (``ISIF``)
are unrecoverable by the resolver, and the cutoff can hide under ``ENMAX``. :mod:`param_resolver`
mitigates this with documented defaults / run_type inversion, but that is a *guess*; the
authoritative fix is VASP's own resolved values, which live in the vasprun header.

This module applies that fix to the *already-built* dataset **without rebuilding shards** —
mirroring :mod:`outcar_recover`. Three steps, each resumable and idempotent:

1. :func:`build_vasprun_keeplist` — scan ``metadata.jsonl`` for records holding a
   ``pymatgen.Vasprun`` calc and emit their ``keep.jsonl`` entries.
2. **Re-fetch** — the ordinary ``fetch`` on that keep-list (targeted ZIP member fetch streams
   just the ``vasprun.xml`` out of a ``.zip``; ``.tar`` falls back to a whole download).
3. :func:`refresh_vasprun_metadata` — read each re-fetched vasprun **header only**
   (:func:`vasprun_params.parse_vasprun_parameters`, cheap regardless of trajectory length) and
   write the authoritative ``parameters`` block into that record's ``calc_parameters``. ONLY
   ``parameters`` is added and the now-stale ``resolved`` cache dropped;
   ``run_type``/``functional``/``incar``/``kpoints``/``potcar``/… stay byte-identical (all
   already pymatgen-authoritative), as do ``calc_id``/``frame_ids``/``shards`` — so ``verify``
   still passes and no shard is opened.

Run ``enrich-metadata`` afterwards to regenerate ``resolved`` (now reading the authoritative
``parameters`` — e.g. ISIF becomes ``source="parameters"`` instead of unresolved).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

from .manifest import JsonlWriter, read_jsonl
from .store import DatasetLock
from .vasprun_params import parse_vasprun_parameters

logger = logging.getLogger(__name__)

# Distinct from enrich's ``metadata.jsonl.bak`` and the OUTCAR refresh's snapshot, so all three
# pre-mutation states survive: this captures the state just before the FIRST vasprun refresh.
_REFRESH_BACKUP = "metadata.jsonl.bak.pre_vasprun_refresh"

VASPRUN_PARSER = "pymatgen.Vasprun"


def _vasprun_records(metadata_path: Path, only_missing: bool) -> tuple[set[str], set[str], int]:
    """Scan ``metadata.jsonl`` once for the vasprun calcs to recover.

    Returns ``(recids, vasprun_calc_ids, n)``. ``only_missing`` restricts to calcs whose
    ``calc_parameters`` still lacks a populated ``parameters`` block (i.e. not yet refreshed),
    so an interrupted campaign targets just the remainder.
    """
    recids: set[str] = set()
    calc_ids: set[str] = set()
    n = 0
    for rec in read_jsonl(metadata_path):
        if rec.get("parser") != VASPRUN_PARSER:
            continue
        if only_missing and (rec.get("calc_parameters") or {}).get("parameters"):
            continue
        cid = rec.get("calc_id")
        recid = (rec.get("provenance") or {}).get("record_id")
        if cid:
            calc_ids.add(cid)
            n += 1
        if recid:
            recids.add(str(recid))
    return recids, calc_ids, n


def build_vasprun_keeplist(dataset_dir: str | Path, keep_path: str | Path,
                           out_path: str | Path, only_missing: bool = False) -> dict:
    """Emit a slim keep-list of just the records that hold a ``pymatgen.Vasprun`` calc.

    Mirrors :func:`outcar_recover.build_outcar_keeplist`: reads which record_ids need recovery
    from ``metadata.jsonl`` and writes their full ``keep.jsonl`` entries (with file URLs) to
    ``out_path``. A record absent from ``keep_path`` is reported under ``missing_from_keep``.
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}

    recids, _calc_ids, n_calcs = _vasprun_records(metadata_path, only_missing)
    keep_by_recid = {str(rec.get("recid")): rec for rec in read_jsonl(keep_path)}

    written = 0
    bytes_total = 0
    missing: list[str] = []
    with JsonlWriter(out_path) as out:
        for recid in sorted(recids):
            entry = keep_by_recid.get(recid)
            if entry is None:
                missing.append(recid)
                continue
            out.write(entry)
            written += 1
            bytes_total += int(entry.get("bytes_total") or 0)
    if missing:
        logger.warning("%d vasprun recid(s) absent from %s — cannot re-fetch without their "
                       "file URLs: %s%s", len(missing), keep_path, missing[:10],
                       " ..." if len(missing) > 10 else "")
    return {
        "ok": True,
        "dataset_dir": str(dataset_dir),
        "out_path": str(out_path),
        "only_missing": only_missing,
        "vasprun_records_targeted": len(recids),
        "records_written": written,
        "vasprun_calcs_targeted": n_calcs,
        "gib_to_refetch": round(bytes_total / 2**30, 2),
        "missing_from_keep": missing,
    }


def refresh_vasprun_metadata(dataset_dir: str | Path, fetched: str | Path,
                             raw_dir: str | Path, dry_run: bool = False,
                             backup: bool = True, only_missing: bool = False) -> dict:
    """Write the authoritative ``parameters`` block into re-fetched vasprun calcs, in place.

    For every calc unit in ``fetched`` whose ``calc_id`` names a ``pymatgen.Vasprun`` record,
    the vasprun HEADER is parsed (:func:`vasprun_params.parse_vasprun_parameters`) into the
    resolved ``parameters`` block, and that record's ``calc_parameters["parameters"]`` is set
    (its stale ``resolved`` cache dropped — regenerate with ``enrich-metadata``). Everything the
    metadata↔shard bijection depends on — ``calc_id``/``frame_ids``/``shards`` — and every other
    ``calc_parameters`` field (``run_type``/``functional``/``incar``/…) is left byte-identical,
    and no shard is opened, so ``verify`` still passes.

    Atomic (temp → ``os.replace``) under the dataset ``.parse.lock`` with a one-time
    ``metadata.jsonl.bak.pre_vasprun_refresh`` snapshot. Idempotent and resumable; ``dry_run``
    reports what WOULD change and writes nothing.
    """
    from .parse import _calc_id, _resolve  # lazy: only this path needs parse's heavy deps

    dataset_dir, raw_dir = Path(dataset_dir), Path(raw_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}
    if not Path(fetched).is_file():
        return {"ok": False, "error": f"no fetched manifest at {fetched}"}

    # Pass 0: which calc_ids are vasprun-parsed (the only ones we may touch).
    _recids, vasprun_calc_ids, n_target = _vasprun_records(metadata_path, only_missing)

    # Pass 1: parse each re-fetched vasprun's header -> its resolved parameters block.
    update: dict[str, dict] = {}
    n_units = n_parse_failed = 0
    for rec in read_jsonl(fetched):
        base_meta = {"provenance": rec["provenance"],
                     "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
        for unit in rec.get("calc_units", []):
            unit_abs = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
            if "vasprun" not in unit_abs:
                continue
            calc_id = _calc_id(unit_abs, base_meta)
            if calc_id not in vasprun_calc_ids or calc_id in update:
                continue
            n_units += 1
            params = parse_vasprun_parameters(unit_abs["vasprun"])
            if params is None:
                n_parse_failed += 1
                logger.warning("vasprun parameters unreadable for %s (%s)", calc_id,
                               unit_abs["vasprun"])
                continue
            update[calc_id] = params

    # Pass 2: rewrite metadata.jsonl, adding only the matched records' parameters block.
    n_meta = n_refreshed = 0

    def _apply(record: dict) -> dict:
        nonlocal n_refreshed
        cid = record.get("calc_id")
        if cid in update and record.get("parser") == VASPRUN_PARSER:
            cp = record.get("calc_parameters")
            if isinstance(cp, dict):
                cp["parameters"] = update[cid]
                cp.pop("resolved", None)     # stale (guessed) cache; regenerate via enrich
                n_refreshed += 1
        return record

    if dry_run:
        for record in read_jsonl(metadata_path):
            n_meta += 1
            _apply(dict(record))  # count only; discard
    else:
        with DatasetLock(dataset_dir):
            tmp = metadata_path.parent / (metadata_path.name + ".vasprun.tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                for record in read_jsonl(metadata_path):
                    n_meta += 1
                    fh.write(json.dumps(_apply(record)) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            bak = metadata_path.parent / _REFRESH_BACKUP
            if backup and not bak.exists():           # one-time pre-refresh snapshot
                shutil.copy2(metadata_path, bak)
            os.replace(tmp, metadata_path)            # atomic swap into place

    return {
        "ok": True,
        "dataset_dir": str(dataset_dir),
        "dry_run": dry_run,
        "vasprun_calcs_targeted": n_target,       # vasprun calcs needing recovery (pass 0)
        "vasprun_units_in_fetched": n_units,      # vasprun calcs found staged in the re-fetch
        "parse_failed": n_parse_failed,
        "metadata_records": n_meta,
        "records_refreshed": n_refreshed,
        # vasprun calcs not yet re-fetched (still to do) — campaign progress.
        "not_yet_refetched": max(0, n_target - len(update)),
    }

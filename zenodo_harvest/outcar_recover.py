"""Targeted recovery of the OUTCAR metadata gap — re-fetch, re-parse (header only), re-write.

The first Zenodo harvest parsed OUTCAR-only calcs through ASE's ``vasp-out`` reader, which
recovers the ionic *trajectory* (→ the extxyz shards) but not the *parameters* — so 93,043
calcs (25.6 % of frames) landed with ``run_type``/``functional``/INCAR all ``null``, unusable
for a functional-consistent MLIP training subset. :mod:`outcar_params` now reads those from
the OUTCAR **header**; this module applies that fix to the *already-built* dataset **without
rebuilding the shards** — because the physical data (positions/energy/forces/stress) is
already correct and unchanged; only the calc-level ``calc_parameters`` was impoverished.

Three steps, each resumable and idempotent:

1. :func:`build_outcar_keeplist` — scan ``metadata.jsonl`` for records that still hold an
   OUTCAR-parsed calc and emit the matching ``keep.jsonl`` entries as a slim keep-list. This
   is the "very targeted" part: only the ~230 records with an OUTCAR calc (not the whole
   keep-list) get re-fetched.
2. **Re-fetch** — run the ordinary ``fetch`` on that keep-list (nothing new here): targeted
   ZIP member fetch is ON by default, so a ``.zip`` record streams only its ``OUTCAR`` files
   over HTTP Range instead of downloading the whole archive; ``.tar``/``.rar``/``.7z`` fall
   back to a whole download. Zenodo records are immutable per version, so a re-fetch stages
   byte-identical files at byte-identical relative paths → byte-identical ``calc_id``\\ s.
3. :func:`refresh_outcar_metadata` — for each re-fetched OUTCAR calc, parse its OUTCAR header
   and **overwrite only that record's ``calc_parameters``** in ``metadata.jsonl`` (in place,
   atomic, one-time backup). ``calc_id``/``frame_ids``/``shards`` are left byte-identical, so
   the metadata↔shard bijection ``verify`` checks still holds and the shards are never touched.

The join key is the ``calc_id``, derived here **exactly** as :mod:`parse` derives it (the
imported :func:`parse._calc_id`/:func:`parse._resolve`), so "which dataset record does this
re-fetched OUTCAR belong to?" is answered the same way parse decided what to write. Only calcs
whose *existing* metadata parser is ``ase.OUTCAR`` are refreshed — a vasprun/vaspout calc that
merely has an OUTCAR sibling keeps its (already rich) vasprun parameters untouched.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any

from .manifest import JsonlWriter, read_jsonl
from .outcar_params import _classify_inputs, classify_run_type, parse_outcar_header
from .store import DatasetLock

logger = logging.getLogger(__name__)

# Distinct from enrich's ``metadata.jsonl.bak`` (the pristine pre-enrich original) so BOTH
# snapshots survive: this captures the state just before the FIRST OUTCAR refresh.
_REFRESH_BACKUP = "metadata.jsonl.bak.pre_outcar_refresh"
# One-time snapshot taken by :func:`reclassify_outcar_run_types` before it rewrites labels.
_RECLASSIFY_BACKUP = "metadata.jsonl.bak.pre_reclassify"

OUTCAR_PARSER = "ase.OUTCAR"
# Marks a calc_parameters block produced by the OUTCAR-header parser (:mod:`outcar_params`),
# i.e. one that carries the ``incar``/``parameters``/``potcar_symbols`` needed to re-classify
# run_type offline. Pre-recovery OUTCAR calcs (the old ASE path) lack it and are left alone.
_HEADER_PROVENANCE = "outcar_header"


def _outcar_records(metadata_path: Path, only_missing: bool) -> tuple[set[str], set[str], int]:
    """Scan ``metadata.jsonl`` once for the OUTCAR calcs to recover.

    Returns ``(recids, outcar_calc_ids, n_outcar_calcs)``: the set of record_ids holding at
    least one OUTCAR-parsed calc (what to re-fetch), the set of those calcs' ``calc_id``\\ s
    (what :func:`refresh_outcar_metadata` is allowed to overwrite), and the calc count.

    ``only_missing`` restricts to calcs that still lack a resolved functional
    (``calc_parameters.run_type is None``) — i.e. not yet refreshed — so an interrupted
    campaign can target just the remainder. With it False, every OUTCAR calc is targeted
    (a full, idempotent refresh).
    """
    recids: set[str] = set()
    calc_ids: set[str] = set()
    n = 0
    for rec in read_jsonl(metadata_path):
        if rec.get("parser") != OUTCAR_PARSER:
            continue
        if only_missing and (rec.get("calc_parameters") or {}).get("run_type") is not None:
            continue
        cid = rec.get("calc_id")
        recid = (rec.get("provenance") or {}).get("record_id")
        if cid:
            calc_ids.add(cid)
            n += 1
        if recid:
            recids.add(str(recid))
    return recids, calc_ids, n


def build_outcar_keeplist(dataset_dir: str | Path, keep_path: str | Path,
                          out_path: str | Path, only_missing: bool = False) -> dict:
    """Emit a slim keep-list of just the records that still hold an OUTCAR-parsed calc.

    Reads which record_ids need recovery from ``metadata.jsonl`` (:func:`_outcar_records`) and
    writes their full ``keep.jsonl`` entries (fetch needs each entry's ``files`` list of
    download URLs) to ``out_path``. A record whose OUTCAR calc is in the dataset but whose
    recid is absent from ``keep_path`` is reported under ``missing_from_keep`` and skipped —
    it cannot be re-fetched without its file URLs (re-run ``discover``/``triage`` for it, or
    pass a keep-list that contains it).

    ``only_missing`` forwards to :func:`_outcar_records`. Returns a summary (records written,
    OUTCAR calcs targeted, GiB to re-fetch, any missing recids).
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}

    recids, _calc_ids, n_calcs = _outcar_records(metadata_path, only_missing)
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
        logger.warning("%d OUTCAR recid(s) absent from %s — cannot re-fetch without their "
                       "file URLs: %s%s", len(missing), keep_path, missing[:10],
                       " ..." if len(missing) > 10 else "")
    return {
        "ok": True,
        "dataset_dir": str(dataset_dir),
        "out_path": str(out_path),
        "only_missing": only_missing,
        "outcar_records_targeted": len(recids),
        "records_written": written,
        "outcar_calcs_targeted": n_calcs,
        "gib_to_refetch": round(bytes_total / 2**30, 2),
        "missing_from_keep": missing,
    }


def _recompute_spin_availability(record: dict, spin_polarized: Any) -> None:
    """Refresh the two availability flags derived from spin polarisation, in place.

    Mirrors :func:`parse.parse_calc_unit`'s derivation exactly so a refreshed OUTCAR record's
    ``availability`` matches how a vasprun record's is computed: ``spin_density`` = charge
    density present AND spin-polarised; ``magnetization`` = the calc carries magnetization data
    (spin-polarised OR non-collinear — :func:`electronic.is_magnetic`). Per-atom magmoms are no
    longer stored, so this no longer keys off ``site_magmoms_present``.
    """
    from .electronic import is_magnetic

    sp = bool(spin_polarized)
    avail = record.get("availability")
    if not isinstance(avail, dict):
        return
    avail["spin_density"] = bool(avail.get("charge_density")) and sp
    avail["magnetization"] = is_magnetic(record.get("calc_parameters") or {})


def refresh_outcar_metadata(dataset_dir: str | Path, fetched: str | Path,
                            raw_dir: str | Path, dry_run: bool = False,
                            backup: bool = True, only_missing: bool = False) -> dict:
    """Overwrite the ``calc_parameters`` of re-fetched OUTCAR calcs, in place, shards untouched.

    ``fetched`` is the ``fetched.jsonl`` produced by re-fetching :func:`build_outcar_keeplist`'s
    keep-list; ``raw_dir`` is where those files were staged. For every calc unit in it whose
    ``calc_id`` names an OUTCAR-parsed record, the unit's OUTCAR header is parsed
    (:func:`outcar_params.parse_outcar_header`) into a full vasprun-schema ``calc_parameters``,
    and that record's block is replaced (its spin-derived availability flags recomputed).
    Everything the metadata↔shard bijection depends on — ``calc_id``, ``frame_ids``,
    ``shards`` — is left byte-identical, and no shard file is opened, so ``verify`` still passes
    afterwards.

    The write streams ``metadata.jsonl`` → a temp file → atomic ``os.replace`` under the
    dataset ``.parse.lock`` (so a concurrent parse can't corrupt it), keeping a one-time
    ``metadata.jsonl.bak.pre_outcar_refresh`` snapshot (distinct from enrich's ``.bak``).
    Idempotent and resumable: re-running with more of the dataset re-fetched refreshes the new
    records and rewrites the already-done ones identically. ``dry_run`` reports what WOULD
    change and writes nothing.

    After this, re-run ``enrich-metadata`` so the ``resolved`` cache picks up the authoritative
    ``parameters`` block (a refreshed OUTCAR calc flips ``incar_complete`` and its target tags
    become ``source="parameters"`` instead of ``unknown``).
    """
    from .parse import _calc_id, _resolve  # lazy: only this path needs parse's heavy deps

    dataset_dir, raw_dir = Path(dataset_dir), Path(raw_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}
    if not Path(fetched).is_file():
        return {"ok": False, "error": f"no fetched manifest at {fetched}"}

    # Pass 0: which calc_ids are OUTCAR-parsed (the only ones we may overwrite).
    _recids, outcar_calc_ids, n_target = _outcar_records(metadata_path, only_missing)

    # Pass 1: parse the OUTCAR header of each re-fetched OUTCAR calc -> new calc_parameters.
    update: dict[str, dict] = {}
    n_units = n_header_failed = 0
    for rec in read_jsonl(fetched):
        base_meta = {"provenance": rec["provenance"],
                     "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
        for unit in rec.get("calc_units", []):
            unit_abs = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
            if "outcar" not in unit_abs:
                continue
            calc_id = _calc_id(unit_abs, base_meta)
            if calc_id not in outcar_calc_ids or calc_id in update:
                continue
            n_units += 1
            cp = parse_outcar_header(unit_abs["outcar"])
            if cp is None:
                n_header_failed += 1
                logger.warning("OUTCAR header unreadable for %s (%s)", calc_id,
                               unit_abs["outcar"])
                continue
            update[calc_id] = cp

    # Pass 2: rewrite metadata.jsonl, replacing only the matched records' calc_parameters.
    n_meta = n_refreshed = 0

    def _apply(record: dict) -> dict:
        nonlocal n_refreshed
        cid = record.get("calc_id")
        if cid in update and record.get("parser") == OUTCAR_PARSER:
            cp = update[cid]
            record["calc_parameters"] = cp
            _recompute_spin_availability(record, cp.get("spin_polarized"))
            n_refreshed += 1
        return record

    if dry_run:
        for record in read_jsonl(metadata_path):
            n_meta += 1
            _apply(dict(record))  # count only; discard
    else:
        with DatasetLock(dataset_dir):
            tmp = metadata_path.parent / (metadata_path.name + ".outcar.tmp")
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
        "outcar_calcs_targeted": n_target,       # OUTCAR calcs needing recovery (pass 0)
        "outcar_units_in_fetched": n_units,      # OUTCAR calcs found staged in the re-fetch
        "header_parse_failed": n_header_failed,
        "metadata_records": n_meta,
        "records_refreshed": n_refreshed,
        # OUTCAR calcs not yet re-fetched (still to do) — campaign progress.
        "not_yet_refetched": max(0, n_target - len(update)),
    }


def _reclassify_one(cp: dict) -> tuple[str, str] | None:
    """Re-derive ``(run_type, functional)`` for a header-parsed ``calc_parameters`` — offline.

    Uses only the stored ``incar`` / ``parameters`` / ``potcar_symbols`` / ``hubbard_u`` (no
    OUTCAR file), running the CURRENT :func:`outcar_params.classify_run_type`, so a classifier
    bugfix (e.g. the boolean-``METAGGA`` → ``"TRUE"`` mislabel) can be applied to an
    already-built dataset without a re-fetch. ``ldau_on`` is reconstructed faithfully from the
    stored ``hubbard_u`` or a ``+U`` already present in the old ``run_type`` (the ``+U`` logic is
    untouched by the fix, so this reproduces the original ``+U`` decision exactly). Returns the
    new pair, or ``None`` if this block was not produced by the OUTCAR-header parser (nothing to
    reclassify — the old ASE path stored no such inputs).
    """
    if not isinstance(cp, dict) or cp.get("parsed_from") != _HEADER_PROVENANCE:
        return None
    incar = cp.get("incar") or {}
    eff = cp.get("parameters") or {}
    titels = cp.get("potcar_symbols") or []
    ldau_on = (cp.get("hubbard_u") is not None) or ("+U" in (cp.get("run_type") or ""))
    view = _classify_inputs(incar, eff, ldau_on)
    run_type = classify_run_type(view, list(titels))
    return run_type, run_type.split("+")[0]


def reclassify_outcar_run_types(dataset_dir: str | Path, dry_run: bool = False,
                                backup: bool = True) -> dict:
    """Re-derive ``run_type``/``functional`` for every OUTCAR-header calc from its STORED
    ``calc_parameters`` (metadata-only), applying the current classifier.

    This is the cheap corrective for a :func:`outcar_params.classify_run_type` bugfix landing
    *after* the 955-GiB OUTCAR recovery already ran: the OUTCAR raw files are purged, but the
    header's ``incar``/``parameters`` were persisted into ``calc_parameters``, so run_type can be
    recomputed with zero network and zero shard access. ONLY ``run_type``/``functional`` change;
    ``calc_id``/``frame_ids``/``shards`` are byte-identical, so ``verify`` still passes. Records
    whose recomputed label equals the stored one are written back unchanged (idempotent).

    Atomic (temp → ``os.replace``) under the dataset ``.parse.lock``, with a one-time
    ``metadata.jsonl.bak.pre_reclassify`` snapshot. Re-run ``enrich-metadata`` afterwards so any
    ``resolved`` cache keyed off the old ``run_type`` is refreshed. Returns a summary including a
    ``changes`` histogram of ``"old -> new"`` label transitions.
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}

    n_meta = n_header = n_changed = 0
    changes: dict[str, int] = {}

    def _apply(record: dict) -> dict:
        nonlocal n_header, n_changed
        if record.get("parser") != OUTCAR_PARSER:
            return record
        cp = record.get("calc_parameters")
        if not isinstance(cp, dict):
            return record
        new = _reclassify_one(cp)            # None unless this is a header-parsed block
        if new is None:
            return record
        n_header += 1
        new_rt, new_func = new
        old_rt = cp.get("run_type")
        if new_rt != old_rt or new_func != cp.get("functional"):
            key = f"{old_rt} -> {new_rt}"
            changes[key] = changes.get(key, 0) + 1
            cp["run_type"] = new_rt
            cp["functional"] = new_func
            n_changed += 1
        return record

    if dry_run:
        for record in read_jsonl(metadata_path):
            n_meta += 1
            _apply(dict(record))
    else:
        with DatasetLock(dataset_dir):
            tmp = metadata_path.parent / (metadata_path.name + ".reclassify.tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                for record in read_jsonl(metadata_path):
                    n_meta += 1
                    fh.write(json.dumps(_apply(record)) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            bak = metadata_path.parent / _RECLASSIFY_BACKUP
            if backup and not bak.exists():
                shutil.copy2(metadata_path, bak)
            os.replace(tmp, metadata_path)

    return {
        "ok": True,
        "dataset_dir": str(dataset_dir),
        "dry_run": dry_run,
        "metadata_records": n_meta,
        "outcar_header_calcs": n_header,
        "records_reclassified": n_changed,
        "changes": dict(sorted(changes.items(), key=lambda kv: -kv[1])),
    }

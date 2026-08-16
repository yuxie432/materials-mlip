"""Targeted recovery of per-calc ``availability`` flags — metadata-only, shards untouched.

The first Zenodo harvest recorded availability **per record** (each heavy-file flag OR'd across
every calc in the record) and from **filenames only**. Two opposite errors followed: an
*over-count* (one DOSCAR anywhere flagged every calc in the record) and an *under-count* (DOS /
eigenvalues / projected written straight into a ``vasprun.xml`` with no standalone
DOSCAR/EIGENVAL/PROCAR file were flagged False). The fetch + parse code now scopes availability
to each calc-unit directory (``fetch._calc_availability`` → per-calc ``calc_availability`` in
``fetched.jsonl``) and refines it with a cheap embedded-content probe of the vasprun/vaspout
(``parse._merge_embedded_availability``).

This module applies that corrected availability to the **already-built** dataset **without
rebuilding shards** — mirroring :mod:`outcar_recover` / :mod:`vasprun_recover`. Three steps,
each resumable and idempotent:

1. :func:`build_availability_keeplist` — every record holding a dataset calc (availability
   affects *all* calcs, not a parser subset), emitted as its ``keep.jsonl`` entry.
2. **Re-fetch** — the ordinary ``fetch`` (NEW code) on that keep-list; its ``fetched.jsonl`` now
   carries per-calc ``calc_availability`` and stages the vasprun/vaspout files for the probe.
3. :func:`refresh_availability_metadata` — for each re-fetched calc whose primary is **still
   staged**, recompute availability = per-calc filename flags ∪ embedded probe + the two
   spin-derived flags (from the record's own ``spin_polarized`` / ``site_magmoms_present``,
   exactly as :func:`parse.parse_calc_unit` does), and overwrite ONLY that record's
   ``availability``. ``calc_id`` / ``frame_ids`` / ``shards`` / ``calc_parameters`` / … are left
   byte-identical and no shard is opened, so ``verify`` still passes.

The **"primary still staged" filter** is what makes step 3 safe under the batched
fetch→refresh→purge loop (see ``scripts/csd3/46_availability_recover.sh``): a calc whose file was
purged by an earlier batch is skipped (its availability is already correct), so a resumed run
never recomputes availability from a missing file — which would silently drop the embedded
DOS/eigen signal. No metadata marker is needed; presence of the source file is the resume state.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

from .manifest import JsonlWriter, read_jsonl
from .store import DatasetLock

logger = logging.getLogger(__name__)

# Distinct from enrich's ``metadata.jsonl.bak`` and the OUTCAR/vasprun refresh snapshots, so
# every pre-mutation state survives independently: this captures the state just before the
# FIRST availability refresh.
_REFRESH_BACKUP = "metadata.jsonl.bak.pre_availability_refresh"


def _dataset_recids(metadata_path: Path) -> set[str]:
    """Every ``provenance.record_id`` that produced a dataset calc (availability affects all)."""
    recids: set[str] = set()
    for rec in read_jsonl(metadata_path):
        recid = (rec.get("provenance") or {}).get("record_id")
        if recid:
            recids.add(str(recid))
    return recids


def build_availability_keeplist(dataset_dir: str | Path, keep_path: str | Path,
                                out_path: str | Path) -> dict:
    """Emit the keep-list of every record that holds a dataset calc, to re-fetch for the probe.

    Unlike the OUTCAR/vasprun recoveries (which target a parser subset), availability is
    recomputed for *every* calc, so this selects every record present in ``metadata.jsonl`` and
    writes its full ``keep.jsonl`` entry (with file URLs). A record absent from ``keep_path`` is
    reported under ``missing_from_keep`` (cannot be re-fetched without its file URLs).
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}

    recids = _dataset_recids(metadata_path)
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
        logger.warning("%d recid(s) absent from %s — cannot re-fetch without their file URLs: "
                       "%s%s", len(missing), keep_path, missing[:10],
                       " ..." if len(missing) > 10 else "")
    return {
        "ok": True,
        "dataset_dir": str(dataset_dir),
        "out_path": str(out_path),
        "records_targeted": len(recids),
        "records_written": written,
        "gib_to_refetch": round(bytes_total / 2**30, 2),
        "missing_from_keep": missing,
    }


def refresh_availability_metadata(dataset_dir: str | Path, fetched: str | Path,
                                  raw_dir: str | Path, dry_run: bool = False,
                                  backup: bool = True) -> dict:
    """Overwrite the ``availability`` of re-fetched calcs, in place, shards untouched.

    ``fetched`` is the ``fetched.jsonl`` produced by re-fetching :func:`build_availability_keeplist`'s
    keep-list with the current code (so it carries per-calc ``calc_availability`` and staged
    vasprun/vaspout files); ``raw_dir`` is where those files were staged. For every calc unit in
    it whose ``calc_id`` names a dataset record **and whose primary file is still present on
    disk**, availability is recomputed exactly as :func:`parse.parse_calc_unit` does — the
    per-calc filename flags OR'd with the embedded vasprun/vaspout probe
    (:func:`parse._merge_embedded_availability`), plus ``spin_density``/``magnetization`` derived
    from that record's stored ``spin_polarized`` / ``site_magmoms_present``
    (:func:`outcar_recover._recompute_spin_availability`) — and that record's ``availability`` is
    replaced. ``calc_id`` / ``frame_ids`` / ``shards`` and every other field are byte-identical,
    and no shard is opened, so ``verify`` still passes.

    **Resume-safe by design:** a calc whose primary file is absent (purged by an earlier batch,
    or never staged) is SKIPPED, so the batched fetch→refresh→purge loop can be interrupted and
    resumed without ever recomputing availability from a missing file (which would wrongly drop
    the embedded DOS/eigen signal). No metadata marker is written; presence of the source file
    is the resume state.

    Atomic (temp → ``os.replace``) under the dataset ``.parse.lock`` with a one-time
    ``metadata.jsonl.bak.pre_availability_refresh`` snapshot. Idempotent; ``dry_run`` reports what
    WOULD change and writes nothing.
    """
    from .outcar_recover import _recompute_spin_availability
    from .parse import _calc_id, _merge_embedded_availability, _resolve

    dataset_dir, raw_dir = Path(dataset_dir), Path(raw_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}
    if not Path(fetched).is_file():
        return {"ok": False, "error": f"no fetched manifest at {fetched}"}

    # Pass 0: the set of dataset calc_ids we may touch (any parser — availability is universal).
    dataset_calc_ids: set[str] = {rec["calc_id"] for rec in read_jsonl(metadata_path)
                                  if rec.get("calc_id")}

    # Pass 1: recompute the heavy availability (filename flags ∪ embedded probe) for each
    # re-fetched calc whose PRIMARY IS STILL STAGED. Spin-derived flags are added in pass 2
    # (they need the record's own spin_polarized / site_magmoms_present).
    update: dict[str, dict] = {}
    n_present = n_absent = 0
    for rec in read_jsonl(fetched):
        base_meta = {"provenance": rec["provenance"],
                     "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
        cavs = rec.get("calc_availability")
        for idx, unit in enumerate(rec.get("calc_units", [])):
            unit_abs = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
            calc_id = _calc_id(unit_abs, base_meta)
            if calc_id not in dataset_calc_ids or calc_id in update:
                continue
            # Primary precedence must match _calc_id (vasprun > vaspout > outcar).
            primary = (unit_abs.get("vasprun") or unit_abs.get("vaspout")
                       or unit_abs.get("outcar"))
            if not primary or not os.path.exists(primary):
                n_absent += 1     # purged/absent -> already refreshed in an earlier batch; skip
                continue
            base_avail = (cavs[idx] if isinstance(cavs, list) and idx < len(cavs)
                          else rec.get("availability") or {})
            update[calc_id] = _merge_embedded_availability(
                base_avail, unit_abs.get("vasprun"), unit_abs.get("vaspout"))
            n_present += 1

    # Pass 2: rewrite metadata.jsonl, replacing only the matched records' availability and
    # re-deriving their two spin flags from the record's own calc_parameters/site_magmoms.
    n_meta = n_refreshed = 0

    def _apply(record: dict) -> dict:
        nonlocal n_refreshed
        cid = record.get("calc_id")
        if cid in update:
            record["availability"] = dict(update[cid])   # 7 heavy flags (filename ∪ probe)
            spin = (record.get("calc_parameters") or {}).get("spin_polarized")
            _recompute_spin_availability(record, spin)    # adds spin_density + magnetization
            n_refreshed += 1
        return record

    if dry_run:
        for record in read_jsonl(metadata_path):
            n_meta += 1
            _apply(dict(record))  # count only; discard
    else:
        with DatasetLock(dataset_dir):
            tmp = metadata_path.parent / (metadata_path.name + ".availability.tmp")
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
        "dataset_calcs": len(dataset_calc_ids),
        "calcs_present_this_pass": n_present,     # staged calcs whose availability we recomputed
        "calcs_skipped_absent": n_absent,         # purged/absent (already done or unstaged)
        "metadata_records": n_meta,
        "records_refreshed": n_refreshed,
    }

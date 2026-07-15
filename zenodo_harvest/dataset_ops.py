"""Dataset-level operations for the array-job harvest model (docs/DESIGN.md §6).

The cluster parallelism story is: split the fetched manifest into N parts, run N
array tasks each parsing its own part into its OWN ``--dataset-dir`` (parse holds
a per-dir lock so tasks never share one), then merge the per-task dataset dirs
into one, verify the join integrity, and purge the raw archives that have been
fully parsed. This module implements those glue steps:

* :func:`split_manifest`  — round-robin a manifest into ``<stem>.part-NNN.jsonl``.
* :func:`merge_datasets`  — fold per-task dataset dirs into one (rename shards,
  renumber, rewrite each metadata record's ``shards`` list, re-verify).
* :func:`verify_dataset`  — metadata<->shard frame_id bijection + curation stats.
* :func:`purge_raw`       — delete raw extracted trees whose calcs are all parsed.

split/verify need only stdlib + ase (via :mod:`store`); merge/verify read frames
back through :func:`store.read_shard_frames_lenient`; purge reuses parse's calc_id
derivation so "is this raw record fully parsed?" is answered the same way parse
decided what to write.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ase import Atoms

from .manifest import read_jsonl
from .store import (
    DatasetLock,
    MetadataWriter,
    _shard_index_of,
    dataset_lock_is_live,
    existing_shard_paths,
    next_shard_index,
    read_shard_frames_lenient,
)

logger = logging.getLogger(__name__)

MERGED_MARKER = "merged.done"


# --------------------------------------------------------------------------- #
# split — round-robin a manifest into N parts (one per array task)            #
# --------------------------------------------------------------------------- #

def split_manifest(in_path: str | Path, parts: int, out_dir: str | Path) -> dict:
    """Round-robin the JSONL lines of ``in_path`` into ``parts`` sibling manifests.

    Output names are ``<stem>.part-000.jsonl`` … ``part-<N-1>.jsonl`` (index
    zero-padded to width 3). Reads via :func:`read_jsonl`, so a crash-torn final
    line in the source manifest is dropped cleanly rather than aborting the split.
    Round-robin (line ``i`` -> part ``i % parts``) keeps the parts within one line
    of each other so the array tasks are evenly loaded.
    """
    if parts < 1:
        raise ValueError(f"parts must be >= 1, got {parts}")
    in_path = Path(in_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = in_path.stem  # "fetched.jsonl" -> "fetched"
    paths = [out_dir / f"{stem}.part-{i:03d}.jsonl" for i in range(parts)]
    handles = [p.open("w") for p in paths]
    counts = [0] * parts
    try:
        for i, rec in enumerate(read_jsonl(in_path)):
            j = i % parts
            handles[j].write(json.dumps(rec) + "\n")
            counts[j] += 1
    finally:
        for h in handles:
            h.close()
    return {
        "in_path": str(in_path),
        "out_dir": str(out_dir),
        "parts": parts,
        "lines_total": sum(counts),
        "parts_written": [{"path": str(p), "lines": c} for p, c in zip(paths, counts)],
    }


# --------------------------------------------------------------------------- #
# Shared integrity check (used by verify AND by merge's post-verify).         #
# --------------------------------------------------------------------------- #

def _load_metadata(dataset_dir: str | Path) -> list[dict]:
    mp = Path(dataset_dir) / "metadata.jsonl"
    return list(read_jsonl(mp)) if mp.is_file() else []


def _read_all_frames(dataset_dir: str | Path) -> tuple[list[Atoms], list[str]]:
    """Read every shard's frames leniently; also return names of truncated shards."""
    frames: list[Atoms] = []
    truncated: list[str] = []
    for shard in existing_shard_paths(dataset_dir):
        fr, was_truncated = read_shard_frames_lenient(shard)
        frames.extend(fr)
        if was_truncated:
            truncated.append(shard.name)
    return frames, truncated


def check_integrity(records: list[dict], frames: list[Atoms],
                    truncated_shards: list[str]) -> dict:
    """metadata<->shard frame_id bijection (the cheap sanity gate after every array job).

    Requires each metadata ``frame_id`` to appear exactly once on disk and every
    on-disk frame to have a metadata owner. Reports up to 10 example ids each way
    plus full counts. A truncated shard is only ever a crashed-writer artifact
    (parse or merge), so orphan disk frames (a frame with no metadata owner) are
    downgraded to a warning WHEN a shard is truncated — that is the prunable tail
    parse's next resume removes; without truncation they are a real mismatch. A
    metadata id missing from disk (or duplicated) is always a hard failure — that
    direction means a dangling reference / data loss, never a prunable artifact.
    """
    meta_ids = [fid for rec in records for fid in rec.get("frame_ids", [])]
    disk_ids = [f.info.get("frame_id") for f in frames]
    meta_counts = Counter(meta_ids)
    disk_counts: Counter = Counter(disk_ids)
    meta_set = set(meta_ids)

    dup_in_metadata = sorted(i for i, c in meta_counts.items() if c > 1)
    missing_on_disk = sorted(i for i in meta_set if disk_counts.get(i, 0) == 0)
    dup_on_disk = sorted(i for i in meta_set if disk_counts.get(i, 0) > 1)
    orphans_on_disk = sorted(str(i) for i in disk_counts if i not in meta_set)

    any_truncated = bool(truncated_shards)
    hard_fail = bool(missing_on_disk or dup_on_disk or dup_in_metadata)
    orphan_fail = bool(orphans_on_disk) and not any_truncated
    return {
        "ok": not (hard_fail or orphan_fail),
        "n_frames_metadata": len(meta_ids),
        "n_frames_on_disk": len(disk_ids),
        "n_missing_on_disk": len(missing_on_disk),
        "missing_on_disk": missing_on_disk[:10],
        "n_duplicate_on_disk": len(dup_on_disk),
        "duplicate_on_disk": dup_on_disk[:10],
        "n_duplicate_in_metadata": len(dup_in_metadata),
        "duplicate_in_metadata": dup_in_metadata[:10],
        "n_orphans_on_disk": len(orphans_on_disk),
        "orphans_on_disk": orphans_on_disk[:10],
        "truncated_shards": truncated_shards,
        "orphans_tolerated_as_truncation": bool(orphans_on_disk) and any_truncated,
    }


# --------------------------------------------------------------------------- #
# verify — integrity gate + curation stats                                    #
# --------------------------------------------------------------------------- #

def _key(value: Any) -> str:
    """JSON-safe grouping key for a metadata value (None -> "null")."""
    return "null" if value is None else str(value)


def _dataset_stats(records: list[dict], frames: list[Atoms]) -> dict:
    """Curation stats in ONE pass over the frames + metadata already read for (a).

    Element/property coverage is the instrument for assembling a coherent MLIP
    training subset (filter by functional / run_type / element / license), so it is
    worth computing every time the cheap integrity gate runs. "Frames by X" weights
    each calc's metadata value by that calc's frame count; element counts weight by
    frames CONTAINING the element (a frame with Fe counts once for Fe).
    """
    by_parser: Counter = Counter()
    by_run_type: Counter = Counter()
    by_functional: Counter = Counter()
    by_license: Counter = Counter()
    by_resource_type: Counter = Counter()
    conv = {"true": 0, "false": 0, "null": 0}
    tot_scf_unconverged = tot_dropped = tot_with_forces = 0
    for rec in records:
        nf = len(rec.get("frame_ids", []))
        cp = rec.get("calc_parameters", {}) or {}
        prov = rec.get("provenance", {}) or {}
        q = rec.get("quality", {}) or {}
        by_parser[_key(rec.get("parser"))] += nf
        by_run_type[_key(cp.get("run_type"))] += nf
        by_functional[_key(cp.get("functional"))] += nf
        by_license[_key(prov.get("license"))] += nf
        by_resource_type[_key(prov.get("resource_type"))] += nf
        ec = q.get("electronic_converged")
        conv["true" if ec is True else "false" if ec is False else "null"] += 1
        tot_scf_unconverged += int(q.get("n_frames_scf_unconverged", 0) or 0)
        tot_dropped += int(q.get("n_frames_dropped_no_energy", 0) or 0)
        tot_with_forces += int(q.get("n_frames_with_forces", 0) or 0)

    elements: Counter = Counter()
    for f in frames:
        for sym in set(f.get_chemical_symbols()):  # once per frame containing the element
            elements[sym] += 1

    return {
        "n_calcs": len(records),
        "frames_by_parser": dict(by_parser),
        "frames_by_run_type": dict(by_run_type),
        "frames_by_functional": dict(by_functional),
        "frames_by_license": dict(by_license),
        "frames_by_resource_type": dict(by_resource_type),
        "calcs_by_electronic_converged": conv,
        "total_n_frames_scf_unconverged": tot_scf_unconverged,
        "total_n_frames_dropped_no_energy": tot_dropped,
        "total_n_frames_with_forces": tot_with_forces,
        "element_frame_counts": dict(sorted(elements.items())),
    }


def verify_dataset(dataset_dir: str | Path) -> dict:
    """Integrity bijection + curation stats for a dataset dir (F3 `verify`).

    Reads metadata + every shard's frames ONCE, then (a) checks the frame_id
    bijection and (b) computes coverage stats from the same in-memory data (no
    extra I/O). ``ok`` is False on any integrity mismatch; the CLI maps that to a
    non-zero exit. A truncated top shard is surfaced as a warning, not a failure.
    """
    records = _load_metadata(dataset_dir)
    frames, truncated = _read_all_frames(dataset_dir)
    integrity = check_integrity(records, frames, truncated)
    stats = _dataset_stats(records, frames)
    stats["n_frames_metadata"] = integrity["n_frames_metadata"]
    stats["n_frames_on_disk"] = integrity["n_frames_on_disk"]
    return {"dataset_dir": str(dataset_dir), "ok": integrity["ok"],
            "integrity": integrity, "stats": stats}


# --------------------------------------------------------------------------- #
# merge-datasets — fold per-task dataset dirs into one                        #
# --------------------------------------------------------------------------- #

def _fail(into: Path, error: str, **extra: Any) -> dict:
    """A refusal summary: nothing was moved or deleted."""
    logger.error("merge refused: %s", error)
    return {"ok": False, "into": str(into), "error": error, **extra}


def merge_datasets(into: str | Path, sources: list[str | Path]) -> dict:
    """Fold per-task dataset dirs (``sources``) into one destination (``into``).

    Crash-safety and ordering live in :mod:`store`'s resume-support comment: for
    each source we MOVE its shards into the destination (rename + renumber, never
    recompressing the opaque gzip blobs) BEFORE appending that source's metadata,
    then drop a ``merged.done`` marker so a re-run skips it instead of
    double-appending. An interruption therefore leaves at worst prunable orphan
    frames in the destination, never metadata pointing at absent frames.

    Refuses the WHOLE merge (returns ``ok=False``, moves/deletes nothing) if: a
    source holds a live lock, a source's metadata is unreadable, a referenced shard
    file is missing, or a frame_id is duplicated within/across sources or already
    present in the destination. Post-verifies the merged destination's bijection.
    """
    into = Path(into)
    src_dirs = [Path(s) for s in sources]
    into_resolved = into.resolve()
    for s in src_dirs:
        if s.resolve() == into_resolved:
            return _fail(into, f"source {s} is the same dir as --into; refusing")

    # 1. Lock the destination for the whole merge; refuse any live-locked source.
    with DatasetLock(into):
        for s in src_dirs:
            if dataset_lock_is_live(s) is not None:
                return _fail(into, f"source {s} is locked (a parse may be writing it); "
                                   f"wait for it to finish before merging")

        # Sources already fully merged in a prior run are skipped (marker present).
        skipped = [str(s) for s in src_dirs if (s / MERGED_MARKER).is_file()]
        pending = [s for s in src_dirs if not (s / MERGED_MARKER).is_file()]

        # 2. Pre-validate every pending source, up front, before touching anything.
        dest_frame_ids: set[str] = set()
        for rec in _load_metadata(into):
            dest_frame_ids.update(rec.get("frame_ids", []))
        seen: set[str] = set()
        plans: list[tuple[Path, list[dict], list[str]]] = []
        for s in pending:
            mp = s / "metadata.jsonl"
            if not mp.is_file():
                return _fail(into, f"source {s} has no metadata.jsonl")
            try:
                records = list(read_jsonl(mp))
            except (OSError, ValueError) as exc:
                return _fail(into, f"source {s} metadata.jsonl unreadable: {exc}")
            referenced: dict[str, None] = {}
            for rec in records:
                for name in rec.get("shards", []):
                    if not (s / name).is_file():
                        return _fail(into, f"source {s} record {rec.get('calc_id')} references "
                                           f"missing shard {name}")
                    referenced.setdefault(name, None)
                for fid in rec.get("frame_ids", []):
                    if fid in seen:
                        return _fail(into, f"duplicate frame_id {fid} within/across sources")
                    if fid in dest_frame_ids:
                        return _fail(into, f"frame_id {fid} from {s} already in destination "
                                           f"(already merged? drop a {MERGED_MARKER} marker to skip it)")
                    seen.add(fid)
            plans.append((s, records, sorted(referenced, key=lambda n: _shard_index_of(Path(n)))))

        # 3-5. Move shards (rename+renumber) then append metadata, per source.
        start = next_shard_index(into)
        counter = start
        per_source: list[dict] = []
        with MetadataWriter(into / "metadata.jsonl") as meta_w:
            for s, records, ref_names in plans:
                mapping: dict[str, str] = {}
                for name in ref_names:
                    new_name = f"shard-{counter:05d}.extxyz.gz"
                    counter += 1
                    # rename only: shard gzip files are opaque, never recompressed.
                    os.replace(s / name, into / new_name)
                    mapping[name] = new_name
                # metadata appended AFTER its shards are in place (see docstring).
                for rec in records:
                    rec["shards"] = [mapping[n] for n in rec.get("shards", [])]
                    meta_w.write(rec)
                (s / MERGED_MARKER).write_text(json.dumps({
                    "merged_into": str(into.resolve()),
                    "at": datetime.now(timezone.utc).isoformat(),
                    "records": len(records), "shards_moved": len(mapping),
                }) + "\n")
                per_source.append({"source": str(s), "records": len(records),
                                   "shards_moved": len(mapping), "shard_map": mapping})
                logger.info("merged %s: %d records, %d shards", s, len(records), len(mapping))

        # 6. Post-verify the merged destination's bijection (delete nothing on failure).
        frames, truncated = _read_all_frames(into)
        integrity = check_integrity(_load_metadata(into), frames, truncated)
        return {
            "ok": integrity["ok"],
            "into": str(into),
            "sources_merged": [str(s) for s, _, _ in plans],
            "sources_skipped_already_merged": skipped,
            "shards_moved": counter - start,
            "records_appended": sum(len(r) for _, r, _ in plans),
            "next_shard_index_start": start,
            "per_source": per_source,
            "integrity": integrity,
        }


# --------------------------------------------------------------------------- #
# purge-raw — reclaim scratch by deleting raw trees whose calcs are all parsed #
# --------------------------------------------------------------------------- #

def _strictly_within(base: Path, target: Path) -> bool:
    """True iff ``target`` resolves to a path strictly BELOW ``base`` (never ==)."""
    base_r, target_r = base.resolve(), target.resolve()
    return target_r != base_r and target_r.is_relative_to(base_r)


def _tree_size(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def purge_raw(raw_dir: str | Path, dataset_dir: str | Path,
              fetched: str | Path | None = None, dry_run: bool = False) -> dict:
    """Delete ``<raw-dir>/<recid>/`` for records whose every calc is in the dataset.

    Cluster scratch is scarce (CLAUDE.md); once a record's calcs are parsed into the
    dataset its raw extracted tree is dead weight. A recid is purgeable iff ALL its
    expected calc_ids (derived exactly as parse derives them, via the imported
    :func:`parse._calc_id`/:func:`parse._resolve`) appear in the dataset's
    metadata.jsonl. Units rejected at parse live in rejections.jsonl, NOT metadata,
    so their calc_id stays "unparsed" here and the recid is KEPT — deliberate: parse
    re-tries rejected units on resume, so deleting their raw files would block a
    post-fix recovery. ``dry_run`` reports identically but deletes nothing.
    """
    from .parse import _calc_id, _resolve  # lazy: only purging needs parse's heavy deps

    raw_dir, dataset_dir = Path(raw_dir), Path(dataset_dir)
    fetched_path = Path(fetched) if fetched else raw_dir.parent / "manifests" / "fetched.jsonl"
    if not fetched_path.is_file():
        raise FileNotFoundError(f"fetched manifest not found: {fetched_path}")

    parsed = {rec["calc_id"] for rec in _load_metadata(dataset_dir) if rec.get("calc_id")}
    fetched_records = list(read_jsonl(fetched_path))  # read fully BEFORE any deletion

    per_recid: list[dict] = []
    n_purged = n_kept = bytes_freed = 0
    for rec in fetched_records:
        recid = rec["recid"]
        base_meta = {"provenance": rec["provenance"],
                     "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
        expected = [_calc_id({k: str(_resolve(raw_dir, v)) for k, v in unit.items()}, base_meta)
                    for unit in rec.get("calc_units", [])]
        unparsed = [c for c in expected if c not in parsed]
        purgeable = bool(expected) and not unparsed
        target = _resolve(raw_dir, rec["local_dir"])
        entry: dict[str, Any] = {"recid": recid, "n_calc_units": len(expected),
                                 "n_unparsed": len(unparsed)}
        if not purgeable:
            entry.update(decision="kept",
                         reason="unparsed_calc_units" if expected else "no_calc_units")
            n_kept += 1
        elif not _strictly_within(raw_dir, target):
            # never rmtree a path that resolves outside raw-dir (legacy absolute
            # manifests, symlinks, traversal) — keep it and flag loudly.
            entry.update(decision="kept", reason="target_outside_raw_dir", target=str(target))
            n_kept += 1
        elif not target.exists():
            entry.update(decision="purged", already_absent=True, bytes_freed=0)
            n_purged += 1
        else:
            size = _tree_size(target)
            entry.update(decision="purged", bytes_freed=size, target=str(target))
            if not dry_run:
                shutil.rmtree(target)
            bytes_freed += size
            n_purged += 1
        per_recid.append(entry)

    return {"ok": True, "raw_dir": str(raw_dir), "dataset_dir": str(dataset_dir),
            "fetched": str(fetched_path), "dry_run": dry_run,
            "recids_total": len(fetched_records), "recids_purged": n_purged,
            "recids_kept": n_kept, "bytes_freed": bytes_freed, "per_recid": per_recid}

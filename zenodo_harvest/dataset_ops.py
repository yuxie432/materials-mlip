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

import errno
import heapq
import json
import logging
import os
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .manifest import read_jsonl
from .store import (
    DatasetLock,
    MetadataWriter,
    _shard_index_of,
    dataset_lock_is_live,
    existing_shard_paths,
    next_shard_index,
    read_shard_frame_meta_lenient,
)

logger = logging.getLogger(__name__)

MERGED_MARKER = "merged.done"
# Written into a source dir at the START of merging it and removed once its marker is
# down. Its presence means "this source was interrupted mid-merge" and carries the
# old->new shard mapping so a re-run resumes idempotently instead of refusing.
MERGE_PROGRESS = ".merge-progress.json"


def _read_progress(s: Path) -> dict | None:
    p = s / MERGE_PROGRESS
    if not p.is_file():
        return None
    try:
        info = json.loads(p.read_text())
        return info if isinstance(info, dict) else None
    except (OSError, ValueError):
        return None


def _write_progress(s: Path, into_resolved: Path, mapping: dict[str, str]) -> None:
    (s / MERGE_PROGRESS).write_text(
        json.dumps({"into": str(into_resolved), "mapping": mapping}))


def _clear_progress(s: Path) -> None:
    (s / MERGE_PROGRESS).unlink(missing_ok=True)


def _move_shard(src_path: Path, dst_path: Path) -> None:
    """Move a shard into place. Prefer ``os.replace`` (atomic rename); fall back to
    ``shutil.move`` (copy+unlink) across filesystems — per-task dirs on node-local
    scratch and the destination on shared Lustre would otherwise raise EXDEV."""
    try:
        os.replace(src_path, dst_path)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            shutil.move(str(src_path), str(dst_path))
        else:
            raise


# --------------------------------------------------------------------------- #
# split — round-robin a manifest into N parts (one per array task)            #
# --------------------------------------------------------------------------- #

def _split_weight(rec: dict) -> int:
    """Parse-cost proxy for one fetched record: its calc-unit count (>= 1).

    A record's calc-units all parse together in one array task, so their *count* is
    a far better balance weight than 1-per-record — record count says nothing about
    how much pymatgen/ASE work a part holds. Reads the top-level ``n_calc_units``
    that :func:`fetch._fetched_entry` writes, falling back to ``len(calc_units)``,
    then to 1 (a keep-list record, or one with no counted units, still costs one
    line/parse attempt). So on a ``keep.jsonl`` — which has no calc-unit counts yet —
    every weight is 1 and calc-weighting degrades cleanly to count-balancing.
    """
    n = rec.get("n_calc_units")
    if n is None:
        n = len(rec.get("calc_units") or [])
    try:
        n = int(n)
    except (TypeError, ValueError):
        n = 0
    return n if n > 0 else 1


def split_manifest(in_path: str | Path, parts: int, out_dir: str | Path,
                   weight_by: str = "records") -> dict:
    """Split the JSONL lines of ``in_path`` into ``parts`` sibling manifests.

    Output names are ``<stem>.part-000.jsonl`` … ``part-<N-1>.jsonl`` (index
    zero-padded to width 3). Reads via :func:`read_jsonl`, so a crash-torn final
    line in the source manifest is dropped cleanly rather than aborting the split.

    ``weight_by`` chooses how the lines are balanced across parts:

    * ``"records"`` (default) — round-robin (line ``i`` -> part ``i % parts``), which
      keeps the parts within one *line* of each other. Streams, so memory is O(1) in
      the manifest size. Balances record COUNT.
    * ``"calcs"`` — greedy longest-processing-time bin-packing by each record's
      :func:`_split_weight` (its calc-unit count), so parse COST is balanced rather
      than record count. This is what evens out array-task wallclock when records vary
      a lot in calc-unit count (a round-robin split can pile the calc-heavy records
      into one part). It materialises the manifest to sort by weight (metadata only —
      small); ``read_jsonl`` still drops a torn final line. It cannot split a *single*
      record across parts, so one record that alone holds most of the calc-units still
      lands whole in one part. On a manifest with no calc-unit counts (a keep-list)
      every weight is 1, so this degrades to the same count-balancing as ``"records"``.

    The assignment is fully deterministic (stable sort + ``(load, part_index)`` heap
    tie-break), so a resumed array job re-derives the identical parts.
    """
    if parts < 1:
        raise ValueError(f"parts must be >= 1, got {parts}")
    if weight_by not in ("records", "calcs"):
        raise ValueError(f"weight_by must be 'records' or 'calcs', got {weight_by!r}")
    in_path = Path(in_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = in_path.stem  # "fetched.jsonl" -> "fetched"
    paths = [out_dir / f"{stem}.part-{i:03d}.jsonl" for i in range(parts)]
    counts = [0] * parts
    weights = [0] * parts
    handles = [p.open("w") for p in paths]
    try:
        if weight_by == "records":
            for i, rec in enumerate(read_jsonl(in_path)):
                j = i % parts
                handles[j].write(json.dumps(rec) + "\n")
                counts[j] += 1
                weights[j] += _split_weight(rec)
        else:  # "calcs": LPT bin-packing by calc-unit count
            records = list(read_jsonl(in_path))
            rec_weight = [_split_weight(r) for r in records]
            heap = [(0, i) for i in range(parts)]  # (current load, part index)
            heapq.heapify(heap)
            assign = [0] * len(records)
            # Assign the heaviest records first; the min-heap always drops the next
            # record onto the currently-lightest part. Stable descending sort +
            # (load, part) tie-break keep the result deterministic.
            for idx in sorted(range(len(records)),
                              key=lambda k: rec_weight[k], reverse=True):
                load, part = heapq.heappop(heap)
                assign[idx] = part
                heapq.heappush(heap, (load + rec_weight[idx], part))
            for idx, rec in enumerate(records):  # write in original order
                j = assign[idx]
                handles[j].write(json.dumps(rec) + "\n")
                counts[j] += 1
                weights[j] += rec_weight[idx]
    finally:
        for h in handles:
            h.close()
    return {
        "in_path": str(in_path),
        "out_dir": str(out_dir),
        "parts": parts,
        "weight_by": weight_by,
        "lines_total": sum(counts),
        "parts_written": [
            {"path": str(p), "lines": c, "weight": w}
            for p, c, w in zip(paths, counts, weights)
        ],
    }


# --------------------------------------------------------------------------- #
# Shared integrity check (used by verify AND by merge's post-verify).         #
# --------------------------------------------------------------------------- #

def _load_metadata(dataset_dir: str | Path) -> list[dict]:
    mp = Path(dataset_dir) / "metadata.jsonl"
    return list(read_jsonl(mp)) if mp.is_file() else []


def _scan_disk(dataset_dir: str | Path) -> tuple[Counter, Counter, list[str]]:
    """Stream every shard ONCE, one shard in memory at a time.

    Returns the on-disk ``frame_id`` multiset (Counter), per-element frame counts
    (Counter; a frame counts once per element it contains), and the names of any
    truncated shards. Uses :func:`store.read_shard_frame_meta_lenient`, which extracts
    just ``(frame_id, {symbols})`` by parsing the extxyz text — NOT building an ``Atoms``
    per frame — so verify scales to tens of millions of frames in minutes rather than
    hours (a full ``Atoms`` build per frame made verify blow its wallclock at ~12M frames).
    Memory is bounded to one shard's lightweight tuples at a time.
    """
    disk_counts: Counter = Counter()
    elements: Counter = Counter()
    truncated: list[str] = []
    for shard in existing_shard_paths(dataset_dir):
        metas, was_truncated = read_shard_frame_meta_lenient(shard)
        if was_truncated:
            truncated.append(shard.name)
        for frame_id, symbols in metas:
            disk_counts[frame_id] += 1
            for sym in symbols:
                elements[sym] += 1
        # `metas` (this shard's tuples) is released before the next shard is read.
    return disk_counts, elements, truncated


def check_integrity(records: list[dict], disk_counts: Counter,
                    truncated_shards: list[str]) -> dict:
    """metadata<->shard frame_id bijection (the cheap sanity gate after every array job).

    Takes the on-disk frame_id multiset (from :func:`_scan_disk`) rather than a list
    of Atoms, so it needs only counts in memory. Requires each metadata ``frame_id``
    to appear exactly once on disk and every on-disk frame to have a metadata owner.
    Reports up to 10 example ids each way plus full counts. A truncated shard is only
    ever a crashed-writer artifact (parse or merge), so orphan disk frames (a frame
    with no metadata owner) are downgraded to a warning WHEN a shard is truncated —
    that is the prunable tail parse's next resume removes; without truncation they are
    a real mismatch. A metadata id missing from disk (or duplicated) is always a hard
    failure — that direction means a dangling reference / data loss, never prunable.
    """
    meta_ids = [fid for rec in records for fid in rec.get("frame_ids", [])]
    meta_counts = Counter(meta_ids)
    meta_set = set(meta_ids)
    n_disk = sum(disk_counts.values())

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
        "n_frames_on_disk": n_disk,
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


def _dataset_stats(records: list[dict], elements: Counter) -> dict:
    """Curation stats from the metadata records + the element Counter that the
    streaming disk scan (:func:`_scan_disk`) already accumulated.

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
    tot_scf_unconverged = tot_dropped = tot_with_forces = tot_with_stress = 0
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
        tot_with_stress += int(q.get("n_frames_with_stress", 0) or 0)

    # `elements` was accumulated during the single streaming disk scan (no re-read).
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
        "total_n_frames_with_stress": tot_with_stress,
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
    disk_counts, elements, truncated = _scan_disk(dataset_dir)
    integrity = check_integrity(records, disk_counts, truncated)
    stats = _dataset_stats(records, elements)
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

    Resumable mid-source: before moving a source's shards we journal the old->new
    shard mapping (``.merge-progress.json``); if a run dies partway through a source
    (some shards renamed into the destination, marker not yet written), a re-run
    reads that journal and resumes idempotently — skipping shards already moved and
    metadata records already appended — rather than refusing on a "missing shard"
    that is in fact already in the destination.

    Refuses the WHOLE merge (returns ``ok=False``, moves/deletes nothing) if: a
    source holds a live lock, a source's metadata is unreadable, a referenced shard
    is genuinely missing (not in the source and not already moved to the destination),
    or a frame_id is duplicated within/across sources or already present in the
    destination (excluding a resuming source's own records). Post-verifies the merged
    destination's bijection.
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

        # A source with a .merge-progress.json (naming THIS destination) was interrupted
        # mid-merge: some of its shards are already in `into`. Load those journals so we
        # RESUME them idempotently instead of refusing on "missing shard".
        journals: dict[Path, dict[str, str]] = {}
        for s in pending:
            j = _read_progress(s)
            if j is not None and j.get("into") == str(into_resolved):
                journals[s] = j.get("mapping", {})

        # A resuming source's records may already be in `into` (appended before the
        # crash) — those are expected, not duplicates. Collect their calc_ids so the
        # dedup check tolerates them and the append step skips them (idempotent).
        dest_records = _load_metadata(into)
        dest_calc_ids = {r.get("calc_id") for r in dest_records}
        resuming_calc_ids: set[str] = set()
        for s in journals:
            if (s / "metadata.jsonl").is_file():
                resuming_calc_ids.update(r["calc_id"] for r in read_jsonl(s / "metadata.jsonl")
                                         if r.get("calc_id"))

        # 2. Pre-validate every pending source, up front, before touching anything.
        # dest_frame_ids EXCLUDES resuming sources' own already-appended records.
        dest_frame_ids: set[str] = set()
        for rec in dest_records:
            if rec.get("calc_id") in resuming_calc_ids:
                continue
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
            jmap = journals.get(s, {})
            referenced: dict[str, None] = {}
            for rec in records:
                for name in rec.get("shards", []):
                    # OK if still in the source, OR (resuming) already moved into `into`.
                    moved = name in jmap and (into / jmap[name]).is_file()
                    if not (s / name).is_file() and not moved:
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

        # 3-5. Move shards then append metadata, per source. Fresh sources reserve new
        # shard indices from a running counter; resuming sources reuse their journalled
        # mapping. Reserve the counter past every journalled index first, so a fresh
        # source's new names can't collide with a resuming source's not-yet-moved ones.
        start = next_shard_index(into)
        reserved_max = max((_shard_index_of(Path(n)) for m in journals.values() for n in m.values()),
                           default=-1)
        counter = max(start, reserved_max + 1)
        per_source: list[dict] = []
        total_moved = 0
        with MetadataWriter(into / "metadata.jsonl") as meta_w:
            for s, records, ref_names in plans:
                mapping = journals.get(s)
                if mapping is None:  # fresh source: allocate new names + journal BEFORE moving
                    mapping = {}
                    for name in ref_names:
                        mapping[name] = f"shard-{counter:05d}.extxyz.gz"
                        counter += 1
                    _write_progress(s, into_resolved, mapping)
                moved_now = 0
                for name in ref_names:
                    src_path, dst_path = s / name, into / mapping[name]
                    if src_path.is_file():
                        _move_shard(src_path, dst_path)  # opaque gzip, never recompressed
                        moved_now += 1
                    # else: already at dst from a partial prior run -> skip (idempotent).
                # metadata appended AFTER its shards are in place; skip records a partial
                # prior run already appended (idempotent).
                appended = 0
                for rec in records:
                    if rec.get("calc_id") in dest_calc_ids:
                        continue
                    rec["shards"] = [mapping[n] for n in rec.get("shards", [])]
                    meta_w.write(rec)
                    appended += 1
                (s / MERGED_MARKER).write_text(json.dumps({
                    "merged_into": str(into_resolved),
                    "at": datetime.now(timezone.utc).isoformat(),
                    "records": len(records), "shards_moved": len(mapping),
                }) + "\n")
                _clear_progress(s)  # committed -> drop the journal
                total_moved += moved_now
                per_source.append({"source": str(s), "records": len(records),
                                   "shards_moved": len(mapping), "shards_moved_this_run": moved_now,
                                   "records_appended_this_run": appended, "shard_map": mapping})
                logger.info("merged %s: %d records, %d shards (%d moved, %d appended this run)",
                            s, len(records), len(mapping), moved_now, appended)

        # 6. Post-verify the merged destination's bijection (delete nothing on failure).
        disk_counts, _elements, truncated = _scan_disk(into)
        integrity = check_integrity(_load_metadata(into), disk_counts, truncated)
        return {
            "ok": integrity["ok"],
            "into": str(into),
            "sources_merged": [str(s) for s, _, _ in plans],
            "sources_skipped_already_merged": skipped,
            "shards_moved": total_moved,
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


def _free_parsed_unit_files(units: list[dict], expected: list[str], parsed: set,
                            raw_dir: Path, resolve: Any, dry_run: bool) -> tuple[int, int]:
    """Delete the files of a record's PARSED calc units, keeping the unparsed ones'.

    Without this, one unparsed unit pins a record's whole staged tree for the rest of the
    run: `purge_raw` is otherwise all-or-nothing per recid, and units rejected at parse
    never reach metadata, so their record is kept forever. Over a full harvest that
    retained staging accumulates monotonically and can exceed the disk budget on its own —
    at which point the pacing loop has nothing left to reclaim and the harvest stalls.

    Only files whose OWN calc is already in the dataset are removed, and never a file that
    an unparsed unit also references (a flat multi-calc directory shares untagged inputs),
    so a later parse re-try of the rejected units still has everything it needs.
    Returns ``(bytes_freed, files_removed)``.
    """
    keep: set[Path] = set()
    drop: set[Path] = set()
    for unit, calc_id in zip(units, expected):
        paths = {resolve(raw_dir, v) for k, v in unit.items() if k != "dir"}
        (drop if calc_id in parsed else keep).update(paths)

    freed = removed = 0
    for path in sorted(drop - keep):
        if not _strictly_within(raw_dir, path) or not path.is_file():
            continue
        freed += path.stat().st_size
        removed += 1
        if not dry_run:
            path.unlink(missing_ok=True)
    return freed, removed


def _prune_empty_dirs(root: Path, raw_dir: Path) -> int:
    """Remove now-empty directories under ``root`` (bottom-up), keeping ``root`` itself.

    Empty directories still consume inodes, and the file-count budget is the binding
    limit on CSD3 scratch — a screening record can leave thousands of empty per-calc dirs
    behind after its files are freed."""
    if not root.is_dir() or not _strictly_within(raw_dir, root):
        return 0
    pruned = 0
    for d in sorted((p for p in root.rglob("*") if p.is_dir()),
                    key=lambda p: len(p.parts), reverse=True):
        try:
            if _strictly_within(raw_dir, d) and not any(d.iterdir()):
                d.rmdir()
                pruned += 1
        except OSError:  # raced/permission — leave it, it is only inode housekeeping
            continue
    return pruned


def purge_raw(raw_dir: str | Path, dataset_dir: str | Path,
              fetched: str | Path | None = None, dry_run: bool = False) -> dict:
    """Reclaim raw staging for calcs that are already in the dataset.

    Cluster scratch is scarce (CLAUDE.md); once a calc is parsed into the dataset its raw
    extracted files are dead weight. A calc counts as parsed iff its calc_id (derived
    exactly as parse derives it, via the imported :func:`parse._calc_id`/
    :func:`parse._resolve`) appears in the dataset's metadata.jsonl.

    * A recid whose EVERY unit is parsed has its whole ``<raw-dir>/<recid>/`` tree deleted.
    * A recid with some unparsed units is **kept**, but the files of its parsed units are
      still freed (:func:`_free_parsed_unit_files`) and emptied directories pruned. Units
      rejected at parse never reach metadata, so without this one bad unit would pin the
      record's entire tree for the whole run and retained staging would accumulate past
      the disk budget — leaving the pacing loop nothing to reclaim. Files an unparsed unit
      needs are never touched, so a post-fix parse re-try still works.

    ``dry_run`` reports identically but deletes nothing.
    """
    from .parse import _calc_id, _resolve  # lazy: only purging needs parse's heavy deps

    raw_dir, dataset_dir = Path(raw_dir), Path(dataset_dir)
    fetched_path = Path(fetched) if fetched else raw_dir.parent / "manifests" / "fetched.jsonl"
    if not fetched_path.is_file():
        raise FileNotFoundError(f"fetched manifest not found: {fetched_path}")

    parsed = {rec["calc_id"] for rec in _load_metadata(dataset_dir) if rec.get("calc_id")}
    fetched_records = list(read_jsonl(fetched_path))  # read fully BEFORE any deletion

    per_recid: list[dict] = []
    n_purged = n_kept = bytes_freed = files_removed = 0
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
            # Still reclaim the parsed units' files so one bad unit cannot pin the whole
            # record's staging for the rest of the harvest (see _free_parsed_unit_files).
            if expected and _strictly_within(raw_dir, target):
                freed, removed = _free_parsed_unit_files(
                    rec.get("calc_units", []), expected, parsed, raw_dir, _resolve, dry_run)
                pruned = 0 if dry_run else _prune_empty_dirs(target, raw_dir)
                entry.update(partial_bytes_freed=freed, partial_files_removed=removed,
                             empty_dirs_pruned=pruned)
                bytes_freed += freed
                files_removed += removed
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
            n_files = sum(1 for p in target.rglob("*") if p.is_file())
            entry.update(decision="purged", bytes_freed=size, files_removed=n_files,
                         target=str(target))
            if not dry_run:
                shutil.rmtree(target)
            bytes_freed += size
            files_removed += n_files
            n_purged += 1
        per_recid.append(entry)

    return {"ok": True, "raw_dir": str(raw_dir), "dataset_dir": str(dataset_dir),
            "fetched": str(fetched_path), "dry_run": dry_run,
            "recids_total": len(fetched_records), "recids_purged": n_purged,
            "recids_kept": n_kept, "bytes_freed": bytes_freed,
            # files_removed matters as much as bytes: the inode quota is the binding limit
            # on CSD3 scratch, so the pacing loop needs to see inodes reclaimed too.
            "files_removed": files_removed, "per_recid": per_recid}

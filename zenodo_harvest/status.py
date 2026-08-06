"""Read-only harvest status report.

Aggregates the append-only manifests, the dataset dir, and the staging tree into one
snapshot of how far the harvest has got and what it has dropped. Touches **no network**
and takes **no lock**, so it is safe to run WHILE a fetch/pipeline job is writing these
files — every manifest is flushed per line, and a torn final line is tolerated by
``read_jsonl``. It only counts lines and walks directories; nothing here mutates state.

Progress percentages come from pairing a numerator with the denominator the harvest
already records: fetched records vs the triage keep-list; parsed calcs vs the calc-units
the fetch manifest says were staged.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from pathlib import Path
from typing import Any

from .manifest import read_jsonl


def _count_nonempty_lines(path: Path) -> int:
    """Number of non-blank lines in a JSONL file (0 if absent). No JSON parsing."""
    if not path.is_file():
        return 0
    n = 0
    with path.open() as fh:
        for line in fh:
            if line.strip():
                n += 1
    return n


def _dir_usage(root: Path) -> tuple[int, int, int]:
    """``(bytes, n_files, n_dirs)`` under ``root`` in a single iterative walk.

    Counts directories separately because CSD3's ``/rds`` is Lustre and its 1M-file cap
    is an *inode* quota (files + dirs), which is exactly what fetch's disk valve charges.
    Returns ``(0, 0, 0)`` if ``root`` is absent. Symlinks are not followed.
    """
    if not root.is_dir():
        return 0, 0, 0
    total = n_files = n_dirs = 0
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            n_dirs += 1
                            stack.append(Path(e.path))
                        elif e.is_file(follow_symlinks=False):
                            n_files += 1
                            total += e.stat(follow_symlinks=False).st_size
                    except OSError:  # a file vanished mid-walk (live job) — skip it
                        continue
        except OSError:
            continue
    return total, n_files, n_dirs


def _pct(num: float | None, den: float | None) -> float | None:
    return (100.0 * num / den) if (num is not None and den) else None


def status_report(
    *,
    manifests_dir: str | Path,
    raw_dir: str | Path,
    dataset_dir: str | Path,
    keep_path: str | Path | None = None,
    max_disk_bytes: int | None = None,
    max_disk_files: int | None = None,
    staging_walk: bool = True,
) -> dict[str, Any]:
    """Build the machine-readable status snapshot (see module docstring).

    ``staging_walk=False`` skips the STAGING section's walk over ``raw_dir``. That walk
    ``stat``s every inode under ``raw/``, which on Lustre with a live job is slow (minutes
    at high inode counts); skipping it returns the rest of the report instantly. Read the
    /rds bytes+inodes from ``lfs quota -u $USER <hpc-work>`` instead.
    """
    manifests_dir, raw_dir, dataset_dir = Path(manifests_dir), Path(raw_dir), Path(dataset_dir)

    # DISCOVER — deduplicated candidate manifest(s); exclude the raw <out>.hits.jsonl checkpoint.
    cand_files = [p for p in sorted(manifests_dir.glob("candidates*.jsonl"))
                  if not p.name.endswith(".hits.jsonl")]
    n_candidates = sum(_count_nonempty_lines(p) for p in cand_files)

    # TRIAGE — the keep-list is the fetch denominator.
    keep = Path(keep_path) if keep_path else manifests_dir / "keep.jsonl"
    n_keep = _count_nonempty_lines(keep)
    # Record ids on the keep-list, so record-level progress can be counted against the
    # SAME set the fetch works over (and never exceed 100% from a stale rejection recid).
    keep_recids: set[str] = set()
    if keep.is_file():
        for rec in read_jsonl(keep):
            rid = rec.get("recid")
            if rid:
                keep_recids.add(str(rid))

    # FETCH — aggregate across every *.fetched.jsonl (pipeline writes one per part; a
    # standalone `fetch` writes a single manifests/fetched.jsonl). rglob covers both.
    # DEDUPE BY recid: within one harvest the part sidecars hold DISJOINT recids (round-robin
    # split), so deduping == summing; but if fetched manifests from more than one flow coexist
    # under the tree (e.g. a leftover `--parts-dir`, or the array flow's fetched.jsonl beside
    # the pipeline's part sidecars), the SAME recid appears twice and a naive sum over-counts
    # (observed as a >100% FETCH figure). Counting distinct recids is correct in both cases.
    n_fetched = n_calc_units = 0
    seen_recids: set[str] = set()
    for p in sorted(manifests_dir.rglob("*.fetched.jsonl")):
        for rec in read_jsonl(p):
            recid = rec.get("recid")
            if recid and recid in seen_recids:
                continue  # same record already counted from another fetched manifest
            if recid:
                seen_recids.add(recid)
            n_fetched += 1
            n_calc_units += int(rec.get("n_calc_units", 0) or 0)

    # PARSE — one metadata line per parsed calc; frames come from each calc's quality block.
    meta = dataset_dir / "metadata.jsonl"
    n_calcs = n_frames = n_frames_forces = 0
    parsed_recids: set[str] = set()  # distinct RECORDS that produced >=1 stored calc
    if meta.is_file():
        for rec in read_jsonl(meta):
            n_calcs += 1
            q = rec.get("quality") or {}
            n_frames += int(q.get("n_frames", 0) or 0)
            n_frames_forces += int(q.get("n_frames_with_forces", 0) or 0)
            # record id: from provenance if present, else the calc_id (zenodo:<recid>:<path>).
            rid = (rec.get("provenance") or {}).get("record_id")
            if not rid:
                cid = rec.get("calc_id") or ""
                if cid:
                    parts = cid.split(":")
                    rid = parts[1] if cid.startswith("zenodo:") and len(parts) > 1 else parts[0]
            if rid:
                parsed_recids.add(str(rid))

    # STORE — shard count + total dataset footprint.
    shards = sorted(dataset_dir.glob("shard-*.extxyz.gz"))
    ds_bytes, _, _ = _dir_usage(dataset_dir)

    # STAGING — raw tree usage vs the /rds quota (bytes AND inodes). The walk is the one
    # expensive part of this report (every inode under raw/), so it is skippable.
    raw_bytes: int | None
    raw_files: int | None
    raw_dirs: int | None
    inodes: int | None
    if staging_walk:
        raw_bytes, raw_files, raw_dirs = _dir_usage(raw_dir)
        inodes = raw_files + raw_dirs
    else:
        raw_bytes = raw_files = raw_dirs = inodes = None

    # ERRORS — rejections carry a machine reason; fetch logs to manifests/, parse to the
    # dataset dir (per-task in the array model, but the pipeline uses the shared one).
    rej_by_reason: Counter = Counter()
    rej_by_stage: Counter = Counter()
    fetch_reject_recids: set[str] = set()  # RECORD-level fetch rejects (id has no ':')
    n_rej = 0
    for rp in (manifests_dir / "rejections.jsonl", dataset_dir / "rejections.jsonl"):
        if rp.is_file():
            for rec in read_jsonl(rp):
                n_rej += 1
                rej_by_reason[str(rec.get("reason", "?"))] += 1
                rej_by_stage[str(rec.get("stage", "?"))] += 1
                rid = rec.get("id")
                # A record-level fetch rejection (per-file ids are "<recid>:<key>",
                # parse ids are "zenodo:<recid>:<path>" — both carry ':').
                if rec.get("stage") == "fetch" and isinstance(rid, str) and ":" not in rid:
                    fetch_reject_recids.add(rid)

    # RECORD-level progress: a record is "attempted" once it is fetched (yielded VASP) or
    # record-level rejected at fetch. This is the denominator status previously lacked —
    # fetched_records alone hides the ~2/3 of the keep-list that is attempted-but-rejected.
    attempted_recids = seen_recids | fetch_reject_recids
    if keep_recids:  # count only records actually on the keep-list (no stale-recid overshoot)
        attempted_recids &= keep_recids
    n_attempted = len(attempted_recids)
    n_fetched_in_keep = len(seen_recids & keep_recids) if keep_recids else n_fetched
    n_fetch_rejected = max(0, n_attempted - n_fetched_in_keep)
    n_untouched = max(0, n_keep - n_attempted)

    return {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "data": {"manifests": str(manifests_dir), "raw": str(raw_dir),
                 "dataset": str(dataset_dir)},
        "discover": {"candidates": n_candidates,
                     "files": [p.name for p in cand_files]},
        "triage": {"keep": n_keep},
        "fetch": {"fetched_records": n_fetched, "to_fetch": n_keep,
                  "pct": _pct(n_fetched, n_keep), "calc_units": n_calc_units},
        "records": {"keep": n_keep, "attempted": n_attempted,
                    "pct": _pct(n_attempted, n_keep), "fetched": n_fetched_in_keep,
                    "fetch_rejected": n_fetch_rejected, "untouched": n_untouched,
                    "with_frames": len(parsed_recids)},
        "parse": {"calcs_parsed": n_calcs, "calc_units_fetched": n_calc_units,
                  "pct": _pct(n_calcs, n_calc_units), "frames": n_frames,
                  "frames_with_forces": n_frames_forces},
        "store": {"shards": len(shards), "dataset_bytes": ds_bytes},
        "staging": {"walked": staging_walk,
                    "bytes": raw_bytes, "files": raw_files, "dirs": raw_dirs,
                    "inodes": inodes,
                    "max_disk_bytes": max_disk_bytes, "max_disk_files": max_disk_files,
                    "pct_bytes": _pct(raw_bytes, max_disk_bytes),
                    "pct_inodes": _pct(inodes, max_disk_files)},
        "errors": {"rejections": n_rej,
                   "by_reason": dict(rej_by_reason.most_common(8)),
                   "by_stage": dict(rej_by_stage)},
    }


def _h(n: float) -> str:
    """Human-readable byte size."""
    f = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if f < 1024 or unit == "TiB":
            return f"{int(f)} B" if unit == "B" else f"{f:.1f} {unit}"
        f /= 1024
    return f"{f:.1f} TiB"  # pragma: no cover


def _pctstr(p: float | None) -> str:
    return f"{p:.1f}%" if p is not None else "n/a"


def format_status(r: dict[str, Any]) -> str:
    """Render the report dict as a compact human-readable block."""
    d, t, f = r["discover"], r["triage"], r["fetch"]
    p, s, st, e = r["parse"], r["store"], r["staging"], r["errors"]
    rc = r["records"]
    lines = [f"harvest status @ {r['generated']}   ({r['data']['dataset']})",
             f"DISCOVER  candidates: {d['candidates']:,}",
             f"TRIAGE    keep-list:  {t['keep']:,}",
             f"FETCH     fetched:    {f['fetched_records']:,} / {f['to_fetch']:,}  "
             f"({_pctstr(f['pct'])})   calc_units: {f['calc_units']:,}",
             f"RECORDS   attempted:  {rc['attempted']:,} / {rc['keep']:,}  ({_pctstr(rc['pct'])})"
             f"   fetched {rc['fetched']:,} · fetch-rejected {rc['fetch_rejected']:,} · "
             f"untouched {rc['untouched']:,}   in-dataset {rc['with_frames']:,}",
             f"PARSE     parsed:     {p['calcs_parsed']:,} / {p['calc_units_fetched']:,} calc_units  "
             f"({_pctstr(p['pct'])})   frames: {p['frames']:,}",
             f"STORE     shards: {s['shards']:,}   dataset: {_h(s['dataset_bytes'])}"]
    if not st.get("walked", True):
        lines.append("STAGING   (walk skipped)  read /rds usage from: "
                     "lfs quota -u $USER <hpc-work>")
    else:
        bytes_part = _h(st["bytes"])
        if st["max_disk_bytes"]:
            bytes_part += f" / {_h(st['max_disk_bytes'])} ({_pctstr(st['pct_bytes'])})"
        inodes_part = f"{st['inodes']:,} (files {st['files']:,} + dirs {st['dirs']:,})"
        if st["max_disk_files"]:
            inodes_part += f" / {st['max_disk_files']:,} ({_pctstr(st['pct_inodes'])})"
        lines.append(f"STAGING   raw: {bytes_part}   inodes: {inodes_part}")
    reasons = ", ".join(f"{k} {v}" for k, v in e["by_reason"].items()) or "none"
    lines.append(f"ERRORS    rejections: {e['rejections']:,}  →  {reasons}")
    return "\n".join(lines)

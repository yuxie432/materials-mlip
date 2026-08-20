"""Read-only harvest status report.

Aggregates the append-only manifests, the dataset dir, and the staging tree into one
snapshot of how far the harvest has got and what it has dropped. Touches **no network**
and takes **no lock**, so it is safe to run WHILE a fetch/pipeline job is writing these
files — every manifest is flushed per line, and a torn final line is tolerated by
``read_jsonl``. It only counts lines and walks directories; nothing here mutates state.

Progress percentages come from pairing a numerator with the denominator the harvest
already records: fetched records vs the triage keep-list; parsed calcs vs the calc-units
staged. "Fetched" and "staged" fold in the dataset's own records — a record parsed in an
earlier run is done even though the global dataset-skip means it never re-enters the fetch
manifest — so a resume that keeps the dataset (fetch manifests reset) still reports honest
progress instead of parsed>fetched (>100%) and the whole dataset as untouched.
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
    candidate_globs: list[str] | None = None,
    keep_name: str = "keep.jsonl",
    extra_rejection_names: tuple[str, ...] = (),
    fetched_globs: list[str] | None = None,
) -> dict[str, Any]:
    """Build the machine-readable status snapshot (see module docstring).

    ``staging_walk=False`` skips the STAGING section's walk over ``raw_dir``. That walk
    ``stat``s every inode under ``raw/``, which on Lustre with a live job is slow (minutes
    at high inode counts); skipping it returns the rest of the report instantly. Read the
    /rds bytes+inodes from ``lfs quota -u $USER <hpc-work>`` instead.

    A second source (NOMAD) names its manifests differently and folds triage into discover,
    so three knobs adapt the SAME counting logic to it (defaults are the Zenodo names, so
    Zenodo behaviour is unchanged): ``candidate_globs`` = the DISCOVER manifest glob(s)
    (Zenodo ``candidates*.jsonl``; NOMAD ``nomad_keep.jsonl``); ``keep_name`` = the fetch
    denominator when ``keep_path`` is None (Zenodo ``keep.jsonl``; NOMAD ``nomad_keep.jsonl``);
    ``extra_rejection_names`` = further rejection logs under ``manifests_dir`` to fold into the
    ERRORS histogram (NOMAD's ``nomad_rejections.jsonl``/``nomad_fetch_rejections.jsonl``);
    ``fetched_globs`` = the fetched-manifest glob(s) for the FETCH count (default covers the
    pipeline part sidecars + Zenodo's standalone ``fetched.jsonl``; NOMAD adds ``nomad_fetched.jsonl``).
    """
    manifests_dir, raw_dir, dataset_dir = Path(manifests_dir), Path(raw_dir), Path(dataset_dir)

    # DISCOVER — deduplicated candidate manifest(s); exclude the raw <out>.hits.jsonl checkpoint.
    globs = candidate_globs or ["candidates*.jsonl"]
    cand_files = [p for g in globs for p in sorted(manifests_dir.glob(g))
                  if not p.name.endswith(".hits.jsonl")]
    n_candidates = sum(_count_nonempty_lines(p) for p in cand_files)

    # TRIAGE — the keep-list is the fetch denominator.
    keep = Path(keep_path) if keep_path else manifests_dir / keep_name
    n_keep = _count_nonempty_lines(keep)
    # Record ids on the keep-list, so record-level progress can be counted against the
    # SAME set the fetch works over (and never exceed 100% from a stale rejection recid).
    keep_recids: set[str] = set()
    if keep.is_file():
        for rec in read_jsonl(keep):
            # Zenodo keep-lists key the record as ``recid``; NOMAD's key it as ``entry_id``
            # (== the ``recid`` the NOMAD fetch manifest later writes), so accept either.
            rid = rec.get("recid") or rec.get("entry_id")
            if rid:
                keep_recids.add(str(rid))

    # FETCH — aggregate across every fetched manifest. The pipeline writes one per part
    # (``part-NNN.fetched.jsonl``, matched by ``*.fetched.jsonl``); a standalone ``fetch``
    # writes a single ``fetched.jsonl`` (Zenodo) / ``nomad_fetched.jsonl`` (NOMAD) — NB those
    # do NOT match ``*.fetched.jsonl`` (no dot before "fetched"), so they are listed explicitly.
    # DEDUPE BY recid: within one harvest the part sidecars hold DISJOINT recids (round-robin
    # split), so deduping == summing; but if fetched manifests from more than one flow coexist
    # under the tree (e.g. a leftover `--parts-dir`, or the standalone fetched.jsonl beside the
    # pipeline's part sidecars), the SAME recid appears twice and a naive sum over-counts
    # (observed as a >100% FETCH figure). Counting distinct recids is correct in both cases.
    fglobs = fetched_globs or ["*.fetched.jsonl", "fetched.jsonl"]
    fetched_files = sorted({p for g in fglobs for p in manifests_dir.rglob(g)})
    n_fetched = n_calc_units = 0
    seen_recids: set[str] = set()
    for p in fetched_files:
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
                    # calc_ids are "<source>:<recid>:<path>" (source is zenodo/nomad/…), so
                    # the record id is the middle segment; fall back to the whole id otherwise.
                    parts = cid.split(":")
                    rid = parts[1] if len(parts) >= 3 else parts[0]
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
    rej_paths = [manifests_dir / "rejections.jsonl", dataset_dir / "rejections.jsonl"]
    rej_paths += [manifests_dir / n for n in extra_rejection_names]
    for rp in rej_paths:
        if rp.is_file():
            for rec in read_jsonl(rp):
                n_rej += 1
                rej_by_reason[str(rec.get("reason", "?"))] += 1
                rej_by_stage[str(rec.get("stage", "?"))] += 1
                rid = rec.get("id")
                # A record-level fetch rejection (per-file ids are "<recid>:<key>",
                # parse ids are "<source>:<recid>:<path>" — both carry ':'). The stage is
                # "fetch" (Zenodo) or "nomad_fetch" (NOMAD), so match on the suffix.
                if (str(rec.get("stage", "")).endswith("fetch")
                        and isinstance(rid, str) and ":" not in rid):
                    fetch_reject_recids.add(rid)

    # RECORD-level progress: a record is "done" once it is fetched (yielded VASP) OR already
    # present in the dataset, and "attempted" once done or record-level rejected at fetch. A
    # record parsed in an EARLIER run is in the dataset but ABSENT from the current fetched
    # manifests — the global dataset-skip means it is never re-fetched, and a clean restart
    # (parts dir cleared) resets those per-part sidecars — so it must be counted done from the
    # dataset, not the manifest. Without this, a resume that keeps the dataset reports the whole
    # dataset as "untouched". In a single continuous run parsed_recids ⊆ seen_recids (a no-op).
    done_recids = seen_recids | parsed_recids
    attempted_recids = done_recids | fetch_reject_recids
    if keep_recids:  # count only records actually on the keep-list (no stale-recid overshoot)
        attempted_recids &= keep_recids
    n_attempted = len(attempted_recids)
    n_done_in_keep = len(done_recids & keep_recids) if keep_recids else n_fetched
    n_fetch_rejected = max(0, n_attempted - n_done_in_keep)
    n_untouched = max(0, n_keep - n_attempted)
    # A calc cannot be parsed unless it was first fetched, so the dataset's parsed-calc count is
    # a lower bound on calc-units fetched. Use it as the FETCH/PARSE denominator when the fetched
    # manifest was reset on restart, else parsed/fetched exceeds 100% (the >100% artefact). In a
    # continuous run n_calc_units (manifest sum) >= n_calcs, so this leaves that case unchanged.
    n_calc_units_known = max(n_calc_units, n_calcs)

    return {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "data": {"manifests": str(manifests_dir), "raw": str(raw_dir),
                 "dataset": str(dataset_dir)},
        "discover": {"candidates": n_candidates,
                     "files": [p.name for p in cand_files]},
        "triage": {"keep": n_keep},
        "fetch": {"fetched_records": n_done_in_keep, "to_fetch": n_keep,
                  "pct": _pct(n_done_in_keep, n_keep), "calc_units": n_calc_units_known},
        "records": {"keep": n_keep, "attempted": n_attempted,
                    "pct": _pct(n_attempted, n_keep), "fetched": n_done_in_keep,
                    "fetch_rejected": n_fetch_rejected, "untouched": n_untouched,
                    "with_frames": len(parsed_recids)},
        "parse": {"calcs_parsed": n_calcs, "calc_units_fetched": n_calc_units_known,
                  "pct": _pct(n_calcs, n_calc_units_known), "frames": n_frames,
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

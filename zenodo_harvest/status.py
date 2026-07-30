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


def _pct(num: float, den: float) -> float | None:
    return (100.0 * num / den) if den else None


def status_report(
    *,
    manifests_dir: str | Path,
    raw_dir: str | Path,
    dataset_dir: str | Path,
    keep_path: str | Path | None = None,
    max_disk_bytes: int | None = None,
    max_disk_files: int | None = None,
) -> dict[str, Any]:
    """Build the machine-readable status snapshot (see module docstring)."""
    manifests_dir, raw_dir, dataset_dir = Path(manifests_dir), Path(raw_dir), Path(dataset_dir)

    # DISCOVER — deduplicated candidate manifest(s); exclude the raw <out>.hits.jsonl checkpoint.
    cand_files = [p for p in sorted(manifests_dir.glob("candidates*.jsonl"))
                  if not p.name.endswith(".hits.jsonl")]
    n_candidates = sum(_count_nonempty_lines(p) for p in cand_files)

    # TRIAGE — the keep-list is the fetch denominator.
    keep = Path(keep_path) if keep_path else manifests_dir / "keep.jsonl"
    n_keep = _count_nonempty_lines(keep)

    # FETCH — sum across every *.fetched.jsonl (pipeline writes one per part; a standalone
    # `fetch` writes a single manifests/fetched.jsonl). rglob covers both.
    n_fetched = n_calc_units = 0
    for p in sorted(manifests_dir.rglob("*.fetched.jsonl")):
        for rec in read_jsonl(p):
            n_fetched += 1
            n_calc_units += int(rec.get("n_calc_units", 0) or 0)

    # PARSE — one metadata line per parsed calc; frames come from each calc's quality block.
    meta = dataset_dir / "metadata.jsonl"
    n_calcs = n_frames = n_frames_forces = 0
    if meta.is_file():
        for rec in read_jsonl(meta):
            n_calcs += 1
            q = rec.get("quality") or {}
            n_frames += int(q.get("n_frames", 0) or 0)
            n_frames_forces += int(q.get("n_frames_with_forces", 0) or 0)

    # STORE — shard count + total dataset footprint.
    shards = sorted(dataset_dir.glob("shard-*.extxyz.gz"))
    ds_bytes, _, _ = _dir_usage(dataset_dir)

    # STAGING — raw tree usage vs the /rds quota (bytes AND inodes).
    raw_bytes, raw_files, raw_dirs = _dir_usage(raw_dir)
    inodes = raw_files + raw_dirs

    # ERRORS — rejections carry a machine reason; fetch logs to manifests/, parse to the
    # dataset dir (per-task in the array model, but the pipeline uses the shared one).
    rej_by_reason: Counter = Counter()
    rej_by_stage: Counter = Counter()
    n_rej = 0
    for rp in (manifests_dir / "rejections.jsonl", dataset_dir / "rejections.jsonl"):
        if rp.is_file():
            for rec in read_jsonl(rp):
                n_rej += 1
                rej_by_reason[str(rec.get("reason", "?"))] += 1
                rej_by_stage[str(rec.get("stage", "?"))] += 1

    return {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "data": {"manifests": str(manifests_dir), "raw": str(raw_dir),
                 "dataset": str(dataset_dir)},
        "discover": {"candidates": n_candidates,
                     "files": [p.name for p in cand_files]},
        "triage": {"keep": n_keep},
        "fetch": {"fetched_records": n_fetched, "to_fetch": n_keep,
                  "pct": _pct(n_fetched, n_keep), "calc_units": n_calc_units},
        "parse": {"calcs_parsed": n_calcs, "calc_units_fetched": n_calc_units,
                  "pct": _pct(n_calcs, n_calc_units), "frames": n_frames,
                  "frames_with_forces": n_frames_forces},
        "store": {"shards": len(shards), "dataset_bytes": ds_bytes},
        "staging": {"bytes": raw_bytes, "files": raw_files, "dirs": raw_dirs,
                    "inodes": inodes,
                    "max_disk_bytes": max_disk_bytes, "max_disk_files": max_disk_files,
                    "pct_bytes": _pct(raw_bytes, max_disk_bytes) if max_disk_bytes else None,
                    "pct_inodes": _pct(inodes, max_disk_files) if max_disk_files else None},
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
    lines = [f"harvest status @ {r['generated']}   ({r['data']['dataset']})",
             f"DISCOVER  candidates: {d['candidates']:,}",
             f"TRIAGE    keep-list:  {t['keep']:,}",
             f"FETCH     fetched:    {f['fetched_records']:,} / {f['to_fetch']:,}  "
             f"({_pctstr(f['pct'])})   calc_units: {f['calc_units']:,}",
             f"PARSE     parsed:     {p['calcs_parsed']:,} / {p['calc_units_fetched']:,} calc_units  "
             f"({_pctstr(p['pct'])})   frames: {p['frames']:,}",
             f"STORE     shards: {s['shards']:,}   dataset: {_h(s['dataset_bytes'])}"]
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

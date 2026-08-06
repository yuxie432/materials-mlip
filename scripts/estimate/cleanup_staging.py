#!/usr/bin/env python3
"""Reclaim raw/ inodes that purge-raw structurally CANNOT: the parsed-calc RESIDUAL
(KPOINTS/OSZICAR + dir inodes left after purge frees a unit's role-slotted files), STRAY
files (extracted but referenced by no calc unit), and ORPHAN record trees left by
interrupted / scancelled fetches (recid absent from every fetched.jsonl).

DRY-RUN BY DEFAULT — prints a summary and writes an audit list, deletes nothing. Pass
--apply to delete. It NEVER touches a pending or terminally-rejected ('recoverable') unit's
directory, nor any file an unparsed unit references, so every parse/retry recovery source
survives. --drop-recoverable additionally removes the failed-calc trees (LAST RESORT: their
frames then need a re-download to recover).

Run ONLY with the harvest job STOPPED (`squeue -u $USER` empty): an in-flight fetch looks
like an orphan and an in-flight parse looks pending. Orphan dirs modified within
--inflight-min are skipped as a second guard.

    python scripts/estimate/cleanup_staging.py                 # dry-run (safe)
    python scripts/estimate/cleanup_staging.py --apply         # delete
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from zenodo_harvest.parse import _PARSE_TERMINAL_REJECT_REASONS, _calc_id, _resolve


def load_jsonl(path: Path):
    if not path.is_file():
        return
    with open(path) as fh:
        for ln in fh:
            ln = ln.strip()
            if ln:
                try:
                    yield json.loads(ln)
                except json.JSONDecodeError:
                    pass


def _is_within(root: Path, p: Path) -> bool:
    try:
        p.relative_to(root)
        return root != p
    except ValueError:
        return False


def plan_record(child: Path, unit_status: dict[Path, str], keep_files: set[Path]):
    """Return (files_to_delete[(cat,path)], dirs_to_delete[path]) for a fetched record.

    A file is deleted when its nearest ancestor calc-unit dir is fully parsed ('redundant')
    or it sits under no unit dir at all ('stray'), and it is not referenced by a kept unit.
    A directory is deleted only when every entry under it is deleted (computed bottom-up),
    so a dir holding any kept ('pending'/'recoverable') unit is never removed.
    """
    del_files: list[tuple[str, Path]] = []
    del_dirs: list[Path] = []
    fully_del: set[Path] = set()

    def file_cat(fp: Path) -> str | None:
        if fp in keep_files:
            return None
        p = fp.parent
        while True:
            if p in unit_status:
                return "redundant" if unit_status[p] == "parsed" else None
            if p == child or p.parent == p:
                break
            p = p.parent
        return "stray"  # under no unit dir

    for dirpath, dirnames, filenames in os.walk(child, topdown=False):
        dp = Path(dirpath)
        all_del = True
        for fn in filenames:
            fp = dp / fn
            cat = file_cat(fp)
            if cat is not None:
                del_files.append((cat, fp))
            else:
                all_del = False
        for dn in dirnames:
            if (dp / dn) not in fully_del:
                all_del = False
        if all_del and _is_within(child.parent, dp):
            fully_del.add(dp)
            del_dirs.append(dp)
    return del_files, del_dirs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    data = os.environ.get("ZENODO_HARVEST_DATA", "data")
    ap.add_argument("--raw", default=str(Path(data) / "raw"))
    ap.add_argument("--dataset", default=str(Path(data) / "dataset"))
    ap.add_argument("--manifests", default=str(Path(data) / "manifests"))
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run)")
    ap.add_argument("--drop-recoverable", action="store_true",
                    help="ALSO delete failed-calc recovery sources (last resort)")
    ap.add_argument("--inflight-min", type=int, default=30,
                    help="skip orphan dirs modified within N minutes (in-flight guard)")
    ap.add_argument("--audit", default=None, help="write a JSONL list of deletions here")
    ap.add_argument("--force", action="store_true",
                    help="apply even if a parse lock is present (NOT recommended)")
    args = ap.parse_args()

    raw, ds, man = Path(args.raw), Path(args.dataset), Path(args.manifests)
    if not raw.is_dir():
        print(f"raw dir not found: {raw}", file=sys.stderr)
        return 1

    # Hard safety gate: never delete under a running parse/pipeline. A live parse is
    # mid-reading calc files; deleting one produces a spurious parse error and lost data.
    lock = ds / ".parse.lock"
    if args.apply and lock.exists() and not args.force:
        print(f"REFUSING --apply: {lock} exists — a parse/pipeline may be running. Stop the "
              f"job (squeue -u $USER) first, or pass --force if you are certain it is idle.",
              file=sys.stderr)
        return 2

    parsed = {r["calc_id"] for r in load_jsonl(ds / "metadata.jsonl") if r.get("calc_id")}
    terminal = {r["id"] for r in load_jsonl(ds / "rejections.jsonl")
                if r.get("stage") == "parse"
                and r.get("reason") in _PARSE_TERMINAL_REJECT_REASONS
                and isinstance(r.get("id"), str)}
    fetched: dict[str, dict] = {}
    for p in sorted(man.rglob("*.fetched.jsonl")):
        for rec in load_jsonl(p):
            if rec.get("recid"):
                fetched.setdefault(str(rec["recid"]), rec)  # dedupe overlapping part manifests

    now = time.time()
    freed = collections.Counter()          # inodes by category
    n_recs = collections.Counter()
    n_preserved_dirs = 0                    # recoverable/pending unit dirs kept
    n_safety_violations = 0                 # deletions under a kept unit dir (must stay 0)
    audit = open(args.audit, "w") if args.audit else None
    RANK = {"pending": 2, "recoverable": 1}  # any kept unit -> keep the dir

    for child in sorted(raw.iterdir()):
        if not child.is_dir():
            continue
        recid = child.name
        rec = fetched.get(recid)

        if rec is None:  # ORPHAN — whole tree, unless possibly in-flight
            if (now - child.stat().st_mtime) < args.inflight_min * 60:
                n_recs["orphan_skipped_inflight"] += 1
                continue
            n = sum(1 for _ in child.rglob("*")) + 1
            freed["orphan"] += n
            n_recs["orphan"] += 1
            if audit:
                audit.write(json.dumps({"recid": recid, "category": "orphan",
                                        "path": str(child), "inodes": n}) + "\n")
            if args.apply:
                shutil.rmtree(child, ignore_errors=True)
            continue

        base_meta = {"provenance": rec["provenance"],
                     "_extracted_root": str(_resolve(raw, rec["local_dir"]) / "extracted")}
        unit_status: dict[Path, str] = {}
        keep_files: set[Path] = set()
        for unit in rec.get("calc_units", []):
            resolved = {k: str(_resolve(raw, v)) for k, v in unit.items()}
            cid = _calc_id(resolved, base_meta)
            udir = _resolve(raw, unit["dir"])
            files = {_resolve(raw, v) for k, v in unit.items() if k != "dir"}
            if cid in parsed:
                st = "parsed"
            elif cid in terminal and not args.drop_recoverable:
                st = "recoverable"
            elif cid in terminal:            # --drop-recoverable: treat as removable
                st = "parsed"
            else:
                st = "pending"
            if st in ("pending", "recoverable"):
                keep_files |= files          # protect files a kept unit references
            if RANK.get(st, 0) >= RANK.get(unit_status.get(udir, ""), 0):
                unit_status[udir] = st       # a kept unit in a shared dir wins

        del_files, del_dirs = plan_record(child, unit_status, keep_files)

        # SAFETY SELF-CHECK: no deletion may fall under a recoverable/pending unit dir.
        kept_dirs = [d for d, st in unit_status.items() if st in ("recoverable", "pending")]
        n_preserved_dirs += len(kept_dirs)
        for _cat, fp in del_files:
            if any(_is_within(kd, fp) or fp == kd for kd in kept_dirs) or fp in keep_files:
                n_safety_violations += 1
                print(f"  !! SAFETY VIOLATION: would delete {fp} under a kept unit — "
                      f"skipping this record", file=sys.stderr)
        if any(any(_is_within(kd, fp) or fp == kd for kd in kept_dirs) or fp in keep_files
               for _cat, fp in del_files):
            continue  # refuse to touch a record whose plan overlaps a recovery source

        if not del_files and not del_dirs:
            continue
        n_recs["cleaned"] += 1
        for cat, fp in del_files:
            freed[cat] += 1
            if audit:
                audit.write(json.dumps({"recid": recid, "category": cat,
                                        "path": str(fp)}) + "\n")
            if args.apply:
                try:
                    fp.unlink()
                except OSError:
                    pass
        freed["dirs_pruned"] += len(del_dirs)
        if args.apply:
            for d in sorted(del_dirs, key=lambda p: len(p.parts), reverse=True):
                try:
                    d.rmdir()
                except OSError:
                    pass

    if audit:
        audit.close()
    total = freed["redundant"] + freed["stray"] + freed["orphan"] + freed["dirs_pruned"]
    mode = "APPLIED" if args.apply else "DRY-RUN (nothing deleted; pass --apply)"
    print(f"cleanup {mode}   raw={raw}")
    print(f"  redundant (parsed residual): {freed['redundant']:>10,} inodes")
    print(f"  stray     (no calc unit):    {freed['stray']:>10,} inodes")
    print(f"  orphan    (not in fetched):  {freed['orphan']:>10,} inodes  "
          f"({n_recs['orphan']} records; {n_recs['orphan_skipped_inflight']} skipped in-flight)")
    print(f"  empty dirs pruned:           {freed['dirs_pruned']:>10,} inodes")
    if args.drop_recoverable:
        print("  (--drop-recoverable: failed-calc recovery sources INCLUDED)")
    print(f"  TOTAL reclaimable:           {total:>10,} inodes   "
          f"across {n_recs['cleaned']} fetched + {n_recs['orphan']} orphan records")
    print(f"  SAFETY: {n_preserved_dirs:,} recoverable/pending calc dirs preserved; "
          f"{n_safety_violations} deletions under a recovery source "
          f"({'OK' if n_safety_violations == 0 else 'ABORTED those records'})")
    if args.audit:
        print(f"  audit list: {args.audit}")
    return 1 if n_safety_violations else 0


if __name__ == "__main__":
    raise SystemExit(main())

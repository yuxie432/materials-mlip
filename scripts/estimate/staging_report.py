#!/usr/bin/env python3
"""Read-only composition report of the raw/ staging tree, by INODE (the binding CSD3
quota). Maps every file/dir to a reclaim tier so you can see where the inodes live and
what is safe to remove, BEFORE deleting anything.

It classifies each staged calc unit exactly as purge/parse do (reusing
``parse._calc_id`` / ``_resolve`` and the terminal-reject reason set), so "already in the
dataset" here means the same thing purge means:

  redundant    calc IS in the dataset            -> safe to delete, zero data loss
                                                    (purge-raw already reclaims these)
  recoverable  calc terminally rejected at parse  -> deletable, but recovery needs a
                                                    re-download + re-parse (the NEB tail)
  pending      fetched, not yet parsed/rejected   -> WAITING or in-flight; KEEP
  structural   ancestor dirs of calc units        -> tree skeleton; pruned when its calcs
                                                    are removed (NOT independently deletable)
  stray        files under no calc unit           -> leftover archives / junk -> deletable
  orphan       record dir not in any fetched.jsonl-> leftover (guard: could be in-flight)

Safe to run against a LIVE job (no writes, no lock). The tree walk over hundreds of
thousands of inodes on Lustre takes a few minutes; bytes are only summed with --with-bytes.

    python scripts/estimate/staging_report.py --top 25
"""
from __future__ import annotations

import argparse
import collections
import json
import os
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


# When a directory is shared by several calc units, keep the tree if anything in it is not
# yet safely in the dataset (pending > recoverable > redundant).
_RANK = {"pending": 3, "recoverable": 2, "redundant": 1}


def walk_counts(root: Path, unit_status: dict[Path, str], skeleton: set[Path],
                with_bytes: bool) -> tuple[collections.Counter, collections.Counter]:
    """(inodes_by_category, bytes_by_category) for one record tree.

    An inode inside a calc-unit dir takes that unit's status; an ancestor dir of some
    calc unit is 'structural'; anything else is a genuine 'stray' leftover.
    """
    inodes: collections.Counter = collections.Counter()
    nbytes: collections.Counter = collections.Counter()

    def cat(path: Path, is_dir: bool) -> str:
        p = path if is_dir else path.parent
        while True:
            if p in unit_status:
                return unit_status[p]
            if p == root or p.parent == p:
                break
            p = p.parent
        return "structural" if (is_dir and path in skeleton) else "stray"

    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        pth = Path(e.path)
                        if e.is_dir(follow_symlinks=False):
                            inodes[cat(pth, True)] += 1
                            stack.append(pth)
                        elif e.is_file(follow_symlinks=False):
                            c = cat(pth, False)
                            inodes[c] += 1
                            if with_bytes:
                                nbytes[c] += e.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return inodes, nbytes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    data = os.environ.get("ZENODO_HARVEST_DATA", "data")
    ap.add_argument("--raw", default=str(Path(data) / "raw"))
    ap.add_argument("--dataset", default=str(Path(data) / "dataset"))
    ap.add_argument("--manifests", default=str(Path(data) / "manifests"))
    ap.add_argument("--top", type=int, default=25, help="top N records by inode count")
    ap.add_argument("--limit", type=int, default=800_000, help="inode quota for %% display")
    ap.add_argument("--with-bytes", action="store_true", help="also sum bytes (slower)")
    ap.add_argument("--inflight-min", type=int, default=30,
                    help="an orphan dir touched within N min is flagged possibly in-flight")
    args = ap.parse_args()

    raw, ds, man = Path(args.raw), Path(args.dataset), Path(args.manifests)

    parsed = {r["calc_id"] for r in load_jsonl(ds / "metadata.jsonl") if r.get("calc_id")}
    terminal = {r["id"] for r in load_jsonl(ds / "rejections.jsonl")
                if r.get("stage") == "parse"
                and r.get("reason") in _PARSE_TERMINAL_REJECT_REASONS
                and isinstance(r.get("id"), str)}

    fetched: dict[str, dict] = {}
    for p in sorted(man.rglob("*.fetched.jsonl")):
        for rec in load_jsonl(p):
            if rec.get("recid"):
                fetched.setdefault(str(rec["recid"]), rec)

    now = time.time()
    per_record: list[dict] = []
    tot_inodes: collections.Counter = collections.Counter()
    tot_bytes: collections.Counter = collections.Counter()
    status_counts: collections.Counter = collections.Counter()

    if not raw.is_dir():
        print(f"raw dir not found: {raw}", file=sys.stderr)
        return 1

    for child in sorted(raw.iterdir()):
        if not child.is_dir():
            continue
        recid = child.name
        rec = fetched.get(recid)

        unit_status: dict[Path, str] = {}
        skeleton: set[Path] = set()
        n_parsed = n_failed = n_pending = 0
        if rec is not None:
            base_meta = {"provenance": rec["provenance"],
                         "_extracted_root": str(_resolve(raw, rec["local_dir"]) / "extracted")}
            for unit in rec.get("calc_units", []):
                resolved = {k: str(_resolve(raw, v)) for k, v in unit.items()}
                cid = _calc_id(resolved, base_meta)
                if cid in parsed:
                    st, n_parsed = "redundant", n_parsed + 1
                elif cid in terminal:
                    st, n_failed = "recoverable", n_failed + 1
                else:
                    st, n_pending = "pending", n_pending + 1
                udir = _resolve(raw, unit["dir"])
                if _RANK.get(st, 0) >= _RANK.get(unit_status.get(udir, ""), 0):
                    unit_status[udir] = st
            for udir in unit_status:  # ancestor dirs form the (non-deletable) skeleton
                a = udir.parent
                while a != child and a.parent != a:
                    skeleton.add(a)
                    a = a.parent

        inodes, nbytes = walk_counts(child, unit_status, skeleton, args.with_bytes)
        rec_inodes = sum(inodes.values())

        if rec is None:
            status = "orphan"
            inodes = collections.Counter({"orphan": rec_inodes})
        elif n_pending == 0 and n_failed == 0 and n_parsed > 0:
            status = "fully_parsed"
        elif n_parsed == 0 and n_pending == 0 and n_failed > 0:
            status = "fully_failed"
        elif n_parsed == 0 and n_failed == 0:
            status = "waiting"
        else:
            status = "partial"
        status_counts[status] += 1
        tot_inodes.update(inodes)
        tot_bytes.update(nbytes)

        per_record.append({
            "recid": recid, "status": status, "inodes": rec_inodes,
            "cats": dict(inodes), "n_parsed": n_parsed, "n_failed": n_failed,
            "n_pending": n_pending,
            "inflight": (now - child.stat().st_mtime) < args.inflight_min * 60,
            "title": ((rec or {}).get("provenance", {}) or {}).get("title", "")[:46],
        })

    grand = sum(tot_inodes.values())
    print(f"STAGING COMPOSITION  raw={raw}")
    print(f"  {grand:,} inodes / {args.limit:,} limit = {100*grand/max(args.limit,1):.1f}%"
          f"   ({len(per_record)} record dirs on disk)\n")

    order = ["pending", "recoverable", "redundant", "structural", "stray", "orphan"]
    labels = {
        "pending": "pending      (waiting/in-flight — KEEP)",
        "recoverable": "recoverable  (failed calc; re-download to recover)",
        "redundant": "redundant    (in dataset — safe delete; purge-raw)",
        "structural": "structural   (calc-tree skeleton; pruned with its calcs)",
        "stray": "stray        (non-calc files: leftover archives/junk)",
        "orphan": "orphan       (record not in fetched — leftover)"}
    print("inodes by reclaim category:")
    for k in order:
        v = tot_inodes.get(k, 0)
        b = f"  {tot_bytes.get(k,0)/1e9:6.1f} GB" if args.with_bytes else ""
        print(f"  {labels[k]:54s} {v:>10,}  ({100*v/max(grand,1):4.1f}%){b}")

    print("\nrecords by status:")
    for k in ["waiting", "partial", "fully_parsed", "fully_failed", "orphan"]:
        print(f"  {k:14s} {status_counts.get(k,0):>6,} records")

    print(f"\ntop {args.top} records by inodes:")
    print(f"  {'inodes':>9}  {'recid':>10}  {'status':13} "
          f"{'redun/recov/pend':18}  title")
    for r in sorted(per_record, key=lambda x: x["inodes"], reverse=True)[:args.top]:
        c = r["cats"]
        mix = f"{c.get('redundant',0)}/{c.get('recoverable',0)}/{c.get('pending',0)}"
        flag = " *IN-FLIGHT?" if r["inflight"] and r["status"] == "orphan" else ""
        print(f"  {r['inodes']:>9,}  {r['recid']:>10}  {r['status']:13} {mix:18}  "
              f"{r['title']}{flag}")

    t1 = tot_inodes.get("stray", 0) + tot_inodes.get("orphan", 0)
    t2 = tot_inodes.get("redundant", 0)
    t3 = tot_inodes.get("recoverable", 0)
    keep = tot_inodes.get("pending", 0) + tot_inodes.get("structural", 0)
    print("\nRECLAIM PLAN (inodes):")
    print(f"  tier 1  zero-risk leftovers (stray+orphan): {t1:>10,}  "
          f"-> stop job (or exclude *IN-FLIGHT*), then delete")
    print(f"  tier 2  redundant (in dataset, no data loss): {t2:>10,}  "
          f"-> `purge-raw` (live-safe); near 0 means purge is keeping up")
    print(f"  tier 3  recoverable (needs re-download):      {t3:>10,}  "
          f"-> LAST RESORT; keeps the NEB/failed recovery source")
    print(f"  KEEP    pending + structural:                 {keep:>10,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

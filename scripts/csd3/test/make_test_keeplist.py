#!/usr/bin/env python
"""Shrink a triaged keep-list to a small, predictable subset for the smoke test.

The production ``triage`` keep-list can hold thousands of records of every size. For a
smoke test we want a handful of SMALL records that are known to contain parseable VASP,
so fetch+parse finish in a couple of minutes and the frame/metadata output is predictable.

By default this picks the ``--count`` smallest records that triage CONFIRMED expose a
primary VASP output (``primary_vasp_files`` non-empty — i.e. a vasprun.xml/OUTCAR was seen
directly or via the zip peek), keeping the cumulative size under ``--max-mb``. Pass
``--recids`` to force an explicit curated set instead.

It also prints the footprint hints the safety-valve test needs (total size and the largest
single record), and warns that these are ADVERTISED archive sizes — the extracted footprint
(what the disk valve actually charges) is typically 1-4x larger.

Run from the repo root (so ``import`` works) or with the repo on PYTHONPATH.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in", dest="in_path", required=True, help="triaged keep-list JSONL")
    ap.add_argument("--out", dest="out_path", required=True, help="small keep-list to write")
    ap.add_argument("--count", type=int, default=8, help="how many records to keep (default 8)")
    ap.add_argument("--max-mb", type=float, default=60.0,
                    help="cap cumulative advertised size (MB) of the picked records (default 60)")
    ap.add_argument("--recids", default=None,
                    help="comma-separated recids to force (overrides the smallest-N heuristic)")
    ap.add_argument("--require-confirmed", action="store_true", default=True,
                    help="only pick records with a confirmed primary VASP file (default on)")
    ap.add_argument("--any-rank", dest="require_confirmed", action="store_false",
                    help="allow any rank>=3 record (may include unconfirmed archives)")
    args = ap.parse_args()

    records = [json.loads(l) for l in Path(args.in_path).read_text().splitlines() if l.strip()]
    if not records:
        print(f"ERROR: {args.in_path} is empty — run discover+triage first.", file=sys.stderr)
        return 2

    if args.recids:
        wanted = {r.strip() for r in args.recids.split(",") if r.strip()}
        picks = [c for c in records if c["recid"] in wanted]
        missing = wanted - {c["recid"] for c in picks}
        if missing:
            print(f"WARNING: requested recids not in keep-list (skipped): {sorted(missing)}",
                  file=sys.stderr)
    else:
        pool = [c for c in records
                if (not args.require_confirmed) or c.get("primary_vasp_files")]
        pool.sort(key=lambda c: c.get("bytes_total", 0))
        picks, running = [], 0
        for c in pool:
            mb = c.get("bytes_total", 0) / 1e6
            if picks and running + mb > args.max_mb:
                break
            picks.append(c)
            running += mb
            if len(picks) >= args.count:
                break

    if not picks:
        print("ERROR: no records matched — try --any-rank or a bigger --count/--max-mb.",
              file=sys.stderr)
        return 2

    out = Path(args.out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(c) + "\n" for c in picks))

    sizes = sorted((c.get("bytes_total", 0), c["recid"]) for c in picks)
    total_mb = sum(s for s, _ in sizes) / 1e6
    largest_mb, largest_id = (sizes[-1][0] / 1e6, sizes[-1][1])
    print(f"wrote {len(picks)} records -> {out}")
    print(f"  recids: {[c['recid'] for c in picks]}")
    print(f"  advertised total: {total_mb:.1f} MB   largest single: {largest_mb:.1f} MB "
          f"(recid {largest_id})")
    print("  NB advertised = archive bytes; extracted footprint (what the disk valve charges) "
          "is typically 1-4x larger.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

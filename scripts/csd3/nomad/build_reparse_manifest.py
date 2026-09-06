#!/usr/bin/env python3
"""Build a fetched-manifest SUBSET for re-parsing a chosen class of parse rejections.

Selects the dataset's parse rejections by ``--reason`` (and optional ``--signature`` substring
in the rejection ``detail``), collects those recids (minus ``--exclude``), and greps the
pipeline's per-part ``*.fetched.jsonl`` for those records into a small fetched manifest. The
staged primaries are still on disk (a parse failure never purges), so re-parsing needs NO
re-fetch — feed the output to ``parse`` on a big-RAM node.

Pure stdlib (no harvest-package import), pure local file I/O (no network) — safe to run on a
login node. Idempotent.

Examples
--------
    # #4 primary_too_large, excluding the 8.51 GB monster:
    python scripts/csd3/nomad/build_reparse_manifest.py --reason primary_too_large \
        --exclude dhOiawS2QOf-3IH1c7bYLS2RN4OR \
        --out $NOMAD_HARVEST_DATA/manifests/nomad_bigvasprun_fetched.jsonl

    # #3 the numeric-ALGO pymatgen-bug calcs:
    python scripts/csd3/nomad/build_reparse_manifest.py --reason vasprun_parse_error \
        --signature "has no attribute 'lower'" \
        --out $NOMAD_HARVEST_DATA/manifests/nomad_int_algo_fetched.jsonl
"""
from __future__ import annotations

import argparse
import glob
import json
import os
from pathlib import Path


def main() -> int:
    user = os.environ["USER"]
    nomad = Path(os.environ.get("NOMAD_HARVEST_DATA", f"/rds/user/{user}/hpc-work/nomad"))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--reason", required=True, help="rejection reason to select (e.g. primary_too_large)")
    ap.add_argument("--signature", default=None,
                    help="only rows whose 'detail' contains this substring (e.g. \"has no attribute 'lower'\")")
    ap.add_argument("--exclude", default="", help="comma-separated entry_ids to leave out")
    ap.add_argument("--rejections", default=str(nomad / "dataset" / "rejections.jsonl"))
    ap.add_argument("--parts-dir", default=str(nomad / "manifests" / "nomad_keep.pipeline_parts"))
    ap.add_argument("--out", required=True, help="output fetched-manifest subset path")
    args = ap.parse_args()

    exclude = {e for e in args.exclude.split(",") if e}
    rej_path = Path(args.rejections)
    if not rej_path.is_file():
        print(f"ERROR: {rej_path} not found")
        return 2

    recids: set[str] = set()
    for line in rej_path.open():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("reason") != args.reason:
            continue
        if args.signature and args.signature not in str(r.get("detail", "")):
            continue
        eid = str(r["id"]).split(":")[1]
        if eid not in exclude:
            recids.add(eid)
    print(f"selected recids (reason={args.reason}"
          f"{', signature=' + repr(args.signature) if args.signature else ''}"
          f"{', excl ' + str(len(exclude)) if exclude else ''}): {len(recids)}")

    part_files = sorted(glob.glob(str(Path(args.parts_dir) / "*.fetched.jsonl")))
    if not part_files:
        print(f"ERROR: no part fetched manifests under {args.parts_dir}")
        return 2

    seen: set[str] = set()
    n = 0
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as o:
        for p in part_files:
            for line in open(p):
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                rid = rec.get("recid")
                if rid in recids and rid not in seen:
                    o.write(line + "\n")
                    seen.add(rid)
                    n += 1
    print(f"wrote {n} fetched records -> {out_path}")
    missing = recids - seen
    if missing:
        print(f"WARNING: {len(missing)} recids not found in part manifests "
              f"(already purged, or a different --parts split): {sorted(missing)[:5]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

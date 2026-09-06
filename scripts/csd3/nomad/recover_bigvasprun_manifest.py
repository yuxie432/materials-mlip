#!/usr/bin/env python3
"""Build a fetched-manifest subset of the ``primary_too_large`` calcs for a big-RAM re-parse.

The campaign skipped vasprun.xml files whose UNCOMPRESSED size exceeded the RAM cap
(``--max-primary-bytes``, finally 1.6 GB), logging ``primary_too_large`` and — because a
parse failure never purges — leaving the staged file on disk. So they can be re-parsed with
NO re-fetch, just more RAM.

This collects the ``primary_too_large`` recids from the dataset's rejections.jsonl (EXCLUDING
any in ``EXCLUDE`` — e.g. the single 8.51 GB monster, deferred to its own higher-RAM run) and
greps the pipeline's per-part ``*.fetched.jsonl`` for those records, writing a small fetched
manifest. Feed it to ``parse --max-primary-bytes 0`` on a himem node (see recover_bigvasprun.sh).

Recids already in the dataset (the ~30 that fit once the cap was raised) are harmless: parse
skips them via its done-calc_ids resume set. Idempotent.
"""
from __future__ import annotations

import glob
import json
import os
from pathlib import Path

USER = os.environ["USER"]
NOMAD = Path(os.environ.get("NOMAD_HARVEST_DATA", f"/rds/user/{USER}/hpc-work/nomad"))
REJ = NOMAD / "dataset" / "rejections.jsonl"
PARTS = NOMAD / "manifests" / "nomad_keep.pipeline_parts"
OUT = NOMAD / "manifests" / "nomad_bigvasprun_fetched.jsonl"

# Entry_ids to defer to a separate, higher-RAM run. dhOiawS2QOf... is the 8.51 GB vasprun
# (~90-100 GB peak at pymatgen's ~10-12x); run it alone later with more cpus-per-task.
EXCLUDE = {"dhOiawS2QOf-3IH1c7bYLS2RN4OR"}


def main() -> int:
    recids: set[str] = set()
    for line in REJ.open():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("reason") == "primary_too_large":
            eid = str(r["id"]).split(":")[1]
            if eid not in EXCLUDE:
                recids.add(eid)
    print(f"primary_too_large recids (excl. {len(EXCLUDE)} deferred monster): {len(recids)}")

    part_files = sorted(glob.glob(str(PARTS / "*.fetched.jsonl")))
    if not part_files:
        print(f"ERROR: no part fetched manifests under {PARTS}")
        return 2
    seen: set[str] = set()
    n = 0
    with OUT.open("w") as o:
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
    print(f"wrote {n} fetched records -> {OUT}")
    missing = recids - seen
    if missing:
        print(f"WARNING: {len(missing)} recids not found in part manifests "
              f"(already purged, or fetched under a different split): {sorted(missing)[:5]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

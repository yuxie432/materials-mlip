#!/usr/bin/env python3
"""Prepare a TARGETED re-run of the net-properties recovery for specific records, reusing the
existing recovery pipeline (script 47). Use after a code fix that changes the computed
`electronic`/convergence values for some records (e.g. the vaspout+OUTCAR net-charge fallback).

Unlike a first run, the map is already complete and every shard is marked applied, so a plain
re-submit does nothing. This helper surgically rewinds JUST the target records so the pipeline
recomputes and re-applies only them:

  1. removes the target records' calc_ids from the Phase-1 map (net_properties.jsonl)  -> compute
     recomputes them (with the fixed code) instead of skipping them as done;
  2. removes the target recids from net_properties_fetched.jsonl + deletes their raw dirs   -> fetch
     restages their primaries (incl. the co-located OUTCARs the fix reads);
  3. prunes the shards those records touch (read from metadata `shards`) out of the
     .net_properties_applied marker  -> Phase 2 re-applies ONLY those shards (idempotent
     strip-then-append updates the changed values; other calcs in the shard are rewritten to
     identical bytes); the metadata rewrite (Phase 2b) runs once and is idempotent.

Everything it edits is backed up (<file>.bak.redo). It ALSO snapshots the current (already-
recovered) metadata to metadata.jsonl.bak.pre_redo so you can roll back the re-run alone
(the script-47 backups predate the FIRST recovery). Dry-run by default; pass --apply to write.

    python scripts/csd3/prep_net_properties_redo.py \
        --recids 16448106 \
        --dataset-dir "$ZENODO_HARVEST_DATA/dataset" \
        --map        "$ZENODO_HARVEST_DATA/manifests/net_properties.jsonl" \
        --fetched    "$ZENODO_HARVEST_DATA/manifests/net_properties_fetched.jsonl" \
        --raw-dir    "$ZENODO_HARVEST_DATA/raw_net_properties" \
        --apply

Then re-submit the recovery (Phase 1 restages+recomputes the target records, gate passes at
map==dataset, Phase 2 re-applies only the pruned shards):
    RESUBMIT=1 sbatch scripts/csd3/47_net_properties_recover.sh
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recids", required=True, help="comma-separated recids, or a file of recids")
    ap.add_argument("--dataset-dir", required=True)
    ap.add_argument("--map", required=True)
    ap.add_argument("--fetched", required=True)
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--marker", default=None, help="default <dataset-dir>/.net_properties_applied")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()

    p = Path(args.recids)
    recids = ({ln.strip() for ln in p.read_text().splitlines() if ln.strip()}
              if p.is_file() else {r.strip() for r in args.recids.split(",") if r.strip()})
    ds = Path(args.dataset_dir)
    meta = ds / "metadata.jsonl"
    marker = Path(args.marker) if args.marker else ds / ".net_properties_applied"
    print(f"target recids: {sorted(recids)}\n")

    # 1. from metadata: calc_ids + shards belonging to the target records
    target_calc_ids: set[str] = set()
    target_shards: set[str] = set()
    n_calcs = 0
    for ln in meta.open():
        if not ln.strip():
            continue
        r = json.loads(ln)
        cid = r.get("calc_id")
        if cid and cid.split(":")[1] in recids:
            n_calcs += 1
            target_calc_ids.add(cid)
            for s in (r.get("shards") or []):
                target_shards.add(s)
    print(f"metadata: {n_calcs} calcs across {len(target_shards)} shards")
    print(f"  shards: {sorted(target_shards)}")

    # 2. plan edits
    map_path = Path(args.map)
    map_keep = map_drop = 0
    for ln in map_path.open():
        if not ln.strip():
            continue
        if json.loads(ln).get("calc_id") in target_calc_ids:
            map_drop += 1
        else:
            map_keep += 1
    fetched = Path(args.fetched)
    f_keep, f_drop = [], []
    for ln in fetched.read_text().splitlines():
        if not ln.strip():
            continue
        rid = str(json.loads(ln).get("recid") or (json.loads(ln).get("provenance") or {}).get("record_id"))
        (f_drop if rid in recids else f_keep).append(ln)
    marker_lines = marker.read_text().split() if marker.is_file() else []
    m_keep = [s for s in marker_lines if s not in target_shards]
    m_drop = [s for s in marker_lines if s in target_shards]
    raw = Path(args.raw_dir)
    raw_dirs = [raw / r for r in recids if (raw / r).is_dir()]

    print(f"\nmap  {map_path.name}: drop {map_drop} calc_ids, keep {map_keep}")
    print(f"fetched: drop {len(f_drop)} recids, keep {len(f_keep)}")
    print(f"marker: drop {len(m_drop)} shards, keep {len(m_keep)}  (dropped: {sorted(m_drop)})")
    print(f"raw dirs to delete: {[str(d) for d in raw_dirs]}")

    if not args.apply:
        print("\nDRY-RUN — pass --apply to write.")
        return

    # 3. apply, backing up everything
    shutil.copy2(meta, meta.with_name(meta.name + ".bak.pre_redo"))
    shutil.copy2(map_path, map_path.with_suffix(map_path.suffix + ".bak.redo"))
    with map_path.open() as fh:
        kept = [ln for ln in fh if ln.strip() and json.loads(ln).get("calc_id") not in target_calc_ids]
    map_path.write_text("".join(kept))
    shutil.copy2(fetched, fetched.with_suffix(fetched.suffix + ".bak.redo"))
    fetched.write_text("\n".join(f_keep) + ("\n" if f_keep else ""))
    if marker.is_file():
        shutil.copy2(marker, marker.with_suffix(marker.suffix + ".bak.redo"))
        marker.write_text("\n".join(m_keep) + ("\n" if m_keep else ""))
    for d in raw_dirs:
        shutil.rmtree(d)
    print("\nAPPLIED. Backed up metadata->*.bak.pre_redo, map/fetched/marker->*.bak.redo.")
    print("Now: RESUBMIT=1 sbatch scripts/csd3/47_net_properties_recover.sh")


if __name__ == "__main__":
    main()

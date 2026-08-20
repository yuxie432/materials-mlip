#!/usr/bin/env python3
"""Prepare a targeted re-fetch of the net-properties Phase-1 stragglers.

Given a list of recids (from diagnose_net_properties_shortfall.py's --out-refetch), this:
  1. drops those recids' lines from net_properties_fetched.jsonl  (so fetch stops skipping them),
  2. deletes their staged dirs under raw_net_properties/<recid>/    (start each clean),
  3. (optional) drops their lines from net_properties_rejections.jsonl (tidy; not required).
The Phase-1 map (net_properties.jsonl) is LEFT UNTOUCHED — compute is resumable by calc_id, so the
re-fetched records simply add their missing calcs to the existing map.

Backs up every file it edits (<file>.bak.refetch). Dry-run by default; pass --apply to write.

    python scripts/csd3/prep_net_properties_refetch.py \
        --recids   "$ZENODO_HARVEST_DATA/manifests/net_properties_refetch_recids.txt" \
        --fetched  "$ZENODO_HARVEST_DATA/manifests/net_properties_fetched.jsonl" \
        --raw-dir  "$ZENODO_HARVEST_DATA/raw_net_properties" \
        --rejections "$ZENODO_HARVEST_DATA/manifests/net_properties_rejections.jsonl" \
        --apply

Then re-submit the recovery: RESUBMIT=1 sbatch scripts/csd3/47_net_properties_recover.sh
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def read_recids(p: Path) -> set[str]:
    return {ln.strip() for ln in p.read_text().splitlines() if ln.strip()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recids", required=True, help="file with one recid per line")
    ap.add_argument("--fetched", required=True)
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--rejections", default=None)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()

    recids = read_recids(Path(args.recids))
    print(f"targeting {len(recids)} recid(s) for re-fetch: {sorted(recids)}\n")

    # 1. filter fetched.jsonl
    fetched = Path(args.fetched)
    kept, dropped = [], []
    for ln in fetched.read_text().splitlines():
        if not ln.strip():
            continue
        rec = json.loads(ln)
        rid = str(rec.get("recid") or (rec.get("provenance") or {}).get("record_id"))
        (dropped if rid in recids else kept).append(ln)
    print(f"fetched.jsonl: {len(kept)+len(dropped)} lines -> keep {len(kept)}, drop {len(dropped)}")

    # 2. raw dirs to delete
    raw = Path(args.raw_dir)
    to_delete = [raw / r for r in recids if (raw / r).is_dir()]
    print(f"raw dirs to delete: {[str(p) for p in to_delete]}")

    # 3. rejections filter (optional)
    rej_kept = rej_dropped = 0
    rej = Path(args.rejections) if args.rejections else None
    rej_lines = None
    if rej and rej.is_file():
        rej_lines = []
        for ln in rej.read_text().splitlines():
            if not ln.strip():
                continue
            rid = str(json.loads(ln).get("id", "")).split(":")[0]
            if rid in recids:
                rej_dropped += 1
            else:
                rej_lines.append(ln)
                rej_kept += 1
        print(f"rejections.jsonl: keep {rej_kept}, drop {rej_dropped}")

    if not args.apply:
        print("\nDRY-RUN — pass --apply to write the changes above.")
        return

    shutil.copy2(fetched, fetched.with_suffix(fetched.suffix + ".bak.refetch"))
    fetched.write_text("\n".join(kept) + ("\n" if kept else ""))
    for p in to_delete:
        shutil.rmtree(p)
    if rej and rej.is_file() and rej_lines is not None:
        shutil.copy2(rej, rej.with_suffix(rej.suffix + ".bak.refetch"))
        rej.write_text("\n".join(rej_lines) + ("\n" if rej_lines else ""))
    print("\nAPPLIED. Now re-submit: RESUBMIT=1 sbatch scripts/csd3/47_net_properties_recover.sh")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Pin down exactly which dataset calcs the net-properties Phase-1 map is missing, and
classify each missing record as re-fetchable (a transient/truncated download) or a genuine
persistent extract failure.

Run on CSD3 from the repo root (needs the dataset's metadata.jsonl, which is not rsynced):

    python scripts/csd3/diagnose_net_properties_shortfall.py \
        --dataset-dir  "$ZENODO_HARVEST_DATA/dataset" \
        --map          "$ZENODO_HARVEST_DATA/manifests/net_properties.jsonl" \
        --fetched      "$ZENODO_HARVEST_DATA/manifests/net_properties_fetched.jsonl" \
        --rejections   "$ZENODO_HARVEST_DATA/manifests/net_properties_rejections.jsonl" \
        --out-missing  "$ZENODO_HARVEST_DATA/manifests/net_properties_missing_calc_ids.txt" \
        --out-refetch  "$ZENODO_HARVEST_DATA/manifests/net_properties_refetch_recids.txt"

It writes two files:
  * net_properties_missing_calc_ids.txt  — the exact dataset calc_ids absent from the map
  * net_properties_refetch_recids.txt    — the recids to re-fetch (missing calcs NOT explained
                                            by a persistent extract failure)
and prints a per-record report.
"""
from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path


def load_jsonl(p: Path):
    with open(p) as fh:
        for ln in fh:
            ln = ln.strip()
            if ln:
                yield json.loads(ln)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-dir", required=True)
    ap.add_argument("--map", required=True)
    ap.add_argument("--fetched", required=True)
    ap.add_argument("--rejections", default=None)
    ap.add_argument("--out-missing", default=None)
    ap.add_argument("--out-refetch", default=None)
    args = ap.parse_args()

    meta_path = Path(args.dataset_dir) / "metadata.jsonl"

    # 1. dataset calc_ids (+ parser, +n_frames) and map calc_ids
    dataset = {}   # calc_id -> {"parser","recid","n_frames"}
    for r in load_jsonl(meta_path):
        cid = r.get("calc_id")
        if not cid:
            continue
        recid = cid.split(":")[1] if ":" in cid else "?"
        dataset[cid] = {
            "parser": r.get("parser"),
            "recid": recid,
            "n_frames": (r.get("quality") or {}).get("n_frames"),
        }
    map_ids = {r["calc_id"] for r in load_jsonl(Path(args.map)) if r.get("calc_id")}

    missing = [c for c in dataset if c not in map_ids]
    extra = [c for c in map_ids if c not in dataset]   # map ids not in dataset (should be 0)

    print(f"dataset calcs      : {len(dataset)}")
    print(f"map calcs          : {len(map_ids)}")
    print(f"MISSING (in ds,     not in map): {len(missing)}")
    print(f"EXTRA   (in map,    not in ds ): {len(extra)}  (expected 0)")
    print()

    # 2. group missing by recid; count frames + parser mix at risk
    by_rec = collections.defaultdict(lambda: {"n": 0, "frames": 0, "parsers": collections.Counter()})
    for cid in missing:
        d = dataset[cid]
        e = by_rec[d["recid"]]
        e["n"] += 1
        e["frames"] += d["n_frames"] or 0
        e["parsers"][d["parser"]] += 1

    # 3. per-record fetched-calc-unit count (how many units the re-fetch actually restaged)
    fetched_units = collections.Counter()
    for rec in load_jsonl(Path(args.fetched)):
        rid = str((rec.get("provenance") or {}).get("record_id"))
        fetched_units[rid] += len(rec.get("calc_units", []))

    # 4. rejection reasons per record (persistent vs transient)
    PERSISTENT = {"extract_error"}   # BadZipFile / unsupported compression etc. won't fix on retry
    rej_by_rec = collections.defaultdict(lambda: collections.Counter())
    rej_detail = collections.defaultdict(list)
    if args.rejections and Path(args.rejections).is_file():
        for r in load_jsonl(Path(args.rejections)):
            rid = str(r.get("id", "")).split(":")[0]
            reason = r.get("reason", "?")
            rej_by_rec[rid][reason] += 1
            if reason in PERSISTENT:
                rej_detail[rid].append(f'{r.get("id")} :: {r.get("detail")}')

    # 5. per-record report. EVERY record with missing calcs goes on the re-fetch list — a
    #    re-fetch is idempotent and harmless: if it succeeds the calc is recovered, if it fails
    #    identically (a genuinely unsupported-compression archive) the calc simply stays missing
    #    and you invoke ALLOW_INCOMPLETE=1 for that residue. The PERSISTENT tag is advisory only
    #    (it flags records with an extract_error worth eyeballing in the detail lines below).
    print(f"{'recid':>12}  {'miss':>5} {'frames':>8} {'fetched_u':>9}  reasons / parsers")
    refetch = []
    n_persistent = 0
    for rid, e in sorted(by_rec.items(), key=lambda kv: -kv[1]["n"]):
        reasons = rej_by_rec.get(rid) or {}
        persistent = any(k in PERSISTENT for k in reasons)
        n_persistent += persistent
        parsers = ",".join(f"{k}:{v}" for k, v in e["parsers"].most_common())
        tag = "extract-err(see detail)" if persistent else "re-fetch"
        refetch.append(rid)
        print(f"{rid:>12}  {e['n']:>5} {e['frames']:>8} {fetched_units.get(rid,0):>9}  "
              f"[{tag}] reasons={dict(reasons)} parsers={{{parsers}}}")
        for line in rej_detail.get(rid, []):
            print(f"                 extract-error: {line}")

    print()
    print(f"records with missing calcs (ALL go on the re-fetch list): {len(refetch)}")
    print(f"  of which carry an extract_error worth eyeballing       : {n_persistent}")
    print(f"missing calcs total: {len(missing)}  (frames at risk: {sum(e['frames'] for e in by_rec.values())})")

    if args.out_missing:
        Path(args.out_missing).write_text("\n".join(sorted(missing)) + "\n")
        print(f"\nwrote missing calc_ids -> {args.out_missing}")
    if args.out_refetch:
        Path(args.out_refetch).write_text("\n".join(sorted(refetch)) + "\n")
        print(f"wrote re-fetch recids  -> {args.out_refetch}")


if __name__ == "__main__":
    main()

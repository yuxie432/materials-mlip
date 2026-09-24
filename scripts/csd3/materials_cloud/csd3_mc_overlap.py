#!/usr/bin/env python
"""Are Materials Cloud records flagged as overlapping a harvested Zenodo record REAL duplicates?

discover flags an MC record when it links to (``zenodo_linked_in_dataset``) or is titled like
(``zenodo_title_similar``) a record already in the Zenodo dataset — it never drops it, because every
MC→Zenodo link seen is ``IsSupplementTo`` and the file sets differ (2026-09-24: Kavanagh's
Sn2SbS2I3 = an AiiDA archive on MC vs raw calc folders + notebooks on Zenodo; ACE-GCN = the full
OUTCAR data on MC vs a 31 MB code snapshot on Zenodo). This settles it at the CALC level, after the
MC pipeline has parsed the records: every calc of each side is fingerprinted from its frames —

  exact key : (formula, n_atoms, n_frames, first + final REF_energy to 1e-6 eV, POTCAR set hash)
  near key  : (formula, n_atoms, final REF_energy per atom to 1e-4 eV)

— and the report gives, per flagged pair, the calcs on each side, how many are exact / near
duplicates of a calc on the other side, and examples. Exact duplicates are the same VASP run
published twice (dedupe at training time); near-only matches are the same system re-run.

Run on CSD3 AFTER the MC pipeline (a login node is fine — it reads only the shards those calcs sit in):

    python scripts/csd3/materials_cloud/csd3_mc_overlap.py \\
        --mc-root $MC_HARVEST_DATA --zenodo-dataset $ZENODO_HARVEST_DATA/dataset
    # -> $MC_HARVEST_DATA/manifests/mc_overlap.json  (+ a summary on stdout)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from zenodo_harvest.manifest import read_jsonl  # noqa: E402

_ZEN_ID_RE = re.compile(r"\(zenodo:(\d+)\)")


def flagged_pairs(candidates: Path) -> dict[str, set[str]]:
    """MC record id -> the Zenodo record ids its overlap flags point at."""
    pairs: dict[str, set[str]] = {}
    for c in read_jsonl(candidates):
        ov = c.get("overlap") or {}
        z = {m.group(1) for s in ov.get("zenodo_linked_in_dataset") or []
             for m in [_ZEN_ID_RE.search(str(s))] if m}
        z |= {str(x.get("zenodo_record_id")) for x in ov.get("zenodo_title_similar") or []}
        if z:
            pairs[str(c["recid"])] = z
    return pairs


def calcs_of(metadata: Path, record_ids: set[str]) -> dict[str, dict[str, Any]]:
    """calc_id -> metadata record, for calcs whose provenance.record_id is in ``record_ids``
    (a streaming scan with a substring prefilter, so a multi-GB metadata.jsonl stays cheap)."""
    out: dict[str, dict[str, Any]] = {}
    if not metadata.is_file():
        return out
    needles = list(record_ids)
    with metadata.open() as fh:
        for line in fh:
            if not any(n in line for n in needles):
                continue
            rec = json.loads(line)
            if str((rec.get("provenance") or {}).get("record_id")) in record_ids:
                out[rec["calc_id"]] = rec
    return out


def fingerprints(dataset: Path, calcs: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """calc_id -> {formula, n_atoms, n_frames, e_first, e_final, potcar} read from its shards."""
    from ase.io import iread
    by_shard: dict[str, set[str]] = defaultdict(set)
    for cid, m in calcs.items():
        for s in m.get("shards") or []:
            by_shard[s].add(cid)
    frames: dict[str, list[tuple[int, float | None, str, int]]] = defaultdict(list)
    for shard, cids in sorted(by_shard.items()):
        path = dataset / shard
        if not path.is_file():
            print(f"  missing shard {path}", file=sys.stderr)
            continue
        for at in iread(str(path), index=":", format="extxyz"):
            cid = at.info.get("calc_id")
            if cid in cids:
                e = at.info.get("REF_energy")
                frames[cid].append((int(at.info.get("ionic_step", 0)),
                                    float(e) if e is not None else None,
                                    at.get_chemical_formula(), len(at)))
    out = {}
    for cid, fr in frames.items():
        fr.sort()
        es = [e for _, e, _, _ in fr if e is not None]
        out[cid] = {"formula": fr[-1][2], "n_atoms": fr[-1][3], "n_frames": len(fr),
                    "e_first": es[0] if es else None, "e_final": es[-1] if es else None,
                    "potcar": (calcs[cid].get("calc_parameters") or {}).get("potcar_set_hash")}
    return out


def _r6(x: float | None) -> float | None:
    return None if x is None else round(x, 6)


def _exact(f: dict[str, Any]) -> tuple:
    return (f["formula"], f["n_atoms"], f["n_frames"], _r6(f["e_first"]), _r6(f["e_final"]),
            f["potcar"])


def _near(f: dict[str, Any]) -> tuple | None:
    if f["e_final"] is None or not f["n_atoms"]:
        return None
    return (f["formula"], f["n_atoms"], round(f["e_final"] / f["n_atoms"], 4))


def compare(mc: dict[str, dict[str, Any]], zen: dict[str, dict[str, Any]]) -> dict[str, Any]:
    z_exact = defaultdict(list)
    z_near = defaultdict(list)
    for cid, f in zen.items():
        z_exact[_exact(f)].append(cid)
        k = _near(f)
        if k:
            z_near[k].append(cid)
    exact, near_only, unique = [], [], []
    for cid, f in sorted(mc.items()):
        if _exact(f) in z_exact:
            exact.append((cid, z_exact[_exact(f)][0]))
        elif _near(f) in z_near:
            near_only.append((cid, z_near[_near(f)][0]))
        else:
            unique.append(cid)
    matched_z = {z for _, z in exact} | {z for _, z in near_only}
    return {"mc_calcs": len(mc), "zenodo_calcs": len(zen),
            "exact_duplicates": len(exact), "near_duplicates_only": len(near_only),
            "mc_unique": len(unique), "zenodo_unmatched": len(set(zen) - matched_z),
            "examples_exact": exact[:10], "examples_near": near_only[:10],
            "examples_mc_unique": unique[:10],
            "examples_zenodo_unmatched": sorted(set(zen) - matched_z)[:10]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--mc-root", required=True, help="$MC_HARVEST_DATA")
    ap.add_argument("--zenodo-dataset", required=True, help="the Zenodo dataset dir")
    ap.add_argument("--out", default=None, help="default: <mc-root>/manifests/mc_overlap.json")
    args = ap.parse_args()
    root = Path(args.mc_root)
    pairs = flagged_pairs(root / "manifests" / "mc_candidates.jsonl")
    print(f"{len(pairs)} flagged MC record(s): {pairs}")
    mc_calcs = calcs_of(root / "dataset" / "metadata.jsonl", set(pairs))
    zen_ids = set().union(*pairs.values()) if pairs else set()
    zen_calcs = calcs_of(Path(args.zenodo_dataset) / "metadata.jsonl", zen_ids)
    mc_fp = fingerprints(root / "dataset", mc_calcs)
    zen_fp = fingerprints(Path(args.zenodo_dataset), zen_calcs)
    report: dict[str, Any] = {}
    for mc_id, zids in sorted(pairs.items()):
        mc_side = {c: f for c, f in mc_fp.items()
                   if str(mc_calcs[c]["provenance"]["record_id"]) == mc_id}
        z_side = {c: f for c, f in zen_fp.items()
                  if str(zen_calcs[c]["provenance"]["record_id"]) in zids}
        report[mc_id] = {"zenodo_records": sorted(zids), **compare(mc_side, z_side)}
        r = report[mc_id]
        print(f"{mc_id} vs zenodo {sorted(zids)}: MC {r['mc_calcs']} calcs / Zenodo "
              f"{r['zenodo_calcs']} -> exact {r['exact_duplicates']}, near-only "
              f"{r['near_duplicates_only']}, MC-unique {r['mc_unique']}, Zenodo-unmatched "
              f"{r['zenodo_unmatched']}")
    out = Path(args.out) if args.out else root / "manifests" / "mc_overlap.json"
    out.write_text(json.dumps(report, indent=1))
    print(f"report -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

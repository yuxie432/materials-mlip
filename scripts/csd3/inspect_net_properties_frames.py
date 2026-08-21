#!/usr/bin/env python3
"""Prove HOW net-properties/convergence are attached to the shards: per-frame vs per-calc.

Reads one (or a few) extxyz.gz shard(s) WITHOUT ASE/pymatgen and reports, from the raw
comment lines:
  * for the first OUTCAR calc and first vasprun calc it meets — a table of every frame's
    (ionic_step, electronic_converged, scf_dE, total_magnetization, total_charge), showing
    that scf_dE/electronic_converged VARY per ionic_step (per-frame) while the two totals are
    CONSTANT across a calc's frames (calc-level values broadcast onto every frame);
  * aggregate per-frame coverage over the shard(s): how many frames carry each key, by parser;
  * confirmation that the per-atom dft_magmom/dft_charge columns are gone.

Run on CSD3:
    python scripts/csd3/inspect_net_properties_frames.py \
        --dataset-dir "$ZENODO_HARVEST_DATA/dataset" --shards 2
"""
from __future__ import annotations
import argparse, glob, gzip, re, collections

C_CALC = re.compile(r'calc_id="([^"]*)"')
C_STEP = re.compile(r'\bionic_step=(\d+)')
C_CONV = re.compile(r'\belectronic_converged=(\S+)')
C_SCF  = re.compile(r'\bscf_dE=(\S+)')
C_MAG  = re.compile(r'\btotal_magnetization=(\S+)')
C_CHG  = re.compile(r'\btotal_charge=(\S+)')
C_PROPS = re.compile(r'Properties=(\S+)')

def parser_of(cid: str) -> str:
    t = cid.rsplit(":", 1)[-1].lower()
    if "vasprun" in t: return "vasprun"
    if t.endswith(".h5") or "vaspout" in t: return "vaspout"
    return "outcar"

def frames(shard):
    with gzip.open(shard, "rt") as fh:
        lines = fh.read().splitlines()
    i = 0
    while i < len(lines):
        try:
            n = int(lines[i].strip())
        except (ValueError, IndexError):
            return
        if i + 2 + n > len(lines):
            return
        yield lines[i + 1], lines[i + 2:i + 2 + n]   # comment, atom_rows
        i += 2 + n

def _shard_index(path):
    m = re.search(r"shard-(\d+)\.extxyz\.gz$", path)
    return int(m.group(1)) if m else -1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-dir", required=True)
    ap.add_argument("--shards", type=int, default=1, help="scan the FIRST N shards (default)")
    ap.add_argument("--indices", default=None,
                    help="comma-separated shard indices to scan, e.g. 0,1200,1211")
    ap.add_argument("--sample", type=int, default=None,
                    help="scan K shards evenly spaced across the whole dataset (mixes parsers)")
    args = ap.parse_args()

    all_shards = sorted(glob.glob(f"{args.dataset_dir}/shard-*.extxyz.gz"), key=_shard_index)
    if args.indices:
        want = {int(x) for x in args.indices.split(",")}
        shard_paths = [s for s in all_shards if _shard_index(s) in want]
    elif args.sample:
        step = max(1, len(all_shards) // args.sample)
        shard_paths = all_shards[::step][: args.sample]
    else:
        shard_paths = all_shards[: args.shards]
    print(f"scanning {len(shard_paths)} of {len(all_shards)} shard(s): "
          f"{[s.split('/')[-1] for s in shard_paths]}\n")

    have = collections.Counter(); tot = collections.Counter()
    dft_cols_seen = 0
    demo = {}                          # parser -> list of per-frame tuples for the first calc
    demo_cid = {}
    for sh in shard_paths:
        for comment, rows in frames(sh):
            m = C_CALC.search(comment)
            cid = m.group(1) if m else "?"
            par = parser_of(cid)
            tot[par] += 1
            if C_MAG.search(comment): have[(par, "total_magnetization")] += 1
            if C_CHG.search(comment): have[(par, "total_charge")] += 1
            if C_CONV.search(comment): have[(par, "electronic_converged")] += 1
            if C_SCF.search(comment): have[(par, "scf_dE")] += 1
            pm = C_PROPS.search(comment)
            if pm and ("dft_magmom" in pm.group(1) or "dft_charge" in pm.group(1)):
                dft_cols_seen += 1
            # capture the first calc of each parser for the per-frame demo table
            if par not in demo_cid and cid != "?":
                demo_cid[par] = cid; demo[par] = []
            if demo_cid.get(par) == cid:
                demo[par].append((
                    (C_STEP.search(comment) or [None, "?"])[1] if C_STEP.search(comment) else "?",
                    (C_CONV.search(comment) or [None, "-"])[1] if C_CONV.search(comment) else "-",
                    (C_SCF.search(comment) or [None, "-"])[1] if C_SCF.search(comment) else "-",
                    (C_MAG.search(comment) or [None, "-"])[1] if C_MAG.search(comment) else "-",
                    (C_CHG.search(comment) or [None, "-"])[1] if C_CHG.search(comment) else "-",
                ))

    for par in demo:
        print(f"=== first {par} calc: {demo_cid[par]}")
        print(f"    {'ionic_step':>10} {'econv':>6} {'scf_dE':>14} {'tot_mag':>12} {'tot_charge':>10}")
        for row in demo[par][:12]:
            print(f"    {row[0]:>10} {row[1]:>6} {row[2]:>14} {row[3]:>12} {row[4]:>10}")
        if len(demo[par]) > 12:
            print(f"    ... ({len(demo[par])} frames total for this calc)")
        print()

    print("=== per-frame coverage over scanned shard(s) ===")
    for par in sorted(tot):
        n = tot[par]
        print(f"  {par}: {n} frames")
        for key in ("total_magnetization", "total_charge", "electronic_converged", "scf_dE"):
            h = have[(par, key)]
            print(f"      {key:>22}: {h:>8} ({100*h/n:.1f}%)")
    print(f"\n  frames still carrying dft_magmom/dft_charge columns: {dft_cols_seen} (want 0)")

if __name__ == "__main__":
    main()

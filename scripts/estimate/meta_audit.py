"""Offline audit of the harvested dataset metadata (dataset_csd3/metadata.jsonl).

Computes completeness, coverage/diversity, and frame-concentration stats WITHOUT
touching the shards. One streaming pass. Prints a compact report + JSON blob.
"""
import json, sys, collections

import os
PATH = sys.argv[1] if len(sys.argv) > 1 else "dataset_csd3/metadata.jsonl"

n_calcs = 0
n_frames = 0
by_parser = collections.Counter()
by_record_frames = collections.Counter()      # frames per record_id
by_calc_frames = []                            # frames per calc (for percentiles)
by_potcar = collections.Counter()             # frames per potcar_set_hash
by_functional = collections.Counter()
records = set()
concept = set()
dois = set()
# completeness of key fields (frame-weighted)
f_null_runtype = 0
f_null_functional = 0
f_null_econv = 0
f_with_forces = 0
f_with_stress = 0
f_scf_unconv = 0
f_dropped = 0
calc_charges = 0
calc_magmoms = 0
# label reliability
max_fme = 0.0
f_bad_fme = 0                                  # frames in calcs with |F-E0|/atom > 0.05 eV
natoms_frames = collections.Counter()          # n_atoms -> frame count (weighted)
avail_any = collections.Counter()              # availability flags (calc-weighted)

for line in open(PATH):
    if not line.strip():
        continue
    r = json.loads(line)
    n_calcs += 1
    q = r.get("quality", {})
    cp = r.get("calc_parameters", {})
    prov = r.get("provenance", {})
    nf = int(q.get("n_frames", 0) or 0)
    n_frames += nf
    rid = prov.get("record_id")
    records.add(rid)
    concept.add(prov.get("conceptrecid"))
    dois.add(prov.get("doi"))
    by_parser[r.get("parser")] += nf
    by_record_frames[rid] += nf
    by_calc_frames.append(nf)
    by_potcar[cp.get("potcar_set_hash")] += nf
    by_functional[cp.get("functional")] += nf
    if cp.get("run_type") is None: f_null_runtype += nf
    if cp.get("functional") is None: f_null_functional += nf
    ec = q.get("electronic_converged")
    if ec is None: f_null_econv += nf
    f_with_forces += int(q.get("n_frames_with_forces", 0) or 0)
    f_with_stress += int(q.get("n_frames_with_stress", 0) or 0)
    f_scf_unconv += int(q.get("n_frames_scf_unconverged", 0) or 0)
    f_dropped += int(q.get("n_frames_dropped_no_energy", 0) or 0)
    if r.get("site_charges_present"): calc_charges += 1
    if r.get("site_magmoms_present"): calc_magmoms += 1
    fme = float(q.get("max_abs_free_minus_e0_per_atom", 0.0) or 0.0)
    max_fme = max(max_fme, fme)
    if fme > 0.05: f_bad_fme += nf
    na = q.get("n_atoms")
    if na is not None: natoms_frames[int(na)] += nf
    for k, v in (r.get("availability") or {}).items():
        if v: avail_any[k] += 1

def pct(x, tot=n_frames): return f"{100*x/tot:.2f}%" if tot else "n/a"

# concentration
top_records = by_record_frames.most_common(15)
calc_sorted = sorted(by_calc_frames, reverse=True)
def cum_frac(sorted_desc, k):
    return sum(sorted_desc[:k]) / n_frames if n_frames else 0

print("="*70)
print(f"CALCS: {n_calcs:,}   FRAMES: {n_frames:,}")
print(f"records(distinct): {len(records)}   concept(distinct): {len(concept)}   dois: {len(dois)}")
print("="*70)
print("\n--- PARSER (frame-weighted) ---")
for k,v in by_parser.most_common(): print(f"  {k:20s} {v:>12,}  {pct(v)}")
print("\n--- COMPLETENESS (frame-weighted) ---")
print(f"  forces present     {f_with_forces:>12,}  {pct(f_with_forces)}")
print(f"  stress present     {f_with_stress:>12,}  {pct(f_with_stress)}")
print(f"  run_type NULL      {f_null_runtype:>12,}  {pct(f_null_runtype)}")
print(f"  functional NULL    {f_null_functional:>12,}  {pct(f_null_functional)}")
print(f"  elec_conv NULL     {f_null_econv:>12,}  {pct(f_null_econv)}")
print(f"  scf UNCONVERGED    {f_scf_unconv:>12,}  {pct(f_scf_unconv)}")
print(f"  dropped_no_energy  {f_dropped:>12,}  {pct(f_dropped)}")
print(f"  |F-E0|/atom>0.05eV {f_bad_fme:>12,}  {pct(f_bad_fme)}   (max seen {max_fme:.4f} eV/atom)")
print(f"  calcs w/ site charges {calc_charges:,}/{n_calcs:,}   site magmoms {calc_magmoms:,}/{n_calcs:,}")
print("\n--- DIVERSITY ---")
print(f"  distinct potcar_set_hash: {len(by_potcar)}")
print(f"  distinct functional:      {len(by_functional)}")
print("\n--- FRAME CONCENTRATION ---")
print(f"  top record       = {pct(top_records[0][1])} of all frames  (record {top_records[0][0]})")
print(f"  top 3 records    = {pct(sum(v for _,v in top_records[:3]))}")
print(f"  top 10 records   = {pct(sum(v for _,v in top_records[:10]))}")
print(f"  top 1 calc       = {pct(calc_sorted[0])}   top 10 calcs = {pct(sum(calc_sorted[:10]))}")
print(f"  calc frames: median={calc_sorted[len(calc_sorted)//2]}  max={calc_sorted[0]}  mean={n_frames//n_calcs}")
print("  top 15 records by frames:")
for rid,v in top_records: print(f"    {rid:>12}  {v:>12,}  {pct(v)}")
print("\n--- TOP POTCAR SETS (frame-weighted) ---")
for k,v in by_potcar.most_common(10): print(f"  {k}  {v:>12,}  {pct(v)}")
print("\n--- n_atoms distribution (frame-weighted, top buckets) ---")
na_items = sorted(natoms_frames.items())
small = sum(v for na,v in na_items if na<=10)
med = sum(v for na,v in na_items if 10<na<=50)
big = sum(v for na,v in na_items if 50<na<=200)
huge = sum(v for na,v in na_items if na>200)
print(f"  <=10 atoms: {pct(small)}   11-50: {pct(med)}   51-200: {pct(big)}   >200: {pct(huge)}")
print(f"  n_atoms range: {na_items[0][0]}..{na_items[-1][0]}")
print("\n--- AVAILABILITY flags (calc-weighted, records with heavy outputs) ---")
for k,v in avail_any.most_common(): print(f"  {k:16s} {v:,} calcs")

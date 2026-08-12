"""Field-availability audit of metadata.jsonl, split by parser (vasprun/vaspout/OUTCAR).

Reports, per parser: fraction of calcs (and frames) that carry each calc_parameter,
each probed INCAR tag (in calc_parameters.incar), each provenance field, and the
availability/site flags. Answers "what is actually collected, for which files."
"""
import json, sys, collections

PATH = sys.argv[1] if len(sys.argv) > 1 else "dataset_csd3/metadata.jsonl"

# top-level calc_parameters keys we care about
CP_KEYS = ["run_type","functional","code_version","spin_polarized","hubbard_u",
           "encut","ediff","ismear","sigma","ispin","kpoints","potcar_symbols",
           "potcar_spec","potcar_set_hash","incar","potcar_titels"]
# INCAR tags (user list + MLIP-relevant), probed inside calc_parameters.incar
INCAR_TAGS = ["ENCUT","ENAUG","PREC","PRECFOCK","LREAL","ADDGRID","LASPH","GGA","METAGGA",
              "LHFCALC","AEXX","HFSCREEN","LDAU","LDAUTYPE","LDAUL","LDAUU","LDAUJ",
              "NKRED","NKREDX","LSORBIT","ISIF","IBRION","NSW","ISMEAR","SIGMA","ISPIN",
              "EDIFF","EDIFFG","KSPACING","ALGO","NELM","ISYM","IVDW","NCORE","NPAR"]
PROV_KEYS = ["record_id","doi","conceptdoi","url","title","creators","license",
             "resource_type","publication_date","keywords"]
AVAIL_KEYS = ["charge_density","spin_density","dos","eigenvalues","projected",
              "local_potential","elf","wavefunction","magnetization"]

# per-parser accumulators
calcs = collections.Counter()
frames = collections.Counter()
cp_present = collections.defaultdict(lambda: collections.Counter())   # parser -> key -> n calcs non-null
incar_present = collections.defaultdict(lambda: collections.Counter())
incar_has_dict = collections.Counter()   # parser -> n calcs that have a non-empty incar dict
prov_present = collections.Counter()
avail_true = collections.Counter()
site_charges = 0; site_magmoms = 0
n_calcs = 0
stress_frames = collections.Counter(); force_frames = collections.Counter()

for line in open(PATH):
    if not line.strip(): continue
    r = json.loads(line); n_calcs += 1
    p = r.get("parser","?")
    cp = r.get("calc_parameters",{}) or {}
    q = r.get("quality",{}) or {}
    nf = int(q.get("n_frames",0) or 0)
    calcs[p]+=1; frames[p]+=nf
    for k in CP_KEYS:
        v = cp.get(k)
        if v not in (None, "", [], {}): cp_present[p][k]+=1
    inc = cp.get("incar") or {}
    if inc: incar_has_dict[p]+=1
    for t in INCAR_TAGS:
        if t in inc and inc[t] is not None: incar_present[p][t]+=1
    prov = r.get("provenance",{}) or {}
    for k in PROV_KEYS:
        if prov.get(k) not in (None,"",[],{}): prov_present[k]+=1
    av = r.get("availability",{}) or {}
    for k in AVAIL_KEYS:
        if av.get(k): avail_true[k]+=1
    if r.get("site_charges_present"): site_charges+=1
    if r.get("site_magmoms_present"): site_magmoms+=1
    force_frames[p]+=int(q.get("n_frames_with_forces",0) or 0)
    stress_frames[p]+=int(q.get("n_frames_with_stress",0) or 0)

parsers = sorted(calcs, key=lambda p:-calcs[p])
def row(label, counts):
    cells = "  ".join(f"{p.split('.')[-1][:8]:>8}:{100*counts[p]/calcs[p]:5.1f}%" for p in parsers)
    print(f"  {label:16s} {cells}")

print(f"TOTAL calcs {n_calcs:,}   frames {sum(frames.values()):,}")
print("parsers:", {p:(calcs[p],frames[p]) for p in parsers})
print("\n=== calc_parameters top-level presence (% of calcs, per parser) ===")
for k in CP_KEYS: row(k, {p:cp_present[p][k] for p in parsers})
print("\n=== full INCAR dict present (% of calcs) ===")
row("incar(non-empty)", {p:incar_has_dict[p] for p in parsers})
print("\n=== INCAR tag presence within calc_parameters.incar (% of calcs, per parser) ===")
for t in INCAR_TAGS: row(t, {p:incar_present[p][t] for p in parsers})
print("\n=== provenance field presence (% of ALL calcs) ===")
for k in PROV_KEYS: print(f"  {k:16s} {100*prov_present[k]/n_calcs:5.1f}%")
print("\n=== availability flags TRUE (% of ALL calcs) ===")
for k in AVAIL_KEYS: print(f"  {k:16s} {100*avail_true[k]/n_calcs:5.1f}%")
print(f"  site_charges_present {100*site_charges/n_calcs:.1f}%   site_magmoms_present {100*site_magmoms/n_calcs:.1f}%")
print("\n=== frame labels (% of frames w/ forces & stress, per parser) ===")
for p in parsers:
    print(f"  {p:18s} forces {100*force_frames[p]/max(frames[p],1):5.1f}%   stress {100*stress_frames[p]/max(frames[p],1):5.1f}%")

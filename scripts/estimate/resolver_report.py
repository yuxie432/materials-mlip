"""Report what the defaults-resolver (`zenodo_harvest.param_resolver`) recovers from a
metadata.jsonl — per-tag source-tier breakdown, split vasprun/vaspout (INCAR-complete)
vs OUTCAR. No network, no re-fetch. Usage: python scripts/estimate/resolver_report.py [metadata.jsonl]"""
import json, sys, collections
from zenodo_harvest.param_resolver import resolve_parameters, TARGET_TAGS

PATH = sys.argv[1] if len(sys.argv) > 1 else "dataset_csd3/metadata.jsonl"
tier = {t: collections.Counter() for t in TARGET_TAGS}
stress = collections.Counter()
n = n_complete = 0
for line in open(PATH):
    if not line.strip():
        continue
    r = json.loads(line); n += 1
    res = resolve_parameters(r.get("calc_parameters"), r.get("quality"))
    n_complete += res["incar_complete"]
    for t in TARGET_TAGS:
        tier[t][res["parameters"][t]["source"]] += 1
    stress[res["derived"]["computes_stress"]["source"]] += 1

print(f"calcs {n:,}   incar_complete {n_complete:,}   outcar {n - n_complete:,}\n")
hdr = f'{"TAG":10s} {"incar":>9} {"default":>9} {"derived":>9} {"not_appl":>9} {"unknown":>9}'
print(hdr)
for t in TARGET_TAGS:
    c = tier[t]
    print(f'{t:10s} {c["incar"]:>9,} {c["default"]:>9,} {c["derived:run_type"]:>9,} '
          f'{c["not_applicable"]:>9,} {c["unknown"]:>9,}')
filled = sum(tier[t]["default"] + tier[t]["derived:run_type"] for t in TARGET_TAGS)
print(f'\ncomputes_stress: {dict(stress)}')
print(f"values filled without re-fetch (default+derived): {filled:,}")

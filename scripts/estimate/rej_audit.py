import json, collections, sys, re

def load(path):
    rows=[]
    for line in open(path):
        line=line.strip()
        if not line: continue
        try: rows.append(json.loads(line))
        except: pass
    return rows

def recid_of(r):
    i = r.get("id") or ""
    # id forms: "<recid>" or "zenodo:<recid>:<path>" or "<recid>:<path>"
    if isinstance(i,str):
        if i.startswith("zenodo:"): return i.split(":")[1]
        return i.split(":")[0]
    return str(i)

PATHS = sys.argv[1:] or ["manifests_csd3/rejections.jsonl", "dataset_csd3/rejections.jsonl"]
for path in PATHS:
    rows = load(path)
    print("="*70)
    print(f"{path}   lines={len(rows):,}")
    print("="*70)
    by_stage = collections.Counter(r.get("stage") for r in rows)
    print("by stage:", dict(by_stage))
    by_reason = collections.Counter(r.get("reason") for r in rows)
    print("\nby reason:")
    for k,v in by_reason.most_common(): print(f"  {k:32s} {v:>8,}")
    # distinct vs duplicate ids
    ids = [r.get("id") for r in rows]
    print(f"\ndistinct ids: {len(set(ids)):,}  (of {len(ids):,} lines -> {len(ids)-len(set(ids)):,} duplicate lines)")
    # concentration by record for the dominant reasons
    print("\ntop records by rejection count (per reason):")
    for reason,_ in by_reason.most_common(6):
        c = collections.Counter(recid_of(r) for r in rows if r.get("reason")==reason)
        tot = sum(c.values()); topn = c.most_common(5)
        share = sum(v for _,v in topn)/tot if tot else 0
        print(f"  [{reason}] {tot:,} rejects across {len(c)} records; top5={share*100:.0f}% -> " +
              ", ".join(f"{rid}:{v}" for rid,v in topn))
    # distinct records touched by rejections
    print(f"\ndistinct records appearing in rejections: {len(set(recid_of(r) for r in rows))}")

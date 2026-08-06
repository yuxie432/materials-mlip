#!/usr/bin/env python3
"""Break down parse-stage rejections (outcar_parse_error, vasprun_parse_error, no_frames,
...) by RECORD and by exception type, to tell a few pathological deposits apart from a
systematic parser regression.

parse rejections are logged with ``id = calc_id = zenodo:<recid>:<primary-path>`` and
``detail = "<ExcType>: <msg>"`` (parse.py), so both the offending record and the ASE/
pymatgen failure mode are recoverable. Records with any unparsed calc are NOT purged, so
their OUTCARs are still on disk under ``raw/<recid>/`` for a hands-on reproduce.

    python scripts/estimate/parse_error_breakdown.py --reason outcar_parse_error --top 15
"""
from __future__ import annotations

import argparse
import collections
import json
import os
from pathlib import Path


def load_jsonl(path: Path):
    if not path.is_file():
        return
    with open(path) as fh:
        for ln in fh:
            ln = ln.strip()
            if ln:
                try:
                    yield json.loads(ln)
                except json.JSONDecodeError:
                    pass


def recid_of(calc_id: str) -> str:
    parts = calc_id.split(":")
    return parts[1] if calc_id.startswith("zenodo:") and len(parts) > 1 else "?"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    data = os.environ.get("ZENODO_HARVEST_DATA", "data")
    ap.add_argument("--dataset", default=str(Path(data) / "dataset"))
    ap.add_argument("--manifests", default=str(Path(data) / "manifests"))
    ap.add_argument("--raw", default=str(Path(data) / "raw"))
    ap.add_argument("--reason", default="outcar_parse_error")
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()

    ds, man, raw = Path(args.dataset), Path(args.manifests), Path(args.raw)

    # recid -> title/url, for context (fetched manifests carry provenance).
    info: dict[str, dict] = {}
    for p in sorted(man.rglob("*.fetched.jsonl")):
        for rec in load_jsonl(p):
            if rec.get("recid"):
                info[str(rec["recid"])] = rec.get("provenance") or {}

    by_recid: collections.Counter = collections.Counter()
    by_exc: collections.Counter = collections.Counter()
    all_parse: collections.Counter = collections.Counter()
    for rec in load_jsonl(ds / "rejections.jsonl"):
        if rec.get("stage") != "parse":
            continue
        all_parse[rec.get("reason", "?")] += 1
        if rec.get("reason") != args.reason:
            continue
        by_recid[recid_of(str(rec.get("id", "")))] += 1
        detail = str(rec.get("detail", ""))
        by_exc[detail.split(":", 1)[0][:40] or "<none>"] += 1

    total = sum(by_recid.values())
    print(f"# parse rejections by reason: {dict(all_parse.most_common())}\n")
    print(f"# '{args.reason}': {total} across {len(by_recid)} distinct records "
          f"(concentration = how few records own most errors)\n")

    print(f"top {args.top} records by '{args.reason}' count:")
    for recid, n in by_recid.most_common(args.top):
        prov = info.get(recid, {})
        title = (prov.get("title") or "")[:60]
        print(f"  {n:6d}  recid {recid:>10}  {title}")
        if prov.get("url"):
            print(f"          {prov['url']}")
    # concentration: what fraction of errors do the top-5 records own?
    top5 = sum(n for _, n in by_recid.most_common(5))
    if total:
        print(f"\n  top-5 records own {top5}/{total} = {100*top5/total:.0f}% of '{args.reason}'")

    print(f"\nexception types in '{args.reason}' detail:")
    for exc, n in by_exc.most_common(10):
        print(f"  {n:6d}  {exc}")

    # Point at the worst record's staged OUTCARs (still on disk — the record wasn't purged).
    if by_recid:
        worst = by_recid.most_common(1)[0][0]
        d = raw / worst
        print(f"\nworst record {worst}: OUTCARs still staged under {d} (record not purged). Try:")
        print(f"  find {d} -iname 'OUTCAR*' | head")
        print(f"  # then reproduce the failure on one (copy out first — ASE trips on "
              f"hash-annotated POTCAR species):")
        print(f"  python -c \"import shutil,tempfile,os; "
              f"from ase.io import read; "
              f"t=tempfile.mkdtemp(); p=shutil.copy('<OUTCAR-path>', t); "
              f"print(len(read(p, format='vasp-out', index=':')))\"")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

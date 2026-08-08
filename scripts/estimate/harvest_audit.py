#!/usr/bin/env python3
"""Per-record harvest integrity audit (fast — reads manifests + metadata + rejections,
does NOT decompress shards). For every fetched record it reconciles its calc units against
the dataset and the parse rejections, and sums its frames, so you can see:

  * total frames recomputed from metadata (cross-checks `status`),
  * records with UNACCOUNTED calcs (fetched but neither parsed nor rejected -> pending, or
    lost if their raw files went missing) — these are re-parseable,
  * FileNotFoundError rejections and which records they hit (a calc whose primary file was
    missing at parse time),
  * the per-record frame distribution (are the recently-fetched records frame-poor?),
  * recids in the keep-list but NOT fetched (the untouched / repeatedly-orphaned records).

    python scripts/estimate/harvest_audit.py
"""
from __future__ import annotations

import collections
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from zenodo_harvest.parse import _calc_id, _resolve


def load(p: Path):
    if not p.is_file():
        return
    with open(p) as fh:
        for ln in fh:
            ln = ln.strip()
            if ln:
                try:
                    yield json.loads(ln)
                except json.JSONDecodeError:
                    pass


def recid_of(cid: str) -> str:
    parts = cid.split(":")
    return parts[1] if cid.startswith("zenodo:") and len(parts) > 1 else "?"


def main() -> int:
    data = Path(os.environ.get("ZENODO_HARVEST_DATA", "data"))
    MAN, DS, RAW = data / "manifests", data / "dataset", data / "raw"

    meta_frames: dict[str, int] = {}
    for r in load(DS / "metadata.jsonl"):
        if r.get("calc_id"):
            meta_frames[r["calc_id"]] = int((r.get("quality") or {}).get("n_frames", 0) or 0)

    rej_reason: dict[str, str] = {}
    fnf: list[str] = []                       # calc_ids whose rejection detail is FileNotFoundError
    for r in load(DS / "rejections.jsonl"):
        if r.get("stage") == "parse" and isinstance(r.get("id"), str):
            rej_reason[r["id"]] = str(r.get("reason"))
            if "FileNotFoundError" in str(r.get("detail", "")):
                fnf.append(r["id"])

    recs: dict[str, dict] = {}
    for p in sorted(MAN.rglob("*.fetched.jsonl")):
        for rec in load(p):
            if rec.get("recid"):
                recs.setdefault(str(rec["recid"]), rec)

    rows = []
    tot_frames = tot_unacc = 0
    for recid, rec in recs.items():
        bm = {"provenance": rec["provenance"],
              "_extracted_root": str(_resolve(RAW, rec["local_dir"]) / "extracted")}
        expected = []
        for u in rec.get("calc_units", []):
            try:
                expected.append(_calc_id({k: str(_resolve(RAW, v)) for k, v in u.items()}, bm))
            except Exception:
                pass
        nparsed = sum(1 for c in expected if c in meta_frames)
        frames = sum(meta_frames.get(c, 0) for c in expected)
        nrej = sum(1 for c in expected if c in rej_reason)
        unacc = len(expected) - nparsed - nrej
        rows.append((frames, recid, len(expected), nparsed, nrej, unacc))
        tot_frames += frames
        tot_unacc += max(0, unacc)
    rows.sort(reverse=True)

    keep = {str(c["recid"]) for c in load(MAN / "keep.jsonl") if c.get("recid")}
    not_fetched = sorted(keep - set(recs))
    fnf_recids = collections.Counter(recid_of(c) for c in fnf)

    print(f"fetched records:              {len(recs)}")
    print(f"total frames (recomputed):    {tot_frames:,}   (compare to `status` frames)")
    print(f"total UNACCOUNTED calcs:      {tot_unacc:,}   (fetched but not parsed/rejected -> pending/lost)")
    print(f"FileNotFoundError rejections: {len(fnf)}   across {len(fnf_recids)} records")
    print(f"in keep-list but NOT fetched: {len(not_fetched)}  -> {not_fetched}")

    unacc_rows = [r for r in rows if r[5] > 0]
    if unacc_rows:
        print(f"\nrecords with UNACCOUNTED calcs ({len(unacc_rows)}) — re-parseable "
              f"(their raw files may still be present):")
        for f, recid, ne, np_, nr, un in unacc_rows[:30]:
            print(f"  recid {recid:>10}: {un:>6} unaccounted / {ne} calcs "
                  f"({np_} parsed, {nr} rejected), {f:,} frames")

    if fnf_recids:
        print(f"\nFileNotFoundError by record (top 15) — a missing primary at parse time:")
        for recid, n in fnf_recids.most_common(15):
            print(f"  recid {recid:>10}: {n} calcs")

    print(f"\ntop 10 fetched records by frames:")
    for f, recid, ne, np_, nr, un in rows[:10]:
        print(f"  recid {recid:>10}: {f:>10,} frames ({np_}/{ne} calcs parsed)")
    print(f"lowest 10 fetched records by frames:")
    for f, recid, ne, np_, nr, un in rows[-10:]:
        print(f"  recid {recid:>10}: {f:>10,} frames ({np_}/{ne} calcs parsed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

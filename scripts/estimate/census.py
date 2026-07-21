"""Exact storage census from one or more candidate manifests.

Everything here is EXACT (no sampling): the Zenodo search API embeds every
record's per-file ``size``, so the discover manifest already carries exact byte
totals for whatever the current scripts found. We reuse ``fetch.py``'s own regexes
so the "what would actually be downloaded" model matches the real pipeline.

For each category, and for the rank>=3 "relevant" subset that triage keeps by
default, reports:
  * n_records, raw_bytes_total    -- full footprint on Zenodo (incl. heavy files
                                     that fetch never downloads)
  * download_bytes(cap)           -- bytes fetch would transfer at a given per-file
                                     cap (archives + directly-exposed VASP files;
                                     heavy direct files are availability-only)
  * a sweep of download_bytes over several caps (the key storage lever)
  * heavy-tail (share of bytes held by the largest K records)

Writes a machine-readable JSON alongside the printed report (for project.py).

Usage:
    python census.py MANIFEST [MANIFEST ...] [--json OUT] [--caps 0.5,2,10,50]
(caps in GB; 0 or omitted "inf" means uncapped)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from zenodo_harvest.fetch import _PARSE_RE, _is_archive  # noqa: E402
from zenodo_harvest.manifest import read_jsonl  # noqa: E402

GB = 1 << 30
TB = 1 << 40
MB = 1 << 20
RELEVANT_CATS = ("vasp_direct", "archive")


def fmt(n: float) -> str:
    if n >= TB:
        return f"{n / TB:.2f} TB"
    if n >= GB:
        return f"{n / GB:.2f} GB"
    return f"{n / MB:.1f} MB"


def load_dedup(paths: list[str]) -> list[dict]:
    """Merge manifests, dedup by conceptrecid (newest recid wins) -- discover's rule."""
    seen: dict[str, dict] = {}
    for p in paths:
        for rec in read_jsonl(p):
            key = rec.get("conceptrecid") or rec["recid"]
            prev = seen.get(key)
            if prev is None or int(rec["recid"]) > int(prev["recid"]):
                seen[key] = rec
    return list(seen.values())


def file_is_downloaded(base: str) -> bool:
    """True if fetch would download this file (an archive, or a direct VASP file)."""
    return _is_archive(base) is not None or bool(_PARSE_RE.search(base))


def download_bytes(rec: dict, cap: int | None) -> int:
    """Bytes fetch would transfer for one record at a per-file cap (None = uncapped)."""
    tot = 0
    for f in rec.get("files", []):
        base = (f.get("key") or "").rsplit("/", 1)[-1]
        size = f.get("size") or 0
        if file_is_downloaded(base) and (cap is None or size <= cap):
            tot += size
    return tot


def fmt_by_format(rec: dict, cap: int | None, acc: Counter) -> None:
    for f in rec.get("files", []):
        base = (f.get("key") or "").rsplit("/", 1)[-1]
        size = f.get("size") or 0
        kind = _is_archive(base)
        is_direct = kind is None and bool(_PARSE_RE.search(base))
        if (kind or is_direct) and (cap is None or size <= cap):
            acc[kind or "direct_vasp"] += size


def summarize(recs: list[dict], caps: list[int | None]) -> dict:
    raw = sum(r.get("bytes_total", 0) for r in recs)
    sweep = {("inf" if c is None else c): sum(download_bytes(r, c) for r in recs) for c in caps}
    fmt_acc: Counter = Counter()
    for r in recs:
        fmt_by_format(r, None, fmt_acc)
    sizes = sorted((r.get("bytes_total", 0) for r in recs), reverse=True)
    tail = {k: sum(sizes[:k]) for k in (1, 5, 10, 25, 50, 100) if sizes}
    return {
        "n_records": len(recs),
        "raw_bytes_total": raw,
        "download_by_cap": sweep,
        "download_by_format_uncapped": dict(fmt_acc),
        "size_max": sizes[0] if sizes else 0,
        "size_median": sizes[len(sizes) // 2] if sizes else 0,
        "heavy_tail_topK_bytes": tail,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("manifests", nargs="+")
    ap.add_argument("--json", default=None, help="write machine-readable summary here")
    ap.add_argument("--caps", default="0.5,2,10,50",
                    help="per-file download caps in GB (comma-sep); 'inf' always added")
    args = ap.parse_args()

    caps: list[int | None] = [int(float(c) * GB) for c in args.caps.split(",") if c.strip()]
    caps.append(None)  # uncapped

    recs = load_dedup(args.manifests)
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for r in recs:
        by_cat[r["vasp_category"]].append(r)

    print(f"\n{'='*82}\nSTORAGE CENSUS over {len(recs)} unique concepts (dedup by conceptrecid)\n{'='*82}")
    order = ["vasp_direct", "archive", "processed_atomistic", "vasp_input_only", "unlikely"]
    print(f"\n{'category':<20}{'n':>7}{'raw':>12}{'dl@0.5GB':>12}{'dl@2GB':>12}{'dl uncap':>12}")
    print("-" * 75)
    for cat in order:
        rs = by_cat.get(cat, [])
        s = summarize(rs, caps) if rs else None
        if not s:
            print(f"{cat:<20}{0:>7}")
            continue
        c05 = s["download_by_cap"].get(int(0.5 * GB), 0)
        c2 = s["download_by_cap"].get(int(2 * GB), 0)
        cinf = s["download_by_cap"]["inf"]
        print(f"{cat:<20}{s['n_records']:>7}{fmt(s['raw_bytes_total']):>12}"
              f"{fmt(c05):>12}{fmt(c2):>12}{fmt(cinf):>12}")

    relevant = [r for c in RELEVANT_CATS for r in by_cat.get(c, [])]
    rs = summarize(relevant, caps)
    print(f"\n{'='*82}\nRELEVANT (rank>=3: vasp_direct + archive) -- kept by default triage --min-rank 3\n{'='*82}")
    print(f"records: {rs['n_records']}    raw footprint: {fmt(rs['raw_bytes_total'])}")
    print(f"largest single record: {fmt(rs['size_max'])}    median: {fmt(rs['size_median'])}")
    print("\nDownload (transfer) volume vs per-file cap:")
    for c in caps:
        label = "uncapped" if c is None else f"{c/GB:g} GB/file"
        key = "inf" if c is None else c
        print(f"  cap {label:>14}:  {fmt(rs['download_by_cap'][key])}")
    print("\nBy archive format (uncapped):")
    for k, v in sorted(rs["download_by_format_uncapped"].items(), key=lambda x: -x[1]):
        print(f"  {k:<14}: {fmt(v)}")
    print("\nHeavy tail (share of relevant raw bytes in the largest K records):")
    for k, v in rs["heavy_tail_topK_bytes"].items():
        share = 100 * v / rs["raw_bytes_total"] if rs["raw_bytes_total"] else 0
        print(f"  top {k:>3}: {fmt(v):>12}  ({share:.1f}%)")

    out = {
        "n_unique_concepts": len(recs),
        "by_category": {c: summarize(by_cat.get(c, []), caps) for c in order},
        "relevant_rank_ge_3": rs,
        "caps_gb": [c / GB if c else None for c in caps],
    }
    if args.json:
        Path(args.json).write_text(json.dumps(out, indent=2))
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()

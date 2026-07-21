"""Measure the raw->dataset storage ratios (and fetch yield) end-to-end on a real,
size-stratified sample of the relevant (rank>=3) records.

Why sampling is required: the final dataset size is NOT proportional to raw archive
bytes. It is (total ionic-step frames) x (compressed bytes/frame). Both
frames-per-record and bytes-per-frame can only be known by actually fetching and
parsing records. Likewise the fetch YIELD -- what fraction of rank>=3 "archive"
records actually contain a parseable primary VASP output at a given size cap -- is
empirical.

Reports (written to <workdir>/ratios.json):
  yield:
    * records_attempted / fetched_ok / parsed_ok   (+ rejection breakdown)
  ratios (over successfully parsed records):
    * extract_ratio        = retained extracted VASP bytes / archive bytes downloaded
    * frames_per_record    (mean/median/max; distribution is heavy-tailed)
    * bytes_per_frame       = compressed extxyz.gz size / frame   <-- dataset multiplier
    * atoms_per_calc, frames_per_calc

Selection is stratified across the relevant set by raw record size (deterministic,
no RNG) so a modest sample spans small single-points through large trajectories.

Usage:
    python sample_storage.py MANIFEST WORKDIR [--n 60] [--cap-gb 2] [--max-total-gb 40]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from zenodo_harvest import config as _cfg  # noqa: E402
from zenodo_harvest.fetch import _PARSE_RE, _is_archive, fetch  # noqa: E402
from zenodo_harvest.manifest import read_jsonl  # noqa: E402
from zenodo_harvest.parse import parse  # noqa: E402

GB = 1 << 30
MB = 1 << 20


def echo(*a):
    print(*a, flush=True)


def download_payload(rec: dict, cap: int) -> int:
    """Sum of sub-cap archive + direct-VASP file bytes (what fetch would transfer)."""
    tot = 0
    for f in rec.get("files", []):
        base = (f.get("key") or "").rsplit("/", 1)[-1]
        size = f.get("size") or 0
        if (_is_archive(base) is not None or bool(_PARSE_RE.search(base))) and size <= cap:
            tot += size
    return tot


def du(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def stats(xs: list) -> dict:
    if not xs:
        return {}
    s = sorted(xs)
    return {"n": len(s), "sum": sum(s), "mean": round(sum(s) / len(s), 2),
            "median": s[len(s) // 2], "min": s[0], "max": s[-1],
            "p90": s[min(len(s) - 1, int(0.9 * len(s)))]}


def rejection_breakdown(path: Path) -> dict:
    from collections import Counter
    c: Counter = Counter()
    if path.is_file():
        for r in read_jsonl(path):
            if r.get("stage") == "fetch":
                c[r.get("reason")] += 1
    return dict(c)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("manifest")
    ap.add_argument("workdir")
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--cap-gb", type=float, default=2.0)
    ap.add_argument("--max-total-gb", type=float, default=40.0,
                    help="stop selecting once cumulative intended download exceeds this")
    ap.add_argument("--env", default=str(Path(__file__).resolve().parents[2] / ".env"))
    args = ap.parse_args()

    _cfg.load_dotenv(args.env)
    cap = int(args.cap_gb * GB)
    work = Path(args.workdir)
    work.mkdir(parents=True, exist_ok=True)

    # candidates: rank>=3 with a real shot at a primary under the cap (a sub-cap
    # archive, or a sub-cap directly-exposed primary). Stratify by raw size.
    recs = [r for r in read_jsonl(args.manifest) if r["vasp_rank"] >= 3]
    recs = [r for r in recs if download_payload(r, cap) >= 1 * MB]
    recs.sort(key=lambda r: r.get("bytes_total", 0))
    if len(recs) > args.n:
        step = len(recs) / args.n
        picks = [recs[int(i * step)] for i in range(args.n)]
    else:
        picks = recs

    # respect a total-download budget
    budget = int(args.max_total_gb * GB)
    chosen, running = [], 0
    for r in picks:
        p = download_payload(r, cap)
        if running + p > budget:
            continue
        chosen.append(r)
        running += p
    picks = chosen

    keep = work / "keep_sample.jsonl"
    with keep.open("w") as fh:
        for r in picks:
            fh.write(json.dumps(r) + "\n")
    echo(f"sample: {len(picks)} rank>=3 records; intended download ~{running/GB:.1f} GB "
         f"(cap {args.cap_gb:g} GB/file)")

    raw, ds = work / "raw", work / "dataset"
    fetched, rej = work / "fetched.jsonl", work / "rejections.jsonl"

    fstats = fetch(str(keep), out_path=str(fetched), raw_dir=str(raw),
                   rejections_path=str(rej), max_bytes=cap)
    echo("FETCH:", json.dumps(fstats))
    retained = du(raw)
    ok_recids = {r["recid"] for r in read_jsonl(fetched)}
    downloaded = sum(download_payload(r, cap) for r in picks if r["recid"] in ok_recids)

    pstats = parse(str(fetched), dataset_dir=str(ds), rejections_path=str(rej), raw_dir=str(raw))
    echo("PARSE:", json.dumps(pstats))
    ds_bytes = du(ds)

    meta = list(read_jsonl(ds / "metadata.jsonl")) if (ds / "metadata.jsonl").is_file() else []
    total_frames = sum(m["quality"]["n_frames"] for m in meta)
    by_rec: dict[str, int] = {}
    for m in meta:
        rid = m["provenance"]["record_id"]
        by_rec[rid] = by_rec.get(rid, 0) + m["quality"]["n_frames"]

    result = {
        "cap_gb": args.cap_gb,
        "yield": {
            "records_attempted": len(picks),
            "records_fetched_ok": fstats["fetched"],
            "records_parsed_ok": len(by_rec),
            "yield_fetch": round(fstats["fetched"] / len(picks), 3) if picks else None,
            "yield_parse": round(len(by_rec) / len(picks), 3) if picks else None,
            "fetch_rejections": rejection_breakdown(rej),
        },
        "ratios": {
            "downloaded_bytes": downloaded,
            "retained_extracted_bytes": retained,
            "extract_ratio_retained_over_downloaded":
                round(retained / downloaded, 4) if downloaded else None,
            "dataset_extxyz_gz_bytes": ds_bytes,
            "total_frames": total_frames,
            "total_calcs": len(meta),
            "bytes_per_frame_compressed": round(ds_bytes / total_frames, 1) if total_frames else None,
            "dataset_bytes_per_downloaded_byte": round(ds_bytes / downloaded, 5) if downloaded else None,
            "frames_per_parsed_record": round(total_frames / len(by_rec), 2) if by_rec else None,
            "frames_per_record_dist": stats(sorted(by_rec.values(), reverse=True)),
            "frames_per_calc_dist": stats([m["quality"]["n_frames"] for m in meta]),
            "atoms_per_calc_dist": stats([m["quality"]["n_atoms"] for m in meta if m["quality"].get("n_atoms")]),
        },
    }
    echo("\n=== STORAGE RATIOS + YIELD ===")
    echo(json.dumps(result, indent=2))
    (work / "ratios.json").write_text(json.dumps(result, indent=2))
    echo(f"\nwrote {work/'ratios.json'}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""Materials Cloud probes to run FROM A CSD3 COMPUTE NODE (the local link is too slow/flaky).

Two independent probes (pick with ``--speed`` / ``--aiida``; default: both):

1. ``--speed`` — the fetch-side numbers that size the harvest job: the per-REQUEST cost (the
   ``/content`` 302 hop, and a 64 KiB Range read through it — what one zip peek or one targeted
   member costs regardless of bandwidth), S3 throughput scaling over 1/2/4/8 parallel streams on
   distinct large files (what ``--workers`` should be), and the API's rate-limit headers.
   Fetch time ≈ bytes_to_fetch (triage summary) ÷ the best aggregate MB/s.

2. ``--aiida`` — the sqlite_zip AiiDA census. Triage extracts VASP from LEGACY AiiDA exports
   (real member names), but a sqlite_zip export (aiida-core ≥2.0) hides every filename in its
   ``db.sqlite3``, so triage reports it as an evidence gap. This probe reads each such export's
   central directory, pulls its ``db.sqlite3`` member over Range (≤ ``--db-max-mb``), and queries
   it for aiida-vasp CalcJobs and for repository entries named ``vasprun.xml``/``OUTCAR``. If it
   finds none, the gap is closed (no VASP hides there); if it finds some, sqlite_zip extraction
   support is worth building (user decision 2026-09-23). Legacy exports are summarised from the
   triage peek cache for completeness.

Usage (repo root, venv active; writes a JSON report):

    srun -A $SBATCH_ACCOUNT -p icelake --cpus-per-task=4 --time=02:00:00 \\
        python scripts/csd3/materials_cloud/csd3_mc_probe.py \\
        --candidates $MC_HARVEST_DATA/manifests/mc_candidates.jsonl \\
        --out $MC_HARVEST_DATA/manifests/mc_probe.json
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # repo root, run without install

from materials_cloud_harvest.client import MaterialsCloudClient, content_url, new_session  # noqa: E402
from materials_cloud_harvest.remote_zip import (  # noqa: E402
    RemoteZipError,
    aiida_format,
    fetch_member,
    read_central_directory,
    zip_evidence,
)
from materials_cloud_harvest.triage import EVIDENCE_RULES_VERSION  # noqa: E402
from zenodo_harvest.manifest import read_jsonl  # noqa: E402

# Distinct large files for the throughput probe (Kristoffersen et al., 10 x ~4.4 GB tar.gz; the
# largest VASP-mentioning record — real harvest traffic). Overridable with --file RECID/KEY.
_SPEED_RECORD = "yspxn-jxt78"


def _lat(samples: list[float]) -> dict:
    xs = sorted(samples)
    if not xs:
        return {}
    return {"n": len(xs), "median_s": round(statistics.median(xs), 3),
            "p90_s": round(xs[min(len(xs) - 1, int(0.9 * len(xs)))], 3),
            "max_s": round(xs[-1], 3)}


def _speed(args: argparse.Namespace) -> dict:
    """Fetch-side sizing: API/redirect latency (the REQUEST cost of peeks + targeted members),
    S3 throughput for 1..N parallel streams on distinct files (the BANDWIDTH cost of the bulk),
    and the rate-limit headers the API advertises."""
    out: dict = {}
    c = MaterialsCloudClient(min_interval=0)
    lat = []
    for _ in range(3):
        t = time.monotonic()
        rec = c.get_record(_SPEED_RECORD)
        lat.append(time.monotonic() - t)
    out["api_get_record"] = _lat(lat)
    t = time.monotonic()
    c.search_page(page=1, size=100)
    out["api_search_page100_s"] = round(time.monotonic() - t, 2)
    s = new_session()
    r = s.get(f"{c.base}/api/records", params={"size": 1}, timeout=120)
    out["rate_limit_headers"] = {k: v for k, v in r.headers.items()
                                 if k.lower().startswith(("x-ratelimit", "retry-after"))}
    files = sorted(((e["key"], e["size"]) for e in rec["files"]["entries"].values()),
                   key=lambda kv: -kv[1])
    targets = args.file or [f"{_SPEED_RECORD}/{k}" for k, _ in files[:max(args.streams, 1)]]
    # per-request cost: the 302 hop alone, then a small Range read through it (what one zip peek /
    # one targeted member costs in latency, independent of bandwidth)
    rid, key = targets[0].split("/", 1)
    hop, small = [], []
    for _ in range(args.latency_n):
        t = time.monotonic()
        r = s.get(content_url(rid, key), allow_redirects=False, timeout=120)
        hop.append(time.monotonic() - t)
        out["presigned_host"] = (r.headers.get("Location") or "")[:40]
    for _ in range(args.latency_n):
        t = time.monotonic()
        with s.get(content_url(rid, key), headers={"Range": "bytes=-65536"}, timeout=120) as r:
            _ = r.content
        small.append(time.monotonic() - t)
    out["redirect_hop"] = _lat(hop)
    out["small_range_read_64KiB"] = _lat(small)
    nbytes = args.mb << 20

    def _pull(target: str) -> tuple[int, float]:
        rid, key = target.split("/", 1)
        sess = new_session()
        t0 = time.monotonic()
        got = 0
        with sess.get(content_url(rid, key), headers={"Range": f"bytes=0-{nbytes - 1}"},
                      stream=True, timeout=300) as resp:
            for chunk in resp.iter_content(1 << 20):
                got += len(chunk)
        return got, time.monotonic() - t0

    scaling = {}
    for n in sorted({1, 2, 4, 8, len(targets)}):
        if n > len(targets):
            continue
        with ThreadPoolExecutor(max_workers=n) as ex:
            t0 = time.monotonic()
            res = list(ex.map(_pull, targets[:n]))
            wall = time.monotonic() - t0
        scaling[str(n)] = {"aggregate_MBps": round(sum(b for b, _ in res) / wall / 1e6, 1),
                           "per_stream_MBps": round(statistics.mean(b / d / 1e6 for b, d in res), 1)}
        print(f"  {n} stream(s): {scaling[str(n)]}", flush=True)
    out["stream_scaling"] = scaling
    out["bytes_per_stream_MB"] = args.mb
    best = max(scaling.items(), key=lambda kv: kv[1]["aggregate_MBps"]) if scaling else None
    if best:
        out["recommended_fetch_workers"] = int(best[0])
    return out


def _query_db(path: Path) -> dict:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cur = con.cursor()
        pts = cur.execute("SELECT process_type, COUNT(*) FROM db_dbnode WHERE process_type IS NOT "
                          "NULL GROUP BY process_type ORDER BY 2 DESC").fetchall()
        n_vasprun = cur.execute("SELECT COUNT(*) FROM db_dbnode WHERE repository_metadata LIKE "
                                "'%vasprun%'").fetchone()[0]
        n_outcar = cur.execute("SELECT COUNT(*) FROM db_dbnode WHERE repository_metadata LIKE "
                               "'%OUTCAR%'").fetchone()[0]
    finally:
        con.close()
    return {"process_types": [(p, n) for p, n in pts[:15]],
            "vasp_calcjobs": sum(n for p, n in pts if p and "vasp" in p.lower()),
            "nodes_with_vasprun": n_vasprun, "nodes_with_outcar": n_outcar}


def _aiida(args: argparse.Namespace) -> dict:
    cands = list(read_jsonl(args.candidates))
    peeks: dict = {}
    cache = Path(args.peek_cache or str(Path(args.candidates).with_name("mc_keep.jsonl"))
                 + ("" if args.peek_cache else ".peeks.jsonl"))
    if cache.is_file():
        for row in read_jsonl(cache):
            # key: "v<rules>\t<recid>\t<file key>\t<size>\t<checksum>" (triage._peek_key); only
            # rows computed under the CURRENT evidence rules are trusted
            parts = str(row.get("k", "")).split("\t")
            if len(parts) >= 3 and parts[0] == f"v{EVIDENCE_RULES_VERSION}":
                peeks[f"{parts[1]}\t{parts[2]}"] = row["v"]
    session = new_session()
    rows = []
    # rows are also appended to <out>.rows.jsonl as they are produced, so a crash or wallclock kill
    # part-way through the census keeps everything measured so far
    rows_fh = open(args.out + ".rows.jsonl", "a") if args.out else None
    for c in cands:
        for f in c.get("files") or []:
            if not str(f["key"]).lower().endswith(".aiida"):
                continue
            row = {"recid": c["recid"], "key": f["key"], "size": f["size"],
                   "vasp_mention": c.get("vasp_mention"), "title": c.get("title", "")[:70]}
            try:
                _aiida_one(args, session, f, row, peeks.get(f"{c['recid']}\t{f['key']}"))
            except Exception as exc:  # noqa: BLE001 - one bad file must not end the census
                row["error"] = f"{type(exc).__name__}: {exc}"[:200]
            rows.append(row)
            if rows_fh:
                rows_fh.write(json.dumps(row) + "\n")
                rows_fh.flush()
            print(json.dumps(row)[:300], flush=True)
    if rows_fh:
        rows_fh.close()
    sq = [r for r in rows if r.get("format") == "sqlite_zip"]
    return {
        "aiida_files": len(rows),
        "by_format": {k: sum(1 for r in rows if r.get("format") == k)
                      for k in ("legacy", "sqlite_zip", None, "unreadable")},
        "errors": sum(1 for r in rows if r.get("error")),
        "legacy_with_vasp": [(r["recid"], r["key"], r.get("n_vasp_primary")) for r in rows
                             if r.get("format") == "legacy" and r.get("n_vasp_primary")],
        "sqlite_zip_with_vasp": [(r["recid"], r["key"], r.get("vasp_calcjobs"),
                                  r.get("nodes_with_vasprun")) for r in sq
                                 if r.get("vasp_calcjobs") or r.get("nodes_with_vasprun")],
        "sqlite_zip_unresolved": [(r["recid"], r["key"], r.get("db")) for r in sq if r.get("db")],
        "files": rows,
    }


def _aiida_one(args: argparse.Namespace, session: requests.Session, f: dict, row: dict,
               cached: dict | None) -> None:
    """Fill ``row`` for one .aiida file (format, VASP counts, db query)."""
    if cached and cached.get("aiida_format") == "legacy":
        # triage already read this export's (possibly 100s-of-MB) central directory
        row.update(format="legacy", n_members=cached.get("n_members"),
                   n_vasp_primary=cached.get("n_primary"), from_triage_cache=True)
        return
    try:
        members, _ = read_central_directory(session, f["download"],
                                            max_cd_bytes=args.cd_max_mb << 20)
    except RemoteZipError as exc:
        row.update(format="unreadable", error=str(exc)[:120])
        return
    fmt = aiida_format([m.name for m in members])
    row.update(format=fmt, n_members=len(members))
    if fmt == "legacy":
        row["n_vasp_primary"] = len(zip_evidence(members).primary)   # triage's own rule
    elif fmt == "sqlite_zip":
        db = next((m for m in members if m.name == "db.sqlite3"), None)
        if db is None:
            row["db"] = "missing"
        elif db.uncomp_size > (args.db_max_mb << 20):
            row["db"] = f"too large ({db.uncomp_size / 1e6:.0f} MB > cap)"
        else:
            with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as td:
                p = Path(td) / "db.sqlite3"
                t0 = time.monotonic()
                if fetch_member(session, f["download"], db, p):
                    row["db_MB"] = round(db.uncomp_size / 1e6, 1)
                    row["db_fetch_s"] = round(time.monotonic() - t0, 1)
                    try:
                        row.update(_query_db(p))
                    except sqlite3.Error as exc:
                        row["db_error"] = str(exc)[:120]
                else:
                    row["db"] = "fetch failed"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--speed", action="store_true")
    ap.add_argument("--aiida", action="store_true")
    ap.add_argument("--candidates", help="mc_candidates.jsonl from `discover` (for --aiida)")
    ap.add_argument("--peek-cache", default=None,
                    help="triage peek cache (default: <manifests>/mc_keep.jsonl.peeks.jsonl)")
    ap.add_argument("--streams", type=int, default=8,
                    help="max parallel S3 streams for the scaling test (1,2,4,8 are tried)")
    ap.add_argument("--latency-n", type=int, default=10,
                    help="sequential requests timed for the per-request cost")
    ap.add_argument("--mb", type=int, default=200, help="MB read per stream in --speed")
    ap.add_argument("--file", action="append", default=None, help="RECID/KEY for --speed")
    ap.add_argument("--db-max-mb", type=int, default=4096)
    ap.add_argument("--cd-max-mb", type=int, default=1024)
    ap.add_argument("--out", default=None, help="write the JSON report here")
    args = ap.parse_args()
    do_speed = args.speed or not args.aiida
    do_aiida = args.aiida or not args.speed
    report: dict = {"generated": time.strftime("%Y-%m-%dT%H:%M:%S"), "host": os.uname().nodename}
    if do_speed:
        report["speed"] = _speed(args)
        print(json.dumps(report["speed"], indent=1), flush=True)
    if do_aiida:
        if not args.candidates:
            ap.error("--aiida needs --candidates (run `discover` first)")
        report["aiida"] = _aiida(args)
        print(json.dumps({k: v for k, v in report["aiida"].items() if k != "files"}, indent=1))
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=1))
        print(f"report -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

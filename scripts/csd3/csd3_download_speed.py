#!/usr/bin/env python
"""Measure Zenodo download throughput + Range-request latency FROM A CSD3 COMPUTE NODE.

The harvest's runtime is dominated by network transfer, and the only unknown in the
CSD3 time estimate is the achievable throughput compute-node -> zenodo.org (which the
CSD3 docs do not state, and which depends on the node's outbound path / any proxy).
This script measures it directly. Run it on a compute node (it needs the same outbound
HTTPS the fetch stage needs), e.g.:

    ACCOUNT=MYGROUP-SL3-CPU
    srun -A "$ACCOUNT" -p icelake --nodes=1 --ntasks=1 --cpus-per-task=4 --time=00:20:00 \
        python scripts/csd3/csd3_download_speed.py --workers 4

What it reports, and why each matters for the estimate:

  * single-stream throughput (MB/s)      -- the floor; one download at a time.
  * N-worker aggregate throughput (MB/s) -- fetch runs `--workers` concurrent record
                                            downloads, so THIS is the number the
                                            transfer-time estimate should divide by.
  * Range round-trip latency (ms)        -- triage's zip-peek and fetch's targeted
                                            zip-member fetch are REQUEST-bound, not
                                            byte-bound; at 100 file-requests/min the
                                            per-request cost sets the peek/walk time.

It downloads real public Zenodo files to a temp dir and deletes them immediately
(nothing is staged). Default targets are small, openly-licensed records; override with
--url to point at your own. No token needed for public records, but it will use
ZENODO_TOKEN from the environment / .env if present (raises the file-endpoint quota).
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

# Public, openly-licensed Zenodo VASP/DFT archives (~400 MB each), verified live
# 2026-07-29 to serve HTTP 206 Range. These are the STABLE
# /api/records/<id>/files/<name>/content download links. If any 404 later (records can
# be versioned/withdrawn), pass your own with --url. Several are listed so the N-worker
# test pulls distinct files rather than hammering one.
DEFAULT_URLS = [
    "https://zenodo.org/api/records/16921907/files/aimd_pristine_npt_300K.zip/content",  # ~401 MB, CC-BY
    "https://zenodo.org/api/records/14649880/files/case1_OUTCAR_AIMD.zip/content",       # ~407 MB, CC-BY
    "https://zenodo.org/api/records/6984110/files/bulk_modulus.zip/content",             # ~404 MB, CC-BY
    "https://zenodo.org/api/records/21513301/files/DFT_output.zip/content",              # ~410 MB, CC-BY
]

MB = 1 << 20


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "zenodo-harvest/0.1 (speedtest)"
    tok = os.environ.get("ZENODO_TOKEN")
    if not tok:
        env = Path(__file__).resolve().parents[2] / ".env"
        if env.is_file():
            for line in env.read_text().splitlines():
                if line.startswith("ZENODO_TOKEN="):
                    tok = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if tok:
        s.headers["Authorization"] = f"Bearer {tok}"
    return s


def _download(session: requests.Session, url: str, dest: Path,
              max_bytes: int) -> tuple[int, float]:
    """Download up to max_bytes of url; return (bytes, seconds). 0 bytes on failure."""
    t0 = time.monotonic()
    got = 0
    try:
        with session.get(url, stream=True, timeout=120) as r:
            if r.status_code != 200:
                print(f"  ! HTTP {r.status_code} for {url}", file=sys.stderr)
                return 0, 0.0
            with dest.open("wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
                    got += len(chunk)
                    if got >= max_bytes:
                        break
    except requests.RequestException as exc:
        print(f"  ! {type(exc).__name__}: {exc}", file=sys.stderr)
        return 0, 0.0
    finally:
        dest.unlink(missing_ok=True)
    return got, time.monotonic() - t0


def _range_latency(session: requests.Session, url: str, n: int = 10) -> list[float]:
    """Round-trip time (ms) of a tiny suffix-Range GET (mirrors the zip central-dir peek)."""
    lat = []
    for _ in range(n):
        t0 = time.monotonic()
        try:
            r = session.get(url, headers={"Range": "bytes=-4096"}, timeout=60, stream=True)
            _ = r.content[:1]  # force the first byte
            r.close()
            if r.status_code in (206, 200):
                lat.append((time.monotonic() - t0) * 1000)
        except requests.RequestException:
            pass
    return lat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", action="append", default=None,
                    help="Zenodo file .../content URL (repeatable). Default: built-in list.")
    ap.add_argument("--workers", type=int, default=4, help="concurrency for the aggregate test")
    ap.add_argument("--cap-mb", type=float, default=400,
                    help="max bytes to pull per download (keeps the test short)")
    args = ap.parse_args()
    urls = args.url or DEFAULT_URLS
    cap = int(args.cap_mb * MB)
    session = _session()
    print(f"target(s): {len(urls)}   cap {args.cap_mb:g} MB/file   "
          f"auth: {'yes' if 'Authorization' in session.headers else 'no'}")

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        # 1. single-stream
        print("\n[1] single-stream throughput")
        got, dt = _download(session, urls[0], tmp / "s0", cap)
        if got:
            print(f"    {got/MB:.0f} MB in {dt:.1f}s  ->  {got/MB/dt:.1f} MB/s")
        else:
            print("    FAILED (see stderr). Fix --url before trusting the rest.")

        # 2. N-worker aggregate (repeat the URL list to reach `workers` tasks)
        print(f"\n[2] {args.workers}-worker aggregate throughput")
        if args.workers > len(urls):
            print(f"    NB: only {len(urls)} distinct URL(s) for {args.workers} workers -> "
                  f"files get double-fetched, and Zenodo's per-file shaping will DEPRESS this "
                  f"aggregate. Pass >= {args.workers} distinct --url for a clean test.")
        tasks = [(urls[i % len(urls)], tmp / f"w{i}") for i in range(args.workers)]
        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            res = list(ex.map(lambda t: _download(session, t[0], t[1], cap), tasks))
        wall = time.monotonic() - t0
        agg = sum(g for g, _ in res)
        if agg:
            print(f"    {agg/MB:.0f} MB across {args.workers} workers in {wall:.1f}s  ->  "
                  f"{agg/MB/wall:.1f} MB/s aggregate")
            print(f"    (single-stream x workers would be ~{got/MB/dt*args.workers:.0f} MB/s "
                  f"if perfectly parallel)" if got and dt else "")

        # 3. Range latency (request-bound peek/walk cost)
        print("\n[3] Range round-trip latency (zip-peek / targeted-fetch request cost)")
        lat = _range_latency(session, urls[0])
        if lat:
            print(f"    n={len(lat)}  median {statistics.median(lat):.0f} ms  "
                  f"min {min(lat):.0f}  max {max(lat):.0f}")
            print(f"    => at ~100 file-requests/min the quota, not latency, bounds peeking; "
                  f"~{60/ (max(statistics.median(lat)/1000, 0.6)):.0f} req/min network-bound")

    print("\nPlug [2]'s aggregate MB/s into the transfer-time estimate; [3]'s latency into "
          "the peek/zip-walk request budget.")


if __name__ == "__main__":
    main()

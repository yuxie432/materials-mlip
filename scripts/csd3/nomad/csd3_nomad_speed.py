#!/usr/bin/env python
"""Measure NOMAD fetch throughput FROM A CSD3 COMPUTE NODE — to size the real run.

The NOMAD harvest's only real unknown is **sustained bandwidth from a CSD3 compute node to
nomad-lab.eu**, and specifically for the path the harvest actually uses: the BULK streaming-zip
endpoint (`POST /entries/raw/query`, one zip per ~300-entry batch). The per-entry request rate
is a non-issue after the bulk redesign (~47k requests for the whole 7.1M — see
docs/NOMAD_HARVEST.md); what governs the wall-clock is MB/s. This script measures it directly,
on the real code path (`nomad_harvest.client`), so `WORKERS`/`--batch-size` and the campaign
length can be chosen from data instead of a guess.

Run it on a compute node (it needs the same outbound HTTPS the fetch stage needs):

    export SBATCH_ACCOUNT=MYGROUP-SL3-CPU
    module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
    srun -A "$SBATCH_ACCOUNT" -p icelake --nodes=1 --ntasks=1 --cpus-per-task=4 \
        --time=00:25:00 python scripts/csd3/nomad/csd3_nomad_speed.py --workers 4

What it reports, and why each matters:

  * BULK single-stream MB/s       -- one batch zip at a time: the throughput floor.
  * BULK N-worker aggregate MB/s  -- `--workers` concurrent batch zips: THIS is the number the
                                     transfer-time estimate divides by (the fetch runs this way).
  * per-entry MB/s + s/entry      -- the `--per-entry` fallback path, for contrast (latency-bound).
  * bulk_rawdir latency (ms)      -- the availability request (one per batch); request-bound cost.
  * extrapolation                 -- full 7.11M-entry vasprun transfer time at the measured
                                     aggregate MB/s, so you can judge the ~1-2 week campaign.

It discovers a small live sample, streams real batch zips to a NODE-LOCAL temp dir, and deletes
them immediately (nothing is staged to the /rds quota; point --tmp-dir at /local on CSD3). No
token is needed (anonymous reads); NOMAD self-throttles + 5xx-backs-off inside the client, so a
transient 503 during the test is retried, not fatal.
"""
from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Import the REAL client + query so the test exercises exactly what the harvest does
# (self-throttle, backoff, the same bulk endpoints and member selection).
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from nomad_harvest.client import (  # noqa: E402
    CANDIDATE_REQUIRED,
    NomadClient,
    direct_upload_vasp_query,
)

MB = 1 << 20
N_FULL = 7_111_067  # direct-upload VASP+DFT entries (live-verified 2026-08-16)


def _discover(client: NomadClient, n: int, elements: list[str] | None) -> list[dict]:
    """Keyset-sample `n` direct-upload VASP-DFT entries (entry_id + upload_id + mainfile)."""
    q = direct_upload_vasp_query(elements=elements)
    return list(client.iter_entries(q, required=CANDIDATE_REQUIRED,
                                    page_size=min(max(n, 10), 1000), max_entries=n))


def _bulk_zip(client: NomadClient, entries: list[dict], dest: Path) -> tuple[int, float]:
    """Stream ONE batch zip of exactly these entries' mainfiles; return (bytes, seconds)."""
    ids = [e["entry_id"] for e in entries]
    include = [e.get("mainfile") for e in entries if e.get("mainfile")]
    t0 = time.monotonic()
    try:
        n = client.bulk_raw_zip(ids, {"include_files": include}, dest)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! bulk_raw_zip failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 0, 0.0
    finally:
        dest.unlink(missing_ok=True)
    return n, time.monotonic() - t0


def _per_entry(client: NomadClient, entry: dict, dest: Path) -> tuple[int, float]:
    """Fetch one entry's mainfile the per-entry way (rawdir-relative path); (bytes, seconds)."""
    from nomad_harvest.harvest import raw_path_rel
    t0 = time.monotonic()
    got = 0
    try:
        rel = raw_path_rel(entry["mainfile"], entry.get("mainfile"))
        got = client.download_raw_file(entry["entry_id"], rel, dest)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! per-entry failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 0, 0.0
    finally:
        dest.unlink(missing_ok=True)
    return got, time.monotonic() - t0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workers", type=int, default=4,
                    help="concurrent batch zips for the aggregate test (the fetch's --workers)")
    ap.add_argument("--batch-size", type=int, default=300,
                    help="entries per bulk zip (the fetch's --batch-size; default 300)")
    ap.add_argument("--elements", nargs="+", default=None,
                    help="restrict the sample to materials containing ALL these elements")
    ap.add_argument("--tmp-dir", default=None,
                    help="node-local dir for the transient zips (CSD3: /local). Default: system temp.")
    ap.add_argument("--per-entry-n", type=int, default=5,
                    help="how many entries to time on the per-entry fallback path (0 to skip)")
    args = ap.parse_args()

    client = NomadClient()
    # Sample enough entries for `workers` distinct batches (so the aggregate test pulls disjoint
    # files, not the same batch N times) plus a few for the per-entry contrast.
    need = args.workers * args.batch_size + max(args.per_entry_n, 0)
    print(f"discovering {need:,} sample entries (batch_size={args.batch_size}, "
          f"workers={args.workers}, elements={args.elements or 'any'}) ...")
    t0 = time.monotonic()
    entries = _discover(client, need, args.elements)
    print(f"  got {len(entries):,} entries in {time.monotonic()-t0:.1f}s")
    if len(entries) < args.batch_size:
        print("  ! not enough entries sampled for one batch; try a broader --elements or wait "
              "out a NOMAD 5xx spell.", file=sys.stderr)
        sys.exit(1)

    tmp_ctx = tempfile.TemporaryDirectory(dir=args.tmp_dir)
    with tmp_ctx as td:
        tmp = Path(td)

        # 1. BULK single-stream ---------------------------------------------------
        # Keep the batch's bytes in dedicated vars — later sections reuse generic names.
        print("\n[1] BULK single-stream throughput (one batch zip)")
        b0 = entries[:args.batch_size]
        s_got, s_dt = _bulk_zip(client, b0, tmp / "s0.zip")
        single_mbs = (s_got / MB / s_dt) if (s_got and s_dt) else 0.0
        if s_got:
            print(f"    {s_got/MB:.0f} MB in {s_dt:.1f}s  ->  {single_mbs:.1f} MB/s   "
                  f"({s_got/len(b0)/1024:.0f} KB/entry avg)")
        else:
            print("    FAILED (see stderr). NOMAD may be under load; retry the test.")

        # 2. BULK N-worker aggregate ---------------------------------------------
        print(f"\n[2] BULK {args.workers}-worker aggregate throughput (concurrent batch zips)")
        batches = [entries[i * args.batch_size:(i + 1) * args.batch_size]
                   for i in range(args.workers)]
        batches = [b for b in batches if len(b) == args.batch_size]
        agg_mbs = 0.0
        if len(batches) < args.workers:
            print(f"    NB only {len(batches)} full batch(es) sampled for {args.workers} workers "
                  f"-> aggregate under-measured. Raise the sample or lower --workers/--batch-size.")
        if batches:
            t0 = time.monotonic()
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                res = list(ex.map(lambda i_b: _bulk_zip(client, i_b[1], tmp / f"w{i_b[0]}.zip"),
                                  list(enumerate(batches))))
            wall = time.monotonic() - t0
            agg = sum(g for g, _ in res)
            agg_mbs = (agg / MB / wall) if (agg and wall) else 0.0
            if agg:
                print(f"    {agg/MB:.0f} MB across {len(batches)} workers in {wall:.1f}s  ->  "
                      f"{agg_mbs:.1f} MB/s aggregate")
                if single_mbs:
                    print(f"    (single-stream x workers would be ~{single_mbs*len(batches):.0f} "
                          f"MB/s if perfectly parallel; ratio {agg_mbs/single_mbs:.1f}x)")

        # 3. bulk_rawdir latency (the availability request, one per batch) --------
        print("\n[3] bulk_rawdir latency (availability request, ~1 per batch)")
        lat = []
        ids0 = [e["entry_id"] for e in b0]
        for _ in range(5):
            t0 = time.monotonic()
            try:
                client.bulk_rawdir(ids0)
                lat.append((time.monotonic() - t0) * 1000)
            except Exception:  # noqa: BLE001
                pass
        if lat:
            print(f"    n={len(lat)}  median {statistics.median(lat):.0f} ms  "
                  f"min {min(lat):.0f}  max {max(lat):.0f}   (negligible vs the zip transfer)")

        # 4. per-entry contrast (the --per-entry fallback path) ------------------
        if args.per_entry_n > 0 and len(entries) > args.batch_size:
            print(f"\n[4] per-entry fallback throughput ({args.per_entry_n} entries, latency-bound)")
            pe = [e for e in entries[args.workers * args.batch_size:] if e.get("mainfile")][:args.per_entry_n]
            times, bytes_ = [], []
            for i, e in enumerate(pe):
                pe_got, pe_dt = _per_entry(client, e, tmp / f"pe{i}")
                if pe_got:
                    times.append(pe_dt)
                    bytes_.append(pe_got)
            if times:
                tot_b, tot_t = sum(bytes_), sum(times)
                print(f"    {len(times)} entries: {tot_b/MB:.1f} MB in {tot_t:.1f}s  ->  "
                      f"{tot_b/MB/tot_t:.2f} MB/s   {tot_t/len(times):.2f} s/entry")
                print(f"    => per-entry alone would need ~{N_FULL*(tot_t/len(times))/86400:,.0f} "
                      f"days for 7.1M (why bulk is the default).")

    # 5. extrapolation ----------------------------------------------------------
    print("\n[5] full-harvest extrapolation (vasprun-only, ~7.11M direct uploads)")
    if agg_mbs and s_got:
        kb_per_entry = s_got / len(b0) / 1024        # from the single-stream batch [1]
        total_tb = N_FULL * (kb_per_entry * 1024) / 1e12
        days = (total_tb * 1e12 / MB) / agg_mbs / 86400
        print(f"    measured ~{kb_per_entry:.0f} KB/entry (as stored) -> ~{total_tb:.1f} TB total "
              f"transient transfer (doc ~6.5 TB)")
        print(f"    at {agg_mbs:.1f} MB/s aggregate -> ~{days:,.1f} days of pure transfer "
              f"(overlapped with parse; wall-clock a bit more)")
        print("    -> plan WORKERS around what sustains this aggregate; if it is far below "
              "~50 MB/s, email support@nomad-lab.eu to request a rate-limit exemption (§2).")
    else:
        print("    (need a successful [1] + [2] to extrapolate — rerun when NOMAD is not 5xx-ing)")

    print("\nPlug [2]'s aggregate MB/s into the transfer-time estimate; sweep --workers 2/4/8 to "
          "find where NOMAD's server-side throughput saturates (more workers stop helping).")


if __name__ == "__main__":
    main()

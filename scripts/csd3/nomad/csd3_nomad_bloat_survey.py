#!/usr/bin/env python
"""Survey the NOMAD upload BLOAT distribution to validate (or reject) the hybrid whole-download
fetch BEFORE implementing it.

WHY
---
The NOMAD fetch is REQUEST-COUNT / throttle-bound: `/uploads/{id}/raw` allows 1 in-flight
connection per IP every ~5 s, and the targeted multi-range pattern makes ceil(n_vaspruns/250)+1
requests per upload — the live job averaged ~47 calcs/request → ~9 days. Streaming a whole upload
in ONE transfer-bound request collapses that to ~2 requests/upload (census: 34,812 → 7,550
requests, 4.6x fewer). BUT whole-download transfers the whole zip, including CHGCAR/WAVECAR bloat.
Whether that pays off depends ENTIRELY on the bloat ratio (wanted-vasprun-bytes / whole-upload-bytes)
of the uploads that hold most entries — and a 6-upload spot check is not enough (it ranged 0.02-0.86).

This surveys it properly:
  1. Full census via a `upload_id` terms-aggregation → EXACT n_entries for all ~3,775 uploads
     (so the targeted request count is exact, no sampling).
  2. CD-samples uploads STRATIFIED by entry count (heavy on the 3k-10k bucket that holds 80% of
     entries), reading each upload's central directory to get total_size, n_vaspruns, wanted_bytes,
     and the byte span the wanted members occupy.
  3. Per upload, models BOTH strategies (targeted vs whole-stream), picks the cheaper, and
     EXTRAPOLATES the sampled per-entry transfer/requests to every upload in that stratum (weighted
     by the census entry counts).
  4. Reports the entry-weighted bloat distribution + projected requests / transfer-TB / days for
     pure-targeted vs the hybrid, and sweeps the chooser's rate assumption for robustness.

Run from anywhere with outbound HTTPS (the bloat distribution is a property of the DATA, not the
node — only THROUGHPUT is node-specific, and that is measured separately by the bottleneck probe):

    python -u scripts/csd3/nomad/csd3_nomad_bloat_survey.py --samples 80

The CD reads are throttled (~5 s each), so `--samples 80` takes ~10-15 min. Nothing is staged.
"""
from __future__ import annotations

import argparse
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from nomad_harvest import upload_zip  # noqa: E402
from nomad_harvest.client import NomadClient, direct_upload_vasp_query  # noqa: E402
from nomad_harvest.harvest import _VASPRUN_RE  # noqa: E402

BASE = "https://nomad-lab.eu/prod/v1/api/v1"
MB = 1 << 20
TB = 1 << 40
RANGES = upload_zip.MAX_RANGES_PER_REQUEST     # 200 members/multi-range request (8 KB header cap)
THROTTLE = 5.0                                  # seconds per request (1-in-flight/5s)

# Strata by n_entries/upload, with how many to CD-sample from each (heavy where the entries are:
# the 3k-10k bucket holds ~80% of all entries).
STRATA = [
    (1, 10, 3),
    (11, 100, 5),
    (101, 1000, 8),
    (1001, 3000, 12),
    (3001, 10000, 24),
    (10001, 10**9, 8),
]


def census(client: NomadClient) -> dict[str, int]:
    """{upload_id: n_entries} for ALL direct-upload VASP-DFT uploads, via a keyset-scrolled
    terms aggregation on upload_id (a handful of cheap entries/query requests)."""
    q = direct_upload_vasp_query()
    counts: dict[str, int] = {}
    after = None
    while True:
        term: dict = {"quantity": "upload_id", "pagination": {"page_size": 1000}}
        if after:
            term["pagination"]["page_after_value"] = after
        body = {"owner": "public", "query": q, "pagination": {"page_size": 0},
                "aggregations": {"by_upload": {"terms": term}}}
        d = client._post("/entries/query", body)          # reuse the paced/retrying client
        t = d["aggregations"]["by_upload"]["terms"]
        for b in t["data"]:
            counts[b["value"]] = b["count"]
        after = t["pagination"].get("next_page_after_value")
        if not after:
            break
    return counts


def sample_upload(client: NomadClient, upload_id: str) -> dict | None:
    """Read one upload's central directory and return its bloat/size stats, or None on failure."""
    try:
        members, total = upload_zip.read_central_directory(client, upload_id)
    except Exception as exc:  # noqa: BLE001
        print(f"    ! {upload_id[:12]}: CD read failed: {type(exc).__name__}", file=sys.stderr)
        return None
    vas = [m for m in members.values()
           if _VASPRUN_RE.search(m.name.rsplit("/", 1)[-1])]
    if not vas:
        return None
    wanted = sum(m.on_disk_size for m in vas)
    offs = [m.local_offset for m in vas]
    ends = [m.local_offset + 30 + len(m.name) + 512 + m.comp_size for m in vas]
    span = max(ends) - min(offs)
    return {"upload_id": upload_id, "n_members": len(members), "n_vaspruns": len(vas),
            "total": total, "wanted": wanted, "span": span,
            "bloat": wanted / total if total else 0.0}


def model(stat: dict, rate_bps: float) -> dict:
    """Model targeted vs whole-stream for one upload at a nominal transfer rate."""
    nv, wanted, span = stat["n_vaspruns"], stat["wanted"], stat["span"]
    req_t = math.ceil(nv / RANGES) + 1                       # member batches + 1 CD read
    time_t = max(req_t * THROTTLE, wanted / rate_bps)        # targeted: throttle-bound
    req_w = 2                                                # CD read + 1 stream
    time_w = THROTTLE + span / rate_bps                      # whole: transfer-bound
    whole = time_w < time_t
    return {"req_t": req_t, "bytes_t": wanted, "time_t": time_t,
            "req_w": req_w, "bytes_w": span, "time_w": time_w,
            "whole": whole,
            "req": req_w if whole else req_t,
            "bytes": span if whole else wanted,
            "time": time_w if whole else time_t}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", type=int, default=60, help="approx uploads to CD-sample")
    ap.add_argument("--rate-mbs", type=float, default=15.0,
                    help="nominal transfer MB/s for the chooser (sweep is reported anyway)")
    args = ap.parse_args()
    client = NomadClient()

    print("[1] census: n_entries per upload (terms aggregation) ...")
    counts = census(client)
    nU = len(counts)
    tot_entries = sum(counts.values())
    print(f"    {nU} uploads, {tot_entries:,} entries")

    # Exact pure-targeted request floor from the census (no sampling needed).
    ns = list(counts.values())
    req_targeted_exact = sum(math.ceil(n / RANGES) + 1 for n in ns)
    print(f"    EXACT pure-targeted requests = {req_targeted_exact:,} "
          f"-> {req_targeted_exact*THROTTLE/86400:.2f}-day throttle floor")

    # Stratify + choose a deterministic spread of uploads to sample per stratum.
    by_stratum: dict[tuple, list[str]] = defaultdict(list)
    for up, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        for lo, hi, _k in STRATA:
            if lo <= n <= hi:
                by_stratum[(lo, hi)].append(up)
                break

    print(f"\n[2] CD-sampling ~{args.samples} uploads stratified by entry count "
          f"(~{THROTTLE:.0f}s each) ...")
    samples: list[dict] = []
    scale = args.samples / 60.0
    for lo, hi, k in STRATA:
        ups = by_stratum[(lo, hi)]
        if not ups:
            continue
        want = max(1, round(k * scale))
        # even spread across the (entry-count-sorted) stratum
        idxs = [round(i * (len(ups) - 1) / max(1, want - 1)) for i in range(want)] if want > 1 else [0]
        picked = [ups[i] for i in sorted(set(idxs))]
        got = 0
        for up in picked:
            s = sample_upload(client, up)
            if s:
                s["stratum"] = (lo, hi)
                s["n_entries"] = counts[up]
                samples.append(s)
                got += 1
        print(f"    stratum {lo}-{hi}: sampled {got}/{len(picked)} "
              f"(of {len(ups)} uploads holding {sum(counts[u] for u in ups):,} entries)")

    if not samples:
        raise SystemExit("no uploads sampled")

    # [3] entry-weighted bloat distribution --------------------------------------------------
    print("\n[3] bloat ratio (wanted vasprun bytes / whole upload bytes)")
    print("    (each sampled upload weighted by its n_vaspruns -> ENTRY-weighted)")
    buckets = [(0.0, 0.1), (0.1, 0.3), (0.3, 0.5), (0.5, 0.7), (0.7, 0.9), (0.9, 1.01)]
    w_by_bucket = defaultdict(float)
    tot_w = sum(s["n_vaspruns"] for s in samples)
    for s in samples:
        for lo, hi in buckets:
            if lo <= s["bloat"] < hi:
                w_by_bucket[(lo, hi)] += s["n_vaspruns"]
                break
    for lo, hi in buckets:
        w = w_by_bucket[(lo, hi)]
        print(f"    bloat {lo:.1f}-{hi:.1f}: {100*w/tot_w:5.1f}% of sampled vaspruns")
    bloats = [s["bloat"] for s in samples]
    print(f"    median bloat (unweighted) = {statistics.median(bloats):.3f}; "
          f"n_samples={len(samples)}")

    # [4] project pure-targeted vs hybrid, extrapolating each stratum's per-entry stats ------
    def project(rate_mbs: float) -> dict:
        rate = rate_mbs * MB
        # per-stratum per-entry averages from the sample, applied to the stratum's census entries
        per_stratum: dict[tuple, list[dict]] = defaultdict(list)
        for s in samples:
            per_stratum[s["stratum"]].append(model(s, rate))
        tot = {"req_t": 0.0, "bytes_t": 0.0, "time_t": 0.0,
               "req_h": 0.0, "bytes_h": 0.0, "time_h": 0.0, "entries_whole": 0.0}
        for (lo, hi, _k) in STRATA:
            ups = by_stratum[(lo, hi)]
            stratum_entries = sum(counts[u] for u in ups)
            ms = per_stratum.get((lo, hi))
            if not ms or not stratum_entries:
                continue
            sample_entries = sum(s["n_entries"] for s in samples if s["stratum"] == (lo, hi))
            f = stratum_entries / sample_entries        # extrapolation factor (entries)
            for m in ms:
                tot["req_t"] += m["req_t"] * f
                tot["bytes_t"] += m["bytes_t"] * f
                tot["time_t"] += m["time_t"] * f
                tot["req_h"] += m["req"] * f
                tot["bytes_h"] += m["bytes"] * f
                tot["time_h"] += m["time"] * f
        # entry-weighted whole fraction
        ew_whole = 0.0
        for s in samples:
            m = model(s, rate)
            if m["whole"]:
                ups = by_stratum[s["stratum"]]
                stratum_entries = sum(counts[u] for u in ups)
                sample_entries = sum(x["n_entries"] for x in samples if x["stratum"] == s["stratum"])
                ew_whole += s["n_entries"] * (stratum_entries / sample_entries)
        tot["entries_whole"] = ew_whole
        return tot

    print(f"\n[4] projection to the full {tot_entries:,}-entry corpus")
    for rate in sorted({8.0, 12.0, args.rate_mbs, 20.0}):
        p = project(rate)
        days_t = max(p["req_t"] * THROTTLE, p["bytes_t"] / (rate * MB)) / 86400
        days_h = max(p["req_h"] * THROTTLE, p["bytes_h"] / (rate * MB)) / 86400
        print(f"  @ {rate:>4.0f} MB/s:")
        print(f"     pure targeted : {p['req_t']/1000:6.1f}k req, {p['bytes_t']/TB:5.2f} TB, "
              f"~{days_t:4.1f} d (throttle-bound)")
        print(f"     HYBRID        : {p['req_h']/1000:6.1f}k req, {p['bytes_h']/TB:5.2f} TB, "
              f"~{days_h:4.1f} d  ({100*p['entries_whole']/tot_entries:.0f}% of entries via whole-stream)")
        print(f"     -> speedup ~{days_t/max(days_h,0.01):.1f}x, "
              f"extra transfer {p['bytes_h']/max(p['bytes_t'],1):.2f}x")
    print("\n  (targeted's real run is ~4.5x its throttle FLOOR from under-filled batches / 48 MB")
    print("   CD reads / 429 retries; whole-stream is transfer-bound so it hits its number.)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""Confirm the PRE-PACKED targeted-extraction fetch path from a CSD3 compute node.

Background (2026-08-17 investigation): the current bulk fetch (`POST /entries/raw/query`) is
SERVER-SIDE per-file-CPU-bound, not bandwidth-bound — NOMAD stores each upload as one big
`raw-public.plain.zip` and that endpoint re-parses the whole zip central directory ~4x per file
(no caching), so it runs ~0.8 s/file (~0.3 MB/s) however you batch it. The fix is to bypass that
code path entirely: `GET /uploads/{id}/raw` streams the ALREADY-PACKED zip straight off disk
(~15-27 MB/s, anonymous, supports Range INCLUDING multi-range). So we read each upload's zip
central directory once (a tail Range read), then pull ONLY the wanted vasprun/OUTCAR members out
by byte range (many per multi-range request), decompressing locally — vasprun-only bytes at
pre-packed speed. Members are STORED (method=0); upload zips are ZIP64 (>4 GB) so this parser is
zip64-aware.

This probe was validated live from a WSL IP. RUN IT FROM A CSD3 COMPUTE NODE to confirm the
throttle + throughput on CSD3's (NAT-shared) IP before committing to the rewrite:

    export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
    sintr -A "$SBATCH_ACCOUNT" -p icelake -N1 -n1 -c1 -t 0:30:0     # interactive node
    module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
    python -u scripts/csd3/nomad/csd3_nomad_prepacked_probe.py

What it reports:
  [1] whole-upload single-stream MB/s (the pre-packed direct stream — the throughput ceiling)
  [2] the "1 in-flight / new every 5 s" download-endpoint throttle (is it there on CSD3's IP?)
  [3] end-to-end targeted extraction: parse a real (zip64) upload's central directory, multi-range
      fetch a batch of vasprun members, decompress + validate they are real vaspruns
  [4] full-7.1M extrapolation (requests + wall-clock) at the measured MB/s

Anonymous (no token). Nothing is staged to /rds — everything streams to memory and is discarded.
"""
from __future__ import annotations

import bz2
import gzip
import struct
import sys
import time
import zlib
from pathlib import Path

import requests

# Reuse the project's discover query/client just to SAMPLE uploads (keyset scan).
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from nomad_harvest.client import (  # noqa: E402
    CANDIDATE_REQUIRED,
    NomadClient,
    direct_upload_vasp_query,
)

BASE = "https://nomad-lab.eu/prod/v1/api/v1"
MB = 1 << 20
N_FULL = 7_111_067
RANGES_PER_REQ = 250          # multi-range members per request (nginx ~8 KB Range-header cap)
S = requests.Session()


def rget(up: str, start: int, end: int, retries: int = 8):
    """One Range GET of upload `up`, honouring the 1-conn/5s 429 throttle (bounded retries).

    Returns (bytes, headers) where headers is requests' case-insensitive mapping (the server
    sends ``content-range`` lower-cased, so plain-dict access by ``Content-Range`` would miss it).
    """
    url = f"{BASE}/uploads/{up}/raw"
    for _ in range(retries):
        r = S.get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=180)
        if r.status_code in (200, 206):
            return r.content, r.headers
        if r.status_code == 429:
            time.sleep(5.5)
            continue
        raise SystemExit(f"HTTP {r.status_code}: {r.text[:160]}")
    raise SystemExit("exhausted retries on a Range GET")


def total_size(up: str) -> int:
    _, h = rget(up, 0, 0)
    return int(h["Content-Range"].split("/")[-1])


def _parse_cd(cd: bytes) -> list[dict]:
    """Parse central-directory bytes -> [{name, method, comp, uncomp, off}], zip64-aware."""
    out: list[dict] = []
    i, n = 0, len(cd)
    while i + 46 <= n and cd[i:i + 4] == b"PK\x01\x02":
        (_, _, _, method, _, _, _, _crc, comp, uncomp,
         nlen, elen, clen, _, _, _attr, off) = struct.unpack("<IHHHHHHIIIHHHHHII", cd[i:i + 46])
        name = cd[i + 46:i + 46 + nlen].decode("utf-8", "replace")
        extra = cd[i + 46 + nlen:i + 46 + nlen + elen]
        if 0xFFFFFFFF in (off, comp, uncomp):
            j = 0
            while j + 4 <= len(extra):
                hid, hsz = struct.unpack("<HH", extra[j:j + 4])
                body = extra[j + 4:j + 4 + hsz]
                if hid == 0x0001:
                    k = 0
                    if uncomp == 0xFFFFFFFF:
                        uncomp = struct.unpack("<Q", body[k:k + 8])[0]; k += 8
                    if comp == 0xFFFFFFFF:
                        comp = struct.unpack("<Q", body[k:k + 8])[0]; k += 8
                    if off == 0xFFFFFFFF:
                        off = struct.unpack("<Q", body[k:k + 8])[0]; k += 8
                j += 4 + hsz
        out.append(dict(name=name, method=method, comp=comp, uncomp=uncomp, off=off))
        i += 46 + nlen + elen + clen
    return out


def central_directory(up: str, total: int) -> list[dict]:
    tail = min(total, 40 << 20)
    blob, _ = rget(up, total - tail, total - 1)
    blob_start = total - tail
    idx = blob.rfind(b"PK\x05\x06")
    if idx < 0:
        raise SystemExit("no EOCD in tail (raise the tail size?)")
    _, _, _, _, _tot, cd_size, cd_off, _ = struct.unpack("<IHHHHIIH", blob[idx:idx + 22])
    if cd_off == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:      # ZIP64
        loc = blob[idx - 20:idx]
        z64_off = struct.unpack("<IIQI", loc)[2]
        z, _ = rget(up, z64_off, z64_off + 55)
        cd_size = struct.unpack("<Q", z[40:48])[0]
        cd_off = struct.unpack("<Q", z[48:56])[0]
    if cd_off >= blob_start:
        cd = blob[cd_off - blob_start: cd_off - blob_start + cd_size]
    else:
        cd, _ = rget(up, cd_off, cd_off + cd_size - 1)
    return _parse_cd(cd)


def _decompress(name: str, method: int, data: bytes) -> bytes:
    raw = data if method == 0 else zlib.decompress(data, -15)
    if name.lower().endswith(".bz2"):
        return bz2.decompress(raw)
    if name.lower().endswith(".gz"):
        return gzip.decompress(raw)
    return raw


def main() -> None:
    client = NomadClient()
    print("sampling a few uploads (keyset scan of direct-upload VASP-DFT) ...")
    ups: dict[str, str] = {}   # upload_id -> a vasprun mainfile
    for e in client.iter_entries(direct_upload_vasp_query(), required=CANDIDATE_REQUIRED,
                                 page_size=200, max_entries=600):
        mf = e.get("mainfile") or ""
        if "vasprun" in mf.lower() and e.get("upload_id") not in ups:
            ups[e["upload_id"]] = mf
        if len(ups) >= 4:
            break
    if not ups:
        raise SystemExit("no vasprun-mainfile uploads sampled; retry")
    up = next(iter(ups))
    print(f"  using upload {up}")

    # [1] whole-upload single-stream throughput (120 MB range) ------------------
    print("\n[1] pre-packed whole-upload single-stream throughput")
    t = time.monotonic()
    blob, h = rget(up, 0, 120 * MB - 1)
    dt = time.monotonic() - t
    total = int(h["Content-Range"].split("/")[-1])
    mbs = len(blob) / MB / dt
    print(f"    {len(blob)/MB:.0f} MB in {dt:.1f}s -> {mbs:.1f} MB/s   (upload total {total/1e9:.1f} GB)")

    # [2] the download-endpoint throttle ---------------------------------------
    print("\n[2] '1 in-flight / new every 5 s' throttle check (2 back-to-back small requests)")
    time.sleep(5.5)
    r1 = S.get(f"{BASE}/uploads/{up}/raw", headers={"Range": "bytes=0-9999"}, timeout=60)
    r2 = S.get(f"{BASE}/uploads/{up}/raw", headers={"Range": "bytes=0-9999"}, timeout=60)
    print(f"    req1={r1.status_code}  req2(immediate)={r2.status_code}  "
          f"-> {'throttled (expected: 1-conn/5s present on this IP)' if r2.status_code == 429 else 'NOT throttled on this IP!'}")

    # [3] end-to-end targeted extraction ---------------------------------------
    print("\n[3] targeted zip64 extraction (parse cd -> multi-range fetch vaspruns -> validate)")
    time.sleep(5.5)
    members = central_directory(up, total)
    time.sleep(5.5)
    # Sort by offset and drop overlaps: a generous over-fetch pad on densely-packed tiny members
    # makes the server COALESCE adjacent ranges into fewer multipart parts (all data still present,
    # but harder to demo-parse). Non-overlapping exact-ish ranges keep one member per part.
    allv = sorted((m for m in members if "vasprun" in m["name"].lower()
                   and m["name"].lower().endswith((".xml", ".xml.bz2", ".xml.gz"))),
                  key=lambda m: m["off"])
    vas, last_end = [], -1
    for m in allv:
        end = m["off"] + 30 + len(m["name"]) + 64 + m["comp"] - 1   # 64 = room for the local extra
        if m["off"] > last_end:
            vas.append((m, end))
            last_end = end
        if len(vas) >= 64:
            break
    seg = [(m["off"], end, m) for m, end in vas]
    vas = [m for m, _ in vas]
    hdr = ",".join(f"{s}-{e}" for s, e, _ in seg)
    print(f"    cd: {len(members)} members, {len(vas)} vaspruns targeted; Range header {len(hdr)} bytes")
    t = time.monotonic()
    r = S.get(f"{BASE}/uploads/{up}/raw", headers={"Range": f"bytes={hdr}"}, timeout=300)
    dt = time.monotonic() - t
    body = r.content
    ct = r.headers.get("Content-Type", "")
    by_name = {m["name"]: m for m in vas}
    # Scan the WHOLE response for local file headers (the server may coalesce adjacent tiny
    # members into one multipart part, so don't rely on one header per part).
    valid = 0
    pos = 0
    seen: set[str] = set()
    while True:
        li = body.find(b"PK\x03\x04", pos)
        if li < 0:
            break
        pos = li + 4
        if li + 30 > len(body):
            break
        nlen, elen = struct.unpack("<HH", body[li + 26:li + 30])
        name = body[li + 30:li + 30 + nlen].decode("utf-8", "replace")
        m = by_name.get(name)
        if not m or name in seen:
            continue
        seen.add(name)
        data = body[li + 30 + nlen + elen: li + 30 + nlen + elen + m["comp"]]
        try:
            content = _decompress(name, m["method"], data)
            if b"<modeling" in content[:3000] or b"<?xml" in content[:80]:
                valid += 1
        except Exception:  # noqa: BLE001
            pass
    print(f"    HTTP {r.status_code} {ct[:28]}  {len(body)/MB:.1f} MB in {dt:.1f}s ({len(body)/MB/dt:.1f} MB/s)")
    print(f"    validated {valid}/{len(vas)} targeted vasprun members are real vaspruns "
          f"(server may coalesce adjacent members -> fewer parts, all bytes present)")

    # [4] extrapolation --------------------------------------------------------
    print("\n[4] full 7.1M extrapolation (targeted, single stream)")
    reqs = N_FULL / RANGES_PER_REQ + 3792
    throttle_days = reqs * 5 / 86400
    xfer_days = (N_FULL * 262 * 1024) / MB / max(mbs, 1) / 86400   # ~262 KB/entry stored
    print(f"    ~{reqs/1000:.0f}k requests -> throttle floor ~{throttle_days:.1f} days; "
          f"transfer ~{xfer_days:.1f} days at {mbs:.0f} MB/s")
    print(f"    => full run ~{max(throttle_days, xfer_days):.1f} days single-stream, no token/exemption "
          f"(vs 2-6 weeks on the current filtered path)")


if __name__ == "__main__":
    main()

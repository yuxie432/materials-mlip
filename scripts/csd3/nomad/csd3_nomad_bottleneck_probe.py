#!/usr/bin/env python
"""Decompose the NOMAD compute-node fetch bottleneck (~4 MB/s) into its causes.

WHY THIS EXISTS
---------------
The live NOMAD job on a CSD3 *compute* node sustained only ~3.9 MB/s, while the Zenodo
harvest on the *same* compute nodes reaches ~50-60 MB/s. That looks like a 15x anomaly, but
the comparison is not apples-to-apples:

  * Zenodo's fetch runs `--workers 4` (ThreadPoolExecutor, one session/thread) -> ~50-60 MB/s
    is the AGGREGATE of ~4 parallel connections, i.e. ~12-15 MB/s PER CONNECTION.
  * NOMAD's fetch is intrinsically SINGLE-connection (the `/uploads/{id}/raw` endpoint is
    rate-limited "1 in-flight connection per IP, a new one every ~5 s"), and it uses the
    choppy multi-range TARGETED pattern, not one long sequential stream.
  * The login-node probe measured NOMAD at 11-15 MB/s WHOLE-upload / 6-9 MB/s TARGETED; the
    compute job measured 3.9 MB/s TARGETED.

So the real question is not "why 15x" but three separable ones, and each has a DIFFERENT fix:
  (1) Serialization: 1 NOMAD connection vs 4 Zenodo connections  (~4x).   Fix: parallelism.
  (2) Pattern: targeted multi-range vs one sequential stream     (~1.5-2x). Fix: whole-upload
      download for low-bloat uploads.
  (3) Path/load: compute->MPCDF(Germany) single-flow vs login, and NOMAD server load (~1.5-2x).
      Fix: TCP buffer tuning / off-peak / a faster egress node — or nothing (accept/scope).

This probe MEASURES each factor on a compute node so we can pick the right fix WITHOUT
guessing (and, critically, without needing a NOMAD rate-limit exemption unless the data says so).

WHAT IT REPORTS
  [A] Egress public IP + TCP round-trip (RTT) to nomad-lab.eu AND zenodo.org, + resolved IPs.
  [B] Zenodo single-connection throughput on THIS node   -> the per-connection reference.
  [C] NOMAD whole-upload single-connection throughput    -> the per-connection ceiling,
      also with an ENLARGED socket receive buffer (tests the "window-limited flow" theory).
  [D] NOMAD targeted multi-range throughput (the real fetch pattern) + whole:targeted ratio.
  [E] NOMAD parallelism semantics: (E1) 2 concurrent streams to DIFFERENT uploads -> 429?
      (E2) a 2nd stream started 5.1 s after a still-in-flight 1st -> 429?  This distinguishes
      "1 in-flight per IP" (truly serial) from "new connection every 5 s" (staggered pipelining
      is allowed -> we can go ~Nx faster from ONE IP with no exemption).
  [F] Bloat ratio (wanted-vasprun bytes / whole-upload bytes) across sampled uploads -> tells us
      how much a hybrid whole-download would help.
  [G] A verdict that interprets the numbers into a recommended fix.

Run it from a COMPUTE node (it needs the same outbound HTTPS the fetch needs):

    export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
    module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
    sintr -A "$SBATCH_ACCOUNT" -p icelake -N1 -n1 -c1 -t 0:30:0
    python -u scripts/csd3/nomad/csd3_nomad_bottleneck_probe.py

Or via the companion launcher (also checks whether TWO compute nodes get DIFFERENT egress IPs,
which is the make-or-break test for multi-node parallel fetch without an exemption):

    ACCOUNT=<MYGROUP>-SL3-CPU bash scripts/csd3/nomad/csd3_nomad_bottleneck_probe.sh

Anonymous (no token). Nothing is staged to /rds — everything streams to memory and is discarded.
Total cost: a few minutes, a few hundred MB of transfer.
"""
from __future__ import annotations

import socket
import statistics
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

try:                                   # urllib3 is a requests dependency; import defensively
    from urllib3.poolmanager import PoolManager
except Exception:                      # noqa: BLE001
    PoolManager = None                 # type: ignore[assignment]

# Reuse the project's discover query/client ONLY to sample upload_ids (a keyset scan of the
# entries/* endpoint — a different, looser throttle bucket than /uploads/{id}/raw).
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from nomad_harvest.client import (  # noqa: E402
    CANDIDATE_REQUIRED,
    NomadClient,
    direct_upload_vasp_query,
)

BASE = "https://nomad-lab.eu/prod/v1/api/v1"
MB = 1 << 20
N_FULL = 7_111_067
UPLOAD_PACE = 5.5                      # honour the "new connection every ~5 s" rule between probes

# A public, openly-licensed Zenodo VASP archive that serves HTTP 200/206 (from the Zenodo
# speed probe). Used to measure this node's per-connection rate to a DIFFERENT well-provisioned
# host (CERN), so we can tell "compute path is fine per-flow" from "compute->MPCDF is the wall".
ZENODO_URL = "https://zenodo.org/api/records/16921907/files/aimd_pristine_npt_300K.zip/content"


# --- low-level Range GETs (raw, so WE control pacing and can SEE 429s) ----------------------

def rget(session: requests.Session, up: str, start: int, end: int,
         retries: int = 6, honour_throttle: bool = True):
    """One Range GET of upload `up`. Returns (status, content_bytes, headers, seconds).

    With honour_throttle, a 429 is waited-out and retried (for the throughput measurements,
    which must not be polluted by the 5 s window). The PARALLEL tests pass honour_throttle=False
    so a 429 is returned verbatim — that IS the signal being measured."""
    url = f"{BASE}/uploads/{up}/raw"
    for _ in range(retries):
        t = time.monotonic()
        r = session.get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=180)
        body = r.content                                     # force the full transfer
        dt = time.monotonic() - t
        if r.status_code in (200, 206):
            return r.status_code, body, r.headers, dt
        if r.status_code == 429 and honour_throttle:
            time.sleep(UPLOAD_PACE)
            continue
        return r.status_code, body, r.headers, dt
    return 429, b"", {}, 0.0


def _parse_cd(cd: bytes) -> list[dict]:
    """Parse central-directory bytes -> [{name, method, comp, uncomp, off}], ZIP64-aware."""
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
                    vals = iter(struct.unpack_from(f"<{len(body) // 8}Q", body))
                    if uncomp == 0xFFFFFFFF:
                        uncomp = next(vals)
                    if comp == 0xFFFFFFFF:
                        comp = next(vals)
                    if off == 0xFFFFFFFF:
                        off = next(vals)
                j += 4 + hsz
        out.append(dict(name=name, method=method, comp=comp, uncomp=uncomp, off=off))
        i += 46 + nlen + elen + clen
    return out


def central_directory(session: requests.Session, up: str) -> tuple[list[dict], int]:
    """Read + parse an upload's (ZIP64) central directory over Range. Returns (members, total)."""
    _, blob, h, _ = _suffix(session, up, 40 << 20)
    total = int(h["Content-Range"].split("/")[-1])
    blob_start = total - len(blob)
    idx = blob.rfind(b"PK\x05\x06")
    if idx < 0:
        raise RuntimeError("no EOCD in tail")
    _, _, _, _, _tot, cd_size, cd_off, _ = struct.unpack("<IHHHHIIH", blob[idx:idx + 22])
    if cd_off == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:
        loc = blob[idx - 20:idx]
        z64_off = struct.unpack("<IIQI", loc)[2]
        _, z, _, _ = rget(session, up, z64_off, z64_off + 55)
        cd_size = struct.unpack("<Q", z[40:48])[0]
        cd_off = struct.unpack("<Q", z[48:56])[0]
    if cd_off >= blob_start:
        cd = blob[cd_off - blob_start: cd_off - blob_start + cd_size]
    else:
        _, cd, _, _ = rget(session, up, cd_off, cd_off + cd_size - 1)
    return _parse_cd(cd), total


def _suffix(session: requests.Session, up: str, n: int):
    """Suffix Range GET (last n bytes): returns (status, body, headers, seconds)."""
    url = f"{BASE}/uploads/{up}/raw"
    for _ in range(6):
        t = time.monotonic()
        r = session.get(url, headers={"Range": f"bytes=-{n}"}, timeout=180)
        body = r.content
        dt = time.monotonic() - t
        if r.status_code in (200, 206):
            return r.status_code, body, r.headers, dt
        if r.status_code == 429:
            time.sleep(UPLOAD_PACE)
            continue
        raise RuntimeError(f"suffix HTTP {r.status_code}")
    raise RuntimeError("suffix exhausted retries")


def _vaspruns(members: list[dict]) -> list[dict]:
    return sorted((m for m in members
                   if "vasprun" in m["name"].lower()
                   and m["name"].lower().endswith((".xml", ".xml.bz2", ".xml.gz"))),
                  key=lambda m: m["off"])


# --- an HTTP adapter that enlarges the socket receive buffer (tests window-limited flow) ----

def big_rcvbuf_session(rcvbuf: int = 16 << 20) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "materials-mlip-nomad/bottleneck-probe"
    if PoolManager is None:
        return s

    class _Adapter(HTTPAdapter):
        def init_poolmanager(self, connections, maxsize, block=False, **kw):  # type: ignore[override]
            kw["socket_options"] = [(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)]
            self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize,
                                           block=block, **kw)

    s.mount("https://", _Adapter())
    return s


# --- sampling -------------------------------------------------------------------------------

def sample_uploads(n: int = 6) -> list[str]:
    """Collect `n` distinct upload_ids that contain a vasprun mainfile (keyset scan)."""
    client = NomadClient()
    ups: list[str] = []
    seen: set[str] = set()
    for e in client.iter_entries(direct_upload_vasp_query(), required=CANDIDATE_REQUIRED,
                                 page_size=200, max_entries=4000):
        mf = (e.get("mainfile") or "").lower()
        up = e.get("upload_id")
        if up and up not in seen and "vasprun" in mf:
            seen.add(up)
            ups.append(up)
        if len(ups) >= n:
            break
    return ups


# --- the probe ------------------------------------------------------------------------------

def main() -> None:
    plain = requests.Session()
    plain.headers["User-Agent"] = "materials-mlip-nomad/bottleneck-probe"

    print("=" * 78)
    print("NOMAD compute-node fetch bottleneck probe")
    print("=" * 78)

    # [A] node identity, egress IP, RTT to both hosts ---------------------------------------
    print("\n[A] node / egress / RTT")
    print(f"    hostname       : {socket.gethostname()}")
    try:
        egress = plain.get("https://api.ipify.org", timeout=20).text.strip()
    except Exception as exc:  # noqa: BLE001
        egress = f"<unknown: {type(exc).__name__}>"
    print(f"    egress public IP: {egress}   "
          f"(compare across nodes -> same = shared NAT; differ = per-node throttle buckets)")
    for host in ("nomad-lab.eu", "zenodo.org"):
        try:
            ip = socket.gethostbyname(host)
            rtts = []
            for _ in range(6):
                t = time.monotonic()
                sk = socket.create_connection((ip, 443), timeout=10)
                sk.close()
                rtts.append((time.monotonic() - t) * 1000)
            print(f"    {host:<14}: {ip}   TCP-connect RTT median {statistics.median(rtts):.0f} ms "
                  f"(min {min(rtts):.0f})")
        except Exception as exc:  # noqa: BLE001
            print(f"    {host:<14}: RTT failed: {type(exc).__name__}: {exc}")

    print("\n    sampling uploads (keyset scan of direct-upload VASP-DFT) ...")
    try:
        ups = sample_uploads(6)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"could not sample uploads: {type(exc).__name__}: {exc}")
    if len(ups) < 2:
        raise SystemExit(f"need >=2 sampled uploads, got {len(ups)}")
    print(f"    got {len(ups)} uploads: {', '.join(u[:12] for u in ups)}")

    z_single = n_whole = n_whole_big = n_targeted = 0.0

    # [B] Zenodo single-connection throughput (the per-connection reference) ----------------
    print("\n[B] Zenodo single-connection throughput (per-connection reference, ~CERN)")
    try:
        t = time.monotonic()
        got = 0
        with plain.get(ZENODO_URL, stream=True, timeout=120) as r:
            if r.status_code == 200:
                for chunk in r.iter_content(1 << 20):
                    got += len(chunk)
                    if got >= 200 * MB:
                        break
        dt = time.monotonic() - t
        if got:
            z_single = got / MB / dt
            print(f"    {got/MB:.0f} MB in {dt:.1f}s -> {z_single:.1f} MB/s single connection")
            print(f"    (Zenodo harvest runs --workers 4, so its wall-clock ~= {z_single*4:.0f} MB/s)")
        else:
            print(f"    FAILED (HTTP {r.status_code}); pass a live --url or ignore [B]")
    except Exception as exc:  # noqa: BLE001
        print(f"    FAILED: {type(exc).__name__}: {exc}")

    # [C] NOMAD whole-upload single-connection throughput (+ big receive buffer) ------------
    print("\n[C] NOMAD whole-upload single-connection throughput (per-connection ceiling)")
    time.sleep(UPLOAD_PACE)
    st, body, h, dt = rget(plain, ups[0], 0, 120 * MB - 1)
    if st in (200, 206) and dt > 0:
        n_whole = len(body) / MB / dt
        total = int(h.get("Content-Range", "0/0").split("/")[-1])
        print(f"    default buffer  : {len(body)/MB:.0f} MB in {dt:.1f}s -> {n_whole:.1f} MB/s "
              f"(upload {total/1e9:.1f} GB)")
    else:
        print(f"    default buffer  : FAILED (HTTP {st})")
    time.sleep(UPLOAD_PACE)
    big = big_rcvbuf_session()
    st, body, h, dt = rget(big, ups[0], 0, 120 * MB - 1)
    if st in (200, 206) and dt > 0:
        n_whole_big = len(body) / MB / dt
        print(f"    16 MB SO_RCVBUF : {len(body)/MB:.0f} MB in {dt:.1f}s -> {n_whole_big:.1f} MB/s "
              f"({'FASTER -> flow was window-limited' if n_whole_big > n_whole*1.25 else 'no gain -> not window-limited'})")
    else:
        print(f"    16 MB SO_RCVBUF : FAILED (HTTP {st})")

    # [D] NOMAD targeted multi-range throughput (the real fetch pattern) ---------------------
    print("\n[D] NOMAD targeted multi-range throughput (real fetch pattern)")
    try:
        time.sleep(UPLOAD_PACE)
        members, total = central_directory(plain, ups[0])
        vas = _vaspruns(members)[:200]
        # Non-overlapping exact-ish ranges (one member per part; server may still coalesce).
        seg, last = [], -1
        for m in vas:
            end = m["off"] + 30 + len(m["name"]) + 64 + m["comp"] - 1
            if m["off"] > last:
                seg.append((m["off"], end))
                last = end
        hdr = ",".join(f"{s}-{e}" for s, e in seg)
        print(f"    cd: {len(members)} members, {len(vas)} vaspruns targeted, "
              f"Range header {len(hdr)} bytes")
        time.sleep(UPLOAD_PACE)
        t = time.monotonic()
        r = plain.get(f"{BASE}/uploads/{ups[0]}/raw",
                      headers={"Range": f"bytes={hdr}"}, timeout=300)
        body = r.content
        dt = time.monotonic() - t
        if r.status_code in (200, 206) and dt > 0:
            n_targeted = len(body) / MB / dt
            print(f"    HTTP {r.status_code}  {len(body)/MB:.1f} MB in {dt:.1f}s -> "
                  f"{n_targeted:.1f} MB/s targeted")
            if n_whole:
                print(f"    whole:targeted ratio = {n_whole/max(n_targeted,0.1):.1f}x "
                      f"(how much a whole-upload stream beats scattered multi-range)")
        else:
            print(f"    FAILED (HTTP {r.status_code})")
    except Exception as exc:  # noqa: BLE001
        print(f"    FAILED: {type(exc).__name__}: {exc}")

    # [E] parallelism semantics — the make-or-break throttle test ---------------------------
    print("\n[E] NOMAD parallelism semantics (can we beat 1 connection WITHOUT an exemption?)")
    parallel_ok = staggered_ok = False

    # E1: two SIMULTANEOUS streams to DIFFERENT uploads. If the 2nd 429s -> a 2nd in-flight
    #     connection is forbidden. If both 206 and aggregate > single -> parallel is allowed.
    print("    E1: 2 concurrent streams to different uploads")
    time.sleep(UPLOAD_PACE)

    def _pull(up: str, nbytes: int):
        s = requests.Session()
        s.headers["User-Agent"] = "materials-mlip-nomad/bottleneck-probe"
        return rget(s, up, 0, nbytes - 1, retries=1, honour_throttle=False)

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(_pull, ups[0], 40 * MB)
        f2 = ex.submit(_pull, ups[1], 40 * MB)
        (s1, b1, _, d1), (s2, b2, _, d2) = f1.result(), f2.result()
    wall = time.monotonic() - t0
    agg = (len(b1) + len(b2)) / MB
    print(f"       stream1 HTTP {s1} ({len(b1)/MB:.0f} MB/{d1:.1f}s)   "
          f"stream2 HTTP {s2} ({len(b2)/MB:.0f} MB/{d2:.1f}s)")
    if s1 in (200, 206) and s2 in (200, 206):
        parallel_ok = True
        print(f"       BOTH SUCCEEDED -> aggregate {agg/wall:.1f} MB/s (vs {n_whole:.1f} single). "
              f"Parallel from ONE IP is allowed!")
    else:
        print("       a 429 -> a 2nd simultaneous connection is refused (throttle is real).")

    # E2: start a LONG stream, wait 5.1 s (past the "every 5 s" window), start a 2nd while the
    #     1st is STILL in flight. 206 here (with the 1st not yet done) means "1 in-flight" is NOT
    #     enforced, only "new every 5 s" -> we can PIPELINE overlapping connections 5 s apart.
    print("    E2: 2nd stream started 5.1 s into a still-in-flight 1st stream")
    time.sleep(UPLOAD_PACE)
    hold: dict = {}

    def _long(up: str):
        s = requests.Session()
        st, b, _, d = rget(s, up, 0, 150 * MB - 1, retries=1, honour_throttle=False)
        hold["end"] = time.monotonic()
        hold["st"] = st
        hold["mb"] = len(b) / MB

    with ThreadPoolExecutor(max_workers=2) as ex:
        fl = ex.submit(_long, ups[2 % len(ups)])
        time.sleep(5.1)
        start2 = time.monotonic()
        st2, b2, _, d2 = _pull(ups[3 % len(ups)], 15 * MB)
        in_flight = "end" not in hold or hold.get("end", 0) > start2
        fl.result()
    print(f"       1st: HTTP {hold.get('st')} ({hold.get('mb',0):.0f} MB)   "
          f"2nd(@5.1s): HTTP {st2} ({len(b2)/MB:.0f} MB)   "
          f"overlapped={in_flight}")
    if st2 in (200, 206) and in_flight:
        staggered_ok = True
        print("       2nd SUCCEEDED while 1st in flight -> only 'new every 5 s' is enforced; "
              "staggered pipelining (~Nx) is possible from ONE IP.")
    elif st2 == 429:
        print("       2nd 429 -> '1 in-flight per IP' IS enforced; the fetch is truly serial "
              "from one IP (need multiple IPs or an exemption to parallelise).")

    # [F] bloat ratio across sampled uploads -> hybrid whole-download payoff -----------------
    print("\n[F] bloat ratio (wanted-vasprun bytes / whole-upload bytes) per upload")
    ratios = []
    for up in ups:
        try:
            time.sleep(UPLOAD_PACE)
            members, total = central_directory(plain, up)
            want = sum((m["comp"] if m["method"] != 8 else m["uncomp"]) for m in _vaspruns(members))
            r = want / total if total else 0.0
            ratios.append(r)
            print(f"    {up[:12]}: {len(members):>7} members, upload {total/1e9:6.2f} GB, "
                  f"vasprun {want/MB:8.1f} MB -> bloat ratio {r:6.3f}")
        except Exception as exc:  # noqa: BLE001
            print(f"    {up[:12]}: cd failed: {type(exc).__name__}")
    bloat_med = statistics.median(ratios) if ratios else 0.0
    if ratios:
        print(f"    median wanted/total = {bloat_med:.3f}  "
              f"(high -> whole-download wastes little; low -> targeted is essential)")

    # [G] verdict ---------------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("[G] VERDICT")
    print("=" * 78)
    if z_single and n_whole:
        if z_single > n_whole * 1.8:
            print(f"  * Per-connection, Zenodo({z_single:.0f}) >> NOMAD-whole({n_whole:.0f}) MB/s on THIS")
            print("    node: the compute->MPCDF(Germany) single flow itself is the wall, not the")
            print("    access pattern. Levers: TCP buffer (see [C]), OFF-PEAK (German night), a")
            print("    faster egress node, or multiple IPs (see [E]/launcher). Parse is already")
            print("    parallel, so ~9 days is otherwise the floor -> consider scoping --max-entries.")
        else:
            print(f"  * Per-connection Zenodo({z_single:.0f}) ~= NOMAD-whole({n_whole:.0f}) MB/s: the")
            print("    compute path is FINE per-flow. The gap vs the real job is (a) serialization")
            print("    (1 vs 4 connections) and (b) the targeted pattern. Fixes below.")
    if n_whole and n_targeted:
        print(f"  * whole:targeted = {n_whole/max(n_targeted,0.1):.1f}x, median bloat {bloat_med:.2f} ->",
              "HYBRID whole-download for low-bloat uploads is worth building"
              if (n_whole > n_targeted * 1.3 and bloat_med > 0.25)
              else "targeted stays best (keep it); whole-download only for tiny/low-bloat uploads")
    if parallel_ok or staggered_ok:
        print("  * [E] shows PARALLEL/STAGGERED streams work from one IP -> biggest win, no")
        print("    exemption: build a parallel/staggered fetch (respect 'new every 5 s'). ~Nx.")
    else:
        print("  * [E] confirms 1-in-flight-per-IP: single IP is serial. Check the launcher's")
        print("    two-node egress IPs — if they DIFFER, run the pipeline on K nodes (K disjoint")
        print("    upload slices -> own dataset dir -> merge) for ~Kx with NO exemption.")
    # Time = max(transfer-bound, THROTTLE-bound). Every request costs ~5 s regardless of size, so
    # the sustained rate is NOT the single-request MB/s above when the pattern makes many small
    # requests. TARGETED is throttle-bound: members are capped at ~250/request by the 8 KB Range
    # header + 1 CD read/upload + small uploads under-fill batches (the real job averaged ~47
    # calcs/request => ~150k requests => ~9 days). WHOLE-download is transfer-bound: one long
    # request per upload, so its MB/s IS sustainable.
    UPLOADS, WANTED_B = 3792, N_FULL * 262 * 1024        # ~1.9 TB of vasprun bytes
    if n_targeted:
        reqs_best = N_FULL / 250 + UPLOADS               # best case: full 250-member batches
        thr_best = reqs_best * 5 / 86400
        thr_real = (N_FULL / 47 + UPLOADS) * 5 / 86400   # real job's ~47 calcs/request
        xfer = WANTED_B / MB / n_targeted / 86400
        print(f"  * TARGETED full-7.1M: transfer-bound {xfer:.1f}d, but THROTTLE-bound "
              f"{thr_best:.1f}d best / ~{thr_real:.0f}d at the real ~47 calcs/request "
              f"-> ~{max(xfer, thr_real):.0f} days (this is why the live job is ~9d, not ~1.5d).")
    if n_whole and bloat_med:
        xfer = (WANTED_B / bloat_med) / MB / n_whole / 86400   # whole-download low-bloat uploads
        thr = 2 * UPLOADS * 5 / 86400
        print(f"  * WHOLE/HYBRID full-7.1M: transfer-bound {xfer:.1f}d (pull ~{1/bloat_med:.1f}x "
              f"wanted bytes at {n_whole:.0f} MB/s), throttle floor {thr:.1f}d "
              f"-> ~{max(xfer, thr):.1f} days. THE win: 1 request/upload, no 5 s-per-batch tax.")
    print("=" * 78)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""Materials Cloud pilot benchmark — run on a CSD3 compute node AFTER ``10_discover.sh``.

Answers "is the MC harvest fetch- or parse-bound, and how should ``--workers``,
``--parse-workers`` and ``--max-primary-bytes`` be set?" with the REAL code on REAL data:

1. picks a stratified sample of fetch units from ``mc_keep.jsonl``: the largest tar-family unit
   that fits half the budget (the long-AIMD type), the smallest Bosoni ACWF VASP export (the
   many-small-calcs type), then random others (seeded) up to ``--budget-gb``;
2. fetches them with the real MC fetch (anonymous session, ``--workers``, transient retries) and
   times it — MB/s INCLUDING extraction, extraction ratio, calcs per downloaded GB;
3. profiles the primaries (count, uncompressed-size distribution);
4. times the real per-calc parse path — ``zenodo_harvest.parse._parse_one``: a forkserver child per
   calc under the timeout guard, exactly what ``pipeline`` runs — on up to ``--small-n`` small calcs
   serially and on another ``--small-n`` with ``--parse-workers`` threads → s/calc + speed-up;
5. measures wall time + peak RSS of the ``--big-k`` largest primaries, each in a fresh subprocess
   (``scripts/csd3/csd3_parse_memory.py``'s method) → s/GB and the RSS/size ratio that sets
   ``--max-primary-bytes`` (a primary too big for this job's RAM is reported, not parsed);
6. projects the whole harvest from the triage report's ``bytes_to_fetch``: fetch hours vs parse
   hours, and suggested settings.

Writes nothing into the real MC tree except ``--out``: the sample is staged under ``--work``
(deleted afterwards unless ``--keep-work``). Usage — see scripts/csd3/materials_cloud/15_bench.sh.
"""

from __future__ import annotations

import argparse
import bz2
import importlib.util
import json
import lzma
import os
import random
import shutil
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))  # run without install

from materials_cloud_harvest.client import new_session  # noqa: E402
from materials_cloud_harvest.fetching import fetch_with_retries  # noqa: E402
from zenodo_harvest.fetch import _dir_usage  # noqa: E402
from zenodo_harvest.fetch import fetch as shared_fetch  # noqa: E402
from zenodo_harvest.manifest import RejectionLogger, read_jsonl, write_jsonl  # noqa: E402

BOSONI = "yf0rj-w3r97"
SMALL_MAX_BYTES = 100_000_000      # a "small" calc: primary < 100 MB uncompressed
RATIO_MIN_BYTES = 50_000_000       # RSS/size ratios are only meaningful for files at least this big
BASELINE_MIB = 256.0               # parse child's interpreter + pymatgen/ASE import footprint
_TAR_LIKE = (".tar.gz", ".tgz", ".tar", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".tar.zst")


def _is_tar_unit(u: dict[str, Any]) -> bool:
    return bool(u.get("files")) and all(str(f["key"]).lower().endswith(_TAR_LIKE)
                                        for f in u["files"])


def select_sample(units: list[dict[str, Any]], budget_bytes: int,
                  seed: int = 0) -> list[dict[str, Any]]:
    """The stratified sample (see the module docstring); deterministic for a given seed."""
    chosen: list[dict[str, Any]] = []
    ids: set[str] = set()
    used = 0

    def take(u: dict[str, Any]) -> None:
        nonlocal used
        if u["recid"] not in ids:
            chosen.append(u)
            ids.add(u["recid"])
            used += int(u.get("bytes_total") or 0)

    tars = sorted((u for u in units if _is_tar_unit(u)), key=lambda u: -int(u["bytes_total"]))
    big = next((u for u in tars if int(u["bytes_total"]) <= budget_bytes // 2), None)
    if big:
        take(big)
    bos = [u for u in units if (u.get("unit") or {}).get("record_id", u["recid"]) == BOSONI]
    if bos:
        take(min(bos, key=lambda u: int(u["bytes_total"])))
    rest = [u for u in units if u["recid"] not in ids]
    random.Random(seed).shuffle(rest)
    for u in rest:
        if used + int(u.get("bytes_total") or 0) <= budget_bytes:
            take(u)
    return chosen


def uncompressed_size(path: Path, cap: int = 200_000_000_000) -> int:
    """True uncompressed byte size of a primary (gzip ISIZE; bz2/xz stream-counted to ``cap``)."""
    from zenodo_harvest.parse import _effective_primary_size
    suf = path.suffix.lower()
    if suf in (".bz2", ".xz", ".lzma"):
        opener = bz2.open if suf == ".bz2" else lzma.open
        n = 0
        with opener(path, "rb") as fh:
            while (chunk := fh.read(1 << 22)) and n <= cap:
                n += len(chunk)
        return n
    return _effective_primary_size(str(path), 0)


def project(total_bytes: int, sample_bytes: int, fetch_wall_s: float, n_small: int,
            small_serial_s: float | None, small_parallel_s: float | None,
            big_gb: float, big_s_per_gb: float | None) -> dict[str, Any]:
    """Scale the sample's measured costs to the whole harvest (``total_bytes`` to fetch)."""
    scale = total_bytes / sample_bytes if sample_bytes else 0.0
    fetch_h = (fetch_wall_s * scale / 3600) if sample_bytes else None
    big_part = big_gb * (big_s_per_gb or 0.0)
    ser = (n_small * small_serial_s + big_part) * scale / 3600 if small_serial_s else None
    par = (n_small * small_parallel_s + big_part) * scale / 3600 if small_parallel_s else None
    bound = None
    if fetch_h is not None and par is not None:
        bound = "parse" if par > fetch_h else "fetch"
    return {"scale_factor": round(scale, 2), "fetch_hours": _r(fetch_h),
            "parse_hours_serial": _r(ser), "parse_hours_with_workers": _r(par),
            "likely_bottleneck": bound,
            "note": ("crude: assumes the sample's calcs/GB and size mix hold for the whole "
                     "keep-list; the pipeline overlaps fetch and parse, so wall ~ max of the two")}


def _r(x: float | None) -> float | None:
    return None if x is None else round(x, 2)


def _load_parse_memory_helper() -> Any:
    spec = importlib.util.spec_from_file_location(
        "csd3_parse_memory", REPO / "scripts" / "csd3" / "csd3_parse_memory.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _alloc_gb() -> float | None:
    cpus = int(os.environ.get("SLURM_CPUS_PER_TASK", "0") or 0)
    memc = os.environ.get("SLURM_MEM_PER_CPU")
    return (cpus * int(memc) / 1024.0) if (cpus and memc) else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep", required=True, help="mc_keep.jsonl (fetch units) from triage")
    ap.add_argument("--report", default=None, help="mc_keep.report.json (for bytes_to_fetch)")
    ap.add_argument("--work", required=True, help="scratch dir for the sample (on /rds)")
    ap.add_argument("--out", required=True, help="write the JSON result here")
    ap.add_argument("--budget-gb", type=float, default=12.0)
    ap.add_argument("--workers", type=int, default=4, help="fetch concurrency to time")
    ap.add_argument("--parse-workers", type=int, default=4, help="parse concurrency to time")
    ap.add_argument("--small-n", type=int, default=150, help="calcs per parse-timing arm")
    ap.add_argument("--big-k", type=int, default=6, help="largest primaries to RSS-profile")
    ap.add_argument("--big-max-gb", type=float, default=None,
                    help="skip RSS-profiling primaries above this (default: job RAM / 12)")
    ap.add_argument("--parse-timeout", type=float, default=1800)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--keep-work", action="store_true")
    args = ap.parse_args()

    work = Path(args.work)
    man, raw = work / "manifests", work / "raw"
    man.mkdir(parents=True, exist_ok=True)
    res: dict[str, Any] = {"generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
                           "host": os.uname().nodename, "alloc_gb": _alloc_gb()}
    units = list(read_jsonl(args.keep))
    total_bytes = sum(int(u.get("bytes_total") or 0) for u in units)
    if args.report and Path(args.report).is_file():
        total_bytes = int(json.loads(Path(args.report).read_text())["summary"]["bytes_to_fetch"])
    sample = select_sample(units, int(args.budget_gb * 1e9), args.seed)
    res["keep_units"], res["keep_bytes_to_fetch"] = len(units), total_bytes
    res["sample_units"] = [{"recid": u["recid"], "bytes": u["bytes_total"],
                            "files": [f["key"] for f in u["files"]]} for u in sample]
    print(f"sample: {len(sample)} unit(s), {sum(u['bytes_total'] for u in sample) / 1e9:.2f} GB "
          f"of {total_bytes / 1e9:.1f} GB to fetch", flush=True)
    keep_s = man / "bench_keep.jsonl"
    write_jsonl(keep_s, sample)
    fetched, rej = man / "bench_fetched.jsonl", man / "bench_fetch_rejections.jsonl"

    # -- 2. fetch ---------------------------------------------------------------------------
    t0 = time.monotonic()
    fsum = fetch_with_retries(
        lambda: shared_fetch(keep_s, out_path=fetched, raw_dir=raw, rejections_path=rej,
                             max_bytes=None, max_member_bytes=1 << 62, workers=args.workers,
                             session_factory=new_session),
        keep_s, fetched, rej, retries=4)
    fetch_wall = time.monotonic() - t0
    frecs = list(read_jsonl(fetched)) if fetched.is_file() else []
    got = {r["recid"] for r in frecs}
    sample_bytes = sum(int(u["bytes_total"]) for u in sample if u["recid"] in got)
    staged_b, staged_i = _dir_usage(raw)
    n_calcs = sum(int(r["n_calc_units"]) for r in frecs)
    res["fetch"] = {"wall_s": round(fetch_wall, 1), "units_fetched": len(frecs),
                    "downloaded_GB": round(sample_bytes / 1e9, 2),
                    "MBps_incl_extraction": round(sample_bytes / fetch_wall / 1e6, 1)
                    if fetch_wall else None,
                    "staged_GB": round(staged_b / 1e9, 2), "staged_inodes": staged_i,
                    "extraction_ratio": round(staged_b / sample_bytes, 2) if sample_bytes else None,
                    "calc_units": n_calcs,
                    "calcs_per_downloaded_GB": round(n_calcs / (sample_bytes / 1e9), 1)
                    if sample_bytes else None,
                    "pending_after_retries": fsum.get("pending_after_retries")}
    print(f"fetch: {res['fetch']}", flush=True)

    # -- 3. primaries -----------------------------------------------------------------------
    from zenodo_harvest.parse import _calc_id, _parse_one, _resolve
    rows: list[dict[str, Any]] = []
    for rec in frecs:
        bm = {"provenance": rec["provenance"],
              "_extracted_root": str(_resolve(raw, rec["local_dir"]) / "extracted")}
        avs = rec.get("calc_availability") or []
        for i, unit in enumerate(rec["calc_units"]):
            ua = {k: str(_resolve(raw, v)) for k, v in unit.items()}
            prim = ua.get("vasprun") or ua.get("vaspout") or ua.get("outcar")
            if not prim:
                continue
            rows.append({"unit": ua, "bm": bm, "av": avs[i] if i < len(avs) else {},
                         "cid": _calc_id(ua, bm), "prim": prim,
                         "size": uncompressed_size(Path(prim))})
    sizes = sorted(r["size"] for r in rows)
    if sizes:
        res["primaries"] = {"n": len(sizes), "total_GB": round(sum(sizes) / 1e9, 2),
                            "median_MB": round(statistics.median(sizes) / 1e6, 2),
                            "p90_MB": round(sizes[int(0.9 * (len(sizes) - 1))] / 1e6, 1),
                            "max_MB": round(sizes[-1] / 1e6, 1),
                            "n_over_1GB": sum(1 for x in sizes if x > 1e9),
                            "n_over_5GB": sum(1 for x in sizes if x > 5e9)}
        print(f"primaries: {res['primaries']}", flush=True)

    # -- 4. small-calc parse throughput (the real forkserver-child path) ---------------------
    small = [r for r in rows if r["size"] < SMALL_MAX_BYTES]
    random.Random(args.seed).shuffle(small)
    rj = RejectionLogger(man / "bench_parse_rejections.jsonl")

    def _p(r: dict[str, Any]) -> bool:
        return _parse_one(r["unit"], r["bm"], r["av"], rj, 0, args.parse_timeout,
                          r["cid"]) is not None

    ser_s = par_s = None
    if small:
        _p(small[0])                       # warm the forkserver (pymatgen preload) untimed
        a, b = small[1:1 + args.small_n], small[1 + args.small_n:1 + 2 * args.small_n]
        if a:
            t = time.monotonic()
            ok_a = sum(_p(r) for r in a)
            ser_s = (time.monotonic() - t) / len(a)
        if b:
            t = time.monotonic()
            with ThreadPoolExecutor(max_workers=args.parse_workers) as ex:
                ok_b = sum(ex.map(_p, b))
            par_s = (time.monotonic() - t) / len(b)
        res["small_parse"] = {"n_small_in_sample": len(small),
                              "serial_s_per_calc": _r(ser_s), "serial_n": len(a),
                              "serial_ok": ok_a if a else None,
                              f"with_{args.parse_workers}_workers_s_per_calc": _r(par_s),
                              "parallel_n": len(b), "parallel_ok": ok_b if b else None,
                              "speedup": _r(ser_s / par_s) if (ser_s and par_s) else None}
        print(f"small parse: {res['small_parse']}", flush=True)
    rj.close()

    # -- 5. largest primaries: wall time + peak RSS in fresh subprocesses --------------------
    pm = _load_parse_memory_helper()
    alloc = res["alloc_gb"]
    big_max = (args.big_max_gb * 1e9 if args.big_max_gb else
               (alloc * 1e9 * 0.8 / 12 if alloc else 4e9))
    big_rows = sorted(rows, key=lambda r: -r["size"])[:args.big_k]
    profiled, skipped = [], []
    for r in big_rows:
        if r["size"] > big_max:
            skipped.append({"file": r["prim"].rsplit("/", 1)[-1], "GB": round(r["size"] / 1e9, 2)})
            continue
        t = time.monotonic()
        m = pm._measure(Path(r["prim"]), str(REPO), int(args.parse_timeout))
        wall = time.monotonic() - t
        if m:
            peak_mb, nfr = m
            gb = r["size"] / 1e9
            # net of the child's interpreter + pymatgen/ASE import footprint, which dominates a
            # small file's peak (the ratio that sizes --max-primary-bytes is the per-byte cost)
            net = max(0.0, peak_mb - BASELINE_MIB) * 1.048576 / (r["size"] / 1e6)
            profiled.append({"file": r["prim"].rsplit("/", 1)[-1], "GB": round(gb, 3),
                             "wall_s": round(wall, 1), "frames": nfr,
                             "peak_GiB": round(peak_mb / 1024, 2),
                             "rss_ratio_net": round(net, 1),
                             "s_per_GB": round(wall / gb, 1) if gb else None})
            print(f"  big: {profiled[-1]}", flush=True)
    # only LARGE files say anything about the per-byte RAM/time cost (a small file's figures are
    # the child's start-up + import cost, already covered by the small-calc arm above)
    sizing = [p for p in profiled if p["GB"] * 1e9 >= RATIO_MIN_BYTES]
    worst = max((p["rss_ratio_net"] for p in sizing), default=None)
    slow = [p["s_per_GB"] for p in profiled if p["GB"] * 1e9 >= SMALL_MAX_BYTES and p["s_per_GB"]]
    s_per_gb = statistics.median(slow) if slow else None
    res["big_parse"] = {"profiled": profiled, "skipped_over_ram_budget": skipped,
                        "worst_rss_ratio": worst, "median_s_per_GB": _r(s_per_gb),
                        "note": (f"ratios/s-per-GB only from primaries >= "
                                 f"{RATIO_MIN_BYTES / 1e6:.0f} MB / {SMALL_MAX_BYTES / 1e6:.0f} MB; "
                                 "None = the sample had no such file")}

    # -- 6. projection + suggestions ---------------------------------------------------------
    big_gb = sum(r["size"] for r in rows if r["size"] >= SMALL_MAX_BYTES) / 1e9
    res["projection"] = project(total_bytes, sample_bytes, fetch_wall, len(small), ser_s, par_s,
                                big_gb, s_per_gb)
    ratio = max(worst or 0.0, 12.0)
    sugg: dict[str, Any] = {"rss_ratio_assumed": ratio}
    for cpus in (12, 16, 24, 32):
        ram = cpus * 6760 / 1024                      # icelake-himem GiB
        for w in (1, 2, 3, 4):
            cap = (0.85 * ram - 8) / (w * ratio)
            sugg[f"cpus{cpus}_workers{w}_max_primary_GB"] = round(cap * 1.073741824, 2)
    res["suggested_caps"] = sugg
    Path(args.out).write_text(json.dumps(res, indent=1))
    print(json.dumps({k: res[k] for k in ("fetch", "projection") if k in res}, indent=1))
    print(f"result -> {args.out}")
    if not args.keep_work:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

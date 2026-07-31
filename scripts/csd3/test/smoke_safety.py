#!/usr/bin/env python
"""Safety-measure + resumability smoke test, driven in-process against real Zenodo data.

Given a SMALL keep-list (see make_test_keeplist.py), this exercises every safety valve and
every resume path end-to-end, sizing the valve limits ADAPTIVELY from the data's own measured
footprint so they are guaranteed to trip on whatever the test records turn out to be:

  1. disk-BYTES valve  — fetch->parse->purge->resume pacing under --max-disk-bytes;
                         asserts the valve trips AND peak staged bytes never exceed the limit.
  2. disk-FILES valve  — same, for the inode budget (--max-disk-files); CSD3's binding limit.
  3. primary/RAM guard — parse with a tiny --max-primary-bytes rejects oversized primaries
                         (primary_too_large), and an uncapped re-parse RECOVERS them (non-terminal).
  4. fetch resume      — a partial fetch (--max-records) then a full fetch skips what is done.
  5. parse resume      — re-parsing skips calcs already in metadata (no duplicate frames).

Each test runs in its own fresh sub-dir, then a final `verify_dataset` confirms the paced
dataset's frame_id bijection is intact. Prints a PASS/FAIL table and exits non-zero on any
failure, so a batch job's exit code is the verdict. Reuses the ZENODO_TOKEN from the env.

Run from the repo root (needs the `parse` extra: pymatgen + ase).
"""
from __future__ import annotations

import argparse
import os
import shutil
import statistics
import sys
from pathlib import Path

from zenodo_harvest.dataset_ops import purge_raw, verify_dataset
from zenodo_harvest.fetch import fetch
from zenodo_harvest.parse import _effective_primary_size, parse

HUGE = 1 << 50  # "no real limit", but enables the budget so peak_staged_* is tracked


def dir_usage(path: Path) -> tuple[int, int]:
    """(bytes, inodes) under path; inodes = files + dirs (Lustre quota model)."""
    if not path.is_dir():
        return 0, 0
    b = n = 0
    for root, dirs, files in os.walk(path):
        n += len(dirs)
        for f in files:
            try:
                b += os.stat(os.path.join(root, f)).st_size
                n += 1
            except OSError:
                pass
    return b, n


def per_record_max(raw: Path) -> tuple[int, int]:
    """Largest single-record (bytes, inodes) under raw/<recid>/ — for valve sizing."""
    mb = mi = 0
    if raw.is_dir():
        for d in raw.iterdir():
            if d.is_dir():
                b, i = dir_usage(d)
                mb, mi = max(mb, b), max(mi, i + 1)  # +1 for the record dir itself
    return mb, mi


def median_primary_size(raw: Path) -> int:
    sizes = []
    for p in raw.rglob("*"):
        if not p.is_file():
            continue
        b = p.name.lower()
        if ("outcar" in b) or (b.startswith("vasprun") or ".vasprun" in b) and b.endswith((".xml", ".xml.gz")) \
                or b == "vaspout.h5":
            sizes.append(_effective_primary_size(str(p)))
    return int(statistics.median(sizes)) if sizes else 0


def paced_run(keep: Path, raw: Path, ds: Path, fetched: Path, rej: Path,
              max_disk_bytes: int | None, max_disk_files: int | None,
              token: str | None, max_passes: int = 100) -> tuple[list, bool, int]:
    """Mirror the pipeline's fetch->parse->purge->resume loop for ONE part, capturing each
    fetch pass's peak. Returns (peaks[(bytes,files,unfittable)], tripped, calcs_parsed_total)."""
    peaks: list[tuple[int, int, int]] = []
    tripped = False
    for _ in range(max_passes):
        s = fetch(str(keep), out_path=str(fetched), raw_dir=str(raw), rejections_path=str(rej),
                  max_bytes=None, token=token, max_member_bytes=1 << 62,
                  max_disk_bytes=max_disk_bytes, max_disk_files=max_disk_files, workers=1)
        peaks.append((s["peak_staged_bytes"], s["peak_staged_files"], s["items_over_whole_budget"]))
        parse(str(fetched), dataset_dir=str(ds), rejections_path=str(ds / "rejections.jsonl"),
              raw_dir=str(raw))
        purge_raw(str(raw), str(ds), fetched=str(fetched))
        if not s["stopped_disk_budget"]:
            break
        tripped = True
    calcs = sum(1 for _ in (ds / "metadata.jsonl").open()) if (ds / "metadata.jsonl").is_file() else 0
    return peaks, tripped, calcs


def _p(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep", required=True, help="small keep-list JSONL (make_test_keeplist.py)")
    ap.add_argument("--work-dir", required=True, help="fresh scratch dir for the test (created)")
    args = ap.parse_args()
    keep = Path(args.keep)
    work = Path(args.work_dir)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    token = os.environ.get("ZENODO_TOKEN")
    results: dict[str, tuple[bool, str]] = {}

    # ---- baseline: one uncapped fetch (budget enabled at HUGE so peak is tracked) + parse ----
    raw_b, fet_b, ds_b, rej_b = work / "raw_base", work / "fetched_base.jsonl", work / "ds_base", work / "rej_base.jsonl"
    sb = fetch(str(keep), out_path=str(fet_b), raw_dir=str(raw_b), rejections_path=str(rej_b),
               max_bytes=None, token=token, max_member_bytes=1 << 62,
               max_disk_bytes=HUGE, max_disk_files=HUGE, workers=2)
    pb = parse(str(fet_b), dataset_dir=str(ds_b), rejections_path=str(ds_b / "rej.jsonl"), raw_dir=str(raw_b))
    base_calcs, base_frames = pb["calcs_parsed"], pb["frames"]
    tot_b, tot_i = dir_usage(raw_b)
    max_b, max_i = per_record_max(raw_b)
    print(f"[baseline] records fetched={sb['fetched']} calc_units={sb['calc_units']} "
          f"parsed_calcs={base_calcs} frames={base_frames}")
    print(f"[baseline] extracted footprint: {tot_b/1e6:.1f} MB / {tot_i} inodes   "
          f"largest single record: {max_b/1e6:.1f} MB / {max_i} inodes   "
          f"transient peak: {sb['peak_staged_bytes']/1e6:.1f} MB / {sb['peak_staged_files']} inodes")
    if base_calcs == 0:
        print("ERROR: baseline parsed 0 calcs — keep-list has no parseable VASP; pick other records.",
              file=sys.stderr)
        return 2

    # ---- 1. disk-BYTES valve ----
    limit_b = max(int(0.5 * tot_b), int(1.6 * max_b) + (1 << 20))
    clean_b = limit_b < tot_b  # below total -> must trip; above largest*1.6 -> shouldn't truncate
    if limit_b >= tot_b:
        limit_b = int(0.8 * tot_b)
    peaks, tripped, calcs = paced_run(keep, work / "raw_vb", work / "ds_vb", work / "fet_vb.jsonl",
                                      work / "rej_vb.jsonl", limit_b, None, token)
    peak_b = max(p[0] for p in peaks)
    trunc = sum(p[2] for p in peaks)
    ver_ok = verify_dataset(str(work / "ds_vb"))["ok"]
    ok = tripped and peak_b <= limit_b and ver_ok and (calcs == base_calcs or trunc > 0)
    results["1. disk-bytes valve"] = (ok, f"limit={limit_b/1e6:.1f}MB peak={peak_b/1e6:.1f}MB "
                                          f"(<=limit:{peak_b<=limit_b}) tripped={tripped} passes={len(peaks)} "
                                          f"calcs={calcs}/{base_calcs} truncated={trunc} verify_ok={ver_ok}")

    # ---- 2. disk-FILES valve ----
    limit_i = max(int(0.5 * tot_i), int(1.6 * max_i) + 5)
    if limit_i >= tot_i:
        limit_i = int(0.8 * tot_i)
    peaks, tripped, calcs = paced_run(keep, work / "raw_vf", work / "ds_vf", work / "fet_vf.jsonl",
                                      work / "rej_vf.jsonl", None, limit_i, token)
    peak_i = max(p[1] for p in peaks)
    trunc = sum(p[2] for p in peaks)
    ver_ok = verify_dataset(str(work / "ds_vf"))["ok"]
    ok = tripped and peak_i <= limit_i and ver_ok and (calcs == base_calcs or trunc > 0)
    results["2. disk-files valve"] = (ok, f"limit={limit_i} peak={peak_i} (<=limit:{peak_i<=limit_i}) "
                                          f"tripped={tripped} passes={len(peaks)} calcs={calcs}/{base_calcs} "
                                          f"truncated={trunc} verify_ok={ver_ok}")

    # ---- 3. primary/RAM guard (reuse baseline raw + fetched manifest) ----
    med = median_primary_size(raw_b)
    cap = max(int(med), 1)  # ~half the primaries exceed the median -> rejected
    ds_pg = work / "ds_pg"
    p1 = parse(str(fet_b), dataset_dir=str(ds_pg), rejections_path=str(ds_pg / "rej.jsonl"),
               raw_dir=str(raw_b), max_primary_bytes=cap)
    rej_txt = (ds_pg / "rej.jsonl").read_text() if (ds_pg / "rej.jsonl").is_file() else ""
    n_too_large = rej_txt.count('"primary_too_large"')
    p2 = parse(str(fet_b), dataset_dir=str(ds_pg), rejections_path=str(ds_pg / "rej.jsonl"),
               raw_dir=str(raw_b), max_primary_bytes=0)  # uncapped re-parse recovers
    final_calcs = p1["calcs_parsed"] + p2["calcs_parsed"]
    ok = n_too_large > 0 and p1["calcs_parsed"] < base_calcs and final_calcs == base_calcs \
        and p2["skipped_existing"] == p1["calcs_parsed"]
    results["3. primary/RAM guard"] = (ok, f"cap={cap}B(median primary) too_large={n_too_large} "
                                           f"capped_calcs={p1['calcs_parsed']} recovered_to={final_calcs}/{base_calcs}")

    # ---- 4. fetch resume (record-level) ----
    raw_r, fet_r = work / "raw_res", work / "fet_res.jsonl"
    r1 = fetch(str(keep), out_path=str(fet_r), raw_dir=str(raw_r), rejections_path=str(work / "rej_r.jsonl"),
               max_bytes=None, token=token, max_records=2, workers=1)
    r2 = fetch(str(keep), out_path=str(fet_r), raw_dir=str(raw_r), rejections_path=str(work / "rej_r.jsonl"),
               max_bytes=None, token=token, workers=1)
    ok = r1["fetched"] == 2 and r2["skipped_existing"] == 2 and (r1["fetched"] + r2["fetched"]) == sb["fetched"]
    results["4. fetch resume"] = (ok, f"pass1 fetched={r1['fetched']} pass2 fetched={r2['fetched']} "
                                      f"skipped_existing={r2['skipped_existing']} total={r1['fetched']+r2['fetched']}/{sb['fetched']}")

    # ---- 5. parse resume ----
    ds_pr = work / "ds_pr"
    q1 = parse(str(fet_b), dataset_dir=str(ds_pr), rejections_path=str(ds_pr / "rej.jsonl"), raw_dir=str(raw_b))
    q2 = parse(str(fet_b), dataset_dir=str(ds_pr), rejections_path=str(ds_pr / "rej.jsonl"), raw_dir=str(raw_b))
    ok = q1["calcs_parsed"] == base_calcs and q2["calcs_parsed"] == 0 and q2["skipped_existing"] == base_calcs
    results["5. parse resume"] = (ok, f"pass1 parsed={q1['calcs_parsed']} pass2 parsed={q2['calcs_parsed']} "
                                      f"skipped_existing={q2['skipped_existing']}")

    print("\n==================== SAFETY / RESUMABILITY SMOKE TEST ====================")
    all_ok = True
    for name, (ok, detail) in results.items():
        all_ok &= ok
        print(f"  [{_p(ok)}] {name:<22} {detail}")
    print(f"==================== OVERALL: {_p(all_ok)} ====================")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

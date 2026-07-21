"""Combine the EXACT census (census.py --json) with the MEASURED ratios
(sample_storage.py) into a final storage projection, as a function of the fetch
per-file size cap (the dominant storage lever).

Three storage numbers, per cap:
  * transfer      -- bytes downloaded from Zenodo (EXACT, from the census sweep)
  * staging(peak) -- extracted VASP files retained before purge-raw
                     (= transfer x extract_ratio; transient, reclaimed after parse)
  * dataset       -- the long-term extxyz.gz dataset. Two independent estimators:
      (byte-ratio)  transfer x dataset_bytes_per_downloaded_byte
      (frame-based) n_relevant x parse_yield x frames_per_record x bytes_per_frame
    They bracket the answer; agreement is a sanity check.

NB: parse_yield and frames_per_record were measured at the SAMPLE's cap, so the
frame-based estimator is most trustworthy near that cap. The byte-ratio estimator
scales cleanly with the (exact) transfer at each cap and is the primary number.

Usage:
    python project.py CENSUS_JSON RATIOS_JSON
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

GB = 1 << 30
TB = 1 << 40
MB = 1 << 20


def fmt(n: float) -> str:
    if n >= TB:
        return f"{n / TB:.2f} TB"
    if n >= GB:
        return f"{n / GB:.2f} GB"
    return f"{n / MB:.0f} MB"


def main() -> None:
    census = json.loads(Path(sys.argv[1]).read_text())
    ratios = json.loads(Path(sys.argv[2]).read_text())

    rel = census["relevant_rank_ge_3"]
    n_rel = rel["n_records"]
    # accept both the nested {yield:{}, ratios:{}} form (sample_storage.py) and a
    # flat form (older/ad-hoc ratio files).
    r = ratios.get("ratios", ratios)
    y = ratios.get("yield", ratios)

    extract_ratio = r["extract_ratio_retained_over_downloaded"] or 0
    bpf = r["bytes_per_frame_compressed"] or 0
    fpr = r["frames_per_parsed_record"] or 0
    byte_ratio = r["dataset_bytes_per_downloaded_byte"] or 0
    parse_yield = y["yield_parse"] or 0
    sample_cap = ratios.get("cap_gb")

    print(f"\n{'='*78}\nSTORAGE PROJECTION  (relevant records: {n_rel})")
    print(f"measured @ sample cap {sample_cap} GB/file:")
    print(f"  parse yield          = {parse_yield:.0%}  ({y['records_parsed_ok']}/{y['records_attempted']})")
    print(f"  frames / record      = {fpr:g}")
    print(f"  bytes / frame (gz)   = {bpf:g}")
    print(f"  extract ratio        = {extract_ratio:g}  (retained / downloaded)")
    print(f"  dataset / dl byte    = {byte_ratio:g}")
    print(f"{'='*78}")

    # frame-based dataset estimate (cap-independent inputs; valid near sample cap)
    est_frames = n_rel * parse_yield * fpr
    est_dataset_frame = est_frames * bpf
    print(f"\nFrame-based dataset estimate (near cap {sample_cap} GB):")
    print(f"  ~{est_frames:,.0f} frames  ->  {fmt(est_dataset_frame)} extxyz.gz")

    print(f"\n{'cap/file':>12}{'transfer':>12}{'staging':>12}{'dataset(byte)':>16}")
    print("-" * 52)
    sweep = rel["download_by_cap"]
    # census json keys are ints or 'inf' (JSON-stringified) -> normalise
    items = []
    for k, v in sweep.items():
        cap_gb = None if k in ("inf", "null", None) else float(k) / GB
        items.append((cap_gb, v))
    items.sort(key=lambda x: (x[0] is None, x[0] or 0))
    for cap_gb, transfer in items:
        label = "uncapped" if cap_gb is None else f"{cap_gb:g} GB"
        staging = transfer * extract_ratio
        dataset_byte = transfer * byte_ratio
        print(f"{label:>12}{fmt(transfer):>12}{fmt(staging):>12}{fmt(dataset_byte):>16}")

    print("\nNotes:")
    print("  * transfer is EXACT (summed from the manifest); dataset/staging apply")
    print("    ratios measured on a sample -> treat as order-of-magnitude, +/- a factor.")
    print("  * yield & frames/record rise with the cap (bigger trajectories admitted),")
    print("    so re-run sample_storage.py at your production cap to refine.")


if __name__ == "__main__":
    main()

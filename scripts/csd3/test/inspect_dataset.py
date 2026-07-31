#!/usr/bin/env python
"""Inspect a harvested dataset dir: frame-label fidelity + metadata + integrity.

This is the "check all results (frames + metadata)" tool. It:

* runs the real ``verify_dataset`` (metadata<->shard frame_id bijection + curation stats);
* reads shard frames back through ASE and asserts the training labels are present and
  survive the round-trip: ``REF_energy`` / ``REF_forces`` on every frame, ``REF_stress``
  where produced, and — the regression that once slipped through — that a frame never reads
  back ``electronic_converged=True`` for a calc whose metadata says convergence is unknown
  (None), i.e. no bare-key-reads-as-True leak;
* prints one full sample frame (info + arrays) and one full metadata record so you can eyeball
  provenance / calc_parameters / quality.

Exit code is non-zero if verify fails or any frame is missing an energy/forces label, so it
doubles as a CI-style gate. Run from the repo root (needs ase + the ``zenodo_harvest`` package).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ase.io import read as ase_read

from zenodo_harvest.dataset_ops import verify_dataset
from zenodo_harvest.store import existing_shard_paths


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset-dir", required=True)
    ap.add_argument("--max-frames", type=int, default=5000,
                    help="cap how many frames to read back for the label scan (default 5000)")
    args = ap.parse_args()
    ds = Path(args.dataset_dir)

    print(f"=== verify_dataset({ds}) ===")
    v = verify_dataset(ds)
    integ, stats = v["integrity"], v["stats"]
    print(f"  ok={v['ok']}  frames metadata={integ['n_frames_metadata']} "
          f"on_disk={integ['n_frames_on_disk']}  calcs={stats['n_calcs']}")
    print(f"  missing_on_disk={integ['n_missing_on_disk']} dup_on_disk={integ['n_duplicate_on_disk']} "
          f"dup_in_metadata={integ['n_duplicate_in_metadata']} orphans={integ['n_orphans_on_disk']} "
          f"truncated_shards={integ['truncated_shards']}")
    print(f"  frames_by_parser={stats['frames_by_parser']}")
    print(f"  frames_by_functional={stats['frames_by_functional']}")
    print(f"  frames_by_license={stats['frames_by_license']}")
    print(f"  calcs_by_electronic_converged={stats['calcs_by_electronic_converged']}")
    print(f"  total_with_forces={stats['total_n_frames_with_forces']} "
          f"total_with_stress={stats['total_n_frames_with_stress']} "
          f"total_dropped_no_energy={stats['total_n_frames_dropped_no_energy']}")
    print(f"  elements={list(stats['element_frame_counts'])}")

    # metadata: which calc_ids are convergence-unknown (their frames must read back None)
    meta_recs = [json.loads(l) for l in (ds / "metadata.jsonl").read_text().splitlines() if l.strip()] \
        if (ds / "metadata.jsonl").is_file() else []
    unknown_conv = {r["calc_id"] for r in meta_recs
                    if (r.get("quality") or {}).get("electronic_converged") is None}

    print("\n=== frame-label scan (read back via ASE) ===")
    shards = existing_shard_paths(ds)
    n = miss_e = miss_f = with_stress = with_efree = bad_conv = 0
    sample = None
    stop = False
    for sh in shards:
        if stop:
            break
        for a in ase_read(sh, index=":", format="extxyz"):
            n += 1
            if "REF_energy" not in a.info:
                miss_e += 1
            if "REF_forces" not in a.arrays:
                miss_f += 1
            if "REF_stress" in a.info:
                with_stress += 1
            if "E_free" in a.info:
                with_efree += 1
            # regression guard: a convergence-unknown calc must NOT read back True
            if a.info.get("calc_id") in unknown_conv and a.info.get("electronic_converged") is True:
                bad_conv += 1
            # ASE must not have re-absorbed REF_* into a calculator on read-back
            if a.calc is not None:
                bad_conv += 0  # (calc presence is checked below on the sample)
            if sample is None:
                sample = a
            if n >= args.max_frames:
                stop = True
                break

    print(f"  scanned {n} frames across {len(shards)} shard(s)")
    print(f"  missing REF_energy={miss_e}  missing REF_forces={miss_f}  "
          f"with REF_stress={with_stress}  with E_free={with_efree}")
    print(f"  frames from a convergence-UNKNOWN calc that wrongly read back True: {bad_conv} "
          f"(must be 0)")

    if sample is not None:
        print("\n=== sample frame ===")
        print(f"  formula={sample.get_chemical_formula()} natoms={len(sample)} pbc={list(sample.pbc)}")
        print(f"  calc attached on read-back: {sample.calc} (must be None — REF_* not reabsorbed)")
        print(f"  info keys: {sorted(sample.info.keys())}")
        print(f"  arrays keys: {sorted(sample.arrays.keys())}")
        print(f"  REF_energy={sample.info.get('REF_energy')}  E_free={sample.info.get('E_free')}  "
              f"scf_dE={sample.info.get('scf_dE')}  electronic_converged={sample.info.get('electronic_converged')}")
        rs = sample.info.get("REF_stress")
        print(f"  REF_stress={rs if rs is None else list(rs)}")

    if meta_recs:
        r = meta_recs[0]
        cp = r.get("calc_parameters", {}) or {}
        q = r.get("quality", {}) or {}
        prov = r.get("provenance", {}) or {}
        print("\n=== sample metadata record (structured view) ===")
        print(f"  calc_id: {r.get('calc_id')}")
        print(f"  parser:  {r.get('parser')}   n shards: {len(r.get('shards', []))}   "
              f"n frame_ids: {len(r.get('frame_ids', []))}")
        print(f"  provenance: source={prov.get('source')} record_id={prov.get('record_id')} "
              f"doi={prov.get('doi')} license={prov.get('license')} "
              f"resource_type={prov.get('resource_type')}")
        print(f"    url={prov.get('url')}")
        print(f"    file_path={prov.get('file_path')}")
        print(f"  calc_parameters: code={cp.get('code')} version={cp.get('code_version')} "
              f"run_type={cp.get('run_type')} functional={cp.get('functional')} "
              f"encut={cp.get('encut')} ediff={cp.get('ediff')} ispin={cp.get('ispin')}")
        print(f"    kpoints={cp.get('kpoints')}")
        print(f"    potcar_symbols={cp.get('potcar_symbols')}  potcar_set_hash={cp.get('potcar_set_hash')}")
        print(f"    incar keys ({len(cp.get('incar', {}))}): {sorted(cp.get('incar', {}))[:20]}...")
        print(f"  quality: {q}")
        print(f"  availability: {r.get('availability')}")

    ok = v["ok"] and miss_e == 0 and miss_f == 0 and bad_conv == 0 \
        and (sample is None or sample.calc is None)
    print(f"\n=== INSPECT {'PASS' if ok else 'FAIL'} ===")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

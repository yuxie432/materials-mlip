# Zenodo harvest — final result (Aug 2026)

The first full Zenodo harvest run on CSD3 is **complete**: every one of the 1,352
triage-kept records has been attempted, and the assembled training dataset is the
`extxyz.gz` shards + `metadata.jsonl` under `data/dataset/`.

## Headline

| quantity | value |
|---|---|
| Discover candidates (all resource types, gates on) | **10,435** |
| Triage keep-list (rank ≥ 3, post-peek) | **1,352** |
| Records **attempted** | **1,352 / 1,352 (100%)** |
| — fetched (yielded VASP) | **311** |
| — fetch-rejected (no parseable VASP) | **1,041** |
| Records **in the dataset** (≥1 stored calc) | **293** |
| Calc units parsed | **176,739 / 195,233 (90.5%)** |
| **Frames** (structures with energy±forces) | **11,870,529** |
| Shards / dataset size | 1,212 × `shard-*.extxyz.gz` / **≈40 GiB** |

## Yield vs. the pre-run estimate

Survey #4 projected **≈416** records with a parseable VASP primary (band ~255–571). The
actual **≈293** sits just below the pessimistic end. The deviation is fully accounted for
and is **not** a systemic miss (verified with `scripts/estimate/yield_by_bucket.py`):

- The high-confidence buckets landed on target — directly-exposed + peek-confirmed-primary
  records (~197) yielded ~98%.
- The shortfall is entirely in the two buckets the survey flagged as least certain:
  - **evidence-gap zips** (nested/ZIP64): survey *assumed* ~20% yield; the fetch pass
    measured **~2.3%** — the assumption was ~10× optimistic (nested content is mostly
    foreign/compressed, not VASP).
  - **non-zip archives** (tar/rar/7z): ~15% vs the survey's ~21% (within its n=28 CI).

Discovery (10,435 ≈ survey's 10,373) and triage (1,352 ≈ 2,800 relevant − ~1,464 peek-dropped
no-VASP zips) both matched the survey model exactly, so the gap is purely fetch-yield of the
speculative tail.

## Parse ceiling (the 90.5%)

The ~9.5% of fetched calc units not in the dataset were **parse-attempted and terminally
rejected**, not uncollected:

| reason | count | note |
|---|---|---|
| `outcar_parse_error` | 17,037 | dominant — OUTCAR-only **NEB / positionless OUTCARs** ASE can't extract positions from (incl. the manually-excluded `14773462`) |
| `vasprun_parse_error` | 665 | corrupt vaspruns |
| `no_frames` | 779 | calcs with no recoverable energy |
| `primary_too_large` | 88 | **recoverable** — skipped for RAM; a higher-RAM re-parse (`--max-primary-bytes`) would collect them |
| `extract_error` | 1,715 | archive members that failed to extract (incl. now-fixed encrypted-7z / corrupt-xz) |

The only cheap recovery is the ~88 `primary_too_large` (bigger-RAM re-parse). The NEB bulk
would need a NEB-aware parser (see "Known gaps").

## Dataset composition — frames ≫ diversity

The dataset is **heavily dominated by a few frame-rich AIMD/MC deposits**:

- `5720009` alone = **5,559,902 frames (47%)**; top-2 records ≈ 61%.
- ~293 records / ~177k calc units produce ~11.9M frames — trajectory-dense but with far
  fewer *independent* configurations than the frame count suggests.

**For training:** report and weight by diversity (distinct compositions, `potcar_set_hash`,
`run_type`, source `resource_type`/`license`) and expect to subsample correlated trajectory
frames — 11.9M frames from ~293 deposits is *deep but narrow*.

## Un-harvested / excluded records

All 1,352 keep-list records were attempted; the un-harvested handful are genuine, documented
limits, not lost science:

- **`18012696` — permanently excluded** (`manually_excluded`). Its archive unpacks to
  **millions of tiny files**, exceeding the CSD3 `/rds` **1M-inode** filesystem limit — it
  cannot be staged on this filesystem at all.
- **`14773462`** — manually excluded earlier (source of the 221 `FileNotFoundError`s).
- **`8005679`** — a nested Monte-Carlo tarball (`MC_rocksalt_data.tar.gz` → per-generation
  `vasp_store.tar.gz`s, ~300k extracted files); it **was** fetched and parsed (**+37,054
  frames**), just slowly.

**Known recoverable gaps** (optional future work): the ~88 `primary_too_large` calcs
(higher-RAM re-parse) and the NEB `outcar_parse_error` calcs (a NEB-aware OUTCAR parser —
their positions/forces are in the per-image OUTCARs, but ASE's `vasp-out` reader can't
reconstruct the band; verify the reported forces are true DFT forces, not tangent-projected,
before ingesting).

## Dataset format & provenance

- **Storage:** rotating `shard-NNNNN.extxyz.gz` + one `metadata.jsonl` record per calc,
  joined by `calc_id` / `frame_id`.
- **Labels (MACE keys, in `atoms.info`/`atoms.arrays`):** per-ionic-step `REF_energy`
  (E0, σ→0), `REF_forces`, `REF_stress` (ASE Voigt convention, eV/Å³), plus `E_free`
  (+`entropy_TS`) for the force/stress-consistent free energy, and the per-structure
  `total_magnetization` (net moment) + `total_charge` on every frame (`electronic.py`).
  *(Superseded 2026-08-17: the earlier per-atom `dft_charge`/`dft_magmom` on the final frame are
  replaced by the totals; run the `net_properties_recover` campaign — CSD3 script 47 — to retrofit
  the existing dataset.)*
- **Per-frame quality:** `electronic_converged` + `scf_dE` (that step's own SCF verdict),
  and calc-level `quality` (frame counts, `max_abs_free_minus_e0_per_atom`).
  *(Superseded 2026-08-18: `electronic_converged`/`scf_dE` are now filled on the **OUTCAR** path
  too — read from the OUTCAR SCF trace, free-energy basis, `scf_dE_key="free_energy"`; the original
  OUTCAR-parsed calcs left them `null`. Calc-level `ionic_converged` also reaches OUTCAR parity
  (was `null`; reimplemented from NSW/IBRION/EDIFFG). The ionic-convergence *magnitude* (last-two-
  frames ΔE) is intentionally NOT stored — not a per-frame training-label signal. The SAME script-47
  campaign retrofits all of this alongside the net moment/charge — see `zenodo_harvest/convergence.py`.)*
- **Provenance/filtering:** source DOI, `resource_type`, `license`, full INCAR/k-points/
  POTCAR, and `potcar_set_hash` (pseudopotential-set fingerprint — absolute energies are
  only comparable within an identical POTCAR set + functional + settings).

## Operational lessons (for the mentor & the NOMAD phase)

- **Lustre small-file pathology is the binding constraint**, not bytes or CPU. Many-file
  archives explode the **inode** count; extraction, `purge-raw`, cleanup walks, and `verify`
  all bottleneck (or hang in uninterruptible `D` state) on Lustre metadata ops. Several jobs
  timed out / OOM'd on this, and a degraded OST (transient `lfs quota` `[…]` warnings) made
  it worse.
- **`files_total` in the manifest counts the Zenodo record's files** (an archive = 1) — it
  says nothing about extracted contents. A "1-file" 10 GB `.tar.gz` can unpack to hundreds of
  thousands of files. Gauge giants by live extraction rate / `tar -tzf | wc -l`, not
  `files_total`.
- **`scripts/csd3/20_pipeline.sh` always runs the full `keep.jsonl` and self-resubmits
  (`RESUBMIT=1`)** — after any `scancel`, re-check `squeue` for a chain successor before
  cleanup/finish, and use `43_finish.sh` (explicit `--in`, no resubmit) to target one record.
- **Tooling added this run:** `scripts/estimate/{yield_by_bucket,attempt_count,inspect_unconfirmed_zips,parse_error_breakdown,staging_report,harvest_audit,cleanup_staging}.py`
  and `scripts/csd3/{40_cleanup,41_parse,42_verify,43_finish}.sh`. `status` gained a
  record-level `RECORDS` line.
- **Bug fixes this run:** extractor error handling now catches `lzma.LZMAError` (corrupt xz)
  and `py7zr.exceptions.PasswordRequired` (encrypted 7z) — previously these crashed the
  fetch worker and re-attempted forever; a post-extract prune drops files no calc unit
  references (KPOINTS/OSZICAR/stray) at fetch time; and `verify` now reads frame metadata by
  text-parsing (no per-frame `Atoms`), so it scales to 10M+ frames.

## Next step

Zenodo is the long tail of DFT data; the large reusable corpora live elsewhere. The natural
next source is **NOMAD** (Phase-0 already built — see `docs/NOMAD_HARVEST.md`), which offers
far more scale and diversity; deduplication against this Zenodo set is the main risk to plan
for.

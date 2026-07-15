# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

`zenodo_harvest` is a computational chemistry/materials science project (summer research project) to build
a machine learning interatomic potential (MLIP) using openly accessible DFT data. The pipeline has three stages:

1. **Harvest** — programmatically pull DFT calculation data (energy, structure, forces, etc.) from open
   databases via REST APIs. Primary targets: Zenodo (https://developers.zenodo.org/#rest-api) and
   Materials Project (`mp-api` package, https://next-gen.materialsproject.org/api). Materials Cloud and
   institutional repositories are likely future sources.
2. **Parse & store** — extract structured data from raw DFT output files and store it in a compact,
   interoperable format.
3. **Train** — train an MLIP (e.g. MACE architecture) on the assembled dataset, then use it for
   ML-accelerated MD.

This document describes the intended architecture and domain conventions agreed with the project
mentor. All five stages (discover → triage → fetch → parse → store) now exist as the
`zenodo_harvest` package and run end-to-end. See `docs/DESIGN.md` for the full data/storage design.

## Code layout & commands

- `zenodo_harvest/` — importable package, runnable without install from the repo root:
  - `client.py` — throttled, retrying Zenodo REST client. Key facts baked in: `q` searches
    metadata only (not file contents), page size caps at 25, the search window caps at 10k
    (bypassed by recursive `created`-date bisection in `iter_records`), file downloads honour
    HTTP Range despite no `Accept-Ranges` header, and **the `/api/records` search endpoint is
    30 req/min even with a token** (token helps other endpoints/quotas + restricted access).
  - `config.py` — `.env` loader (no dependency) + data-dir layout (`data/{manifests,raw,dataset}`),
    overridable via `ZENODO_HARVEST_DATA` (point at cluster scratch).
  - `models.py` — `Candidate` dataclass (JSONL-serialisable) + `classify_files` VASP file-signal
    classifier (`vasp_direct` / `archive` / `processed_atomistic` / `unlikely`).
  - `manifest.py` — JSONL read/append helpers + `RejectionLogger` (auditable dropped-record log).
  - `discover.py` — stage 0: keyword search → deduplicated candidate manifest (dedup by
    `conceptrecid`, newest version wins).
  - `triage.py` — stage 1: filter candidates; optional `--peek` reads a remote `.zip` central
    directory over HTTP Range to confirm `vasprun.xml`/`OUTCAR` without downloading the archive.
  - `fetch.py` — stage 2: download (checksum-verified, resumable, `--max-bytes` capped) and
    **selectively extract only VASP files** from archives; records availability of heavy files
    (CHGCAR/WAVECAR/DOSCAR/EIGENVAL/…) without extracting them; skips `.rar`/`.7z` (no portable
    tooling) with a logged rejection. Emits `fetched.jsonl` (one calc-unit list per record).
  - `parse.py` — stage 3: pymatgen `Vasprun` (primary) → per-ionic-step ASE frames; tags each
    frame with *its own* step's electronic convergence bool + magnitude (`scf_dE`), drops steps
    with no recoverable energy, records `run_type`, full INCAR/k-points/POTCAR, availability
    flags. OUTCAR-only calcs fall back to ASE's `vasp-out` reader (read from an isolated temp
    copy — ASE otherwise crashes on uploads with hash-annotated POTCAR species). Manifest paths
    resolve against `--raw-dir`.
  - `store.py` — stage 4: `ShardedExtxyzWriter` (rotating `shard-NNNNN.extxyz.gz`) +
    `MetadataWriter` (one JSONL record per calc), joined by `calc_id`/`frame_id`.
  - `cli.py` — `python -m zenodo_harvest.cli {discover,triage,fetch,parse} ...` (loads `.env`).
- Manifests are JSONL under `data/manifests/`; the dataset (extxyz.gz shards + `metadata.jsonl`)
  under `data/dataset/` (all gitignored). Full trial run:
  ```
  python -m zenodo_harvest.cli discover --query VASP --query OUTCAR --max-records 200 \
      --out data/manifests/candidates.jsonl
  python -m zenodo_harvest.cli triage --in data/manifests/candidates.jsonl \
      --out data/manifests/keep.jsonl --min-rank 3 --peek
  python -m zenodo_harvest.cli fetch --in data/manifests/keep.jsonl --max-bytes 500000000
  python -m zenodo_harvest.cli parse --in data/manifests/fetched.jsonl
  ```
  Full cluster harvest: add `--exhaustive` to `discover` (recursive date-partitioning past 10k).
- `ZENODO_TOKEN` lives in `.env` (gitignored, loaded by `config.load_dotenv`). Stage 0–1 depend
  only on `requests`; stages 2–4 need `pymatgen` + `ase` (`pip install -e .[parse]`).
- **Frame properties**: per-ionic-step energy (`e_0_energy`, σ→0, with pymatgen's `final_energy`
  bugfix applied to *every* step) and forces are written to extxyz under MACE's default
  **`REF_energy`/`REF_forces`** keys — placed directly in `atoms.info`/`atoms.arrays` (not via a
  `SinglePointCalculator`, whose reserved `energy`/`forces` keys ASE re-absorbs into a calculator on
  read-back, removing them from `info`/`arrays`). A step with no recoverable energy (e.g.
  GW/response) is **dropped** (energy-only steps, no forces, are kept); kept frames keep their
  original ionic-step index in `frame_id`/`ionic_step`. Each frame's `electronic_converged`/`scf_dE`
  are **that step's own** SCF verdict/magnitude (calc-level `quality` keeps the final-step verdict
  plus counts: `n_frames`, `n_frames_scf_unconverged`, `n_frames_with_forces`,
  `n_frames_dropped_no_energy`). **Stress is parsed but not a training label**: the
  raw VASP 3×3 tensor stays in the frame info as `stress_kbar` (kBar) — VASP's kBar sign/scale
  convention must be confirmed before feeding stress to training. Per-atom DFT charges/spins come
  from OUTCAR (end-of-run) and attach to the final frame only, under explicit output keys
  `dft_charge`/`dft_magmom` (not ASE's `initial_*` input fields).

## Scope and starting point

- Start with **VASP calculations only** (most common DFT code in materials science). Main output files
  to parse: `vasprun.xml` (or `vaspout.h5`) and `OUTCAR`.
- Parse with **pymatgen's `Vasprun` class** in preference to ASE's VASP parsers — mentor's guidance is
  pymatgen is usually more complete/reliable for this. Example notebooks using pymatgen:
  https://matgenb.materialsvirtuallab.org
- A single VASP run may be a geometry relaxation trajectory, not a single-point calculation — pull out
  the structure/energy/forces at *each* ionic step, not just the final one.

## Domain conventions (agreed with mentor — important, non-obvious)

- **Electronic convergence**: always check whether the SCF loop converged before the ionic step finished.
  Pymatgen exposes `Vasprun.converged_electronic` as a bool
  (see https://github.com/materialsproject/pymatgen-core/blob/v2026.5.18/src/pymatgen/io/vasp/outputs.py#L711),
  but store the actual **magnitude** of non-convergence too: the energy difference between the last and
  second-last electronic step of the final ionic step
  (`vasprun.ionic_steps[-1]["electronic_steps"]` vs. `vasprun.ionic_steps[-2]["electronic_steps"]`).
  Unconverged calculations are noisier and must be tagged, not silently included. This is now tagged
  **per frame** (each ionic step's own SCF verdict/magnitude, mirroring pymatgen's
  `len(electronic_steps) < NELM`), with the calc-level `converged_electronic` (final step) retained
  in `quality`.
- **Run-type classification**: pymatgen's `Vasprun.run_type` classifies DFT flavour and Hubbard U / vdW
  corrections — useful metadata but too coarse alone to guarantee dataset consistency (foundation-model
  papers, e.g. the NequIP foundation potentials draft, discuss why "one-size-fits-all" settings/datasets
  are problematic).
- **Storage format**: default to **extxyz(.gz)** for the training dataset — easy to read/write with ASE
  and pymatgen, and a de facto standard for MLIP training data. Revisit only if a clearly better option
  is identified.
- **Properties to store** (per structure/frame): positions, chemical symbols, energy, forces, charges,
  spin (if available). Just *record the availability* (not the full data, for storage-size reasons) of:
  charge density, spin density, electronic structure (eigenvalues), magnetization.
- **Provenance is mandatory** for every record: source database, database-specific ID, and any citation
  attached to the data. Also record the full calculation parameter set (k-points, pseudopotentials/POTCARs,
  INCAR-equivalent settings, etc.) — this is as important as the physical data itself for later filtering
  or reproducibility.
- Balance data usefulness against storage cost deliberately — storage on the HPC cluster is expected to
  be a real constraint (discuss with mentor if a dataset needs more space rather than silently dropping data).

## Environment

- Primary development/compute environment is an HPC cluster (CSD3). Long-running harvest jobs and MLIP
  training jobs are expected to run there via the batch scheduler, not interactively.
- Most work is Python + terminal based. Core libraries: `pymatgen`, `ase`, `mp-api`, and (for training)
  MACE or similar MLIP architectures.
- Toolchain (declared in `pyproject.toml` `[dev]`): **pytest** (tests), **mypy** (types), **ruff** (lint).
  From the repo root:
  ```
  python -m pytest tests/ -q         # offline unit tests (no network / no pymatgen-ase)
  python -m mypy zenodo_harvest/     # type-check (clean)
  ruff check zenodo_harvest/         # lint (install ruff first; not in base env)
  ```
  `tests/test_harvest.py` covers the pure logic (file classifier, the triage/fetch VASP-name
  matchers, the remote zip-peek parser, discover dedup) — fast and network-free; end-to-end runs
  exercise the pymatgen/ase paths.

## Credentials

- API tokens must never be committed. `.gitignore` already excludes `.env`/`.env.*`, `*.key`, `secrets.*`,
  and the pymatgen/mp-api config files `.mprc` and `.pmgrc.yaml`.
- The Materials Project API key is read from the `PMG_MAPI_KEY` environment variable (or `.pmgrc.yaml`) by
  `mp-api`/`pymatgen`. Load secrets from the environment or an untracked `.env`, never hard-coded.
- Large artifacts stay out of git: harvested DFT outputs (`vasprun.xml`, `OUTCAR`, `CHGCAR`, …), the
  `data/`/`datasets/`/`raw/`/`processed/` trees, `*.extxyz(.gz)` datasets, and MLIP checkpoints
  (`*.model`, `*.pt`, `checkpoints/`) are all gitignored — they live on the cluster / external storage.

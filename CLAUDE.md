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
mentor. Stages 0–1 (discover + triage) now exist as the `zenodo_harvest` package; stages 2–4
(fetch, parse, store) are still to be built. See `docs/DESIGN.md` for the full data/storage design.

## Code layout & commands

- `zenodo_harvest/` — importable package, runnable without install from the repo root:
  - `client.py` — throttled, retrying Zenodo REST client. Key facts baked in: `q` searches
    metadata only (not file contents), page size caps at 25, the search window caps at 10k
    (bypassed by recursive `created`-date bisection in `iter_records`), and file downloads honour
    HTTP Range despite no `Accept-Ranges` header.
  - `models.py` — `Candidate` dataclass (JSONL-serialisable) + `classify_files` VASP file-signal
    classifier (`vasp_direct` / `archive` / `processed_atomistic` / `unlikely`).
  - `discover.py` — stage 0: keyword search → deduplicated candidate manifest (dedup by
    `conceptrecid`, newest version wins).
  - `triage.py` — stage 1: filter candidates; optional `--peek` reads a remote `.zip` central
    directory over HTTP Range to confirm `vasprun.xml`/`OUTCAR` without downloading the archive.
  - `cli.py` — `python -m zenodo_harvest.cli {discover,triage} ...`.
- Manifests are JSONL under `data/manifests/` (gitignored). Example trial run:
  ```
  python -m zenodo_harvest.cli discover --query VASP --query OUTCAR --max-records 200 \
      --out data/manifests/candidates.jsonl
  python -m zenodo_harvest.cli triage --in data/manifests/candidates.jsonl \
      --out data/manifests/keep.jsonl --min-rank 3 --peek
  ```
  Full cluster harvest: add `--exhaustive` to `discover` (recursive date-partitioning past 10k).
- `ZENODO_TOKEN` env var (optional) raises the anonymous ~30 req/min rate limit; needed for a full
  harvest. Stage 0–1 depend only on `requests`; pymatgen/ase are deferred to stages 2–4.

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
  Unconverged calculations are noisier and must be tagged, not silently included.
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
- No build/lint/test tooling exists yet. When a Python package structure, dependency manager (e.g. `uv`/
  `poetry`/`conda`), and test suite are added, update this section with the actual commands. The
  `.gitignore` already anticipates the toolchain — **ruff** (lint), **mypy** (types), and **pytest**
  (tests) — so prefer those when wiring up the project unless there's reason to deviate.

## Credentials

- API tokens must never be committed. `.gitignore` already excludes `.env`/`.env.*`, `*.key`, `secrets.*`,
  and the pymatgen/mp-api config files `.mprc` and `.pmgrc.yaml`.
- The Materials Project API key is read from the `PMG_MAPI_KEY` environment variable (or `.pmgrc.yaml`) by
  `mp-api`/`pymatgen`. Load secrets from the environment or an untracked `.env`, never hard-coded.
- Large artifacts stay out of git: harvested DFT outputs (`vasprun.xml`, `OUTCAR`, `CHGCAR`, …), the
  `data/`/`datasets/`/`raw/`/`processed/` trees, `*.extxyz(.gz)` datasets, and MLIP checkpoints
  (`*.model`, `*.pt`, `checkpoints/`) are all gitignored — they live on the cluster / external storage.

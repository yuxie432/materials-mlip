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
  - `triage.py` — stage 1: filter candidates. `--peek` is **ON by default** — it reads a remote
    `.zip` central directory over HTTP Range (~tens of KB) to confirm `vasprun.xml`/`OUTCAR`
    without downloading the archive, and by default **drops** an archive record when a successful
    peek of every archive proves it holds no VASP (fail-safe: kept if any archive is unpeekable
    tar/rar/7z, too big, or the peek failed). `--no-peek` disables peeking; `--keep-unconfirmed`
    peeks-to-upgrade but never drops. Peeking is ~1000× cheaper than a wasted download.
  - `fetch.py` — stage 2: download (checksum-verified, resumable, `--max-bytes` capped;
    `--max-bytes 0` = no cap) and **selectively extract only VASP files** from archives; the
    archive is deleted right after extraction (persistent disk = extracted files, not archives);
    records availability of heavy files (CHGCAR/WAVECAR/DOSCAR/EIGENVAL/…) without extracting them.
    `.rar`/`.7z` extract when the `archives` extra (py7zr/rarfile + an `unrar`/`bsdtar` binary) is
    installed, else a logged rejection. `--max-member-bytes` caps each extracted file. Emits
    `fetched.jsonl` (one calc-unit list per record).
  - `parse.py` — stage 3: pymatgen `Vasprun` (primary) → per-ionic-step ASE frames; tags each
    frame with *its own* step's electronic convergence bool + magnitude (`scf_dE`), drops steps
    with no recoverable energy, records `run_type`, full INCAR/k-points/POTCAR, availability
    flags. OUTCAR-only calcs fall back to ASE's `vasp-out` reader (read from an isolated temp
    copy — ASE otherwise crashes on uploads with hash-annotated POTCAR species). Manifest paths
    resolve against `--raw-dir`.
  - `store.py` — stage 4: `ShardedExtxyzWriter` (rotating `shard-NNNNN.extxyz.gz`) +
    `MetadataWriter` (one JSONL record per calc), joined by `calc_id`/`frame_id`.
  - `dataset_ops.py` — array-job glue (stages over dataset dirs, not the network):
    - `split` — round-robin a manifest into `<stem>.part-NNN.jsonl` parts, one per array task.
    - `merge-datasets` — fold per-task dataset dirs into one (rename+renumber shards, never
      recompressing them; rewrite each metadata record's `shards`; refuse locked/duplicate
      sources; post-verify the merged join).
    - `verify` — assert the metadata↔shard `frame_id` bijection + report curation stats
      (frames by parser/run_type/functional/license, element coverage) — the cheap gate after
      every array job.
    - `purge-raw` — delete `<raw-dir>/<recid>/` trees whose every calc_id is already in the
      dataset (reclaim scratch); `--dry-run` reports without deleting.
  - `cli.py` — `python -m zenodo_harvest.cli {discover,triage,fetch,parse,split,merge-datasets,verify,purge-raw} ...`
    (loads `.env`; `verify`/`merge-datasets` exit non-zero on an integrity failure). Parse and
    merge take a `.parse.lock` on the dataset dir so two writers never corrupt one dir.
- Manifests are JSONL under `data/manifests/`; the dataset (extxyz.gz shards + `metadata.jsonl`)
  under `data/dataset/` (all gitignored). Full trial run:
  ```
  python -m zenodo_harvest.cli discover --query VASP --query OUTCAR --max-records 200 \
      --out data/manifests/candidates.jsonl
  python -m zenodo_harvest.cli triage --in data/manifests/candidates.jsonl \
      --out data/manifests/keep.jsonl --min-rank 3          # peek is ON by default
  python -m zenodo_harvest.cli fetch --in data/manifests/keep.jsonl --max-bytes 500000000
  python -m zenodo_harvest.cli parse --in data/manifests/fetched.jsonl
  ```
  Full cluster harvest: add `--exhaustive` to `discover` (recursive date-partitioning past 10k).
  Parallel parse on the cluster (array-job model): `split` the fetched manifest into N parts,
  run N array tasks each parsing its part into its OWN `--dataset-dir`, then `merge-datasets`
  the per-task dirs into one, `verify` the merged dataset, and `purge-raw` the parsed archives:
  ```
  python -m zenodo_harvest.cli split --in data/manifests/fetched.jsonl --parts 16 \
      --out-dir data/manifests/parts
  # array task i: parse --in .../fetched.part-0i.jsonl --dataset-dir data/dataset/task-i ...
  python -m zenodo_harvest.cli merge-datasets --into data/dataset data/dataset/task-*
  python -m zenodo_harvest.cli verify --dataset-dir data/dataset
  python -m zenodo_harvest.cli purge-raw --raw-dir data/raw --dataset-dir data/dataset
  ```
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
  `n_frames_dropped_no_energy`). **Stress is a training label** (mentor 2026-07-20): per-frame
  stress is written under MACE's **`REF_stress`** key in **ASE's convention** — a Voigt-6 vector
  `[xx,yy,zz,yz,xz,xy]` in eV/Å³ with ASE's sign. The vasprun/vaspout path converts VASP's raw
  kBar tensor (`× −0.1 × ase.units.GPa` + Voigt reorder, exactly ASE's own vasprun.xml reader);
  the OUTCAR path uses ASE's already-converted `vasp-out` stress. Per-atom DFT charges/spins come
  from OUTCAR (end-of-run) and attach to the final frame only, under explicit output keys
  `dft_charge`/`dft_magmom` (not ASE's `initial_*` input fields).
- **Energy-reference tracking** (2026-07-20): the label is E0 (σ→0) but VASP's forces/stress are
  consistent with the *free* energy F, so every frame stores F as `E_free` (vasprun also the exact
  entropy term `entropy_TS` = E−F); `quality.max_abs_free_minus_e0_per_atom` lets a train-time
  filter drop frames where |F−E0| makes E0 an unreliable label for the stored forces.
  `calc_parameters.potcar_set_hash` (a hash of the ordered POTCAR TITEL strings; works on both
  parser paths) fingerprints the pseudopotential set — a real cross-record consistency key, since
  absolute VASP energies are only comparable within an identical POTCAR set + functional + settings.

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

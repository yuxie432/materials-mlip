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
    **`.zip` uses targeted member fetch by default** (`zipstream.py`; `--no-zip-stream` to
    disable): a ZIP's tail central directory gives every member's byte offset, so the VASP
    files are pulled straight out over HTTP Range (~1 request/file, CRC-verified) **without
    downloading the whole archive** — so a huge zip whose bulk is CHGCAR/WAVECAR (incl. a
    >4 GB ZIP64 heavy file beside 32-bit VASP outputs) transfers only its vasprun/OUTCAR and
    never stages the archive at all (transfer, transient disk, and time all drop; validated
    on Zenodo — the third survey investigation's recommendation #2). It is chosen over a
    whole download only when worthwhile (heavy bytes to skip, or a huge archive) and the
    target count is within `--zip-stream-max-files` (default 128; each member ≈ 1–2 HTTP
    requests, so a many-small-member VASP dump is cheaper whole); anything not addressable
    (ZIP64/encrypted/odd-compression *target* member, enumeration failure, Range ignored, a
    corrupt member) **falls back to the whole-archive download** — never a regression.
    Targeted fetch is deliberately NOT bounded by `--max-bytes`/`--max-member-bytes` (every
    targeted member is a wanted VASP file); only the disk valve bounds it. Enumeration alone
    can prove a zip holds no VASP and skip the download entirely. tar/rar/7z/zst have no tail
    index (or are non-seekable when compressed) and keep the whole-download path.
    **Nested archives** (an archive inside an archive — a `.zip` of per-run `.tar.gz`s, a
    `.tar` of `.zip`s, …) are unpacked recursively *after download* for every archive type
    (`_recurse_nested_archives`, depth cap 8): the extractors also pull out nested-archive
    members and each sub-archive is extracted then deleted+refunded, so VASP files inside
    sub-archives are reached (CHGCAR inside a sub-archive is still recorded as availability
    only). Targeted ZIP fetch **falls back to a whole download** when the zip contains a
    nested archive (the central-directory peek can't see inside it), letting the recursion
    unpack it — random access *into* a compressed sub-archive is impossible (its index sits
    in a non-seekable DEFLATE blob).
    `.rar`/`.7z`/`.tar.zst` extract when the `archives` extra (py7zr/rarfile/zstandard + an
    `unrar`/`bsdtar` binary for rar) is installed, else a logged rejection. Emits
    `fetched.jsonl` (one calc-unit list per record). Three independent size/pacing levers:
    `--max-bytes` (per downloaded file), `--max-member-bytes` (per *extracted* file —
    decompression-bomb guard), and `--max-disk-bytes`/`--max-disk-files` (**staging-budget
    valve**: stop cleanly and resumably once the staging tree reaches a budget — the
    mechanism that lets an *uncapped* multi-TB harvest run inside a fixed quota).
    The valve (`StagingBudget`) charges **every ~1 MB chunk before it is written and every
    inode as it is created**, refunding on delete — never a declared size (archive header,
    manifest `size`, `Content-Length`), since with `--max-bytes 0` the budget is the only
    bound on what reaches disk. Inodes count **directories too** (CSD3 `/rds` is Lustre, so
    its 1M-file quota is an inode quota). `.7z` extracts in one py7zr call, so its charging
    lives in a `WriterFactory` and the refusal is recorded, not raised, because an exception
    inside py7zr's threads would be lost. On a breach: roll the record back whole → stop
    resumably → the pacing loop reclaims → re-fetch. A refusal is classified by the
    **record's own footprint** (so "too big for the whole budget" is deterministic under
    `--workers N`), a space refusal never yields a *terminal* reason (no record is lost to
    pacing), leftovers of an unrecorded record are deleted+refunded at once (`purge-raw`
    could never reclaim them), and a parallel pass that stages nothing retries serially so
    the loop cannot deadlock. `--workers N` downloads N records concurrently (records are
    independent; each stages into its own `raw_dir/<recid>/`, one shared tally, thread-safe).
    Interrupted transfers **resume over HTTP Range** (a cluster job's wallclock
    can expire mid-pull of a >100 GB archive; resume only happens when Zenodo supplied a
    checksum, so stale bytes are always caught). Stats report `peak_staged_bytes`/
    `peak_staged_files`, so "did we stay inside the limits?" is answerable from the summary.
  - `pipeline.py` — stages 2–4 **overlapped**: `run_pipeline` fetches batch *i+1* in the
    foreground while `parse`+`purge-raw` for batch *i* runs in the background, so the
    network is not idle during parsing (parses serialise on the dataset dir's lock). It is
    I/O-agnostic (plain callables) so it unit-tests without the network or pymatgen. A
    `fetch_fn` returning `False` means "batch only partly fetched" (the disk valve tripped):
    the orchestrator drains the background parse+purge to reclaim staging and **resumes the
    same batch** — a partly-fetched batch is never carried on and silently dropped. If it
    still cannot complete with nothing left to drain, the run stops with a reported error
    rather than under-fetching every later batch.
  - `parse.py` — stage 3: pymatgen `Vasprun` (primary) → per-ionic-step ASE frames; tags each
    frame with *its own* step's electronic convergence bool + magnitude (`scf_dE`), drops steps
    with no recoverable energy, records `run_type`, full INCAR/k-points/POTCAR, availability
    flags. OUTCAR-only calcs fall back to ASE's `vasp-out` reader (read from an isolated temp
    copy — ASE otherwise crashes on uploads with hash-annotated POTCAR species). Manifest paths
    resolve against `--raw-dir`. `--max-primary-bytes` (0 = off) refuses to *attempt* a
    primary output above a size and logs a `primary_too_large` rejection instead — pymatgen
    holds a whole ionic trajectory in memory, so under a batch scheduler one huge
    `vasprun.xml` is not a catchable `MemoryError` but a cgroup SIGKILL of the entire job
    (taking the fetch progress with it). If a smaller sibling primary exists in the same
    unit (a huge `vasprun.xml` beside a modest `OUTCAR`) it is used instead of dropping the calc.
  - `store.py` — stage 4: `ShardedExtxyzWriter` (rotating `shard-NNNNN.extxyz.gz`) +
    `MetadataWriter` (one JSONL record per calc), joined by `calc_id`/`frame_id`.
  - `status.py` — read-only progress snapshot: line-counts the append-only manifests +
    walks the raw/dataset trees to report per-stage counts, fetch/parse **progress %**
    (fetched vs keep-list; parsed calcs vs fetched calc-units), staging **bytes+inodes vs
    quota**, and a **rejection-reason histogram**. No network, no lock — safe to run (or
    `watch`) *while* a fetch/pipeline job is writing the same files.
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
  - `cli.py` — `python -m zenodo_harvest.cli {discover,triage,fetch,parse,pipeline,split,merge-datasets,verify,purge-raw,status} ...`
    (loads `.env`; `verify`/`merge-datasets`/`pipeline` exit non-zero on an integrity failure;
    `status` is read-only and always exits 0, `--json` for machine output).
    Parse and merge take a `.parse.lock` on the dataset dir so two writers never corrupt one
    dir. `pipeline` always runs `verify` and prints a JSON summary — including any
    `fetch_error`/`process_errors` — even when a stage failed hard, so a long unattended run
    never loses its report.
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
  Full cluster harvest (batch templates in `scripts/csd3/`): add `--exhaustive` to `discover`,
  then run stages 2–4 as one overlapped, disk-paced command:
  ```
  python -m zenodo_harvest.cli pipeline --in data/manifests/keep.jsonl \
      --parts 40 --workers 4 --max-bytes 0 --max-member-bytes 30000000000 \
      --max-disk-bytes 800000000000 --max-disk-files 800000 --max-primary-bytes 2000000000
  ```
  `--max-disk-bytes` bounds the **whole** raw staging dir — it already covers both
  concurrently-staged batches, so size it ~0.8 × quota (leaving headroom for in-flight
  archives, which are briefly downloaded *and* extracted, plus the dataset dir on the same
  quota). Discovery is capped server-side at 30 req/min — single-stream by design,
  do NOT parallelise it; only `fetch` benefits from `--workers`.
  **`--max-primary-bytes` is a RAM guard, not a disk lever**: pymatgen's peak RSS is
  ~10 × the vasprun.xml/OUTCAR size (measured on CSD3 icelake-himem: a 534 MB file peaks at
  ~5.6 GB), and an over-budget parse is a cgroup SIGKILL — so run this on `icelake-himem` and
  size the cap to the job's RAM (`~0.85 × cpus-per-task × 6.76 GB ÷ 10`, less room for the
  concurrent fetch; e.g. `--cpus-per-task=4` → ~26 GiB → ~2.0 GB cap). It only skips (logs
  `primary_too_large`, keeps the staged file) — the calc can be re-parsed later on a
  bigger-RAM job. Calibrate with `scripts/csd3/csd3_parse_memory.py`.
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
- **CSD3 constraints the harvest is designed around** (verified against
  [docs.hpc.cam.ac.uk](https://docs.hpc.cam.ac.uk/), 2026-07-27):
  - Wallclock **36 h** (SL1/SL2) / **12 h** (SL3) per job; up to 7 days only via the
    `-long` QoS, which must be requested from support. The harvest can outlast one job,
    so *every* stage is resumable and `scripts/csd3/20_pipeline.sh` can self-resubmit.
  - `/rds/user/<crsid>/hpc-work` (Lustre, **not backed up**) is **1 TB and 1 million
    files** — point `ZENODO_HARVEST_DATA` there, size `--max-disk-bytes` off the 1 TB, and
    pace it with `--max-disk-files` (measured: the inode limit binds before the byte one).
    `/home` is 50 GB (code only).
  - Login nodes are capped at ~4 CPUs per user — fine for a smoke test, not the real fetch.
  - **Open question: the docs do not say whether compute nodes have outbound internet
    access**, which the fetch stage requires. Verify with `scripts/csd3/00_check_network.sh`
    before submitting; if they are firewalled, use a proxy (`https_proxy`, honoured by
    `requests`) or run only `fetch` on a login node.
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

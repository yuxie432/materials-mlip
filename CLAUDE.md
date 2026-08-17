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
    records availability of heavy files (CHGCAR/WAVECAR/DOSCAR/EIGENVAL/…) without extracting
    them. **Availability is scoped PER CALC**: each heavy file is attributed to the calc whose
    primary sits in the SAME directory (VASP writes CHGCAR/DOSCAR beside the run's vasprun/OUTCAR),
    not OR'd across every calc in the record (the old over-count). `fetched.jsonl` carries a
    `calc_availability` list aligned index-for-index with `calc_units` (parse reads it; a
    record-level `availability` union is kept for older manifests/report tooling).
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
    flags. Every frame also carries the calc's **net magnetic moment** `total_magnetization` +
    **net charge** `total_charge` (broadcast; `electronic.py`), mirrored into a metadata
    `electronic` block. `parse_vasprun` enables `parse_eigen` (for the occupancy method) ONLY
    for a collinear spin-polarised vasprun **without** an OUTCAR — a cheap streaming ISPIN
    pre-scan (`_scan_vasprun_spin_flags`) gates it, so the ISPIN=1 majority and every
    OUTCAR-accompanied calc parse unchanged. **Per-atom** `dft_charge`/`dft_magmom` are NO LONGER
    stored (removed for cross-parser consistency — OUTCAR-only never had them); only the totals. Availability is taken PER CALC from fetch's `calc_availability` (falling back to the
    record-level union for pre-fix manifests) and **refined by a cheap embedded probe of the
    primary**: DOS/eigenvalues/projected written straight into a vasprun.xml (a streaming
    `<dos`/`<eigenvalues`/`<projected` tag scan, never parsing the arrays) or vaspout.h5 (its
    `/results` group names) are OR'd on, so a calc with no standalone DOSCAR/EIGENVAL/PROCAR file
    is no longer under-counted. OUTCAR-only calcs fall back to ASE's `vasp-out` reader (read from an isolated temp
    copy — ASE otherwise crashes on uploads with hash-annotated POTCAR species). Manifest paths
    resolve against `--raw-dir`. `--max-primary-bytes` (0 = off) refuses to *attempt* a
    primary output above a size and logs a `primary_too_large` rejection instead — pymatgen
    holds a whole ionic trajectory in memory, so under a batch scheduler one huge
    `vasprun.xml` is not a catchable `MemoryError` but a cgroup SIGKILL of the entire job
    (taking the fetch progress with it). If a smaller sibling primary exists in the same
    unit (a huge `vasprun.xml` beside a modest `OUTCAR`) it is used instead of dropping the calc.
    **`--parse-timeout`** (seconds; CLI default 1200 = 20 min, 0 = off) parses each calc unit
    in a **forkserver child process hard-killed on timeout**, so one *non-terminating*
    pymatgen/ASE parse (e.g. a truncated `vasprun.xml` whose OUTCAR fallback loops for hours)
    is logged `parse_timeout` and skipped rather than silently **freezing the whole overlapped
    pipeline** until wallclock — a Python-level timeout can't help (the hang is in a C
    extension that ignores signals, and a stuck thread can't be killed). It also isolates a
    parse OOM into the child instead of a cgroup-kill of the job. A `parse_timeout` is
    non-terminal (absent from metadata), so a later/longer run re-attempts it, like
    `primary_too_large`. Each calc is logged (`INFO parsing <calc_id>`) *before* it starts, so
    a slow/hung parse is identifiable live via `tail -f` and by the last log line on a kill.
    On resume, calcs already rejected with a **deterministic** parse failure
    (`vasprun`/`vaspout`/`outcar_parse_error`, `no_frames`) are **skipped** rather than
    re-parsed (`_rejected_calc_ids`) — re-running the parser on a proven-unparseable file
    wastes minutes per resume on a big bad record and duplicated rejection lines;
    `--retry-rejected` re-attempts them (use after a pymatgen/ase upgrade). `parse_timeout`/
    `primary_too_large`/`parse_worker_died` stay retryable (absent from the skip set).
    **OUTCAR-only calcs now recover the FULL calc parameters from the OUTCAR header** (not
    just the trajectory): `_parse_outcar_ase` reads the header via `outcar_params` so
    `run_type`/`functional`/INCAR/effective `parameters`/k-points/POTCAR are populated to
    parity with the vasprun path (closing the 25.6%-null-functional gap). The vasprun path
    also stores VASP's resolved `parameters` block now (the metadata-gaps "better fix").
  - `outcar_params.py` — parse an OUTCAR **header** into a vasprun-schema `calc_parameters`:
    the user `INCAR:` echo (via pymatgen's canonical `Incar` parser, stdlib fallback) + the
    resolved-parameter blocks (`parameters`) + POTCAR titels + k-points. `run_type` is
    classified **faithfully to pymatgen 2026.5.4's `Vasprun.run_type`** (same GGA/METAGGA
    tables + hybrid/+U/+vdW rules), validated by a live same-calc cross-check (mine(OUTCAR) ==
    pymatgen(vasprun), incl. HSE06/GGA+U). Reads `IVDW`/`METAGGA` from the user INCAR (never
    the resolved default `IVDW=0`, which would add a spurious `+vdW-no-correction`). Pure
    stdlib except the optional pymatgen `Incar`; unit-tested offline.
  - `param_resolver.py` — pure-logic resolver filling effective VASP params from stored
    metadata: user INCAR > resolved `parameters` (authoritative, `source="parameters"`) > VASP
    documented defaults / run_type-derived hybrid mixing; genuinely-unknown values stay None.
    Exposed as `enrich-metadata` (writes `calc_parameters.resolved`; ADDS a field only, so the
    frame_id bijection holds). Backward compatible: records with no `parameters` block behave
    exactly as before.
  - `outcar_recover.py` — targeted recovery of the OUTCAR metadata gap **without rebuilding
    shards** (the physical data is already correct; only calc-level `calc_parameters` was
    impoverished). `build_outcar_keeplist` emits a slim keep-list of just the ~230 records
    holding an OUTCAR calc; re-fetch them with the ordinary `fetch` (targeted-zip pulls only
    the OUTCAR out of a `.zip`); `refresh_outcar_metadata` re-parses each OUTCAR header and
    overwrites ONLY those records' `calc_parameters` in metadata.jsonl —
    `calc_id`/`frame_ids`/`shards` left byte-identical (verify still passes, no shard opened),
    atomic rewrite under `.parse.lock` + one-time `metadata.jsonl.bak.pre_outcar_refresh`,
    idempotent, `--only-missing` for incremental resume. `reclassify_outcar_run_types`
    (CLI `reclassify-outcar`) re-derives `run_type`/`functional` for every header-parsed OUTCAR
    calc **from its already-stored `incar`/`parameters`** — metadata-only, NO re-fetch and NO
    shard access — so a `classify_run_type` bugfix that lands *after* the 955-GiB recovery ran
    can be applied cheaply (atomic under `.parse.lock` + `metadata.jsonl.bak.pre_reclassify`;
    e.g. it fixed the 57 calcs a boolean-`METAGGA` had mislabeled run_type `"TRUE"` → `GGA`).
  - `vasprun_params.py` + `vasprun_recover.py` — the vasprun analog of the OUTCAR recovery,
    for the missing resolved **`parameters`** block on the ~83k `pymatgen.Vasprun` calcs (they
    were parsed before `_calc_parameters` stored it, so only the user `incar` is present →
    `ISIF`/`ADDGRID`/etc. rely on the resolver's guess). `vasprun_params.parse_vasprun_parameters`
    reads **only the `<parameters>` element** of a vasprun.xml (iterparse, stops before the
    `<calculation>` trajectory → cheap regardless of file size) via pymatgen's own
    `_parse_params`, then `resolved_parameters` renames vasprun's **`ENMAX`→`ENCUT`** (the
    `<parameters>` block has no `ENCUT`) and restricts to `EFFECTIVE_TAGS` — the SAME function
    `parse._calc_parameters` now uses, so a live parse and a recovered record emit a
    byte-identical block. `build_vasprun_keeplist` + `refresh_vasprun_metadata` (CLI
    `vasprun-keeplist`/`refresh-vasprun`) re-fetch the vasprun records and write ONLY the
    `parameters` block into their `calc_parameters` (dropping the stale `resolved`;
    `run_type`/`functional`/`incar`/`calc_id`/`frame_ids`/`shards` untouched → verify passes),
    atomic under `.parse.lock` + `metadata.jsonl.bak.pre_vasprun_refresh`, `--only-missing` for
    resume. CSD3 campaign: `scripts/csd3/45_vasprun_recover.sh` (mirrors 44). Run
    `enrich-metadata` after to regenerate `resolved` from the authoritative parameters.
  - `availability_recover.py` — the **per-calc `availability`** analog of the OUTCAR/vasprun
    recovery, for the dataset built before availability was scoped per-calc + probed. Applies the
    corrected flags to the already-built dataset **without rebuilding shards**:
    `build_availability_keeplist` selects EVERY record holding a dataset calc (availability is
    universal, not a parser subset); re-fetch with the ordinary `fetch` (targeted-zip pulls the
    vasprun + central-directory listing out of a `.zip`); `refresh_availability_metadata` recomputes
    each calc's availability = per-calc filename flags (from the re-fetch's `calc_availability`) ∪
    the embedded vasprun/vaspout probe (`parse._merge_embedded_availability`) + spin_density/
    magnetization re-derived from the record's own `spin_polarized`/`site_magmoms_present`
    (`outcar_recover._recompute_spin_availability`), and overwrites ONLY that record's
    `availability` (`calc_id`/`frame_ids`/`shards`/`calc_parameters` byte-identical → verify passes).
    CLI `availability-keeplist`/`refresh-availability`; atomic under `.parse.lock` +
    `metadata.jsonl.bak.pre_availability_refresh`. **Resume-safe with NO metadata marker**: the
    refresh only touches calcs whose primary is STILL STAGED, so a purged (already-done) batch is
    skipped — presence of the source file IS the resume state. CSD3 campaign:
    `scripts/csd3/46_availability_recover.sh` (mirrors 45; batched fetch→refresh→purge, dedicated
    `raw_availability` dir, explicit pre-job metadata backup, verify-only finalize — no enrich).
  - `electronic.py` — per-calc **net magnetic moment** (μ_B, = N_up−N_down = 2·S) + **net charge**
    (e, = Z_neutral−NELECT; + = electron-deficient), the values stored on every frame
    (`total_magnetization`/`total_charge`) and mirrored into the metadata `electronic` block. Net
    moment: `Outcar.total_mag`-style last-`magnetization`-line scan when an OUTCAR is present (norm
    for non-collinear); else doped's eigenvalue-**occupancy** method (`N_up−N_down`, needs
    `parse_eigen`) for a collinear spin vasprun/vaspout without an OUTCAR; 0 for ISPIN=1; **null**
    for non-collinear without an OUTCAR (needs the heavy projected magnetization). Net charge:
    `NELECT` from parameters + Z_neutral from the vasprun `<atominfo>` valence col (pymatgen
    discards it) / OUTCAR header `POMASS;ZVAL`×`ions per type` / the bundled `potcar-summary-stats`
    titel→ZVAL table (vaspout fallback). Unknowns stay `None` (never a fabricated 0). Also
    `is_magnetic` (spin-polarised OR non-collinear) — the single source of the
    `availability["magnetization"]` flag now that per-atom magmoms are gone. Pure-ish + offline-tested.
  - `net_properties_recover.py` — retrofit net moment/charge onto an **already-built** dataset;
    UNLIKE the other recoveries this **REWRITES SHARDS** (the values live on the frames). A shard
    mixes calcs, so **two phases**: `compute_net_properties` (Phase 1, disk-paced) re-fetches every
    record and computes each calc's net moment/charge (`parse.electronic_block_for_unit`, the same
    code a fresh parse uses) into a `net_properties.jsonl` map (resumable by map membership, no shard
    touched); `apply_net_properties` (Phase 2, local, ONCE after Phase 1 completes) drives the map in
    via a **text-level** shard edit — append the two totals to each frame's comment line + strip the
    `dft_magmom`/`dft_charge` columns, leaving every untouched value **byte-identical** (no ASE
    re-serialise → no float drift) — then an atomic metadata rewrite (add `electronic`, drop
    `site_*_present`). Idempotent + resumable (`.net_properties_applied` marker); `verify` still
    passes (frame_ids/shards/calc_id untouched). CLI `net-properties-keeplist`/`compute-net-properties`/
    `apply-net-properties`; CSD3 campaign `scripts/csd3/47_net_properties_recover.sh` (Phase-1 batched
    loop, then Phase-2 apply+verify once). Structured so a later per-frame field (e.g. an OUTCAR
    ionic-convergence ΔE) is another Phase-1 field + the same Phase-2 edit.
  - `store.py` — stage 4: `ShardedExtxyzWriter` (rotating `shard-NNNNN.extxyz.gz`) +
    `MetadataWriter` (one JSONL record per calc), joined by `calc_id`/`frame_id`.
  - `status.py` — read-only progress snapshot: line-counts the append-only manifests +
    walks the raw/dataset trees to report per-stage counts, fetch/parse **progress %**
    (fetched vs keep-list; parsed calcs vs fetched calc-units), staging **bytes+inodes vs
    quota**, and a **rejection-reason histogram**. No network, no lock — safe to run (or
    `watch`) *while* a fetch/pipeline job is writing the same files.
  - `dataset_ops.py` — array-job glue (stages over dataset dirs, not the network):
    - `split` — split a manifest into `<stem>.part-NNN.jsonl` parts, one per array task.
      `--weight-by records` (default) round-robins (balances record count); `--weight-by
      calcs` LPT-bin-packs by each record's `n_calc_units` so per-task *parse cost* is
      balanced (fixes the array-parse imbalance when records vary a lot in calc-unit count;
      it cannot split one record across parts, and degrades to count-balancing on a
      keep-list, which has no calc counts yet).
    - `merge-datasets` — fold per-task dataset dirs into one (rename+renumber shards, never
      recompressing them; rewrite each metadata record's `shards`; refuse locked/duplicate
      sources; post-verify the merged join).
    - `verify` — assert the metadata↔shard `frame_id` bijection + report curation stats
      (frames by parser/run_type/functional/license, element coverage) — the cheap gate after
      every array job.
    - `purge-raw` — delete `<raw-dir>/<recid>/` trees whose every calc_id is already in the
      dataset (reclaim scratch); `--dry-run` reports without deleting.
  - `cli.py` — `python -m zenodo_harvest.cli {discover,triage,fetch,parse,pipeline,split,merge-datasets,verify,enrich-metadata,outcar-keeplist,refresh-outcar,reclassify-outcar,vasprun-keeplist,refresh-vasprun,availability-keeplist,refresh-availability,net-properties-keeplist,compute-net-properties,apply-net-properties,purge-raw,status} ...`
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
  concurrent fetch; e.g. `--cpus-per-task=4` → ~26 GiB → ~2.0 GB cap). The cap is compared
  against the **uncompressed** size for gzip primaries (`vasprun.xml.gz`/`OUTCAR.gz`, read
  from the gzip ISIZE trailer), since RAM tracks the decompressed trajectory, not the bytes
  on disk. It only skips (logs `primary_too_large`, keeps the staged file) — the calc can
  be re-parsed later on a bigger-RAM job. Calibrate with `scripts/csd3/csd3_parse_memory.py`.
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
  **OUTCAR metadata recovery** (fill run_type/functional/INCAR for the OUTCAR-parsed calcs,
  metadata-only — shards untouched; CSD3 template `scripts/csd3/44_outcar_recover.sh` runs
  this as a self-resubmitting, disk-paced campaign):
  ```
  python -m zenodo_harvest.cli outcar-keeplist --dataset-dir data/dataset \
      --keep data/manifests/keep.jsonl --out data/manifests/outcar_keep.jsonl
  python -m zenodo_harvest.cli fetch --in data/manifests/outcar_keep.jsonl \
      --out data/manifests/outcar_fetched.jsonl --max-bytes 0    # targeted-zip pulls just OUTCARs
  python -m zenodo_harvest.cli refresh-outcar --dataset-dir data/dataset \
      --fetched data/manifests/outcar_fetched.jsonl              # overwrites calc_parameters only
  python -m zenodo_harvest.cli enrich-metadata --dataset-dir data/dataset   # refresh resolved cache
  python -m zenodo_harvest.cli verify --dataset-dir data/dataset            # bijection still holds
  ```
  **Availability recovery** (recompute per-calc `availability` for a dataset built before the
  per-calc-scoping + embedded-probe fixes, metadata-only — shards untouched; CSD3 template
  `scripts/csd3/46_availability_recover.sh` runs this as a self-resubmitting, disk-paced,
  batched campaign that also takes an explicit pre-job metadata backup):
  ```
  python -m zenodo_harvest.cli availability-keeplist --dataset-dir data/dataset \
      --keep data/manifests/keep.jsonl --out data/manifests/availability_keep.jsonl  # EVERY record
  python -m zenodo_harvest.cli fetch --in data/manifests/availability_keep.jsonl \
      --out data/manifests/availability_fetched.jsonl --raw-dir data/raw_availability --max-bytes 0
  python -m zenodo_harvest.cli refresh-availability --dataset-dir data/dataset \
      --fetched data/manifests/availability_fetched.jsonl --raw-dir data/raw_availability  # overwrites `availability` only
  python -m zenodo_harvest.cli verify --dataset-dir data/dataset            # bijection still holds
  ```
  (no `enrich-metadata` step — availability does not feed the resolver. The refresh must run
  BEFORE `purge-raw` each batch: the embedded DOS/eigen probe reads the STAGED vasprun.)
  **Net moment/charge recovery** (add `total_magnetization`/`total_charge` to every frame + the
  metadata `electronic` block, and strip per-atom `dft_*`, on a dataset built before this — this
  one **REWRITES SHARDS**; CSD3 template `scripts/csd3/47_net_properties_recover.sh` runs the whole
  two-phase campaign). Phase 1 is a batched fetch→compute→purge loop building the map; Phase 2
  applies it ONCE (verify still passes; frame_ids/shards/calc_id untouched):
  ```
  python -m zenodo_harvest.cli net-properties-keeplist --dataset-dir data/dataset \
      --keep data/manifests/keep.jsonl --out data/manifests/net_properties_keep.jsonl  # EVERY record
  python -m zenodo_harvest.cli fetch --in data/manifests/net_properties_keep.jsonl \
      --out data/manifests/net_properties_fetched.jsonl --raw-dir data/raw_net_properties --max-bytes 0
  python -m zenodo_harvest.cli compute-net-properties --fetched data/manifests/net_properties_fetched.jsonl \
      --raw-dir data/raw_net_properties --out data/manifests/net_properties.jsonl --dataset-dir data/dataset
  python -m zenodo_harvest.cli apply-net-properties --dataset-dir data/dataset \
      --net-properties data/manifests/net_properties.jsonl   # REWRITES SHARDS (adds totals, strips dft_*)
  python -m zenodo_harvest.cli verify --dataset-dir data/dataset            # bijection still holds
  ```
  (`compute-net-properties` re-uses the SAME `electronic.py` code a fresh parse uses, so a
  recovered value is identical to a freshly-parsed one. `apply` is idempotent + resumable via a
  `.net_properties_applied` marker; run Phase 2 only after Phase 1 is complete — a shard mixes
  calcs, so a partial map would leave some frames' totals unset.)
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
  the OUTCAR path uses ASE's already-converted `vasp-out` stress. **Net magnetic moment +
  net charge** (`electronic.py`): every frame carries the calc's `total_magnetization` (μ_B,
  = N_up−N_down = 2·S) and `total_charge` (e, = Z_neutral−NELECT), broadcast (net charge is
  frame-invariant; net moment is the calc's converged value) and mirrored into the metadata
  `electronic` block. Net moment ← OUTCAR magnetization line when present, else the eigenvalue-
  occupancy method (doped's `N_up−N_down`) for a collinear spin vasprun/vaspout without an
  OUTCAR (0 for ISPIN=1; null for non-collinear-without-OUTCAR). **Per-atom** DFT charges/spins
  are NOT stored (only the totals) — removed for cross-parser consistency (OUTCAR-only never
  had them); retrofit the existing dataset with `net_properties_recover.py` (CSD3 script 47).
- **Energy-reference tracking** (2026-07-20): the label is E0 (σ→0) but VASP's forces/stress are
  consistent with the *free* energy F, so every frame stores F as `E_free` (vasprun also the exact
  entropy term `entropy_TS` = E−F); `quality.max_abs_free_minus_e0_per_atom` lets a train-time
  filter drop frames where |F−E0| makes E0 an unreliable label for the stored forces.
  `calc_parameters.potcar_set_hash` (a hash of the ordered POTCAR TITEL strings; works on both
  parser paths) fingerprints the pseudopotential set — a real cross-record consistency key, since
  absolute VASP energies are only comparable within an identical POTCAR set + functional + settings.

## Second data source: NOMAD (`nomad_harvest/`)

A second source adapter harvests VASP data from **NOMAD** (https://nomad-lab.eu) into the
*same* `data/dataset/` schema, so both sources feed one MLIP training set. Full design +
live-verified API facts: `docs/NOMAD_HARVEST.md`. Scope (mentor): **direct uploads only**
(~7.1M VASP-DFT entries; AFLOW/OQMD/MP excluded — MP is harvested via `mp-api`), **raw
`vasprun.xml` re-parsed by the existing `parse.py`** (not NOMAD's normalized archive, whose
σ→0 energy label and stress sign are unreliable), **vasprun-only** (no wholesale OUTCAR — its
tail is ~170 TB).

- `nomad_harvest/` imports the shared `zenodo_harvest` **stages 3-5 (parse/store/verify/merge)
  unmodified**; only stages 0-2 are NOMAD-specific:
  - `client.py` — throttled, retrying NOMAD v1 REST client (anonymous reads; **keyset
    pagination only** — offset caps at 10k; self-throttle + exponential backoff on
    502/503/504). Discover/per-entry-fallback use `entries/*`. **`upload_raw_get`** does a
    paced, 429-retrying **Range GET of `GET /uploads/{id}/raw`** (the pre-packed upload zip) —
    that endpoint's limit is **1 in-flight connection per IP, a new one every ~5 s** (a separate,
    stricter bucket than `entries/*`), so it is serialised. No bulk `entries/raw/query` methods.
  - `upload_zip.py` — **targeted extraction from an upload's PRE-PACKED zip over HTTP Range.**
    NOMAD stores each published upload as one `raw-public.plain.zip`; `read_central_directory`
    reads the zip tail (suffix Range) and parses its **ZIP64-aware** central directory (the big
    uploads are >4 GB), and `fetch_members` **multi-range-pulls the wanted members by byte
    offset** (~250/request, ~8 KB Range-header cap), CRC-verified, mapping each by file offset
    (robust to the server coalescing adjacent ranges). Members are STORED → exact bytes, zero
    server assembly. The NOMAD analog of `zenodo_harvest/zipstream.py`, extended for ZIP64.
  - `harvest.py` — stage 0 discover (keyset-scan → license-gate + Zenodo-`references`-dedup →
    slimmed keep-list) and stage 2 fetch. **`fetch_candidates` groups the keep-list by
    `upload_id`** (`split_by_upload` keeps each upload whole in one pipeline part → its central
    directory is read once), then per upload reads the CD and multi-range-fetches each entry's
    `mainfile` member (vasprun, or OUTCAR for OUTCAR-mainfile entries; `--want-outcar` also grabs
    a sibling OUTCAR). Disk/inode-paced by the shared `StagingBudget` (reserving each entry's
    EXACT footprint from the CD), manifest resume, `stopped_disk_budget` for the pipeline. It is
    **serial** (the endpoint's 1-conn/5s limit); an upload/member the pre-packed path can't
    deliver **falls back to the per-entry `entries/{id}/raw` path** (a separate throttle bucket)
    → no coverage loss. **Availability is derived from the upload's central directory** (the zip's
    own per-calc file list) OR'd with NOMAD's parsed `available_properties` (`dos_electronic[_new]`
    →`dos`, `band_structure_electronic`→`eigenvalues`) + the parse-time embedded-vasprun probe —
    so the old fragile `rawdir/query` step is gone. `available_properties` is kept by
    `slim_candidate`; the unreliable `trajectory` property is not mapped.
  - `cli.py` — `python -m nomad_harvest.cli {discover,fetch,pipeline,status,smoke}`. `pipeline`
    splits the keep-list **by upload** (`split_by_upload`) and drives the **shared** `run_pipeline`
    (fetch batch *i+1* ∥ parse+purge batch *i*), disk-paced, into its own `data/dataset/nomad`
    dir; `merge-datasets` folds it in. No `--workers`/`--batch-size` (the fetch is serial).
  - `smoke.py` — live end-to-end Phase-0 validation in an isolated temp dir.
- **The shared parser namespaces by source.** `parse._calc_id`/`_frame` derive the source from
  `provenance.source` (`_source_of`, default `"zenodo"`), so NOMAD frames are tagged
  `source="nomad"` and calc_ids are `nomad:<entry_id>:…` — byte-identical for Zenodo, no
  cross-source id collision at `verify`/`merge-datasets`. No CLI flag; the provenance field
  drives it.
- CSD3 batch templates: `scripts/csd3/nomad/{10_discover,20_pipeline}.sh` (single-stream discover,
  then the overlapped disk-paced pipeline, self-resubmitting) + `csd3_nomad_prepacked_probe.py`
  (confirm the pre-packed fetch MB/s + throttle from a compute node). Full harvest of 7.1M is
  **targeted Range-extraction from ~3,792 upload zips** (~30k requests at ~15–30 MB/s) →
  **~1.5–3 days**, one self-resubmitting campaign. It is **serial** (the `/uploads/{id}/raw`
  1-conn/5s limit — no `--workers`, and CSD3's shared NAT can't parallelise it). Slicing with
  `--max-entries` is OPTIONAL. A **token does NOT help** (per-IP limit; the endpoint is anonymous);
  the one accelerator is a rate-limit **exemption** from `support@nomad-lab.eu` (lifting the per-IP
  concurrency cap → hours), optional not required.
- Offline tests: `tests/test_nomad.py` (network-free — query builder, keyset paging, backoff,
  dedup, staging-name logic, ZIP64 central-directory parse + multi-range extraction + CRC against
  an in-memory pre-packed zip, the disk-valve/resume/fallback of `fetch_candidates`, and
  `split_by_upload`). Live path: `python -m nomad_harvest.cli smoke -n 12`.

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
- **Properties to store** (per structure/frame): positions, chemical symbols, energy, forces, and the
  per-structure **net charge** (`total_charge`) + **net magnetic moment / total spin**
  (`total_magnetization`) — the TOTALS, not per-atom charges/spins (which are not stored; see
  `electronic.py`). Just *record the availability* (not the full data, for storage-size reasons) of:
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
  - **Compute nodes have outbound internet — VERIFIED 2026-07-31** (`00_check_network.sh`
    PASSed on both `icelake` and `icelake-himem`), so the fetch stage runs in batch as
    designed; no proxy / login-node fallback is needed. (Re-run the probe if the account or
    site network policy changes; if a future node type is ever firewalled, set `https_proxy`
    — honoured by `requests` — or run only `fetch` on a login node.)
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

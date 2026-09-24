## Third data source: Materials Cloud (`materials_cloud_harvest/`)

A third source adapter harvests VASP data from the **Materials Cloud Archive**
(https://archive.materialscloud.org — EPFL/MARVEL's InvenioRDM repository) into the *same*
`extxyz.gz` + `metadata.jsonl` schema as Zenodo and NOMAD. Full design + live-verified API facts +
the full census: `docs/MATERIALS_CLOUD_HARVEST.md`; CSD3 runbook: `scripts/csd3/materials_cloud/`.
Scope (user decisions 2026-09-23, revised 2026-09-24 after the CSD3 census): **full census** of
all ~1.2k records (no keyword recall limit); VASP-mentioning records kept fail-safe, any other record
kept on **positive peek evidence** or — `--unresolved all` — when an archive cannot be settled by a
peek (tars, unreadable/nested zips: the residual blind spot, ~1.1 TB, fetched too); **both AiiDA
formats extracted** (legacy zip/tar; sqlite_zip mapped through its `db.sqlite3`); the Bosoni ACWF
record limited to its two `*_results_vasp.aiida`; licence gate **admits NC/NC-SA**, drops ND /
no-licence / `mcloud-ne-1.0` / `asl`; overlap with Zenodo/NOMAD **flagged, never dropped** (the
two flagged records were inspected: not record-level duplicates — `docs/MATERIALS_CLOUD_HARVEST.md` §6).

- Stages 0-1 + CLI are MC-specific; **stage 2 is the SHARED `zenodo_harvest.fetch`** (driven with an
  anonymous MC session) and stages 3-5 the shared parse/store/verify/merge, unmodified:
  - `client.py` — throttled, retrying InvenioRDM client (anonymous; 500 req/60 s; 429 honours
    `Retry-After`/`X-RateLimit-Reset`). `iter_records` pages the WHOLE archive (`sort=oldest`,
    100/page, latest versions only, 10k-window guard). `content_url` percent-encodes the key (~13% of
    keys have spaces/unicode). **Downloads 302 → presigned CSCS S3 (~60 s expiry)**; `requests`
    keeps `Range` across it and mints a fresh URL per request, so resume/peeks just work.
    `new_session()` is deliberately anonymous (no `Authorization`).
  - `records.py` — `record_to_candidate`: MC record → a Zenodo-keep-list-compatible dict (`recid`,
    `files[]={key,size,ext,checksum,download}`, `vasp_*` classification) + a ready-made
    `provenance` (`source="materials_cloud"`, version/concept DOI, `mcid`, licence, MC citations,
    related identifiers). `files.entries` is a DICT keyed by filename. `classify_mc_files` ranks
    `.aiida` as an archive. `licence_verdict(policy)` (nc-ok/strict/none). DOI / `mcid` helpers.
  - `remote_zip.py` — **ZIP64-aware** remote central-directory reader (reuses NOMAD's tested ZIP64
    parser `nomad_harvest.upload_zip._parse_central_directory`). Follows the ZIP64 locator
    WHENEVER present (CPython's zipfile writes ZIP64 end records past 2 GiB with real 32-bit EOCD
    values — the sentinel-only test failed 25 CSD3 peeks) and accepts a 16-bit-truncated entry
    count (walks the directory bytes). `aiida_format` (= the shared
    `zenodo_harvest.aiida_archive.archive_format`), `zip_evidence` (fetch's own name rules; a
    nested `.aiida` counts as a sub-archive), `sqlite_evidence` (a sqlite_zip archive: pull its
    `db.sqlite3` over Range to `$TMPDIR`, list the VASP nodes with the SHARED
    `aiida_archive.vasp_nodes`), `fetch_member` (one CRC-checked member over Range).
  - `discover.py` — stage 0: enumerate → candidates → access + licence gates (drops to
    `mc_rejections.jsonl`) → **overlap FLAGS** (`zenodo_linked_in_dataset` via related-identifier
    DOIs in the Zenodo dataset, `zenodo_title_similar`, `nomad_calcs_citing` via a substring-
    prefiltered scan of NOMAD's metadata.jsonl, counted once per calc) → `mc_candidates.jsonl`.
    Every MC→Zenodo link is `IsSupplementTo` (e.g. Kavanagh's MC record = the raw data behind his
    Zenodo deposit), so dropping is opt-in (`--drop-linked`).
  - `triage.py` — stage 1: Range-peek every `.zip`/`.aiida` (`--peek-workers` in parallel, request
    starts paced across threads; sqlite_zip databases up to `--max-db-bytes`; cached in
    `<out>.peeks.jsonl`, so a re-run is resumable) → `_decide`: mention records dropped only if
    every archive peeks empty (`peek_proved_no_vasp`); others kept with a peeked VASP primary
    (`vasp_evidence`) or, per `--unresolved {all,dft,none}` (default all), when an archive is
    unresolvable (`unresolved_fetch`). File pruning: proven-empty zips/AiiDA archives dropped;
    legacy `.aiida` with VASP → `archive_kind="zip"`; sqlite_zip / unresolvable `.aiida` →
    `archive_kind="aiida"` (the shared AiiDA extractor); a single compressed DATA file
    (`*.json.gz`, `*.xyz.bz2`) is never fetched as a "misnamed tarball".
    `DEFAULT_FILE_ALLOWLIST` (Bosoni). **Evidence uses fetch's OWN name rules** (`_PARSE_RE` +
    `_unit_role`: `OUTCAR1`, `vasprun_1.xml` count) — a stricter rule would prune archives fetch
    parses. **Fetch units**: one keep-list entry per archive, `recid=<record_id>~<tag>` with the tag
    derived from the archive KEY (`unit_id`; stable across re-triage — a positional id could land
    on another unit's terminal rejection), plus `~loose` for directly-exposed outputs; calc_ids stay
    IDENTICAL (they use `provenance.record_id` + the path under `extracted/`) while the disk valve
    paces per archive. Peek cache is versioned (`EVIDENCE_RULES_VERSION`) and never caches
    transient/cap-dependent verdicts. Writes `mc_keep.jsonl` + `mc_keep.report.json`.
  - `fetching.py` — `fetch_with_retries`: re-runs the shared fetch while any unit is still *pending*
    (neither fetched nor terminally rejected = a transient failure), up to `--transient-retries`
    (default 4) extra passes, each resuming the kept `.part` over Range. The shared fetch gives up
    at the first mid-transfer drop and leaves it to the NEXT run — fine for Zenodo's many re-runs,
    but a one-job MC harvest that exits 0 never resubmits (seen live: `ChunkedEncodingError`).
    Units that only need a bigger staging budget (`record_exceeds_disk_budget`) are not "pending"
    (retrying under the same budget cannot help) — they are reported as
    `units_exceeding_disk_budget`; the pipeline exits non-zero only for genuinely pending units.
  - `cli.py` — `python -m materials_cloud_harvest.cli {discover,triage,fetch,pipeline,status,smoke}`.
    `mc_paths()`: `$MC_HARVEST_DATA`, else a `materials_cloud` sibling of an absolute Zenodo root,
    else `data/materials_cloud`. `pipeline` = `split_manifest` + the shared `run_pipeline` (fetch
    i+1 ∥ parse+purge i) + `verify`; fetch rejections → `mc_fetch_rejections.jsonl`, parse
    rejections → `<dataset>/rejections.jsonl`.
  - `smoke.py` — live end-to-end check in an isolated dir (default record `ydn09-ngs56`, a 16.5 MB
    zip of HSE06 OUTCARs); asserts no `Authorization` header is ever sent.
- **Shared-code changes** (backward-compatible; Zenodo/NOMAD behaviour unchanged except the two
  recall fixes noted):
  `zenodo_harvest/fetch.py` — `_record_provenance` passes a keep-list record's own `provenance`
  through; a per-file `archive_kind` overrides the filename sniff; `fetch(session_factory=…)` builds
  every session; and the Zenodo token session now attaches `Bearer $ZENODO_TOKEN` ONLY to
  `zenodo.org` hosts (`_ZenodoOnlyBearer`), so no keep-list can make it leak to another host.
  `zenodo_harvest/status.py` — fetch units join to their record via `record_id` /
  `provenance.record_id` for parse progress (identity for Zenodo/NOMAD).
  2026-09-24: `zenodo_harvest/aiida_archive.py` (pure-stdlib AiiDA archive helpers) +
  `fetch._extract_aiida` (`archive_kind="aiida"`: legacy zip/tar or sqlite_zip → VASP files at the
  node's legacy path; an `.aiida` INSIDE an archive is now recursed — recall fix);
  `zipstream._parse_central_directory` + `triage.peek_zip_filenames` walk the directory BYTES
  (a truncated count could make targeted fetch "prove" an archive VASP-free — recall fix);
  `parse(parse_mem_budget=…, parse_rss_ratio=…)` — FIFO RAM admission for parallel parse.
- calc_ids `materials_cloud:<record_id>:<archive-subdir>/<path>`; AiiDA-sourced ones embed the
  node UUID path (`…/nodes/fb/16/c207-…/path/vasprun.xml`) → full AiiDA traceability.
- Offline tests: `tests/test_materials_cloud.py` (89; fake Range sessions serving in-memory zips —
  incl. forced-ZIP64 ones, a 65,537-member truncated-count zip, a legacy-AiiDA-layout export and a
  genuine sqlite_zip archive (real `db.sqlite3`) built from ASE's bundled VASP fixtures — through the
  real triage → shared fetch → pymatgen parse → verify; plus a mid-download drop recovered in-run
  over Range, and the overlap script) + `tests/test_parse_mem_budget.py` (the parse RAM admission).
  Live: `cli smoke`.
- CSD3 sizing (`15_bench.sh`): `csd3_mc_probe.py --speed` (API-hop + small-Range latency, S3
  throughput over 1/2/4/8 streams → `recommended_fetch_workers`, rate-limit headers), `--aiida` (the
  sqlite_zip census: each new-format export's `db.sqlite3` → aiida-vasp CalcJobs / `vasprun`
  entries — decides whether sqlite_zip extraction is worth building), and `csd3_mc_bench.py` (the
  REAL fetch + per-calc parse path on a stratified ~12 GB sample → MB/s incl. extraction, s/calc
  serial vs N workers, net RSS ratio + s/GB of the largest primaries, fetch-vs-parse projection).
- Stage bounds (measured on CSD3 2026-09-24): triage peeks request-latency-bound (0.1 s reads;
  `--peek-workers 4`); fetch S3-bandwidth-bound (52 MB/s/stream, 96 MB/s over 8 → `--workers 8`,
  ~1.2 TB ≈ 3.5 h); small calcs parse-throughput-bound (0.19 s each, ×3.8 at 4 workers →
  `--parse-workers 6`); big primaries RAM-bound → `--parse-mem-budget` (cpus × 6760 MiB − 20 GiB)
  with `--max-primary-bytes` ≈ budget/12 (~10 GB on 20 cores); `30_bigparse.sh` only for anything
  above. No API token needed (all public; bytes come from presigned S3).

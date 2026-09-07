# NOMAD harvest — final result (Sep 2026)

The full NOMAD harvest is **complete**. Companion to `HARVEST_RESULT.md` (Zenodo) and the
design note `NOMAD_HARVEST.md`. Scope (mentor): **direct uploads only**, **raw `vasprun.xml`
re-parsed by the shared `zenodo_harvest.parse`**, vasprun-preferred (OUTCAR only where it is the
entry mainfile), deduplicated against the Zenodo dataset.

## Headline

| | |
|---|---|
| **Calcs in dataset** | **7,073,592** |
| **Frames (ionic steps)** | **52,459,065** |
| Frames with forces | 52,459,065 (100%) |
| Frames with stress | 51,997,725 (99.1%) |
| Shards / size | 5,328 `shard-*.extxyz.gz` / ~59 GiB |
| `verify` | **OK** — metadata↔shard `frame_id` bijection exact (0 missing / 0 dup / 0 orphan) |
| License | 100% CC BY 4.0 |
| Dataset dir | `$NOMAD_HARVEST_DATA/dataset` (standalone; not yet merged with Zenodo) |

Parser split: **pymatgen.Vasprun 49,153,673 frames**, **ase.OUTCAR 3,305,392 frames**.
Functional coverage (frames): PBE 34.9M, PBEsol 10.1M, GGA 6.5M, SCAN 0.5M, HSE06 76k, ML 0.24M,
plus the vdW/+U/meta-GGA long tail. Electronic convergence: 7,041,518 calcs converged /
32,073 unconverged (542,135 individual frames SCF-unconverged, tagged per-frame). Full
periodic-table element coverage (O 10.5M, H 6.8M, S 4.2M, … down to Fr/Ra/Cm at ≤1.6k frames).

## The discover → dataset funnel

| Stage | Count |
|---|---|
| VASP-DFT direct uploads discovered | ~7.11M |
| Keep-list (after license gate + Zenodo dedup) | 7,091,051 |
| Fetched | 7,059,154 + 18,615 recovered (see below) |
| **Parsed into dataset** | **7,073,592 calcs** = **99.75% of the keep-list** |

Fetch is targeted Range-extraction from each upload's pre-packed zip (`/uploads/{id}/raw`),
serial (1-conn/IP throttle), disk/inode-paced by the shared `StagingBudget`, run as a
self-resubmitting `pipeline` over 9 attempts. See `NOMAD_HARVEST.md` §3/§7 for the mechanism.

## Post-harvest recoveries (2026-09-07)

After the main run, a gap investigation (logs + rejection manifests + code) drove three targeted
recoveries, **+20,012 calcs / +107,656 frames**, all appended in the same schema on the same
pymatgen 2026.5.4 (disjoint calc_ids, `verify` still exact). Scripts: `scripts/csd3/nomad/recover_*`.

1. **Dedup false-positives — +~18,615 calcs (the big one).** The discover dedup dropped any entry
   whose references contained a `10.5281/zenodo.*` DOI (`harvest.zenodo_overlap`, broad
   prefix branch). All 25,454 drops (20,766 distinct) traced to just **two** Zenodo DOIs: one
   truly held (`zenodo.7852083`, 2,151 entries — correctly deduped) and one **never harvested**
   (`zenodo.18598420` — a BSD *code* release with **no VASP data**, 18,615 entries **merely
   citing** it). Those 18,615 unique VASP calcs were re-queried by entry_id, re-gated with a
   **narrowed** dedup (drop only DOIs actually in the Zenodo set), and fetched+parsed.
   *Note: the broad-prefix rule in `harvest.py` was left as-is (the recovery inlines the narrowed
   rule); tighten it before any future NOMAD discover.*
2. **pymatgen numeric-`ALGO` bug — +~1,380 calcs.** pymatgen 2026.5.4 crashes in
   `Vasprun.__init__` (`converged_electronic` → `self.incar.get("ALGO","").lower()`) when an
   upload's INCAR has a **numeric** `ALGO` (e.g. `ALGO = 68`). Fixed with a surgical monkeypatch
   that coerces a non-str `ALGO` to str **via the `Incar.data` UserDict store** (a plain
   `incar["ALGO"]=...` is silently re-coerced back to int by `proc_val`), re-parsed **in-process**
   (so the patch applies — a forkserver child would load clean pymatgen). Files were valid; only
   pymatgen mishandled them.
3. **`primary_too_large` — +23 calcs / 46 frames (low value).** The 24 deferred vaspruns >1.6 GB
   were re-parsed uncapped on a himem node. Yield was only **~2 frames/calc**: these are big not
   because they are long AIMD but because of **heavy per-step data** (LORBIT projected characters,
   dense eigenvalues, DOS) and/or large systems — few ionic steps. "Big file ≠ many frames." One
   8.51 GB vasprun (`dhOiawS2QOf…`) was deferred; check its `<calculation>` count before spending a
   higher-RAM job — almost certainly the same low yield.

## Remaining gaps — evaluated, and why they are NOT worth chasing

The unrecovered ~0.5% is dominated by **uploader-side and NOMAD-server-side** problems, not by the
harvest scripts (every staged file is CRC-verified against the zip central directory, so a parse
failure means the file is bad *as stored on NOMAD*).

| Bucket | Count | Cause | Recoverable? |
|---|---|---|---|
| Dead uploads | ~31,050 entries | `/uploads/{id}/raw` **and** `/entries/{id}/rawdir` both HTTP 500 — broken on NOMAD's server (all 11 re-probed 500 on 2026-09-07) | No (only NOMAD support) |
| vasprun truncation | ~3,111 | truncated/incomplete `vasprun.xml` (`IndexError`) — uploader crashed/killed runs | No |
| `outcar_parse_error` | 872 | truncated OUTCARs + ~350 ASE `vasp-out` brittleness (`ion_types`/`NBANDS`) | Mostly no; ~350 maybe via pymatgen `Outcar` (low value) |
| `no_vasp_primary` | 530 | NOMAD tagged VASP-DFT but no fetchable vasprun/OUTCAR in the raw files | No (no data) |
| `no_frames` | 185 | parsed OK but no energy at any ionic step | No (no data) |
| `primary_too_large` (8.5 GB) | 1 | deferred; likely few frames | Optional (check first) |
| Correctly deduped | ~2,151 | `zenodo.7852083` — genuinely held from the Zenodo harvest | N/A (not a loss) |
| `fetch_error` | 519 | transient NOMAD 500s during fallback — **mostly self-healed** on later resumes; residual is inside the dead uploads | N/A |

**Verdict: the harvest is done.** The only remaining action with any value is the optional 8.5 GB
check; everything else is genuinely unrecoverable (truncated/absent data, or a NOMAD server fault).

## Data format & provenance

Identical schema to the Zenodo dataset (shared `parse`/`store`): per-ionic-step frames in
extxyz.gz under MACE keys `REF_energy` (σ→0), `REF_forces`, `REF_stress` (ASE Voigt convention),
each frame carrying `total_magnetization`/`total_charge`, per-frame `scf_dE`/`electronic_converged`,
and the full `calc_parameters` (INCAR / k-points / POTCAR titels / `potcar_set_hash`). Provenance
is `source="nomad"`, `record_id=<entry_id>`, DOI/URL/license/references/authors; calc_ids are
`nomad:<entry_id>:…`, so a future `merge-datasets` with Zenodo cannot collide. Availability
(CHGCAR/DOS/eigenvalues/…) is recorded, not the heavy files themselves.

## Combining with Zenodo (future)

The NOMAD dataset is standalone under `$NOMAD_HARVEST_DATA/dataset`. To feed one training set,
`zenodo_harvest.cli merge-datasets` folds it into the combined dir — **but `merge_datasets`
materialises the destination metadata**, so folding NOMAD (7M calcs) needs the streaming fix or a
large-RAM node. Not required until training wants a single corpus.

## Post-harvest cleanup (see the campaign notes / session log for the exact commands)

- **Delete** `$NOMAD_HARVEST_DATA/raw/` — staging; everything recoverable is in the dataset (decide
  the 8.5 GB monster first, since its source lives here). Reclaims the bulk of the /rds quota.
- **Delete** `manifests/nomad_keep.pipeline_parts/` — the per-part `*.jsonl` are regenerable from
  `nomad_keep.jsonl` and the `*.fetched.jsonl` (~10+ GB) are superseded by `metadata.jsonl` for
  parsed calcs; `.done*` markers are run state. (Archive `*.fetched.jsonl` compressed only if a
  per-entry fetch audit is wanted.)
- **Delete** the small recovery manifests (`nomad_{recover_keep,bigvasprun_fetched,int_algo_fetched}.jsonl`)
  — regenerable, tiny.
- **Keep** (audit trail, all small): `nomad_keep.jsonl` (scope of record), `nomad_rejections.jsonl`,
  `nomad_fetch_rejections.jsonl`, `nomad_dead_uploads.json`, the dataset itself, and the key logs
  (final pipeline completion + the three recovery jobs).

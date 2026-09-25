# Materials Cloud harvest design (VASP → MLIP dataset)

Companion to `DESIGN.md` (Zenodo) and `NOMAD_HARVEST.md`. This note scopes the **third source
adapter**, `materials_cloud_harvest/`, which pulls VASP DFT data from the **Materials Cloud
Archive** (https://archive.materialscloud.org — EPFL/MARVEL's InvenioRDM repository) into the same
`extxyz.gz` + `metadata.jsonl` schema as the Zenodo and NOMAD datasets.

Everything under "measured" was obtained live from the MC API on **2026-09-23** (a full census of
every record). It supersedes the raw plan in `EXTERNAL_DATA_SOURCES.md` §5, several of whose
numbers turned out wrong (see §9).

> **Status (2026-09-25): HARVEST COMPLETE — 102 records / 75,751 calcs / 2,545,669 frames, verify
> exact; every rejection bucket evaluated, none worth a recovery job.** Outcome, funnel, yield by
> decision and the data-quality notes for training: **`MATERIALS_CLOUD_HARVEST_RESULT.md`**. This
> file keeps the design: the first CSD3 census (§11) exposed a ZIP64 reader bug and two policy
> questions, decided as **fetch every archive no peek can settle** (it found half of all calcs) and
> **extract sqlite_zip AiiDA archives**; parse runs under a RAM budget (§5).

---

## 0. TL;DR

| Question | Answer |
|---|---|
| Worth harvesting? | **Yes, as a cheap, provenance-rich completeness sweep** — individual-group deposits, CC-BY-dominant, ~0 deposit-level overlap with Zenodo/NOMAD, novel chemistries. Small next to NOMAD (52M frames) / Zenodo (12M). |
| How big is MC? (measured) | **1,241 records / 2.55 TB** in total (latest versions; 1,535 incl. superseded). All public, no embargoes. |
| How much VASP? (measured) | **44 records mention VASP** (43 in their own text + the Bosoni ACWF record via an author affiliation); ~84 GB + Bosoni 75.5 GB (of which only its **3.1 GB of VASP AiiDA exports** are harvested). **No bare `vasprun.xml`/`OUTCAR` anywhere** — all VASP sits in archives. |
| Discovery | **Full census** — enumerate every record (13 paged requests), no keyword recall limit. Keywords only decide how fail-safe triage is. |
| Triage | Range-peek every `.zip` and `.aiida` central directory (ZIP64-aware; a sqlite_zip AiiDA archive's `db.sqlite3` is pulled and queried). VASP-mentioning records kept fail-safe; others kept on **positive evidence** (a peek found `vasprun`/`OUTCAR`/`vaspout`) **or when an archive cannot be settled by a peek** (tars, unreadable/nested zips — `unresolved_fetch`, decided 2026-09-24). |
| AiiDA exports (894 GB, 35% of MC bytes) | **Both formats extracted**: legacy exports keep real member names (Bosoni's VASP exports); sqlite_zip archives are mapped through their database by the shared `_extract_aiida` (aiida-vasp calcs found in the AMaRaNTA 2D-magnets record). A database proving an export VASP-free prunes it. |
| Code | `materials_cloud_harvest/` (stages 0-1, retrying fetch glue, CLI) + backward-compatible additions to the shared code: fetch hooks, an AiiDA extractor (`zenodo_harvest/aiida_archive.py`), the zip-count fix, a parse RAM budget. calc_ids `materials_cloud:<record_id>:<path>`. |
| **Result (2026-09-25)** | **102 records / 75,751 calcs / 2.55M frames** (91% OUTCAR-parsed; 65.6% of frames from one AIMD record), verify exact, in a 15 min discover job + a 9 h 41 min pipeline job; the blind fetch yielded 38 records = 49.6% of calcs — `MATERIALS_CLOUD_HARVEST_RESULT.md` |
| Cost | One ~30 min discover/triage job + one ≤12 h pipeline job: **~1.2k fetch units / ~1.2 TB** transfer (1.1 TB of it blind-fetched archives, ~3.5 h at the measured 96 MB/s). |

---

## 1. Why Materials Cloud needs its own adapter

| | Zenodo | NOMAD | **Materials Cloud** |
|---|---|---|---|
| unit | arbitrary deposit (archives) | one parsed calculation | arbitrary deposit (archives + **AiiDA exports**) |
| size | 7.3M records | 19M entries | **1,241 records** |
| discovery | metadata-text search, 30 req/min, 10k window | indexed `program_name=VASP` | **enumerate everything** (500 req/min) |
| file access | `/files/{key}/content`, Range, no `Accept-Ranges` | pre-packed upload zip, 1 conn/5 s | **302 → presigned CSCS S3** (~60 s expiry), Range |
| dominant code | VASP-rich long tail | VASP 14.7M | **Quantum ESPRESSO** (MARVEL/EPFL); VASP a minority |
| unique feature | — | normalised archive | **AiiDA provenance exports** (`.aiida`, two formats) |

So discovery and triage are MC-specific; the fetch mechanics are Zenodo-like (per-file download,
md5, Range, archives), which is why the shared fetch is reused with three small hooks (§5).

---

## 2. API facts (live-verified 2026-09-23)

* **Search** `GET /api/records?q=&size=&page=&sort=` returns metadata **and the file listing
  inline**: `files.entries` is a **dict keyed by filename** → `{key, size, checksum:"md5:…", ext,
  mimetype}` — **no download link**. `size` up to 1000 accepted (slow to transfer; we page at 100).
  Default = latest version of each record only (1,241; `allversions=true` → 1,535); the search
  matches author affiliations too (`q=VASP` catches the Bosoni record only via "VASP Software GmbH").
* **Record ids** are Invenio pids (`1etna-5se79`); concept = `parent.id`; version DOI
  `10.24435/materialscloud:2020.0006/v1` (**contains `:`**), concept DOI
  `10.24435/materialscloud:wm-6j`, short id `pids.mcid` = `2020.0006/v1` (old archive URLs
  `…/record/2020.0006/v1`). Citations in `custom_fields.mc_references`; related works in
  `metadata.related_identifiers` (all 60 MC→Zenodo links are `IsSupplementTo`).
* **Licence** = the first `metadata.rights[]` entry with an `id` (some records add an id-less
  "License addendum"). Ids seen: cc-by-4.0 1,153 · cc-by-nc-4.0 18 · mit 15 · cc-by-sa-4.0 15 ·
  **mcloud-ne-1.0 14** ("Materials Cloud non-exclusive license to distribute" — arXiv-style,
  distribution right to MC only) · cc-by-nc-sa-4.0 10 · gpl/lgpl 11 · cc0-1.0 3 · apache-2.0 1 ·
  **asl 1** (Academic Software Licence). The shared NC/ND token gate would wrongly pass the two
  non-open MC ids — the adapter's own gate handles them.
* **Downloads** `GET /api/records/{id}/files/{key}/content` → **302 to a presigned CSCS S3 URL
  (`rgw.cscs.ch`, `Expires` ≈ +60 s)** → 200/206. Range works (incl. a suffix range longer than the
  file — no Zenodo small-file underflow); the bytes match the listed md5. `requests` follows the
  302 and **keeps the `Range` header**, and each request mints a fresh presigned URL, so resumable
  downloads and peeks never meet an expired signature. ~13% of keys contain spaces/unicode → the
  key is fully percent-encoded.
* **Rate limit** `X-RateLimit-Limit: 500` per 60 s (`Retry-After: 60`); the MC hop costs ~1.5–2 s
  of latency per request (the S3 hop is fast). No token is needed — and none must be sent (§5).

---

## 3. The census (measured, all 1,241 records)

* **Formats by bytes**: `.aiida` 894 GB · `.tar.gz` 739 GB · `.zip` 532 GB · `.tgz` 78 GB ·
  `.tar.bz2` 77 GB · `.tar` 60 GB · `.tar.xz` 57 GB · `.h5` 24 GB · `.7z` 17 GB · … By count: 1,206
  zips, 616 tar.gz, 253 `.aiida`, 170 tgz.
* **VASP-mentioning records: 44** (title/description/subjects), **all CC-BY / CC-BY-SA**. The ten
  smallest were peeked: most hold only structures/inputs (POSCARs, INCARs) — e.g. `jvwfw-x9075`,
  `58hhx-t4q48`, `nc4x8-yme87` → triage drops them before download; `ydn09-ngs56` holds real HSE06
  OUTCARs. Big ones: Kristoffersen CO-coupling AIMD (`yspxn-jxt78`, 43.6 GB, 10 tar.gz), impurity
  adsorption HT (`mm9k2-h9a91`, 11 GB), OCV multi-code workflow (`emryk-yfa58`, 10 GB), organic ML
  potential (`bf2kn-k1195`, 5.6 GB), Kavanagh Sn₂SbS₂I₃ (`hmdsb-k3h43`, 3.3 GB).
* **Records not mentioning VASP but holding archives: 1,069 (2.15 TB)** — 1,170 zips (516 GB,
  peekable), 1,051 tar-family archives (971 GB, unpeekable), 639 GB of `.aiida`. Indirect VASP clues
  in their text are almost nil (3 records say "projector-augmented", all QE Hubbard-methodology
  papers); code mentions are QE 57, CP2K 15, LAMMPS 13, … — MC is QE/AiiDA-dominated, and 984 of
  them name no code at all. This is the (small) metadata blind spot the evidence policy covers.
* **AiiDA exports** (253 files in 119 records): both on-disk formats occur. **Legacy** (aiida-core
  1.x, `export_version` 0.x): a zip with `metadata.json`, `data.json` and every node's repository
  under `nodes/<uuid[:2]>/<uuid[2:4]>/<uuid[4:]>/path/<real filename>` — verified on Bosoni's
  `…_results_vasp.aiida` (359,272 members; each aiida-vasp *retrieved* folder holds `vasprun.xml`,
  `OUTCAR`, `CONTCAR`, `DOSCAR`, `EIGENVAL`; the CalcJob folder holds `INCAR`/`POSCAR`/`KPOINTS`).
  **sqlite_zip** (aiida-core ≥ 2.0): `repo/<sha256>` content-addressed blobs + `db.sqlite3`; file
  names exist only in the db. Only Bosoni's two exports are explicitly VASP by name/metadata.
* **Bosoni et al. ACWF verification** (`yf0rj-w3r97`, 75.5 GB): 20 exports for 10 codes; its VASP
  exports (unaries 1.27 GB + oxides 1.85 GB) ≈ 7k PBE equation-of-state single points on 960 cubic
  prototypes across Z = 1–96 (≈4k `vasprun.xml` extrapolated from a 7,150-member sample of the
  unaries export).

---

## 4. Decisions (user, 2026-09-23 and 2026-09-24)

| # | Decision | Chosen | Alternatives considered |
|---|---|---|---|
| 1 | Discovery scope | **Full census + evidence gate**: VASP-mentioning records fail-safe; others only on positive peek evidence | VASP-mention only (the raw plan); + tar head-peek (~70 GB, low expected yield); download everything (~2.1 TB, mostly QE) |
| 2 | AiiDA exports | **Extract legacy-format exports now**; sqlite_zip detected + probed (db.sqlite3) on CSD3, extraction built only if aiida-vasp calcs are found | skip all `.aiida` (the raw plan — would lose Bosoni's VASP); full sqlite_zip support now |
| 3 | Bosoni ACWF record | **Include only its two `*_results_vasp.aiida`** (file allowlist) | exclude by ID (the raw plan) |
| 4 | Licence | **Admit NC / NC-SA** (matches the Zenodo dataset after its NC expansion); drop ND, no-licence, `mcloud-ne-1.0`, `asl` | strict Zenodo default (drop NC); no gate |
| 5 | Residual blind spot (2026-09-24, after the CSD3 census) | **Fetch every unresolved archive** (`--unresolved all`): records that never mention VASP and show none in any peek, but hold archives a peek cannot settle — 530 records, ~1.1 TB | DFT-worded records only (~323 GB); README scan only; keep evidence-only (the 2026-09-23 policy) |
| 6 | sqlite_zip AiiDA (2026-09-24) | **Build the extractor now** (the probe found aiida-vasp calcs) | defer to a follow-up job; skip |

Also decided by evidence (not a user question): **overlap is flagged, never auto-dropped** (§6).
Decision 5 rests on the census's measured hit rate: where a non-mentioning record's zips could be
peeked, **7.8%** held VASP outputs (12% with DFT words in the metadata, 1.8% naming another code),
so ~35-40 of the 477 tar-only records are expected to hide VASP — e.g. `ervm4-pn188` (ACE-GCN),
whose README says "OUTCARs" but whose data are tar.bz2 in a record with no VASP keyword. CSD3 pulls
S3 at 52 MB/s on one stream / 96 MB/s on 8, and fetch keeps only VASP files, so the ~1 TB costs
~3 h of transfer and nothing persistent.

---

## 5. The pipeline

| Stage | Where | What it does |
|---|---|---|
| 0 discover | `materials_cloud_harvest.discover` | full enumeration → `records.record_to_candidate` (Zenodo-keep-list-compatible dict + ready-made `provenance`) → access + licence gates (drops logged to `mc_rejections.jsonl`) → overlap flags vs the Zenodo/NOMAD datasets → `mc_candidates.jsonl` |
| 1 triage | `materials_cloud_harvest.triage` | ZIP64-aware Range peek of every `.zip`/`.aiida` (`remote_zip`, reusing NOMAD's tested ZIP64 parser; results cached in `mc_keep.jsonl.peeks.jsonl`) → evidence policy → file pruning → **fetch units** → `mc_keep.jsonl` + `mc_keep.report.json` |
| 2 fetch | **shared** `zenodo_harvest.fetch.fetch` + `fetching.fetch_with_retries` | downloads over the 302→S3 redirect with an anonymous MC session (md5-verified, Range-resumable, targeted zip members where worthwhile, nested-archive recursion, disk/inode valve); units that fail **transiently** get up to 4 more passes in the same run, each resuming the kept `.part` |
| 3-4 parse/store | **shared** `zenodo_harvest.parse` / `store` | unchanged; `provenance.source="materials_cloud"` → calc_ids `materials_cloud:<record_id>:<archive-subdir>/<path>`, frames tagged `source="materials_cloud"` |
| pipeline | **shared** `zenodo_harvest.pipeline.run_pipeline` | fetch part i+1 ∥ parse+purge part i, disk-paced, then `verify` |
| status / verify / merge | **shared** `status_report` / `verify_dataset` / `merge-datasets` | MC manifest names passed in; the MC dataset lives in its own tree until merged |

**The evidence policy** (`triage._decide`). Per file: `.zip`/`.aiida` → peeked (a sqlite_zip
archive through its database); tar-family / 7z / rar → unpeekable. A VASP-mentioning record is
dropped only if every archive was peeked OK and none holds a VASP primary or a nested archive
(`peek_proved_no_vasp`) — exactly Zenodo's fail-safe rule. Any other record is kept on a peeked
`vasprun`/`OUTCAR`/`vaspout` member (`vasp_evidence`) or, under `--unresolved all` (decision 5),
when some archive could not be settled by a peek (`unresolved_fetch`: a tar, an unreadable or
too-big zip/AiiDA archive, a zip holding sub-archives); `--unresolved dft|none` narrow that.
Proven-empty zips and AiiDA archives are removed from the fetch list; a legacy AiiDA export with
VASP members is kept with `archive_kind="zip"` (real names → the zip path, targeted member fetch
included); a sqlite_zip one — and any AiiDA archive a peek could not resolve (legacy tar exports,
unreadable ones) — with `archive_kind="aiida"`. A **single compressed data file**
(`optimade.jsonl.gz`, `*.json.bz2`, `*.xyz.gz`) is not treated as a misnamed tarball (the shared
fetch's bare-`.gz` heuristic, right for Zenodo's `…-vasp-raw.gz`), which keeps ~10 GB of JSON dumps
out of the blind fetch. Gaps (what a peek could not see) are listed per record in the report.

**Evidence uses fetch's own name rules.** A peeked member counts as a VASP output iff the shared
fetch would extract it (`_PARSE_RE`) and seed a calc unit from it (`_unit_role` ∈ vasprun/vaspout/
outcar — `OUTCAR1`, `vasprun_1.xml`, `OUTCAR.gz` all count); for a sqlite_zip archive the SAME rules
run over the database's file trees (`zenodo_harvest.aiida_archive.vasp_nodes`), so triage lists
exactly the calcs the extractor will write. Triage only ever prunes what fetch would have found
nothing in. The central-directory reader validates what it read (EOCD found by its comment length,
the member count vs the end record, the first header signature, `zipfile`-style correction for
data prepended to the archive), so a misparse is a *failed* peek — retried once, never cached, and
recorded as a gap — rather than "proven empty". Two real-world shapes it now reads (CSD3 census,
§11): **ZIP64 end records whose 32-bit EOCD fields are NOT sentinels** (CPython's zipfile writes
them once the directory starts past 2 GiB; the old sentinel-only test misplaced the directory by
76 bytes → 25 failed peeks) and **a 16-bit-truncated entry count** without ZIP64 records (132,202
members, count 1,130). The peek cache is versioned (`EVIDENCE_RULES_VERSION` 3) and never stores
transient or cap-dependent verdicts.

**Fetch units.** A kept record becomes one keep-list entry per archive (`recid =
<record_id>~<tag>`, the tag derived from the archive's KEY so that re-running triage can never
renumber a unit onto another unit's terminal rejection); directly-exposed VASP outputs form one
`~loose` unit. Because every
archive is extracted into its own subdir and the calc_id is built from `provenance.record_id` +
the path under `extracted/`, the calc units and calc_ids are **identical to a whole-record fetch**
(asserted by the end-to-end test) — but the disk valve now paces per archive, so e.g. the 10 ×
4.4 GB Kristoffersen tarballs never have to fit the staging budget at once.

**Shared-code changes** (all backward-compatible; Zenodo keep-lists never carry the new keys, so
Zenodo behaviour is unchanged except where noted — the pre-existing tests pass untouched). In
`zenodo_harvest/fetch.py`:

1. `_record_provenance` — a keep-list record may carry its own `provenance` (with `source`); it is
   passed through (keeping the source's `record_id` for split fetch units). Else the Zenodo block
   is derived exactly as before.
2. per-file `archive_kind` — a declared archive kind (e.g. `"zip"` for a peek-confirmed legacy
   `.aiida`) overrides the filename sniff; unknown values are ignored.
3. `session_factory` — `fetch()` builds every session (serial and per worker thread) from it. The
   MC adapter passes an anonymous factory: without it the shared fetch would fall back to
   `$ZENODO_TOKEN` and **send the Zenodo token to Materials Cloud** (verified-by-test never).
4. token scoping — the Zenodo token session attaches `Bearer $ZENODO_TOKEN` only to `zenodo.org`
   hosts (`_ZenodoOnlyBearer`), so even a Zenodo CLI run pointed at an MC keep-list cannot leak it.
5. **AiiDA extractor** (`archive_kind="aiida"`, `_extract_aiida` + the pure-stdlib
   `zenodo_harvest/aiida_archive.py`): a legacy zip → the zip extractor, a legacy tar export → the
   tar extractor, a **sqlite_zip** archive → its `db.sqlite3` copied out (charged + refunded),
   every node holding a VASP primary listed (`vasp_nodes`, opened read-only + immutable so it works
   on lock-less Lustre), and each such node's VASP-named files streamed from their `repo/<sha256>`
   blobs to the node's **legacy path** (`nodes/<uu>/<id>/<rest>/path/<file>`) — calc units,
   calc_ids and per-calc availability are then identical in shape to a legacy export's. Also, an
   `.aiida` file INSIDE another archive is now a nested archive (recursed) — a Zenodo-side
   behaviour change only for archives that bundle AiiDA exports (recall gain).

In `zenodo_harvest/zipstream.py` + `zenodo_harvest/triage.py` (a correctness fix for Zenodo too): the
central directory is walked by its BYTES, not the EOCD entry count. The old loops stopped after
`total` headers, so a 16-bit-truncated count could make targeted zip fetch's "enumeration proves
no VASP" shortcut (and Zenodo triage's peek) silently skip an archive whose VASP members sit past
the truncated count; any other count mismatch now counts as unpeekable (whole download / kept).

In `zenodo_harvest/parse.py`: **RAM-aware admission** for `parse_workers > 1`
(`parse_mem_budget`, default off): each parse reserves ~`parse_rss_ratio` × its largest
uncompressed primary (+ a 0.5 GiB child footprint) first, FIFO, so small calcs run N-way while a
multi-GB AIMD vasprun waits for room and runs alone — the primary cap becomes a per-FILE bound
(`ratio × cap ≤ budget`) instead of `workers × ratio × cap ≤ RAM`. The pipeline runs 8
`icelake-himem` cores: 4 parse workers AND a ~3.2 GB cap (a 20-core, ~10 GB-cap configuration was
dropped 2026-09-25: no primary that large exists in the evidenced data, and the big block queued for
hours).

And in `zenodo_harvest/status.py`: fetch units are joined to their record (`record_id` /
`provenance.record_id`) for the parse-progress figures — the identity for Zenodo and NOMAD.

**In-run transient retries** (`fetching.py`). The shared fetch keeps a dropped transfer's `.part`
and leaves it to the *next* run — right for the many-times-resumed Zenodo campaign, wrong for a
one-job MC harvest that exits 0 (no resubmit → a once-dropped 4 GB tarball would silently stay
unfetched). So the MC fetch re-runs the shared fetch while units are *pending* (neither fetched
nor terminally rejected), bounded, each pass resuming over Range with a fresh presigned URL
(validated live on the flaky WSL link and by a simulated-drop test). No shared-code change.

**Provenance per calc**: `source`, `record_id`, `conceptrecid`, version `doi`, `conceptdoi`,
`mcid`, `url`, `title`, `creators`, `license`, `resource_type`, `publication_date`, `keywords`,
`references` (MC's citations), `related_identifiers`, and `linked_harvested` (overlap flags) when
present — plus, for AiiDA-sourced calcs, the node UUID in the calc_id path (full AiiDA traceability).

---

## 6. Overlap with Zenodo / NOMAD — flag, don't drop

All 60 MC→Zenodo links are `IsSupplementTo`, mostly pointing at code releases. So discover
**flags** instead of dropping: `zenodo_linked_in_dataset` (a linked DOI is in the harvested Zenodo
dataset), `zenodo_title_similar` (token-Jaccard ≥ 0.6 against Zenodo dataset titles),
`nomad_calcs_citing` (NOMAD calcs whose references cite the MC record, counted once per calc). The
flags travel in `provenance.linked_harvested` for the planned training-time physics-level dedup;
`--drop-linked` exists if a hard drop is ever wanted.

**The CSD3 census flagged 2 records** (0 NOMAD citations); both were inspected (2026-09-24) and
neither is a record-level duplicate:

| MC record | Zenodo record | What each holds | Verdict |
|---|---|---|---|
| `hmdsb-k3h43` — *Hidden spontaneous polarisation in … Sn₂SbS₂I₃* (Kavanagh, Savory, Scanlon, Walsh; 3.3 GB) | `4683140` (Kavanagh, Scanlon, Walsh; 32 files, 2.5 GB; linked by DOI, title 0.75) | **MC**: ONE `Sn2SbS2I3_AiiDA_Archive.zip` — the project's **AiiDA archive** (aiida-core 1.6.3 + aiida-vasp ≥ 2, i.e. a legacy export): relaxations with PBEsol/PBE-TS/HSE06/optB86b-vdW, phonons, HSE06+SOC electronic structure, optics, Born charges/dielectric, polarisation, misc. **Zenodo**: the same project as raw calc folders (`Structures`, `NEB_Results`, `MD` (AIMD), `Miscellaneous`, `Cmcm/Cmc2_1` phonons, `Cmc2_1_Lobster`, `BS_DOSs`, `ELFCARs`, `IsoSurfaces`, …) + 8 notebooks + videos. | **Partial overlap at most**: same project, different packaging and partly different calc sets (NEB and AIMD appear only on Zenodo; the functional-comparison relaxations, BEC/dielectric and polarisation runs only in the AiiDA archive; phonons / band structures likely in both, possibly as the same runs). Kept; calc-level check below. |
| `ervm4-pn188` — *Adsorbate chemical environment-based ML framework for heterogeneous catalysis* (Ghanekar, Deshpande, Greeley; 1.9 GB; MIT) | `7023990` (same authors; title 1.0) | **MC**: `Pt3Sn_NO.tar.bz2` + `Pt_OH.tar.bz2` — the **full DFT data**: POSCAR/CONTCAR/**OUTCAR** trajectories (nested `raw_files/*.tar.bz2`) for 1-6 NO* on Pt₃Sn(111) and OH* on Pt(100)/Pt(221), plus pickled graph objects and figure CSVs. **Zenodo**: one `ace_gcn-main.tar.gz` (31 MB) — the ACE-GCN **code** snapshot, with whatever small example outputs it ships (enough to be in the Zenodo dataset). | **Not a duplicate** — the Zenodo copy is the code (+ at most a small example subset); MC has the real dataset. It was DROPPED by the first triage (tar.bz2, no VASP keyword) and is recovered by decision 5. |

Whether any individual *calc* appears on both sides is settled after the MC pipeline by
`scripts/csd3/materials_cloud/csd3_mc_overlap.py`: every calc of each flagged pair is fingerprinted
from its frames (exact key = formula, n_atoms, n_frames, first + final `REF_energy` to 1e-6 eV,
POTCAR set hash; near key = formula, n_atoms, final energy/atom to 1e-4 eV) → exact duplicates (the
same run published twice — dedupe at training time), near-only matches (same system re-run),
per-side uniques → `mc_overlap.json`.

---

## 7. Running it (CSD3)

Full runbook: `scripts/csd3/materials_cloud/README.md`. In short:

```bash
export MC_HARVEST_DATA=/rds/user/$USER/hpc-work/materials_cloud
mkdir -p logs_mc
DISC=$(sbatch --parsable scripts/csd3/materials_cloud/10_discover.sh)   # census + triage (v3 rules)
RESUBMIT=1 sbatch --dependency=afterok:$DISC scripts/csd3/materials_cloud/20_pipeline.sh
python -m materials_cloud_harvest.cli status --max-disk-bytes 780000000000 --max-disk-files 900000
# afterwards: the overlap check, and 30_bigparse.sh ONLY if primaries were deferred
python scripts/csd3/materials_cloud/csd3_mc_overlap.py --mc-root $MC_HARVEST_DATA \
    --zenodo-dataset /rds/user/$USER/hpc-work/zenodo/dataset
```

**What bounds each stage** (measured on CSD3 2026-09-24, §11): triage peeks are
*request-latency*-bound (~0.1 s per small Range read; 1,434 peeks in 10 min 4-way, paced under
MC's 500 req/60 s); the fetch is *S3-bandwidth*-bound (52 MB/s on one stream, 96 MB/s on 8 →
`--workers 6`; ~1.2 TB ≈ 4-5 h); the parse of the many small calcs is *parse-throughput*-bound
(0.19 s/calc serial, ×3.8 with 4 workers → `--parse-workers 4`), and multi-GB primaries are
*RAM*-bound → the parse memory budget (§5) with a ~3.2 GB cap; anything bigger stays staged for the
optional `30_bigparse.sh`. No API token is needed: every record is public, the request limit is
never approached, and the bytes come from presigned S3 URLs a token would not speed up.

---

## 8. Expected cost & yield

* **Transfer**: ~1.2k fetch units / **~1.2 TB** (simulated from the CSD3 census with the v3 rules:
  ~105 GB for the 83 records with a VASP mention or VASP evidence + ~1.09 TB of unresolved
  archives in 530 records); ≈ 3.5 h at 96 MB/s. The blind-fetched archives are deleted right after
  extraction, so only VASP files persist.
* **Yield**: the 83 evidenced records ≈ **4-5×10⁴ calcs** (the pilot: 437 calc units per GB
  downloaded; Bosoni alone ~7k EOS points), plus whatever the ~35-40 expected hidden-VASP records
  among the blind-fetched ones hold; frames dominated by the AIMD records.
* **Parse**: ~2-4 h of parse work, overlapped with the fetch (4 workers under a ~37 GiB budget,
  cap ~3.2 GB; the pilot's largest primary was 1.04 GB and the evidenced set has none over 2.5 GB,
  so deferrals are not expected).
* **Disk** (dedicated ~880 GB / ~990k inodes free): staging valve 780 GB / 900k inodes; the pilot
  staged 4.3 bytes per downloaded byte for evidenced units (extracted vaspruns), 5.4 inodes/calc.
* **Measured (2026-09-25)**: 1,200 units / 1.25 TB → 75,751 calcs: 38,175 from the 84 evidenced
  records (64 yielding) and 37,576 from 38 of the 524 blind-fetched ones (7.3% hit rate vs 7.8%
  predicted); 9 h 41 min (fetch-bound, one attempt), 0 deferrals, staging peak 257 GB / 240k inodes.

---

## 9. Corrections to the raw plan (`EXTERNAL_DATA_SOURCES.md` §5)

* "44 VASP records / 159.8 GB" → 44 records, but **~84 GB + Bosoni** (whose VASP part is 3.1 GB of
  its 75.5 GB); Bosoni was reached by `q=VASP` only through an affiliation.
* "the 66 GB of `.aiida` are provenance sidecars → skip them" → skipping would lose **Bosoni's VASP**
  (which exists *only* inside AiiDA exports); legacy exports are now extracted.
* "`q=VASP` is the VASP ceiling on MC regardless of query breadth" → recall no longer depends on the
  query at all (full census + peeks).
* "files.entries inline … download link" → no link; the content URL is built from id + key and
  answers with a 302 to presigned S3.
* "exclude Bosoni by ID" → include its VASP subset (user decision).
* "a few NC/ND-licensed records → the licence gate drops them" → NC now admitted; the real gate
  issue is `mcloud-ne-1.0`/`asl`, which the shared gate would have wrongly admitted.

---

## 10. Limitations & future work

* **AiiDA archives whose peek cannot be completed** (a central directory > 1 GiB — SSSP's two 13 GB
  exports — or a database > 2 GB) are blind-fetched under decision 5 and resolved by the extractor
  after download; nothing is skipped, it only costs transfer.
* **Non-VASP formats holding VASP-derived data are not parsed**: VASP's own MLFF training database
  `ML_AB` (seen in `e1dwv-nvh07`), CONTCAR+OSZICAR-only sets (energies without forces, e.g. a
  cluster-expansion `DFT_training_data.zip` of 14k files), processed `.extxyz`/JSON (e.g. the
  Alexandria JSON dumps — an institutional product anyway). Records mentioning VASP whose peeks show
  only inputs/structures are dropped as `peek_proved_no_vasp` (11 in the census; spot-checked: all
  POSCAR/CONTCAR/INCAR-only).
* **Split archives** are not reassembled (logged `archive_multipart_unsupported`); `.rar`/`.7z` need
  the `archives` extra + an `unrar` on PATH (the pipeline script checks and warns).
* **Parser limits met in the harvest** (details + counts in `MATERIALS_CLOUD_HARVEST_RESULT.md`):
  VASP 4.x OUTCARs (ASE's chunking assumes the ≥ 5 block order; 236 calcs, validated fix
  recorded), ASE header quirks (477), misnamed / truncated archives (all non-VASP here); and two
  training-time flags — NEB images (VTST prints NEB, not DFT, forces) and VASP MLFF runs.
* **Point-in-time**: a re-run of `10_discover.sh` picks up new records (the census is cheap).
* The same full-census + evidence idea **does not scale verbatim to Zenodo** (7.3M records, 30
  req/min search, ~5k req/h file endpoint): a filtered variant (paper-graph via OpenAlex +
  depositor/ORCID snowball, then peeks of the survivors) is the realistic analogue — see the
  discussion recorded alongside `HARVEST_RESULT.md`'s "Limitations".

---

## 11. CSD3 census + sizing bench (2026-09-24, jobs 36221072 / 36221079)

**Census + triage (rules v2).** 1,241 records → 1,226 candidates (15 licence drops) → 101 below the
rank gate → 1,434 archives peeked in 10 min (4 workers; 26 in-run retries) → 83 records kept / 206
fetch units / 105.4 GB: 31 `vasp_mention`, **52 `vasp_evidence`** (the blind spot recovered by
peeks — more than the mention records), 10 `peek_proved_no_vasp`, 917 `no_vasp_evidence`, 115
`evidence_gap_only`. Left unfetched: 1,033 tar-family files / 957 GB in non-mentioning records.
Overlap flags: 2 (§6), NOMAD citations: 0.

**What the logs showed was wrong, and the fixes**:
* 25 peeks failed "central directory does not start with a file header" — ZIP64 end records with
  real 32-bit EOCD fields (all 0.09-4 GiB; 14 were AiiDA exports, incl. two of AMaRaNTA's), + 1
  "132202 of 1130 entries" (16-bit-truncated count) → reader fixed (§5); the same count bug could
  silently skip archives in the SHARED zipstream/Zenodo peek → fixed there too.
* the sqlite_zip census (130 readable databases) found aiida-vasp CalcJobs in exactly one record,
  `wygeh-jrc57` (AMaRaNTA, 2D-magnet exchange parameters): 931 retrieved folders with
  `vasprun.xml` + `OUTCAR` in 8 exports (+ the 2 unreadable ones) → decision 6, extractor built.
* 477 tar-only non-mentioning records (945 GB) vs a 7.8% peek hit rate → decision 5.
* a `.tar.lzma` (tarfile reads it) was ranked "unlikely" → now declared a tar.

**Speed probe** (`mc_speed.json`): API record GET 0.11 s, search page (100) 3.3 s, redirect hop
0.04 s, 64 KiB Range read 0.09 s (p90 0.30 s); S3 throughput 52.3 MB/s on 1 stream, 30.4 on 2,
56.6 on 4, **96.3 on 8** (per-stream falls to 19 MB/s) → 8 fetch workers.

**Pilot** (`mc_bench.json`, 12 GB stratified sample, 16 cores): 11.3 GB downloaded in 473 s
(23.8 MB/s incl. extraction, 4 workers), 4.3× extraction ratio (48.6 GB staged, 26.8k inodes),
4,928 calc units (437 per GB); primaries median 0.85 MB, p90 3.1 MB, max 1.04 GB; small-calc parse
0.19 s serial → 0.05 s with 4 workers (×3.79); the largest primaries (single-frame DOS-heavy
vaspruns, 0.65-1.04 GB) parsed in 8-12 s at 3.5-3.8 GiB peak (net RSS ratio 3.7-5.4). Projection
for the 105 GB keep-list: fetch 1.2 h vs parse 1.7 h (workers) / 3.5 h (serial) — parse-bound,
which the new parse memory budget + 6 workers address.

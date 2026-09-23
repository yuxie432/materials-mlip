# Materials Cloud harvest design (VASP → MLIP dataset)

Companion to `DESIGN.md` (Zenodo) and `NOMAD_HARVEST.md`. This note scopes the **third source
adapter**, `materials_cloud_harvest/`, which pulls VASP DFT data from the **Materials Cloud
Archive** (https://archive.materialscloud.org — EPFL/MARVEL's InvenioRDM repository) into the same
`extxyz.gz` + `metadata.jsonl` schema as the Zenodo and NOMAD datasets.

Everything under "measured" was obtained live from the MC API on **2026-09-23** (a full census of
every record). It supersedes the raw plan in `EXTERNAL_DATA_SOURCES.md` §5, several of whose
numbers turned out wrong (see §9).

> **Status (2026-09-24): BUILT, offline-tested, not yet run on CSD3.** 76 offline tests (incl. a
> full triage → shared fetch → shared pymatgen parse → verify run on a legacy-AiiDA-export-shaped
> zip); the live path (302→S3 redirect, md5, tar/zip extraction) validated from WSL on four real
> records. Next: the CSD3 probe + `10_discover.sh` census, review the report, then `20_pipeline.sh`
> (runbook: `scripts/csd3/materials_cloud/README.md`).

---

## 0. TL;DR

| Question | Answer |
|---|---|
| Worth harvesting? | **Yes, as a cheap, provenance-rich completeness sweep** — individual-group deposits, CC-BY-dominant, ~0 deposit-level overlap with Zenodo/NOMAD, novel chemistries. Small next to NOMAD (52M frames) / Zenodo (12M). |
| How big is MC? (measured) | **1,241 records / 2.55 TB** in total (latest versions; 1,535 incl. superseded). All public, no embargoes. |
| How much VASP? (measured) | **44 records mention VASP** (43 in their own text + the Bosoni ACWF record via an author affiliation); ~84 GB + Bosoni 75.5 GB (of which only its **3.1 GB of VASP AiiDA exports** are harvested). **No bare `vasprun.xml`/`OUTCAR` anywhere** — all VASP sits in archives. |
| Discovery | **Full census** — enumerate every record (13 paged requests), no keyword recall limit. Keywords only decide how fail-safe triage is. |
| Triage | Range-peek every `.zip` and `.aiida` central directory (ZIP64-aware). VASP-mentioning records kept fail-safe; **any other record kept on positive evidence** (a peek found `vasprun`/`OUTCAR`/`vaspout`). |
| AiiDA exports (894 GB, 35% of MC bytes) | **Legacy-format exports are extracted** (real member names — verified: Bosoni's VASP export holds per-calc `vasprun.xml` + `OUTCAR`); new sqlite_zip exports are detected + probed on CSD3, extraction deferred until VASP is found in one. |
| Code | `materials_cloud_harvest/` (stages 0-1, retrying fetch glue, CLI) + small backward-compatible hooks in the shared `zenodo_harvest/fetch.py` (+ a status join); fetch/parse/store/verify/merge are the shared code, **unmodified in behaviour** for Zenodo/NOMAD. calc_ids `materials_cloud:<record_id>:<path>`. |
| Cost | One ~1-2 h discover/triage job + one ≤12 h pipeline job (transfer ~90 GB + positives; exact figure is in the triage report). |

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

## 4. Decisions (user, 2026-09-23)

| # | Decision | Chosen | Alternatives considered |
|---|---|---|---|
| 1 | Discovery scope | **Full census + evidence gate**: VASP-mentioning records fail-safe; others only on positive peek evidence | VASP-mention only (the raw plan); + tar head-peek (~70 GB, low expected yield); download everything (~2.1 TB, mostly QE) |
| 2 | AiiDA exports | **Extract legacy-format exports now**; sqlite_zip detected + probed (db.sqlite3) on CSD3, extraction built only if aiida-vasp calcs are found | skip all `.aiida` (the raw plan — would lose Bosoni's VASP); full sqlite_zip support now |
| 3 | Bosoni ACWF record | **Include only its two `*_results_vasp.aiida`** (file allowlist) | exclude by ID (the raw plan) |
| 4 | Licence | **Admit NC / NC-SA** (matches the Zenodo dataset after its NC expansion); drop ND, no-licence, `mcloud-ne-1.0`, `asl` | strict Zenodo default (drop NC); no gate |

Also decided by evidence (not a user question): **overlap is flagged, never auto-dropped** (§6).

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

**The evidence policy** (`triage._decide`). Per file: `.zip`/`.aiida` → peeked; tar-family →
unpeekable. A VASP-mentioning record is dropped only if every archive was peeked OK and none holds
a VASP primary or a nested archive (`peek_proved_no_vasp`) — exactly Zenodo's fail-safe rule. Any
other record needs a peek to show a `vasprun`/`OUTCAR`/`vaspout` member (`vasp_evidence`). Proven-
empty zips are removed from the fetch list; a legacy AiiDA export with VASP members is kept with
`archive_kind="zip"`; sqlite_zip / unreadable exports are dropped from the fetch list and listed
as **evidence gaps** in the report; so are nested-archive-only zips of non-mentioning records. The
report also totals the deliberately-skipped unpeekable tars of non-mentioning records.

**Evidence uses fetch's own name rules.** A peeked member counts as a VASP output iff the shared
fetch would extract it (`_PARSE_RE`) and seed a calc unit from it (`_unit_role` ∈ vasprun/vaspout/
outcar — `OUTCAR1`, `vasprun_1.xml`, `OUTCAR.gz` all count). Triage only ever prunes what fetch
would have found nothing in. The central-directory reader validates what it read (EOCD found by
its comment length, the member count vs the end record, the first header signature, `zipfile`-style
correction for data prepended to the archive), so a misparse is a *failed* peek — retried once,
never cached, and recorded as a gap — rather than "proven empty". The peek cache is versioned and
never stores transient or cap-dependent verdicts.

**Fetch units.** A kept record becomes one keep-list entry per archive (`recid =
<record_id>~<tag>`, the tag derived from the archive's KEY so that re-running triage can never
renumber a unit onto another unit's terminal rejection); directly-exposed VASP outputs form one
`~loose` unit. Because every
archive is extracted into its own subdir and the calc_id is built from `provenance.record_id` +
the path under `extracted/`, the calc units and calc_ids are **identical to a whole-record fetch**
(asserted by the end-to-end test) — but the disk valve now paces per archive, so e.g. the 10 ×
4.4 GB Kristoffersen tarballs never have to fit the staging budget at once.

**Shared-code changes** (all backward-compatible; Zenodo keep-lists never carry the new keys, so
Zenodo behaviour is unchanged — the 444 pre-existing tests pass untouched). In `zenodo_harvest/fetch.py`:

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

All 60 MC→Zenodo links are `IsSupplementTo`, mostly pointing at code releases. The one
VASP-mentioning case (`hmdsb-k3h43` → `zenodo.4683140`) is Kavanagh's own Zenodo deposit
(structures, notebooks, NEB results) which the 3.3 GB MC record **supplements with the heavy raw
data** — dropping it at record level would lose real data (the NOMAD harvest learnt the same:
citing ≠ duplicating). So discover **flags** instead: `zenodo_linked_in_dataset` (a linked DOI is
in the harvested Zenodo dataset), `zenodo_title_similar` (token-Jaccard ≥ 0.6 against Zenodo
dataset titles), `nomad_calcs_citing` (NOMAD calcs whose references cite the MC record, counted
once per calc). The flags travel in `provenance.linked_harvested` for the planned training-time
physics-level dedup; `--drop-linked` exists if a hard drop is ever wanted.

---

## 7. Running it (CSD3)

Full runbook: `scripts/csd3/materials_cloud/README.md`. In short:

```bash
export MC_HARVEST_DATA=/rds/user/$USER/hpc-work/materials_cloud
mkdir -p logs_mc
DISC=$(sbatch --parsable scripts/csd3/materials_cloud/10_discover.sh)   # census + triage
sbatch --dependency=afterok:$DISC scripts/csd3/materials_cloud/15_bench.sh   # sizing pilot
# review mc_keep.report.json + mc_speed.json + mc_bench.json, then:
RESUBMIT=1 sbatch scripts/csd3/materials_cloud/20_pipeline.sh
sbatch scripts/csd3/materials_cloud/30_bigparse.sh    # after the pipeline: deferred big primaries
python -m materials_cloud_harvest.cli status
```

**What bounds each stage** (confirmed per run by `15_bench.sh`): triage peeks are
*request-latency*-bound (1-3 small Range reads each through the ~0.3-2 s API redirect → run 4-way
in parallel, paced under MC's 500 req/60 s); the fetch bulk is *S3-bandwidth*-bound (a few
multi-GB AIMD tarballs carry most of the ~90 GB → `--workers` from the stream-scaling probe); the
parse is expected to be the longest stage — ~10⁴ small calcs are *parse-throughput*-bound (one
forkserver child each → `--parse-workers`), and long-AIMD primaries are *RAM*-bound (pymatgen
~10-12× the file → a moderate cap in the pipeline, the rest deferred to `30_bigparse.sh` on a fat
allocation). No API token is needed: every record is public, the request limit is never
approached, and the bytes come from presigned S3 URLs a token would not speed up.

---

## 8. Expected cost & yield (estimates — the triage report gives the exact bytes)

* **Transfer**: ≤ ~90 GB (the 44 VASP-mentioning records minus peek-pruned zips, + Bosoni's 3.1 GB)
  + whatever the census recovers from non-mentioning records. At CSD3 speeds (to be measured by the
  probe) this is under an hour.
* **Yield**: of the 44, perhaps ~20–30 hold VASP outputs (the small ones are often inputs only);
  ~10⁴ calcs (Bosoni alone ~7k EOS points) and **10⁵–10⁶ frames**, dominated by the AIMD records.
* **Parse RAM**: long-AIMD vaspruns may be multi-GB → `20_pipeline.sh` runs 3 parse workers on 16
  `icelake-himem` cores (~106 GiB) with a 2.5 GB (uncompressed) cap; anything bigger stays staged
  and `30_bigparse.sh` parses it one at a time on 32 cores (~211 GiB, 16 GB cap).
* **Disk** (dedicated ~800 GB / ~900k inodes): staging valve 680 GB / 765k inodes.

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

* **sqlite_zip AiiDA exports** are an evidence gap until the CSD3 probe (`csd3_mc_probe.py --aiida`)
  has queried their `db.sqlite3`s; if it finds aiida-vasp calcs, add extraction (map
  `repository_metadata` names → `repo/<sha256>` members, then the ordinary unit/parse path).
* **Unpeekable tars in non-mentioning records** (~971 GB, overwhelmingly QE) are deliberately not
  downloaded; a bounded tar head-peek (stream the first ~64 MB, read member names) is the cheap
  follow-up if the census suggests hidden VASP is non-negligible.
* **Tar-format `.aiida`**, `.tar.lzma` (one 3 KB record) and split archives are not handled.
* **Nested archives inside zips of non-mentioning records** are listed as gaps, not fetched.
* **Point-in-time**: a re-run of `10_discover.sh` picks up new records (the census is cheap).
* The same full-census + evidence idea **does not scale verbatim to Zenodo** (7.3M records, 30
  req/min search, ~5k req/h file endpoint): a filtered variant (paper-graph via OpenAlex +
  depositor/ORCID snowball, then peeks of the survivors) is the realistic analogue — see the
  discussion recorded alongside `HARVEST_RESULT.md`'s "Limitations".

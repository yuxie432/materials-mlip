# NOMAD harvest design (VASP → MLIP dataset)

Companion to `DESIGN.md` (Zenodo). This note scopes a **second source adapter** that
pulls VASP DFT data from **NOMAD** (https://nomad-lab.eu) into the *same*
`data/dataset/` (extxyz.gz shards + `metadata.jsonl`) the Zenodo pipeline produces,
so both feed one MLIP training set.

All API facts below were **verified live against the production API on 2026-08-05**
(NOMAD `1.4.3.post1`) and cross-checked against the official docs. Where a number is
live, it is reproducible from the `curl` snippets given. Anything unverified is flagged **⚠**.

---

## 0. TL;DR

| Question | Recommendation |
|---|---|
| Is NOMAD worth harvesting? | **Yes.** ~**14.7M** public VASP entries, CC BY 4.0, one well-documented paginated REST API. |
| **Scope (mentor 2026-08-10)** | **Direct uploads only** — exclude AFLOW/OQMD/Materials Project (52% of VASP; MP is also harvested directly via `mp-api`). **~7,110,950** VASP-DFT direct-upload entries remain (live-verified). Dedup those against the already-fetched Zenodo records. |
| Raw VASP files, or NOMAD's parsed "archive"? | **Raw `vasprun.xml`, re-parsed by the *existing* `parse.py`** (mentor: preserve all metadata). Keeps the NOMAD dataset byte-for-byte schema-identical to the Zenodo one. The archive is rejected as the label source: no drop-in σ→0 energy (`energy.total_t0` anomalous), undocumented stress sign, and ~0.7 s/entry. |
| **Estimated size** (measured 2026-08-05) | ~7.1M calcs → **~30M frames** (median **1** / mean **~4.6** ionic steps/entry; range ~15–100M — AIMD tail is the uncertainty). Final on disk **~10 GB** extxyz.gz + **~15–20 GB** `metadata.jsonl`. Transient download **~6.5 TB** vasprun-only over the whole run (deleted after parse); **do not fetch OUTCAR wholesale** (~170 TB — its mean is 23 MB with a 412 MB tail). |
| Biggest risk? | **Deduplication.** Handled by (a) the `not external_db` scope filter (drops AFLOW/OQMD/MP), and (b) scanning each entry's `references` for `zenodo.org`/DOI overlap with the Zenodo dataset. |
| How to find VASP DFT | `results.method.simulation.program_name = "VASP"` + `results.method.method_name = "DFT"`. **Do NOT filter on the `trajectory` available-property** — measured, it tags only ~0.1% of direct uploads yet most ARE multi-step; the ionic steps live in the raw vasprun and are recovered at parse time. |
| Pagination | **Keyset** (`page_after_value`/`next_page_after_value`), never offset (offset is hard-capped at 10 000, exactly like Zenodo's window). |
| Auth | **Not required** for public reads. `owner="public"` selects published, non-embargoed data. |
| Licence | Default **CC BY 4.0** → redistributing a derived training set is explicitly allowed *with attribution to submitter + source*. Read the per-entry `license` field and keep only CC-BY. NB: `license` is a **derived** field — it can't go in `required.include` (422); use `required: {"exclude": ["quantities"]}` on a discover scan. |
| Code | A separate **`nomad_harvest/` package** (stages 0-2) that *imports* the shared `zenodo_harvest` stages 3-5 (`parse`/`store`/`verify`) unmodified. Hand-rolled `requests` client, **not** the heavyweight `nomad-lab` package. |

---

## 1. Why NOMAD is architecturally different from Zenodo

This is the single most important framing, and it flips a core Zenodo assumption:

| | Zenodo | NOMAD |
|---|---|---|
| What a "record" is | An arbitrary upload (often a `.zip`/`.tar` of many calcs, or unrelated files) | **One parsed calculation** (one VASP mainfile → one entry) |
| Where DFT outputs live | Inside archives; `q` can't see file contents (`q=vasprun` → 0 hits) | **Indexed metadata** — you can query `program_name=VASP` and get 14.7M exact hits |
| Data form offered | Raw files only | **Both** raw files *and* a code-agnostic **normalized archive** (energies/forces/stress already parsed into SI) |
| Search precision | Low recall/precision funnel; peek-into-zip needed | High — the metadata *is* the DFT provenance |

Consequence: the Zenodo pipeline's hardest stages — **discover** (keyword recall) and
**triage** (peek-into-zip to confirm VASP) — largely *collapse* on NOMAD. You query
VASP directly and get structured provenance back. The effort shifts to **filtering the
firehose** and **deduplication**.

---

## 2. Verified API facts (live, 2026-08-05)

- **Base URL:** `https://nomad-lab.eu/prod/v1/api/v1`
- **Swagger UI:** `https://nomad-lab.eu/prod/v1/api/v1/extensions/docs`
- **OpenAPI JSON (authoritative, 87 endpoints):** `https://nomad-lab.eu/prod/v1/api/v1/openapi.json`
- **Docs (note: the old `/howto/programmatic/` path 404s now):**
  `https://nomad-lab.eu/prod/v1/docs/howto/manage/program/api.html` (+ `auth.html`, `archive_query.html`, `download.html`)

### Scale (all VASP-filtered unless noted)

```
total entries (all codes) ............ 19,394,094
VASP entries ......................... 14,748,789
VASP AND method_name=DFT ............. 14,748,680
  ├─ external_db = AFLOW ............. 6,788,660
  ├─ external_db = OQMD .............. 568,645
  ├─ external_db = Materials Project . 280,496
  └─ direct uploads (no external_db) . 7,110,988   ← sums exactly to VASP total
available_properties ⊇ trajectory .... 2,336,438   (has per-ionic-step data)
```
Reproduce any line:
```bash
curl -sS -X POST 'https://nomad-lab.eu/prod/v1/api/v1/entries/query' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"results.method.simulation.program_name":"VASP"},"pagination":{"page_size":0}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["pagination"]["total"])'
```

### Key endpoints (from OpenAPI)

| Purpose | Endpoint |
|---|---|
| Search metadata | `POST /entries/query` (or `GET /entries`) |
| **Bulk normalized archive** (parsed energy/forces/…) | `POST /entries/archive/query` (`required` selects sections) |
| **Bulk raw files** (original VASP files, as one zip) | `POST /entries/raw/query` → `application/zip` |
| List an entry's raw files (no download) | `GET /entries/{entry_id}/rawdir` |
| Download one raw file by path | `GET /entries/{entry_id}/raw/{path}` |
| One entry's archive | `GET /entries/{entry_id}/archive` |
| Token (optional) | `POST /auth/token` (password grant) · `GET /auth/app_token` (long-lived) |

### Pagination — keyset only

- `page_size` max = **10 000** (10 001 → HTTP 400). Default 10.
- **Offset (`page`/`page_offset`) is hard-capped at the 10 000th result** (10 001 → HTTP 422) — same trap as Zenodo's 10k window.
- **Keyset scrolls the entire set:** omit `page_after_value` on request 1; feed each response's `pagination.next_page_after_value` into the next request's `pagination.page_after_value`; stop when it's absent. Order by a unique key (`order_by: entry_id`, default).

### Auth & rate limits

- **Anonymous read works** (every query in this doc ran token-free). Pass `owner="public"` for published + non-embargoed scope. Tokens are only for private/own data or writes; **no documented read-rate benefit** from a token. ⚠
- Documented limits are installation-dependent, floor **"~30 req/s or 10 concurrent"**. **HTTP 503 = rate-limited** → back off. There are **no `RateLimit-*`/`Retry-After` headers**, so self-throttle. For a multi-million-entry pull, email **support@nomad-lab.eu** first (documented etiquette).

---

## 3. The central decision — raw files vs normalized archive

NOMAD uniquely offers **both**. They lead to very different amounts of new code.

### Option A — raw files → existing `parse.py`  ✅ recommended primary

Download the original `vasprun.xml`/`OUTCAR` and feed them straight into the
**existing pymatgen parser**. Verified: a sample AFLOW entry's raw files are
`vasprun.xml.relax1` (1.16 MB) + `vasprun.xml.relax2` (0.55 MB) — modest, no
CHGCAR/WAVECAR bloat; `POST /entries/raw/query` returns them as a zip.

- **Pros:** reuses *everything* already built and mentor-approved — per-ionic-step
  `e_0_energy` (σ→0) with pymatgen's `final_energy` bugfix, `E_free`/`entropy_TS`,
  `REF_stress` in ASE convention, per-frame `scf_dE`, `dft_charge`/`dft_magmom`,
  POTCAR `titel`, `run_type`, availability flags. The NOMAD dataset comes out
  **schema-identical** to the Zenodo one → `merge-datasets` + `verify` just work.
- **Cons:** must re-download + re-parse; must handle NOMAD's mainfile **naming**
  (`vasprun.xml.relax1`, not bare `vasprun.xml`) — point pymatgen at the exact file.
- **Effort:** small. New = a NOMAD client + discover/fetch adapter; parse/store/merge unchanged.

### Option B — normalized archive (`/entries/archive/query`)

Pull NOMAD's already-parsed arrays. **Verified live** for a VASP entry (16 ionic
steps): each `run.calculation[i]` has `energy.total.value`, `forces.total.value`
(per-atom N×3), **and** `stress.total.value`; each `run.system[i].atoms` has
`positions`, `labels`, `lattice_vectors`. **All SI units:**

```
energy   run.calculation[].energy.total.value   J     × 6.2415e18  → eV
forces   run.calculation[].forces.total.value   N     × 6.2415e8   → eV/Å
stress   run.calculation[].stress.total.value   Pa    × 6.2415e-12 → eV/Å³
pos/cell run.system[].atoms.positions/lattice   m     × 1e10       → Å
```
Trajectory wiring (verified): each ionic step is its own `calculation[k]` **and**
`system[k]`, linked by `run.calculation[k].system_ref` (e.g. `#/run/0/system/50`);
per-step `energy`/`forces`/`stress` were present on **51/51** steps of a real
relaxation. `calculation.step` is unpopulated (`None`) — **use the array index** as the
step number. (The `nomad-lab` client returns pint quantities, so `.to('eV').magnitude`
avoids the manual factors above.)

- **Pros:** no per-file parsing; code-agnostic; clean arrays; forces+stress per step present.
- **Cons:** (1) **slow — ~0.7 s/entry** at `page_size=20` (14.7M ⇒ months single-stream);
  (2) a **second parser** mapping NOMAD metainfo → the existing frame schema + SI→eV/Å;
  (3) **⚠ the energy label doesn't line up** — verified across two VASP versions,
  `energy.free`=VASP free energy **F**, `energy.total`=energy-without-entropy **E**, and
  `energy.total_t0` is **anomalous** (reads ~+0.001–0.003 eV, *not* the ~−26 eV σ→0
  value its name implies). So there is **no drop-in equivalent of the project's
  `e_0_energy` (σ→0)** label in the archive; Option A gets it for free from pymatgen's
  `final_energy` bugfix;
  (4) **⚠ the stress sign convention is undocumented** — the 3×3 tensor is stored in Pa
  but the sign must be validated against a raw re-parse before use as a `REF_stress` label;
  (5) trust NOMAD's normalization rather than the vetted pymatgen path.

**Recommendation:** **Option A as the label source**, **Option B for metadata/filtering
and as an optional cross-check.** Rationale: schema parity with the Zenodo dataset,
preservation of every mentor-agreed convention, and the archive path is too slow to be
the bulk workhorse anyway. Keep a thin metainfo→frame adapter (Option B) in the back
pocket for entries whose raw files are missing/partial.

---

## 4. Filtering to the right data

Query fields (all live-verified):

- **Code:** `results.method.simulation.program_name = "VASP"` (fixed vocabulary — exact spelling).
- **DFT only (exclude GW/BSE/DMFT):** `results.method.method_name = "DFT"`.
- **Multi-step / forces — do NOT try to pre-filter.** ⚠ The
  `results.properties.available_properties` value `"trajectory"` is **not** a "multi-step"
  proxy: measured on the direct-upload subset it tags only **7,143 (0.1%)** of entries, yet
  the archive sample shows a **median of 1** and **mean of ~4.6** ionic steps/entry — most
  are single-point or short relaxations, and the step count lives in the raw vasprun, not in
  a searchable flag. There is likewise no first-class "has forces" flag (only
  `final_force_maximum`, single-point). So take all VASP-DFT direct uploads and **recover
  steps + confirm forces at parse time** (the Zenodo "reject at parse time, not search time"
  rule).
- **Chemistry (optional):** `results.material.elements` with `any`/`all`/`none`
  (e.g. `{"results.material.elements": {"all": ["Ti","O"]}}`).
- **Public only:** top-level `owner: "public"` (do **not** filter `with_embargo=false`
  as a query-string — the string mismatches the boolean and returns 0).

Metadata worth persisting per candidate (returned by `entries/query`):
`entry_id`, `upload_id`, `mainfile`, `external_db`, `external_id`, `references`,
`datasets`, `license`, `results.method.*` (functional, program_version, basis),
`results.material.elements`, `results.properties.available_properties`.

---

## 5. Scale & deduplication — the thing to get right first

**~52% of VASP entries are bulk ingests** of three high-throughput databases. This
matters three ways:

1. **Overlap with your own project.** Your `CLAUDE.md`/roadmap already lists
   **Materials Project via `mp-api`** as a target → NOMAD's **280 k MP entries would
   double-count**. Pick one path for MP (probably `mp-api` direct, and *exclude*
   `external_db="Materials Project"` from the NOMAD harvest).
2. **Overlap with Zenodo.** A Zenodo-origin upload mirrored into NOMAD is **not**
   tagged with an `external_db`; it surfaces as a `zenodo.org`/`doi.org` URL in the
   entry's **`references`**. Detect by scanning `references` against your Zenodo
   `metadata.jsonl` DOIs.
3. **Diversity vs volume.** AFLOW/OQMD are systematic high-throughput relaxations —
   enormous but **low-diversity** (many similar compositions/settings), which is often
   *not* what a foundation-model MLIP wants (cf. the "one-size-fits-all datasets are
   problematic" note in `CLAUDE.md`). The **7.1M direct uploads** are paper-backed and
   more diverse, but less uniformly labeled.

**Dedup strategy:**
- Primary key when present: **`(external_db, external_id)`**.
- Else scan **`references`** for DOIs/URLs already in the Zenodo dataset.
- Last resort: a structure+composition+energy hash (OPTIMADE fields help — see §7).
- **Propagate** `external_db` + `external_id` + `references` + any `datasets[].doi` into
  the dataset's provenance record (satisfies the mandatory-provenance rule and gives
  correct CC-BY attribution).

**Decision (mentor 2026-08-10): direct uploads only.** Exclude AFLOW/OQMD/MP via the
`not external_db:any` clause (their high-throughput, low-diversity bulk is set aside, and
MP is harvested via `mp-api`), leaving ~**7.1M** direct-upload VASP-DFT entries. Even that
is far more than one quota holds, so scope further (element systems, or a cap) and pace with
the disk/inode valve — see the size estimate next.

### Size estimate — measured 2026-08-05

Sampled ~140 direct-upload entries spread across the `entry_id` keyspace (≈140 distinct
uploads; `entry_id` is a hash, so a keyset scan spreads across uploads). Per entry:

| per entry | median | mean | tail |
|---|---|---|---|
| ionic steps (= frames) | **1** | **~4.6** | p90 = 11, max 98 (AIMD tail higher) |
| atoms / frame | 6 | ~9 | max 32 |
| `vasprun.xml` as stored (often `.bz2`/`.gz`) | 0.36 MB | ~0.9 MB | max 14 MB |
| `OUTCAR` | 0.03 MB | ~23 MB | **max 412 MB** |

The 7.11M entries come from only **~3,792 uploads** (~1,875 entries each), which is why
per-upload bulk fetch is cheap on request count. Extrapolated to the full harvest (pre-dedup):

| | estimate |
|---|---|
| frames | **~30M** (mean 4.6 steps); range **~15–100M** — the AIMD tail is the whole uncertainty |
| final `extxyz.gz` | **~10 GB** (cells are small) — robustly tens of GB even at 100M frames |
| `metadata.jsonl` | **~15–20 GB** (one record/calc × 7.1M — this *dominates* the payload) |
| **final total on disk** | **~25–40 GB** |
| transient download, vasprun-only | **~6.5 TB** over the whole run (deleted after each parse) |
| transient download +OUTCAR | ~170 TB → **don't**; OUTCAR is dominated by a 412 MB tail |
| peak staging disk | disk/inode-valve budget (e.g. 200–800 GB) + the growing dataset — **fits the 1 TB / 1M-inode CSD3 quota** |

Unit economics for scoping a subset: **per 100k entries** ≈ 0.5M frames, ~90 GB vasprun
download, ~1 GB `extxyz.gz`. Re-measure on your own sample with
`python -m nomad_harvest.smoke -n 200 --keep` (it prints this calibration live).

---

## 6. Licensing & provenance

- **Default licence CC BY 4.0** (NOMAD Terms of Use, verbatim: content "may be copied,
  distributed, transmitted, **and adapted** … provided proper attribution is given to
  the Submitter(s) and to the source"). Metadata is CC0. → A **derived training set is
  explicitly permitted**, with attribution.
- **A submitter can deviate** ("unless specified otherwise") — there is a per-entry
  **`license`** field (sample value `"CC BY 4.0"`). **Read it; keep only CC-BY; log
  anything else** as a rejection (mirrors the Zenodo `license` capture).
- **Embargo:** ≤3-year embargo, auto-opens after. `owner="public"` avoids all of it.
- Store `source="NOMAD"`, `entry_id`, `external_db`/`external_id`, `references`, licence
  — same provenance shape the Zenodo metadata already carries.

---

## 7. Pipeline — the `nomad_harvest/` package (built)

NOMAD lives in a **separate top-level package** `nomad_harvest/` (stages 0-2) that
**imports the shared `zenodo_harvest` stages 3-5 unmodified** (parse → store → merge/verify),
so both sources produce one schema-identical dataset. Nothing in `zenodo_harvest/` is changed.

| Stage | Zenodo module | `nomad_harvest/` |
|---|---|---|
| 0 discover | `discover.py` (keyword search) | `client.iter_entries` keyset-paginates `entries/query` (the direct-upload VASP-DFT query); `harvest.discover_candidates` license-gates + Zenodo-dedups inline and writes a **slimmed** keep-list. ~700 requests total — trivial. |
| 1 triage | `triage.py` (peek-into-zip) | **folded into discover** — no zip-peek. `is_reusable_license` (reused) + `references`→Zenodo dedup, both audited to a rejection log. |
| 2 fetch | `fetch.py` (download+extract archives) | `harvest.fetch_candidates` → per entry, `rawdir` then `download_raw_file` the vasprun under a **canonical name** (handles `.bz2`/`.gz` + odd naming); groups via the shared `_find_calc_units`; writes `nomad_fetched.jsonl`. Heavy outputs → availability only, never fetched. |
| 3 parse | `parse.py` | **reused unmodified**: `python -m zenodo_harvest.cli parse --in nomad_fetched.jsonl --dataset-dir data/dataset/nomad`. |
| 4 store | `store.py` | **reused** — `REF_energy/forces/stress` + `metadata.jsonl`. |
| join/verify | `dataset_ops.py` | **reused** — `verify` gates it; `merge-datasets` folds the NOMAD dataset into the Zenodo one. |

What the Zenodo pipeline needs that NOMAD does **not**: keyword-recall discover + date-bisection,
zip-peek triage, archive download/selective-extract, nested-archive recursion, zip-stream — all
gone. NOMAD's indexed metadata + per-file raw API replace them.

**One deferred shared-core change (Phase 3, at merge time).** The shared parser hard-codes
`calc_id = "zenodo:<recid>:…"` and tags frames `source="zenodo"`. The authoritative source is
already correct in `metadata.jsonl` (`provenance.source="nomad"`), so for an *isolated* NOMAD
dataset this is cosmetic; but before *merging* into the combined dataset the parser should gain a
small backward-compatible `source` parameter so NOMAD calc_ids namespace as `nomad:…` (no
cross-source id collision in `verify`'s bijection). Prototyped and then reverted per your
instruction to keep `zenodo_harvest/` untouched — it's a ~7-line change to make then.

### Rate-limiting process during the run

- **Not discovery** (~700 keyset requests for all 7.1M) and **not request count** — the 7.1M
  entries come from only **~3,792 uploads**, so fetch can batch per upload.
- The bottleneck is a race between **fetch throughput** (~6.5 TB pulled under NOMAD's
  ~10-concurrent / ~30-req/s ceiling *and* its 5xx flakiness — 502/503/504 all appear under
  load, no `Retry-After` headers, so conservative self-throttle + exponential backoff) and
  **parse CPU** (~7.1M pymatgen parses ≈ tens of CPU-days). Parse parallelises across array-job
  cores; fetch is capped by NOMAD's limits and can't. **So fetch is the rate-limiting stage**,
  with parse the close second that parallelism keeps hidden behind it (same fetch∥parse overlap
  as the Zenodo `pipeline`). Final storage (~25-40 GB) is a non-issue; peak *staging* disk is
  bounded by the valve.

**Complementary path — OPTIMADE.** NOMAD implements OPTIMADE at
`https://nomad-lab.eu/prod/v1/optimade/v1` (18.8M structures). Standardized
`elements`/`nelements`/`chemical_formula_*` filters are handy for *cross-database
structure-level* selection/dedup — but OPTIMADE returns structures, **not**
forces/energies, so it's a discovery/dedup aid only, never a label source.

---

## 8. Worth noting (consolidated gotchas)

1. **Dedup** (§5) — the scope filter drops AFLOW/OQMD/MP; a `references` scan catches Zenodo overlap. MP also comes from `mp-api`, so don't double-harvest it.
2. **Keyset pagination only** — offset dies at 10 000 (HTTP 422), just like Zenodo.
3. **`license` is a DERIVED field** — it 422s inside `required.include`; use `required: {"exclude": ["quantities"]}` on a discover scan (keeps `license`/`references`/`results`) and slim the keep-list yourself (`slim_candidate`) so manifest size ≠ response size. *(caught by the smoke test)*
4. **`raw/{path}` is relative to the mainfile's DIRECTORY**, not the upload root — the full path 404s, the mainfile-dir-relative path 200s (`raw_path_rel`). *(caught by the smoke test — every fetch failed until fixed)*
5. **No multi-step / forces search flag** — the `trajectory` property tags only ~0.1% of direct uploads (median 1, mean ~4.6 steps); **don't pre-filter on it** — recover steps + confirm forces at parse time.
6. **Compression + naming** — NOMAD stores files `.bz2`/`.gz` under varied names (`GEO3_vasprun.xml.bz2`, `vasprun.xml.relax1`); stage under a **canonical name** (`vasprun.xml[.bz2]`) so `_find_calc_units` + pymatgen `zopen` read them unchanged. Do **not** rely on the server-side `decompress` param — it doesn't handle `.bz2`. *(smoke verifies compressed-kept == locally-decompressed)*
7. **Self-throttle + 5xx backoff** — ~30 req/s, ≤10 concurrent, no `Retry-After`; **502/503/504 all appear under load** (seen repeatedly in the smoke). The client backs off exponentially. Email support before a multi-million pull.
8. **Archive label caveats (only if you ever use the normalized archive)** — `energy.free`=F, `energy.total`=E, `energy.total_t0` anomalous (no drop-in σ→0), stress sign undocumented, `calculation.step`=None. This is *why* we re-parse raw vasprun instead.
9. **Raw files can be missing/partial** — `rawdir` first; skip+log an entry with no vasprun/OUTCAR primary.
10. **Read the per-entry `license`** — keep only CC-BY, log the rest.
11. **calc_id namespacing before merge** — the shared parser prefixes `zenodo:`; add a small `source` param (§7) before merging NOMAD into the combined dataset so ids can't collide.
12. **`nomad-lab` PyPI client** returns the *archive* (not raw files) and is a heavy dependency — prefer the hand-rolled `requests` client.
13. **Scale** — even scoped to ~7.1M direct uploads it's ≫ the Zenodo harvest. Filter hard (elements / a cap); pace with the disk/inode valve.

---

## 9. Concrete next steps

- **Phase 0 — smoke test — ✅ BUILT & PASSING.** `python -m nomad_harvest.smoke -n 12`
  runs the whole path live in an isolated dir (discover → dedup → fetch → decompress →
  **existing pymatgen parser** → store → `verify`) with PASS/FAIL checks + size calibration.
  Validated 2026-08-10: 6/6 real NOMAD vaspruns parsed → 113 frames, forces+stress present,
  `verify` ok, `.bz2` handled. Offline logic: `pytest tests/test_nomad.py` (30 tests).
- **Phase 1 — scoped discover + fetch.** `python -m nomad_harvest.cli discover --elements … \
  --out data/manifests/nomad_keep.jsonl`, then `… fetch --in nomad_keep.jsonl`; parse + verify
  with `zenodo_harvest.cli`. Start with one element system or a `--max-entries` cap.
- **Phase 2 — dedup review.** discover already dedups against the Zenodo `metadata.jsonl`
  (`--zenodo-metadata`, default path); review `nomad_rejections.jsonl` with the mentor.
- **Phase 3 — scale on CSD3.** Add the `source` calc_id param (§7), batch fetch per `upload_id`
  (~3,792 batches), reuse the disk/inode valve + array-job parse; `merge-datasets` into the
  combined dataset. Notify `support@nomad-lab.eu` if the pull is large.

## 10. Open questions for the mentor

- Target **size / element coverage** for the first scoped NOMAD slice? (7.1M is too big for one pass.)
- Also fetch **OUTCAR** (per-atom charges/spins) for the subset where it's small, or **vasprun-only** everywhere? (Wholesale OUTCAR ≈ 170 TB — see §5.)
- Add the `source` calc_id param to the shared parser now, or keep NOMAD in its **own** dataset dir until an explicit merge?

*(Resolved 2026-08-10: scope = direct uploads only; MP via `mp-api` not NOMAD; raw re-parse, not the normalized archive.)*

## 11. References (official)

- API how-to: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/api.html
- Auth: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/auth.html
- ArchiveQuery: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/archive_query.html
- OpenAPI: https://nomad-lab.eu/prod/v1/api/v1/openapi.json · Swagger: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs
- Terms / licence: https://nomad-lab.eu/nomad-lab/terms.html · FAQ: https://nomad-lab.eu/nomad-lab/faqs.html
- OPTIMADE: https://nomad-lab.eu/prod/v1/optimade/v1 · provider `nmd`: https://providers.optimade.org/index-metadbs/nmd

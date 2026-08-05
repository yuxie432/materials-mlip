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
| Raw VASP files, or NOMAD's parsed "archive"? | **Primary: download the raw `vasprun.xml`/`OUTCAR` and run them through the *existing* `parse.py`.** Keeps the NOMAD dataset byte-for-byte schema-identical to the Zenodo one (same `REF_*` keys, same quality tags, trivially mergeable). Use the normalized archive only for *metadata filtering/dedup*, not as the bulk label source (it's ~0.7 s/entry — far too slow at 14.7M). |
| Biggest risk? | **Deduplication.** ~52% of VASP entries are bulk ingests of **AFLOW (6.79M) / OQMD (0.57M) / Materials Project (0.28M)** — and MP overlaps your *own* planned `mp-api` harvest. Decide inclusion + dedup *before* scaling. |
| How to find VASP-DFT-with-forces | `results.method.simulation.program_name = "VASP"` + `results.method.method_name = "DFT"`, then `results.properties.available_properties` contains `trajectory` (multi-step) — confirm forces per entry at parse time. |
| Pagination | **Keyset** (`page_after_value`/`next_page_after_value`), never offset (offset is hard-capped at 10 000, exactly like Zenodo's window). |
| Auth | **Not required** for public reads. `owner="public"` selects published, non-embargoed data. |
| Licence | Default **CC BY 4.0** → redistributing a derived training set is explicitly allowed *with attribution to submitter + source*. Read the per-entry `license` field and keep only CC-BY. |
| Client | Hand-rolled `requests` client mirroring `client.py` (resumable, disk-paced), **not** the heavyweight `nomad-lab` package. |

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
- **Per-ionic-step data (for trajectories):** `results.properties.available_properties`
  contains `"trajectory"` (2.34M entries). ⚠ **There is no first-class "has per-step
  forces" flag** — the only force-named searchable value is `final_force_maximum`
  (single-point). Forces live in the archive/raw file, so **confirm forces at parse
  time** (exactly the Zenodo "reject at parse time, not search time" rule).
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

**Decision to take to the mentor (see §9):** include AFLOW/OQMD at all, or scope the
first NOMAD harvest to **direct uploads + selected element systems**? At 14.7M entries
you *cannot and should not* pull everything — filter hard, pace with the disk/inode valve.

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

## 7. Proposed pipeline (mapped onto the existing package)

NOMAD becomes a **source adapter** feeding the same stages 3–4. Reuse is high:

| Stage | Zenodo module | NOMAD plan |
|---|---|---|
| 0 discover | `discover.py` (keyword search) | **new** `nomad_discover` — keyset-paginate `entries/query` (VASP+DFT [+filters]) → candidate manifest (`entry_id`, `mainfile`, `external_db`, `references`, `license`, elements, available_properties). Single-stream, self-throttled ≤30 req/s. |
| 1 triage | `triage.py` (peek-into-zip) | **simpler** — no peek needed. Filter `method_name=DFT`, `license` CC-BY, `available_properties ⊇ trajectory`, **dedup** vs MP-plan + Zenodo (§5). |
| 2 fetch | `fetch.py` (download+extract VASP from archives) | **new thin** `nomad_fetch` — pull only the VASP files with **server-side** `files.glob_pattern="*vasprun.xml*"` (also catches AFLOW's `.relax1/.relax2`; `include_files` matches exact names only so it misses those). Either bulk `POST /entries/raw/query` (one zip for a whole batch) or per-file `GET /entries/{id}/raw/{path}` (supports `offset`/`length`/`decompress`), into the **same** `raw_dir/<entry_id>/`. `GET /entries/{id}/rawdir` lists files first if you want to inspect. Reuse the **disk/inode valve** (`StagingBudget`) and resumability. |
| 3 parse | `parse.py` | **unchanged** — pymatgen `Vasprun` on the downloaded `vasprun.xml*`. One tweak: accept NOMAD mainfile names (`vasprun.xml.relax1`, `OUTCAR` variants). |
| 4 store | `store.py` | **unchanged** — `REF_energy/forces/stress`, `metadata.jsonl`. |
| join/verify | `dataset_ops.py` | **unchanged** — `merge-datasets` folds NOMAD task dirs into the Zenodo dataset; `verify` gates it. |

New code is essentially: **`nomad_client.py`** (throttled/retrying REST client with
keyset pagination + 503 backoff, mirroring `client.py`) and **`nomad_discover.py` +
`nomad_fetch.py`**. Parse/store/merge/verify/status are reused as-is. Optionally add
`nomad_archive.py` (Option B metainfo→frame adapter) as a fallback.

**Complementary path — OPTIMADE.** NOMAD implements OPTIMADE at
`https://nomad-lab.eu/prod/v1/optimade/v1` (18.8M structures). Standardized
`elements`/`nelements`/`chemical_formula_*` filters are handy for *cross-database
structure-level* selection/dedup — but OPTIMADE returns structures, **not**
forces/energies, so it's a discovery/dedup aid only, never a label source.

---

## 8. Worth noting (consolidated gotchas)

1. **Dedup before scale** (§5) — MP double-count vs your `mp-api` plan; Zenodo overlap via `references`; AFLOW/OQMD volume-vs-diversity.
2. **Keyset pagination only** — offset dies at 10 000 (HTTP 422), just like Zenodo.
3. **Archive query is slow (~0.7 s/entry)** — not a bulk label source; use raw+existing parser.
4. **No "has per-step forces" search flag** — filter on `trajectory`, confirm at parse time.
5. **SI units** in the archive (J/N/m/Pa) — only relevant if you ever use Option B; Option A (pymatgen) sidesteps it.
6. **Archive label caveats (Option B only)** — verified: `energy.free`=VASP F, `energy.total`=energy-without-entropy E, and `energy.total_t0` is **anomalous** (not σ→0), so there's **no drop-in `e_0_energy`**; and the **stress sign is undocumented**. Validate both against a raw re-parse before trusting archive labels. Option A (pymatgen) avoids this entirely.
7. **Mainfile naming** — `vasprun.xml.relax1` etc.; `parse.py`/`classify_files` must match `vasprun.xml*`.
8. **AFLOW multi-segment relaxations** — a 2-stage relax can appear as *separate* entries (`.relax1`, `.relax2`), with `.relax2` also present as an aux file in the `.relax1` entry's dir. Harvest both for the full trajectory, but **dedup** so a segment isn't double-counted.
9. **Ionic-step index** — `calculation.step` is unpopulated (`None`); use the array index (`calculation[k]`↔`system[k]` linked by `system_ref`).
10. **Self-throttle** — ~30 req/s, ≤10 concurrent, 503-backoff, no headers to read; email support before millions of requests.
11. **Raw files can be missing/partial** for some entries — check `rawdir`; fall back to archive (Option B) or skip+log.
12. **Read the per-entry `license`** — keep only CC-BY, log the rest.
13. **`nomad-lab` PyPI client** wraps this API (`from nomad.client.archive import ArchiveQuery`) and handles pagination/auth/parallelism — but it's a **heavy dependency** and returns the *archive*, not raw files. Prefer the hand-rolled `requests` client for a resumable, disk-paced HPC harvest (consistent with `client.py`).
14. **Scale reality** — 14.7M ≫ the Zenodo harvest. Filter hard; never "pull everything."

---

## 9. Concrete next steps

- **Phase 0 — smoke test (½ day).** Pull ~20 VASP entries' raw `vasprun.xml*` via
  `GET /entries/{id}/raw/{path}`, run them through the *current* `parse.py`, and confirm
  frames match the Zenodo frame schema. Validates the mainfile-naming tweak end-to-end.
- **Phase 1 — client + discover.** Write `nomad_client.py` (keyset, 503-backoff) and
  `nomad_discover.py`; produce a candidate manifest for a **bounded** subset (one
  element system, or direct-uploads-only) to keep it small.
- **Phase 2 — triage + dedup.** Implement the `(external_db, external_id)` + `references`
  dedup against the Zenodo `metadata.jsonl` and the MP plan; settle inclusion policy with mentor.
- **Phase 3 — fetch + scale on CSD3.** `nomad_fetch.py` reusing the disk/inode valve;
  run stages 3–4 with the existing `pipeline`/array-job tooling; `merge-datasets` into
  the combined dataset. Notify `support@nomad-lab.eu` if the pull is large.

## 10. Open questions for the mentor

- Include the AFLOW/OQMD/MP **ingests**, or scope NOMAD to **direct uploads** (paper-backed, more diverse)?
- **MP**: harvest via `mp-api` *or* via NOMAD — not both. Which?
- Target **size / element coverage** for the NOMAD slice?
- **Trust NOMAD's archive** labels (fast, Option B) anywhere, or always **re-parse raw** (Option A)?

## 11. References (official)

- API how-to: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/api.html
- Auth: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/auth.html
- ArchiveQuery: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/archive_query.html
- OpenAPI: https://nomad-lab.eu/prod/v1/api/v1/openapi.json · Swagger: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs
- Terms / licence: https://nomad-lab.eu/nomad-lab/terms.html · FAQ: https://nomad-lab.eu/nomad-lab/faqs.html
- OPTIMADE: https://nomad-lab.eu/prod/v1/optimade/v1 · provider `nmd`: https://providers.optimade.org/index-metadbs/nmd

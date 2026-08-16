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

- **Anonymous read works** (every query in this doc ran token-free). Pass `owner="public"` for published + non-embargoed scope. **A token does NOT raise the read rate limit** (the ~30 req/s / ≤10-concurrent floor is enforced **per IP**, not per user — a token changes *who* you are, not *how fast* a given IP may pull), so for this public harvest a token buys nothing. Tokens exist only for private/own uploads or writes. If you ever want one anyway: register a free account at https://nomad-lab.eu, then either `POST /auth/token` (username+password grant, ~short-lived) or `GET /auth/app_token` (a long-lived "app token"); send it as `Authorization: Bearer <token>`. The genuine lever for more throughput is a **rate-limit exemption** requested from `support@nomad-lab.eu` (§7) — an IP-level allowance, not a token. ⚠
- Documented limits are installation-dependent, floor **"as low as 30 requests per second or 10 concurrent"**, enforced **per IP** (shared across a NAT), per the API how-to
  (https://docs.nomad-lab.eu/1.4.3/howto/manage/program/api.html). **HTTP 503 = rate-limited** → back off. There are **no `RateLimit-*`/`Retry-After` headers**, so self-throttle.
- **Contacting NOMAD is NOT a documented prerequisite for a large harvest** (checked 2026-08-14 across the API how-to, download how-to, FAQ, Terms, and Support pages — none require or request it). The *only* contact text is **reactive**: the 503 FAQ
  (https://nomad-lab.eu/nomad-lab/faqs.html) says to lower your request rate and, if the limit is genuinely too low for a legitimate use, to "contact us … if you want to get exempted from the rate-limit." So emailing **support@nomad-lab.eu** is an **optional courtesy** — worth doing only to *request a rate-limit exemption* that could shorten a multi-TB pull, not a gate to clear before starting. NOMAD's own **endorsed bulk mechanism is the streaming-zip query endpoint** `POST /entries/raw/query` (docs: https://docs.nomad-lab.eu/1.4.3/howto/manage/program/download.html) — exactly what our bulk fetch uses. There is **no public data dump**; NOMAD Oasis is a local-install of the software, not a mirror of the public archive.

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
| 2 fetch | `fetch.py` (download+extract archives) | `harvest.fetch_candidates` → per entry, `rawdir` then `download_raw_file` the vasprun under a **canonical name** (handles `.bz2`/`.gz` + odd naming); groups via the shared `_find_calc_units`; writes `nomad_fetched.jsonl`. Heavy outputs → availability only, never fetched — with **availability taken PRIMARILY from NOMAD's parsed metadata** (`results.properties.available_properties`: `dos_electronic[_new]`→`dos`, `band_structure_electronic`→`eigenvalues`; `nomad_metadata_availability`), OR'd over the filename scan (fallback for charge-density/wavefunction/…) and, at parse, the shared embedded-vasprun DOS/eigen/projected probe. `available_properties` is kept by `slim_candidate`; the unreliable `trajectory` flag is not mapped. **Now production-paced:** reuses the shared `StagingBudget` (bytes **and** inodes) as the disk/inode valve, `--workers` concurrent downloads, resume by manifest; returns `stopped_disk_budget` so the pipeline can reclaim + resume. |
| pipeline 2-4 | `pipeline.py` | **reused** — `nomad_harvest.cli pipeline` splits the keep-list into parts and drives the shared `run_pipeline` (fetch batch *i+1* ∥ parse+purge batch *i*), disk-paced. One command for a long CSD3 job (`scripts/csd3/nomad/20_pipeline.sh`). |
| 3 parse | `parse.py` | **reused unmodified**: `python -m zenodo_harvest.cli parse --in nomad_fetched.jsonl --dataset-dir data/dataset/nomad`. |
| 4 store | `store.py` | **reused** — `REF_energy/forces/stress` + `metadata.jsonl`. |
| join/verify | `dataset_ops.py` | **reused** — `verify` gates it; `merge-datasets` folds the NOMAD dataset into the Zenodo one. |

What the Zenodo pipeline needs that NOMAD does **not**: keyword-recall discover + date-bisection,
zip-peek triage, archive download/selective-extract, nested-archive recursion, zip-stream — all
gone. NOMAD's indexed metadata + per-file raw API replace them.

**The one shared-core change — DONE (2026-08-11).** The shared parser previously hard-coded
`calc_id = "zenodo:<recid>:…"` and tagged frames `source="zenodo"`. It now derives the source
from `base_meta["provenance"]["source"]` (`_source_of`, defaulting to `"zenodo"` for older
manifests), so NOMAD frames are tagged `source="nomad"` and calc_ids namespace as
`nomad:<entry_id>:…` — no cross-source id collision in `verify`'s bijection or at
`merge-datasets`. The change is **backward-compatible and byte-identical for Zenodo** (its
`provenance.source` is always `"zenodo"`, verified across all 176,739 existing records) and
needs **no CLI flag** (the authoritative provenance field drives it). `purge-raw`, which
re-derives calc_ids via the same `_calc_id`, follows automatically.

### Rate-limiting process — and the fetch redesign (built 2026-08-11)

Measured live: the naive **per-entry** fetch is **latency-bound** — ~**1 s/entry** (2 requests
each: `rawdir` + download; a 0.15 MB file takes ~0.8 s, almost all overhead), and per-entry
**concurrency makes it *worse*** (a 503 storm). That extrapolates to **~89 days** for 7.1M — the
constraint is **NOMAD's request rate**, not CSD3, disk, bandwidth, or parse (parse is ~**86 ms/calc**
→ ~**2 h on one 76-core node**; storage is ~25–40 GB, a non-issue).

**So the fix is to collapse the request count** — implemented as the default
`fetch_candidates_bulk` (fixes #1 + #2, all API behaviour verified live):
- **#1** `POST /entries/raw/query` with `{"entry_id:any": [batch]}` + `files.include_files=[<exact
  mainfiles>]` streams **ONE zip of exactly the wanted files** (verified: no over-fetch); members
  are `<upload_id>/<mainfile>`, mapped back exactly. ~1 download request per **300-entry batch**.
- **#2** `POST /entries/rawdir/query` gets availability for a whole batch in one request (not one `rawdir` each).
- Net: ~**2 requests per ~300 entries** (~47k total) vs **14.2M**. Fetch becomes **bandwidth-bound**
  (~1–6.5 TB), and a few concurrent long-lived batch streams (`--workers`) multiply bandwidth
  *without* tripping the req/s limiter (safe, unlike per-entry concurrency) → **~days, not months**;
  feasible inside two weeks. NOMAD's server-side throughput is now the remaining governor (heavy and
  variable during testing, ~1.5 MB/s/stream), NOT the request rate. This is the one place emailing
  `support@nomad-lab.eu` helps — not as a required step, but to *request a rate-limit exemption* (the
  documented purpose of contacting them; §2) if the throttle makes two weeks tight.
- **Coverage/quality unchanged — ALL ~7.1M direct uploads are covered.** Every VASP entry's
  mainfile is either a `vasprun.xml` or an `OUTCAR` (measured live 2026-08-14 on a 1000-entry
  keyspace-spread sample: **92.5% vasprun-mainfile, 7.5% OUTCAR-mainfile, 0% neither**), and we
  fetch **exactly that mainfile** — so the ~7.5% OUTCAR-only entries (no vasprun in the upload)
  are kept via their OUTCAR, and the ~92.5% vasprun entries pull only the vasprun (not the OUTCAR
  beside it, if any — that is the transfer-size saving). The FULL vasprun/OUTCAR is fetched, so the
  enhanced parser recovers full `calc_parameters` (run_type, INCAR, resolved `parameters`, k-points,
  POTCAR — confirmed on live NOMAD vaspruns; the OUTCAR path recovers the same from the header). Any
  entry a zip can't deliver **falls back to the per-entry path**, and availability is recovered
  per-entry if a batch `rawdir/query` fails — so no coverage or metadata is lost. The per-entry
  `fetch_candidates` remains as `--per-entry`. Parse is overlapped with fetch in `pipeline`; peak
  *staging* disk stays bounded by the valve.
  **One consequence of vasprun-only (mentor-agreed):** per-atom DFT **charges/spins**
  (`dft_charge`/`dft_magmom`) come only from an OUTCAR, so they are recorded only for the ~7.5%
  OUTCAR-mainfile entries (or any run with `--want-outcar`). For the ~92.5% vasprun entries they are
  absent — but this is the "if available" clause of the storage spec, and the **availability** of
  DOS/eigenvalues (from NOMAD's parsed `available_properties`, the authoritative primary source)
  and of magnetization/charge-density/spin-density (from the rawdir file listing + ISPIN, plus the
  shared parser's embedded-vasprun probe for DOS/eigen/projected) is still recorded for every entry.
  Wholesale OUTCAR is ~170 TB (§5), which is why it is excluded by default.

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
7. **Self-throttle + 5xx backoff** — ~30 req/s, ≤10 concurrent (per IP), no `Retry-After`; **502/503/504 all appear under load** (seen repeatedly in the smoke). The client backs off exponentially. Contacting support is **not** a documented prerequisite (§2) — do it only to request a rate-limit *exemption* if throughput is the bottleneck.
14. **`rawdir/query` (the availability listing) is fragile AND slow — this is the availability step, NOT the download.** Live-verified 2026-08-16: a bulk `POST /entries/rawdir/query` with a big `entry_id:any` list **500s** (server-side timeout) — n≈300 reliably fails, n≈50 is borderline under load; the **`raw/query` DOWNLOAD endpoint handles 300 fine**. So `client.bulk_rawdir` splits the listing into small `RAWDIR_CHUNK` (=25) sub-requests, decoupled from the download batch size, and is **resilient** (a failed sub-request doesn't discard the good ones — those entries recover via a per-entry `GET /rawdir`). Measured ~**0.2 s/entry** for the listing, so at 7.1M it can **rival the download in wall-clock** and may govern the run (workers parallelise it; it overlaps parse). It supplies only the **charge_density** (→ spin_density) availability flag — DOS/eigenvalues come from `available_properties`, magnetization from ISPIN, none needing rawdir — so if it proves the bottleneck, raising `RAWDIR_CHUNK` (cautiously; it 500s when too big) or trimming heavy-file availability is the lever. Measure it on CSD3 with `csd3_nomad_speed.py` section [3].
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
- **Phase 1 — scoped discover + fetch — ✅ BUILT.** `python -m nomad_harvest.cli discover
  --max-entries N [--elements …] --out data/manifests/nomad_keep.jsonl`, then either the
  overlapped `pipeline` (below) or `… fetch --in nomad_keep.jsonl` + the shared parse/verify.
- **Phase 2 — dedup review.** discover already dedups against the Zenodo `metadata.jsonl`
  (`--zenodo-metadata`, default path); review `nomad_rejections.jsonl` with the mentor.
- **Phase 3 — scale on CSD3 — ✅ BUILT (2026-08-11).** The `source` calc_id param is done (§7);
  the fetch is disk/inode-valve-paced with `--workers`; `nomad_harvest.cli pipeline` runs the
  overlapped fetch∥parse+purge disk-paced, and `scripts/csd3/nomad/{10_discover,20_pipeline}.sh`
  wrap it as self-resubmitting SLURM jobs (mirroring the Zenodo templates). `merge-datasets`
  folds the NOMAD dataset (`data/dataset/nomad`) into the combined one. Optionally email
  `support@nomad-lab.eu` to request a rate-limit exemption for a multi-million-entry pull
  (not required — see §2).

**Run it (bounded sample).** NOMAD writes to its OWN tree (`$NOMAD_HARVEST_DATA`, default a
sibling `/rds/user/$USER/hpc-work/nomad`), kept fully separate from the Zenodo tree; the CLI
defaults resolve there, so no `--raw-dir`/`--dataset-dir` are needed. The only Zenodo path read
is `dataset/metadata.jsonl` for cross-source dedup.
```bash
export NOMAD_HARVEST_DATA=/rds/user/$USER/hpc-work/nomad     # NOMAD's own root (sibling of zenodo/)
python -m nomad_harvest.cli discover --max-entries 200000    # -> $NOMAD_HARVEST_DATA/manifests/nomad_keep.jsonl
python -m nomad_harvest.cli pipeline --in $NOMAD_HARVEST_DATA/manifests/nomad_keep.jsonl \
    --parts 40 --workers 4 \
    --max-disk-bytes 600000000000 --max-disk-files 600000 --max-primary-bytes 2000000000
python -m nomad_harvest.cli status                           # read-only progress (NOMAD-aware)
python -m zenodo_harvest.cli verify --dataset-dir $NOMAD_HARVEST_DATA/dataset
```
Before sizing `--workers`, measure real bandwidth from a compute node with
`scripts/csd3/nomad/csd3_nomad_speed.py` (it times the BULK-zip path and extrapolates the
full-harvest transfer time). Disk budget when co-running with Zenodo jobs: the valve bounds only
its own raw dir, so give each job a **fixed slice** summing under the shared 1 TB / 1M-inode quota
(NOMAD 600 GB/600k + a Zenodo recovery 150 GB/150k + ~100 GB existing + headroom) — see
`scripts/csd3/nomad/README.md`.

## 10. Open questions for the mentor

- Target **size / element coverage** for the first scoped NOMAD slice? (7.1M is too big for one pass.)
- Also fetch **OUTCAR** (per-atom charges/spins) for the subset where it's small, or **vasprun-only** everywhere? (Wholesale OUTCAR ≈ 170 TB — see §5.)
- Add the `source` calc_id param to the shared parser now, or keep NOMAD in its **own** dataset dir until an explicit merge?

*(Resolved 2026-08-10: scope = direct uploads only; MP via `mp-api` not NOMAD; raw re-parse, not the normalized archive.)*
*(Resolved 2026-08-11: first slice = a **bounded sample** via `--max-entries` (default 200k in the CSD3 script; a keyset scan spreads it across uploads); **vasprun-only** everywhere (OUTCAR excluded — its 412 MB tail is ~170 TB wholesale); the **`source` param was added** to the shared parser now — backward-compatible, byte-identical for Zenodo — so NOMAD namespaces `nomad:…` and can merge cleanly.)*

## 11. References (official)

- API how-to: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/api.html
- Auth: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/auth.html
- ArchiveQuery: https://nomad-lab.eu/prod/v1/docs/howto/manage/program/archive_query.html
- OpenAPI: https://nomad-lab.eu/prod/v1/api/v1/openapi.json · Swagger: https://nomad-lab.eu/prod/v1/api/v1/extensions/docs
- Terms / licence: https://nomad-lab.eu/nomad-lab/terms.html · FAQ: https://nomad-lab.eu/nomad-lab/faqs.html
- OPTIMADE: https://nomad-lab.eu/prod/v1/optimade/v1 · provider `nmd`: https://providers.optimade.org/index-metadbs/nmd

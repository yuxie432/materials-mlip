# External data sources — evaluation & harvest plans (Sep 2026)

Companion to `HARVEST_RESULT.md` (Zenodo) and `NOMAD_HARVEST_RESULT.md` (NOMAD). This note
records the research into **other open DFT databases** that could extend the assembled MLIP
training set beyond the completed Zenodo + NOMAD harvests, a concrete **harvest plan for
Materials Cloud**, and an analysis of **non-VASP computational codes**. It is written for the
next building phase (another agent may implement from it).

> **Update 2026-09-23:** the Materials Cloud harvest (§5) is now BUILT — `materials_cloud_harvest/`,
> designed from a full live census that corrected several §5 numbers; see
> **`docs/MATERIALS_CLOUD_HARVEST.md`** (authoritative; §5 below is kept as the original plan).
>
> Status: evaluation complete; nothing new harvested yet. Numbers marked *measured* were
> obtained live from the databases' APIs during this survey (2026-09-20/21); a few are flagged
> as estimates. The multi-code section (§6) is being finalised with in-progress research.

## Baseline (what we already have)

| Source | records / calcs | frames | notes |
|---|---|---|---|
| **Zenodo** | ~303 records / 182,111 calcs | 12,088,722 | long tail, individual deposits, raw VASP re-parsed |
| **NOMAD** (direct uploads) | 7,073,592 calcs | 52,459,065 | individual uploads, VASP-only, raw re-parsed |

Both are stored in the shared schema (`extxyz.gz` + `metadata.jsonl`, MACE keys
`REF_energy`/`REF_forces`/`REF_stress`, full `calc_parameters`, per-frame convergence, net
moment/charge). Source-namespaced calc_ids (`zenodo:…`, `nomad:…`) so a `merge-datasets` never
collides.

## Objective & inclusion criteria (mentor + user, 2026-09-20)

The goal is **not** maximum size — it is to **fill the gap** left by the mainstream MLIP training
sets (Materials Project, OMat24, Alexandria) by assembling **useful DFT data that is spread across
the web but not yet used for MLIP training** (because it is troublesome to assemble — which is the
value this pipeline adds). A source is worth harvesting only if it is:

1. **Raw or at least fully traceable/provenanced** (ideally raw `vasprun.xml`/`OUTCAR` re-parseable to per-ionic-step energy + **forces** + stress).
2. **Uploaded by individual researchers/labs** — heterogeneous, diverse — *not* homogeneous institutional high-throughput.
3. **Not easily downloadable as one package/API call**, *and* **not already widely used for MLIP training**.

Explicitly **excluded**: (a) processed datasets (lack the provenance), (b) homogeneous
institution-mass-calculated databases, (c) easily-accessible sets already used for MLIP training.

---

## 1. Two categories of source (the framing that drives every verdict)

- **Aggregator platforms — Zenodo, NOMAD.** Heterogeneous, multi-depositor raw output, not
  available in any single convenient form. The pipeline's *discovery* + *raw re-parse* value is
  **high**. This is the project's genuine niche. **Done.**
- **Curated institutional databases — Materials Project, OQMD, AFLOW, Alexandria, OMat24, GNoME,
  JARVIS, OC20.** Each is already one coherent, consistently-computed, downloadable product.
  Discovery value ≈ 0, and they ship *processed* labels (no per-calc INCAR/POTCAR), so re-parse
  value ≈ 0 too. **"Buy, don't build."** They are self-sufficient standalone training sets
  (MPtrj-only is the Matbench "compliant" tier; OMat24 is SOTA); the Zenodo+NOMAD aggregation is
  the **complementary long tail**, not the backbone.

---

## 2. Institutional databases — evaluated (do NOT re-harvest into this set)

The decisive filter for MLIP is **per-step forces + off-equilibrium trajectories**. All sizes/licences *measured or paper-sourced*.

| Database | VASP? | forces + off-eq trajectories? | ships raw vasprun/OUTCAR? | functional / energy ref | size (MLIP frames) | download | licence | verdict |
|---|---|---|---|---|---|---|---|---|
| **OMat24** | ✅ | ✅ rattled + AIMD single-points | ❌ aselmdb (processed) | PBE(+U), POTCAR 54 — **separate ref** (~13.5 meV/atom vs MP2020, up to 52 in WBM cmp) | ~100M | ~195 GB | CC-BY-4.0 | buy-not-build (top external set) |
| **Alexandria** | ✅ | ✅ full relaxation paths | ❌ json.bz2 / aselmdb | PBE (+PBEsol/SCAN) — ~MP-compatible | ~30M | 105 GB json.bz2 / **15.5 GB aselmdb** | CC-BY-4.0 | buy-not-build |
| **LeMat-Traj** (MP+Alex+OQMD unified) | ✅ | ✅ harmonised + deduped | ❌ parquet | PBE 113M / PBEsol 7.3M / r2SCAN 0.5M / SCAN 0.17M | 121M | ~85 GB | CC-BY-4.0 | buy-not-build (shortcut for all three) |
| **MatPES** | ✅ | ✅ single-point statics (DIRECT-sampled) | ❌ JSON | **PBE + r2SCAN** (clean, separable) | 0.5M | ~1–3 GB | BSD-3 | buy-not-build (adds r2SCAN) |
| **MPtrj** | ✅ | ✅ filtered relaxation frames | ❌ JSON | PBE/PBE+U (MP2020) | 1.58M | 12 GB | MIT / CC-BY | ⊂ MP; the standard baseline |
| **Materials Project** (`mp-api`) | ✅ | ✅ per-step via `TaskDoc.calcs_reversed[].ionic_steps` (parsed) | ⚠️ mostly parsed JSON | PBE/GGA+U + r2SCAN | ~1.37M tasks | API @ 25 req/s | CC-BY-4.0 | MPtrj/LeMat-Traj already package it |
| **JARVIS-DFT** | ✅ | `alignn_ff_db` 307k F+stress; `raw_files` **have vasprun** (144,895) | ✅ (figshare raw_files) | **OptB88vdW** (separate ref) | ~307k | 40 MB JSON; raw large | CC-BY-4.0 | institutional + already-MLIP; distinct functional only |
| **OQMD** | ✅ | ❌ **final structures only** (no trajectories, no raw distributed) | ❌ | PBE+U | — | 21 GB SQL | CC-BY-4.0 | **rule out** (no forces to speak of) |
| **AFLOW** | ✅ | ⚠️ trajectories only inside raw `OUTCAR.relax.xz`; **no bulk dump** (~40 TB per-entry tree) | OUTCAR only | PBE | ~3.5M entries | none | **non-commercial** | **rule out** (licence + no dump + homogeneous) |
| **GNoME** | ✅ | ❌ public release = structures + energies only | ❌ | PBE | — | CIF/CSV | **CC-BY-NC** | **rule out** |
| **OC20 / OC22** | ✅ | ✅ forces + trajectories, **no stress** | ❌ extxyz/lmdb | **RPBE** / PBE+U (surfaces) | 1.28M / 62k relaxations | 225 / 20 GB | CC-BY-4.0 | only if catalysis/surfaces in scope |

**Key cross-cutting rule:** these are *different energy references* — never mix absolute energies
across them. Each stays its own consistency group (the existing `potcar_set_hash` + functional
provenance is the right instrument). If any is ever wanted, the cheapest correct path is a
processed-label ingest adapter (one ASE-LMDB reader covers OMat24 + Alexandria; one parquet reader
covers LeMat-Traj), **not** the raw pipeline — and for most, LeMat-Traj already unifies MP+Alex+OQMD.

---

## 3. NOMAD ↔ AFLOW / OQMD / MP (measured from NOMAD's aggregation API)

NOMAD total = **19,415,315** entries; **14,755,697 VASP**. By `external_db` (bulk-mirrored DBs):

| population in NOMAD | entries | VASP entries |
|---|---|---|
| AFLOW (external mirror) | 6,788,692 | 6,788,660 |
| OQMD (external mirror) | 623,129 | 568,645 |
| Materials Project (external mirror) | 282,648 | 280,496 |
| **direct uploads (VASP, no external_db)** | **~7.12M** | **= our harvest (7.07M)** |

So the NOMAD harvest cleanly took the **individual-researcher direct-upload VASP** and left the
institutional mirrors. AFLOW/OQMD are homogeneous institutional (Curtarolo/Duke, Wolverton/NW) →
correctly excluded. NOMAD *does* retain their raw files (an AFLOW entry carries `vasprun.xml.relax1/2`;
OQMD entries carry `OUTCAR`), so harvesting them via NOMAD would be technically easy — but they fail
criterion (2). Not worth it.

---

## 4. Long-tail repositories (the "other Zenodo-likes") — evaluated

Counts are **metadata-text matches** (each platform's `q`/DataCite `query`), the same method as
Zenodo's `q`: a lower bound that also includes text-only false positives and (on Mendeley)
version-DOI doubling. Refined by inspecting records/files/creators/related-identifiers.

| Repository | total | VASP-metadata | genuine raw-VASP records | nature | licence | Zenodo overlap | harvest API | verdict |
|---|---|---|---|---|---|---|---|---|
| **Materials Cloud Archive** | 1,239 | 44 | ~30–35 | 39 individual / 5 institutional; **raw + AiiDA provenance** | CC-BY | 1/44 | InvenioRDM, files listable, Range works | **YES — cheap, see §5** |
| **Mendeley Data** | — | 226 → **~49 unique** | ~15–25 (rest processed `.extxyz/.mtp` or figure `.docx`) | individual (self-deposit) | mostly CC-BY | 0/40 | public API lists files | Yes — small |
| **ScienceDB** (Science Data Bank, CAS) | — | 129 → **120 anchored** | unconfirmed | individual **Chinese** catalysis/2D groups (diverse) | mostly CC-BY | 0/40 | **file API undocumented + connectivity flaky** | **Maybe — needs feasibility spike** (biggest at ~2.67 TB, riskiest) |
| **Dryad** | — | 33 | ~9 real materials | individual (skews bio) | CC0/CC-BY | 0 | native API | small |
| **DaRUS** (Stuttgart Dataverse) | — | 16 | ~16 | individual modelling groups; raw OUTCARs in `.tar.gz` | CC-BY | 0 | Dataverse API | small |
| **figshare** | huge | many | scattered | **big hits institutional** (ORNL etc.); individual tail scattered | mixed | — | API listable | marginal |
| B2SHARE · RADAR · CaltechDATA · Harvard Dataverse | 12k / 6.6k / 77k / — | ~0 | ~0 | no materials VASP | — | — | — | **No** |
| Catalysis-Hub | — | ~80k traj | — | multi-group **but processed ASE-db, QE/GPAW, already-MLIP** (CatBench) | — | — | GraphQL | marginal/no |
| 2DMatpedia · MPContribs · NRELMatDB · CMR/C2DB | — | — | — | institutional / processed / GPAW (not VASP) | — | — | — | **No** |
| **MDF** (Materials Data Facility) | 1.8M indexed | — | — | publish+index service | — | — | `mdf-forge`+Globus | use as a **discovery layer** |
| **ColabFit Exchange** | 504 datasets | — | — | **MLIP-purpose, manually curated** (hand-picks some Zenodo/figshare) | — | — | — | the "already-packaged" map — diff against it |

**Findings.** (i) There is **no second Zenodo/NOMAD-scale source.** The remaining individual-researcher
raw-VASP long tail is scattered across ~6 repos (~a few hundred VASP-metadata records total → ~100–250
likely-yielding). (ii) **DataCite REST API is a single unifying discovery layer** across all of them
(Mendeley/Dryad/ScienceDB/DaRUS/figshare/OSF/Zenodo all mint DataCite DOIs — verified). A DataCite
census showed **135 of 200 OUTCAR-mentioning DOIs are already-harvested Zenodo**, the rest a thin
scatter. (iii) The niche of a *systematic* raw-provenance long-tail sweep is **largely open** —
ColabFit is the only partial occupant and it curates by hand. (iv) Deposit-level overlap with
Zenodo/NOMAD is ~nil (distinct DOI namespaces); precise physics-level dedup is a training-time step.

**Recommended shape (direction #2):** one **DataCite + OpenAlex→data-DOI** discovery front-end feeding
per-repo file adapters, rather than bespoke harvesters. Cheap first wins: Materials Cloud + Mendeley.

---

## 5. Materials Cloud — harvest plan (for the building phase)

> **Superseded by `docs/MATERIALS_CLOUD_HARVEST.md` (2026-09-23).** A full census of all 1,241 records
> corrected this plan: VASP-mentioning records are ~84 GB (+ Bosoni's 3.1 GB VASP AiiDA exports), not
> 160 GB; `.aiida` is NOT a skippable sidecar (Bosoni's VASP lives only in legacy AiiDA exports, which
> are now extracted); discovery enumerates every record instead of relying on `q=VASP`; downloads are a
> 302 to presigned S3; `mcloud-ne-1.0`/`asl` must be gated explicitly; Bosoni's VASP subset is included.

Materials Cloud Archive runs **InvenioRDM — the same platform family as Zenodo** — which is why ~80%
of the pipeline reuses.

### 5.1 API access (measured)
- **Discover:** `GET https://archive.materialscloud.org/api/records?q=<query>&size=N` → hits with
  `metadata` (title/creators/rights/related_identifiers) **and `files.entries` inline** (filename,
  size, **md5 checksum**, ext, mimetype). File listings are free (no download) → triage is free.
- **Download:** `GET /api/records/{id}/files/{key}/content` — **HTTP Range works (verified 206)**,
  no `Accept-Ranges` header (the *same quirk the pipeline already handles for Zenodo*), so resumable
  downloads and targeted-zip member fetch both work. md5 checksums provided → verification reuses.
- **Rate limit:** `X-RateLimit-Limit: 500`/window (generous). EU/EPFL server (reliable from CSD3;
  only intermittent SSL latency seen in the survey sandbox — reuse the existing 429/5xx retry).

### 5.2 How the record count was derived, and query reuse (answers a specific question)
- The "**44 VASP records**" = `q=VASP` total on the Invenio API (metadata-text, exactly Zenodo's `q`).
- **The full Zenodo `DEFAULT_QUERIES` set is reusable verbatim** (same Invenio `q` engine). But
  *measured on MC it does not raise the VASP count*: the VASP-filename net
  (`VASP OR vasprun OR OUTCAR OR POSCAR OR INCAR OR …`) = **44**, identical to `q=VASP`. MC is small
  (1,239 records) and its VASP records all carry "VASP" in metadata — there is no sparse-metadata
  archive tail as on Zenodo. So **~44 is the VASP ceiling on MC regardless of query breadth.**
- The broad DFT-materials recall query returns **736** records — but MC is **QE/AiiDA-dominant**, so
  the extra ~700 are overwhelmingly **Quantum ESPRESSO / other-code** (exact-phrase `"Quantum
  ESPRESSO"` = 52; CP2K = 15; FHI-aims/GPAW ≈ 1 each). Those matter only if going multi-code (§6).
- **Recommendation:** reuse Zenodo's query set + the licence/access gates + `models.classify_files`
  unchanged (consistency + it costs nothing), but expect the *VASP* yield to be ~30–35 records.

### 5.3 Pipeline reuse

| Stage | Reuse | New work |
|---|---|---|
| **discover** | — | Small MC InvenioRDM backend: run the query set → dedup by `parent.id` → `Candidate` manifest. **No date-bisection** (1,239 ≪ 10k window). |
| **triage** | `models.classify_files` **as-is**; `zipstream` central-dir peek **reuses for the 17/44 records that contain a `.zip`** (Range confirmed) | ~40 lines glue to feed MC file listings in |
| **fetch** | **core reuses**: `download_file` (Range/resume/**md5**), `StagingBudget`, selective VASP extraction, targeted-zip (`zipstream`), nested-archive recursion | MC manifest adapter (records → `fetched.jsonl` with MC content URLs) + add `.aiida` to the skip-list |
| **parse / store / verify / merge** | **fully reuse, 0 changes** | set `source="materials_cloud"` → calc_ids `materials_cloud:<recid>:…` (parse already supports the source param) |

Cleanest shape: a **`materials_cloud_harvest/` mini-package mirroring `nomad_harvest/`** (which already
reused stages 3–5 unchanged).

### 5.4 Data profile (measured over the 44 VASP records)
- **159.8 GB** total; **42/44 harvestable without AiiDA** (0 records are aiida-only; the 66 GB of
  `.aiida` files are provenance sidecars beside plain archives → skip them, **no AiiDA dependency**).
- **17/44 contain a `.zip`** (targeted-fetch/peek eligible); 27/44 are `.tar.gz`/`.tar`/`.7z` (whole
  download + extract — fine at this scale). md5 checksums on all files.
- **39/44 individual**, 5 institutional-signal. Only **1/44 references Zenodo** (overlap ~nil).
- **Richest provenance of any source** — full AiiDA provenance graphs alongside the raw files.
- Two giants dominate bytes: **Bosoni 75 GB** (a multi-code *precision-verification* archive — mostly
  non-VASP; low VASP value → **exclude by ID**) and **Kristoffersen 44 GB**. Peeked a 102 MB zip
  record ≈ 11 `vasprun` files (~11 calcs) — a size anchor.

### 5.5 Difficulties / bottlenecks
1. 27/44 records are non-zip → no peek/targeted-fetch (whole download+extract; small totals, fine).
2. Two giants dominate transfer (exclude the 75 GB Bosoni multi-code verification record).
3. `.aiida` sidecars (66 GB) → add `.aiida` to the archive skip-list (no data lost, no AiiDA dep).
4. ~9–14 records are processed MLIP / figures / input-only → parse yields nothing (precision gate handles it).
5. Metadata blind spot: `q=VASP`=44 may miss sparse-metadata VASP, but the DataCite census (MC = 2 of 200 OUTCAR-DOIs) says MC's discoverable VASP is genuinely small.
6. A few NC/ND-licensed records → the licence gate drops them.

### 5.6 Effort, time, and yield
- **Code change: ~500–700 lines new + ~200 test lines (~2–4 days).** Breakdown: MC client ~120,
  discover ~120, fetch-adapter ~150, triage glue ~40, CLI ~80, offline tests ~200 (mirror
  `test_nomad.py`), + one CSD3 batch script. Stages 3–5 + fetch core + triage classifier + `zipstream`
  all reuse.
- **Harvest job: ~2–4 hours, ONE CSD3 job** (fits 12 h SL3), single node. Transfer ~80–160 GB at
  ~30–66 MB/s ≈ 30–90 min; parse ~30 min–2 h (use `icelake-himem` + `--max-primary-bytes`). Inodes
  ≪ 1M (44 records); final extxyz a few GB. **No inode wall.**
- **Data yield (estimate): ~30–35 raw records → ~2,000–10,000 calcs / ~10⁵–10⁶ frames** (central
  ~100k–500k; heavy-tailed). Small vs NOMAD (52M) / Zenodo (12M) — a completeness contribution;
  exact number only from the run.

### 5.7 Verdict + implementation checklist
**Worth doing as a cheap, provenance-rich completeness sweep** (individual, richest provenance, CC-BY,
~0 overlap, novel chemistries). Its InvenioRDM adapter **generalises** to CaltechDATA/B2SHARE and is
the first component of the direction-#2 DataCite layer. Skip if only scale matters.

Checklist for the building agent:
- [ ] `materials_cloud_harvest/client.py` — InvenioRDM search + record/files + `content` download URL + Range-aware GET (crib from `zenodo_harvest/client.py` + `nomad_harvest/client.py`).
- [ ] `materials_cloud_harvest/discover.py` — reuse Zenodo `DEFAULT_QUERIES` + gates; dedup by `parent.id`; emit `Candidate` manifest.
- [ ] triage: call `zenodo_harvest.models.classify_files` + `zipstream` peek for zip records.
- [ ] fetch adapter: build `fetched.jsonl` with MC content URLs + md5; reuse `zenodo_harvest.fetch`; add `.aiida` to the skip-list; exclude record `Bosoni 75 GB` by ID.
- [ ] parse/store/verify: reuse unchanged; `source="materials_cloud"`.
- [ ] offline tests mirroring `tests/test_nomad.py`; one `scripts/csd3/` batch script.

---

## 6. Other computational codes (multi-code) — analysis

*(Quantitative core measured live from the NOMAD/Materials Cloud APIs; code capabilities from established DFT-code facts.)*

### 6.0 Field usage and role in MLIP training (the field-wide picture)

**Field-wide code prevalence in computational materials (periodic solids), best-first:**
1. **VASP** — #1 by a wide margin; commercial (paid license, *not* open-source), the default wherever a
   group can afford it (US / UK / East-Asia production groups), and the backbone of *every* high-throughput
   screening database that feeds materials foundation models (MP, OQMD, AFLOW, JARVIS, Alexandria, GNoME,
   OMat24, MatPES). Its Kresse method papers are among the most-cited in the physical sciences.
2. **Quantum ESPRESSO** — clear #2; free/open (GPL); the AiiDA / Materials Cloud high-throughput ecosystem
   (EPFL/Marzari); strongest in continental Europe and wherever VASP licensing is a barrier.
3. **CP2K** — #3 by footprint but *disproportionately important* for one workload: efficient AIMD
   (Gaussian-and-plane-wave; hybrids via ADMM) of **liquids / interfaces / electrochemistry / disordered
   systems** — exactly the finite-temperature, off-equilibrium regime the VASP relaxation-screening
   databases under-sample. Open/GPL; European-led.
Then **CASTEP** (UK commercial; phonons/NMR), **FHI-aims** (all-electron numeric-orbital; high accuracy for
organics/hybrids/vdW; FHI-Berlin, tightly coupled to NOMAD), **GPAW** (real-space PAW, ASE-native; Nordic/C2DB),
**ABINIT** (DFPT/phonons), **WIEN2k** (all-electron reference/benchmark), **exciting/CRYSTAL/SIESTA** (niche).
*(No hard % market-share survey was retrievable this session; the qualitative ranking is well supported by the HT-database pattern.)*

**Role in MLIP training — the decisive facts:**
- **All materials foundation-model training sets are VASP** (MPtrj, OMat24, Alexandria, GNoME, MatPES) — a
  direct consequence of the above (the automated screening stack — pymatgen/atomate/custodian — was built
  VASP-first). OMat24's VASP provenance was confirmed from its paper.
- **Non-VASP MLIP data is niche but real, and code-specific:** **CP2K** is *the* standard source for
  liquid/water/interface potentials (e.g. Cheng et al., *PNAS* 2019, revPBE0-D3 AIMD); **QE** underlies the
  AiiDA/Materials-Cloud ML sets and **PET-MAD** (§6.2 — the one non-VASP *foundation-scale* materials MLIP
  set); **FHI-aims** gives all-electron accuracy for organic/hybrid systems; **GPAW** underlies C2DB
  (property screening, not force trajectories).
- **Molecular MLIP datasets are a different domain** — SPICE / MACE-OFF23 (Psi4, ωB97M), OMol25 (ORCA,
  ωB97M-V), ANI / QM9 (Gaussian), Transition1x (ORCA) — all *quantum-chemistry* codes on isolated molecules
  (no periodicity, no stress tensor). They would **not** extend a *materials* (periodic) MLIP; using them
  needs a structurally different (or multi-task) model.
- **Multi-code / multi-fidelity training** is emerging but not mainstream. Clean exemplar: **DPA-2**
  (arXiv:2312.15492) — one multi-task model over VASP + ABACUS + Gaussian + CP2K, which states plainly that
  a VASP set and a Gaussian set "cannot be concurrently trained" without its machinery. MatPES's PBE+r2SCAN
  is multi-*fidelity* but single-*code*.

### 6.1 Where non-VASP materials data lives (NOMAD, measured by code)
NOMAD entries by simulation `program_name` (the `external_db` mirrors are VASP-only, so non-VASP
totals ≈ the individual-upload multi-code pool):

| code | NOMAD entries | domain | basis / type | licence |
|---|---|---|---|---|
| **VASP** | 14,755,697 | periodic materials | plane-wave PAW | commercial |
| **FHI-aims** | **1,384,722** | periodic + molecular | all-electron numeric atomic orbitals | free-for-academia |
| Quantum Espresso | 133,956 | periodic materials | plane-wave pseudopotential | **open (GPL)** |
| Octopus | 107,866 | real-time TDDFT | real-space grid | open |
| exciting | 34,713 | periodic materials | all-electron (L)APW+lo | open |
| ABINIT | 18,826 | periodic materials | plane-wave pseudopotential | open |
| Crystal | 12,619 | periodic materials | Gaussian-type orbitals | commercial |
| GPAW | 9,475 | periodic materials | PAW (grid/PW/LCAO) | open |
| CASTEP | 6,222 | periodic materials | plane-wave pseudopotential | commercial (UK-academic-free) |
| CP2K | 5,473 | periodic + large-scale/AIMD | mixed Gaussian + plane-wave | open |
| *(molecular: Gaussian 2.2M · ORCA 99k · NWChem 2.5k)* | | **molecules, not periodic** | Gaussian-type | mixed |

So NOMAD's **non-VASP periodic-materials pool ≈ 1.6M calcs, dominated by FHI-aims (1.38M)**, then
QE (134k), exciting (35k), ABINIT (19k), Crystal/GPAW/CASTEP/CP2K (5–13k each) — **all already
API-accessible via the existing NOMAD pipeline**. The 2.3M Gaussian/ORCA are molecular (a different
domain — molecular MLIPs like OMol25, out of scope for a periodic-materials MLIP).

### 6.2 Where non-VASP data lives (elsewhere)
- **Materials Cloud** is QE-dominant (the AiiDA/QE flagship): its *individual* Archive VASP subset is
  ~44 (§5) but the broad materials pool (736) is mostly QE/CP2K; and its curated institutional QE DBs
  (MC3D ~72,589 structures [QE+SIRIUS]; MAD 95,595 [QE/PBEsol]; MC2D ~a few thousand) are "buy-not-build" institutional products.
- **PET-MAD / MAD** (Materials Cloud Archive; COSMO/EPFL; QE-PBEsol) — the **one non-VASP materials MLIP
  set of note**: ~95,600 structures (MAD record 17.6 GB; MC3D record ~102 GB) deliberately sampled for *diversity* (far-from-equilibrium, distorted,
  high-energy, surfaces/defects), **with forces + stress**, released ML-ready (extxyz-style — "MPtrj but
  QE-based") to train the PET-MAD foundation potential. Presumed CC-BY (confirm on the record). **Easiest
  non-VASP ingest** — one archive download, no discovery/parse pipeline — but it is a *processed,
  institutional, purpose-built* set (buy-not-build), so its value is as a ready complement at training
  time (its own reference group), not a raw harvest. *(Exact size/license/route flagged uncertain — the
  live dashboards are JS apps that weren't retrievable this session.)*
- **CMR / C2DB (DTU)** = GPAW, processed ASE-db, institutional, and — decisively — **property-screening,
  not force/trajectory data** (final relaxed structures + band gaps/magnetism/optics) → wrong data shape
  for MLIP force labels → out.
- Individual-researcher QE/CP2K long tail exists in Zenodo/figshare/Materials Cloud Archive but is
  small and scattered (same DataCite-unified discovery applies).

### 6.3 Code features, MLIP suitability, and cross-code consistency
**Forces + stress + parser support** (what MLIP training needs, per code — from established code capabilities):

| code | E + forces | stress | ASE reader (`ase.io`) | pymatgen | MLIP notes |
|---|---|---|---|---|---|
| VASP | ✅ | ✅ | vasprun / OUTCAR | ✅ primary | the pipeline's native path |
| Quantum ESPRESSO | ✅ (`tprnfor`) | ✅ (`tstress`) | `espresso-out` | ✅ (`io.pwscf`) | open; periodic; forces+stress standard |
| FHI-aims | ✅ | ✅ (analytical, periodic) | `aims-output` | partial | all-electron (arguably *higher* fidelity than PP-VASP); free-academic |
| CP2K | ✅ | ✅ (`STRESS_TENSOR ANALYTICAL`) | `cp2k-out` | `io.cp2k` | strong for large-cell AIMD / liquids |
| GPAW | ✅ | ✅ (PW mode) | ASE-native | — | ASE-native; C2DB uses it |
| CASTEP | ✅ | ✅ | `castep` | — | commercial (UK-academic-free) |
| ABINIT | ✅ | ✅ | `abinit-out` | partial | open; periodic |
| exciting | ✅ | ⚠️ limited (all-electron stress is hard) | `exciting` | — | all-electron |
| CRYSTAL | ✅ | ✅ (cell gradients) | `crystal` | — | Gaussian-basis periodic; commercial |
| WIEN2k | ✅ | ⚠️ historically unavailable / very limited (AE-FLAPW) | partial | — | weak for stress labels |
| Octopus | ✅ (ground state) | ⚠️ | `octopus` | — | mostly real-time TDDFT in NOMAD → not standard PES |
| *(Gaussian / ORCA / NWChem / Psi4)* | ✅ (gradients) | ❌ molecular (non-periodic) | various | — | molecular domain (OMol25 / ANI / SPICE / QM9) |

So **QE, FHI-aims, CP2K, GPAW, ABINIT, CASTEP** all emit energy + forces + stress and have ASE
readers → parseable to the frame schema via the same ASE path the pipeline already uses for OUTCAR.
**WIEN2k / exciting** are weak on stress (all-electron). **Octopus** is TDDFT-focused (not a standard
ground-state PES). **Molecular codes** give no stress and are a different (molecular) domain.

**The central caveat (holds regardless):** absolute DFT energies are **not comparable across codes**
even at the same functional (different pseudopotentials/basis-set completeness/all-electron-vs-PP and
total-energy zeroes). So multi-code data cannot be pooled into one energy reference — each code must
be its own **fidelity/reference group** (the same discipline already applied per functional +
`potcar_set_hash`). Forces and stress are more transferable than energies, but the standard practice
in 2024–2026 foundation MLIPs is per-reference handling (separate heads / energy shifts / grouping),
not naive pooling. So multi-code data is a **diversity asset only if kept in its own group** — it does
not enlarge the VASP reference.

**Cross-code handling in practice:** the best-evidenced mechanism is DPA-2's — a shared descriptor
backbone trained across all datasets + a **separate fitting head per dataset** + **per-dataset,
per-element energy-bias least-squares** to absorb code-to-code offsets. Per-element isolated-atom
reference fitting is already near-universal (MACE / NequIP / M3GNet / CHGNet); DPA-2 extends it per-dataset.
Delta-learning and structure-matched offset calibration are the alternatives; the conservative default
(what this project does today) is simply not mixing energy scales — single-code models. Even *within* VASP,
OMat24 reports a 52 meV/atom shift from a pseudopotential-version change alone, so cross-code gaps are
larger still.

**Parser tooling maturity (drives which code to add first):** **FHI-aims** — pymatgen `io.aims` + ASE
`ase.io.aims` mature (forces/stress) → best tractability *and* the largest pool. **QE** — ASE
`ase.io.espresso` + pymatgen QE I/O mature → second. **CP2K** — pymatgen `io.cp2k` exists but less
battle-tested; small pool. **GPAW** — **no portable text output** (binary `.gpw`/`.traj`, needs the GPAW
package to deserialize) → hardest, smallest pool, and C2DB is property-screening → lowest priority. Note the
current `triage.py` classifier keys on **VASP filenames** (`vasprun.xml`/OUTCAR), so any non-VASP records in
Zenodo/Materials-Cloud are invisible to it until its signature set is widened.

### 6.4 Gain vs cost of going multi-code
- **Gain:** the largest accessible non-VASP materials pool is **FHI-aims + QE in NOMAD (~1.6M calcs)**,
  individual-researcher, raw, re-parseable — a real diversity addition (all-electron FHI-aims is a
  *higher-fidelity* reference than PP-VASP; QE adds the open/European/AiiDA ecosystem + PBEsol). It is
  reachable **through the pipeline already built for NOMAD** — only a new parser + dropping the
  VASP-only filter are needed.
- **Cost:** new per-code parsers (ASE `ase.io` reads QE/CP2K/FHI-aims/GPAW/CASTEP/ABINIT; pymatgen has
  some) — moderate effort; plus disciplined reference-group tagging (machinery exists). Cross-code
  energy incompatibility means the payoff is diversity, not scale-of-one-reference.
- **Steer (parser-maturity-prioritized):** if multi-code is pursued, add **FHI-aims first** (largest
  accessible pool, 1.38M in NOMAD; most mature ASE/pymatgen parser; all-electron → genuine *fidelity*
  diversity), **QE second** (134k in NOMAD + MC3D/MC2D as a small AiiDA-provenanced bonus; mature parser),
  **CP2K only if liquids/interfaces are an explicit target** (small pool, less-tested parser, but the
  targeted fix for the AIMD gap), and **skip GPAW** (binary output, tiny pool, property-only C2DB). All are
  reachable via the **existing NOMAD pipeline** — the blocker is a per-code parser + dropping the VASP-only
  filter, not discovery. Keep each code its own reference group. This is a **new scope decision** (project
  is VASP-only by mentor scope); at ~64.6M frames already assembled, a non-VASP push is about
  *diversity/robustness*, not scale.
- **The one cheap non-VASP win that needs no parser:** ingest **PET-MAD** (§6.2) as a ready processed set
  (its own reference group) — the closest thing to "MPtrj but QE", explicitly built for MLIP, one download.

---

## 7. Recommendations (priority order)

1. **Do NOT re-harvest the institutional databases** (MP/OMat24/Alexandria/OQMD/AFLOW/GNoME/JARVIS/OC20)
   — buy-not-build; if ever wanted, ingest their processed form (or LeMat-Traj) at training time as a
   separate reference group.
2. **Materials Cloud** — a cheap, provenance-rich VASP completeness sweep (~30–35 records; §5). Low
   effort (~80% pipeline reuse); its InvenioRDM adapter generalises.
3. **Long-tail sweep (direction #2)** — a DataCite + OpenAlex→data-DOI discovery layer feeding per-repo
   adapters (Mendeley + Materials Cloud first; ScienceDB after a feasibility spike). Incremental but
   distinct; the niche is open.
4. **Multi-code (direction #3)** — if scope is widened, **FHI-aims + QE via the existing NOMAD pipeline**
   (~1.6M non-VASP periodic calcs, already API-accessible) is the biggest gain-per-effort; keep each
   code its own reference group. A separate scope decision with the mentor.
5. **The scientific payoff** is still to *use* the assembled long tail: dedup vs the megasets, quantify
   what is unique, and show in a training ablation that it helps (direction #1).

---

## Appendix — methods & flagged uncertainties

- All record counts are **metadata-text** matches (Invenio `q` / DataCite `query`) unless a file/record
  inspection is noted — lower bounds that include text-only false positives; Mendeley counts are
  inflated ~2× by version-DOIs (deduped to unique in §4).
- NOMAD numbers are exact from the aggregation API (2026-09-21). Materials Cloud numbers are exact from
  its Invenio API. Institutional-DB sizes are paper/dataset-card-sourced (see the memory notes
  `external-db-evaluation`, `longtail-repo-survey`).
- Frame/calc yields for un-harvested sources are **estimates**; the exact figure comes only from the run
  (as it did for Zenodo and NOMAD).
- Flagged uncertain: ScienceDB harvestability (API undocumented + connectivity flaky); Materials Cloud
  frame yield; exact Materials Cloud MC3D/MC2D/MAD sizes (§6.3, being finalised).

# Zenodo census — closing the keyword blind spot (FURTHER_WORK part A)

The Zenodo harvest found VASP deposits through keyword search of record metadata. This note records
why that misses real VASP data (measured), the replacement built in `zenodo_census/` — a census of
every record that could hold VASP output, scored offline and peeked selectively — and the decisions
taken. Everything under "measured" was obtained live on **2026-09-25**. CSD3 runbook:
`scripts/csd3/census/README.md`.

> **Status (2026-09-26): BUILT, offline-tested (71 census tests incl. a census → triage → shared
> fetch → parse → verify run), live-smoked (census stage, link parsers), and independently reviewed
> (two review passes; every finding fixed with a regression test — §12). First CSD3 census run
> (job 36358803): 79 windows / 462,524 records (2013 → 2026-06-20) and both link channels done, then
> it died on a record Zenodo's own JSON serializer cannot return — the census now routes around such
> records without losing them (§2); resubmit to resume (~1 h left).** Results go in §11.

---

## 0. TL;DR

| Question | Answer |
|---|---|
| Why is VASP data missed? | Zenodo search sees only metadata text. Measured beyond that: sparse metadata (`13888307`), `&nbsp;` gluing words into one token (`4541602`), quoted phrases never stemmed (every plural missed), and ~8 weeks of records created after the last keyword discover (2026-07-30). Of the 4 known-missed Kavanagh VASP datasets, 3 are discovery misses and 1 (`10630244`) is the no-licence gate. |
| The replacement | **Census**: every record that HOLDS an archive or a loose VASP primary — `files.entries.ext:(zip OR gz OR …)` + `files.entries.key:(*OUTCAR* …)` — **583,443 records, all resource types, ~5.8k search pages ≈ 3.4 h** at the token's page size of 100. All scoring is then offline. |
| How records are chosen | Offline signals (robust local text matching; depositor account / ORCID / name / community shared with the 303 known-VASP records; paper links that cite the VASP method papers; Europe PMC data-availability mentions; file names) → tiers **T1** strong / **T2** plausible / **T3** low-signal / **T0** another domain. |
| How archives are read | Zip central directories over HTTP Range (**ZIP64-aware** — the Zenodo triage could not read `13888307`'s 19.6 GB ZIP64 zip) and a **head-peek** of tar-family streams (first 8 MB, one request). One pacer keeps every request inside Zenodo's 100/min + 5,000/h. |
| What is kept (user) | Positive evidence (any tier); for T1 also archives no peek can settle (fail-safe); ND / no-licence records listed for approval, not harvested. T3 is **sampled** (~3k) to measure the residual; the whole non-software T3 tier is peeked only if the rate is ≳ 1 in 2,000. |
| Output | An ORDINARY Zenodo keep-list → `scripts/csd3/20_pipeline.sh` unchanged, straight into the production dataset (same fetch, parse, calc_ids `zenodo:<recid>:<path>`, schema). |

---

## 1. Why keyword discovery misses VASP data (measured)

* **Metadata-only search** (known): archives' contents are never indexed; filename search sees only
  top-level names (33 records Zenodo-wide expose a loose `OUTCAR`/`vasprun`/`vaspout`).
* **Sparse metadata**: `13888307` (19.6 GB, Kavanagh) says only "Accompanying data & analysis
  notebooks for '<title>' … Article link"; `12518256` "Data and code required to generate figures".
  Neither contains a DFT/computation word, so no `(computation) AND (materials)` query can match.
* **`&nbsp;` glues tokens** — `4541602`'s description reads "Using ab initio&nbsp;defect
  techniques": `q="ab initio" AND recid:4541602` → 0, `q=defect …` → 0, `q=vacancy …` → 1. The
  indexer does not split on the entity, so both words vanish from the index.
* **Quoted phrases are not stemmed** — `"potential energy surface"` → 0 on `4541602`,
  `"potential energy surfaces"` → 1, while bare words stem (`vacancy` matches "vacancies"). Most
  multi-word phrases in `discover.DEFAULT_QUERIES` (`"interatomic potential"`, `"thin film"`,
  `"grain boundary"`, `"band structure"`, `"crystal structure"`, …) therefore miss their plurals.
* **Freshness** — the newest record in the dataset was created 2026-07-30 (the keyword discover);
  the 2026-09-19 re-discover kept only NC-licensed records. The dataset gained 10–20 records per month
  in 2026, so ~8 weeks of new open VASP records were never evaluated. The census covers them.
* **Licence gate** (intentional): `10630244` (10.5 GB, no licence) — handled by the review list (§6).

---

## 2. The census (stage 0')

**API facts (live):** Zenodo's `/api/records` search accepts InvenioRDM field syntax: `files.entries.ext`
(lower-cased last extension — an upper-case query matches nothing), `files.entries.key` with wildcards
(case-sensitive), `_exists_:metadata.related_identifiers`, `metadata.related_identifiers.relation_type.id`,
and `file_type=zip` as a URL parameter. Page size is 25 anonymously (26 → HTTP 400) and **100 with a
token**; the search (and OAI-PMH) endpoint is **30 req/min** regardless of the token; single-record and
file endpoints report 133/min, and Zenodo documents **100/min + 5,000/h** for authenticated clients.

| population (2026-09-25) | records |
|---|---|
| Zenodo total | 7,345,291 |
| any archive (`zip gz tgz tar 7z rar xz bz2 zst tbz2 tbz txz tzst lzma aiida`) | **583,410** — dataset 228,368 · software 260,417 · publication 69,164 · other 10,769 · model 5,102 · rest ~9.6k |
| a loose VASP primary by name | 33 |
| an AiiDA export (`*.aiida`) | 12 |

`python -m zenodo_census.cli census` pages `CENSUS_QUERY` exhaustively (the shared recursive
`created`-bisection past the 10k window) at 100 per page, pacing **request starts** 2.05 s apart (the
shared client waits after each response; a 100-record page takes 2–3 s to serve, which would double
the run; 2.2 s leaves margin for jitter). Each hit is slimmed (~2.6 KB: what `Candidate.from_record`
reads + depositor `owners`, ORCIDs, affiliations, communities, related identifiers, references, notes,
journal) and appended to `census.jsonl`; a sentinel per finished leaf window makes it resumable, and
the first run fixes the `created` range in `census.jsonl.bounds.json` so a resume on a later day
bisects the same windows (the census is a snapshot of that range; `--fresh` starts a new one). A torn
final line left by a kill is cut before any append. `iter_census` yields the deduplicated view
(newest version per concept). Live smoke: one day = 199 records in 2 pages, 17 s.

**Records Zenodo cannot serialize (measured 2026-09-26).** The first CSD3 run died after 3 h on page 25
of the window `2026-06-20T00:45 … 2026-06-29T19:52:30`: HTTP 500 on all six attempts, and still 500
twenty hours later — deterministic, not an outage. Bisecting that page with bodiless `HEAD` requests
(10 × size 10, then 10 × size 1) isolated **one** record at offset 2486, `20797668` ("ACTRIS/EARLINET
Level 3 2000-2021 climatological dataset", atmospheric lidar data; its 99 page neighbours serialize
fine). Zenodo's default ("legacy") JSON serializer fails on it everywhere — `/api/records/20797668`
is a 500 too — while InvenioRDM's own serializer (`Accept: application/vnd.inveniordm.v1+json`)
returns it. Not the cause: its custom rights entry (1,749 such records passed in the finished range);
the likely trigger is its only location being a geometry-only `Polygon` (point locations without a
place passed). So `CensusClient` now:

* on a page that still fails after the usual retries, first checks that Zenodo answers at all — a
  probe for a page past the end, which returns `hits.total` without serializing any hit (verified) —
  and waits out an outage (60 s doubling to 10 min, up to an hour, then stops resumably) instead of
  mistaking it for broken records;
* if Zenodo answers, splits the page into aligned sub-pages (100 → 10 → 1), so every record the
  default serializer can return still comes from it, and reads each record that still fails from the
  native serializer, rewritten into the default shape by `legacy_from_native` — **exact** for every
  field the census keeps (checked on nine live records in both serializations: identical `slim_hit`
  and keep-list entries) and tagged `_serializer: "inveniordm"`;
* logs each such record to `census.jsonl.poison.jsonl` (the native hit; a record neither serializer
  returns — none seen — is listed with its window and offset, never dropped silently), and counts
  them in the run summary (`poison_converted` / `poison_unresolved`);
* counts a window whose newest hit is such a record with the same probe, and serves
  `search_page` (the `resolve` step's batched lookups) through the native serializer when the default
  one fails.

The cost is ~2 min per such record; none occurred in the first 462k records, and the record's
community (`actris-ares`) holds 48 records in all.

---

## 3. Signals and tiers (stage 0'', offline)

All text is HTML-stripped and entity-decoded (`&nbsp;` → space) before matching, and every phrase is
written to match its plural. Per record (`zenodo_census/signals.py`):

| signal | what | measured on the 303 known-VASP records |
|---|---|---|
| text | VASP words; DFT-method words (ambiguous ones — "DFT", "NEB", "PAW", "HSE", "lobster" … — count only with a second cue); MLIP words; specific materials words; chemical formulas in title/keywords (≥ 2 elements, a lower-case letter: `LiNiO2`, `CdTe`; not `SSP5`/`CO2`) | 97% reach T1/T2 on text alone |
| negatives | other-domain words (genomics, clinical, ecology, climate, social science, NLP, …) and file types (`.fastq`, `.nii`, `.shp`, `.nc`, audio/video, GROMACS/AMBER trajectories) | 0 positives demoted |
| depositor | the Zenodo **account** (`owners`) of a known-VASP record; institutional accounts (> 300 census records) only weakly | 43% leave-one-out (219 accounts for 303 records) |
| ORCID | a creator ORCID of a known-VASP record | 46% leave-one-out (77% of records list an ORCID) |
| name | a distinctive creator name (≤ 30 census records) of a known-VASP record — only beside a content cue | 72% leave-one-out, but noisy (surname + initial) |
| community | a Zenodo community (≤ 3,000 records) that holds a known-VASP record | 19% (e.g. `wmd-group`, `bam`) |
| paper | a DOI linked from the record (related identifiers, description hrefs, references; arXiv ids) or a paper citing it (DataCite events) that **cites the VASP method papers** (OpenAlex) — or sits in a materials/physics/chemistry field | 26% link a paper; **72% of those papers cite VASP** |
| Europe PMC | the record is named in the full text of an OA paper that mentions VASP | independent recall check (~592 papers) |
| files | a loose VASP primary; archive names with `vasp`/`vasprun`/`outcar`/`dft` (strong) or workflow words `scf`, `neb`, `aimd`, `phonon`, `relaxation`, … (weak) | — |

**Tier rule** (`signals.tier_of`; terms are counted as distinct STEMS, so "phonon" + "phonons" is
one): **T1** = a strong cue — a loose VASP file, a linked VASP method paper, a linked/citing paper
that cites VASP, a Europe PMC mention, a reliable DFT term (or two DFT terms) with a materials cue, an
ambiguous DFT word ("DFT", "NEB", "PAW", "HSE" …) with ≥ 2 materials cues or a formula, MLIP words in
context — or a name-level cue ("VASP" in the text or a file name, a seed depositor/ORCID) on a record
that is not clearly another domain; **T2** = any weak cue (a DFT or MLIP word alone, ≥ 2 materials
words, a formula, one materials word on a sparse record, a materials-field paper, a seed community, a
workflow file name such as `DFT_*`/`scf`/`neb`, a seed name beside a content cue), and also a
name-level cue on a negative-looking record ("VASP" is a cell-biology protein too — peeked, fetched
only on evidence); **T0** = another domain (negative words + negative file types outnumber the
materials cues, no reliable DFT term or formula); **T3** = the rest. Supplementary movies are not a
negative file type (MD movies are common in materials deposits).
On a random sample of 619 archive-bearing records the text rule gave T1+T2 ≈ 2.7%, T0 ≈ 26%, T3 ≈ 71%;
on a one-day live census 1% / 1.5% / 27% / 70%. All three hidden Kavanagh records land in T1/T2 on
text alone (`4541602` T1; `13888307` and `12518256` T2, lifted to T1 by the paper / depositor signals).

Excluded before scoring: records already in the dataset (the seeds) and everything the keyword harvest
evaluated (`candidates_full`, `keep`, `nc_candidates`, `nc_keep`, `byid_10579527_keep` — matched on
recid or concept, so a newer version is excluded too); scoring refuses to run if the dataset
metadata or every manifest is missing (a wrong data root would otherwise re-admit harvested records).
`candidates_nolicense.jsonl` is deliberately NOT excluded: it holds post-2026-07-30 open records the
NC filter dropped unevaluated. **Re-checked, not excluded**: keyword candidates the old triage dropped
without really examining them — archives it did not recognise (`.aiida`, `.txz`, `.tzst`, bare
`.zst` …, which ranked such records below its gate) and zips ≥ 100 MB that its 16-bit entry-count bug
(fixed 2026-09-24) may have "proven" empty. Records in a keep-list (fetched) are always final.

---

## 4. Link channels (network, cached)

| channel | source | size / cost |
|---|---|---|
| paper → dataset | DataCite event store: `/events?prefix=10.5281&source-id=crossref&relation-type-id=references` (Crossref reference lists citing Zenodo DOIs). DataCite's per-DOI `citationCount` is mostly the depositor's own `IsSupplementTo` links (already in the census) and list responses omit the citing DOIs. | 46,190 events, 47 cursor pages → 46,133 links (the rest name no parseable Zenodo id) |
| data availability | Europe PMC full text: `("VASP" OR "Vienna ab initio" …) AND zenodo` → Zenodo ids in each OA paper (PMC by PMCID, preprints by their `PPR` id) | 592 hits (7 preprints) → 572 full texts, 554 naming a Zenodo id; 20 answer HTTP 500 persistently (the publisher does not allow the XML — NCBI's copy of one is front matter only); re-tried each run, not cached |
| versions | papers often cite a VERSION DOI while the census holds the newest version: batched `recid:(a OR b …)` searches with `all_versions=true` map ids to concepts | ≤ ~250 searches |
| paper verdict | OpenAlex singleton lookup per DOI (free; list/filter calls are metered since 2026 — anonymous $0.10/day, $1/day with a free key): `referenced_works` ∩ {Kresse–Furthmüller 1996 PRB/CMS, Kresse–Joubert 1999, Blöchl 1994, Kresse–Hafner 1993/1994} + primary-topic field. Only papers of records still in T2/T3 are looked up. | ~50k lookups at ≤ 8/s |

None of these hosts receives the Zenodo token or a contact address (`OPENALEX_API_KEY` is passed
when set).

---

## 5. Triage (stage 1')

* **Selection**: all T1 + T2; a seeded random sample of 3,000 non-software T3 records (the residual
  measurement, reported with a Wilson 95% interval and extrapolated to the tier); 300 T0 records
  (negative-filter check). Records already kept by an earlier run are skipped.
* **Zip peek**: `materials_cloud_harvest.remote_zip.peek_archive` — ZIP64-aware, validating, fetch's
  own name rules — now given the file's listed **size**, so the tail read is an ordinary range: Zenodo
  mis-serves a suffix range longer than the file (206, underflowed start, a body it never sends —
  re-verified on a 275 KB file), which would have failed every zip under the 1 MiB tail.
* **Head-peek** (`zenodo_census/headpeek.py`): one Range read of the first 8 MB; compression sniffed
  from magic bytes (gzip/bzip2/xz/zstd, or a zip/7z/rar in disguise); decompressed under a 128 MB cap;
  tar headers walked (ustar prefix, GNU long names, PAX paths, base-256 sizes, checksums). A stream
  that fits in the head gives an exact listing; otherwise the leading members are evidence (a VASP
  run directory shows `INCAR`/`POSCAR`/`OUTCAR` early). A head that turns out to be a different
  container than the name says makes the keep-list declare `archive_kind`, since the shared fetch picks
  its extractor from the name.
* **Strict evidence**: a head listing is "complete" (can prove absence) only when the whole file
  was read, every compressed stream ended (multi-stream gzip/bzip2/xz are followed) and the tar's own
  end block was reached; checksums (signed or unsigned) and size fields are verified, only regular
  files count, VASP names use the anchored triage rule (`INCAR`, not `incarnation.txt`) and heavy
  outputs their exact names (`CHGCAR`, not `chgnet_0.3.0.pt`); zstd output is bounded. A head too
  small to decode anything is "no verdict", not "a single file".
* **Real extractors**: a peek that finds another container than the name says (a zip named
  `.tar.gz`, zstd named `.tar.xz`, a 7z/rar in disguise, a `.tbz`), or an AiiDA sqlite archive under
  any name, makes the keep-list declare `archive_kind`, since the shared fetch picks its extractor
  from the name; a plain zip that merely contains a `db.sqlite3` is judged by its member names.
* **Pacing**: every request of every peek waits on one pacer (0.8 s between starts = 75/min,
  4.5k/h); 429s honour `Retry-After`; each failed peek is retried once and never cached (nor is a
  failed zip→tar fallback); the verdict cache repairs a torn last line and keys head-peeks by head
  size.
* **Decision** (`triage.decide`): keep on positive evidence (a peeked VASP primary; a head listing
  VASP-named files; a loose primary) — with every unresolved sibling archive — or, for T1 only, when
  an archive cannot be settled; prune proven-empty archives; divert ND / no-licence records to
  `*.licence_review.jsonl`. Each kept record's evidence is `primary` (a VASP output seen) or `hint`
  (only VASP-named inputs / heavy files in a partial head); the report gives the residual sample's
  rate both ways, and the full-T3 go/no-go uses the strict `rate_primary`.

---

## 6. Decisions (user, 2026-09-25)

| # | Decision | Chosen | Alternatives |
|---|---|---|---|
| 1 | Unpeekable (tar-family) archives in records without a VASP mention | **Head-peek; fetch on evidence; also fetch unresolved ones for strong-signal (T1) records** | evidence only; fetch every unresolved archive in plausible records (MC decision 5; TBs) |
| 2 | Residual (low-signal) records | **Sample ~3k first; if hidden VASP ≳ 1 in 2,000, peek all non-software T3** (~2–3 days) | sample and report only; everything incl. GitHub software |
| 3 | ND / no-licence VASP records | **List for case-by-case approval** (as `10579527` was) | harvest no-licence automatically; exclude |
| — | Licence admitted | open + NC/NC-SA (the dataset's current scope after the NC expansion) | — |
| — | Destination | direct into the production Zenodo dataset (metadata backup first), as the NC expansion | staging + merge |

---

## 7. Running it

See `scripts/csd3/census/README.md`: `10_census.sh` (census + link pulls) → `20_score.sh`
(links top-up, `resolve`, `openalex`, `score`) → review `score_report.json` → `30_triage.sh`
(`RESUBMIT=1`) → review `census_keep.report.json` + the licence-review list → `20_pipeline.sh` with
`IN=…/census_keep.jsonl RAW_DIR=…/raw_census`.

---

## 8. Expected cost and yield

* **Requests**: census ~6k searches (3.4 h); links ~0.7k; resolve ≤ 250; OpenAlex ~50k (free);
  triage ~20-30k Zenodo file requests for T1/T2 (≈ 5-8 h) + ~4-5k for the samples.
* **Transfer before the pipeline**: census ~1.5 GB of JSON; peeks ≤ 1 MiB per zip tail (+ central
  directories) and 8 MB per tar head — tens of GB at most. Nothing is staged.
* **Yield**: unknown until the runs — the ~8 weeks of new records alone should hold a few dozen VASP
  records (the dataset gained 10–20 per month in 2026); the Kavanagh sample suggests keyword search
  missed ~20% of VASP deposits (3 of ~13 of his). The residual sample and the Europe PMC coverage
  (`score_report.json` → `epmc_coverage`: how many VASP-paper-cited Zenodo records were already in
  the dataset) turn this into a measured recall.

---

## 9. Shared-code changes (backward-compatible)

* `zenodo_harvest/client.py`: `ZenodoClient._get` takes optional per-request `headers` (the census
  asks for the native serializer with an `Accept` header); they are passed to the session only when
  given, so existing callers and test doubles see exactly the old call.

* `materials_cloud_harvest/remote_zip.py`: `read_central_directory` / `peek_archive` take an optional
  `size` (tail read as an ordinary range — the Zenodo suffix-range bug above); without it the
  behaviour is unchanged. `peek_archive` also turns a body that breaks mid-read
  (`requests.RequestException`) into a failed, uncached peek instead of raising (a crash of the whole
  triage before).
* `materials_cloud_harvest/remote_zip.py` also retries a tail read that a stale listed size pushed
  past the end of the file (HTTP 416) as a suffix read.
* `zenodo_harvest/models.py`: `is_reusable_license` treats Zenodo's legacy `other-closed` ("Other
  (Not Open)") as not reusable — a gap the review found (no record in the current dataset has it).
  Nothing else in `zenodo_harvest` changed: the census writes a standard keep-list and the pipeline,
  fetch, parse and store are used as they are.

---

## 10. Limitations and future work

* **Residual dark matter**: a record with bare metadata, no seed identity, no paper link and only
  unpeekable archives is found only by the T3 sample (or the full-T3 run); GitHub software is not
  brute-forced (user decision) — VASP test fixtures inside code repositories stay outside unless a
  signal flags the record.
* **Head-peek is evidence, not proof**, for streams larger than 8 MB: a tar whose leading members are
  not VASP files reads as unresolved (fetched only for T1). rar/7z archives are never peeked.
* **Renamed outputs**: VASP files under non-VASP names (`run1.xml`) are invisible to peeks and fetch
  alike; a single gzipped file under a non-VASP name cannot be used by the shared fetch.
* **Unsupported containers**: `.lzma` (legacy LZMA-alone, no magic bytes) tarballs are in the census
  but neither peekable nor extractable by the shared fetch; split archives are not reassembled.
* **Links cover papers with DOIs and reference lists**: OpenAlex lacks references for some papers
  (e.g. `10.1021/acsenergylett.4c01307` shows none of the VASP papers), and DataCite events miss
  papers that name data only in the text — Europe PMC covers the open-access part of that.
* **Point in time**: re-running `10_census.sh` + the later steps picks up new records (the census
  resumes; `--fresh` restarts it).

---

## 11. Results

### First census run — CSD3 job 36358803 (2026-09-25, FAILED after 3 h 05 min, resumable)

| step | outcome |
|---|---|
| census | 79 leaf windows contiguous from 2013-01-01 to 2026-06-20T00:45 = **462,524 records** (+2,400 lines of the unfinished 80th window, harmless duplicates), 3 h 03 min at ~26 req/min — on the planned pace. Died on the record above (`20797668`); bounds `2013-01-01 … 2026-09-25` fixed. |
| still to do | **121,468 records** created 2026-06-20T00:45 … 2026-09-26 (counted live 2026-09-26; the summer of 2026 is dense) ≈ 1,300 pages ≈ 1 h on resume, after ~6 min of count calls that re-derive the finished windows (their counts moved by 78 records in all, too little to change any leaf). |
| DataCite links | complete: 46,133 links (47 pages, ~2 min). |
| Europe PMC | complete bar 20 papers with no downloadable XML (above): 572 / 592. |

*(to be filled after the remaining runs: census size, tier sizes, link coverage, triage funnel and
yield per signal, residual rate + extrapolation, Europe PMC coverage / recall of the keyword method,
the Kavanagh probes, pipeline outcome.)*

---

## 12. Review (2026-09-25)

Two independent review passes (discovery/scoring; peeks/triage/fetch integration) found — and the
code now fixes, each with a regression test — among others: vocabulary terms followed by a hyphen
never matching ("VASP-based"); a backtracking formula regex (now a linear parser); a later-day
resume re-paging the whole census (bounds now fixed); torn JSONL lines breaking later runs (tails
cut before appending); `other-closed` licences admitted; head listings wrongly marked complete
(multi-stream xz, signed checksums, missing `Content-Range`, tiny bzip2 heads — "complete" now needs
the tar end block); unbounded zstd output; a hang on negative tar sizes; AiiDA sqlite archives under
a `.zip` name fetched with the wrong extractor; transient fallback failures cached as verdicts;
Europe PMC coverage miscounting versions; single ambiguous cues (the VASP protein, `DFT_*` file
names, "phonon"+"phonons") reaching T1; and a missing-exclusions run silently re-admitting harvested
records.

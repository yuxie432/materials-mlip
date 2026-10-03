# Zenodo census — closing the keyword blind spot (FURTHER_WORK part A)

The Zenodo harvest found VASP deposits through keyword search of record metadata. This note records
why that misses real VASP data (measured), the replacement built in `zenodo_census/` — a census of
every record that could hold VASP output, scored offline and peeked selectively — and the decisions
taken. Everything under "measured" was obtained live on **2026-09-25**. CSD3 runbook:
`scripts/csd3/census/README.md`.

> **Status (2026-10-02): census COMPLETE (583,930 records, every one of the 303 dataset records
> among them), SCORED (T1 2,933 · T2 33,175 · T3 344,635 · T0 200,669) and T1 TRIAGED on CSD3, then
> RE-TRIAGED with the deep peeks (§5): VASP evidence in 272 T1 records (incl. all four known keyword
> misses), 2,227 proven VASP-free, 434 kept by the fail-safe → keep-list 680 records / 3.09 TB (§11);
> the residual T3 sample found 0 in 3,000 → T3 stops. **T1 HARVEST DONE** (pipeline 2026-09-29 → 10-01
> + targeted recovery, `verify` OK): **+230 records / +173,289 calcs / +5,669,216 frames** → the
> production Zenodo dataset reached ~533 records / 355,400 calcs / 17,757,938 frames (§11). **T2 TRIAGED**
> (2026-10-01): VASP evidence in 103 of 33,175 records (0.31%), 26,855 proven VASP-free, 6,217
> unresolved (25.7 TB) left unharvested; keep-list 103 records / 210 GB, 102 after excluding
> `12792088` (decisions 9-13, §6). **T2 HARVEST DONE** (pipeline job 37089987 + recovery job 37127828,
> `verify` OK): **+87 records / +28,449 calcs / +455,181 frames**. **PART A COMPLETE (2026-10-02): the
> census added +317 records / +201,738 calcs / +6,124,397 frames** (+105% / +111% / +51%) to the 303
> records / 182,111 calcs / 12,088,722 frames of the keyword harvest → production Zenodo dataset
> **~620 records / 383,849 calcs / 18,213,119 frames** (§11, "Census outcome").** **SEED SNOWBALL
> DONE (2026-10-02, decisions 14-16, §11)**: re-score with all ~620 dataset records as seeds → 541
> records whose tier rose → 51 kept → pipeline job 37197208 (`verify` OK): **+9 records / +2,576 calcs /
> +30,571 frames** → production Zenodo dataset **629 records / 386,425 calcs / 18,243,690 frames**;
> nothing worth a recovery run. 98 offline census tests; three review passes (§12).

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
| Result (2026-10-02) | **+317 records / +201,738 calcs / +6,124,397 frames** — the dataset's records and calcs more than doubled, frames +51%. 783 records kept by triage (T1 680, T2 103), 317 yielded calcs; 3 of the 4 known keyword misses are now in the dataset (the 4th has no licence). |

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
(fixed 2026-09-24) may have "proven" empty — matched on recid or concept (the census's newer version
of such a candidate is re-checked too), and always peeked: one the census signals put in T3 / T0 is
promoted to T2 (reason `keyword_recheck`, the signal tier kept as `tier_by_signals`; added
2026-09-28 after the first scoring left some unpeeked). Records in a keep-list (fetched) are always
final.

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
* **Deep peeks** (`zenodo_census/deeppeek.py`, 2026-09-29) — a second look at the files of records
  the standard peeks leave unresolved, for the containers that CAN be listed without a download:
  an **uncompressed tar** is walked header to header over Range (a member's size locates the next
  header; the read size doubles along runs of small members and drops back to 64 KiB after a big
  one); a **7z** is listed from its end header (py7zr through a Range-backed file, 2-3 reads); the
  **archives nested in a zip** are read in place (local header + first 2 MiB in one read, inflated,
  then head-peeked, local-header-walked, or listed through their own central directory / end header
  when stored), VASP-hinting names first, ≤ 40 per zip. A walk stops at its first VASP output; each
  archive gets ≤ 300 reads (`DEEP_MAX_REQUESTS`; 100 suffices for T2, fetched only on evidence) and
  ≤ 256 MiB (`DEEP_MAX_BYTES` — a walk over thousands of small members must not become a download;
  no single read over 64 MiB). An exact listing ("complete", the only proof of "no VASP") needs a
  tar walked to its end block or to the exact end of the file, a 7z end header decoded, or — for a
  zip — EVERY nested archive listed exactly with nothing nested further (a rar, an AiiDA export, a
  deflated 7z, a member past the cap or the budget all leave it partial).
  In T1, 49% of the unresolved records held at least one such archive and 46% nothing else; the
  live check listed a 2 GB tar in 10 reads and a 49 GB 7z in 3 (both VASP-free). Compressed tar
  streams (gzip / bzip2 / xz / zstd) and rar have no in-place listing. A T1 record deep peeks prove
  empty is not downloaded; one they prove VASP is kept on evidence; one still unresolved stays
  fail-safe (downloaded whole, **no size cap**, as in the keyword harvest); for T2 — no fail-safe —
  deep peeks are the only way its VASP inside such archives is found.
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

**2026-09-29, after the T1 triage**

| # | Decision | Chosen | Alternatives |
|---|---|---|---|
| 4 | T1 fail-safe records (590; 3.38 TB unresolved) | **Deep-peek their unresolved archives; download whatever stays unresolved WHOLE, no size cap** (as the keyword harvest) | whole downloads without deep peeks; a 20 GB cap with a review list |
| 5 | Deep peeks for T2 | **Yes** — T2 has no fail-safe, so they are the only route to its VASP inside such archives | none (unresolved T2 records dropped) |
| 6 | ND (CC BY-NC-ND) records with VASP (3, 0.07 GB) | **Exclude** (ND never admitted) | include |
| 7 | No-licence records (24) | **Exclude** from the pipeline | case by case |
| 8 | Residual T3 (0 VASP in a 3,000 sample) | **Stop** — no full T3 run | extend the sample; full T3 (~3 days of peeks) |

**2026-10-02, after the T2 triage**

| # | Decision | Chosen | Alternatives |
|---|---|---|---|
| 9 | `12792088` (ISSTA 2024 "Sleuth" fuzzer artifact, 13.4 GB; its only hint is one heavy-output-named file in a source tree) | **Exclude** — a `manually_excluded` line in the fetch rejection log, so the keep-list (and its round-robin parts) stays unchanged | fetch it with the rest |
| 10 | T2 unresolved (6,217 records, 25.7 TB; T1's measured rate suggests ~5 VASP records among them) | **Skip** — not downloaded | download all / a size-capped subset; peek the rar files |
| 11 | Content probe of proven-empty records showing VASP inputs but no outputs (331 records, 0.22 TB) | **Not needed** — 12 records spot-checked (§11) hold inputs, final structures, light outputs (`OSZICAR`/`DOSCAR`/`XDATCAR`), processed data or another code; no renamed VASP output | scan every such archive's member contents |
| 12 | Seed snowball (re-score with the ~620 records now known to hold VASP as identity seeds) | **Later option** — not run now | run it before closing part A |
| 13 | T3 / T0 | **Stop** — no full T3 run, no T0 run (samples 0 / 3,000 and 0 / 300; T2 itself yielded 0.31%) | full T3 (~3 days of peeks) |

**2026-10-02, the seed snowball (scoping in §11)**

| # | Decision | Chosen | Alternatives |
|---|---|---|---|
| 14 | Seed snowball (decision 12) | **Run now**: re-score with all ~620 dataset records as seeds, then triage only the records whose tier rose (`select-moved`: 541 — 148 T3 → T1, 55 T3 → T2, 338 T2 → T1) | fold into a later freshness refresh; skip |
| 15 | Fail-safe for the movers | **T1 rule as before**: what stays unresolved in a T1 mover is downloaded whole, no cap — incl. the 28 T2 → T1 records the T2 triage left unresolved (138 GB) | evidence only (the first hop's identity-only fail-safe: 1.47 TB for 1 record); account-linked only, ≤ 10 GB |
| 16 | Extra channels | **None** — the identity re-score only | depositors / ORCIDs of records linked to VASP papers (1,019 T3 records, ~1-5 finds); + the 24 T0 records gaining a strong link |

---

## 7. Running it

See `scripts/csd3/census/README.md`: `10_census.sh` (census + link pulls) → `20_score.sh`
(links top-up, `resolve`, `openalex`, `score`) → review `score_report.json` → `30_triage.sh`
(`RESUBMIT=1`) → review `census_keep.report.json` + the licence-review list → `20_pipeline.sh` with
`IN=…/census_keep.jsonl RAW_DIR=…/raw_census`. A later re-score (the seed snowball) is triaged
through `select-moved` → `30_triage.sh` with `SCORED=` (README step 6).

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
* **Measured (2026-10-02, §11 "Census outcome")**: 317 new records / 201,738 calcs / 6.12M frames —
  inside the ~100-400 records estimated before triage (§11, scoring), at the cost of ~1 week of
  calendar time and ~125 job-hours on CSD3, most of it request-paced triage and the T1 pipeline.

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
  not VASP files reads as unresolved (fetched only for T1). Compressed tars have no in-place listing
  and rar archives are never peeked (7z are, by the deep peeks). Measured in T2: 6,217 records /
  25.7 TB stayed unresolved — gzip-tar heads 12.9 TB, nested zips 3.6 TB, xz tars 3.2 TB, tars that
  spent the deep-peek budget 2.3 TB, rar 1.3 TB (1,220 records unresolved only by a rar). They are
  not harvested (decision 10); at T1's measured rate (5.7% of the records that yielded VASP were
  unresolved ones) they hide ~5 VASP records.
* **Proofs use the pipeline's own names**: "proven VASP-free" is exact relative to the shared fetch
  (same primary-name rule), so it never drops data fetch could have used. The rule cuts both ways:
  name look-alikes pass it (`outcar.db`, `OUTCAR_read.m`, `outcarParser.py`, sisl's `outcar.py`) and
  parse rejects them cheaply; VASP test fixtures inside code repositories pass it too (22 of T2's
  103 evidence records are software).
* **Renamed outputs**: VASP files under non-VASP names (`run1.xml`) are invisible to peeks and fetch
  alike; a single gzipped file under a non-VASP name cannot be used by the shared fetch. None turned
  up in 12 spot-checked "inputs only" records (decision 11): depositors who leave out
  `vasprun.xml`/`OUTCAR` publish inputs, final structures and light outputs instead.
* **Processed data is out of scope**: energies and forces already converted (extxyz, DeePMD `npy`,
  phonopy `FORCE_SETS`, ASE databases, CSV tables) are not read — the pipeline harvests raw VASP
  output with its full calculation parameters.
* **Signal precision**: OpenAlex's broad fields make `paper_field` weak (22,082 T2 records, 0.09%
  with VASP; its Astronomy subfield 0 / 5,598), as are bulk (> 300-record) seed accounts (0 / 2,883)
  and the keyword re-checks (0 / 128). T2's best signals (workflow-named file 4.2%, MLIP text 3.8%,
  seed name 2.8%) stay below T1's weakest (MLIP + materials text, 6.3% of records yielding calcs).
* **Unsupported containers**: `.lzma` (legacy LZMA-alone, no magic bytes) tarballs are in the census
  but neither peekable nor extractable by the shared fetch; split archives are not reassembled.
* **Links cover papers with DOIs and reference lists**: OpenAlex lacks references for some papers
  (e.g. `10.1021/acsenergylett.4c01307` shows none of the VASP papers; 23% of the 52,241 linked DOIs
  OpenAlex resolved have no reference list — 71% of preprints, 5% of articles — and 8 of T2's 20
  `paper_field` positives sit behind such a paper), and DataCite events miss papers that name data
  only in the text — Europe PMC covers the open-access part of that.
* **Records without an archive** are outside the census unless they hold a loose VASP primary: of
  the 631 records VASP papers name in Europe PMC, 218 were never triaged (no archive, or already
  evaluated by the keyword harvest).
* **Seed snowball** (decisions 12, 14-16; scoping in §11): the census finds are weaker seeds than the
  keyword records (second-hop precision: account 5.7%, ORCID 0.5%); the re-score promoted ~200
  never-peeked records and yielded +9 records / +2,576 calcs (§11); the paper-graph siblings are used up.
* **Deep-peek verdicts are cached per read budget**, complete listings included: a run with another
  `DEEP_MAX_REQUESTS` reads every deep-peeked archive again (cheap for the 541 movers; ~90k requests
  if a whole T2 tier were re-triaged at 300).
* **Point in time**: the census covers records created up to 2026-09-25. A plain re-run of
  `10_census.sh` resumes inside the first run's fixed `created` range (`census.jsonl.bounds.json`), so
  it adds nothing new; a refresh needs `--fresh` (the whole ~4 h census again; peek verdicts stay
  cached).

---

## 11. Results

### First census run — CSD3 job 36358803 (2026-09-25, FAILED after 3 h 05 min, resumable)

| step | outcome |
|---|---|
| census | 79 leaf windows contiguous from 2013-01-01 to 2026-06-20T00:45 = **462,524 records** (+2,400 lines of the unfinished 80th window, harmless duplicates), 3 h 03 min at ~26 req/min — on the planned pace. Died on the record above (`20797668`); bounds `2013-01-01 … 2026-09-25` fixed. |
| still to do | **121,468 records** created 2026-06-20T00:45 … 2026-09-26 (counted live 2026-09-26; the summer of 2026 is dense) ≈ 1,300 pages ≈ 1 h on resume, after ~6 min of count calls that re-derive the finished windows (their counts moved by 78 records in all, too little to change any leaf). |
| DataCite links | complete: 46,133 links (47 pages, ~2 min). |
| Europe PMC | complete bar 20 papers with no downloadable XML (above): 572 / 592. |

### Census COMPLETE — CSD3 job 36465881 (2026-09-26, resume, 56 min)

| | |
|---|---|
| resume | 79 finished windows skipped after ~7 min of count calls; the unfinished window re-paged, its page 25 isolated in 2.7 min (`20797668` read through the native serializer); 17 new windows, 121,441 records. |
| census | **583,930 records** (586,365 lines = + 2,400 duplicate lines of the re-paged window + 35 concepts seen in two versions), 96 contiguous `created` windows 2013-01-01 → 2026-09-26; 1 record converted from the native serializer, 0 unresolved; 4 h 00 min of paging in all. |
| check | **all 303 dataset records are in the census** — the archive / loose-VASP query misses none of the known VASP records. |

### Scoring — CSD3 job 36465906 (2026-09-26, 2 h 32 min)

| step | outcome |
|---|---|
| links top-up | no-op (DataCite complete; the same 20 Europe PMC papers still HTTP 500) |
| resolve | 19,888 cited ids → 18,090 concepts, 1,798 unknown (deleted / restricted); 8 min |
| OpenAlex | 55,001 paper DOIs of still-T2/T3 records: 52,240 found, 2,761 unknown, 0 failed; **232 cite the VASP method papers** (0.4%; 103 of them in Materials Science); 2 h 04 min at 8/s |
| score | 581,410 records scored in 9 min: 303 in the dataset and 2,217 evaluated by the keyword harvest excluded, 364 keyword candidates re-admitted for a re-check |

| tier | records | dataset | software | publication | other types | zips | other archives | archive bytes (zip + other) |
|---|---|---|---|---|---|---|---|---|
| **T1** | **2,932** | 1,743 | 654 | 397 | 138 | 4,774 | 1,559 | 3.5 + 3.1 TB |
| T2 | 33,046 | 21,824 | 7,157 | 3,159 | 906 | 52,360 | 32,588 | 54.7 + 31.1 TB |
| T3 | 344,689 | 87,433 | 193,323 | 47,863 | 16,070 | 462,793 | 149,131 | 331 + 168 TB |
| T0 | 200,743 | 116,183 | 58,757 | 17,422 | 8,381 | 439,755 | 330,659 | 334 + 220 TB |

Signals (a record can carry several) — T1: DFT + materials text 1,466 · seed ORCID 814 · Europe PMC
mention 363 · seed depositor account 285 · MLIP + materials text 253 · linked paper cites VASP 242 ·
"VASP" in the text 113 · VASP-named archive 45 · loose VASP primary 13. T2: linked paper in a
physical-science field 22,081 · sparse metadata + a materials cue 3,799 · formula 3,298 · materials
text 3,132 · bulk (> 300-record) seed account 2,883 · DFT text 2,330 · seed name 576 · workflow-named
file 432 · MLIP text 210 · seed community 181. Licences in T1: 2,840 open · 14 NC · 16 ND · 62 none
(ND / none go to the review list only if a peek finds VASP).

**Known misses (probes)** — all four T1: `4541602` (text + depositor + ORCID), `13888307` (depositor
+ ORCID), `12518256` (ORCID), `10630244` (Europe PMC + text + ORCID; no licence → review list).

**Recall of the keyword method (Europe PMC)** — the full texts of VASP papers name 627 distinct
Zenodo records: 48 are in the dataset, 102 were evaluated by the keyword harvest and not harvested,
**363 were never evaluated (all T1)**, 114 hold no archive. Keyword discovery had examined only 150
of the 513 archive-bearing records VASP papers point to (29%); among those it examined, 48 (32%)
held harvestable VASP output.

**Expected gain (before triage — an estimate, not a measurement).** The dataset's records are
heavy-tailed (median 54 calcs / 408 frames; the top 10 of 303 hold 82% of the frames), so the record
count is the predictable number: at the 32% rate above, the 363 never-evaluated paper-linked records
alone hold ~60–120 VASP records (half to the full rate); with the depositor / ORCID / text / paper
signals of the rest of T1 and the long T2 tail, **~100–400 new records** (+30–130% on 303) is the
plausible range — tens of thousands of calcs, frames anywhere from ~0.5 M to ~10 M depending on
whether a few large trajectory sets are among them. Triage turns this into measured numbers
(`census_keep.report.json`: VASP evidence per record, yield per signal, the residual rate).

**Triage workload** — T1 + T2 = 57,134 zips (~1.3 requests each) + up to 34,147 other archives (one
≤ 8 MB head read per tar-family stream; rar/7z are not peeked) ≈ 95–110k requests ≈ 21–25 h at
4.5k/h — about 3× the design estimate, because T2 is 33k (two thirds of it `paper_field`). T1 plus
the residual (3,000) and negative (300) samples ≈ 15k requests ≈ 3.5 h.

### Re-score — CSD3 job 36640142 (2026-09-28, 37 min, all links cached)

With the recheck promotion (§3): T1 2,933 · T2 33,175 (128 keyword rechecks promoted from T3/T0) ·
T3 344,635 · T0 200,669; 2 newer versions of recheck candidates re-admitted (evaluated 2,215); Europe
PMC picked up 4 new papers (596 hits).

### T1 triage — CSD3 job 36640154 (2026-09-28, 3 h 26 min)

6,233 records (T1 2,933 + residual sample 3,000 + negative sample 300), 14,827 files peeked at
~4,300/h (10,268 zip directories, 4,545 heads; 14 failures/odd containers).

| T1 outcome | records | share |
|---|---|---|
| VASP evidence | 252 | 8.6% — 219 a VASP output seen, 11 a loose output, 22 only VASP-named inputs in a partial head; **all four known keyword misses** |
| proven VASP-free | 2,091 | 71.3% |
| unresolved → kept by the fail-safe | 590 | 20.1% (576 admitted + 14 licence review) |

Kept (admitted licences): **815 records** — 239 on evidence (578.7 GB of evidenced archives; ~109k VASP
primaries in their zip listings, 22 records > 1,000 each) + 576 fail-safe (3.38 TB of unresolved
archives). Licence review: 27 (24 no licence, 3 CC BY-NC-ND) — all excluded (decisions 6-7).
Residual T3 sample 0 / 3,000 (95% upper bound 1 in 782), negative T0 sample 0 / 300 → T3 stops.

**Hit rate per T1 signal** (resolved records): VASP-named archive 65% (28/43) · "VASP" in the text
61% (60/98) · known depositor account 32% (74/231) · Europe PMC mention 15.5% (46/297) · known ORCID
14.5% (94/649) · linked paper cites VASP 9.6% (19/198) · DFT + materials text 9.3% (110/1,178) ·
MLIP + materials text 7.3% (14/193) · loose VASP file 100% (13/13).

**The fail-safe records** carry: DFT + materials text 288 (49%), ORCID 165 (28%), Europe PMC 68
(12%), MLIP text 60 (10%), account 54 (9%), paper 44 (7%), "VASP" 15, VASP-named file 2 — mostly one
signal alone (236 text only, 120 ORCID only, 55 Europe PMC only). Applying the per-signal hit rates:
**~63-85 VASP records** expected among them (flat 10.8% rate; per-record best signal) — an upper-middle
estimate, since an unresolved archive whose head showed only non-VASP members is less likely VASP.
Their unresolved bytes by container: gzip tar 1.09 TB · zips nesting archives 0.85 TB · uncompressed
tar 0.52 TB · xz/bzip2/zstd tar 0.43 TB · 7z 0.31 TB · rar 0.06 TB — about half deep-peekable (§5).
By size, the 65 records over 20 GB hold 2.52 TB for ~11 of the expected records (tomography,
patents, phase-field and MD sets among them); they are fetched whole all the same (decision 4)
unless a deep peek proves them VASP-free.

### T1 re-triage with deep peeks — CSD3 job 36712258 (2026-09-29, 1 h 51 min)

Every standard peek came from the cache (6,004 files); the deep peeks read 488 unresolved files of
290 fail-safe records in 8,347 Range requests (~17 per file, at the paced 75/min: request-bound as
designed; 262 zips holding archives, 136 uncompressed tars, 89 7z, 1 "zip" that was not one).

| T1 outcome | first run | re-run | change |
|---|---|---|---|
| VASP evidence | 252 | **272** | +20 found inside archives the standard peeks could not open |
| proven VASP-free | 2,091 | **2,227** | +136 fail-safe records no longer downloaded |
| unresolved → fail-safe | 590 | **434** | 134 still partial after a deep peek + 300 with nothing deep-peekable |
| kept (admitted licences) | 815 (239 + 576) | **680 (259 + 421)** | licence review 27 → 26 (excluded) |
| whole-download ("blind") bytes | 3.38 TB | **2.45 TB** | −0.94 TB (−28%) |
| all kept bytes | 3.96 TB | **3.09 TB** | −0.87 TB; evidenced archives 579 → 643 GB |

* **What they found** (20 records, all open-licence): 11 with VASP outputs actually seen — e.g.
  `15323838` (7z, 255 phonopy OUTCAR / vasprun / vaspout.h5), `20504471` (7z, AIMD OUTCARs),
  `4088537` (vaspruns inside a `.tar.gz` inside a zip), `15855333` (19.9 GB, OUTCAR + vasprun in
  nested trajectory zips), `18511449`, `19323296`, `15198620`, `15225372`, `11483708`, `21822803`,
  `18316581` — and 9 with only VASP-named inputs / heavy files (e.g. `10302508`, 33.5 GB AIMD of
  liquid Ga). All 20 were fail-safe records (downloaded whole anyway); 20 of the 156 fail-safes the
  deep peeks settled hold VASP (12.8%), inside the 10.7-14.4% the per-signal hit rates predicted — so
  the 434 still unresolved should hold ~45-60.
* **By container** (fail-safe bytes left): uncompressed tar 0.52 → 0.04 TB (93% settled), 7z 0.31 →
  0.04 TB (88%), zips holding archives 0.85 → 0.67 TB (21% — their inner archives are mostly
  compressed tars larger than the 2 MiB inner head, so a listing stays partial); gzip tar 1.08 TB,
  xz/bzip2/zstd tar 0.43 TB and rar 0.06 TB untouched (no in-place listing exists).
* **Still partial after a deep peek**: 8 tars stopped by the 300-read or 256 MiB budget (29 GB, e.g.
  `4590731` 20.9 GB, 590 members in 300 reads); 19 complete 7z/tar listings that contain further
  archives (e.g. `19110103`: two 7z of one tar each, 32 GB) — all downloaded whole, as decided. Six
  `.zip` files are not zips (11.6 GB; `21471245`, `10045070`, `6560359`): downloaded and logged
  `extract_error`, the fail-safe's known cost.
* **Proofs checked**: the 136 proven records are CP2K / GROMACS DFT/MM runs, GW / DFTB benchmarks,
  NetCDF MD trajectories, FEFF inputs, "DFT inputs" releases, figure data — no title or listing
  suggests a missed VASP output.
* **Parse RAM**: 229 evidence records show 109,612 VASP outputs in their listings (the most in one
  archive 12,672); only `13843222` (AIMD of doped Bi2O3, six ~1.9 GB zips of 8.1-8.4 GB vaspruns)
  holds primaries near or over the 3.8 GB cap of the 8-core pipeline — its 8.15 GB vasprun is
  deferred `primary_too_large` (staged, re-parsed later on a bigger job).
* `zenodo_census.cli status` reports `keep_records: 0` for a non-default `OUT` (it reads
  `census_keep.jsonl` only) — cosmetic; the triage `.out`/`.err` and `<out>.report.json` are the record.

### T1 pipeline — CSD3 jobs 36712306 → 36792981 → 36859936 → 36932425 (2026-09-29 → 10-01)

Four 12 h rounds (8 icelake-himem cores, 4 fetch + 4 parse workers, 3.79 GB primary cap), every
round resuming cleanly (finished parts re-scanned in ~6 min; transient download failures retried);
the last round ended 04:06 BST with `verify` OK (17,357,661 frames, metadata ↔ shards exact).

| | |
|---|---|
| added | **229 records with calcs, 173,009 calcs, 5,268,939 frames** (dataset now 355,120 calcs / 17,357,661 frames) |
| scoping | none of the 680 keep-list records (recid or concept) is among the 303 pre-census records — excluded at scoring; parse's own calc_id skip never fired (`skipped_existing` 0 in every first-time batch) |
| fetch | 248 fetched, 432 rejected: 385 `no_vasp_files_fetched` (mostly fail-safe archives with no VASP inside), 47 inputs only; no record left transient |
| parse | 213,494 calc units fetched, 40,485 rejected (81% parsed; 91% leaving out 5248078's 23k MP2 files) |

**Yield by triage class** — the per-class yields match the keyword harvest's (confirmed ~98%,
blind 2-15%); what differs is the mix (34% of this keep-list had outputs seen before download vs
~15%), so the record yield is 33.7% here vs 21.7% for the keyword keep-list:

| class | records | fetched | with calcs | calcs |
|---|---|---|---|---|
| a VASP output seen in a listing | 229 | 227 | 211 (92%) | 103,581 |
| only VASP-named inputs seen | 30 | 5 | 5 (17%) | 273 |
| fail-safe (unlistable archives, 2.43 TB downloaded whole) | 421 | 16 | 13 (3.1%) | 69,155 |

The fail-safe yield is far below the 10.7-14.4% the per-signal rates predicted (they came from
resolvable records), but by calcs it holds 40% of the run — almost all `19536185` (67,613 single
points). The "output seen" misses are names that only look like outputs (`outcar.db`,
`OUTCAR_read.m`, `OUTCAR_*.dat`), code-repo examples, and three fetch-side losses (below).

**Parse rejections (40,485), by cause** — almost all are what was deposited, not parser faults:

| cause | calcs | share | main records |
|---|---|---|---|
| post-DFT / non-self-consistent outputs (MP2, RPA, AFQMC, CCSD(T), BSE/GW, NSC-EXX/BEEF, non-SCF HF): no DFT forces | 30,870 | 76% | `5248078` (23,271 `ALGO=MP2` OUTCARs), `21895644`, `19482680` |
| crashed / incomplete runs (stopped mid-SCF, VASP errors, Fortran overflows; USPEX `ERROR-OUTCAR-*`) | 6,382 | 16% | `7318435` (3,649), `19536185` (1,826) |
| not VASP output matched by name (scripts, excerpts, extxyz, 5 KB "vaspruns" an ML-potential workflow wrote) | 2,603 | 6% | `8096932` (1,936), `13119926` (392 `outcar.sh`), `15686940` |
| NEB parent directories / images, MLFF prediction runs | 431 | 1% | `21855564`, `6802056`, `12210642` |
| **numeric `ALGO`** (pymatgen `'int'.lower()`) — **fixed in `parse.py` 2026-10-01** | 158 | 0.4% | `7506565` |
| `primary_too_large` (over the 3.79 GB cap) | 4 | — | `13843222` ×3 (AIMD), `22084774` |
| MD `vaspout.h5` pymatgen's `Vaspout` cannot read (its energy table lacks VASP's MD labels; no σ→0 energy stored) | 26 | — | `18390757` (205,726 MD steps × 160 atoms) |
| other `vaspout.h5` without an energies dataset | 11 | — | `12663897`, `11483708` |

Recoverable — and recovered by the job below: the 158 numeric-ALGO and 4 `primary_too_large`
calcs (from their still-staged files), and two evidenced zips fetch could not extract — `3359829`
(5.4 GB, `Truncated file header`: written without ZIP64, so its offsets are truncated to 32 bits and
zipfile shifts every member by 4 GiB) and `14809725` (one unreadable member aborted the whole zip).
Both fetch gaps were fixed first (2026-10-01): `_extract_zip` skips an unreadable member (one
`extract_partial` rejection per archive) and `_open_zip_member` retries a member at its offset
±k·4 GiB, accepted only where zipfile finds the signature AND the exact name (CRC-32 checked on
read) — verified on a real 4.6 GB non-ZIP64 zip. Not recovered: `22171731` (its only archive still
answers HTTP 403) and the 18390757 MD trajectories (pymatgen cannot read MD `vaspout.h5`; usable —
F ≈ E0 for this insulator — but one heavily correlated system, so left out). `status` listed only the top 8 rejection reasons — which hid `primary_too_large` — and now lists
all. `sacct` MaxRSS ≈ the 52.8 GiB allocation in rounds 1-2 without any OOM kill: page cache.

### T1 recovery — CSD3 job 37029347 (2026-10-01, 1 h 23 min, 20 icelake-himem cores)

`scripts/csd3/census/40_recover_t1.sh`: a `--retry-rejected` re-parse of three records from their
staged files plus a re-fetch of the two zips with the fixed fetch (6 min), parse 40 min (2 workers,
budget 122 GiB, cap 10.9 GB; RAM reservation peaked at 91.6 GiB for the 8.15 GB vasprun), then
`verify` OK and `purge-raw` (35.5 GB freed). The 1,162 calcs already stored were skipped.

| record | why it was lost | recovered | still rejected |
|---|---|---|---|
| `13843222` AIMD of doped Bi2O3 | 3 vaspruns over the 3.79 GB cap | 3 calcs, ~400k frames | 0 |
| `7506565` ZrO2 under pressure | numeric `ALGO = 48` (pymatgen) | 156 calcs | 23 (16 non-SCF band runs with no ionic step, 7 broken OUTCARs — genuine) |
| `14809725` H2CO adsorption single points | one unreadable member aborted a 1.2 GB zip | 114 calcs — the whole zip (211 units = its 422 listed outputs); the bad member was an input | 0 |
| `3359829` Fermi-arc band/SCF OUTCARs (new record) | 5.4 GB zip with 32-bit offsets | 6 calcs (all 6 listed outputs) | 0 |
| `22084774` band/DOS vaspruns | 1 vasprun over the cap | 1 calc | 2 (non-SCF band runs) |
| **total** | | **+280 calcs / +400,277 frames / +1 record** | |

**T1 outcome (pipeline + recovery):** **+230 records / +173,289 calcs / +5,669,216 frames** — the
production Zenodo dataset grew from 303 records / 182,111 calcs / 12,088,722 frames to **~533 records
/ 355,400 calcs / 17,757,938 frames** (+76% records, +95% calcs, +47% frames; parser split
pymatgen.Vasprun 11,920,652 · ase.OUTCAR 5,742,711 · pymatgen.Vaspout 94,575 frames), `verify` exact.
What remains rejected is what was deposited (post-DFT outputs, crashed runs, non-VASP files, NEB
parents) plus the two documented exceptions above.

### T2 triage — CSD3 jobs 36712312 → 36792980 → 36859870 → 36932228 → 37003110 (2026-09-29 → 10-01)

Five 12 h rounds beside the T1 pipeline (`INTERVAL=1.2`, ≤ 100 deep-peek reads per archive), each
resuming from the verdict cache; the last ended 15:21 BST on 1 Oct. Standard peeks covered ~78k
files (rar never peeked); the deep peeks read 5,423 unresolved files in ~90k requests (~30 h,
request-bound at 50/min: the first protein-crystallography beamline tars spent 45-63 reads each
without completing, later files 13-22).

| T2 outcome | records | share |
|---|---|---|
| VASP evidence | 103 | 0.31% — 89 a VASP output seen, 14 only VASP-named inputs / heavy files; all open licence (licence review empty) |
| proven VASP-free | 26,855 | 81.0% |
| unresolved → not fetched (T2 has no fail-safe) | 6,217 | 18.7% — 25.7 TB (median 40 MB per record; the largest 100 hold 8.8 TB) |

Kept: **103 records** — 169.5 GB of evidenced archives + 40.7 GB of unresolved sibling archives,
~11k VASP outputs in their zip listings (largest 0.89 GB, well under the 8-core pipeline's 3.8 GB
cap); `12792088` is excluded (decision 9) → **102 to fetch**. By type: dataset 61, software 22 (VASP
test fixtures of chgnet / deepmd / LAMMPS / TDEP / BoltzTraP, name-only hits), publication 15,
model 3, other 2.

* **Where the evidence came from**: zip central directories 74, tar-family heads 21, deep peeks 8
  (4 with VASP outputs seen only inside nested zips — `19669846`, the largest at 37.5 GB,
  `17242504`, `22051980`, `22871174` — and 4 hints). The deep peeks also proved ~1,500 records
  VASP-free, but at ~11k requests per record found (T1: ~420) T2 is where they stop paying.
* **Hit rate per T2 signal** (evidence / records with the signal): workflow-named file 4.2%
  (18/432) · MLIP text 3.8% (8/210) · seed name 2.8% (16/576) · DFT text 1.4% (33/2,331) · seed
  community 1.1% (2/181) · sparse metadata + materials cue 0.89% (34/3,799) · formula 0.55%
  (18/3,298) · materials text 0.48% (15/3,131) · paper field 0.09% (20/22,082) · bulk seed account
  0/2,883 · keyword re-check 0/128. The rate rises with the number of signals (one 0.20%, two
  0.76%, three or more 2.0%). Positives have sparse metadata (median description 99 characters vs
  430 across T2) and are recent (74% created 2025-26).
* **Against T1** — its per-signal REAL yields (records with calcs after the pipeline): loose primary
  92% · VASP-named archive 58% · "VASP" in the text 46% · seed account 26% · Europe PMC 11.5% · seed
  ORCID 10.7% · paper cites VASP 7.9% · DFT + materials text 6.5% · MLIP + materials text 6.3%.
  T1's weakest signal beats T2's strongest: the tier split separates as intended.
* **Unresolved, not fetched** (decision 10), by file: gzip-tar partial heads 12.9 TB, nested zips
  3.6 TB, xz tars 3.2 TB, tars that spent the 100-read budget 2.3 TB, rar 1.3 TB (2,586 files,
  never peeked; 1,220 records unresolved only by a rar). In T1, 13 of the 230 records that yielded
  calcs (5.7%) were fail-safe ones, so T2's unresolved hide ~5 VASP records (~0.1% of them) — 25.7 TB
  of downloads for about five records.
* **False negatives**: "proven VASP-free" applies the shared fetch's own primary-name rule, so it is
  exact for what the pipeline could harvest; what it cannot see is renamed outputs, processed
  formats and records without archives (§10). Spot check (decision 11) of 12 proven-empty records
  whose listings show VASP inputs but no outputs — `22230934`, `22147013`, `20010848`, `8388390`,
  `22084010`, `17534311`, `17952154`, `12685655`, `13323474`, `19058875`, `18491981`, `18925151`:
  inputs and final structures, light outputs (`OSZICAR`, `DOSCAR`, `XDATCAR`; `18491981` publishes
  369 run directories with `vasprun.xml`/`OUTCAR` left out), processed data (CSV property tables,
  phonopy `FORCE_SETS`, DeePMD `npy`, xyz trajectories) or another code (CP2K DP-GEN `18925151`,
  GPAW `13323474`) — no renamed VASP output.
* **Europe PMC recall** (all tiers): of the 631 records VASP papers name, 90 are now in the dataset
  (48 before the census + 42 found by it), 273 proven VASP-free, 39 fail-safe downloads without VASP,
  11 evidence records that yielded no calcs, 218 outside the triage (no archive, or already evaluated
  by the keyword harvest).
* **Expected from the T2 pipeline**: at T1's per-class yields (output seen 92%, hints 17%) ~80-85
  records; ~10k calcs from the listed outputs, about half in `11234637` (a coupled-cluster benchmark
  of oxide surfaces — post-DFT VASP runs among them would be rejected by parse, as in T1).

### T2 pipeline — CSD3 job 37089987 (2026-10-01, 4 h 44 min, one round)

`IN=census_keep_t2.jsonl PARTS=10`, 8 icelake-himem cores (4 fetch + 4 parse workers, 3.79 GB
primary cap), `12792088` skipped by its `manually_excluded` line; `verify` OK (18,208,162 frames,
metadata ↔ shards exact).

| | |
|---|---|
| added | **86 records with calcs, 28,014 calcs, 450,224 frames** (dataset now 383,414 calcs / 18,208,162 frames) |
| fetch | 92 of 103 fetched (29,090 calc units); 11 rejected: `12792088` excluded, 9 hints that held no VASP output (`no_calc_units_after_extract` ×8, `no_vasp_files_fetched` ×1), and `18706703` (transient: HTTP 429 on its 16.4 GB zip, below) |
| parse | 28,014 of 29,090 units parsed (96.3%), 1,076 rejected |
| staging | peak 473 GB / 43k inodes (inside the 800 GB / 800k valve): `19669846`'s 15,300 training OUTCARs (~29 MB each) unpacked from tar.gz files nested in a 37.5 GB zip |

**Yield**: VASP output seen in a listing 82 / 89 records with calcs (92%, as in T1), hints 4 / 14
(T1 17%) — 86 records against the ~80-85 expected. Calcs came out at 2.8× the estimate because
`19669846` (spinel configurational-entropy single points, **15,273 calcs**) kept its OUTCARs in nested
tar.gz files that a peek sees only seven of; `11234637` (metal clusters on MgO with hybrid
functionals, not the feared post-DFT runs) added 4,975 — both mostly single points. Frames are
concentrated elsewhere: two batches hold 74% of them, the one with `20613933` (Ti-BEA epoxidation,
2,113 calcs) + `3585618` (171k frames) and the one with `16918762` + `4000977` + `21316138` (160k).

**Parse rejections (1,076), by cause**:

| cause | calcs | records |
|---|---|---|
| OUTCARs with a few non-UTF-8 bytes (ASE reads strictly; VASP prints an uninitialised buffer in "vdW correction parametrized for the method …" with TS/MBD + hybrids) — **parser gap, fixed 2026-10-02** | 337 | `11234637` |
| 7 KB tutorial "vaspruns" without `<generator>` (a code's example data, not VASP output) | 300 | `159046` (PROPhet) |
| post-DFT / non-SCF: MP2 natural orbitals, Wannier, GW / scGW / BSE, TDHF optics | 220 | `18604340`, `17138478`, `10844461` |
| no frame with an energy (frequency / band / incomplete runs) | 96 | `3585618`, `11234637` (M06-L slabs), `19669846` |
| VASPsol `LAMBDA_D_K=****` in `<parameters>` with no OUTCAR beside the vasprun (pymatgen raised) — **parser gap, fixed 2026-10-02** | 71 | `20403107` (70), `17254051` |
| incomplete / crashed OUTCARs, overflowed numbers, test fixtures | 49 | `19669846` (13), … |
| `parse_timeout` — three ~2.5 GB OUTCARs past the 20 min limit (non-terminal) | 3 | `21316138` |

**Recoverable** — by one `40_recover_t1.sh` run on T2 (README step 5): the 337 + 71 calcs read by the
two parse fixes (both mirror the numeric-`ALGO` guard: a file that parsed before is read exactly as
before; tests in `tests/test_parse_overflow_and_encoding.py`; the real 11234637 OUTCAR checked: Ag₄,
PBE0+TS-HI, converged), the three timeouts with `PARSE_TIMEOUT=7200`, and a re-fetch of `18706703`
(V₂O₅ charged point defects: 25 vasprun + OUTCAR pairs, 3.7 GB of outputs). The 429 burst at 19:25
cost that one record because the run finished in a single round: a transient failure is retried
only by a later run. Everything else is what was deposited. All but one of them were recovered, below.

### T2 recovery — CSD3 job 37127828 (2026-10-02, 2 h 10 min, 20 icelake-himem cores)

`40_recover_t1.sh` with `KEEP=census_keep_t2.jsonl`: a re-fetch of `18706703` (targeted zip fetch, 25
calc units, 8 min), a `--retry-rejected` parse of five records (1 h 25 min; 2 workers, budget 122 GiB,
cap 10.9 GB, timeout 2 h; RAM reservation peaked at 59.9 GiB with two big OUTCARs at once; the 5,213
calcs already stored were skipped), `verify` OK (383,849 calcs / 18,213,119 frames, exact) and
`purge-raw` (11.2 GB freed).

| record | why it was lost | recovered | frames | still rejected |
|---|---|---|---|---|
| `11234637` metal clusters on MgO | non-UTF-8 bytes in the OUTCAR (fixed in parse.py) | all 337 | 337 single points — PBE0 / revPBE + TS / TS-HI / MBD, B3LYP-D2 | 24 (21 M06-L slabs with no energy, 2 overflowed numbers, 1 incomplete — genuine) |
| `20403107` MadNEB solvation surrogate | VASPsol `LAMBDA_D_K=****` in `<parameters>` (fixed) | all 70 | together with `18706703`: 4,424 (by functional PBE+U 3,046 — presumably the V₂O₅ defects —, B3LYP 1,083, PBE/GGA 295) | 0 |
| `18706703` V₂O₅ charged point defects (new record) | HTTP 429 during the T2 pipeline | all 25 | (above) | 0 |
| `21316138` Mn₇C₃ under pressure | three ~2.5 GB OUTCARs past the 20 min timeout | all 3 | 196 | 0 |
| `17254051` IrO₂ structures | a `****` outside the parameters block | 0 | — | 1 (pymatgen) |
| **total** | | **+435 calcs / +1 record** | **+4,957** | 25 |

**T2 outcome (pipeline + recovery): +87 records / +28,449 calcs / +455,181 frames** — 87 of the 103
kept records (84%; 83 of the 89 with a VASP output seen, 4 of the 14 hints); 28,449 of the 29,115
calc units fetched were parsed (97.7%). What stays rejected is what was deposited (tutorial and
fixture files, post-DFT and incomplete runs) plus one `17254051` vasprun.

### Census outcome (T1 + T2, 2026-10-02) — part A complete

| | T1 | T2 | census |
|---|---|---|---|
| records triaged | 2,933 | 33,175 | 36,108 (+ 3,300 sample records) |
| kept by triage (keep-list) | 680 (259 evidence + 421 fail-safe), 3.09 TB | 103 (evidence only), 0.21 TB | 783 |
| fetched with VASP files | 249 | 93 | 342 |
| **records with calcs** | **230** | **87** | **317** |
| **calcs** | **173,289** | **28,449** | **201,738** |
| **frames** | **5,669,216** | **455,181** | **6,124,397** |
| records with calcs / kept | 33.8% | 84.5% | 40.5% |
| records with calcs / triaged | 7.8% | 0.26% | 0.88% |

* **Growth of the production Zenodo dataset**: 303 records / 182,111 calcs / 12,088,722 frames
  (keyword harvest) → **~620 records / 383,849 calcs / 18,213,119 frames** — records +105%, calcs
  +111%, frames +51%. A typical census record looks like a keyword one (T1 median 49 calcs / 331
  frames vs 54 / 408); frames grew less than calcs because the keyword harvest already held the
  largest trajectory sets and the census's biggest finds are single-point collections.
* **By evidence class (both tiers)**: a VASP output seen in a listing 295 / 318 records with calcs
  (93%), only VASP-named inputs 9 / 44 (20%), T1 fail-safe 13 / 421 (3.1%, but 69k calcs — almost all
  `19536185`). Seeing an output before download is what makes a kept record worth fetching.
* **Concentration**: five records hold 54% of the census calcs (`19536185` 67.5k single points,
  `19669846` 15.3k, `21895644` 9.0k, `3666992` 8.8k, `11093002` 8.8k); ten T1 records hold 61% of the
  census frames (`13843222`'s AIMD 1.21M, `11093002` 0.67M, `21855564` 0.51M, …).
* **Known misses**: `13888307` (1,347 calcs), `4541602` (86) and `12518256` (2) — three of the four
  Kavanagh datasets keyword search could not see — are in the dataset; `10630244` has no licence
  (excluded, decision 7).
* **Not recovered, by decision**: T2's 6,217 unresolved records (25.7 TB, ~5 VASP records expected),
  `22171731` (HTTP 403), `18390757`'s MD `vaspout.h5`, and T3 / T0. The seed snowball follows
  (decisions 14-16, below).
* **Cost**: about a week of calendar time from build to finish; CSD3 wall time ~4 h census + ~3 h
  scoring + ~5 h T1 triage, then the T1 pipeline (~46 h) beside the T2 triage (~57 h, request-paced),
  and ~10 h of T2 pipeline and recoveries. Requests stayed inside Zenodo's documented limits.

### Seed snowball — scoping (2026-10-02; offline, plus a 56-record live pilot)

Decision 12 asked whether the ~620 records now known to hold VASP find more as identity seeds.
Measured on the rsynced CSD3 data: `score` re-run with the 303 keyword records as seeds reproduces
the production `scored.jsonl` exactly (219 owners, 566 ORCIDs, 22 communities; the only difference
is the 343 Materials Cloud creator names the local copy lacked — 19 T2 `seed_name` rows). With all
620 dataset records the seeds are 424 owners, 966 ORCIDs, 1,747 names and 31 communities, and the
tiers become T1 3,189 · T2 32,786 · T3 344,451 · T0 200,669. `triage.decide` replayed on the cached
verdicts gives the state of every record that moved:

| the new seeds move | records | state |
|---|---|---|
| T3 → T1 (account 41, account + ORCID 15, ORCID only 92) | 148 | 144 never peeked (244 files: 228 zips, 16 tar heads); 4 from the residual sample |
| T3 → T2 (seed name / community) | 55 | 52 never peeked (71 files) |
| T2 → T1 | 338 | peeked in full by the T2 triage: 310 proven VASP-free, 28 unresolved (138 GB) |

None is in a keep-list. 24 T0 records gain a strong link but stay T0 (no identity overrules another
domain), and ~2,150 more gain a weak or redundant link without changing tier.

**Precision** (records ending with calcs, on records the T2 triage peeked in full):

| link | first hop (303 seeds) | second hop (census finds as seeds) |
|---|---|---|
| depositor account | 4 / 33 (12%) | 6 / 105 (5.7%) |
| account + ORCID | 11 / 48 (23%) | 11 / 68 (16%) |
| ORCID only | 12 / 231 (5.2%) | 1 / 183 (0.5%) |

Census finds are weaker seeds than the keyword records, their ORCIDs most of all: large
collaborations spread them (`15055758`, CoRE MOF 2024 — 20 creators, 17 ORCIDs — alone promotes 21
records). Hold-out: over
200 random halves of the 317 census finds, adding one half as seeds raises the identity reach of the
other from 35% to 48%. Of the first hop's records that identity alone lifted out of T3, 15 in 418
yielded calcs (account 8 / 61, account + ORCID 4 / 50, ORCID 3 / 307; 13,220 calcs, 1.02 M frames)
and their 74 fail-safe downloads (1.09 TB) none; across all 730 identity-only T1 records the
fail-safe downloaded 1.47 TB for one record (72 calcs). Seed filters do not separate (software-only
seeds 2.5% vs 4.0%; seeds with < 5 calcs 3.3% vs 3.6%).

**Live pilot** (the 56 account-linked T3 → T1 movers, ~70 paced requests, 4.6 min): 6 list a VASP
output — 5 real deposits, each a sibling of a census find (`16749502`, 153 OUTCARs of N₂ reduction on
Al/Fe; `3970974`, 50 outputs of probe-molecule LOBSTER runs; `17137049` "my link", 20 outputs in
43 GB of zips that are mostly heavy files; `15794580`, 2; `15064164`, 1) and one name look-alike
(`21619301`, `outcar.cpython-311.pyc`) —, 43 proven VASP-free, 7 unresolved. The largest output
listed is 0.4 GB. (These verdicts are not in the CSD3 cache; the triage reads them again.)

**Expected**: ~6-8 records with calcs (account-linked 5-6, ORCID-only ~0.5-1, T3 → T2 ~0, the T2 → T1
fail-safe ~0.2), ~200-1,000 calcs and ~5-50 k frames — +1% records, +0.1-0.3% calcs — with the first
hop's fat tail (one of those 15 finds, `11093002`, held 8,757 calcs / 674 k frames). Cost: re-score
~40 min (link tables cached; Europe PMC re-queried), triage ~2-3 k requests ≈ 1 h (~400 standard
reads; deep peeks of the T3 movers; ~1.8 k deep reads for the T2 → T1 movers, whose T2 verdicts are
keyed by the 100-read budget), one pipeline round with ~200-300 GB of fail-safe downloads.

**Other channels, measured** (not run, decision 16): other records of a seed's paper DOI (5),
co-cited with a seed by DataCite / Europe PMC papers (23) and Zenodo-to-Zenodo related identifiers
(9) — all but one already triaged or keyword-evaluated, so the paper graph is used up; seed names
resolved to ORCIDs 0 / 24 with evidence; co-authors' ORCIDs 0 / 1,308 with calcs; depositors and
ORCIDs of records linked to VASP papers 1.0% (11 / 1,151) on triaged records the seeds miss → 1,019
T3 records, perhaps 1-5 finds. NOMAD and Materials Cloud provenance hold author names only.

**Freshness is the larger lever**: the dataset holds 30 / 36 / 32 VASP records created in July /
August / September 2026 (to the 25th), among ~38 k archive-bearing records a month — each month
after the census cut-off holds several times the snowball's yield. A refresh re-scores with the
then-current seeds; `select-moved` also selects records the earlier scoring never saw.

### Seed snowball — triage and pipeline (CSD3, 2026-10-02)

**Re-score + selection**: `20_score.sh` as predicted (424 owners, 966 ORCIDs, 2,078 names with the
Materials Cloud ones, 31 communities; T1 3,191 · T2 32,802 · T3 344,433 · T0 200,669); Europe PMC
added 5 papers. `select-moved`: 541 records (149 T3 → T1, 339 T2 → T1, 53 T3 → T2; the two extra
T1s are records the new papers name — both proven VASP-free).

**Triage** (`SCORED=scored_snowball.jsonl`, 23 min): 316 standard peeks + 56 deep peeks (1,399
requests; 39 proofs, no finds). Of the 56 records piloted from the local machine, CSD3 agreed on every
evidence and empty verdict. Kept **51 records** (licence review empty, none already in the dataset or
an earlier keep-list): 9 on evidence (55 GB of evidenced archives) and 42 by the T1 fail-safe —
all 28 T2 → T1 records the T2 triage had left unresolved plus 14 T3 → T1 (166 GB, 32 of them linked by
an ORCID only); 485 proven VASP-free, 5 T2 movers unresolved and not fetched.

**Pipeline** — job 37197208, one round, 2 h 58 min (`PARTS=5`, 8 icelake-himem cores, staging peak
57 GB), `verify` OK (386,425 calcs / 18,243,690 frames, metadata ↔ shards exact):

| record | found by | calcs | frames | note |
|---|---|---|---|---|
| `3970974` probe molecules (LOBSTER) | account | 2,008 | 12,014 | the zip listed 50 outputs; 1,984 units came from its nested `.tar.gz` files |
| `18891007` Na₂WO₄/SiO₂ catalysts | ORCID, **fail-safe** (`.rar`) | 247 | 5,440 | the one fail-safe record with VASP (1 of 42) |
| `16749502` N₂ reduction on Al/Fe | account | 153 | 7,646 | relaxations + ZPE |
| `17643350` Cu/Ag C–N coupling | ORCID | 125 | 4,007 | |
| `15161709` birefringent materials | ORCID | 21 | 21 | band-structure single points |
| `17137049` "my link" | account | 10 | 10 | 20 outputs pulled from 43 GB of zips |
| `11181410` PACMAN | ORCID, hint | 9 | 1,430 | VASP runs inside nested `.tar.gz` |
| `15794580`, `15064164` | account | 2 + 1 | 3 | |
| **total** | | **2,576** | **30,571** | **+9 records** → 629 records |

Rejected: 41 fail-safe records held no VASP (38 `no_vasp_files_fetched`, 3 inputs only), and
`15654431` / `17239454` logged 201 `extract_error`s for PyTorch `.pth.tar` checkpoints (zips, not
tars). Parse rejected 10 of 2,586 units: `21619301`'s 5 `outcar*.py[c]` files (the name look-alike
triage flagged) and 5 runs that never started — their OUTCARs hold no SCF `Iteration` (ASE's
"Incomplete OUTCAR") and their vaspruns end before `<generator>` / `<parameters>` (`18891007` ×3) or
hold no ionic step (`17643350`, `3970974`). **No recovery run**: nothing transient, too large or timed
out, and nothing a parser change could read.

**Against the scoping estimate** (~6-8 records, ~200-1,000 calcs, ~5-50 k frames): 9 records — the
ORCID-only stratum gave 3 (estimate ~0.5-1) and the fail-safe 1 (~0.2), small-number luck inside the
measured intervals; frames inside the range; calcs 2.6× the top of the range because the estimate
counted the outputs the peeks listed, and nested archives hide most of theirs (`3970974`: 25 listed,
2,009 fetched — as `19669846` in T2). Listings with nested archives are lower bounds.

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

**Deep peeks, 2026-09-29.** An independent review of `deeppeek.py` + its triage integration found
11 defects, all fixed with regression tests (`tests/test_zenodo_census_deeppeek.py`) and a
randomized check against tarfile / zipfile / py7zr ground truth (1,400 archives and truncations, no
false proof): FALSE PROOFS from a 7z entry stored without a name (py7zr names it after the file —
now given the file's name, and a nameless entry with no name known leaves the listing partial), a
member >= 8 GiB whose real size sits in a PAX `size` record, GNU sparse members and sized directory
headers (both walks now follow tarfile's own hop rules — `walk_tar` too), an AppleDouble `._` sidecar
stopping a walk; a py7zr SPIN on a corrupt packed header (the header is now decoded in a child
process killed after 30 s, memory-capped, fed by the parent's paced reads); a transient failure
inside a nested walk cached as a verdict; reads outside the byte budget (the outer central
directory — now read through it via zipfile — and failed nested walks); inverted Range requests;
the `.aiida` extractor dropped by the overlay; a zip's own top-level VASP inputs counted as new
evidence; and `20_pipeline.sh` accepting a job too small for its parse budget (now refused for any
worker count; successors keep `--mem`).

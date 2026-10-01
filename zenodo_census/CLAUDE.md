## Zenodo census (`zenodo_census/`) — FURTHER_WORK part A

Not a new data source: a **replacement discovery front-end for Zenodo** that finds the VASP deposits
keyword search cannot see, and hands the ORDINARY Zenodo pipeline a standard keep-list (same fetch,
parse, store, schema and calc_ids `zenodo:<recid>:<path>` as the existing dataset). Design,
measurements and user decisions: `docs/ZENODO_CENSUS.md`; CSD3 runbook: `scripts/csd3/census/`.
Status 2026-09-29: census COMPLETE on CSD3 (583,930 records incl. all 303 dataset records; the first run
died on a record Zenodo's default JSON serializer cannot return, `20797668` — now routed around losslessly,
below), SCORED (T1 2,933 · T2 33,175 · T3 344,635 · T0 200,669) and T1 TRIAGED: 252 VASP-evidence records
(all 4 known misses), 2,091 proven VASP-free, 590 fail-safe (3.38 TB); residual T3 0/3,000 → T3 stops
(`docs/ZENODO_CENSUS.md` §11). T1 RE-TRIAGED with the deep peeks (job 36712258, 1 h 51 min): +136
proven VASP-free, +20 VASP found → 272 evidence / 434 fail-safe, keep-list 680 records / 3.09 TB
(−0.87 TB). T1 HARVEST DONE 2026-10-01 (pipeline + recovery job `scripts/csd3/census/40_recover_t1.sh`: +230 records /
+173,289 calcs / +5.67M frames → dataset ~533 records / 355,400 calcs / 17.76M frames, verify OK; §11); T2
triage finishing; next the T2 report review, then the T2 pipeline.

Why (measured live 2026-09-25): Zenodo's `q` sees metadata text only; beyond that, an `&nbsp;` glues
words into one token (`4541602` "ab initio&nbsp;defect" matches neither), quoted phrases are not
stemmed (every plural of `discover.DEFAULT_QUERIES`' phrases is missed), sparse metadata (`13888307`)
has no computation word at all, and the keyword discover's cut-off is 2026-07-30.

- Stages (each resumable; CLI `python -m zenodo_census.cli {census,links,resolve,openalex,score,triage,status}`,
  data under `$ZENODO_CENSUS_DATA`, default `<ZENODO_HARVEST_DATA>/census`):
  - `census.py` — **census**: `CENSUS_QUERY` = any archive (`files.entries.ext:(zip OR gz OR …)`) or a
    loose VASP primary (`files.entries.key:(*OUTCAR* …)`) → 583,443 records, all resource types, at
    page size 100 (token; 25 anonymous) ≈ 5.8k pages ≈ 3.4 h. `CensusClient` paces request STARTS
    (2.2 s; the shared client waits after each response, which doubled the run). `slim_hit` keeps what
    `Candidate.from_record` reads + `owners` (depositor account), ORCIDs, communities, related ids,
    references, notes. **Serializer-proof** (2026-09-26): a page that still fails after the retries is
    first checked against a health probe (a page past the end: `hits.total`, no hit serialized) — an
    outage is waited out (60 s doubling, ≤ 1 h, then `ZenodoOutage`, resumable), never taken for bad
    records; otherwise it is split into aligned sub-pages (100 → 10 → 1) and each record that still
    fails is read from InvenioRDM's native serializer (`Accept: application/vnd.inveniordm.v1+json`),
    rewritten by `legacy_from_native` (exact for every kept field — checked on live pairs, fixture
    `tests/zenodo_serializer_pairs.json`), tagged `_serializer: "inveniordm"` and logged to
    `census.jsonl.poison.jsonl` (summary `poison_converted`/`poison_unresolved`); `count` and
    `search_page` (used by `resolve`) fall back the same way. Leaf-window sentinels (`census.jsonl.windows.jsonl`) for resume, the first
    run's `created` range fixed in `census.jsonl.bounds.json` (a later-day resume must bisect the
    same windows); `truncate_torn_tail` cuts a torn last line before ANY append (used by every
    writer in the package — census, links, peek cache); `iter_census` = newest version per concept,
    its last copy (a cheap id-prefix pass, then a streaming pass); `census_keys` = every recid+concept.
  - `signals.py` — pure offline signals + `tier_of`: text (HTML stripped, entities decoded, plural-
    safe phrases; VASP / DFT / MLIP / specific-materials vocabularies; `AMBIGUOUS_DFT` single words
    need a second cue; chemical formulas with a lower-case letter), negatives (other-domain words and
    file types), identities vs `Seeds` (depositor account, ORCID, distinctive name ≤ `NAME_DF_MAX`
    census records and only beside a content cue, community ≤ `COMMUNITY_MAX`; accounts owning
    > `OWNER_BULK` records count weakly), files (loose primary; VASP-named / workflow-named archives),
    paper DOIs (related ids, description hrefs, notes, references, arXiv → `10.48550/arxiv.*`; data-
    repository DOIs dropped), licence classes (`open`/`nc` admitted, `nd`/`none` → review).
    Terms are counted as stems (`term_stem`). Formulas by a linear segmenter (`_formula_symbols`;
    a single regex backtracked exponentially). Name-level cues ("VASP", VASP-named files, seed
    depositor/ORCID) are T1 only on records that are not negative-dominated, else T2. Tiers T1
    strong / T2 plausible / T3 low / T0 another domain.
  - `links.py` — network channels, cached: DataCite events (46k Crossref→Zenodo references; cursor
    resume), Europe PMC full-text mentions (~592 VASP papers; PMCID or `PPR` preprint id; per-paper
    resume), `resolve_versions` (cited VERSION ids → concepts, batched all-versions searches),
    OpenAlex singleton lookups (free; `cites_vasp` = references ∩ `VASP_WORKS`, primary-topic field).
    Anonymous sessions only — never the Zenodo token or a contact e-mail.
  - `score.py` — `Exclusions` (dataset records + everything the keyword harvest evaluated:
    `candidates_full`/`keep`/`nc_candidates`/`nc_keep`/`byid_10579527_keep`; NOT
    `candidates_nolicense`, which holds post-cut-off records never evaluated; dropped candidates the
    old triage could not examine — `needs_recheck`: archives invisible to `models.ARCHIVE_EXTS`, or
    zips ≥ 100 MB under its 16-bit-count bug — are re-checked, matched on recid OR concept, and always
    peeked: one the signals put in T3/T0 is promoted to T2 (`keyword_recheck`, `tier_by_signals` kept);
    the CLI refuses to score without the
    exclusion inputs unless `--allow-missing-exclusions`), `build_seeds`,
    `record_signals`, `lookup_dois` (only papers of still-T2/T3 records), `score` → `scored.jsonl` +
    `score_report.json` (tier × type counts, peek workload, `PROBE_RECIDS` = the 4 Kavanagh misses,
    `epmc_coverage` = the keyword method's recall check).
  - `headpeek.py` — first 8 MB of a tar-family stream in ONE Range read: magic-byte sniffing (a zip /
    7z / rar in disguise is reported), streaming decompression under a 128 MB cap (trailing padding
    tolerated), tar header walk (ustar prefix, GNU long names, PAX path, base-256 sizes, checksums).
    `complete` = the whole file read, every stream ended (multi-stream followed), nothing capped AND
    the tar end block reached (then "no VASP" is proof); else leading members = evidence. Signed
    checksums accepted, strict octal sizes (a bad header stops the walk — no hang), regular files
    only, zstd bounded, anchored VASP-name / exact heavy-name rules (no `incarnation.txt`/`chgnet`).
  - `deeppeek.py` (2026-09-29) — lists what the standard peeks cannot, without a download: `tar_walk`
    (uncompressed tar, header to header over Range; read size doubles along small members, back to
    64 KiB after a big one; stops at the first VASP primary), `sevenzip_peek` (py7zr on the end header
    through `RangeFile`, a budgeted Range-backed file), `nested_zip_peek` (archives inside a zip: each
    inner member's local header + first 2 MiB in one read, inflated, then head-peeked /
    `zip_local_names`-walked / listed via its own directory when stored; VASP-hinting names first,
    ≤ 40). Budgets per archive: 300 reads (`DEEP_MAX_REQUESTS`), 256 MiB, no read over 64 MiB —
    every read goes through one `_Budget` (streamed, capped; the outer central directory is read
    via zipfile over the `RangeFile`; inverted ranges refused). Tar hops follow tarfile exactly
    (PAX `size`, GNU sparse = a file, links/dirs carry no data; `headpeek.walk_tar` too); py7zr runs
    in a forkserver child killed after `SEVENZIP_TIMEOUT` (30 s), memory-capped, reading through the
    parent; `RangeFile.name` = the file name (py7zr names nameless entries after it, as fetch does).
    `complete` (the only proof of "no VASP") needs an end block / exact EOF / a decoded 7z header /
    every nested archive listed exactly — anything else is partial. Failures are status strings
    (`peek_failed: …` transient, never cached). Review regressions: `tests/test_zenodo_census_deeppeek.py`.
  - `triage.py` — selection (T1+T2, seeded T3 residual sample (not software), T0 control sample; skips
    records in earlier keep-lists), peeks under ONE `Pacer` shared by all threads (`PacedSession`:
    token only to zenodo.org; 0.8 s between request starts = 4.5k/h), zip peeks via
    `materials_cloud_harvest.remote_zip.peek_archive(size=…)` (ZIP64; known size → ordinary range,
    because Zenodo breaks over-long suffix ranges), head-peeks, zip⇄tar fallback with a declared
    `archive_kind` when the name lies, one retry, a shared verdict cache (`peeks.jsonl`); `decide`:
    keep on positive evidence (+ unresolved siblings), T1 also fail-safe on unresolved, prune
    proven-empty archives; ND / no-licence → `<out>.licence_review.jsonl`. `archive_kind` declared
    whenever the content disagrees with the name (zip⇄tar, zstd, 7z/rar in disguise, `.tbz`, AiiDA
    sqlite under any name); a plain zip with a stray `db.sqlite3` is judged by names; failed
    fallbacks are never cached; cache keys include the head size; earlier keep-lists excluded by
    recid AND concept. Writes `<out>` (Candidate dicts, files pruned, `census` bookkeeping),
    `<out>.report.json` (Wilson-CI residual rate — all positives AND `rate_primary`, the strict
    go/no-go — + extrapolation, yield per signal, probes; per record `evidence` strength,
    `bytes_evidence`/`bytes_blind`, `deep`), `<out>.rejections.jsonl` (rewritten). **Deep peeks**
    (`deep_peek`, default on; CLI `--no-deep-peek`/`--deep-max-requests`/`--nested-max-members`):
    records left `strong_unresolved` (T1 fail-safe: a proof of "no VASP" saves the whole download)
    or `unresolved_not_fetched` (T2: a find is recall gained) get `deeppeek` verdicts for their
    unresolved files (`deep_kind`: uncompressed tar / 7z incl. in disguise / zip nesting archives),
    cached under separate keys (`DEEP_RULES_VERSION`, budget), overlaid on the standard verdict
    (`_deep_verdict`, `as_kind` kept). Whatever stays unresolved in a T1 record is downloaded whole,
    NO size cap (user decision 2026-09-29, as the keyword harvest).
- Shared-code changes (backward-compatible): `materials_cloud_harvest/remote_zip.py` —
  `read_central_directory`/`peek_archive` accept `size=` (ordinary-range tail read) and report a body
  that breaks mid-read as a failed peek instead of raising; `zenodo_harvest/client.py` —
  `ZenodoClient._get(headers=…)`, passed to the session only when given (the native-serializer
  `Accept`); `zenodo_harvest/models.is_reusable_license` treats `other-closed` as not reusable;
  `remote_zip.ZipEvidence.primary_max_bytes` (largest primary per zip, for parse-RAM sizing);
  `zenodo_harvest.cli` `parse`/`pipeline` take `--parse-workers/--parse-mem-budget/--parse-rss-ratio`
  (defaults = serial, as before) and `scripts/csd3/20_pipeline.sh` sizes them from the job's RAM.
- Offline tests: `tests/test_zenodo_census.py` (census resume/dedup/torn lines/pacing; signals incl.
  the real metadata of the three hidden Kavanagh records; exclusions/seeds/score/EPMC coverage; link
  parsers + resume; head-peeks over real tar/gz/bz2/xz/zst/GNU/PAX streams; Zenodo's broken suffix
  range reproduced; triage verdicts, decisions, samples, cache reuse; and census → score → triage →
  SHARED fetch → pymatgen/ASE parse → verify with calc_ids/provenance checked).

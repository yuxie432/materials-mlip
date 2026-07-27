# How much accessible + relevant VASP data is on Zenodo? (measured 2026-07-21)

Estimate produced with the harvest scripts, verified against the live Zenodo API.
Numbers below use the **fixed** default queries (see "Query fix"). Reproduce with the
measurement tools in [`scripts/estimate/`](../scripts/estimate/README.md).

## TL;DR (across ALL of Zenodo, current scripts)

| Question | Answer |
|---|---|
| Discoverable relevant **records** (rank ≥ 3) | **629** candidates (9 `vasp_direct` + 620 `archive`) |
| …that **genuinely contain a VASP primary output** | **~107** (95% band 62–175), measured by peeking zips + downloading a sample of tar/rar/7z |
| **Raw footprint** of the 629 | **2.21 TB** (heavy-tailed — top 100 = 92%, largest single = 186 GB) |
| **Download/transfer** to harvest | **82 GB → 1.96 TB** by per-file cap; **less with peek** (junk zips dropped pre-download) |
| **Final `extxyz.gz` + `metadata.jsonl` dataset** | **~15–75 GB** — fits 1 TB with huge margin |

All are **lower bounds** (metadata-only search can't see VASP data with silent metadata).

## Query fix (the big correction)

Zenodo's default boolean operator is **OR** (confirmed live: `VASP OUTCAR` ≡ `VASP OR
OUTCAR` = 333 hits, vs `VASP AND OUTCAR` = 29). Two default queries were therefore
broken and have been rewritten:

| # | before | after | effect |
|---|---|---|---|
| broad | `"ab initio" molecular dynamics forces` → **52,886** hits, **4%** VASP-signal (worm-tracking, genomes, GC-MS…, verified 0/54 zips VASP) | `("ab initio molecular dynamics" OR AIMD) AND (VASP OR forces OR OUTCAR)` → **29** hits, **90%** signal | removes a 50k-record junk flood |
| MACE | `MACE OR NequIP OR Allegro OR GAP AND training data` (malformed OR/AND, bare acronyms) → 364 hits, **0%** signal | `(MACE OR NequIP OR Allegro OR "Gaussian approximation potential") AND (VASP OR DFT OR forces OR "training data")` → 63 hits, **88%** signal | 0% → 88% precision, *more* real hits |

The other 10 queries were reviewed and are sound (single tokens, quoted phrases, or
explicit `AND` anchors — e.g. `"machine learning" AND (…) AND DFT` at 98% signal).
**Rule now documented in `discover.py`:** every multi-word intent must be a quoted
phrase or `AND`-joined, and every acronym `AND`-anchored to a VASP/DFT term.

## Discovery census (exact — the API returns every file's size)

Fixed queries → **925 unique concepts** (gates dropped 75 non-open, 58 NC/ND/no-license):

| category | n | note |
|---|---|---|
| `vasp_direct` | 9 | primary output exposed directly |
| `archive` | 620 | has an archive; VASP confirmed only by peek/fetch |
| `processed_atomistic` | 50 | extxyz/xyz/traj — **not ingested** (no reader) |
| `vasp_input_only` | 22 | inputs only |
| `unlikely` | 224 | no VASP/archive/atomistic files |

→ **Relevant (rank ≥ 3) = 629**, raw **2.21 TB**. By format (uncapped download): zip
1.37 TB, tar 540 GB, **zst 108 GB**, direct 28 GB, rar 24 GB, 7z 8 GB (**rar/7z/zst all
now extractable via the `archives` extra**). NB the `.tar.zst`/`.zst` line was previously
**missing from the download census and un-fetchable** — see "Corrections" below.

## Precision — how much *actually* has a parseable VASP output

Method: bucket the 629 relevant records by how their VASP content can be confirmed;
bucket **sizes are exact** (from the manifest); the **VASP-content rate** per bucket is
sampled and reported with a Wilson 95% CI. "Has VASP" here means a **primary output**
(vasprun/OUTCAR/vaspout) — the thing parse can actually turn into frames — not merely
an input file (POSCAR/INCAR). (My earlier "24%" counted *any* VASP file incl.
input-only; the parseable-primary rate is lower.)

| bucket | records | raw | how confirmed | primary-VASP rate |
|---|---|---|---|---|
| A direct | 9 | 34 GB | already exposed | 100% |
| B peekable `.zip` | 437 | 1.39 TB | HTTP-Range peek (n=89) | **12%** (CI 7–21%) |
| C unpeekable tar/rar/7z | 164 | 531 GB | **downloaded** a sample (n=26) | **27%** (CI 14–46%) |
| giant (>20 GB, unpeekable) | 19 | 306 GB | not sampled | unknown |

The peekable-zip "no-VASP" records are genuinely non-VASP (spot-checked: ABINIT/JTH
PAW files, DeePMD `.raw`, structures.xyz, CSVs, cell-tracking) — not nested archives.
tar/rar/7z score higher because people commonly `tar.gz` a whole VASP run directory.

⇒ **parseable-VASP ≈ 107 records (95% band 62–175)** = 9 + 12%·437 + 27%·164. A full
no-cap fetch+parse on CSD3 nails the exact number.

## Storage

**Download vs per-file cap** (exact; the dominant lever):

| cap / file | transfer |
|---|---|
| 0.5 GB | 82 GB |
| 2 GB | 275 GB |
| 10 GB | 864 GB |
| uncapped (`--max-bytes 0`) | 2.06 TB |

With **peek ON (now default)** the ~76% of zips proven non-VASP are dropped *before*
download, so real transfer is well below these gross figures.

**Ratios** (measured by real fetch+parse, 79 calc units / 2,812 frames):
~**10.6 KB/frame** (`extxyz.gz`), dataset ≈ **3.7% of downloaded bytes**, extract ratio
≈ 1.0 (extracted VASP ≈ archive size; archive deleted after extraction).

**Final dataset**: transfer × ~3.7% ⇒ **~15–75 GB** across the whole cap range → **1 TB
is far more than enough** for the extxyz output. Only the *transient* download exceeds
storage, and archives are deleted on extraction, so with peek-filtering + periodic
`purge-raw` (or the per-array-task flow) peak disk stays a fraction of 1 TB.

## Recommendation

- **No file cap** (`--max-bytes 0`): the output is tiny; capping only loses data in big
  archives. Keep the per-member cap (`--max-member-bytes`, default 2 GB) as a
  decompression-bomb guard, or raise it for very long AIMD trajectories.
- Peek is now **on by default** and drops proven-non-VASP zips pre-download.
- Pipeline **fetch → parse → purge-raw** in batches (or per array task) so transient
  extracted staging never accumulates.
- Run the exact full census + a larger no-cap storage sample on CSD3 (`README.md`).

## Corrections (2026-07-22 codebase review)

A review for systematic data-miss bugs found and fixed three:

1. **`.zst`/`.tar.zst` archives were silently unreachable.** `models.ARCHIVE_EXTS`
   classified them as `archive` (so they counted toward the 629 + 2.21 TB), but
   `fetch._is_archive` did not recognise them, so fetch downloaded nothing and rejected
   the record as `no_vasp_files_fetched`. This lost **≥4 relevant records / ~113 GB**
   whose names (`training_data.tar.zst`, `dft-reference-elfs-…tar.zst`,
   `…vasp_data.z0N`, `mc_traj.tar.zst`) indicate real VASP/DFT content — and it
   under-counted the download census by ~108 GB. **Fixed:** fetch now streams
   `.tar.zst`/`.tzst`/bare-`.zst` through `zstandard`+`tarfile` (added to the `archives`
   extra); split/spanned parts (`.z0N`) now emit a visible `archive_multipart_unsupported`
   rejection instead of vanishing.
2. **Nested-archive false negative in triage.** A peekable `.zip` containing only a
   *nested* archive (whose members the central-directory peek can't see) and no visible
   `vasprun`/`OUTCAR` was "negatively confirmed" and **dropped**. **Fixed:** a peeked zip
   holding a nested archive is now an evidence gap (kept for fetch to decide).
3. **Storage-sample size bias.** `sample_storage.py`'s budget filter walked
   size-ascending and skipped over-budget records, dropping the entire large-size tail —
   biasing yield and frames/record *down* (both rise with archive size). **Fixed:** the
   sample is now thinned evenly across the size range, drops only the few largest that
   overflow the hard budget, and reports what the budget cost.

## Caveats

Recall ceiling (metadata-only); `type=dataset` filter excludes ~24% of clean
VASP-metadata records (software/publication/other — see the recall note below);
`processed_atomistic` (51 rec / 95 GB) not ingested; single-day >10k window truncation
in exhaustive discovery (moot for the current small precise queries); tar/rar/7z/zst
confirmable only at download; multi-part spanned archives not reassembled.

**Bigger picture:** Zenodo is the *long tail* of DFT data (paper-attached deposits). The
large reusable VASP corpora live in dedicated repositories — Materials Project, NOMAD,
OQMD, AFLOW, Materials Cloud — which dwarf Zenodo by orders of magnitude. If the goal is
dataset *volume*, adding one of those sources will move the needle far more than any
Zenodo-side recall tweak.

---

# SECOND SURVEY — expanded keywords + all resource types (measured 2026-07-24/27)

Re-run after the query set was **expanded** (`discover.py`: 20 queries built from
computation×materials and MLIP×materials chunk cross-products, vs the 13 focused queries
above) and extended to survey **all four Zenodo resource types**. Method identical
(discover → census → triage-peek sample with Wilson 95% CIs → bucket-C sample download →
one fetch+parse ratios sample). This section is ADDITIVE — the 2026-07-21 numbers above
stand as the previous baseline.

## Effect of expanding the keywords (`dataset` only, old vs new)

| Metric | OLD (focused) | NEW (expanded) | change |
|---|---|---|---|
| Unique concepts | 925 | 3,596 | ×3.9 |
| Relevant (rank≥3) | 629 | 2,391 | ×3.8 |
| Raw footprint | 2.21 TB | 13.62 TB | ×6.2 |
| Gross transfer (uncapped) | 2.06 TB | 12.17 TB | ×5.9 |
| **B-rate** (primary in peekable zips) | 12% | **5.3%** | ~halved |
| **C-rate** (primary in other archives) | 27% | **25%** | ≈same |
| **Est. records w/ VASP primary** | ≈107 | **≈270** | ×2.5 |
| Precision (primary ÷ relevant) | ~17% | ~11% | ↓ |

**Verdict:** the expansion is a genuine recall win (~2.5× more parseable-VASP records) but
costs ~6× the I/O and roughly halves the peekable-zip hit-rate — the extra generic anchors
(`materials`/`crystal`/`DFT`/`phonon`/…) pull in a large low-precision tail. C-rate is
keyword-independent (people still tar whole run dirs). Keep the expansion; lean harder on
peek-filtering + the license/access gates to control the download bill.

## Per-resource-type results (gates ON; disjoint sets)

| | dataset | publication | software | other |
|---|---|---|---|---|
| Unique concepts | 3,596 | 14,527 | 1,003 | 285 |
| — `unlikely` (junk) | 1,021 | 13,711 | 132 | 169 |
| Gates dropped (access/license) | 355 / 404 | 1,145 / 2,666 | 43 / 68 | 15 / 42 |
| **Relevant (rank≥3)** | **2,391** | **733** | **865** | **115** |
| A — direct | 13 | 1 | 1 | 4 |
| B — peekable-zip | 1,715 | 600 | 775 | 95 |
|  ↳ B primary-rate (CI) | 5.3% (3.0–9.0) | 5.1% (2.7–9.4) | 6.4% (3.6–11.1) | 3.8% (1.3–10.6) |
| C — other archive | 663 | 132 | 89 | 16 |
|  ↳ C yield (sample) | 25% (6/24) | 12.5% (3/24) | 16.7% (4/24) | 0% (0/11) |
| **≈ records w/ VASP primary** | **≈270** | **≈48** | **≈66** | **≈8** |
| Raw footprint | 13.62 TB | 1.30 TB | 323 GB | 192 GB |

Combined (all 4): **4,104 relevant, 15.4 TB raw, ≈392 records with a parseable VASP
primary** (~234–759). Necessity for the harvest: **software** = clear win (best useful
records per byte); **publication** = low-yield but +48 real records (expensive discovery,
94% PDFs — worth it for recall, gates+peek bound the cost); **other** = optional (≈8
records; cheap to include). `resource_type` is recorded end-to-end (fetch→parse→metadata),
so sources can be weighted/filtered at train time.

## Peek-aware transfer (the transfer that actually has to be DOWNLOADED)

`census.py` sums *all* archive bytes, but `triage --peek` DROPS zips it proves hold no
VASP before any download. The real transfer = census of the **peek-kept** set. Measured by
running the real `triage --peek` over all `dataset` zip records then censusing the keep-list
(1,268 kept of 2,404; **1,136 dropped**):

| per-file cap | gross transfer | **peek-aware transfer** | saved by peek |
|---|---|---|---|
| 0.5 GB | 336 GB | **176 GB** | 48% |
| 2 GB | 1.47 TB | **829 GB** | 45% |
| 10 GB | 5.51 TB | **4.51 TB** | 18% |
| **uncapped** | 12.17 TB | **11.17 TB** | **8%** |

**Key finding (counter-intuitive):** peek drops ~47% of records but only ~8% of *uncapped*
bytes — the dropped negatives are overwhelmingly **small** (kept-set median jumps 247 MB →
1.27 GB). The byte mass lives in (a) large zips peek keeps (nested/ZIP64/confirmed) and (b)
non-peekable tar/7z/zst/rar (**4.65 TB, all kept, cannot be Range-peeked**). So peek's
byte-saving is large only under a per-file **cap** (≈45–48% at ≤2 GB), because the dropped
small negatives are mostly sub-cap. **Implication:** if the uncapped ~11 TB transfer is too
much, a 2 GB cap cuts it to ~0.83 TB peek-aware while losing only the longest trajectories —
a deliberate recall/bandwidth trade for the mentor to weigh.

## `dataset` summary (peek-aware transfer; the headline table)

| quantity | value |
|---|---|
| Relevant records (rank≥3) | 2,391 |
| — directly-exposed VASP (A) | 13 |
| — peekable-zip archives (B) | 1,715 (~5.3% have a primary → ≈91) |
| — other archives (C, tar/rar/7z/zst) | 663 (~25% have a primary → ≈166) |
| **Est. records with a parseable VASP primary** | **≈270 (range ~165–520)** |
| Raw footprint (all relevant) | 13.62 TB |
| **Transfer to download (peek-aware), uncapped** | **≈11.2 TB** |
| Transfer (peek-aware) @2 GB / @10 GB cap | 0.83 TB / 4.51 TB |
| Final `extxyz.gz` dataset (rough, thin sample) | order 1–100 GB — **fits 1 TB easily** |

## Code changes made this session (all: 99 tests + mypy + ruff green)

1. **Peek small-file underflow FIX** (`triage.py`) — zips < the 64 KiB Range tail hit a
   Zenodo suffix-range offset underflow (`IncompleteRead`); ~15% of zip records were
   silently un-peekable. Now falls back to a whole-file GET on the broken read. This is
   what made the peek-aware census above reliable.
2. **`fetch --max-disk-bytes` / `--max-disk-files`** — the staging budget (bytes *and*
   inodes), enforced on actual usage: every byte/file is charged as written and refunded on
   delete, so no decompression-ratio estimate is involved (measured expansion here spans ~1×
   to 4.1× on real records). On breach the record is rolled back whole and fetch stops
   cleanly, for paced fetch→parse→purge→resume.
3. **`fetch --workers N` (default 4)** — parallel record downloads sharing that budget
   (thread-safe), so peak disk respects it regardless of concurrency. ~single-stream
   20–35 h → ~3–6 h for the multi-TB pull. (Zenodo global limits 100 req/min, 5000 req/hour
   authenticated; 30/min is search-only; CSD3 login nodes cap test runs at 4 CPUs.)
4. **`pipeline` command** (`pipeline.py`) — overlaps fetch(batch i+1) with parse+purge(batch
   i); disk-paced; one command for stages 2–4. Run commands live in the harvest memory note.

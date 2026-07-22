# How much accessible + relevant VASP data is on Zenodo? (measured 2026-07-21)

Estimate produced with the harvest scripts, verified against the live Zenodo API.
Numbers below use the **fixed** default queries (see "Query fix"). Reproduce with the
tools here (`README.md`).

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

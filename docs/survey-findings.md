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

---

# THIRD INVESTIGATION — remote archive-peek (tar-header walk), archive-type census, VASP-archive structure & discovery precision (2026-07-29)

**Motivation.** The mentor proposed reading `.tar` member headers remotely over HTTP Range
(seek each 512-byte header, skip `512 + ceil(size/512)*512` to the next) to peek/target
`.tar` archives the way `triage --peek` already peeks `.zip` central directories. This section
measures whether that is worth building, characterises the unpeekable-archive population, and
uses the same data to tighten discovery precision. Method: a bounded live discover
(`--max-records 3000`, current expanded default queries, `dataset`, gates on → 3,000 concepts,
**2,008 rank ≥ 3**), a fine-grained archive-type census, and a live remote header-walk of the
largest uncompressed `.tar`. Tooling in `scripts/estimate/` + one-off scripts.

## Mechanism & the zip/tar asymmetry

- **ZIP** has a *central directory* at the file tail listing every member's name, size,
  compression method, **and byte offset** → random access. One Range read enumerates the whole
  archive regardless of member count (this is what the existing `peek_zip_filenames` exploits).
- **TAR** has *no index*. Members are header+data blocks concatenated; to reach member *N* you
  must read headers 1…*N*−1 from the front. Enumeration is therefore **O(members) requests**.
- **Compression kills the tar walk.** `.tar.gz`/`.bz2`/`.xz`/`.zst` are non-seekable streams —
  the 512-byte structure exists only after full decompression, so a walk = a full download.
  Only *uncompressed* `.tar` is header-walkable.

## Archive-type census — proportions among UNPEEKABLE (non-zip) archives

Refines the earlier surveys' single lumped **"tar"** line into compressed vs uncompressed.
Over the 2,008 rank ≥ 3 records: **9.08 TB of archives; unpeekable (non-zip) = 2,089 files /
4.19 TB** (close to survey #2's 4.65 TB, as expected for a 3k-cap subset of the same queries).

| archive type | files | % unpk files | bytes | % unpk bytes | walkable? |
|---|--:|--:|--:|--:|:--|
| **tar (uncompressed)** | 360 | **17.2%** | **1.24 TB** | **29.7%** | ✅ header-walk |
| tar.gz / tgz | 1042 | 49.9% | 2.02 TB | 48.1% | ❌ compressed |
| bare `.gz/.bz2/.xz` (as tar) | 371 | 17.8% | 362.9 GB | 8.5% | ❌ compressed |
| tar.xz | 89 | 4.3% | 178.7 GB | 4.2% | ❌ compressed |
| 7z | 37 | 1.8% | 150.8 GB | 3.5% | ❌ (indexed, not tar) |
| tar.bz2 | 56 | 2.7% | 80.1 GB | 1.9% | ❌ compressed |
| tar.zst | 11 | 0.5% | 77.6 GB | 1.8% | ❌ compressed |
| rar | 122 | 5.8% | 55.1 GB | 1.3% | ❌ no seek |
| bare `.zst` | 1 | — | 45.7 GB | 1.1% | ❌ compressed |
| *(zip — peekable today)* | *3723* | — | *4.90 TB* | — | *central dir* |

**Uncompressed `.tar` is only ~17% of unpeekable files / ~30% of unpeekable bytes.** The other
~70%/~64% is compressed or non-tar and **not** header-walkable.

## What is actually INSIDE the uncompressed `.tar` (live remote walk of the top 25 by size)

Implemented the mentor's walk (hardened: 206 check, 429+5xx retry, GNU-longname/PAX support)
and classified every member with the pipeline's own regexes — **parse-target** (`_PARSE_RE`:
vasprun/OUTCAR/INCAR/…, what fetch extracts) vs **heavy** (`_AVAILABILITY`: CHGCAR/WAVECAR/…,
availability-only) vs **other**. Result over the 25 largest (most of the 1.24 TB):

- **13 / 25 were not walkable at all** — **7 are gzip streams misnamed `.tar`** (compressed →
  walk impossible; `tarfile.open` auto-detects them at fetch, the walk cannot), **6 returned
  HTTP 504** (Zenodo Gateway-Timeout on Range reads of very large files; *transient* — a retry
  pass salvaged 2 of them).
- **8 / 25 walked cleanly — and ALL were 100% non-VASP** (few, huge members): protein **GROMACS
  MD** (`.xtc`/`.pdb`/`.gro`/`.tpr`, e.g. recid 8246448, 17161058), **nested `.tar.gz`**
  (recid 11280333 cell-biology, 17234053 reaction-paths), **BerkeleyGW** GW-BSE inputs (15277150),
  and **DOS-as-text** (recid 6573616 `ferro.tar`: 2364 `.txt` + `.csv` + `.DS_Store`).
- **Only 1 genuinely VASP-rich tar** (recid 6573616 `LLMGGA_data_combined.tar`, 15.8 GB,
  **6273 members**): vasprun.xml/OUTCAR/EIGENVAL/DOSCAR/PROCAR + much `.txt`/custom — the
  many-small-member shape, which **truncated at the 800-request cap**. Notably the giant
  CHGCAR/WAVECAR were **absent** (deleted before upload — see structure section).

## Cost model — walk vs download

With a small batch, walk transfer → ~0, so the trade collapses to **M requests (walk) vs S bytes
(download)**; file-endpoint limit is **100 req/min, 5000 req/hour** (search's 30/min does *not*
apply). Measured: `R_walk ≈ M` for large members (17161058: 20 members→15 reqs; 8246448: 74→74),
falling below M only when members are small enough to batch (ferro.tar: 2483→703). So:

| archive shape | walk cost | download cost | winner |
|---|---|---|---|
| few large members (non-VASP blobs, nested) | tiny (≈M reqs, ~0 bytes) | full S | **walk** (to drop) |
| many small members (raw VASP dumps) | **≈M reqs → 1000s, quota-bound** | full S, ~1 req | **download** |

Deciding factor is **member count M** (≈ requests), the scarce resource.

## Why VASP archives are many-member (theory + sources)

Authoritative evidence that raw VASP archives are many-small-files **by construction**:
- A single VASP calc = **~15–20 files** (4 small-text inputs + ~10–20 outputs); only
  CHGCAR/WAVECAR/LOCPOT/`vaspout.h5` are large binaries, and these are **routinely deleted
  before archiving** (cleanup scripts target them). Source: **VASP Wiki**
  ([Input](https://vasp.at/wiki/Category:Input_files) /
  [Output](https://vasp.at/wiki/Category:Output_files) files) — official, highest reliability.
- Tooling puts **one directory per calc**, nesting `relax1`/`relax2`/`static`/NSCF steps
  (pymatgen/atomate2/custodian; [atomate2 docs](https://materialsproject.github.io/atomate2/user/codes/vasp.html),
  [MP methodology](https://docs.materialsproject.org/methodology/materials-methodology/electronic-structure))
  — maintainer docs. High-throughput → thousands of such dirs (an
  [AFLOW entry](https://aflowlib.duke.edu/AFLOWDATA/LIB2_WEB/Ca_svCo/19) alone ≈ 60 files).
- **Small files are a Lustre pathology** (inode-bound, MDS-bottlenecked); HPC centres recommend
  tar/HDF5 containers ([Bonn](https://wiki.hpc.uni-bonn.de/marvin/lustre-best-practices),
  [ULHPC](https://hpc-docs.uni.lu/filesystems/lustre/)) — corroborates the CSD3 `/rds` inode limit.

**This explains the walk results:** the few-member walkable tars were consolidated/foreign
(no raw VASP); the one VASP-rich tar was a raw-directory dump (6273 members). The walk is
structurally most expensive on exactly the archives that contain harvestable raw VASP.

## Are the big public datasets even on Zenodo?

**No — not in full.** Distinctive-name searches return almost nothing (`MPtrj`=6, `OMat24`=5,
`OQMD`=21), and those are only **small derived subsets** (OC20-Ni 0.4 GB, OpenCatalyst DGL
graphs 7.9 GB, OQMD-for-CGNN 0.1 GB, jarvis-dft preprocessing 4.8 GB). The full corpora live on
figshare/HuggingFace/oqmd.org/aflowlib — **separate future sources, no special Zenodo handling
needed now.** Zenodo *does* host large **native** materials/MLIP sets (MP Phonon DB v1.1 79 GB,
SiC-melting ML 186 GB, amorphous-carbon 184 GB, split-vacancy foundation-model 64 GB), but these
are **consolidated** formats (extxyz/traj/hdf5) → handled by the `processed_atomistic`/direct
paths, not the raw-VASP-tar path. The heavy tail is also **keyword-contaminated**: a 117 GB
equine-MRI "T1 relaxation", a 181 GB hemocyanin BLASTp, a 158 GB climate set.

## Discovery-precision review (measured over the 2,008 rank ≥ 3)

50% of rank ≥ 3 records have **no** DFT/VASP metadata signal (`models.metadata_signal`). Using
"empty signal" as an over-flagging proxy for "likely non-VASP", per-term risk (word-boundary
matched in title+keywords):

| term | matches | empty% | read |
|---|--:|--:|---|
| molecular dynamics | 268 | **80%** | 🔴 protein/classical MD |
| relaxation | 62 | **90%** | 🔴 MRI/NMR/structural |
| forces | 34 | **88%** | 🔴 generic |
| force field | 23 | 57% | 🔴 biomolecular/classical |
| surface / interface / monolayer | 167/67/32 | 47/73/50% | 🟠 materials, high volume |
| materials / material | 171/72 | 43/54% | 🟠 generic |
| crystal / crystal structure | 62/25 | 42/44% | 🟡 mostly real materials |
| ab initio / first principles | 60/18 | **0/0%** | 🟢 excellent |
| band gap / density of states | 7/3 | 0% | 🟢 excellent |
| defect / vacancy / semiconductor / catalyst / perovskite | — | 24/24/33/10/19% | 🟢 clean |

Foreign-domain leakage confirming contamination: protein 92%, gromacs 100%, dna 93%,
rna/genome/climate/ocean 100% (none are query terms — they enter via the 🔴/🟠 terms).
Per-query: query 1 (VASP filenames) is cleanest (**7% empty**); the two big
`computation-chunk-0 × materials` cross-products carry the tail (**1,141 records at 64–68%
empty**), driven by `molecular dynamics`/`relaxation`/`forces` on the computation side.

**Fix applied (2026-07-29):** deleted the ambiguous computation-chunk hooks (`molecular
dynamics`, `relaxation`, `forces`, `force field`) so the `(computation) AND (materials)`
cross-products require a DFT-specific computation term (deletion ≡ anchoring, since the DFT
terms are the rest of the OR). **Materials terms kept** — precision now comes from the `AND`, so
`crystal`/`surface` cost recall if removed. NB: metadata-signal is a *ranking* hint only, **not**
a filter — the empty-signal set also contains real terse-metadata materials records (phonons,
elastic constants, clusters), so gating on it would lose real recall.

## Decision & recommendations

1. **Do NOT build the tar-header walk.** Uncompressed `.tar` is a minority of unpeekable bytes
   (~30%), most of *that* is non-VASP (foreign-domain MD, nested archives, DOS-text, misnamed
   gzip), and where real VASP exists it is many-member → thousands of requests (quota-bound) with
   the heavy binaries usually already deleted (little to skip). Current download-extract-delete
   already handles uncompressed `.tar` correctly.
2. **The worthwhile random-access play is ZIP targeted-fetch** — enumeration is 1 request
   (central dir, already built), targeted extraction ~2 requests/file, and zip is the dominant
   addressable class (4.90 TB). Shares only the *nested-archive* limitation (already fail-safed).
   ✅ **IMPLEMENTED 2026-07-29** (`zenodo_harvest/zipstream.py` + `fetch.py`, on by default,
   `--no-zip-stream` to disable): standard 32-bit ZIP (STORED/DEFLATE), ~1 Range request per
   member (CRC-verified), chosen over a whole download when worthwhile (heavy bytes to skip, or
   a huge archive) and the target count is within `--zip-stream-max-files` (default 128);
   everything else (ZIP64/encrypted/odd-compression *target* member, enumeration failure, Range
   ignored, corrupt member) falls back to the whole-archive download — no regression. A huge
   ZIP64 *heavy* member beside 32-bit VASP outputs is fine (we skip it anyway). Measured on a
   4.2 MB test zip whose bulk was a 4 MB CHGCAR: **1.9% transferred**, archive never staged.
3. **Harden `download_file` with 5xx/504 retry** (currently only 429 retries in-run; 504 is
   transient — a re-run recovers it, but in-run retry avoids leaving files for the next pass).
4. **Discovery precision:** the applied keyword deletions remove the protein/GROMACS/foreign tail
   at ~zero VASP recall cost; consider restoring `geometry optimization` / `potential energy
   surface` / `transition state` (DFT-specific, low contamination — deleting them costs recall).
5. **Big public corpora (MPtrj/OMat24/OQMD/Alexandria/OC20) are a separate next-step source**,
   not a Zenodo concern; Zenodo-native large sets are consolidated and already handled.

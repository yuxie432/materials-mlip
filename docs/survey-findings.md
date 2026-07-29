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
   The **nested-archive limitation is now closed** (`_recurse_nested_archives`): sub-archives
   of any type are unpacked recursively after download (depth cap 8), and targeted ZIP fetch
   falls back to a whole download when a zip contains one so the recursion can reach it.
3. **Harden `download_file` with 5xx/504 retry** (currently only 429 retries in-run; 504 is
   transient — a re-run recovers it, but in-run retry avoids leaving files for the next pass).
4. **Discovery precision:** the applied keyword deletions remove the protein/GROMACS/foreign tail
   at ~zero VASP recall cost; consider restoring `geometry optimization` / `potential energy
   surface` / `transition state` (DFT-specific, low contamination — deleting them costs recall).
5. **Big public corpora (MPtrj/OMat24/OQMD/Alexandria/OC20) are a separate next-step source**,
   not a Zenodo concern; Zenodo-native large sets are consolidated and already handled.

---

# FOURTH SURVEY — post-keyword-fix + zip-walk + nested-recursion, all resource types (measured 2026-07-29)

Re-run after three code changes since survey #2/#3: (a) the **2026-07-29 keyword fix**
(deleted the ambiguous computation hooks `molecular dynamics`/`relaxation`/`forces`/`force
field`, so the `(computation) AND (materials)` cross-products now require a DFT-specific
term); (b) **targeted ZIP-walk fetch** (`zipstream.py`, on by default); (c) **recursive
nested-archive unpacking** + the disk/inode staging valve. Method identical to prior
surveys (discover → census → **full** triage-peek → tar-header-walk → download samples →
fetch+parse ratios), plus an end-to-end zip-walk validation. Full windowed discovery of
**all three resource types** (no single query exceeds the 10k window, so windowed == exhaustive).
All tooling: `scripts/estimate/` + the survey scripts; gates (access + license) ON throughout.

## Discovery (current keywords, gates ON) — the keyword fix lands

| resource type | unique concepts | relevant (rank≥3) | A direct | archive | processed | unlikely | gates dropped (access/license) |
|---|--:|--:|--:|--:|--:|--:|--:|
| **dataset** | 2,463 | **1,711** | 13 | 1,698 | 138 | 582 | 246 / 228 |
| **software** | 719 | **633** | 1 | 632 | 6 | 80 | 34 / 37 |
| **publication** | 7,191 | **456** | 1 | 455 | 57 | 6,673 | 679 / 1,134 |
| **COMBINED** (dedup) | **10,373** | **2,800** | 15 | 2,785 | 201 | 7,335 | 959 / 1,399 |

vs survey #2 (same gates): dataset relevant **2,391 → 1,711** (−28%), software **865 → 633**,
publication **733 → 456**, combined **4,104 → 2,800** (−32%). The deletions removed the
protein-MD / MRI / foreign-domain tail (survey #3's diagnosis) at ~zero VASP-recall cost —
a genuine precision win. Discovery took **~51 min** wall (dataset 13, software 3,
publication 35) single-stream at ~28 req/min; publication is 93% `unlikely` (PDFs), the
expensive-but-low-yield type.

## Census (combined relevant = 2,800 records) — exact from the manifest

Raw footprint **9.01 TB** (largest single record 224 GB, **median 42 MB** — heavy-tailed:
top-100 records hold 56% of bytes). Whole-archive **download vs per-file cap**:

| cap/file | 0.5 GB | 2 GB | 10 GB | 50 GB | uncapped |
|---|--:|--:|--:|--:|--:|
| transfer | 302 GB | 1.12 TB | 3.75 TB | 7.93 TB | **8.46 TB** |

By format (uncapped): zip **5.20 TB**, tar **2.99 TB**, tar.zst 124 GB, rar 84 GB, 7z 41 GB,
directly-exposed VASP 30 GB. vs survey #2's 15.4 TB raw / 12.2 TB transfer — the keyword fix
roughly halved the I/O bill.

## The three bucket questions (A/B/C), answered

Buckets over the 2,800 relevant records (A = a VASP primary exposed directly; B = has a
peekable `.zip`; C = only non-zip archives):

| bucket | records | raw bytes | how VASP is confirmed | VASP-primary rate |
|---|--:|--:|---|---|
| **A** directly-exposed | **15** | 45 GB | already visible | 100% |
| **B** peekable-zip | **2,205** | 5.94 TB | **full HTTP-Range peek of 2,209 zips** | **8.2%** primary visible |
| **C** other-archive (tar/rar/7z/zst) | **580** | 3.03 TB | **downloaded a sample (n=28)** | **21.4%** (6/28, CI ~10–40%) |

* **A — directly-exposed VASP:** 15 records (13 dataset + 1 software + 1 publication).
* **B — peekable VASP in zips (near-exact, not sampled — every zip's central directory read):**
  of 2,209 zip records, **182 (8.2%) expose a VASP *primary*** (vasprun/OUTCAR/vaspout);
  361 (16.3%) show *any* VASP name; **1,464 (66.3%) are proven no-VASP** (peek drops them
  before any download); **472 (21.4%) are an evidence gap** (contain a nested archive or the
  central directory couldn't be read → kept for fetch to decide). vs survey #2's *sampled*
  B-rate of 5.3% — the full peek + keyword fix put it at 8.2%.
* **C — VASP in other archives (sample n=28 downloaded, real multi-file archives, fixed code):**
  **21.4% yield** (6/28), consistent with survey #2's 27%/25%. These records are **frame-rich**:
  6 records → **61,457 frames** (mean 10,243/record, one with 39,767 — screening/AIMD dumps).
  NB the C bucket is inflated by 36 records (126 GB) that are single bare-`.gz` data files
  (classified `archive` because `.gz ∈ ARCHIVE_EXTS`); the 546 *real* multi-file archives
  (2.98 TB) are the VASP-bearing sub-bucket the 21.4% is measured over.

**⇒ Estimated records with a parseable VASP primary ≈ 416** (band ~255–571) = 15 (A) +
182 (B peek-confirmed) + ~94 (≈20% of the 472 B evidence-gaps, via nested-archive recursion) +
~124 (21.4% of the 580 C). A full no-cap fetch+parse on CSD3 nails the exact number.

## Non-peekable (non-zip) archive breakdown — the `.tar`-walk question

Exact from the manifest over the relevant set: **1,985 non-zip archive files / 3.23 TB**.

| format | files | % files | bytes | % bytes | header-walkable? |
|---|--:|--:|--:|--:|:--|
| tar.gz / tgz | 993 | 50.0% | 1.88 TB | 58.1% | ❌ compressed |
| **tar (uncompressed)** | **239** | **12.0%** | **787 GB** | **23.8%** | ✅ |
| bare .gz/.bz2/.xz | 413 | 20.8% | 174 GB | 5.3% | ❌ compressed |
| tar.xz | 77 | 3.9% | 129 GB | 3.9% | ❌ compressed |
| rar | 155 | 7.8% | 84 GB | 2.5% | ❌ no seek |
| tar.zst | 15 | 0.8% | 78 GB | 2.4% | ❌ compressed |
| tar.bz2 | 49 | 2.5% | 47 GB | 1.4% | ❌ compressed |
| bare .zst | 1 | 0.1% | 46 GB | 1.4% | ❌ compressed |
| 7z | 43 | 2.2% | 41 GB | 1.2% | ❌ (indexed, not tar) |

**Uncompressed `.tar` is only ~12% of non-peekable files / ~24% of bytes** — matching survey
#3 (17%/30%). The other ~76%/~64% is compressed or non-tar and **not** header-walkable.

### Remote tar-header-walk — re-run on the 20 largest uncompressed `.tar` (506 GB ≈ 64% of tar bytes)

Implemented the mentor's walk (hardened: 206-check, 429/5xx retry, GNU-longname/PAX, **GNU
base-256 sizes**, windowed reads to batch small members). Result:

| outcome | count | note |
|---|--:|---|
| `misnamed_gzip` — **not walkable** | **9/20** | named `.tar` but actually a gzip stream (fetch still handles via `tarfile` auto-detect on download; a *remote walk* cannot) |
| walkable, **nested-only** (no direct primary) | 7/20 | VASP, if any, is inside sub-archives the walk can't see |
| walkable, **VASP primary present** | **3/20** | 337 / 2,329 / 9,988 members — **many-member ⇒ O(members) Range requests**, all truncated at ~200 reqs |

**Decision: do NOT build the tar-walk** — re-confirmed with the current keywords + pipeline.
Even among the *largest* uncompressed tars, a walk usefully targets only 3/20, and those are
the most request-expensive (thousands of members) against a file endpoint that throttled the
survey hard (225 × HTTP 429 during the zip probe).

### Would nested-archive *recursion* make tar-walk worthwhile? (mentor Q, 2026-07-29)

Because an uncompressed outer `.tar` is seekable, byte offsets **compose**: the tar header
gives a nested archive's absolute byte range, so **if** the nested archive is itself
randomly-indexable (an uncompressed `.tar` header-walk, or a `.zip` central-directory read)
one *could* recurse a Range-into-Range walk and pull VASP members without a full download.
But of the 8 walkable nested tars, the nested members were: `.gz`/`.bz2` single files (5),
`.tar.gz` (1) — **all compressed, unreachable** without downloading+decompressing the whole
sub-blob — and only **`.zip` (1) + uncompressed `.tar` (1)** seekable. So recursion helps only
that rare seekable sub-case (~2/8 here); the common compressed nesting hits the same wall as a
top-level `.tar.gz`; and the payoff is thin anyway (heavy CHGCAR/WAVECAR are usually deleted
before archiving, so little bulk to skip; VASP archives are many-member → request-bound). The
pipeline already reaches all of these by **downloading then recursively extracting**
(`_recurse_nested_archives`), so **no data is lost** — only marginal transfer, so the
don't-build verdict stands.

## ZIP-walk pipeline — validated end-to-end + aggregate transfer

The full peek also modelled the targeted-fetch transfer (compressed VASP-member bytes vs whole
archive), using fetch's real decision constants. Per-zip modes over 2,209 records: `no_target`
2,898 (proven no-VASP, download nothing), `fallback_nested` 286, `fallback_many_members` 117,
`fallback_small_allvasp` 213, **`targeted` 94**, `fallback_zip64_odd` 2.

* **End-to-end validation** (10 confirmed-primary `targeted`-eligible zips, real fetch+parse):
  **10/10 fetched, 324 frames parsed**, transferring **48.5 MB of compressed VASP members vs
  922 MB of whole archives — 95% less network** (per-record 75–99.9%). The zip-walk works and
  is highly effective *where it applies*.
* **Aggregate** over all of bucket B: whole 5.32 TB → **walk+peek-aware 4.28 TB (only ~20%
  saved)**. The apparent contradiction with the 95% above is real and important: `targeted`
  applies to only 94 zips; most VASP-bearing *bytes* sit in **nested / many-member** zips that
  fall back to a whole download, and the no-VASP zips that peek drops are **small** (survey #2's
  counter-intuitive finding, reconfirmed). So the zip-walk's big win is on the archives with
  heavy-file bulk to skip; in aggregate the byte saving is modest but free (never a regression).

## Storage — transfer AND the compiled dataset (both, per the mentor's ask)

Measured ratios (C-real sample, n=28 @ 2 GB cap): extract ratio 1.55, **4.6 KB/frame**
(compressed extxyz.gz; B-walk small-cell sample gave 3.0 KB/frame — range 3–11 KB across cell
sizes), 91 atoms/calc mean, frames/record heavy-tailed (median 1,913, mean 10,243).

**1. Transfer to download from Zenodo** (the transient, network cost — the real constraint):

| policy | transfer |
|---|--:|
| whole-archive, uncapped (no peek/walk) | 8.46 TB |
| **peek + zip-walk-aware, uncapped** | **≈7.5 TB** (B 4.28 TB walk + non-zip 3.24 TB whole + direct 0.03) |
| whole-archive @ 2 GB/file cap | 1.12 TB (peek-aware less) |

Of the ~7.5 TB, **~3.2 TB is non-zip archives downloaded whole** (can't peek), of which ~79%
is non-VASP (the C yield is 21%) — i.e. ~2.5 TB of unavoidable "download-to-reject" for the
tar/rar/7z/zst buckets. That is the price of no tar-peek, and (per above) a tar-walk wouldn't
meaningfully cut it.

**2. Transient staging** (extracted VASP awaiting parse+purge): bounded by the disk valve
(`--max-disk-bytes`/`--max-disk-files`), *not* a running total — peak ≈ largest in-flight
archive + not-yet-purged extracts. Sized ~0.8 TB / 800k inodes on CSD3.

**3. Final compiled dataset** (`extxyz.gz` shards + `metadata.jsonl`, the long-term artifact):
bucket-weighted frame estimate ≈ **0.3–1.4 M frames → ~1.3–6.4 GB**; with cell-size and
evidence-gap uncertainty, **order 5–40 GB**. The two estimators bracket it (frame-based ~26 GB
if C's frame-rich ratio holds broadly; byte-ratio is unreliable here because it over-applies
the frame-rich C sample to the whole transfer). **Either way it fits the 1 TB quota with vast
margin** — consistent with all prior surveys. **Storage of the compiled dataset is never the
constraint; transfer is.**

## Estimated CSD3 runtime (limiting factors)

The harvest is **network/rate-limited, not CPU-limited**. Components:

| stage | cost | limiter |
|---|---|---|
| discover | **~1 h** (measured ~51 min, 3 types) | Zenodo search **30 req/min**, single-stream — cannot parallelize |
| triage `--peek` | **~1 h** for ~2,200 zips | file-endpoint request rate; **throttled hard** (225×429 observed) |
| **fetch** | **transfer ÷ throughput** — dominant | compute-node download speed (**unknown until measured**) + request throttling on small files |
| parse | **not the bottleneck** — 47 frames/s/core, embarrassingly parallel (array job) or overlapped in `pipeline` | RAM per big vasprun (use `--max-primary-bytes` on `icelake-himem`) |

**Fetch dominates.** At the peek+walk-aware ~7.5 TB (uncapped) and an assumed compute-node
aggregate throughput (`--workers 4`):

| aggregate speed | ~7.5 TB uncapped | ~1.1 TB @ 2 GB cap |
|---|--:|--:|
| 50 MB/s | ~42 h | ~6 h |
| 200 MB/s | ~10 h | ~1.6 h |
| 400 MB/s | ~5 h | ~0.8 h |

So an **uncapped** harvest is a **1–2 job affair** (36 h SL1/SL2 wallclock; every stage is
resumable and `20_pipeline.sh` self-resubmits), while a **2 GB cap fits comfortably in one
job**. **Measure the actual speed first** with `scripts/csd3/csd3_download_speed.py` (added
this survey; verified live URLs) on a compute node, and plug it into the table. The inode
limit (1M files) is the *other* binding constraint — the fetch→parse→purge pacing loop handles
it, but it means the uncapped run proceeds in disk-valve-bounded waves, not one straight pull.

## Resource-type necessity (dataset / software / publication)

| type | relevant | ≈ VASP-primary records | raw | verdict |
|---|--:|--:|--:|---|
| dataset | 1,711 | ~260 | 8.22 TB | **core** — the bulk of usable data |
| software | 633 | ~55 | 226 GB | **clear win** — best useful-records-per-byte (tiny footprint) |
| publication | 456 | ~40 | 588 GB | **marginal** — 93% PDFs, low yield, but +~40 real records; gates+peek bound the cost, so keep it |

`resource_type` is recorded end-to-end (fetch→parse→metadata), so sources can be weighted or
filtered at train time. All three are worth including; software is the standout efficiency.

## Bug found and fixed during this survey — AppleDouble / `__MACOSX` cruft

The bucket-C sample surfaced a real defect: macOS-created archives carry AppleDouble `._<name>`
sidecars (and `__MACOSX/`, `.DS_Store`). Because `._OUTCAR`/`._vasprun.xml` match the VASP name
regex (the `_` before the stem is a valid separator) and `._Data.zip` matches the archive
detector, the pipeline **extracted `._vasprun.xml.gz` sidecars and fed them to pymatgen** (20
spurious `vasprun_parse_error` on one Mac-tarred record) and tried to **unzip 4 KB AppleDouble
headers** (71 `BadZipFile`), with a latent risk of a `._vasprun.xml` displacing the real primary
in a calc unit. **Fixed** (`fetch._is_junk_member`, applied in `_want_member`, the direct-file
branch, `_find_calc_units`, the targeted-zip target/nested filters, and the nested-recursion
glob) + 5 regression tests (`tests/test_junk_members.py`). After the fix the C-real sample's
`extract_error`s dropped **134 → 2** and yield rose to a clean 21.4%. All 177 prior tests still
pass.

## Methodology limitations (especially on CSD3)

1. **Metadata-only recall ceiling** (unchanged): `q` searches title/description/keywords, never
   file contents, so VASP with silent metadata is invisible. All counts are lower bounds. The
   keyword fix *raised precision*; recall of terse-metadata materials records is the residual gap
   (metadata-signal is a ranking hint, deliberately **not** a filter).
2. **Zenodo file-endpoint throttling is the real pacing constraint**, and tighter than survey #3
   assumed (~100/min): the survey absorbed **225 × HTTP 429** on the zip peek alone. Triage-peek
   and the many-small-file parts of fetch are **request-bound**, so on CSD3 they run slower than a
   naive "bytes ÷ bandwidth" estimate implies — budget ~1 h for peek and expect fetch of many
   small members to be rate- not bandwidth-limited.
3. **Compute-node outbound network is unverified** (CSD3 docs are silent) — run
   `scripts/csd3/00_check_network.sh` before submitting; the whole fetch stage depends on it.
4. **Download-to-reject on the non-zip buckets:** ~3.2 TB of tar/rar/7z/zst must be pulled whole
   because they can't be peeked, and ~79% holds no VASP. A tar-walk would not fix this (see above);
   the only real levers are the per-file cap and accepting the wasted transfer.
5. **`--max-primary-bytes` is a parse-RAM guard, not a download/disk lever** — it does *not* limit
   what fetch transfers or stages. A huge `vasprun.xml` is still downloaded + extracted (charged to
   the disk valve) and only *skipped at parse* (`primary_too_large`), left on disk for a bigger-RAM
   re-parse. Those skipped-but-extracted giants **accumulate in staging** (purge-raw keeps a record
   with any unparsed unit, though it frees the parsed siblings), slowly eating the 1 TB / 1M-inode
   quota over a long run; drain them with a periodic `icelake-himem` re-parse pass. To actually cap
   downloads/disk use `--max-bytes` / `--max-member-bytes` / the disk valve.
6. **Heavy-tail + sampling:** raw bytes and frames/record are extremely heavy-tailed (one record
   = 224 GB; one calc = 39,767 frames), so the C-rate (n=28) and dataset-size projection carry a
   factor-few uncertainty. The exact numbers come only from the full no-cap fetch+parse on CSD3.
7. **B evidence-gap (472 records)** hold unknown VASP inside nested/ZIP64 zips — the ~416 estimate
   assumes ~20% of them yield; the true count needs the fetch pass.

## Bottom line

Current keywords + zip-walk + nested-recursion give a **cleaner, cheaper** harvest than survey
#2: **2,800 relevant records**, **~416 with a parseable VASP primary** (band ~255–571),
**~7.5 TB peek+walk-aware transfer** (or ~1.1 TB @ 2 GB cap), and a **~5–40 GB final dataset**
that fits 1 TB trivially. The peek and zip-walk are net wins (peek drops 66% of zips pre-download;
the walk cuts 95% on the archives it applies to) but bounded in aggregate by nested/many-member
zips and un-peekable tars. The tar-walk (even recursive) is **not** worth building. The one code
gap found (AppleDouble) is fixed and tested.

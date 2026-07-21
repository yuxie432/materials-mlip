# How much accessible + relevant VASP data is on Zenodo? (measured 2026-07-21)

Estimate produced with the **current harvest scripts**, verified with live tests
against the Zenodo API (WSL, anonymous+token, ~30 req/min). Reproduce/refresh with
the tools in this directory (`README.md`).

## TL;DR

| Question | Answer |
|---|---|
| Relevant VASP dataset **records** (current scripts) | **~750** discoverable candidates (rank ≥ 3); **~350–450 genuinely contain VASP** (`triage --peek` confirms ~45% of archives on a sample of 82) |
| **Raw footprint** of those records | **3.70 TB** (exact; heavy-tailed — top 100 records = 83%) |
| **Transfer** to harvest them | **0.15 TB → 3.3 TB** depending on the per-file size cap (a 22× lever) |
| **Final `extxyz.gz` dataset** | **~5 – 125 GB**; ~15–50 GB with a sensible cap. ≈ 3.7% of transferred bytes |
| The broad `"ab initio" molecular dynamics forces` query | **Noise** — ~53k hits, ~27k "archive" records, **verified 0/54 contain VASP** |

All numbers are **lower bounds**: metadata-only search cannot see VASP data whose
title/description/keywords don't mention it.

## 1. Discovery (exact, from the API)

Context: Zenodo has ~6.96M records, ~660k `dataset`s.

The 12 default queries (`type=dataset`, open-access + reusable-license gated,
deduplicated by concept) split cleanly into two groups:

**11 precise queries → 1,199 unique concepts.** Category breakdown:

| category | n | meaning |
|---|---|---|
| `vasp_direct` | 9 | primary VASP output exposed directly (confirmed) |
| `archive` | 745 | contains an archive; VASP presence unconfirmed until peek/fetch |
| `processed_atomistic` | 60 | uploaded extxyz/xyz/traj — **not ingested** (no reader yet) |
| `vasp_input_only` | 22 | only POSCAR/INCAR/… (no outputs to train on) |
| `unlikely` | 363 | no VASP/archive/atomistic files |

→ **Relevant (rank ≥ 3) = 754 records** (9 confirmed + 745 archives: 550 peekable
`.zip`, 195 tar/rar/7z). Gates dropped 86 non-open and 102 non-reusable-license
(mostly CC-BY-NC-*, `notspecified`).

**Precision of the archives** — `triage --peek` reads each `.zip` central directory:
on a sample of 82 archives, **37 (~45%) contain a vasprun/OUTCAR** (the true rate for
`.zip` specifically is higher — the denominator includes unpeekable tar/rar counted as
unconfirmed). So of the 754 candidates, **≈ 350–450 genuinely contain VASP**; the rest
are archives of adjacent data (structures-only, other codes, supp. info). A parseable
run additionally requires valid content + a size cap that admits it (§3).

**1 broad query `"ab initio" molecular dynamics forces` → 52,886 hits.** Zenodo's
default operator is **OR**, so this is really `"ab initio" OR molecular OR dynamics OR
forces`. Temporal sampling extrapolates ~26,647 additional rank ≥ 3 "archive"
records — but **direct content inspection proves these are false positives**:

- Peeked **54 zips** across the 2017 / 2021 / 2024 spikes → **0** contained a
  vasprun/OUTCAR/vaspout.
- VASP/DFT metadata signal present in **0–6%** of them.
- Actual content: C. elegans worm-tracking (`.wcon.zip`), wheat genomes, GC-MS
  metabolomics, MALDI-TOF, lunar craters, exoplanet catalogs, cyclone maps,
  microbiome fastq… OR-matching "molecular"/"dynamics"/"forces".
- The 2017-10-23 single-day bulk upload alone is 12,122 records (and *truncates* the
  10k window — a recall bug, but here it only drops junk).

**⇒ The broad query contributes ≈ 0 genuine VASP and, if harvested, would waste
enormous download + peek effort. Fix it (make it AND) or drop it.**

## 2. Storage census (exact — the manifest carries every file's size)

Relevant set (754 records), raw footprint **3.70 TB**:

- Largest single record **186 GB**; median **157 MB**. Heavy tail: top-1 = 4.9%,
  top-25 = 40%, top-100 = **83%** of all bytes.
- By format (uncapped): zip **2.65 TB**, tar **610 GB**, rar **37 GB**,
  direct VASP files **28 GB**, 7z **8 GB**. (rar+7z need the optional `archives`
  extra installed, else ~45 GB is skipped.)

**Transfer volume vs the fetch per-file cap** (the dominant lever):

| cap / file | transfer |
|---|---|
| 0.5 GB (near default) | **147 GB** |
| 2 GB | **451 GB** |
| 10 GB | **1.22 TB** |
| uncapped | **3.31 TB** |

Most of the volume lives in a few multi-GB archives, so the cap swings the total by >20×.

## 3. Storage ratios (measured by real fetch + parse)

Sample of 45 rank ≥ 3 records @ 200 MB/file cap → 7 fetched, 6 parsed (79 calc units,
2,812 frames of genuine VASP):

- **~10.6 KB / frame** compressed (`extxyz.gz`); ~318 atoms/frame median.
- **Final dataset ≈ 3.7% of downloaded bytes.**
- Extract ratio ≈ **1.03** (retained VASP files ≈ archive size — extraction
  decompresses, so staging ≈ transfer; transient, reclaimed by `purge-raw`).
- frames/record: median **27**, mean **469**, max **2109** (AIMD trajectories dominate).
- Fetch **yield** @200 MB cap ≈ **13%** (cap-limited: most VASP sits in >200 MB archives).

## 4. Projected dataset size

Applying the measured `dataset ≈ 3.7% × transfer`:

| cap / file | transfer | final `extxyz.gz` dataset |
|---|---|---|
| 0.5 GB | 147 GB | ~5 GB |
| 2 GB | 451 GB | ~17 GB |
| 10 GB | 1.22 TB | ~46 GB |
| uncapped | 3.31 TB | ~125 GB |

Order-of-magnitude (ratios from a thin sample of small records). Direction of bias:
large archives carry more heavy files we discard (→ dataset smaller than 3.7%), but
long AIMD trajectories carry more frames/byte (→ larger). Re-measure at the production
cap on CSD3 (`sample_storage.py --cap-gb …`).

## 5. Caveats / limits of the current scripts

1. **Recall ceiling** — metadata-only `q`; VASP data with silent metadata is invisible.
   These figures are lower bounds. (Future: `--community`, OAI-PMH.)
2. **Precision of `archive`** — rank 3 only means "has an archive". True VASP content
   is confirmed only by `triage --peek` (zip) or at fetch (tar/rar/7z). Budget for a
   yield < 100% on the 745 archives.
3. **Per-file cap** is a 20×+ storage lever — set it deliberately.
4. **Single-day >10k truncation** in exhaustive discovery (seen 2017-10-23).
5. **`processed_atomistic`** (60 records) not ingested — 0 contribution until a reader exists.
6. **rar/7z** (~45 GB) skipped unless the `archives` extra is installed.

## Recommendation

Real, high-confidence target ≈ **the ~750 precise-query records (3.7 TB raw)**. With a
2 GB cap you would transfer ~450 GB and end with a **~15–50 GB** MACE-ready dataset.
Before a full harvest: (a) fix the broad OR query, (b) always `triage --peek`, (c) pick
the cap, (d) install `archives`, (e) run the exact full census + a larger storage
sample on CSD3 (see `README.md`).

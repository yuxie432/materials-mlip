# Dataset evaluation — statistics, comparison with MLIP training sets, recognised benchmarks

Evaluation of the three harvested VASP corpora (Zenodo, NOMAD direct uploads, Materials Cloud
Archive) for MLIP training, written 2026-10-03. Three parts, as asked:

1. **Statistics** of the datasets — measured by the new read-only `dataset_stats/` package
   (`dataset_stats/CLAUDE.md`; CSD3 runbook `scripts/csd3/stats/`).
2. **Comparison** with the MLIP training sets in common use (MPtrj, OMat24, sAlex/Alexandria,
   MatPES, OC20/22/25, MAD, …), taking de-duplication and subsampling into account.
3. **Recognised benchmarks and metrics** for judging a dataset, and how this corpus can earn
   objective recognition.

**Status: final.** The metadata statistics (§3) come from all three `metadata.jsonl` files. The
frame statistics (§4) and the like-for-like comparison with MPtrj, OMat24 (val), sAlex (val), MP
and Alexandria (§5) come from one CSD3 run on 2026-10-03: jobs 37235786 (scan), 37235787
(references) and 37235788 (report), about 1 h wall time in total. That run covers **individual
uploads only**; the Alexandria group's runs inside NOMAD are excluded (§1.1). The generated
`report.json` + `report.md` (~80 kB, every table per source) are kept out of git in
`stats_csd3/`, and all numbers below come from them.

---

## 1. Key findings

1. **Most of the NOMAD harvest is the Alexandria database, not the long tail.** 6,203,632 of
   NOMAD's 7,073,592 calcs (88%) and 42,371,188 of its 52,459,065 frames (81%) are institutional
   high-throughput runs of the Alexandria group (M. A. L. Marques, S. Botti). They were uploaded to
   NOMAD as ordinary "direct uploads", with no `external_db` tag, so the harvest's filter could not
   see them:
   * 5,953,659 calcs whose NOMAD mainfile paths carry **Alexandria ids**
     (`xxx_02a-00_agm004014910_spg216/GEO1_vasprun.xml.bz2`), uploaded mostly in 2024. These are
     Alexandria's own PBE (4.05M), PBEsol (1.48M) and SCAN (0.43M) cell relaxations and statics,
     run with one recipe (`PREC = High`, `ISMEAR = 1, SIGMA = 0.2`, ISPIN = 2, Gamma-centred grids);
   * 249,973 calcs of the 2017 Schmidt–Marques–Botti cubic-perovskite high-throughput set
     (`perovskites/Li_LiF3Cs_xxx_02p-00_spg221b/`), which uses the same group's naming scheme.

   These are the data behind Alexandria, sAlex, OMat24's seeds and LeMat-Traj, so they are already
   used for MLIP training. They are identified by provenance (path patterns, `meta.origin_of`), not
   by author name. Zenodo and Materials Cloud contain none. All statistics below are for
   **individual uploads only** (`--individual-only`, the default): `scan` and `report` skip these
   calcs, and `meta` still describes them (§1.1).
2. **The genuine long tail** is Zenodo + Materials Cloud + NOMAD[long-tail]:
   * **2,707 deposits, 1,332,136 calcs, 30,877,236 frames and 3.41 billion atom-level force
     labels**, at 110 atoms per frame on average. MPtrj has 49.3M force labels at 31 atoms per frame.
   * 713–787 distinct first authors: 787 by exact name, 713 once name variants such as
     "Kavanagh, Seán" / "Kavanagh, Seán R." are merged.
   * In raw frames it is ~20× MPtrj (1.58M) and ~3× sAlex (10.4M). After per-trajectory subsampling
     with sAlex's rule (keep a frame only if its energy moved > 10 meV/atom; same code for every
     dataset), **2.67M frames remain — 3.8× MPtrj's 0.71M under the same rule** (8.7M at
     1 meV/atom).
   * The rule collapses equilibrium MD, which is half of Zenodo's frames, so 2.67M is a lower bound
     (§4.4).
3. **It is a different kind of data from the megasets.**
   * **Structures**: by frames, 35% are bulk, 52% slab / 2D and 12% molecule / cluster
     (vacuum-gap classification; bulk stays at 34.7–35.8% for 4.5–8 Å thresholds). Every reference
     set is ≥ 97.7% bulk. The median frame has 39–131 atoms, depending on the source, against
     8–22 in the references.
   * **Calculation types** (by frames): 38% AIMD (3,621 runs; median 300 K, up to ~1,500 K), 54%
     relaxation steps, 2.4% phonon finite displacements, 2.4% NEB images and 2.9% statics.
   * **Functionals**: half of the frames are not plain PBE: RPBE 25%, PBEsol 8.7%,
     dispersion-corrected 12.5%, HSE06 1.0%, r2SCAN/SCAN 1.0%, +U 1.7%.
   * **Magnetism and charge**: 17.5% of frames are magnetic (|net moment| > 0.5 μB) and 7.0% are
     charged cells (charged defects).
   * **Forces**: Zenodo and Materials Cloud frames are far from equilibrium (median max |F|
     2.2–2.3 eV/Å, 61–78% of frames above 1 eV/Å). That resembles OMat24's rattled / AIMD frames
     (2.38 eV/Å), but here the frames come from real MD of surfaces and interfaces.
     NOMAD[long-tail] is relaxation-like: 0.24 eV/Å, against MPtrj's 0.20.

   OMat24's own paper lists "point defects, surfaces, non-stoichiometry and lower dimensional
   structures" as absent; most of the long tail sits exactly there.
4. **A measurable share of its chemistry is absent from all the megasets.** MP ∪ Alexandria is the
   superset of MPtrj, of sAlex and of OMat24's seed structures.
   * Against MP ∪ Alexandria, 26,771 of the long tail's 55,189 chemical systems are absent. They
     hold 9.7% of calcs, 12.9% of frames and 7.2% of effective frames, and occur in 434 of the
     2,707 deposits (16%).
   * Against MPtrj alone, 73% of the chemical systems are absent; they hold 32% of calcs, 34% of
     frames and 29% of effective frames, in 28% of deposits.
   * Seven elements (Am, At, Cm, Fr, Po, Ra, Rn) occur in none of the references. The long tail
     contains every element any reference has.
   * **Caveat**: distinct-system counts are dominated by combinatorial deposits. About half of the
     absent systems (≈13.5k) come from one Materials Cloud deposit (HEA25: random 6–8-element
     alloys), so the calc, frame and deposit shares are the robust numbers. Formula and prototype
     novelty are higher (75.9% / 71.0% of frames against MP ∪ Alexandria), but slab and supercell
     compositions inflate them (§5.4).
5. **It is heterogeneous, so it must be bucketed.**
   * There are 106 / 40 / 65 consistency buckets (XC label × POTCAR release family) in Zenodo /
     Materials Cloud / NOMAD[long-tail]; the largest is plain PBE.
   * 5.49M long-tail frames (17.8%, from 1,330 deposits) follow the Materials Project GGA(+U) recipe
     exactly: MP's POTCAR symbols in the PBE release, MP's U values, and no vdW / meta-GGA / hybrid.
     They therefore share MPtrj's energy reference and can be co-trained with it. 1.68M of them
     also use ENCUT ≥ 520 eV.
   * 27,021 groups of calcs (156k calcs) start from an identical structure but use different
     functionals. These are ready-made multi-fidelity pairs.
6. **Labels are clean but must be filtered.**
   * Integrity is exact: every scanned frame matches its metadata, no frame is unreadable, and
     1,135 frames carry a non-finite label.
   * **96.2% of frames (29.69M) pass the default training-time filters.** The largest removals are
     NEB images with projected VTST forces (749k), SCF-unconverged frames (195k), max |σ| > 80 GPa
     (125k), E > 0 (70k), atoms closer than 0.5 Å (31k) and max |F| > 50 eV/Å (30k).
   * **Gap 1**: a few corrupt frames have absurd energies (E/atom down to −263,758 eV) and no
     default filter catches them. The 1st–99th percentiles are physical, so this is < 1% of frames.
   * **Gap 2**: the "E > 0" rule is a poor proxy for a bad label, because in some deposits the
     vdW-DF-family energies sit near zero.
   * Both gaps need a per-bucket robust energy filter in part B (§7).
   * 31% of frames carry no stress, mostly ISIF = 0 MD.
7. **Redundancy is modest and mostly inside NOMAD.**
   * 91% of frames are unique structures.
   * There are 2.50M exact duplicate frames (8.1%) and 135k redundant calcs. 70k of the
     redundant-calc groups are the same calculation stored in several NOMAD uploads.
   * Overlap between sources is small: 90,075 identical frames between Zenodo and NOMAD, and 490
     between Zenodo and Materials Cloud.
8. **Concentration is the main caveat.**
   * In Zenodo, one deposit (concept record `5720008`, RPBE AIMD of water on transition-metal
     surfaces) holds 30.5% of frames. The effective number of first authors by frames (1/HHI) is 8.8.
   * In Materials Cloud, one record holds 65.6% of frames.
   * In NOMAD[long-tail], the top author holds 29.6% of frames (1/HHI 8.3).
   * By calcs, Zenodo is far more even (1/HHI 22 deposits / 21 first authors). NOMAD[long-tail] is
     not (1/HHI 5.1 first authors): TU Darmstadt's 350k high-throughput statics alone are 40% of its
     calcs.
   * Per-trajectory subsampling and per-deposit weighting are prerequisites for training.
9. **The long tail is growing.**
   * Distinct first authors publishing raw VASP data, per year: 50 (2020), 71, 85, 110, 128,
     201 (2025), and 229 in 2026 up to the harvest in September. 177 of those 2026 first authors
     are new.
   * Deposit counts overstate the growth: one NOMAD uploader made 622 of the 712 NOMAD uploads
     dated 2026.

### 1.1 How the Alexandria-group uploads were found, and what they are

* **Found** while building the metadata statistics. 63% of NOMAD's frames reported VASP "6.0" with
  one identical recipe, and 1,865 of its 3,695 uploads were created in 2024 — unusual for
  independent uploads. Grouping by uploader showed one person (M. Marques) behind 5.95M calcs
  (84%). Their functionals are exactly Alexandria's three sets (PBE, PBEsol, SCAN), and the
  conclusive evidence is in the stored NOMAD `mainfile` paths: Alexandria material ids
  (`agm004014910`) and the group's directory naming (`xxx_02a-00_…_spg216/GEO1_vasprun.xml.bz2`;
  GEO1, GEO2, … are successive relaxation runs). The 2017 uploads (S. Botti) use the same naming
  for a cubic-perovskite set. The identification is by ID, not inference. A structural
  cross-check against Alexandria's material list was not needed for the decision and was not run;
  `INDIVIDUAL_ONLY=0` would run it (~40 CPU-h).
* **Not one upload but a bulk, standardised deposition**: 1,719 uploads (1,704 by Marques between
  Sept 2024 and Jan 2025, 15 by Botti in July 2017), 50–16,896 calcs each (median 3,354). They are
  split by functional (1,272 PBE uploads, 432 PBEsol + SCAN), and none mixes with other data:
  NOMAD's 1,976 other uploads contain no such calc. In the shards, 4,030 of 5,345 hold only these
  calcs and 411 are mixed.
* **Homogeneous**: every calc is a cell relaxation or a static, with ISPIN = 2, Gamma-centred
  grids, no dispersion correction and almost all `PREC = High`; 98.8% of frames use
  `ISMEAR = 1, SIGMA = 0.2`. There are 9 consistency buckets (vs 65 in NOMAD's 870k-calc long
  tail), and 68.5% of frames have MP-compatible POTCARs and U values. Their NOMAD metadata carry no
  paper reference or DOI.
* **Recommendation**: leave them out of the delivered dataset. They fail two of the inclusion
  criteria agreed for this project (`docs/EXTERNAL_DATA_SOURCES.md`: data from individual
  researchers, not homogeneous institutional high-throughput; not already used for MLIP
  training), for the same reason the NOMAD harvest already excluded AFLOW / OQMD / MP. Alexandria
  is public (CC-BY-4.0) and is the parent of sAlex and OMat24's seeds, so delivering it adds
  nothing new. If wanted, it can ship as a clearly separated optional bucket; `origin` selects it.
  A related judgement call remains within the long tail: some individual uploads are themselves a
  lab's high-throughput screening (e.g. TU Darmstadt's 350k PBE statics, 40% of NOMAD[long-tail]
  calcs). They are not in any megaset, but they are homogeneous, so treat them as their own bucket
  or cap their weight.

---

## 2. What was measured, and how

`python -m dataset_stats.cli {meta, scan, ref-fetch, ref-scan, report}` — read-only on the
datasets; details in `dataset_stats/CLAUDE.md`.

| pass | input | output | measured on CSD3, 2026-10-03 |
|---|---|---|---|
| `meta` | `metadata.jsonl`, streamed in byte ranges | one row per calc: parser, XC family / label, +U, dispersion, POTCAR releases, MP/OMat24 compatibility, calc type, ENCUT/EDIFF/PREC/smearing/k-grid, convergence, net moment/charge, availability, licence, year, deposit, origin | 6–80 s per source on 32 cores (NOMAD's 27.8 GB: 80 s) |
| `scan` | every `shard-*.extxyz.gz` holding individual uploads: 3,467 shards (all 7,497 with `INDIVIDUAL_ONLY=0`) | one row per frame (energy, E_free−E0, max/mean/RMS \|F\|, \|ΣF\|, pressure, max \|σ\|, volume, net moment/charge, SCF tag, structure + frame hashes); per calc the first/last frame's formula, chemical system, vacuum gaps, shortest distance, space group, density; per-atom \|F\| histogram | 5.37 CPU-h (Zenodo 2.36, NOMAD 2.72, Materials Cloud 0.29), i.e. 16 min wall on 32 cores. Before the run it was checked frame by frame against ASE's extxyz reader on 6 real shards: 45,046 frames, 0 mismatches |
| `ref-fetch`, `ref-scan` | MPtrj extxyz, OMat24 val, sAlex val, MP and Alexandria materials, MP elemental references | the same rows, same code | 7.8 GB downloaded at 75–110 MB/s; 40 min on 16 cores (Alexandria's 5.78M entries: 17 min) |
| `report` | all of the above | `report.json` + `report.md` | 17 min on 4 icelake-himem cores |

The jobs finished far inside their 3–4 h limits for three reasons:

* The limits were deliberate safety caps; SL3 charges only what is used, about 21 core-hours in
  total here.
* `--individual-only` left 4,030 of NOMAD's 5,345 shards unread. Those shards hold 6.2M
  single-frame calcs, and their per-calc space-group and neighbour-list analysis is what made a
  full scan ~40 CPU-h.
* The remaining 5.4 CPU-h ran 32-way parallel.

Definitions used throughout:

* **deposit** — the unit a depositor published: a Zenodo / Materials Cloud concept record, a NOMAD
  upload. **First author** — the deposit's first creator (a NOMAD upload lists its uploader), the
  proxy for "independent research groups".
* **calc type** — from the effective INCAR: `md` (IBRION = 0), `relax` / `relax-cell` (IBRION
  1/2/3; ISIF ≥ 3), `phonon` (IBRION 5–8), `neb` (IMAGES > 0), `saddle` (dimer/Lanczos), `static`
  (NSW ≤ 0), `nscf` (ICHARG ≥ 10 or line-mode k-points), `mlff` (ML_LMLFF).
* **XC family / label** — re-derived from the effective parameters (LHFCALC, HFSCREEN, METAGGA,
  GGA tag, else the POTCAR prefix), plus `+U` and the dispersion method (IVDW from the user INCAR;
  LUSE_VDW = nonlocal). pymatgen's `run_type` echo is kept but not used for grouping
  (`revPBE+Padé` is GGA = RP, i.e. RPBE).
* **MP-compatible** — every POTCAR symbol is MPRelaxSet's for that element and its titel is in
  the PBE release; MP's U values on O/F compounds of Co/Cr/Fe/Mn/Mo/Ni/V/W and no U otherwise; PBE
  without vdW, meta-GGA or hybrid. **OMat24-compatible** — the same recipe with the PBE_54 release,
  W_sv and Yb_3.
* **dimensionality** — from the vacuum gaps of a calc's first frame along the cell axes (a gap
  ≥ 6 Å is vacuum): bulk (none), slab / 2D (one axis), wire (two), molecule / cluster (three).
* **bulk prototype** — reduced formula × spglib space group (symprec 0.1, cells ≤ 400 atoms) of a
  bulk calc's first frame; P1 cells are left out, since they carry no prototype information.
* **duplicates** — a structure hash (species + cell + positions rounded to 10⁻⁴ Å) and a frame
  hash (+ energy to 10⁻⁶ eV) find exact duplicates only. An exact duplicate calc has identical
  first and last frames and the same length.
* **effective frames** — frames kept by the sAlex rule (within a trajectory, keep a frame only if
  its energy differs by > 10 meV/atom from the last kept one); 1 and 50 meV/atom and
  "endpoints only" are reported too.
* **formation-energy proxy** — E/atom minus MP's elemental reference energies (2023-02-07). It is
  computed only for frames on MP's energy scale (MP-compatible, no +U) and for the references.
  Slabs and molecules include their surface or binding energy.
* **novelty** (vs a reference) — the share of calcs / frames / effective frames / deposits whose
  element set, chemical system, reduced formula or bulk prototype is absent from the reference.
  Prototype shares are taken over calcs that have a prototype (non-P1 bulk).

---

## 3. Results from the metadata

### 3.1 Size and concentration

| | Zenodo | Materials Cloud | NOMAD [long-tail] | NOMAD [Alexandria group] | NOMAD (all) |
|---|---:|---:|---:|---:|---:|
| deposits | 629 | 102 | 1,976 | 1,719 | 3,695 |
| first authors (exact name) | 494 | 78 | 222 | 2 | 223 |
| calcs | 386,425 | 75,751 | 869,960 | 6,203,632 | 7,073,592 |
| frames | 18,243,690 | 2,545,669 | 10,087,877 | 42,371,188 | 52,459,065 |
| atoms with a force label | 2.48 B | 0.28 B | 0.66 B | 0.54 B | 1.19 B |
| mean atoms per frame | 136 | 108 | 65 | 13 | 23 |
| frames with stress | 10,856,184 | 706,523 | 9,626,537 | 42,371,188 | 51,997,725 |
| frames / calc (mean; median; p95; max) | 47; 1; 136; 200,000 | 34; 1; 84; 7,439 | 12; 1; 62; 35,611 | 7; 1; 29; 250 | 7; 1; 31; 35,611 |
| top-1 deposit share of frames | 30.5% | 65.6% | 15.2% | 0.2% | 2.9% |
| top-1 first author share of frames | 30.5% | 65.6% | 29.6% | 99.4% | 80.3% |
| effective number of first authors (1/HHI, frames) | 8.8 | 2.2 | 8.3 | 1.0 | 1.5 |

Merging name variants ("Surname, Given" vs "Given Surname", middle initials, affiliation text in
the name field) lowers the first-author counts to 455–468 for Zenodo, 77–78 for Materials Cloud
and 219–222 for NOMAD[long-tail].

### 3.2 Calculation types (share of frames; calcs in brackets)

| | Zenodo | Materials Cloud | NOMAD [long-tail] | NOMAD [Alexandria] |
|---|---:|---:|---:|---:|
| MD | 51.1% (2,654) | 65.6% (422) | 7.6% (545) | — |
| relaxation (ions) | 37.1% (89,664) | 28.4% (17,222) | 55.8% (160,788) | — |
| relaxation (cell) | 4.3% (35,663) | 3.5% (6,751) | 25.2% (104,350) | 94.4% (3,820,753) |
| NEB images | 3.1% (5,184) | 0.2% (39) | 1.7% (1,351) | — |
| phonon displacements | 2.1% (4,311) | 0.2% (262) | 3.6% (6,466) | — |
| static | 1.6% (242,834) | 2.0% (50,785) | 5.6% (569,338) | 5.6% (2,382,879) |
| non-self-consistent | 0.0% (4,556) | 0.0% (266) | 0.3% (26,986) | — |
| saddle / other / MLFF | 0.7% | 0.0% | 0.3% | — |

MD temperatures (TEBEG, frame-weighted, where the INCAR records it): median 300 K, 95th
percentile ~1,200–1,500 K. Materials Cloud's MD is OUTCAR-only without an INCAR echo, so its
temperatures are unknown.

### 3.3 Exchange-correlation and settings (share of frames)

| | Zenodo | Materials Cloud | NOMAD [long-tail] | NOMAD [Alexandria] |
|---|---:|---:|---:|---:|
| PBE | 47.6% | 27.1% | 90.8% | 75.9% |
| RPBE | 32.7% | 69.7% | 0.2% | — |
| PBEsol | 12.8% | 1.3% | 3.0% | 23.1% |
| SCAN / r2SCAN | 1.2% | 0.2% | 0.9% | 1.0% |
| hybrids (HSE06, PBE0, B3LYP, HF) | 1.4% | 0.5% | 0.8% | — |
| vdW-DF family (optPBE, optB88, optB86b, vdW-DF2, -cx, BEEF) | 2.1% | 1.1% | 2.9% | — |
| any dispersion correction | 15.1% | 2.5% | 10.3% | 0 |
| +U | 1.5% | 1.3% | 2.2% | 0.9% |
| spin-polarised calcs (ISPIN = 2) | 30.9% | 13.7% | 40.7% | 100% |
| SOC calcs | 5,890 | 1,673 | 14,970 | 0 |
| VASP 6.x frames | 32.4% | 5.0% | 17.0% | 98.8% |
| ENCUT (frames: p5 / median / p95, eV) | 350 / 400 / 700 | 300 / 400 / 500 | 172 / 400 / 650 | 281 / 359 / 520 |
| PREC = Accurate / High | 40.2% | 87.7% | 44.4% | 100% |
| k-point density (KPPRA, median calc) | 873 | 6,912 | 864 | 1,100 |

POTCARs: 83–98% of frames use titels that exist in the PBE releases (52/54/64 or the legacy PBE
set); LDA / PW91 / ultrasoft potentials are < 3%. Every calc carries its titels and
`potcar_set_hash`, so consistency buckets can be exact.

### 3.4 Quality, label consistency, electronic properties

| | Zenodo | Materials Cloud | NOMAD [long-tail] | NOMAD [Alexandria] |
|---|---:|---:|---:|---:|
| SCF-unconverged frames (tagged) | 51,563 (0.28%) | 3,701 (0.15%) | 139,999 (1.39%) | 402,136 (0.95%) |
| relaxations not ionically converged | 12,310 of 125,327 | 514 of 23,973 | 5,965 of 265,138 | 12,787 of 3,820,753 |
| frames without stress | 40.5% | 72.2% | 4.6% | 0% |
| max \|E_free − E0\| per atom (calc p99) | 23 meV | 41 meV | 23 meV | 3 meV |
| net moment known / charge known (frames) | 99.9% / 99.6% | 100% / 100% | 98.2% / 100% | 96.2% / 100% |
| magnetic, \|m\| > 0.5 μB (calcs / frames) | 79,511 / 1.68M | 6,071 / 0.17M | 208,623 / 3.57M | 1.76M / 12.9M |
| charged cells, \|q\| > 0.01 e (calcs / frames) | 27,357 / 1.91M | 1,864 / 35.5k | 9,221 / 206k | 0 |
| MP-compatible frames (calcs, deposits) | 2.57M (110,279; 227) | 0.26M (7,222; 37) | 2.66M (177,368; 1,066) | 29.0M (3.96M; 1,287) |

### 3.5 Chemistry (from the structures in the shards)

| | Zenodo | Materials Cloud | NOMAD [long-tail] | long tail (union) |
|---|---:|---:|---:|---:|
| elements | 88 | 96 | 95 | 96 |
| chemical systems | 4,147 | 22,864 | 31,339 | 55,189 |
| reduced formulas | 14,336 | 27,059 | 124,009 | 162,819 |
| non-P1 bulk prototypes | 7,384 | 2,057 | 267,394 | 274,681 |
| unary / binary / ternary / 4+ (calcs) | 6 / 33 / 25 / 35% | 13 / 26 / 24 / 38% | 18 / 14 / 50 / 18% | — |
| most frequent chemical systems (calcs) | Ga–N, C–H–N–O–S, C–H–O, Mg–O–Zn, Cd–Te, Co–Cr–Fe–Mn–Ni | Ge, Cu–In–S, H, C–H, H–O–Pt | Fe–Ti, Co–Fe, Cr, B–Fe, C–H–N–O–Zn | — |

* **The sources are complementary.** Few chemical systems are shared between them: Zenodo ∩
  NOMAD 2,301, Zenodo ∩ Materials Cloud 457, Materials Cloud ∩ NOMAD 813. Each source has many of
  its own: Zenodo 1,799, NOMAD 28,635, Materials Cloud 22,004 (of which ≈13.5k are HEA25's random
  high-entropy alloys).
* **Element sets from the POTCARs differ from the structures in a few calcs.** In 658 Zenodo and
  28 Materials Cloud calcs, the POTCAR list names an element that has no atoms in the structure.
  Examples: a GaN POTCAR set used for a structure that holds only N, and a six-element HEA POTCAR
  used for 4–5 element cells. NOMAD has 11 such calcs, partly the titel-parsing case fixed in §8.
  This explains the ±1 element and ±2 chemical-system differences from the POTCAR-based metadata
  counts.
* **The labels are unaffected**: the stored species are VASP's own, and the statistics use the
  structure's elements.

### 3.6 Availability of heavy outputs (recorded, not stored) and provenance

* **Heavy outputs** (long tail, share of calcs whose source deposit holds each output): DOS 43.5%,
  eigenvalues 46.1%, magnetization 37.9%, projections 25.0%, charge density 19.9% (in 1,436
  deposits), wavefunction 15.9%, spin density 12.4%.
* **Licences** (by frames): Zenodo 97.8% CC-BY-4.0. NOMAD 100% CC-BY-4.0. Materials Cloud 65.6%
  CC-BY-SA-4.0, 26.6% CC-BY-4.0 and 7.8% MIT.
* **Growth**, as distinct first authors per year across the long tail (new ones in brackets):

  | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 (to Sept.) |
  |---:|---:|---:|---:|---:|---:|---:|
  | 50 (37) | 71 (59) | 85 (68) | 110 (85) | 128 (108) | 201 (164) | 229 (177) |

  The census also found ~30 new VASP deposits a month on Zenodo alone. Deposit counts are a poorer
  measure: one NOMAD uploader made 622 of the 712 NOMAD uploads dated 2026. Two Zenodo records
  carry depositor-entered publication years of 2027 and 2100.

---

## 4. Results from the frames (CSD3 scan)

### 4.1 Structure types (share of frames; calcs in brackets)

| | Zenodo | Materials Cloud | NOMAD [long-tail] | long tail | references (MPtrj, OMat24 val, sAlex val, MP, Alexandria) |
|---|---:|---:|---:|---:|---:|
| bulk (no vacuum) | 35.0% (67.4%) | 5.4% (69.7%) | 42.3% (80.7%) | 35.0% (76.2%) | 97.7–100% |
| slab / 2D | 63.4% (22.5%) | 93.9% (23.4%) | 22.1% (4.1%) | 52.4% (10.5%) | 0.0–2.1% |
| wire / 1D | 0.3% | 0.0% | 0.2% | 0.3% (0.1%) | 0.0–1.2% |
| molecule / cluster | 1.3% (9.7%) | 0.7% (6.9%) | 35.4% (15.3%) | 12.4% (13.2%) | ≤ 0.1% |
| atoms per frame, p5 / median / p95 | 32 / 108 / 299 | 24 / 131 / 144 | 8 / 39 / 194 | mean 110 | medians 8–22, p95 20–102 |
| bulk space groups / P1 share of bulk calcs | 178 / 65% | 103 / 60% | 226 / 6% | — | 113–229 / 1–10% (OMat24: 95%, rattled) |

* **Slab-heavy sources.** Zenodo and Materials Cloud are slab-dominated by frames: surface /
  interface MD and adsorbate relaxations, with a median vacuum width of 15 Å.
* **NOMAD[long-tail]'s molecules.** Its 35% molecule / cluster frames are clusters and molecules
  in boxes; 63k of its calcs have no neighbour within 3 Å, i.e. isolated atoms or very sparse cells.
* **Why Zenodo's bulk is mostly P1.** The bulk P1 share is high in Zenodo and Materials Cloud
  because they hold MD snapshots, defect supercells and amorphous cells. These are real
  low-symmetry configurations, unlike OMat24's rattled cells.
* **Short distances.** 2,171 calcs start with two atoms closer than 0.5 Å, and 94k calcs have a
  pair closer than 1 Å; a pair below 1 Å is normal for bonds to hydrogen (O–H is 0.97 Å). Frames
  with atoms closer than 0.5 Å are removed by the curation filter in §4.3.

### 4.2 Labels: forces, stress, energies

| | Zenodo | Materials Cloud | NOMAD [long-tail] | long tail | MPtrj | OMat24 val | sAlex val |
|---|---:|---:|---:|---:|---:|---:|---:|
| median max \|F\| (eV/Å) | 2.16 | 2.29 | 0.243 | ≈ 1.0 | 0.203 | 2.38 | 0.030 |
| p95 max \|F\| (eV/Å) | 7.04 | 4.56 | 4.2 | 6.2 | 2.36 | 18.5 | 1.68 |
| frames with max \|F\| < 0.05 eV/Å | 11.2% | 2.3% | 20.6% | 13.6% | 17.7% | 0.1% | 52.1% |
| frames with max \|F\| > 1 eV/Å | 61.3% | 78.4% | 22.2% | 50.0% | 13.2% | 78.1% | 10.6% |
| median per-atom \|F\| (eV/Å) | 0.383 | 0.533 | 0.041 | ≈ 0.29 | 0.090 | 1.15 | 0.0069 |
| median pressure (GPa) | −0.26 | −1.3 | −0.04 | — | 0.01 | 1.34 | 0.003 |
| frames with max \|σ\| > 10 GPa (of frames with stress) | 4.1% | 3.8% | 1.8% | 3.0% | 4.9% | 17.3% | 4.7% |
| p95 \|E_free − E0\| per atom | 1.6 meV | 1.5 meV | 0.5 meV | — | — | — | — |

* **Forces by calc type.** Zenodo's MD has a median max |F| of 3.0 eV/Å and NEB images 4.4 eV/Å.
  Relaxations sit at 0.28 eV/Å, cell relaxations at 0.03 eV/Å and phonon displacements at
  0.41 eV/Å (`report.md` has every calc type).
* **Net force.** The net force per atom |ΣF|/N is ~10⁻⁹–10⁻⁸ eV/Å for relaxations and statics,
  i.e. zero. MD frames carry a small net force of 3–7 × 10⁻⁴ eV/Å per atom (median). That is
  negligible against MLIP force errors of ~30 meV/Å, and subtracting the mean force removes it.
* **Absolute energies separate by XC family and deposit.** Median E/atom in Zenodo: PBE −6.24,
  RPBE −4.53, PBEsol −5.87, r2SCAN −9.36, vdW-DF2 −0.73 eV. NOMAD's HLE17 sits at −62 eV/atom. Even
  one meta-GGA moves between deposits: SCAN has a median of −13.1 eV/atom in NOMAD and −20.7 in
  Materials Cloud. Absolute energies are therefore only usable within an XC label × POTCAR set
  bucket (§6.3).
* **Formation-energy proxy** (MP-scale frames only: 5.49M long-tail frames).
  * Medians: Zenodo −1.46 eV/atom, NOMAD 0.28, Materials Cloud 0.11. The references: MPtrj −0.79,
    OMat24 0.21, sAlex −0.09, MP −0.75 and Alexandria −0.16.
  * Share of frames above 0.5 eV/atom: 15.5% in the long tail, against 4.5% in MPtrj and 33% in
    OMat24.
  * These high-energy frames are mostly slabs and clusters, which carry surface energy, and
    off-equilibrium MD — the regime the megasets sample only by rattling.

### 4.3 Label quality and default training-time filters (frames removed; filters overlap)

| filter | Zenodo | Materials Cloud | NOMAD [long-tail] | long tail |
|---|---:|---:|---:|---:|
| NEB image (projected VTST force) | 568,555 | 5,505 | 174,853 | 748,913 |
| SCF unconverged | 51,498 | 3,701 | 139,999 | 195,198 |
| max \|σ\| > 80 GPa | 107,167 | 3,086 | 15,241 | 125,494 |
| E > 0 | 34,156 | 1,535 | 34,729 | 70,420 |
| non-self-consistent (ICHARG ≥ 10, line mode) | 6,729 | 266 | 26,988 | 33,983 |
| atoms closer than 0.5 Å | 11,250 | 2,075 | 17,592 | 30,917 |
| max \|F\| > 50 eV/Å | 17,425 | 1,472 | 11,135 | 30,032 |
| \|E_free − E0\| > 50 meV/atom | 15,029 | 643 | 2,718 | 18,390 |
| VASP MLFF run | 1,209 | 95 | 0 | 1,304 |
| non-finite or unreadable | 238 | 243 | 654 | 1,135 |
| **passing all** | **17,480,838 (95.8%)** | **2,527,959 (99.3%)** | **9,680,869 (96.0%)** | **29,689,666 (96.2%)** |

* **The filters pick up genuine corruption.** Single frames reach max |F| = 1.3 × 10⁷ eV/Å,
  pressures of 6.6 × 10⁵ GPa and E/atom from −263,758 to +12,062 eV.
* **The energy-based rules need replacing in part B.**
  * Absurdly negative energies pass every current filter.
  * "E > 0" is a poor proxy for a bad label. In some deposits the vdW-DF-family energies sit near
    zero: Zenodo's vdW-DF2 median is −0.73 eV/atom and Materials Cloud's BEEF-vdW median −0.38.
    A positive energy there is not necessarily wrong.
  * The replacement is a robust per-bucket rule: median ± k·MAD of E/atom within each XC label ×
    `potcar_set_hash`, plus a per-trajectory jump test.
* **Labels to keep but tag.** The SCF verdict and magnitude are already stored per frame, so
  SCF-unconverged frames can be kept with their tag where useful (e.g. VASP MLFF training runs).

### 4.4 Redundancy and effective size

| | Zenodo | Materials Cloud | NOMAD [long-tail] | long tail | MPtrj | OMat24 val | sAlex val |
|---|---:|---:|---:|---:|---:|---:|---:|
| frames | 18,243,690 | 2,545,669 | 10,087,877 | 30,877,236 | 1,580,395 | 1,025,361 | 553,218 |
| unique structures | 17,009,658 | 2,493,561 | 8,605,412 | 28.0M | 1,556,698 | 1,025,315 | 547,517 |
| exact duplicate frames | 1,135,589 | 37,173 | 1,330,150 | 2,502,912 | 45 | 20 | 3,701 |
| redundant calcs (groups spanning deposits) | 29,523 (4,115) | 3,079 (220) | 102,706 (70,625) | 135,308 (74,960) | — | — | — |
| frames kept, ΔE > 1 meV/atom | 4,727,065 | 617,992 | 3,394,355 | 8,739,412 | 1,051,470 | 995,057 | 551,517 |
| frames kept, ΔE > 10 meV/atom (sAlex rule) | 818,305 | 121,884 | 1,734,788 | **2,674,977** | **707,123** | 832,225 | 538,141 |
| frames kept, ΔE > 50 meV/atom | 482,630 | 89,663 | 1,209,723 | 1,782,016 | 560,938 | 654,275 | 412,020 |
| endpoints only | 506,747 | 90,110 | 1,092,615 | 1,689,472 | 674,406 | 700,368 | 387,886 |
| groups with one start structure, several XC labels | 6,625 | 257 | 20,139 | 27,021 | — | — | — |

* **Comparable rows.** MPtrj is a complete set, so its row is directly comparable: the long tail
  keeps 3.8× as many frames under the same rule. The OMat24 and sAlex rows are random validation
  samples, i.e. trajectory fragments, so their subsampling numbers only show that they were
  already decorrelated.
* **The ΔE rule undercounts MD.** Consecutive MD frames differ in structure while their energies
  per atom barely move, so the rule keeps few of them. The MD-heavy sources (Zenodo 51%, Materials
  Cloud 66% MD frames) are therefore undercounted. A descriptor-based estimate (QUESTS entropy or
  DIRECT sampling, §6.1 #7) is the right measure of their effective size; 8.7M frames at
  1 meV/atom bounds it from above.
* **Cross-source duplicates are few**: 90,075 identical frames Zenodo ∩ NOMAD, 490 Zenodo ∩
  Materials Cloud, 2 NOMAD ∩ Materials Cloud. They are mostly the same authors depositing on two
  platforms.

---

## 5. Comparison with the MLIP training sets in common use

### 5.1 Published characteristics

| dataset | DFT | frames / structures | materials | structure types | sampling | form | licence |
|---|---|---|---|---|---|---|---|
| **MPtrj** (Deng 2023) | VASP PBE / PBE+U (MP) | 1,580,395 (49.3M atom forces) | 145,923 MP materials, 89 elements | bulk crystals | MP relaxation trajectories, subsampled (StructureMatcher, ~every 10th step) | processed JSON / extxyz | MIT (figshare) |
| **OMat24** (Barroso-Luque 2024) | VASP PBE / PBE+U, PBE_54 | 100.8M train + 1.03M val | ~3.2M parent structures from Alexandria | bulk only; 1–100 atoms, mostly < 20 | rattled (300–1000 K Boltzmann), AIMD 1000 / 3000 K, rattled relaxations | aselmdb | CC-BY-4.0 |
| **sAlex** (OMat24 paper) | VASP PBE / PBE+U | 10.4M train + 0.55M val | Alexandria | bulk | Alexandria relaxations, ΔE > 10 meV/atom subsample, WBM-matched removed | aselmdb | CC-BY-4.0 |
| **Alexandria** (Schmidt 2022–25) | VASP PBE / PBEsol / SCAN | trajectories: 110.8M PBE + 6.1M PBEsol steps | 5,777,914 PBE 3D entries (2025.07.02 release, measured) (+ 2D / 1D sets) | bulk (+2D/1D) | high-throughput relaxations | JSON | CC-BY-4.0 |
| **MatPES** (Kaplan 2025) | VASP PBE + r2SCAN, PBE_64, ENCUT 680 | 434,712 PBE + 387,897 r2SCAN | from 281,572 MP structures | bulk | 300 K NpT MD, 2-stage DIRECT selection | JSON | — |
| **MP-ALOE** (Kuner 2025) | VASP r2SCAN | 909,792 frames / 303,264 relaxations | 89 elements | bulk | active learning, off-equilibrium | — | — |
| **OC20** (Chanussot 2021) | VASP RPBE, no stress | ~265M single points / 1,281,040 relaxations | 82 adsorbates on inorganic surfaces | slabs + adsorbates | relaxations, MD, rattled | lmdb | CC-BY-4.0 |
| **OC22** (Tran 2023) | VASP PBE+U | ~9.85M / 62,331 relaxations | oxide surfaces | slabs + adsorbates | relaxations | lmdb | CC-BY-4.0 |
| **OC25** (2025) | VASP | 7.80M calcs, 1.51M solvent environments | 88 elements | solid–liquid interfaces, avg 144 atoms | relaxations + MD | — | — |
| **MAD** (Mazitov 2025) | Quantum ESPRESSO PBEsol | 95,595 | 85 elements | bulk, rattled, random, surfaces, clusters, molecules | designed for diversity | extxyz | — |
| **MAD-1.5** (2026) | FHI-aims r2SCAN | 216,803 | 102 elements | as MAD | as MAD + LLPR outlier cleaning | — | — |
| **LeMat-Traj** (2025) | VASP PBE / PBEsol / SCAN / r2SCAN | ~120M | MP + Alexandria + OQMD | bulk | relaxation trajectories, filtered | parquet | CC-BY-4.0 |
| ColabFit Exchange | many codes | > 230M configurations in ~400 datasets | — | mixed | an aggregator of published MLIP sets | — | mixed |
| **this work — long tail** | VASP; PBE 60% of frames, RPBE 25%, PBEsol 9%, + vdW-DF, SCAN/r2SCAN, hybrids, LDA; +U; 11 dispersion schemes | **30.9M frames / 1.33M calcs / 2,707 deposits; 2.67M after ΔE > 10 meV/atom** | 96 elements, 55,189 chemical systems, 162,819 formulas | 35% bulk, 52% slab / 2D, 12% molecule / cluster (frames): surfaces, adsorbates, interfaces, defects, molecules | AIMD, relaxations, NEB, phonons, statics | extxyz + raw-provenance metadata | CC-BY 92.6%, CC-BY-SA 5.6% of frames |
| this work — NOMAD Alexandria group | VASP PBE / PBEsol / SCAN | 42.4M frames / 6.2M calcs | Alexandria | bulk | Alexandria relaxations | as above | CC-BY-4.0 |

The long tail has several things none of these sets has:

* **Raw-file provenance per calc**: full INCAR, POTCAR titels, k-points, code version, the
  per-step SCF verdict, net moment and charge, and the deposit's DOI and licence.
* **Many functionals and dispersion schemes from real research practice**, each a separate bucket.
* **Finite-temperature AIMD, NEB, phonon and charged-defect configurations** from published
  studies, mostly on surfaces, interfaces and molecules.
* **A growing source**: 229 first authors published in 2026 alone.

The megasets are internally consistent but single-recipe and almost entirely bulk; the long tail is
the reverse. That is why it complements them rather than competes with them.

### 5.2 Side by side, same code

Every column below was measured by the same scan. OMat24 and sAlex are their published validation
splits (random samples of the training sets); MP and Alexandria are relaxed structures without
forces.

| | long tail | Zenodo | NOMAD [LT] | Materials Cloud | MPtrj | OMat24 val | sAlex val | MP | Alexandria PBE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| frames | 30,877,236 | 18,243,690 | 10,087,877 | 2,545,669 | 1,580,395 | 1,025,361 | 553,218 | 154,718 | 5,777,914 |
| calcs / trajectories | 1,332,136 | 386,425 | 869,960 | 75,751 | 454,594 | 556,347 | 343,645 | — | — |
| deposits / materials | 2,707 | 629 | 1,976 | 102 | 145,923 | 465,875 | 343,645 | 154,718 | 5,777,914 |
| atoms with force labels | 3.41 B | 2.48 B | 0.66 B | 0.28 B | 49.3M | 19.2M | 5.7M | 0 | 0 |
| elements | 96 | 88 | 95 | 96 | 89 | 88 | 86 | 89 | 89 |
| chemical systems | 55,189 | 4,147 | 31,339 | 22,864 | 48,443 | 174,295 | 87,570 | 49,906 | 740,812 |
| reduced formulas | 162,819 | 14,336 | 124,009 | 27,059 | 100,327 | 386,231 | 157,486 | 105,583 | 3,258,894 |
| non-P1 bulk prototypes | 274,681 | 7,384 | 267,394 | 2,057 | 122,789 | 25,209 | 176,126 | 123,336 | 4,980,910 |
| median atoms / frame | (mean 110) | 108 | 39 | 131 | 22 | 12 | 10 | 20 | 8 |
| % frames bulk / slab / molecule | 35 / 52 / 12 | 35 / 63 / 1 | 42 / 22 / 35 | 5 / 94 / 1 | 98 / 1 / 0 | 100 / 0 / 0 | 100 / 0 / 0 | 98 / 2 / 0 | 100 / 0 / 0 |
| median max \|F\| (eV/Å) | ≈ 1.0 | 2.16 | 0.243 | 2.29 | 0.203 | 2.38 | 0.030 | — | — |
| % frames max \|F\| > 1 eV/Å | 50.0% | 61.3% | 22.2% | 78.4% | 13.2% | 78.1% | 10.6% | — | — |
| formation-energy proxy, median (eV/atom) | −0.50 | −1.46 | 0.28 | 0.11 | −0.79 | 0.21 | −0.09 | −0.75 | −0.16 |
| unique structures | 28.0M | 17.0M | 8.61M | 2.49M | 1.56M | 1.03M | 0.55M | 0.15M | 5.78M |
| frames after ΔE > 10 meV/atom | 2.67M | 0.82M | 1.73M | 0.12M | 0.71M | 0.83M | 0.54M | — | — |

### 5.3 What the long tail adds: chemistry absent from each reference

All long-tail sources together; per-source tables are in `report.md`.

| reference (its chemical systems) | chemical systems absent (of 55,189) | % calcs | % frames | % effective frames | % deposits with any |
|---|---:|---:|---:|---:|---:|
| MPtrj (48,443) | 40,497 (73%) | 32.4% | 34.1% | 28.6% | 28.0% |
| MP materials (49,906) | 40,479 (73%) | 32.4% | 34.1% | 28.6% | 27.7% |
| OMat24 val (174,295) | 36,873 (67%) | 25.8% | 37.3% | 25.8% | 52.1% |
| sAlex val (87,570) | 42,554 (77%) | 48.0% | 55.4% | 49.1% | 73.3% |
| Alexandria PBE (740,812) | 26,876 (49%) | 9.8% | 13.2% | 7.3% | 16.7% |
| **MP ∪ Alexandria (743,074)** | **26,771 (49%)** | **9.7%** | **12.9%** | **7.2%** | **16.0%** |

At the other levels, against MP ∪ Alexandria:

* **Formulas**: 113,439 of the 162,819 formulas are absent, holding 41.4% of calcs and 75.9% of
  frames.
* **Bulk prototypes**: 222,497 of the 274,681 non-P1 bulk prototypes are absent, holding 67.4% of
  bulk-prototype calcs and 71.0% of their frames.

Against MPtrj, 65.7% of calcs and 82.5% of frames have a formula MPtrj lacks.

Where the absent chemical systems come from:

* **Materials Cloud** contributes 18,460 of them, ≈13.5k from HEA25 alone.
* **NOMAD[long-tail]** contributes 7,729.
* **Zenodo** contributes 662: few systems, but they carry 13.9% of Zenodo's frames and occur in
  23% of its deposits.

Compounds of the seven elements no reference has occur in NOMAD[long-tail] and Materials Cloud
(At only in Materials Cloud).

### 5.4 How to read the comparison

* **Use MP ∪ Alexandria to bound what the full megasets cover.** OMat24 and sAlex enter as
  validation splits (1% / 5% random samples), so their element / system / formula counts are lower
  bounds of the full sets. OMat24 and sAlex are both drawn from Alexandria; MPtrj ⊂ MP.
* **OMat24's prototypes carry no information.** 95% of its bulk cells are P1 after rattling, so
  "99.9% of long-tail prototypes absent from OMat24" means nothing.
* **Formula novelty is inflated.** Slabs with adsorbates, defect and doped supercells, and
  molecules have stoichiometries no crystal database lists. The chemical system is the robust
  level, and its calc, frame and deposit shares are more robust than distinct counts (HEA25).
* **The ΔE subsampling rule collapses MD** (§4.4).
* **The formation-energy proxy compares energy ranges, not stability.** It covers only the MP-scale
  subset, and slabs and molecules carry surface and binding energies.
* **Exact hashes miss near-duplicates.** Near-duplicates across datasets (the same material from a
  different workflow) show up as composition and prototype overlap. That is the standard way such
  comparisons are made: LeMat-Bulk uses a bonding-graph hash, MPtrj and Matbench Discovery use
  structure matching.

---

## 6. Recognised benchmarks and metrics (research)

No accepted "dataset score" exists. Datasets earn recognition through a shared template, visible in
OMat24, MatPES, MAD, MP-ALOE, LeMat-Traj and MPtrj:

1. composition and label statistics against MPtrj / OMat24 / Alexandria (done: §4–§5);
2. evidence of label consistency;
3. a fixed model architecture trained or fine-tuned with and without the data, scored on shared
   benchmarks;
4. application tests;
5. an open, documented release.

The current norm is quality and diversity over size: MatPES (~0.4M structures) and MAD (~0.1M)
claim parity with far larger sets. Lead with coverage and novelty, not with 73M frames.

### 6.1 Ranked shortlist for this corpus

| # | evaluation | what it shows | cost | tools |
|---|---|---|---|---|
| 1 | **Label-quality audit + consistent subsets** (done in part by `dataset_stats`): SCF tags, \|F−E0\|, extreme labels, **net-force drift \|ΣF\|** (a recognised DFT-error symptom, Kuryla et al. 2025), NEB/nscf/MLFF flags, buckets, MP-compatible subset; a tight-settings recompute of a few hundred stratified frames | that the labels are trustworthy, and which subset is MP-consistent | CPU-hours (+ ~10³–10⁴ core-h DFT for the recompute) | this repo |
| 2 | **Coverage / novelty vs the megasets** (done: §5.3) + **QUESTS** information entropy (Schwalbe-Koda et al., *Nat. Commun.* 2025): dataset entropy H, diversity, and **differential entropy δH of the long tail relative to MPtrj / OMat24** (δH > 0 = environments the reference lacks) | "long-tail value" without training | CPU-hours on a stratified sample | `pip install quests` (BSD-3) |
| 3 | **Zero-shot errors of universal MLIPs** (MACE-MP-0, MACE-OMAT-0 / MPA-0, CHGNet, SevenNet, ORB, UMA) on a stratified sample, by structure class and functional bucket; the **softening scale** (slope of predicted vs DFT forces; Deng et al., *npj Comput. Mater.* 2025) | where state-of-the-art models fail = where this data adds information | inference only (GPU-hours; CPU feasible for ~10⁴ frames) | `mace-torch`, `fairchem`, … |
| 4 | **Fixed-architecture ablation / fine-tuning**: baseline (e.g. MACE on MPtrj or a foundation model) vs + the MP-compatible long-tail subset vs + a random same-size subset; held out **by deposit**; scored on Matbench Discovery plus application tests | the gold standard: does the data improve models | GPU-days | MACE, MatterTune |
| 5 | **Application benchmarks that reward long-tail data**: surfaces / adsorbates (OC20/OC22, CatBench, surface-energy benchmarks, Focassio et al.), point defects, AIMD stability (Matbench Discovery's MD task, MLIP Arena), NEB barriers, phonons (MDR), amorphous; experiment-grounded UniFFBench | improvement where megasets are weak | inference + existing references | MLIP Arena, LAMBench, CHIPS-FF |
| 6 | **Multi-fidelity transfer**: pretrain with per-functional heads (SevenNet-MF / MACE multi-head style), fine-tune on a small clean target (MatPES r2SCAN); the 27k same-structure multi-XC groups (§4.4) are natural test pairs | turns heterogeneity into a result | GPU-days | SevenNet, MACE |
| 7 | **Effective size / redundancy** (exact duplicates and ΔE subsampling done: §4.4): QUESTS compression or DIRECT sampling for the MD-heavy sources, accuracy-vs-subset-size curve | an honest "effective" size | CPU-days | `quests`, `maml` DIRECT |
| 8 | **Release package**: datasheet (Gebru et al.), Croissant metadata, dataset card, Technical Validation; entry in the Matbench Discovery dataset registry (`datasets.yml`); ColabFit submission; a *Scientific Data* Data Descriptor (MAD's precedent) | recognition and reuse | person-days | — |

### 6.2 Benchmark suites, briefly

* **Matbench Discovery** (Riebesell et al., *Nat. Mach. Intell.* 2025): stability classification
  on WBM (F1, discovery acceleration factor), geometry RMSD, thermal-conductivity κ_SRME; CPS =
  0.5 F1 + 0.1 RMSD + 0.4 κ_SRME. Models declare their training sets: MPtrj-only ("compliant")
  versus MPtrj + sAlex + OMat24 and others. It is bulk- and PBE-centred, so it rewards this data
  mainly through the MP-compatible subset, and it is the place to show "no harm / small gain" after
  fine-tuning. Its dataset registry is where a released corpus is listed.
* **MLIP Arena** (NeurIPS 2025 Datasets & Benchmarks; `pip install mlip-arena`): physics stress
  tests (diatomics, EOS, MD stability, NEB, elasticity). **LAMBench** (2025): generalisability
  across domains, including catalysis and reactions. **MDR phonon** benchmark. **CHIPS-FF** (NIST):
  elastic, phonon, defects, surfaces, amorphous. **JARVIS-Leaderboard**. **UniFFBench**:
  experiment-based, functional-agnostic. **MOFSimBench**. **OC20/OC22** leaderboards: surfaces and
  adsorbates.
* Precedents for how datasets justify themselves: element heat-maps and E/F/σ distributions
  versus MPtrj and Alexandria (OMat24, MatPES); several architectures × several datasets
  (MatPES); latent-space maps against other sets (MAD); dedup statistics (LeMat-Bulk); ID / OOD
  splits (OC20, OMat24).

### 6.3 Pitfalls the evaluation must avoid

* **Split by deposit, not by frame.** Trajectory frames are correlated, so a random frame split
  leaks.
* **Never mix absolute energies across buckets.** §4.2 measures eV/atom offsets between XC
  families and between deposits using the same meta-GGA. OMat24 itself reports MP-vs-OMat24
  formation-energy shifts of ~13.5 meV/atom from POTCAR and setting changes alone. Forces
  transfer better than energies; use per-bucket references or heads.
* **Decontaminate against benchmark test sets** before claiming benchmark gains, as Matbench
  Discovery did with WBM prototypes.
* **Report raw and effective sizes separately**, and keep the Alexandria-origin NOMAD subset out
  of any "long-tail" claim.

---

## 7. Next steps (FURTHER_WORK B and C)

**B — combined, curated corpus** (from the long tail only):

1. **Select.** Drop `origin ≠ 0`, i.e. the Alexandria group (§1.1). Decide how to handle the
   homogeneous high-throughput uploads inside the long tail (TU Darmstadt's 350k statics): as their
   own bucket, or with capped weight.
2. **De-duplicate.**
   * Remove the 2.50M exact duplicate frames and the 135k redundant calcs.
   * Most of those calcs are the same calculation in several NOMAD uploads, so de-duplicate
     across deposits, not within them.
   * Then remove the 90k frames shared between Zenodo and NOMAD.
3. **Filter.**
   * Apply the default filters of §4.3.
   * Replace "E > 0" by a per-bucket robust energy rule (median ± k·MAD of E/atom within XC label
     × `potcar_set_hash`) plus a per-trajectory jump test, so that corrupt energies are caught and
     vdW-DF frames are kept.
   * Drop non-finite labels at load.
   * NEB: keep the endpoints and drop the images, whose forces are VTST projections.
4. **Bucket and weight.**
   * Train within XC label × POTCAR-set buckets.
   * Start from the MP-compatible subset: 5.49M frames, or 1.68M at ENCUT ≥ 520 eV.
   * Weight per deposit, because the top deposit holds 30.5% of Zenodo's frames.
5. **Subsample.** Use the ΔE rule for relaxations and descriptor-based selection (QUESTS / DIRECT)
   for MD, and report the resulting effective size.

**C — value study** (§6.1, in rising cost):

6. **QUESTS δH** of the long tail against MPtrj and OMat24 (CPU).
7. **Zero-shot errors of universal MLIPs** on a stratified sample by structure type and bucket,
   including the softening scale.
8. **Fine-tuning ablation.** MACE on MPtrj vs MPtrj + the MP-compatible long tail vs MPtrj + a
   random same-size subset. Hold out by deposit, and score on Matbench Discovery plus surface /
   defect / MD tests.
9. **Release.** Datasheet, Croissant, the Matbench Discovery dataset registry and a *Scientific
   Data* Data Descriptor.

**Optional checks:**

* `INDIVIDUAL_ONLY=0` (~40 CPU-h) to confirm that the Alexandria-group calcs have ≈ 0 novelty
  against Alexandria.
* A yearly census refresh, to follow the long tail's growth (~180 new first authors a year).

---

## 8. Reproducing on CSD3

See `scripts/csd3/stats/README.md`:

```bash
pip install ase-db-backends                       # once (reads OMat24 / sAlex .aselmdb)
S=$(sbatch --parsable scripts/csd3/stats/10_stats.sh)    # meta + scan (individual uploads): 16 min
R=$(sbatch --parsable scripts/csd3/stats/15_refs.sh)     # references: 40 min
sbatch --dependency=afterok:$S:$R scripts/csd3/stats/20_report.sh   # report: 17 min
# then copy stats/report/{report.json,report.md} home (stats_csd3/, gitignored)
```

Three fixes landed after the 2026-10-03 run. None of them changes a number in this document.

* **Reference structure hashes.** The reference readers now record per-calc structure hashes.
  This run's report therefore drops the references' "shared initial structure" counts, and a fresh
  `ref-scan` would fill them in.
* **Markdown tables.** `report.md` table cells now escape `|`.
* **POTCAR titels.** A titel without its library prefix (`Nb_pv 08Apr2002`) now yields the symbol
  `Nb_pv`, not the date. This affected the POTCAR element sets of 11 NOMAD calcs.

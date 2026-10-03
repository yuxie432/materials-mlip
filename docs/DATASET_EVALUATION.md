# Dataset evaluation — statistics, comparison with MLIP training sets, recognised benchmarks

Evaluation of the three harvested VASP corpora (Zenodo, NOMAD direct uploads, Materials Cloud
Archive) for MLIP training, written 2026-10-03. Three parts, as asked:

1. **Statistics** of the datasets — measured by the new read-only `dataset_stats/` package
   (`dataset_stats/CLAUDE.md`; CSD3 runbook `scripts/csd3/stats/`).
2. **Comparison** with the MLIP training sets in common use (MPtrj, OMat24, sAlex/Alexandria,
   MatPES, OC20/22/25, MAD, …), taking de-duplication and subsampling into account.
3. **Recognised benchmarks and metrics** for judging a dataset, and how this corpus can earn
   objective recognition.

**Status.** Everything that comes from `metadata.jsonl` is final and below (§2–§3, computed from
the rsynced metadata of all three datasets). Everything that needs the frames themselves —
elements, compositions, structure types, force/stress/energy distributions, duplicates, effective
size after subsampling, and the like-for-like comparison with MPtrj / OMat24 / sAlex / MP /
Alexandria — comes from one CSD3 run of `scripts/csd3/stats/` (§7) and is marked ⏳ until then.

---

## 1. Key findings so far

1. **Most of the NOMAD harvest is the Alexandria database, not the long tail.** 6,203,632 of
   NOMAD's 7,073,592 calcs (88%) and 42,371,188 of its 52,459,065 frames (81%) are institutional
   high-throughput runs of the Alexandria group (M. A. L. Marques, S. Botti), uploaded to NOMAD as
   ordinary "direct uploads" (no `external_db` tag, so the harvest's filter could not see them):
   * 5,953,659 calcs whose NOMAD mainfile paths carry **Alexandria ids**
     (`xxx_02a-00_agm004014910_spg216/GEO1_vasprun.xml.bz2`), uploaded mostly in 2024 —
     Alexandria's own PBE (4.05M), PBEsol (1.48M) and SCAN (0.43M) cell relaxations and statics,
     run with one recipe (`PREC = High`, `ISMEAR = 1, SIGMA = 0.2`, ISPIN = 2, Gamma-centred grids);
   * 249,973 calcs of the 2017 Schmidt–Marques–Botti cubic-perovskite high-throughput set
     (`perovskites/Li_LiF3Cs_xxx_02p-00_spg221b/`), the same group's naming scheme.

   These are the data behind Alexandria, sAlex, OMat24's seeds and LeMat-Traj, i.e. already used
   for MLIP training. They are identified by provenance (path patterns, `meta.origin_of`), not by
   author name. Zenodo and Materials Cloud contain none. The statistics are computed for
   **individual uploads only** by default (`--individual-only`): `scan` and `report` skip them,
   while `meta` still describes them (§1.1 below).
2. **The genuine long tail** (Zenodo + Materials Cloud + NOMAD[long-tail]) is **2,707 deposits,
   1,332,136 calcs, 30,877,236 frames, 3.41 billion atom-level force labels** (≈110 atoms per frame;
   MPtrj has 49.3M force labels at ≈31 atoms per frame), from ~794 distinct first authors (494
   Zenodo, 222 NOMAD, 78 Materials Cloud; some overlap). In raw frames that is ~20× MPtrj (1.58M)
   and ~3× sAlex (10.4M), but it is strongly correlated (38% of frames are MD) and concentrated
   (below), so the honest size is the de-duplicated, subsampled one ⏳.
3. **It is a different kind of data from the megasets.** By frames: 38% AIMD (3,621 MD runs;
   300 K median, up to ~1,500 K), 54% relaxation steps, 2.4% phonon finite-displacement frames,
   2.4% NEB images, 2.9% statics. Half of the frames are not plain PBE: RPBE 25%, PBEsol 8.7%,
   dispersion-corrected 12.5%, HSE06 1.0%, r2SCAN/SCAN 1.1%, +U 1.7%;
   17.5% of frames are magnetic (|net moment| > 0.5 μB) and 7.0% are charged cells (net charge ≠ 0,
   e.g. charged defects). MPtrj, sAlex and OMat24 are PBE(+U) bulk crystals by construction; OMat24's
   own paper lists "point defects, surfaces, non-stoichiometry and lower dimensional structures" as
   absent.
4. **It is heterogeneous, so it must be bucketed.** 106 / 40 / 65 consistency buckets (XC label ×
   POTCAR release family) in Zenodo / Materials Cloud / NOMAD[long-tail]. The largest single
   bucket is plain PBE. 5.49M long-tail frames (17.8%, from 1,328 deposits) follow the Materials
   Project GGA(+U) recipe exactly (same POTCAR symbols in the PBE release, MP's U values, no vdW /
   meta-GGA / hybrid), so they share MPtrj's energy reference and can be co-trained with it.
   1.68M of those also use ENCUT ≥ 520 eV.
5. **Labels are clean but must be filtered.** SCF-unconverged frames are 0.63% of the long tail
   (tagged per frame). 31% of long-tail frames carry no stress, mostly ISIF = 0 runs, for which
   VASP computes none (MD defaults to ISIF = 0): Zenodo 40%, Materials Cloud 72%. The flags to
   filter on are already in the data: NEB images, which carry the projected VTST force (749k
   frames); ICHARG ≥ 10 / line-mode non-self-consistent runs (34k frames, invalid energy labels);
   VASP MLFF runs.
6. **Concentration is the main caveat.** In Zenodo one deposit (`5720009`, RPBE AIMD of water on
   metal surfaces) holds 30.5% of frames, and the effective number of first authors by frames
   (1/HHI) is 8.8. Materials Cloud: one record holds 65.6%. NOMAD[long-tail]: top author 29.6%,
   1/HHI 8.3. By calcs Zenodo is far more even (1/HHI 22 deposits / 21 first authors), but
   NOMAD[long-tail] is not (1/HHI 5.1 first authors: TU Darmstadt's 350k high-throughput statics
   alone are 40% of its calcs).
   Per-trajectory subsampling and per-deposit weighting are prerequisites for training.

### 1.1 How the Alexandria-group uploads were found, and what they are

* **Found** while building the metadata statistics. 63% of NOMAD's frames reported VASP "6.0" with
  one identical recipe, and 1,865 of its 3,695 uploads were created in 2024 — unusual for
  independent uploads. Grouping by uploader showed one person (M. Marques) behind 5.95M calcs
  (84%). Their functionals are exactly Alexandria's three sets (PBE, PBEsol, SCAN), and the
  conclusive evidence is in the stored NOMAD `mainfile` paths: Alexandria material ids
  (`agm004014910`) and the group's directory naming (`xxx_02a-00_…_spg216/GEO1_vasprun.xml.bz2`;
  GEO1, GEO2, … are successive relaxation runs). The 2017 uploads (S. Botti) use the same naming
  for a cubic-perovskite set. The identification is by ID, not inference; the CSD3 comparison
  against Alexandria's material list is the structural cross-check.
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
  training) for the same reason the NOMAD harvest already excluded AFLOW / OQMD / MP. Alexandria
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

| pass | input | output | cost (CSD3) |
|---|---|---|---|
| `meta` | `metadata.jsonl`, streamed in byte ranges | one row per calc: parser, XC family / label, +U, dispersion, POTCAR releases, MP/OMat24 compatibility, calc type, ENCUT/EDIFF/PREC/smearing/k-grid, convergence, net moment/charge, availability, licence, year, deposit, origin | minutes (NOMAD's 27.8 GB: 104 s on 8 local cores) |
| `scan` | every `shard-*.extxyz.gz` holding individual uploads (all shards with `INDIVIDUAL_ONLY=0`) | one row per frame (energy, E_free−E0, max/mean/RMS \|F\|, \|ΣF\|, pressure, max \|σ\|, volume, net moment/charge, SCF tag, structure + frame hashes); per calc the first/last frame's formula, chemical system, vacuum gaps (bulk / slab / wire / molecule at 6 Å), shortest distance, space group, density; per-atom \|F\| histogram | ~5 CPU-h for individual uploads (3,467 shards); ~40 CPU-h for everything (measured on 6 real shards: 2,000–3,400 frames/s per core; 220 frames/s for NOMAD shards of 10k single-frame calcs). Checked frame by frame against ASE's extxyz reader on those shards: 45,046 frames, 0 mismatches |
| `ref-fetch`, `ref-scan` | MPtrj extxyz, OMat24 val, sAlex val, MP and Alexandria materials, MP elemental references | the same rows, same code | ~1–2 h, ~10–15 GB download |
| `report` | all of the above | `report.json` + `report.md` | ~0.5 h, ~27 GB RAM (≤ 54 GB for everything) |

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
* **effective frames** — frames kept by the sAlex rule (within a trajectory, keep a frame only if
  its energy differs by > 10 meV/atom from the last kept one); 1 and 50 meV/atom and
  "endpoints only" are reported too.
* **novelty** (vs a reference) — the share of calcs / frames / effective frames / deposits whose
  element set, chemical system, reduced formula or bulk prototype (reduced formula × space group,
  non-P1 bulk cells only) is absent from the reference.

---

## 3. Results available now (from the metadata)

### 3.1 Size and concentration

| | Zenodo | Materials Cloud | NOMAD [long-tail] | NOMAD [Alexandria group] | NOMAD (all) |
|---|---:|---:|---:|---:|---:|
| deposits | 629 | 102 | 1,976 | 1,719 | 3,695 |
| first authors | 494 | 78 | 222 | 2 | 223 |
| calcs | 386,425 | 75,751 | 869,960 | 6,203,632 | 7,073,592 |
| frames | 18,243,690 | 2,545,669 | 10,087,877 | 42,371,188 | 52,459,065 |
| atoms with a force label | 2.48 B | 0.28 B | 0.66 B | 0.54 B | 1.19 B |
| mean atoms per frame | 136 | 108 | 65 | 13 | 23 |
| frames with stress | 10,856,184 | 706,523 | 9,626,537 | 42,371,188 | 51,997,725 |
| frames / calc (mean; median; p95; max) | 47; 1; 136; 200,000 | 34; 1; 84; 7,439 | 12; 1; 62; 35,611 | 7; 1; 29; 250 | 7; 1; 31; 35,611 |
| top-1 deposit share of frames | 30.5% | 65.6% | 15.2% | 0.2% | 2.9% |
| top-1 first author share of frames | 30.5% | 65.6% | 29.6% | 99.4% | 80.3% |
| effective number of first authors (1/HHI, frames) | 8.8 | 2.2 | 8.3 | 1.0 | 1.5 |

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
| MP-compatible frames (calcs, deposits) | 2.57M (110,271; 226) | 0.26M (7,222; 37) | 2.66M (177,367; 1,065) | 29.0M (3.96M; 1,287) |

### 3.5 Chemistry from the POTCAR element sets (preliminary; exact compositions come from the scan ⏳)

| | Zenodo | Materials Cloud | NOMAD [long-tail] | NOMAD [Alexandria] | long tail (union) |
|---|---:|---:|---:|---:|---:|
| elements | 89 | 96 | 96 | 89 | ~96 |
| chemical systems | 4,149 | 22,865 | 31,340 | 360,293 | 55,193 |
| unary / binary / ternary / 4+ (calcs) | 6 / 33 / 25 / 35% | 13 / 26 / 24 / 38% | 18 / 14 / 50 / 18% | 0 / 6 / 66 / 28% | — |

Pairwise overlap of chemical systems between the long-tail sources is small (Zenodo ∩ Materials
Cloud 457, Zenodo ∩ NOMAD[long-tail] 2,301, Materials Cloud ∩ NOMAD[long-tail] 813), so the three
sources are complementary. 31,317 of the 55,193 long-tail systems (56.7%) do not occur in the
Alexandria subset inside NOMAD. That subset is only part of Alexandria; the comparison with the
full MP and Alexandria material sets is §4.2. Elements only the long tail has: Am, Cm, Fr, Ra, Rn,
Po, At.

### 3.6 Availability of heavy outputs (recorded, not stored) and provenance

Long tail, share of calcs with each output recoverable from the source deposit: DOS 43.5%,
eigenvalues 46.1%, magnetization 37.9%, projections 25.0%, charge density 19.9% (in 1,436 deposits),
wavefunction 15.9%, spin density 12.4%. Licences: Zenodo 97.8% CC-BY-4.0 (by frames), NOMAD 100%
CC-BY-4.0, Materials Cloud 65.6% CC-BY-SA-4.0 / 26.6% CC-BY-4.0 / 7.8% MIT. Deposits by year
(long tail): 2014–2019 0.5k, 2020–2023 0.6k, 2024–2026 1.6k, so the long tail is growing (the
census found ~30 new VASP deposits a month on Zenodo alone).

---

## 4. Comparison with the MLIP training sets in common use

### 4.1 Published characteristics

| dataset | DFT | frames / structures | materials | structure types | sampling | form | licence |
|---|---|---|---|---|---|---|---|
| **MPtrj** (Deng 2023) | VASP PBE / PBE+U (MP) | 1,580,395 (49.3M atom forces) | 145,923 MP materials, 89 elements | bulk crystals | MP relaxation trajectories, subsampled (StructureMatcher, ~every 10th step) | processed JSON / extxyz | MIT (figshare) |
| **OMat24** (Barroso-Luque 2024) | VASP PBE / PBE+U, PBE_54 | 100.8M train + 1.03M val | ~3.2M parent structures from Alexandria | bulk only; 1–100 atoms, mostly < 20 | rattled (300–1000 K Boltzmann), AIMD 1000 / 3000 K, rattled relaxations | aselmdb | CC-BY-4.0 |
| **sAlex** (OMat24 paper) | VASP PBE / PBE+U | 10.4M train + 0.55M val | Alexandria | bulk | Alexandria relaxations, ΔE > 10 meV/atom subsample, WBM-matched removed | aselmdb | CC-BY-4.0 |
| **Alexandria** (Schmidt 2022–24) | VASP PBE / PBEsol / SCAN | trajectories: 110.8M PBE + 6.1M PBEsol steps | ~4.5M PBE 3D materials (+ 2D / 1D sets) | bulk (+2D/1D) | high-throughput relaxations | JSON | CC-BY-4.0 |
| **MatPES** (Kaplan 2025) | VASP PBE + r2SCAN, PBE_64, ENCUT 680 | 434,712 PBE + 387,897 r2SCAN | from 281,572 MP structures | bulk | 300 K NpT MD, 2-stage DIRECT selection | JSON | — |
| **MP-ALOE** (Kuner 2025) | VASP r2SCAN | 909,792 frames / 303,264 relaxations | 89 elements | bulk | active learning, off-equilibrium | — | — |
| **OC20** (Chanussot 2021) | VASP RPBE, no stress | ~265M single points / 1,281,040 relaxations | 82 adsorbates on inorganic surfaces | slabs + adsorbates | relaxations, MD, rattled | lmdb | CC-BY-4.0 |
| **OC22** (Tran 2023) | VASP PBE+U | ~9.85M / 62,331 relaxations | oxide surfaces | slabs + adsorbates | relaxations | lmdb | CC-BY-4.0 |
| **OC25** (2025) | VASP | 7.80M calcs, 1.51M solvent environments | 88 elements | solid–liquid interfaces, avg 144 atoms | relaxations + MD | — | — |
| **MAD** (Mazitov 2025) | Quantum ESPRESSO PBEsol | 95,595 | 85 elements | bulk, rattled, random, surfaces, clusters, molecules | designed for diversity | extxyz | — |
| **MAD-1.5** (2026) | FHI-aims r2SCAN | 216,803 | 102 elements | as MAD | as MAD + LLPR outlier cleaning | — | — |
| **LeMat-Traj** (2025) | VASP PBE / PBEsol / SCAN / r2SCAN | ~120M | MP + Alexandria + OQMD | bulk | relaxation trajectories, filtered | parquet | CC-BY-4.0 |
| ColabFit Exchange | many codes | > 230M configurations in ~400 datasets | — | mixed | an aggregator of published MLIP sets | — | mixed |
| **this work — long tail** | VASP; PBE 60% of frames, RPBE 25%, PBEsol 9%, + vdW-DF, SCAN/r2SCAN, hybrids, LDA; +U; 11 dispersion schemes | **30.9M frames / 1.33M calcs / 2,707 deposits** (⏳ effective) | ⏳ | bulk, slabs, adsorbates, molecules, interfaces, defects (⏳ shares) | AIMD, relaxations, NEB, phonons, statics | extxyz + raw-provenance metadata | CC-BY 92.6%, CC-BY-SA 5.6% of frames |
| this work — NOMAD Alexandria group | VASP PBE / PBEsol / SCAN | 42.4M frames / 6.2M calcs | Alexandria | bulk | Alexandria relaxations | as above | CC-BY-4.0 |

What the long tail has that none of these has, already visible from the metadata: **raw-file
provenance per calc** (full INCAR, POTCAR titels, k-points, code version, per-step SCF verdict,
net moment and charge, the deposit's DOI and licence); **many functionals and dispersion schemes
from real research practice** (each a separate bucket); **finite-temperature AIMD, NEB, phonon and
charged-defect configurations** from published studies; and a **growing** source (~1.6k of the
2,707 deposits are from 2024–2026). The megasets are internally consistent but single-recipe and
mostly bulk; the long tail is the reverse, which is why it complements them rather than competes.

### 4.2 Like-for-like comparison ⏳ (CSD3)

`scripts/csd3/stats/15_refs.sh` downloads MPtrj (Matbench Discovery's extxyz), OMat24's and sAlex's
validation splits (random samples of the training sets), MP's and Alexandria's relaxed materials
and MP's elemental references, and runs them through the same scan. `report.md` then contains:

* a **side-by-side table** for every source, subset and reference: frames, trajectories, deposits
  or materials, atoms with force labels, elements, chemical systems, formulas, non-P1 bulk
  prototypes, atoms per frame, shares of bulk / slab / molecule cells, max-|F| distribution (median,
  p95, shares below 0.05 and above 1 eV/Å), per-atom |F|, pressure, share with |σ| > 10 GPa, a
  **formation-energy proxy** (E/atom minus MP's elemental references, for MP-recipe frames without
  +U and for the references, so energies sit on one scale), unique structures, and frames after
  ΔE > 10 meV/atom subsampling;
* a **novelty table** against MPtrj, OMat24, sAlex, MP, Alexandria and MP ∪ Alexandria: the share of
  calcs / frames / effective frames / deposits whose element set, chemical system, formula or
  prototype is absent from the reference, plus the distinct counts. This answers "what fraction of
  the long tail is genuinely new chemistry / structure, and where". The NOMAD[alexandria-group]
  subset doubles as a sanity check: its novelty against Alexandria should be ≈ 0.

How de-duplication and subsampling enter: within each source, exact duplicates (same structure and
energy) and identical calcs (first + last frame + length) are counted, including across deposits.
Calcs that start from an identical structure with different functionals are counted as
multi-fidelity groups. Every novelty share is also given on effective (subsampled) frames, so
long correlated trajectories cannot inflate it. Between sources, identical frames and structures
are counted pairwise. Near-duplicates across datasets (the same material from a different
workflow) appear as composition / prototype overlap rather than frame identity, which is the
standard way such comparisons are made (LeMat-Bulk uses a bonding-graph hash; MPtrj and
Matbench Discovery use structure matching).

---

## 5. Recognised benchmarks and metrics (research)

No accepted "dataset score" exists. Datasets earn recognition through a shared template, visible in
OMat24, MatPES, MAD, MP-ALOE, LeMat-Traj and MPtrj:

1. composition and label statistics against MPtrj / OMat24 / Alexandria;
2. evidence of label consistency;
3. a fixed model architecture trained or fine-tuned with and without the data, scored on shared
   benchmarks;
4. application tests;
5. an open, documented release.

The current norm is quality and diversity over size: MatPES (~0.4M structures) and MAD (~0.1M)
claim parity with far larger sets. Lead with coverage and novelty, not with 73M frames.

### 5.1 Ranked shortlist for this corpus

| # | evaluation | what it shows | cost | tools |
|---|---|---|---|---|
| 1 | **Label-quality audit + consistent subsets** (done in part by `dataset_stats`): SCF tags, \|F−E0\|, extreme labels, **net-force drift \|ΣF\|** (a recognised DFT-error symptom, Kuryla et al. 2025), NEB/nscf/MLFF flags, buckets, MP-compatible subset; a tight-settings recompute of a few hundred stratified frames | that the labels are trustworthy, and which subset is MP-consistent | CPU-hours (+ ~10³–10⁴ core-h DFT for the recompute) | this repo |
| 2 | **Coverage / novelty vs the megasets** (§4.2) + **QUESTS** information entropy (Schwalbe-Koda et al., *Nat. Commun.* 2025): dataset entropy H, diversity, and **differential entropy δH of the long tail relative to MPtrj / OMat24** (δH > 0 = environments the reference lacks) | "long-tail value" without training | CPU-hours on a stratified sample | `pip install quests` (BSD-3) |
| 3 | **Zero-shot errors of universal MLIPs** (MACE-MP-0, MACE-OMAT-0 / MPA-0, CHGNet, SevenNet, ORB, UMA) on a stratified sample, by structure class and functional bucket; the **softening scale** (slope of predicted vs DFT forces; Deng et al., *npj Comput. Mater.* 2025) | where state-of-the-art models fail = where this data adds information | inference only (GPU-hours; CPU feasible for ~10⁴ frames) | `mace-torch`, `fairchem`, … |
| 4 | **Fixed-architecture ablation / fine-tuning**: baseline (e.g. MACE on MPtrj or a foundation model) vs + the MP-compatible long-tail subset vs + a random same-size subset; held out **by deposit**; scored on Matbench Discovery plus application tests | the gold standard: does the data improve models | GPU-days | MACE, MatterTune |
| 5 | **Application benchmarks that reward long-tail data**: surfaces / adsorbates (OC20/OC22, CatBench, surface-energy benchmarks, Focassio et al.), point defects, AIMD stability (Matbench Discovery's MD task, MLIP Arena), NEB barriers, phonons (MDR), amorphous; experiment-grounded UniFFBench | improvement where megasets are weak | inference + existing references | MLIP Arena, LAMBench, CHIPS-FF |
| 6 | **Multi-fidelity transfer**: pretrain with per-functional heads (SevenNet-MF / MACE multi-head style), fine-tune on a small clean target (MatPES r2SCAN) | turns heterogeneity into a result | GPU-days | SevenNet, MACE |
| 7 | **Effective size / redundancy**: dedup, ΔE subsampling, QUESTS compression or DIRECT sampling, accuracy-vs-subset-size curve | an honest "effective" size | CPU-days | `quests`, `maml` DIRECT |
| 8 | **Release package**: datasheet (Gebru et al.), Croissant metadata, dataset card, Technical Validation; entry in the Matbench Discovery dataset registry (`datasets.yml`); ColabFit submission; a *Scientific Data* Data Descriptor (MAD's precedent) | recognition and reuse | person-days | — |

### 5.2 Benchmark suites, briefly

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

### 5.3 Pitfalls the evaluation must avoid

* **Split by deposit, not by frame.** Trajectory frames are correlated, so a random frame split
  leaks.
* **Never mix absolute energies across buckets.** OMat24 itself reports MP-vs-OMat24 formation
  energy shifts of ~13.5 meV/atom from POTCAR and setting changes alone. Forces transfer better
  than energies; use per-bucket references or heads.
* **Decontaminate against benchmark test sets** before claiming benchmark gains, as Matbench
  Discovery did with WBM prototypes.
* **Report raw and effective sizes separately**, and keep the Alexandria-origin NOMAD subset out
  of any "long-tail" claim.

---

## 6. What this means for the next phase (FURTHER_WORK B and C)

* **B (combined corpus)**: build it from the long tail. Treat `NOMAD[alexandria-group]` as
  Alexandria (already public and used for training): exclude it, or keep it only as a tagged,
  separate bucket. The `origin` flag (`meta.origin_of`) is the selector. B3's curation flags are
  now counted per source (`report.md` "Default training-time filters"), and B4's buckets are the
  `label × POTCAR release` table.
* **C (value study)**: the novelty and side-by-side tables (§4.2) are C1. The MP-compatible
  long-tail subset (5.49M frames, 1,328 deposits; 1.68M frames at ENCUT ≥ 520 eV) is the natural
  first ablation set (§5.1 #4), since it shares MPtrj's energy reference.

---

## 7. Reproducing / completing on CSD3

See `scripts/csd3/stats/README.md`:

```bash
pip install ase-db-backends                       # once (reads OMat24 / sAlex .aselmdb)
S=$(sbatch --parsable scripts/csd3/stats/10_stats.sh)    # meta + scan (individual uploads), ~5 CPU-h
R=$(sbatch --parsable scripts/csd3/stats/15_refs.sh)     # references, ~1-2 h
sbatch --dependency=afterok:$S:$R scripts/csd3/stats/20_report.sh
# then copy stats/report/{report.json,report.md} home
```

The ⏳ items in §1, §4 and the long-tail sizes then come straight from `report.md`.

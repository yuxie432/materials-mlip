# Materials Cloud harvest — final result (Sep 2026)

Outcome of the third source adapter (`materials_cloud_harvest/`; design, API facts and decisions in
`MATERIALS_CLOUD_HARVEST.md`). One discover+triage job and one pipeline job on CSD3, code at
`e7e2fe9` + the 8-core sizing of `1b2744f`. Every log, manifest, rejection line and the full
`metadata.jsonl` were reviewed afterwards (2026-09-25); suspicious buckets were settled by probing
the source files themselves over HTTP Range. **Verdict: complete — nothing left that is worth a
recovery job.**

## Headline

| | |
|---|---|
| **Records in dataset** | **102** (of 1,241 on the whole archive) |
| **Calcs** | **75,751** — OUTCAR (ASE) 54,678 · `vasprun.xml` (pymatgen) 21,073 |
| **Frames (ionic steps)** | **2,545,669** — OUTCAR 2,315,872 (91.0%) · vasprun 229,797 |
| Frames with forces / stress | 100% / 706,523 (27.8%) |
| SCF-unconverged frames | 3,701 (0.15%) — tagged per frame, 305 calcs unconverged at the final step |
| Shards / size | 268 `shard-*.extxyz.gz` / 7.3 GiB |
| `verify` | **OK** — exact `frame_id` bijection (0 missing / 0 duplicate / 0 orphan) |
| Licences (frames) | CC BY-SA 4.0 65.6% (one record) · CC BY 4.0 26.6% · MIT 7.8% · CC BY-NC 4.0 55 frames |
| Elements | 96 |
| Metadata coverage (calcs) | net moment 99.98% · net charge 99.90% · POTCAR-set hash 100% · functional 99.90% · resolved `parameters` 99.90% · `ionic_converged` 100% · user INCAR 74.7% (12.5% of frames) |
| Availability flags (calcs) | eigenvalues 58.8% · DOS 56.9% · magnetization 16.3% · projected 6.1% · charge density 1.7% · WAVECAR 1.6% |
| Dataset dir | `$MC_HARVEST_DATA/dataset` (standalone; not merged with Zenodo/NOMAD) |
| Jobs | discover+triage `36245037` (2026-09-24, 15 min); pipeline `36251084` (2026-09-24 18:58 → 09-25 04:40 BST, **9 h 41 min, one attempt, exit 0**) |

## The census → dataset funnel

| Stage | Count | Notes |
|---|---|---|
| Materials Cloud Archive | 1,241 records / 2.54 TB | full census, latest versions |
| discover | 1,226 candidates | 15 licence drops (14 `mcloud-ne-1.0`, 1 `asl`; none mentions VASP) |
| triage (1,434 archives peeked) | **608 records kept → 1,200 fetch units / 1.25 TB** | dropped: 100 below the rank gate, 507 `no_vasp_evidence`, 11 `peek_proved_no_vasp` |
| fetch | 251 units (109 records) with VASP files → 82,828 calc units | 949 units terminal (853 no VASP file, 96 inputs only); **0 pending, 0 deferred for disk** |
| parse | **75,751 calcs** (91.5% of calc units) | 7,076 rejected (every bucket below) + 1 lost to an unlogged exception (now fixed) |
| **dataset** | **102 records** | 7 fetched records yielded nothing (all rejections explained below) |

## Yield by triage decision

| Decision | Records kept | Yielding | Calcs | Frames |
|---|---|---|---|---|
| `vasp_mention` (fail-safe) | 31 | 14 | 15,892 | 1,886,711 |
| `vasp_evidence` (a peek saw a primary) | 53 | 50 | 22,283 | 280,448 |
| `unresolved_fetch` (blind, decision 5) | 524 (984 units, 1.13 TB) | **38 (7.3%; census predicted 7.8%)** | **37,576** | 378,510 |
| **Total** | 608 | 102 | 75,751 | 2,545,669 |

The blind fetch paid off: **half of all MC calcs (49.6%) come from records that no keyword and no
peek could identify** — e.g. `jkaf5-anv04` (24,779 high-entropy-alloy single points), `ervm4-pn188`
(ACE-GCN adsorbates, 3,641 OUTCARs), `2b6s2-z1a23`, `pamwe-h1r97`, `0n7bp-3dg82`, `a93bd-e0216`.
Frames are heavy-tailed: `yspxn-jxt78` (Kristoffersen, CO–CO coupling AIMD at Cu/water) alone is
1,671,206 frames (65.6%) from 440 calcs; then `ervm4-pn188` 7.8%, `mm9k2-h9a91` 7.6%, `njdbv-4yr45`
4.3%, `kf8d0-27289` 2.2%. By calc count: `jkaf5-anv04` 24,779, Bosoni ACWF `yf0rj-w3r97` 7,226
(legacy AiiDA exports), `yayv3-eha98` 4,969, `3t20t-26564` 3,925.

## Run health (pipeline `36251084`, 8 `icelake-himem` cores)

* **Disk valve never tripped**: peak staging 257 GB / 240k inodes against 780 GB / 900k.
* **Parse RAM budget**: 4 workers under 36.8 GiB, peak reservation 35.7 GiB, **0 waits**; primary cap
  3.25 GB → **no `primary_too_large`, no `parse_timeout`** — `30_bigparse.sh` is not needed.
* **Transient failures**: 6 `ConnectTimeout`s, all resolved by the in-run retry pass (2 then fetched —
  `mm9k2` `Pd_100.tgz` 55 calcs, `1etna` `eels.tar.gz` 88 — and 4 reached a terminal no-VASP
  verdict); `pending_units = []`, so no resubmission.
* **Staging left behind**: `raw/` 3.9 GiB / 13.8k inodes = only the rejected calcs' files.

## Rejections — every bucket evaluated

### Parse (7,076 calcs)

| Bucket | Calcs | Where | Cause | Recoverable? |
|---|---|---|---|---|
| Pickles named `OUTCAR_*.pkl` | 2,139 | `ervm4-pn188` | pickled Python objects, not OUTCAR text (the record's real OUTCARs are in: 3,641 calcs) | No — not VASP output (and unpickling untrusted data is unsafe) |
| Fake `NNN_vasprun.xml` | 1,552 | `5jsfj-cdg70` | Quantum ESPRESSO forces rewritten as forces-only XML for `thirdorder_vasp.py` | No — not VASP, no energies |
| VASP MLFF prediction runs | 1,018 | `zybrs-6s419` (`MLFF_pred/`, `4-phonon/`) | `ML_MODE=run` vaspruns: ML-predicted, no electronic steps | No — must not be ingested |
| DFT+DMFT OUTCARs | 1,268 | `zgv8z-jp760` 1,191, `fcwhc-7rd27` 77 | charge-self-consistent DMFT runs, no ionic-step blocks | No |
| Force-less post-processing | 165 | `hdb97-me114` 151 (probed: VASP 6.4 `ALGO = None` runs), `3yke8-xxf82` 14 (HSE/PBE0 band-gap runs, presumed the same) | no `POSITION`/force block at all | No |
| Other no-energy runs | 129 | 17 records, incl. `hmdsb-k3h43` 62, `yf0rj-w3r97` 16 | parsed, but no energy at any ionic step (non-SCF / response) | No |
| Truncated / header-only | 92 | 24 records | `Incomplete OUTCAR` 62, truncated vasprun 15, malformed lines 15 | No |
| **VASP 4.x OUTCARs** | 236 | one Ca-battery group: `45yfz` 116, `4nfmg` 56, `kwfgp` 41, `61kg0` 18 (probed on `61kg0`; the others fail identically), + 5 unverified | VASP 4.6 prints `FREE ENERGIE` *before* `POSITION`; ASE's chunker assumes the ≥ 5.x order (next section) | In principle |
| ASE header quirks | 477 | `jkaf5-anv04` 235 (`NBANDS=` glued to its value), `2a0rw-qhz41` 213 (> 10 species: `ions per type` wraps), 27 fused lattice numbers (c > 100 Å overflows the print) | ASE's `vasp-out` header parser | In principle; almost all single-frame |

Plus **1 calc lost without a rejection line**: a VASP-named test file inside a LAMMPS *source*
tarball (`yzhmw-vje16~lammps-stable.tar.gz`) had 4 atoms but 2 force rows; the frame builder
accepted it, the extxyz writer raised, and the parallel loop logged it without a calc_id. No data
value — but the gap is fixed (below).

### Fetch (1,200 units)

| Bucket | Units | Cause | Recoverable? |
|---|---|---|---|
| `no_vasp_files_fetched` | 853 | no VASP-named member at all — mostly the blind fetch's expected misses (486 of its 524 records held no VASP) + non-VASP archives of mention records | No |
| `no_calc_units_after_extract` | 96 | VASP *inputs* only (INCAR/POSCAR/KPOINTS, e.g. `vasp_input_files_dataset.tar.gz`, `Input_Phonons_Si_R2SCAN.tar.xz`); spot-check `brass_DFT_data.zip` = RuNNer `input.data` + one example run whose OUTCAR is misspelt `OUTCAT` | No |
| `extract_error` (file level) | 18 files | 3 misnamed archives, content probed: `nf76v` `trajectories.tar.xz` (23.6 GB) is **zstd** → LAMMPS `.lammpstrj`; `p354j` `MNb3S6_data_v2.tgz` is a **zip** → Elk FP-LAPW files; `f1dny` `Data.tgz` is **gzip-in-gzip** → Quantum ESPRESSO (its README). 3 sub-archives truncated inside the deposits (outer archives md5-verified): `y0fe9`, `43pkp` `calcs.tar.gz`, `nb3a2` `xtb.tar.gz`. 9 PyTorch `.pth.tar` checkpoints (zips, `2by8y`), 1 ORCA tar (`698j0`), 2 gzipped n2p2 `input.data` (`jcrjj`) | No VASP in any |

### Triage / discover

* The 54 records with peek gaps (sub-archives inside zips, AiiDA databases > 2 GB) were all kept and
  blind-fetched — no triage-stage loss. Their AiiDA exports (MC2D/MC3D, COF discovery, SSSP, JuCLS)
  are QE/FLEUR, as expected.
* The 100 below-rank records hold structures (`*.vasp` POSCARs, CONTCARs), INCAR templates and
  other codes' outputs (spot-check: QuantumATK `.out`) — no VASP output.

## Recoverable in principle — and why no recovery job

1. **VASP 4.x OUTCARs (236 calcs)**. Probed on `61kg0` samples: `vasp.4.6.35` writes each ionic
   step as SCF → `FREE ENERGIE` → lattice → `POSITION/TOTAL-FORCE`; VASP ≥ 5 writes the energy
   *last*. ASE splits an OUTCAR at `FREE ENERGIE`, so its first chunk has no positions and the whole
   file raises. **Validated fix**: split at the `Iteration N(   1)` step markers and feed each step
   to ASE's own `OutcarChunkParser` — tested on a 4.6.35 and a 5.3.3 OUTCAR (3/3 steps each, every
   frame's energy = that step's own σ→0). **Trap**: a "tolerant" reader that just skips ASE's bad
   first chunk would pair step k−1's positions/forces with step k's energy. The NEB images in this
   group are VASP's built-in NEB, whose `TOTAL-FORCE` is the DFT force (see the NEB note), so they
   would be valid labels. Cost: the fix (gated on `vasp.4`, so VASP ≥ 5 parses stay byte-identical)
   + re-fetch of 4 records (~0.5 GB) + a `--retry-rejected` parse; gain ~236 calcs, a few thousand
   frames (~0.1–0.2%). **Not done** — recorded for a future parser pass that could serve every source.
2. **ASE header quirks (477 calcs, ~0.03% of frames)** — a text normalisation of the temp OUTCAR copy
   (split `NBANDS=12345`, join a wrapped `ions per type`, fixed-width lattice lines). NOMAD has ~350
   of the same kind. Not worth it.
3. **Misnamed archives** — extraction dispatches on the file extension; content sniffing
   (magic bytes, peeling a double gzip) would open all three, but all three are non-VASP. Revisit
   only if another source shows VASP-bearing cases.
4. **Truncated archives** — a streaming tar extractor could keep the members before the truncation
   point; none of the three is VASP-evidenced (`43pkp`'s other archives, incl. `aimd.tar.gz`, hold no
   VASP output).

## Data-quality notes for the training phase

* **NEB images (~220 calcs / ≤ 9.9k frames incl. some end-point runs, 0.4%)** parse normally when
  written by VASP ≥ 5: `45yfz-7xa87` (`VPO/NEBx*/{GGA,OPT}/NN`, ~2.9k), `j9cgx-zvp67` (IMAGES=14, 2,086), `a93bd-e0216`
  (IMAGES 1–9, ~1,970), `1etna-5se79` (`neb_hop_*/NN`, ≤ 1,522), `16jd7-0zz18` (ICHAIN=0, 1,451).
  Whether their `REF_forces` are DFT forces depends on the implementation: under **VTST** the
  `TOTAL-FORCE` block *is* the NEB force (projection + spring; G. Henkelman on the VTST forum), under
  VASP's **built-in** NEB it is the DFT force — verified on a VASP 4.6 image, where
  `CHAIN + TOTAL = TOTAL-FORCE + CHAIN-FORCE` to 5×10⁻⁶ eV/Å with zero net drift. This refines the
  Zenodo-era rule "NEB forces are wrong labels" (true for VTST only). **Treat NEB images as suspect**
  (numbered image dirs under an NEB path, `IMAGES`/`ICHAIN` in the INCAR) unless shown built-in, and
  run the same check on Zenodo/NOMAD. The metadata does not record the implementation; the OUTCAR
  does (VTST prints `NEB: projections on to tangent (spring, REAL)`, built-in prints `CHAIN + TOTAL`).
* **VASP MLFF**: exactly one on-the-fly training run reached the dataset (`zybrs-6s419`
  `SOAP/1-train/vasprun.xml`, `ML_LMLFF=T`, `ML_MODE=train`, 95 frames). All 95 frames carry a
  converged SCF trace, i.e. they are the DFT steps. Rule: in a calc with `ML_LMLFF=True`, keep only
  frames that carry `scf_dE`. The MLFF *prediction* runs never got in. Worth the same
  `incar.ML_LMLFF` scan on Zenodo/NOMAD.
* **Functional labels** are pymatgen's `run_type` echo of the GGA tag (faithful, not bugs):
  `revPBE+Padé` = VASP `GGA=RP` (RPBE; 69% of frames), `.P` = the INCAR typo `GGA = .pe.`
  (`bzch9-1xh79`, 1,468 frames), `91` = PW91, `ML+rVV10` = `GGA=ML` (vdW-DF2 exchange — not machine
  learning), `BF` = BEEF, `CA`/`PZ` = LDA. An OUTCAR-only calc from a VASP version that prints neither
  the INCAR nor `IVDW` cannot show a vdW correction: the Kristoffersen AIMD (65.6% of frames) reads as
  plain RPBE — check the paper before mixing it with D3 data.
* **Frames ≫ diversity**: sub-sample the 440 correlated Kristoffersen trajectories; CC BY-SA (share
  alike) applies to anything redistributed from that record.

## Code fixed from this run

`zenodo_harvest/parse.py` (shared; affects future runs only): `_frame` rejects a force array whose
shape is not `(n_atoms, 3)` — it is assigned straight into `atoms.arrays`, bypassing ASE's check, and
a 1-row array would even be *silently broadcast* onto every atom by the extxyz writer. Anything that
still escapes a calc's parse or frame write is now a non-terminal `parse_error` rejection against
that calc in both loops; a storage `OSError` (disk full) stops the parse instead of rejecting every
remaining calc. Tests: `tests/test_parse_unexpected_errors.py`.

## Remaining optional steps

1. **Calc-level overlap check** of the two Zenodo-flagged records (login node, minutes; see
   `MATERIALS_CLOUD_HARVEST.md` §6):
   `python scripts/csd3/materials_cloud/csd3_mc_overlap.py --mc-root $MC_HARVEST_DATA --zenodo-dataset /rds/user/$USER/hpc-work/zenodo/dataset`
   → `mc_overlap.json`; exact duplicates are deduped at training time.
2. **Cleanup**: `rm -rf $MC_HARVEST_DATA/raw` (3.9 GiB / 13.8k inodes of rejected calcs' files — a
   future VASP-4 re-parse re-fetches anyway). Keep `manifests/` (provenance of every decision) and
   `dataset/`.

## Dataset statistics (`dataset_stats/`) — evaluation of the harvested corpora

Read-only measurement of the three harvested datasets (Zenodo, NOMAD, Materials Cloud; same
`extxyz.gz` + `metadata.jsonl` schema) and, with the SAME code, of the reference MLIP datasets
(MPtrj, OMat24 val, sAlex val, MP and Alexandria materials), for the dataset-evaluation report.
CSD3 runbook: `scripts/csd3/stats/`. CLI: `python -m dataset_stats.cli {meta,scan,ref-fetch,
ref-scan,report}`; outputs under `$DATASET_STATS_DATA` (default a `stats` sibling of the Zenodo
root, i.e. `/rds/user/$USER/hpc-work/stats`).

Three passes, each over numpy rows joined by `calc_key` (blake2b-64 of the calc_id):

- `meta.py` — **metadata pass**: `metadata.jsonl` streamed in byte-range chunks (N processes, no
  record materialised) -> `CALC_DTYPE` rows (categoricals coded per chunk, remapped by `load_meta`)
  + per-deposit provenance (deposit = Zenodo/MC concept record, NOMAD upload) + counters (POTCAR
  symbols, U values, INCAR tags). Minutes even for NOMAD; simply re-run.
- `params.py` — pure normalisation of `calc_parameters`: XC family re-derived from the effective
  parameters (pymatgen's `run_type` echo is kept but `revPBE+Padé`=RPBE, `GGA`=no tag, `HF`=AEXX
  default quirk), +U map, dispersion method (IVDW from the user INCAR only), calc type (static /
  relax / relax-cell / md / phonon / neb / saddle / mlff / nscf = ICHARG>=10 or line-mode k-points),
  explicit k-grid, POTCAR releases containing every titel (pymatgen's summary-stats table), and
  `mp_compatible` (MPRelaxSet POTCAR symbols in the PBE release, MP U values for O/F compounds,
  PBE without vdW/meta/hybrid) / `omat24_compatible` (same recipe, PBE_54, W_sv, Yb_3).
- `scan.py` — **shard scan**: every shard decompressed once, frames split by text, comment lines
  parsed into raw key/values (`extxyz_fast.py`), numeric columns of consecutive frames with one
  `Properties` layout read by ONE `np.loadtxt`. Per frame a `FRAME_DTYPE` row (energy, E_free−E0,
  max/mean/rms |F|, |ΣF| drift, pressure and max |σ| in GPa with ASE sign, volume, net moment/charge,
  SCF tag, flags, structure hash = species+cell+positions at 1e-4 Å, frame hash = + energy at
  1e-6 eV); per calc the `structure.describe` of its first and last frame in the shard (reduced
  formula, chemsys, vacuum gaps -> bulk/slab/wire/molecule at 6 Å, shortest distance, spglib space
  group ≤ 400 atoms at symprec 0.1, density, reciprocal lengths); per shard a per-atom |F|
  histogram. One atomic `.npz` per shard, skipped on a re-run (resumable). `KeyMap` maps another
  extxyz's keys onto the same rows (`MPTRJ_KEYS`). `--individual-only` (the CSD3 default) scans
  only calcs with `origin == 0`: `meta.individual_include` maps shard index -> wanted calc keys from
  each calc's `shard_lo`/`shard_hi`, unwanted shards are not opened and unwanted calcs are dropped
  after their comment line; `filter.json` records the filter and a scan dir never mixes two.
  The report applies the same filter to the metadata rows (implied by the scan's `filter.json`).
- `reference.py` — download (Range resume, md5 where published) and scan of the references:
  Matbench Discovery's MPtrj extxyz zip (one member per material), OMat24/sAlex `.aselmdb`
  (needs `ase-db-backends`; trajectory = `sid` minus its step, group = Alexandria `parent_id`), MP /
  Alexandria `ComputedStructureEntry` JSON (one structure per material, no forces).
- `report.py` + `render.py` — joins and statistics (vectorised over codes): size, concentration
  (top-k, Gini, 1/HHI over deposits), chemistry (elements by frames/calcs/atoms/deposits, n-ary,
  formulas, chemical systems, element pairs, structure-vs-POTCAR element check), structure (atoms,
  dimensionality at 4.5/6/8 Å, vacuum width, volume/atom, shortest distance, crystal systems, non-P1
  bulk prototypes), labels (E/atom by XC family, max |F| shares, per-atom |F| quantiles, |ΣF| by
  calc type, pressure, |F−E0|, SCF, non-finite), settings, quality, net moment/charge,
  availability, provenance, MP/OMat24 compatibility, consistency buckets (XC label × POTCAR
  release family), redundancy (unique structures, exact duplicate frames/calcs and whether they
  span deposits, shared initial structures with several XC labels = multi-fidelity groups,
  effective frames after the sAlex ΔE>10 meV/atom rule), default curation filters, and the
  **novelty** section: share of calcs/frames/effective frames/deposits whose element set, chemical
  system, formula or prototype lies outside each reference (and MP ∪ Alexandria).

Conventions: the scan never builds an `ase.Atoms` per frame (the `verify` lesson: 10M+ frames);
nothing reads all metadata into Python objects; every output is written atomically. Tests:
`tests/test_dataset_stats.py` (synthetic shards from the harvest's own writer + reference key maps).

# Further work — handoff for the next phase (written 2026-09-25)

A self-contained brief for a new agent session. All three VASP harvests are done. What remains is
finding more open VASP data that keyword search cannot see, combining the three datasets into one
curated corpus, and showing that corpus is useful for MLIP training.

**Order chosen by the user: A → B → C** (A in its own session). Scope stays **VASP only**, and **no
new databases** are to be harvested. The institutional/curated sets (Materials Project, OMat24,
Alexandria, OQMD, MPtrj, MatPES, …) are "buy, not build" and are used only as *comparison* baselines
in C, never ingested.

---

## 0. Read first

| What | Where |
|---|---|
| Architecture, stage-by-stage code map, domain conventions (mentor-agreed) | `CLAUDE.md` (root), then `nomad_harvest/CLAUDE.md`, `materials_cloud_harvest/CLAUDE.md` |
| Zenodo result + its limitations / further-work list | `docs/HARVEST_RESULT.md` ("First harvest stage — COMPLETE", "Limitations", "Potential further work") |
| NOMAD result | `docs/NOMAD_HARVEST_RESULT.md` |
| Materials Cloud result + training-time data-quality notes | `docs/MATERIALS_CLOUD_HARVEST_RESULT.md` |
| Storage / schema design | `docs/DESIGN.md`; evaluation history `docs/EVALUATION.md` |
| Why other DBs are not harvested | `docs/EXTERNAL_DATA_SOURCES.md` |

### The three datasets (CSD3, `/rds/user/$USER/hpc-work/…`, 1 TB / 1M-inode Lustre quota)

| Source | Dir | Records | Calcs | Frames | Size | Licence |
|---|---|---|---|---|---|---|
| Zenodo | `zenodo/dataset` | ~303 | 182,111 | 12,088,722 | ~40 GB | CC0/BY/BY-SA (+2 BY-NC recs, 1 no-licence by permission) |
| NOMAD | `nomad/dataset` | direct uploads | 7,073,592 | 52,459,065 | ~59 GiB, 5,328 shards | CC BY 4.0 |
| Materials Cloud | `materials_cloud/dataset` | 102 | 75,751 | 2,545,669 | 7.3 GiB, 268 shards | BY-SA 65.6% of frames / BY / MIT / BY-NC |

All three have the same schema: `shard-NNNNN.extxyz.gz` (MACE keys `REF_energy` = σ→0 E0,
`REF_forces`, `REF_stress` in ASE Voigt eV/Å³, plus `E_free`, per-frame `electronic_converged`/`scf_dE`,
`total_magnetization`/`total_charge`) and `metadata.jsonl` (one record per calc: `calc_id`, `frame_ids`,
`shards`, `provenance`, `calc_parameters` incl. `potcar_set_hash`/`run_type`/`functional`/`incar`/
`parameters`, `quality`, `availability`, `electronic`). calc_ids are namespaced
`zenodo:` / `nomad:` / `materials_cloud:`. `verify` passes on each dataset separately; **they have not
been merged yet**.

### Working conventions (user preferences — follow them)

* **The user runs everything on CSD3** (batch templates in `scripts/csd3/…`). The local WSL link is
  slow and flaky (~15–35 KB/s), so keep local live tests to tiny probes and put speed/volume tests in
  CSD3 scripts ("do not over-prepare"). Env: `module load python/3.11.0-icl && source
  ~/materials-mlip/.venv/bin/activate`; jobs set `TMPDIR=/local`. Login nodes have a watchdog that
  kills long processes, so anything longer than a few minutes goes in a batch job.
* **Wallclock**: 12 h (SL3) / 36 h (SL1/2). Long work must be resumable and able to resubmit itself.
* **The user commits manually**: finish with commit groups + one-line messages. Clear test files and
  caches at the end. Never commit tokens (`.env` holds `ZENODO_TOKEN`); never send the Zenodo token to
  any non-zenodo.org host.
* **Ask the user on genuinely ambiguous decisions**, with options explained. Use at most 1–2 subagents
  (an earlier overspawn hit the spend limit); do simple web/API lookups directly.
* Toolchain: `python -m pytest tests/ -q` (545 offline tests), `python -m mypy zenodo_harvest/`,
  `ruff check zenodo_harvest/`. Shared-code changes must keep existing datasets reproducible (no
  behaviour change for data that already parses).

### Small leftover: the Materials Cloud ↔ Zenodo overlap check

`scripts/csd3/materials_cloud/csd3_mc_overlap.py` was killed by the login-node watchdog. Either fold it
into the physical dedup in **B2**, or run it on a compute node:

```bash
# batch (no interactivity needed; the script streams metadata and reads only the flagged calcs' shards)
sbatch -A <MYGROUP>-SL3-CPU -p icelake -c 4 -t 01:00:00 -o logs_mc/mc-overlap-%j.out --wrap \
  "module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate && \
   python scripts/csd3/materials_cloud/csd3_mc_overlap.py \
     --mc-root /rds/user/$USER/hpc-work/materials_cloud \
     --zenodo-dataset /rds/user/$USER/hpc-work/zenodo/dataset"
# or interactively: srun -A <MYGROUP>-SL3-CPU -p icelake -c 4 -t 01:00:00 --pty bash
#   (CSD3 also provides the `sintr` wrapper for interactive jobs; check its docs for current QoS limits)
```

Output: `$MC_HARVEST_DATA/manifests/mc_overlap.json` (for each flagged pair: exact / near / unique
calcs). The flagged pairs are `hmdsb-k3h43` ↔ Zenodo `4683140` (same Kavanagh project, different
packaging) and `ervm4-pn188` ↔ `7023990` (the Zenodo side is only a code snapshot).

---

## A. Recover the VASP data Zenodo's keyword search cannot see (do first)

### Problem (measured)

Zenodo's `q` indexes metadata **text** + top-level filenames, never archive contents, and nearly all
VASP data sits in archives. A record is found only if its title, description or keywords say
DFT/VASP. Proven case: Sean Kavanagh (mentor, ORCID 0000-0003-4577-9647) has 30 records, and 15 of
them are missing from the dataset. Four of those are real VASP datasets: `13888307` (19.6 GB,
`keywords: null`, description "Accompanying data … article link"), `10630244` (10.5 GB, no licence),
`4541602` (4.8 GB) and `12518256`. The gap is systematic, with an unknown size across Zenodo.

**The user rejected a by-ID harvest of Kavanagh's records** ("cherry-picking = meaningless"). The
method must be **systematic**; Kavanagh's records serve only as a *recall check* (does the method find
them without being told?).

### What was already measured (2026-09-23, `zenodo-blind-spot-scaling` analysis)

* Zenodo: 7.33M records; **705,752 datasets** (172k with a `.zip`), 295k software (mostly GitHub
  snapshots).
* **A token lifts the search page size from 25 to 100** (anonymous `size=26` returns HTTP 400; with a
  token `size=100` returns 200). `zenodo_harvest/client.py` still pages at 25 — change that first.
  The `/api/records` search stays at **30 req/min even with a token**, so enumerating every dataset is
  ~7.1k requests ≈ 4 h (software +1.6 h): one CSD3 job.
* **Peeking every zip does not scale**: ~172k zip datasets ≈ 2–4 days at the ~5k req/h file-endpoint
  cap, >99.9% of it confirming "not VASP". Precision filters are needed **before** any per-file
  request.
* Filename search is useless (≈8 bare `vasprun.xml`/`OUTCAR` Zenodo-wide).

### Recommended design (~300–500 new lines feeding the existing stages)

1. **Enumerate** all datasets (+ software, optionally) cheaply with the token at page size 100,
   reusing `iter_records`' `created`-date bisection (10k-window cap). Store slim candidates.
2. **Precision filters, ranked**:
   1. **Depositor / ORCID snowball.** Seeds are the creators (names + ORCIDs) of every record already
      yielding VASP in Zenodo, Materials Cloud and NOMAD. Their other records are high-prior
      candidates. This generalises the Kavanagh census (it found 15/30 missing) at ~a few thousand
      searches ≈ 1–2 h. Iterate one hop if the yield justifies it.
   2. **Paper-graph filter** via **OpenAlex** (free; batch lookups of 50 DOIs/request; set a `mailto`).
      Take the DOIs from each record's `related_identifiers`/description, keep the records whose
      paper **cites the VASP method papers** (Kresse & Furthmüller 1996 PRB/CMS, Kresse & Joubert
      1999, Blöchl 1994 PAW). This would have caught `13888307`. The reverse direction is also
      possible: VASP-citing papers → their data-availability DOIs (Crossref/OpenAlex/DataCite links).
   3. **Negative filters**: resource types and formats that are never VASP (e.g. `.glb` 3D models,
      neuro/genomics/climate communities) to shrink the net.
3. **Then the existing machinery**: survivors → `triage` (zip central-directory peek over Range is ON
   by default, cheap) → keep-list → `pipeline` straight into the **production** Zenodo dataset (the
   user prefers direct-into-production, as for the NC expansion). `scripts/csd3/20_pipeline.sh` takes
   `IN=… RAW_DIR=…` (defaults unchanged); diff against the existing dataset by `conceptrecid`
   (newest version wins) so nothing is re-fetched.
4. **Report** recall on the Kavanagh 15 (how many the method found unaided), the funnel (enumerated →
   filtered → peeked → kept → parsed), and yield per filter. That tells whether the gap is small
   (close it) or large (a result in itself).

### Gotchas

* The licence gate is intentional: keep CC0/BY/BY-SA + permissive. NC was tested (378 candidates →
  2 records yielded), so it isn't worth widening. No-licence records are only added by explicit
  permission (e.g. mentor's `10579527`).
* Keep discovery single-stream (30 req/min is server-side); only fetch benefits from `--workers`.
* Many zips contain nested archives, and tars are unpeekable: triage keeps them fail-safe (`--peek`
  drops only proven-empty zips). Budget transfer accordingly.
* Materials Cloud and NOMAD need no rerun (MC was a full census; NOMAD is indexed by code). Other
  repositories (Mendeley, ScienceDB, figshare…) are **out of scope** unless the user reopens that.

---

## B. Build one combined, curated corpus

### B1. Make `merge-datasets` scale (a bug that must be fixed first)

`zenodo_harvest/dataset_ops.py` `merge_datasets` still **materialises all metadata**
(`_load_metadata` at ~L536 for the destination records and ~L630 for the post-merge integrity
check). The same pattern cost 79 GiB and OOM-killed the NOMAD job in `verify`/`purge-raw` until they
were switched to streaming (commit `7dfdc14`; `verify_dataset` now streams two passes). Stream it the
same way (only the calc_id / frame_id sets need to be in memory).

Also decide whether to **physically merge or index**. The three dirs total ~110 GB and ~5.6k shards,
and a physical merge renames/moves shards (it never recompresses, so it's fine inside the quota
while keeping the originals if space allows). An alternative that avoids moving data is a
**virtual corpus**: one index (JSONL/SQLite/Parquet) of `calc_id → source dir, shards, frame_ids,
curation flags, bucket`. Training readers then stream from the three dirs. Ask the user; the virtual
index is cheaper and keeps each source's `verify` intact.

### B2. Physical (content) deduplication

Record-level dedup already happened (NOMAD vs Zenodo `zenodo.*` uploads; MC overlap flagged, not
dropped). The same VASP run can still appear twice across sources, and the same system can be re-run
with different settings. Generalise the fingerprint in `csd3_mc_overlap.py`:

* **exact duplicate**: (reduced formula, n_atoms, n_frames, first + final `REF_energy` to 1e-6 eV,
  `potcar_set_hash`) → the same run published twice → keep one (prefer the richer provenance /
  vasprun parser);
* **near duplicate**: (formula, n_atoms, final E/atom to 1e-4 eV) → the same system re-run → keep
  both but tag it.

Build it streaming (fingerprint per calc from its shards; 64M frames, so a batch job, resumable per
shard). This subsumes the leftover MC overlap check.

### B3. Curation flags ("safe to train on"), per calc and per frame

Store them as an added field (like `enrich-metadata` does: never touch `calc_id`/`frame_ids`/`shards`,
so `verify` still passes), or keep them in the B1 index. Known issues to encode:

1. **NEB images.** VASP ≥ 5 image OUTCARs parse normally, so NEB images are in the datasets (MC alone:
   ~220 calcs / ≤ 9.9k frames). Under **VTST**, the `TOTAL-FORCE` block *is* the NEB force
   (projection + spring; G. Henkelman on the VTST forum), so the labels are wrong. Under VASP's
   **built-in** NEB it is the DFT force (verified on a VASP 4.6 image: `CHAIN + TOTAL = TOTAL-FORCE +
   CHAIN-FORCE`). Detect NEB images by `incar.IMAGES`/`ICHAIN` or numbered image dirs under an NEB
   path. The metadata can't tell VTST from built-in, but the OUTCAR can (VTST prints `NEB:
   projections on to tangent (spring, REAL)`, built-in prints `CHAIN + TOTAL`). Default: exclude
   unless shown built-in. This refines the Zenodo-era rule "NEB forces are wrong labels" (true for
   VTST only).
2. **VASP MLFF runs** (`incar.ML_LMLFF`): in an on-the-fly training run (`ML_MODE=train`) keep only
   frames that carry `scf_dE` (the DFT steps). Prediction runs never parsed. Scan Zenodo/NOMAD for
   these.
3. **Electronic convergence**: per-frame `electronic_converged == False` and large `scf_dE`.
4. **Label/force consistency**: `quality.max_abs_free_minus_e0_per_atom` (large |F−E0| means E0 is a
   poor label for the stored forces, which are consistent with F).
5. **Functional labels** (a faithful pymatgen `run_type` echo, so map them): `revPBE+Padé` = GGA=RP =
   RPBE; `.P` = INCAR typo `GGA=.pe.`; `91` = PW91; `ML+rVV10` = GGA=ML (vdW-DF2, not machine
   learning); `BF` = BEEF; `CA`/`PZ` = LDA.
6. **Invisible vdW**: OUTCAR-only calcs without an INCAR echo or an `IVDW` line cannot show a D3
   correction (e.g. MC's Kristoffersen AIMD, 65.6% of MC frames, reads as plain RPBE).
7. **Non-DFT-label runs** already excluded at parse (DMFT, `ALGO=None`, GW/RPA no-energy steps). Keep
   them excluded.

### B4. Consistency buckets

Absolute VASP energies are comparable only within **identical `potcar_set_hash` + functional (+U
values, vdW) + key settings** (ENCUT, PREC, …; `calc_parameters.parameters` / `resolved`). Define
buckets, report their sizes, and never mix absolute energies across buckets (forces transfer better
than energies). Per-source energy references (e.g. MC is 91% OUTCAR-parsed) stay separate.

---

## C. Show the long tail is valuable (the scientific "so what")

1. **Uniqueness vs the megasets.** Compare the curated corpus against MPtrj / OMat24 / Alexandria /
   MatPES (comparison only). Measure coverage of compositions/elements, structure hashes (e.g. a
   pymatgen `StructureMatcher` / composition + space-group key), distance from equilibrium
   (force/stress distributions), surfaces/interfaces/defects/AIMD regimes, and functionals. Output:
   which fraction is genuinely new, and where.
2. **Subsampling.** Frame count ≫ diversity: AIMD/relaxation trajectories are highly correlated (one MC
   record = 65.6% of MC frames; one Zenodo record = ~47% of early Zenodo frames). Subsample per
   trajectory (stride / farthest-point on descriptors) before training.
3. **MLIP ablation (MACE)**: a baseline trained on a megaset versus baseline + long-tail (one
   consistent bucket, e.g. PBE / PBE+U), evaluated on held-out long-tail systems and standard
   benchmarks. There is **no training code in the repo yet**. It needs a CSD3 GPU allocation
   (`ampere` A100 partition, an `-SL3-GPU` account — confirm with the user) and MACE's `REF_*` keys
   (already the dataset's defaults).
4. **Release.** A dataset card: sources, licences (CC BY-SA share-alike for the Kristoffersen record;
   the NC records are few), provenance per calc (source/record/DOI/citations, already stored),
   curation flags, buckets, and known limitations (from the three result docs).

---

## Deferred (judged not significant, 2026-09-25)

Parser recoveries worth ~0.2% of frames each. Do them only if a parser pass is already happening:
* **VASP 4.x OUTCARs** (MC 236 calcs): VASP 4.6 prints `FREE ENERGIE` before `POSITION`, which breaks
  ASE's chunking. Validated fix: split at `Iteration N(   1)` and feed each step to ASE's
  `OutcarChunkParser`, gated on the `vasp.4` banner. **Never** just skip ASE's bad first chunk: that
  pairs step k−1's forces with step k's energy.
* ASE header quirks (glued `NBANDS=`, > 10 species types, fused lattice numbers): MC 477, NOMAD ~350.
* Content-sniffing for misnamed archives (zstd named `.tar.xz`, zip named `.tgz`, double gzip) and a
  streaming tar salvage of truncated archives: all MC cases were non-VASP.
* Multi-code parsing (QE/CP2K/CASTEP) is out of scope (VASP focus).

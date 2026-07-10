# Data harvest & storage design

This note answers the design questions for the Zenodo → VASP → MLIP-dataset
pipeline, grounded in the Zenodo REST API behaviour observed on 2026-07-09.

## TL;DR recommendations

| Question | Recommendation |
|---|---|
| Storage format for atomistic data | **extxyz.gz**, one frame per ionic step, sharded (≈10k frames/shard) |
| Where metadata / calc params live | **separate columnar metadata store** (JSONL while harvesting → Parquet), keyed by a stable `frame_id`; *not* stuffed into every extxyz header |
| How to find only materials-DFT data | **two-stage funnel**: permissive metadata search (recall) → parse-time validation (precision) |
| Filter by `.extxyz` file extension? | **No** — misses ~all raw VASP (it's inside archives) and inherits the uploader's processing. Use it only as a bonus "already-processed" source. |
| Maximise size / exhaust Zenodo | broad keyword set + **recursive date-partitioning** past the 10k window + relevant **communities**; OAI-PMH for a true full sweep |
| Quality assurance | reject at parse time, not search time; **log every rejection with a reason** so recall is auditable; tag electronic convergence + its magnitude |

---

## 1. Zenodo API facts that drive the design

Verified against `https://zenodo.org/api/records`:

- **`q` searches metadata text only** (title/description/keywords), *not* file
  contents. `q=vasprun` → **0** hits; `q=VASP` → 314; `q=OUTCAR` → 37. You cannot
  find a `vasprun.xml` by searching for it.
- **The DFT outputs live inside archives.** The `file_type` aggregation for
  `q=VASP` is dominated by `zip`, `gz`, `tar`, `tgz`, `rar` — only *2* records
  expose a raw `.xml`. So **file-extension filtering finds almost nothing.**
- **The search response already lists each record's files** (`files[].key`,
  `size`, `checksum`, download link). Triage on filenames is therefore free.
- **Rate limit**: ~30 req/min anonymous (`Retry-After` on 429). A personal
  access token (`ZENODO_TOKEN`) raises it — needed for a full harvest.
- **Page size caps at 25**; `size>=50` → HTTP 400.
- **Search window caps at 10,000** (`page*size <= 10000`). Bigger result sets
  must be partitioned (we bisect the `created` date range).
- **Range requests work** on file downloads (206) even though Zenodo omits the
  `Accept-Ranges` header — this is what lets us peek inside a remote `.zip`.

## 2. The extraction funnel (precision vs recall)

Precision ("everything collected is what I want") and recall ("I collect as much
of what I want as possible") pull in opposite directions. Resolve them by
splitting the work: **be permissive early, strict late.**

```
                         cheap, no download        expensive, but authoritative
 keyword search  ─────►  file-listing triage  ─►  download  ─►  pymatgen parse  ─►  accept/reject
 (high recall)           (+ optional zip-peek)                  (high precision)     (+ reason log)
   ~10^4–10^5 recs         ~10^3–10^4 recs          GBs           final dataset
```

- **Stage 0 discover** (`discover.py`): cast a wide net with many metadata
  keywords. False positives are fine here — they're cheap to carry.
- **Stage 1 triage** (`triage.py`): classify each record from its file listing
  into `vasp_direct` / `archive` / `processed_atomistic` / `unlikely`. For
  `archive` records where the payload is hidden, **peek the zip central
  directory over HTTP Range** to confirm `vasprun.xml`/`OUTCAR` *without
  downloading gigabytes* (works for `.zip`; `.tar.gz`/`.rar` need download).
- **Stage 3 parse**: the real precision gate. If pymatgen parses it as a valid
  `Vasprun` with the required properties, keep it; otherwise reject **and log the
  reason**. This is also where consistency (functional, etc.) is recorded.

**Why not just filter by `.extxyz(.gz)`?** Three reasons:
1. **Recall**: raw VASP data is almost never uploaded as extxyz — it's in
   archives of `vasprun.xml`/`OUTCAR`. Filtering by extension would discard the
   bulk of the harvestable data.
2. **Precision/provenance**: an uploaded extxyz has already been through someone
   else's (unknown) processing — you lose control over which ionic steps were
   kept, whether unconverged frames were dropped, unit conventions, and which
   property is in which column. Parsing the VASP source yourself gives uniform,
   trustworthy provenance and convergence tags.
3. **extxyz ≠ VASP**: extxyz files on Zenodo come from many codes (QE, CP2K,
   GPAW, LAMMPS-ML…) with inconsistent property keys — not what we want while
   scoped to VASP.
   → Treat existing `.extxyz`/`.xyz` as a *bonus* `processed_atomistic` bucket to
   revisit later, not the primary filter.

## 3. Storage format & structure

### 3a. Atomistic data → sharded `extxyz.gz`

extxyz is the right default (mentor's call, and it is the de-facto MLIP training
format; round-trips through ASE and pymatgen). **One frame = one ionic step.** A
relaxation with 40 ionic steps becomes 40 frames.

Per-frame layout (ASE `Atoms`, written with `ase.io.write(..., format="extxyz")`):

| Quantity | extxyz location | key |
|---|---|---|
| positions | per-atom | (columns) |
| chemical symbols | per-atom | `species` |
| cell + PBC | frame header | `Lattice`, `pbc` |
| forces | per-atom array | `forces` |
| charges (Bader/Mulliken/…) | per-atom array | `initial_charges` / `charges` |
| spins / magmoms | per-atom array | `initial_magmoms` / `magmoms` |
| total energy | frame info | `energy` (MACE reads `REF_energy`) |
| stress (if present) | frame info | `stress` |
| convergence flags | frame info | `electronic_converged`, `dE_last_scf` |
| **link to metadata** | frame info | `frame_id`, `source_recid` |

Keep the extxyz header **small**: only physical data + a `frame_id` foreign key.
Do **not** repeat the full calculation parameters / provenance in every frame —
that is bulky, and identical across all frames of a run.

**Sharding**: write ~10⁴ frames per `*.extxyz.gz` shard (e.g.
`data/dataset/shard-00042.extxyz.gz`). This keeps files a manageable size for
parallel writes on the cluster, lets jobs append independently, and lets training
stream shards. gzip typically gives ~3–5× on extxyz.

*Availability-only* signals (mentor: record but don't store) — charge density,
spin density, electronic eigenvalues, magnetisation, DOS — become **boolean
columns in the metadata store**, plus the source file name, so the heavy data can
be re-fetched later if wanted.

### 3b. Metadata & provenance → columnar store (JSONL → Parquet)

Everything non-atomistic lives in a **separate table keyed by `frame_id`** (and a
coarser `calc_id` for things constant within a run). During harvest, append
JSONL (crash-safe, resumable, parallel-friendly); periodically compact to
**Parquet** for fast column queries and small size. This "big pile of extxyz
shards + a metadata sidecar table + a manifest" pattern is the standard way large
MLIP datasets (OC20/OC22, Materials Project trajectories, Alexandria, MPtrj) are
organised — the atomistic bytes stream from flat files, the queryable metadata
sits in a database/columnar table.

Proposed metadata record (one per calculation; frames reference it):

```jsonc
{
  "calc_id": "zenodo:17378016:LaSn5/relax/vasprun.xml",   // stable, unique
  "provenance": {
    "source": "zenodo",
    "record_id": "17378016",
    "concept_doi": "10.5281/zenodo....",
    "version_doi": "10.5281/zenodo.17378016",
    "url": "https://zenodo.org/records/17378016",
    "license": "cc-by-4.0",                 // matters if the dataset is redistributed
    "citation": ["<from record metadata / .bib>"],
    "file_path_in_archive": "LaSn5/relax/vasprun.xml",
    "harvested_at": "2026-07-09T..."
  },
  "calc_parameters": {                       // store ALL of it — as important as the physics
    "code": "vasp", "code_version": "6.4.2",
    "functional": "PBE", "run_type": "GGA+U",   // pymatgen Vasprun.run_type
    "hubbard_u": {"Fe": 4.0}, "vdw": null,
    "encut": 520, "ediff": 1e-6, "ismear": -5, "sigma": 0.05,
    "kpoints": {"scheme": "Monkhorst", "grid": [8,8,8]},
    "potcars": [{"symbol": "Fe_pv", "hash": "..."}],
    "incar": { ... full INCAR ... },
    "spin_polarized": true
  },
  "quality": {
    "electronic_converged": true,            // pymatgen Vasprun.converged_electronic
    "dE_last_scf": 3.1e-7,                    // |E[-1] - E[-2]| of final ionic step's SCF
    "ionic_converged": true,
    "n_ionic_steps": 40, "n_atoms": 12
  },
  "availability": {                          // recorded, not stored (too big)
    "charge_density": true, "chgcar_file": "CHGCAR",
    "spin_density": true, "eigenvalues": true,
    "dos": false, "magnetisation": true
  },
  "frame_ids": ["...#0", "...#1", ...]
}
```

**Convergence** (mentor's emphasis): store both the boolean and the *magnitude* —
the energy difference between the last and second-last electronic step of the
final ionic step (`vasprun.ionic_steps[-1]["electronic_steps"]`). Unconverged
frames are kept but **tagged** (`electronic_converged=false`), never silently
mixed in.

### 3c. The manifest ties it together

`data/manifests/*.jsonl` records what was discovered/triaged/downloaded/parsed,
so any stage is resumable and the whole harvest is auditable. `calc_id` /
`frame_id` are the join keys between extxyz shards and the metadata table.

## 4. Maximising coverage (exhausting Zenodo)

1. **Broad keyword set** (OR-combined): VASP, DFT, first-principles, ab-initio,
   vasprun, OUTCAR, plus method/tooling terms (MACE, NequIP, GAP, "interatomic
   potential", "training data") — see `discover.DEFAULT_QUERIES`.
2. **Recursive date-partitioning** (`client.iter_records`): any query > 10k hits
   is bisected on `created` date until each window is ≤ 10k, then fully paged.
   This is the only way past the 10k window and is the cluster full-harvest path.
3. **Communities**: harvest relevant Zenodo communities directly
   (e.g. Materials Cloud mirrors, MLIP-dataset communities) via the
   `communities` filter — often higher precision than free-text.
4. **De-duplicate by `conceptrecid`** (keep newest version) so re-uploads don't
   inflate counts.
5. **OAI-PMH** (`/oai2d`) for a genuine exhaustive sweep of all datasets, if the
   keyword approach proves to miss things — heavier, do on the cluster.
6. **Beyond Zenodo**: the same funnel applies to Materials Cloud, Materials
   Project (`mp-api`), NOMAD — plug in additional `discover` backends later.

## 5. Ensuring quality (both directions)

- **Precision (no junk)**: authoritative check is *"does pymatgen parse it and
  does it carry the required properties?"* Filenames only *route* work; they
  never decide inclusion.
- **Recall (miss little)**: every rejected candidate is written to a
  `rejections.jsonl` with a machine-readable reason (`no_vasp_files`,
  `parse_error`, `no_forces`, `license_excluded`, …). Periodically sample
  rejections by hand to catch systematic misses (e.g. a filename pattern the
  classifier didn't know) and widen the rules.
- **Consistency**: record `functional`/`run_type`/`hubbard_u`/`vdw`/`encut`/
  k-point density so the training set can later be filtered to a coherent subset
  — `run_type` alone is too coarse to guarantee this, so keep the raw INCAR too.
- **Integrity**: verify Zenodo `md5` checksums on download; store them.
- **Dedup at the physics level** (later): identical structures/energies across
  records (common when papers repackage MP data) should be detected via a
  structure+energy hash to avoid double-counting in training.

## 6. Scaling to CSD3

- Stages are independent CLI steps over JSONL manifests → trivially resumable and
  parallelisable (split the manifest, run N array-job tasks, each writing its own
  shards).
- Keep one `requests.Session`, honour `Retry-After`, use a token; harvest is
  I/O-bound so a modest array (or async) saturates the rate limit safely.
- Parsing is CPU-bound and embarrassingly parallel (one task per archive).
- Only the final `extxyz.gz` shards + Parquet metadata are kept long-term; raw
  archives can be staged in scratch and deleted after parsing (all gitignored).

# Running the NOMAD harvest on CSD3

Batch templates for harvesting VASP data from **NOMAD** on Cambridge's CSD3. They mirror
the Zenodo templates one directory up (`scripts/csd3/`) and share their conventions — set the
account once with `export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU`, activate the env before `sbatch`,
`mkdir -p logs_nomad` before the first submit. See `../README.md` for the CSD3 facts (wallclock,
the 1 TB / 1M-inode `/rds` quota, node internet) these are built around.

```
scripts/csd3/nomad/10_discover.sh       # stage 0-1: keyset scan -> license-gated, deduped keep-list (1 core, minutes)
scripts/csd3/nomad/20_pipeline.sh       # stage 2-4 overlapped (fetch || parse+purge) + verify, disk-paced, self-resubmitting
scripts/csd3/nomad/csd3_nomad_speed.py  # measure BULK fetch MB/s from a compute node (run before sizing WORKERS)
```

## Separate NOMAD tree (never mixed with Zenodo)

Everything NOMAD writes — raw staging, manifests, the dataset — lives under **its own sibling
root** `$NOMAD_HARVEST_DATA` (default `/rds/user/$USER/hpc-work/nomad`), completely separate from
the Zenodo tree at `$ZENODO_HARVEST_DATA` (default `.../hpc-work/zenodo`). This matters because
the disk/inode valve walks *its own* raw dir: a shared staging tree would make each harvest's
valve count the other's files. The **only** Zenodo path the NOMAD job touches is a *read-only*
`dataset/metadata.jsonl`, used at discover time for cross-source dedup.

```
/rds/user/$USER/hpc-work/
├── zenodo/   raw, manifests, dataset, raw_availability, ...   (Zenodo harvest + recoveries)
└── nomad/    raw, manifests, dataset                          (this harvest)
logs_nomad/   SLURM .out/.err + per-job summary JSON (repo-relative, gitignored)
```

## What's NOMAD-specific vs shared

NOMAD's indexed metadata collapses the Zenodo discover+triage funnel: you query
`program_name=VASP` + `method_name=DFT` directly (no keyword recall, no zip-peek). Only
**stages 0-2** are source-specific (`nomad_harvest`). **Stages 3-5 are the shared, unmodified
`zenodo_harvest` code** — the same parser, `REF_energy/forces/stress` keys, full
`calc_parameters` (INCAR + resolved `parameters` from vasprun, OUTCAR header for OUTCAR-mainfile
entries), per-calc availability + embedded DOS/eigen probe, disk/inode valve, `verify`, and
`merge-datasets`. Frames are tagged `source="nomad"` and calc_ids namespaced `nomad:<entry_id>:…`,
so the NOMAD dataset is schema-identical to the Zenodo one and merges without id collisions.

## Order of operations

```bash
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo   # Zenodo root (read-only: dedup metadata)
export NOMAD_HARVEST_DATA=/rds/user/$USER/hpc-work/nomad     # NOMAD's OWN root (separate)
mkdir -p logs_nomad

# MAX_ENTRIES scopes an OPTIONAL bounded slice (an early checkpoint); the full 7.1M is Zenodo-
# scale and can run as one self-resubmitting campaign — see docs/NOMAD_HARVEST.md:
MAX_ENTRIES=200000 sbatch --parsable scripts/csd3/nomad/10_discover.sh      # ELEMENTS="Ti O" to focus
# ...then the overlapped, disk-paced pipeline (queue it to wait on discover):
DISC=<jobid-from-above>
sbatch --dependency=afterok:$DISC scripts/csd3/nomad/20_pipeline.sh
# self-resubmitting long run: RESUBMIT=1 sbatch --dependency=afterok:$DISC scripts/csd3/nomad/20_pipeline.sh
```

The NOMAD dataset lands in `$NOMAD_HARVEST_DATA/dataset/`. To combine it with the Zenodo dataset
into one training set afterwards:

```bash
python -m zenodo_harvest.cli merge-datasets --into $ZENODO_HARVEST_DATA/dataset \
    $NOMAD_HARVEST_DATA/dataset
python -m zenodo_harvest.cli verify --dataset-dir $ZENODO_HARVEST_DATA/dataset
```

## Disk / inode budget when co-running with Zenodo jobs

The 1 TB / 1M-inode `/rds` quota is **shared across everything you run**, but each job's valve
only bounds *its own* raw dir — it cannot see the other job's staging or the datasets. So give
each job a **fixed slice** whose limits sum under quota after reserving the fixed/growing bits.
Worked partition (NOMAD + one small Zenodo recovery), quota ~1000 GB / 1.0M inodes:

| consumer | bytes | inodes | how |
|---|---|---|---|
| existing Zenodo raw + dataset | ~100 GB | ~0.1M | already on disk |
| NOMAD dataset + manifests (grows) | ~40–60 GB | ~few-k | not valve-tracked |
| **NOMAD raw valve** | **600 GB** | **600k** | `20_pipeline.sh` defaults |
| Zenodo-recovery raw valve | 150 GB | 150k | `MAX_DISK_BYTES=150000000000 MAX_DISK_FILES=150000 sbatch scripts/csd3/46_availability_recover.sh` |
| headroom | ~100 GB | ~50k | Lustre + in-flight overshoot |

Running NOMAD **solo**? Raise `MAX_DISK_BYTES=800000000000 MAX_DISK_FILES=800000`. The valve
charges every byte as written + every inode as created and refunds on delete, so `staged <= limit`
holds exactly, and the pipeline paces itself (fetch → parse → purge → resume) rather than sizing
the whole scope onto disk at once — **inodes bind first** (~3–4 per entry).

## Monitoring & sizing

* **Progress (NOMAD-aware, read-only, safe while the job writes):**
  ```bash
  python -m nomad_harvest.cli status --max-disk-bytes 600000000000 --max-disk-files 600000
  ```
  It defaults to the NOMAD tree and knows NOMAD's manifest names (`nomad_keep.jsonl`,
  `nomad_fetched.jsonl`, the two rejection logs). Pass the same limits you gave the pipeline so
  STAGING reads as a % of quota; add `--no-staging-walk` to skip the slow Lustre inode walk and
  read `/rds` usage from `lfs quota -u $USER $NOMAD_HARVEST_DATA` instead; `--json` for scripting.
* **Bandwidth (run BEFORE choosing WORKERS):** `srun … python scripts/csd3/nomad/csd3_nomad_speed.py
  --workers 4` measures the real BULK-zip MB/s from a compute node and extrapolates the full-harvest
  transfer time. Sweep `--workers 2/4/8` to find where NOMAD's server-side throughput saturates.
* **SLURM / RAM:** `sacct -j <jobid> --format=JobID,State,Elapsed,MaxRSS,ExitCode`;
  `tail -f logs_nomad/nomad-pipeline-<jobid>.out`.

## Rate limits & etiquette

Fetch is the rate-limiting stage, but after the **bulk redesign it is bandwidth-bound, not
request-bound** (~2 requests per ~300-entry batch, ~47k total for the whole 7.1M). NOMAD's floor is
~30 req/s / ≤10 concurrent per IP with 5xx under load (the client self-throttles + backs off; CSD3
parallelism cannot raise a server-side cap). Use `WORKERS=4-8` (long-lived batch streams multiply
bandwidth without spiking the request rate). **Emailing NOMAD is NOT a documented prerequisite** for
a large harvest — it is only worth doing *reactively*, to request a rate-limit **exemption**
(`support@nomad-lab.eu`, the documented purpose) if the speed script shows sustained throughput far
below ~50 MB/s. Discovery (`10_discover.sh`) is single-stream by design — do not parallelise it.

## Optional: many-core array parse

The pipeline's serial (overlapped) parse is usually enough for NOMAD (small vaspruns parse fast).
To throw many cores at an already-fetched `nomad_fetched.jsonl` instead, the shared array flow
works unchanged — `split` it, run the shared parse per task into per-task dirs under
`$NOMAD_HARVEST_DATA/dataset/`, then `merge-datasets` + `verify`. See the `split → array → merge →
verify` recipe in the top-level `CLAUDE.md`.

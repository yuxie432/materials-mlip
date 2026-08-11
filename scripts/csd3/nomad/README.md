# Running the NOMAD harvest on CSD3

Batch templates for harvesting VASP data from **NOMAD** on Cambridge's CSD3. They mirror
the Zenodo templates one directory up (`scripts/csd3/`) and share all their conventions —
set the account once with `export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU`, activate the env before
`sbatch`, `mkdir -p logs` before the first submit. See `../README.md` for the CSD3 facts
(wallclock, the 1 TB / 1M-inode `/rds` quota, node internet) these are built around.

```
scripts/csd3/nomad/10_discover.sh   # stage 0-1: keyset scan -> license-gated, deduped keep-list (1 core, minutes)
scripts/csd3/nomad/20_pipeline.sh   # stage 2-4 overlapped (fetch || parse+purge) + verify, disk-paced, self-resubmitting
```

## What's NOMAD-specific vs shared

NOMAD's indexed metadata collapses the Zenodo discover+triage funnel: you query
`program_name=VASP` + `method_name=DFT` directly (no keyword recall, no zip-peek). Only
**stages 0-2** are source-specific (`nomad_harvest`). **Stages 3-5 are the shared,
unmodified `zenodo_harvest` code** — the same parser, `REF_energy/forces/stress` keys, disk/
inode valve, `verify`, and `merge-datasets`. Frames are tagged `source="nomad"` and calc_ids
namespaced `nomad:<entry_id>:…` (via the parser's provenance-derived `source`), so the NOMAD
dataset is schema-identical to the Zenodo one and merges without id collisions.

## Order of operations

```bash
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo   # same scratch root as Zenodo
mkdir -p logs

# Scope the bounded sample (see docs/NOMAD_HARVEST.md — the full 7.1M is a multi-week campaign):
MAX_ENTRIES=200000 sbatch --parsable scripts/csd3/nomad/10_discover.sh      # ELEMENTS="Ti O" to focus
# ...then the overlapped, disk-paced pipeline (queue it to wait on discover):
DISC=<jobid-from-above>
sbatch --dependency=afterok:$DISC scripts/csd3/nomad/20_pipeline.sh
# self-resubmitting long run: RESUBMIT=1 sbatch --dependency=afterok:$DISC scripts/csd3/nomad/20_pipeline.sh
```

The NOMAD dataset lands in `$ZENODO_HARVEST_DATA/dataset/nomad/` (kept separate from the
Zenodo `dataset/`). To combine them into one training set afterwards:

```bash
python -m zenodo_harvest.cli merge-datasets --into $ZENODO_HARVEST_DATA/dataset \
    $ZENODO_HARVEST_DATA/dataset/nomad
python -m zenodo_harvest.cli verify --dataset-dir $ZENODO_HARVEST_DATA/dataset
```

## Monitoring & sizing (same tools as Zenodo)

* **Progress:** `python -m zenodo_harvest.cli status --dataset-dir $ZENODO_HARVEST_DATA/dataset/nomad
  --raw-dir $ZENODO_HARVEST_DATA/raw --max-disk-bytes 800000000000 --max-disk-files 800000`
  (read-only; safe while the job writes). Pass the same limits you gave the pipeline so
  STAGING reads as a % of quota.
* **SLURM / RAM:** `sacct -j <jobid> --format=JobID,State,Elapsed,MaxRSS,ExitCode`;
  `tail -f logs/nomad-pipeline-<jobid>.out`.
* **The valve binds on inodes first.** Each entry stages ~3-4 inodes, so ~200k entries fill
  the 1M-inode quota before the byte one — which is why `MAX_DISK_FILES` matters as much as
  `MAX_DISK_BYTES`, and why the harvest is paced (fetch → parse → purge → resume), not sized
  to fit the whole scope on disk at once.

## Rate limits & etiquette

Fetch is the rate-limiting stage (NOMAD ~30 req/s, ≤10 concurrent, 5xx under load — the
client self-throttles + backs off; CSD3 parallelism cannot raise a server-side cap). Use
`WORKERS=4-8`. **Before a multi-million-entry pull, email `support@nomad-lab.eu`** (their
documented etiquette). Discovery (`10_discover.sh`) is single-stream by design — do not
parallelise it.

## Optional: many-core array parse

The pipeline's serial (overlapped) parse is usually enough for NOMAD (small vaspruns parse
fast). To throw many cores at an already-fetched `nomad_fetched.jsonl` instead, the shared
array flow works unchanged — `split` it, run the shared `30_parse_array.sh` equivalent per
task into per-task dirs under `dataset/nomad/`, then `merge-datasets` + `verify`. See the
`split → array → merge → verify` recipe in the top-level `CLAUDE.md`.

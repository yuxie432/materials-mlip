# Running the NOMAD harvest on CSD3

Batch templates for harvesting VASP data from **NOMAD** on Cambridge's CSD3. They mirror
the Zenodo templates one directory up (`scripts/csd3/`) and share their conventions — set the
account once with `export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU`, activate the env before `sbatch`,
`mkdir -p logs_nomad` before the first submit. See `../README.md` for the CSD3 facts (wallclock,
the 1 TB / 1M-inode `/rds` quota, node internet) these are built around.

```
scripts/csd3/nomad/10_discover.sh       # stage 0-1: keyset scan -> license-gated, deduped keep-list (1 core, minutes)
scripts/csd3/nomad/20_pipeline.sh       # stage 2-4 overlapped (fetch || parse+purge) + verify, disk-paced, self-resubmitting
scripts/csd3/nomad/csd3_nomad_prepacked_probe.py  # confirm the pre-packed fetch (MB/s + throttle) from a compute node
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
only bounds *its own* raw dir. NOMAD raw staging is **INODE-bound, not byte-bound**: each entry is
4 inodes (`<entry_id>/extracted/calc/vasprun.xml`) but only ~0.26 MB, so the inode valve caps peak
staging and the byte valve is a loose safety net — **peak bytes ≈ (inode-limit ÷ 4) × 0.26 MB**
(~26 GB at 400k inodes). So partition **inodes** first, then give bytes a generous margin (for the
OUTCAR-mainfile / AIMD tail). Worked partition (NOMAD + a Zenodo recovery), quota ~1000 GB / 1.0M:

| consumer | bytes | inodes | how |
|---|---|---|---|
| existing Zenodo raw + dataset | ~100 GB | ~0.1M | already on disk |
| NOMAD dataset + manifests (grows) | ~40–60 GB | ~few-k | not valve-tracked |
| **NOMAD raw valve** | **200 GB** | **400k** | `20_pipeline.sh` defaults (peak raw ~26 GB) |
| Zenodo-recovery raw valve | 300 GB | 300k | `MAX_DISK_BYTES=300000000000 MAX_DISK_FILES=300000 sbatch scripts/csd3/46_availability_recover.sh` |
| headroom | ~150 GB | ~150k | Lustre + in-flight overshoot |

Inodes sum to ~950k < 1M; bytes well under 1 TB. This frees ~400 GB + ~250k inodes vs the old
600 GB/600k NOMAD slice — the Zenodo recovery gets 300k inodes (was 150k → fewer resume cycles).
Running NOMAD **solo**? Raise to `MAX_DISK_BYTES=400000000000 MAX_DISK_FILES=800000` (and
`PARTS=48+`). The valve charges every byte + inode as created and refunds on delete, so
`staged <= limit` holds exactly, and the pipeline paces itself (fetch → parse → purge → resume).

## Monitoring & sizing

* **Progress (NOMAD-aware, read-only, safe while the job writes):**
  ```bash
  python -m nomad_harvest.cli status --max-disk-bytes 200000000000 --max-disk-files 400000
  ```
  It defaults to the NOMAD tree and knows NOMAD's manifest names (`nomad_keep.jsonl`,
  `nomad_fetched.jsonl`, the two rejection logs). Pass the same limits you gave the pipeline so
  STAGING reads as a % of quota; add `--no-staging-walk` to skip the slow Lustre inode walk and
  read `/rds` usage from `lfs quota -u $USER $NOMAD_HARVEST_DATA` instead; `--json` for scripting.
* **Confirm the fetch path (run before a big campaign):** `srun … python
  scripts/csd3/nomad/csd3_nomad_prepacked_probe.py` measures the pre-packed `GET /uploads/{id}/raw`
  MB/s from a compute node, confirms the 1-conn/5s throttle on CSD3's IP, does an end-to-end
  targeted zip64 extraction, and extrapolates the full-harvest time. There is no `--workers` to
  sweep — the fetch is serial by design (below).
* **SLURM / RAM:** `sacct -j <jobid> --format=JobID,State,Elapsed,MaxRSS,ExitCode`;
  `tail -f logs_nomad/nomad-pipeline-<jobid>.out`.

## Rate limits & etiquette

Fetch pulls each entry's `mainfile` out of its upload's **pre-packed zip** via `GET
/uploads/{id}/raw` + HTTP Range/multi-range (see `docs/NOMAD_HARVEST.md` §3). That endpoint is
rate-limited **per IP to one in-flight connection, a new one only every ~5 s** (explicit 429), so
the fetch is **intrinsically serial** — there is no `--workers` (a 2nd connection just 429s), and
CSD3's shared NAT means extra nodes cannot parallelise it either. Grouping entries by upload (so
each upload's central directory is read once) and packing ~250 members per multi-range request is
what keeps the run to ~30k requests / ~1.5-3 days for the whole 7.1M. The client paces itself and
waits out any 429; discovery (`10_discover.sh`) is single-stream by design too.

* **Availability is free now** — it is read from each upload's central directory (the zip's own
  file list) + NOMAD's `available_properties`, so the old fragile `rawdir/query` step is gone. No
  `--batch-size` / `--no-rawdir` knobs any more.
* **The fast path is anonymous** — no token helps (the limit is per-IP at the proxy). **Emailing
  `support@nomad-lab.eu` is optional**: the one lever that would beat ~1.5-3 days is a rate-limit
  **exemption** raising the per-IP concurrent-connection cap, which would let the targeted fetch run
  several streams in parallel (→ hours). Cheap for NOMAD (static byte-range serving, no assembly);
  worth requesting for the full run, not required.

## Optional: many-core array parse

The pipeline's serial (overlapped) parse is usually enough for NOMAD (small vaspruns parse fast).
To throw many cores at an already-fetched `nomad_fetched.jsonl` instead, the shared array flow
works unchanged — `split` it, run the shared parse per task into per-task dirs under
`$NOMAD_HARVEST_DATA/dataset/`, then `merge-datasets` + `verify`. See the `split → array → merge →
verify` recipe in the top-level `CLAUDE.md`.

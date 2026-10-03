# Dataset statistics on CSD3

Batch templates for `dataset_stats/` (see `dataset_stats/CLAUDE.md`): size, chemistry, structure
types, label distributions, settings, quality, availability, provenance, redundancy and
consistency buckets of the three harvested datasets, measured side by side with MPtrj, OMat24,
sAlex and the MP / Alexandria material sets. Everything is **read-only** on the datasets; outputs
go to `$DATASET_STATS_DATA` (default `/rds/user/$USER/hpc-work/stats`).

```
scripts/csd3/stats/10_stats.sh    # meta + scan of each source (resumable per shard)  5.4 CPU-h, 16 min on 32 cores
scripts/csd3/stats/15_refs.sh     # download + scan the reference datasets              40 min on 16 cores
scripts/csd3/stats/20_report.sh   # combine -> report.json + report.md                   17 min
```

**Individual uploads only (default, `INDIVIDUAL_ONLY=1`).** NOMAD's direct uploads include the
Alexandria group's own high-throughput runs: 6,203,632 calcs / 42.4M frames whose paths carry
Alexandria ids (`agm…`) or that group's naming — institutional data already inside Alexandria /
sAlex / OMat24 (`docs/DATASET_EVALUATION.md` §1). `meta` reads every calc (so their metadata-level
description stays available), but `scan` and `report` keep only calcs of individual origin: 4,030
of NOMAD's 5,345 shards hold nothing else and are not read at all, and the mixed ones skip the
excluded calcs after their comment line. `INDIVIDUAL_ONLY=0` scans everything (~40 CPU-h; use a
fresh `DATASET_STATS_DATA`, since one scan dir never mixes the two, and `sbatch -c 8` for the report).

## Order of operations (from the repo root)

```bash
cd ~/materials-mlip && git pull
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
pip install ase-db-backends           # once: reads OMat24 / sAlex .aselmdb (no torch)
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
mkdir -p logs_stats

# 0. (~2 min, login node) smoke: two shards of Materials Cloud into a throw-away root
DATASET_STATS_DATA=$HOME/ds_smoke python -m dataset_stats.cli meta --source materials_cloud --workers 2
DATASET_STATS_DATA=$HOME/ds_smoke python -m dataset_stats.cli scan --source materials_cloud --limit 2
DATASET_STATS_DATA=$HOME/ds_smoke python -m dataset_stats.cli report --sources materials_cloud \
    && head -40 $HOME/ds_smoke/report/report.md && rm -rf $HOME/ds_smoke

# 1+2 in parallel (independent), then 3:
S=$(sbatch --parsable scripts/csd3/stats/10_stats.sh)
R=$(sbatch --parsable scripts/csd3/stats/15_refs.sh)
sbatch --dependency=afterok:$S:$R scripts/csd3/stats/20_report.sh
```

A scan killed at wallclock resumes: submit `10_stats.sh` again (done shards are skipped). One
source on a whole node: `SOURCES=nomad sbatch -c 76 scripts/csd3/stats/10_stats.sh`.

## Sizing (measured 2026-10-03 on real shards)

| | per shard | shards scanned (individual only / all) | CPU (individual only / all) |
|---|---|---|---|
| Zenodo | ~3 s (10k frames, 55 calcs) | 1,884 / 1,884 | ~2 h |
| NOMAD | 4–46 s (46 s when a shard holds 10k single-frame calcs: per-calc spglib + neighbour list) | 1,315 / 5,345 | ~2–3 h / ~35 h |
| Materials Cloud | ~3 s | 268 / 268 | ~0.2 h |

RAM: ~1 GB per scan worker (one decompressed shard), so `icelake` (3.4 GB/core) is enough; the
report holds ~31M frame rows (87 B each) plus sort buffers for individual uploads (`icelake-himem
-c 4`), and ~20–25 GB for all of NOMAD with its two origin subsets (`-c 8`).
Disk: the scan writes one compressed `.npz` per shard (~3–10 MB; ~7.5k inodes), the references
~10–15 GB of downloads (`rm -rf $DATASET_STATS_DATA/refs` afterwards; the scans are kept).

## Outputs

```
stats/<source>/meta/chunk-NNN.npz       one row per calc (codes + tables), deposits, counters
stats/<source>/scan/shard-NNNNN.npz     one row per frame, per-calc structure records, aggregates
stats/ref_<name>/scan/shard-*.npz       the same for each reference dataset
stats/refs/<name>/                      the reference downloads
stats/report/report.json, report.md     THE result — copy these two home
```

Copy home: `rsync -av <crsid>@login.hpc.cam.ac.uk:/rds/user/<crsid>/hpc-work/stats/report/ stats_csd3/`
(gitignored). Measured times are from the 2026-10-03 run (jobs 37235786/7/8); the results are
written up in `docs/DATASET_EVALUATION.md`.

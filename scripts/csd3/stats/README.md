# Dataset statistics on CSD3

Batch templates for `dataset_stats/` (see `dataset_stats/CLAUDE.md`): size, chemistry, structure
types, label distributions, settings, quality, availability, provenance, redundancy and
consistency buckets of the three harvested datasets, measured side by side with MPtrj, OMat24,
sAlex and the MP / Alexandria material sets. Everything is **read-only** on the datasets; outputs
go to `$DATASET_STATS_DATA` (default `/rds/user/$USER/hpc-work/stats`).

```
scripts/csd3/stats/10_stats.sh    # meta + scan of each source (resumable per shard)  ~40 CPU-h
scripts/csd3/stats/15_refs.sh     # download + scan the reference datasets              ~1-2 h
scripts/csd3/stats/20_report.sh   # combine -> report.json + report.md                   ~0.5-1 h
```

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

| | per shard | shards | CPU |
|---|---|---|---|
| Zenodo | ~3 s (10k frames, 55 calcs) | 1,884 | ~2 h |
| NOMAD | 4–46 s (46 s when a shard holds 10k single-frame calcs: per-calc spglib + neighbour list) | 5,346 | ~35 h |
| Materials Cloud | ~3 s | 268 | ~0.2 h |

RAM: ~1 GB per scan worker (one decompressed shard), so `icelake` (3.4 GB/core) is enough; the
report holds NOMAD's ~52M frame rows (87 B each), its two origin subsets and sort buffers (~20–25 GB peak), hence `icelake-himem -c 8`.
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

Copy home: `rsync -av <crsid>@login.hpc.cam.ac.uk:/rds/user/<crsid>/hpc-work/stats/report/ dataset_csd3/report/`

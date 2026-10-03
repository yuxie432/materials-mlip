#!/bin/bash
#SBATCH -J ds-stats
#SBATCH -p icelake                     # CPU-bound (gzip, text parse, spglib); ~1 GB RSS per worker
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 32                          # one scan worker per core (`sbatch -c 76 ...` for a full node)
#SBATCH -t 06:00:00                    # measured 2026-10-03: ~40 CPU-h for all three sources
#SBATCH -o logs_stats/ds-stats-%j.out
#SBATCH -e logs_stats/ds-stats-%j.err
#SBATCH --mail-type=END,FAIL
#
# Dataset statistics, pass 1+2 for each harvested source: `meta` (metadata.jsonl -> one row per
# calc, minutes) then `scan` (every shard -> one row per frame + per-calc structure descriptors).
# READ-ONLY on the datasets; outputs go to $DATASET_STATS_DATA/<source>/{meta,scan}. The scan is
# resumable per shard (an atomically written .npz per shard is skipped on a re-run), so a job
# killed at wallclock is simply submitted again. Then run 20_report.sh.
#
# Before submit (from the repo root):
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs_stats
#   sbatch scripts/csd3/stats/10_stats.sh                      # all three sources, in turn
#   SOURCES="nomad" sbatch -c 76 scripts/csd3/stats/10_stats.sh  # one source, a full node

set -uo pipefail
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
export DATASET_STATS_DATA="${DATASET_STATS_DATA:-/rds/user/$USER/hpc-work/stats}"
SOURCES="${SOURCES:-materials_cloud zenodo nomad}"
WORKERS="${WORKERS:-${SLURM_CPUS_PER_TASK:-4}}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"

echo "=== ds-stats START $(date -Is) on $(hostname): sources [$SOURCES], $WORKERS workers ==="
rc=0
for src in $SOURCES; do
    echo "--- [$src] meta $(date -Is)"
    python -m dataset_stats.cli meta --source "$src" --workers "$WORKERS" || rc=1
    echo "--- [$src] scan $(date -Is)"
    python -m dataset_stats.cli scan --source "$src" --workers "$WORKERS" || rc=1
done
echo "=== ds-stats DONE exit=$rc $(date -Is) (re-submit to resume an incomplete scan) ==="
exit "$rc"

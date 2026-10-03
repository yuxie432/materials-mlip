#!/bin/bash
#SBATCH -J ds-report
#SBATCH -p icelake-himem               # NOMAD: ~52M frame rows (~4.6 GB), its two origin subsets and sort buffers (~20-25 GB peak)
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8                           # ~54 GiB (6.76 GiB/core) for RAM; the report is single-threaded
#SBATCH -t 03:00:00
#SBATCH -o logs_stats/ds-report-%j.out
#SBATCH -e logs_stats/ds-report-%j.err
#SBATCH --mail-type=END,FAIL
#
# Combine the meta + scan outputs of the three sources (and of every scanned reference) into
# $DATASET_STATS_DATA/report/report.json + report.md. Read-only; re-run freely.
#
#   sbatch scripts/csd3/stats/20_report.sh
# then copy the two small files home, e.g.
#   rsync -av <crsid>@login.hpc.cam.ac.uk:/rds/user/<crsid>/hpc-work/stats/report/ dataset_csd3/report/

set -uo pipefail
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
export DATASET_STATS_DATA="${DATASET_STATS_DATA:-/rds/user/$USER/hpc-work/stats}"
REFS="${REFS:-mptrj omat24_val salex_val mp_materials alexandria_pbe}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"

echo "=== ds-report START $(date -Is) on $(hostname) ==="
# shellcheck disable=SC2086
python -m dataset_stats.cli report --refs $REFS
rc=$?
echo "=== ds-report DONE exit=$rc $(date -Is) ==="
exit "$rc"

#!/bin/bash
#SBATCH -J zh-verify
#SBATCH -p icelake                      # CPU-bound frame parsing; streams one shard at a time (low RAM)
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 1
#SBATCH -t 8:00:00                       # parses ~11.8M frames single-threaded; read-only, so overrun only wastes queue
#SBATCH -o logs/zh-verify-%j.out
#SBATCH -e logs/zh-verify-%j.err
#SBATCH --mail-type=END,FAIL
#
# Read-only dataset integrity check: the metadata<->shard frame_id bijection + curation stats.
# It parses EVERY frame (one shard at a time), so it is long and CPU-bound — run on a compute
# node, NOT the login node (the login watchdog kills long CPU jobs). Nothing is written or
# deleted, so it is safe to run alongside anything.
#
# Before submit: export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU; source <path>/.venv/bin/activate; mkdir -p logs
#   sbatch scripts/csd3/42_verify.sh

set -uo pipefail
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"

echo "=== zh-verify START $(date -Is) on $(hostname) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$ZENODO_HARVEST_DATA/dataset"
rc=$?
echo "=== zh-verify DONE exit=$rc $(date -Is)  (exit=0 => bijection OK; non-zero => mismatch, see above) ==="
exit "$rc"

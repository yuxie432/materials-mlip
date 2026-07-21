#!/bin/bash
#SBATCH -J zen-discover
#SBATCH -A YOUR_ACCOUNT-SL2-CPU        # <-- your CSD3 project account
#SBATCH -p icelake                     # CPU partition (sapphire also fine)
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # discovery is network-bound, not CPU-bound
#SBATCH --time=04:00:00                # generous; the broad OR query dominates
#SBATCH -o discover-%j.out
#SBATCH -e discover-%j.err

# Exhaustive discovery of ALL default queries (recursive date-bisection past the
# 10k search window). Serial + polite: Zenodo search is ~30 req/min even with a
# token, so more cores/tasks do NOT help. Fully resumable -- if the job hits the
# walltime, just resubmit and it continues from the sidecar checkpoint.
set -euo pipefail
cd "${SLURM_SUBMIT_DIR:-$PWD}"

# repo root (this script lives in scripts/estimate/)
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

module load python/3.11 2>/dev/null || true   # or: source ~/miniconda3/bin/activate base
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-$PWD/data}"   # point at scratch if large
# ZENODO_TOKEN is read from .env (gitignored) or the environment.

python -m zenodo_harvest.cli discover --exhaustive \
    --out "$ZENODO_HARVEST_DATA/manifests/candidates_full.jsonl" -v

echo "discovery complete: $ZENODO_HARVEST_DATA/manifests/candidates_full.jsonl"

#!/bin/bash
#SBATCH -J zh-discover
#SBATCH -A CHANGEME-SL3-CPU            # your account — find it with: mybalance
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # rate-limited to 30 req/min: one core is plenty
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH -o logs/zh-discover-%j.out
#SBATCH -e logs/zh-discover-%j.err
#
# Stage 0-1: exhaustive discovery + triage -> a keep-list for the fetch pipeline.
# Needs outbound HTTPS (see 00_check_network.sh). Both stages are single-stream by
# design: Zenodo caps /api/records at 30 requests/minute even with a token, and the
# zip-peek Range GETs share the global request budget.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
# ZENODO_HARVEST_DATA is read at IMPORT time, so it must be set before python starts.
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# module load python/3.11        # or: source ~/miniforge3/bin/activate zenodo-harvest
cd "${SLURM_SUBMIT_DIR:-.}"      # repo root (holds .env with ZENODO_TOKEN)
# --------------------------------------------------------------------------------

mkdir -p logs "$ZENODO_HARVEST_DATA/manifests"
MAN="$ZENODO_HARVEST_DATA/manifests"

echo "=== stage 0: discover (exhaustive) $(date -Is) ==="
# --exhaustive = recursive created-date bisection, to get past Zenodo's 10k search
# window. Three resource types: dataset + software + publication (measured 2026-07:
# 'other' holds ~8 useful records and is not worth the paging).
python -m zenodo_harvest.cli -v discover --exhaustive \
    --resource-type dataset --resource-type software --resource-type publication \
    --out "$MAN/candidates_full.jsonl"

echo "=== stage 1: triage (peek on) $(date -Is) ==="
# Peek reads each remote .zip's central directory over HTTP Range (~tens of KB) and
# DROPS archives proven to hold no VASP — ~1000x cheaper than a wasted download.
python -m zenodo_harvest.cli -v triage \
    --in "$MAN/candidates_full.jsonl" \
    --out "$MAN/keep.jsonl" --min-rank 3

echo "=== done $(date -Is): $(wc -l < "$MAN/keep.jsonl") records to fetch ==="

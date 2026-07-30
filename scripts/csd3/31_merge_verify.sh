#!/bin/bash
#SBATCH -J zh-merge
#SBATCH -A CHANGEME-SL3-CPU            # your account — find it with: mybalance
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # RAM headroom (~13 GiB): verify/merge read ONE shard's
                                       # frames into memory at a time, big for large structures
#SBATCH --time=12:00:00
#SBATCH -o logs/zh-merge-%j.out
#SBATCH -e logs/zh-merge-%j.err
#
# Tail of the array-parse flow: fold the per-task dataset dirs into one, verify the
# metadata<->shard integrity, then reclaim scratch. Run after 30_parse_array.sh:
#   sbatch --dependency=afterany:$ARRAY scripts/csd3/31_merge_verify.sh
#
# merge MOVES shards (never recompresses them) and appends metadata only once a
# source's shards are in place, so an interrupted merge is resumable — re-running it
# picks up from its journal instead of double-appending.
#
# NB: `mkdir -p logs` from the repo root before submitting — SLURM opens the -o/-e paths
# above before this script body runs.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# Activate the harvest env BEFORE `sbatch` (sbatch captures your submit env by default):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

mkdir -p logs
DS="$ZENODO_HARVEST_DATA/dataset"
RAW="$ZENODO_HARVEST_DATA/raw"

shopt -s nullglob
sources=("$DS"/task-*)
if (( ${#sources[@]} == 0 )); then
    echo "no $DS/task-* dirs to merge (already merged?)" >&2
else
    echo "=== merge ${#sources[@]} task dirs $(date -Is) ==="
    # NB os.replace across filesystems falls back to a copy, but keep task dirs on the
    # SAME filesystem as --into for a fast rename-only merge.
    python -m zenodo_harvest.cli -v merge-datasets --into "$DS" "${sources[@]}"
fi

echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DS"

echo "=== purge-raw (dry run first) $(date -Is) ==="
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" --dry-run \
    | tail -20
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" \
    > "logs/zh-purge-${SLURM_JOB_ID:-local}.json"
grep -E '"recids_(purged|kept)"|"bytes_freed"' "logs/zh-purge-${SLURM_JOB_ID:-local}.json"
echo "=== done $(date -Is) ==="

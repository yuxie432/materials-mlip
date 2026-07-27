#!/bin/bash
#SBATCH -J zh-parse
#SBATCH -A CHANGEME-SL3-CPU            # your account — find it with: mybalance
#SBATCH -p icelake                     # icelake-himem for very long trajectories
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # parse is single-threaded; parallelism = the array
#SBATCH --time=12:00:00
#SBATCH --array=0-15                   # must match the --parts used by `split`
#SBATCH -o logs/zh-parse-%A_%a.out
#SBATCH -e logs/zh-parse-%A_%a.err
#
# OPTIONAL many-core parse: parse is CPU-bound and embarrassingly parallel, so a big
# already-fetched manifest parses far faster as an array job than inside `pipeline`.
# Each task parses ONE part into its OWN dataset dir (parse takes a per-dir lock, so
# tasks can never share one), and 31_merge_verify.sh folds them together afterwards.
#
# Submit (from the repo root):
#   python -m zenodo_harvest.cli split --in $ZENODO_HARVEST_DATA/manifests/fetched.jsonl \
#       --parts 16 --out-dir $ZENODO_HARVEST_DATA/manifests/parts
#   ARRAY=$(sbatch --parsable scripts/csd3/30_parse_array.sh)
#   sbatch --dependency=afterany:$ARRAY scripts/csd3/31_merge_verify.sh
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# module load python/3.11        # or: source ~/miniforge3/bin/activate zenodo-harvest
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-4000000000}"   # see 20_pipeline.sh
mkdir -p logs
i=$(printf "%03d" "${SLURM_ARRAY_TASK_ID:-0}")
PART="$ZENODO_HARVEST_DATA/manifests/parts/fetched.part-$i.jsonl"

if [[ ! -s "$PART" ]]; then
    echo "part $PART is empty/absent — nothing to do for this task"
    exit 0
fi

echo "=== parse task $i: $PART $(date -Is) ==="
python -m zenodo_harvest.cli -v parse \
    --in "$PART" \
    --dataset-dir "$ZENODO_HARVEST_DATA/dataset/task-$i" \
    --raw-dir "$ZENODO_HARVEST_DATA/raw" \
    --max-primary-bytes "$MAX_PRIMARY_BYTES"
echo "=== parse task $i done $(date -Is) ==="

#!/bin/bash
#SBATCH -J zh-parse
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # 6760 MiB/core: parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # bought for RAM (~26 GiB); parse is single-threaded,
                                       # so the SPEEDUP is the array, not these cores
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
#
# NB: `mkdir -p logs` from the repo root before submitting — SLURM opens the -o/-e paths
# above before this script body runs.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# Activate the harvest env BEFORE `sbatch` (sbatch captures your submit env by default):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
# Parse copies each OUTCAR into $TMPDIR before ASE reads it; keep that on fast node-local
# scratch (/local, auto-removed at job end) so a large temp copy neither eats the RAM budget
# --max-primary-bytes is sized against (a tmpfs /tmp would) nor the /rds quota.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# RAM guard: pymatgen peak RSS is ~10x the primary file size (see 20_pipeline.sh). Sized to
# the ~26 GiB that --cpus-per-task=4 on icelake-himem gives (0.85 x 26 GiB / 10 ~= 2 GB); no
# concurrent fetch here (parse-only job), so this can go a little higher than the pipeline's
# if you raise --cpus-per-task with it. Keep it EQUAL to 20_pipeline.sh's value unless you
# are deliberately doing a high-RAM second pass for the primaries the pipeline skipped.
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-2000000000}"
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

#!/bin/bash
#SBATCH -J zh-cleanup
#SBATCH -p icelake                     # metadata walk is I/O-bound — no himem needed
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 1
#SBATCH -t 8:00:00                      # a full raw/ walk + deletes (31044's ~129k-inode walk is slow); resumable
#SBATCH -o logs/zh-cleanup-%j.out
#SBATCH -e logs/zh-cleanup-%j.err
#SBATCH --mail-type=END,FAIL
#
# Reclaim raw/ staging INODES on a COMPUTE node. The login node kills a long metadata walk
# (and contends with a running harvest), so run this stage in batch instead. DRY-RUN by
# default; APPLY=1 does the real delete. Both write a full audit list to scratch.
#
# Before submitting (sbatch captures your submit environment, exactly like 20_pipeline.sh):
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU      # from `mybalance`
#   source <path-to>/.venv/bin/activate          # the harvest env (cleanup imports pymatgen via parse)
#   squeue -u $USER                              # make sure the harvest job is NOT running
#   mkdir -p logs
#   sbatch                     scripts/csd3/40_cleanup.sh   # 1. DRY-RUN (review the summary + audit)
#   APPLY=1 sbatch             scripts/csd3/40_cleanup.sh   # 2. delete (subtree-conservative, safe)
#   APPLY=1 DROP_RECOVERABLE=1 sbatch scripts/csd3/40_cleanup.sh  # only if step 2 doesn't free enough
#
# --apply is idempotent + resumable: if the job hits wallclock, just resubmit — already-
# deleted files are gone, so it continues where it left off.

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"          # repo root (where you ran sbatch)

MODE="DRY-RUN"
FLAGS=(--audit "$ZENODO_HARVEST_DATA/cleanup_audit-${SLURM_JOB_ID:-local}.jsonl")
if [[ "${APPLY:-0}" == "1" ]]; then FLAGS+=(--apply); MODE="APPLY"; fi
if [[ "${DROP_RECOVERABLE:-0}" == "1" ]]; then FLAGS+=(--drop-recoverable); fi
if [[ "${ONLY_ORPHANS:-0}" == "1" ]]; then FLAGS+=(--only-orphans); fi

echo "=== zh-cleanup $MODE  $(date -Is)  on $(hostname) ==="
echo "raw usage BEFORE:"; lfs quota -u "$USER" /rds/user/"$USER"/hpc-work 2>/dev/null || true
python scripts/estimate/cleanup_staging.py "${FLAGS[@]}"
rc=$?
echo "raw usage AFTER:"; lfs quota -u "$USER" /rds/user/"$USER"/hpc-work 2>/dev/null || true
echo "=== zh-cleanup exit=$rc  $(date -Is) ==="
exit "$rc"

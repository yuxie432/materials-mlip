#!/bin/bash
#SBATCH -J zh-pipeline
#SBATCH -A CHANGEME-SL3-CPU            # your account — find it with: mybalance
#SBATCH -p icelake                     # icelake-himem if parse needs more RAM/core
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5              # 4 download threads + 1 parse thread
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH -o logs/zh-pipeline-%j.out
#SBATCH -e logs/zh-pipeline-%j.err
#
# Stages 2-4 in ONE overlapped, disk-paced command: fetch(batch i+1) runs while
# parse+purge(batch i) runs, so the network is never idle during parsing. Ends with
# `verify` (metadata<->shard bijection + coverage stats).
#
# The harvest can outlast one job's wallclock. Everything is resumable, so either
# re-submit this script by hand, or set RESUBMIT=1 to have it chain a follow-on job
# automatically:   RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# module load python/3.11        # or: source ~/miniforge3/bin/activate zenodo-harvest
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# ---- HARVEST PARAMETERS ---------------------------------------------------------
PARTS="${PARTS:-40}"                   # batches; more parts = smaller peak staging
WORKERS="${WORKERS:-4}"                # concurrent downloads (Zenodo: 100 req/min global)
MAX_BYTES="${MAX_BYTES:-0}"            # 0 = uncapped per-file download
MAX_MEMBER_BYTES="${MAX_MEMBER_BYTES:-30000000000}"   # ~30 GB: bomb guard, keeps long AIMD
# Bounds the WHOLE raw staging dir (both concurrently-staged batches included), so this
# is ~0.8 x the 1 TB hpc-work quota. The remaining ~200 GB is headroom for: the archive
# sitting beside its extracted output while a record is being staged, the dataset dir on
# the same quota (est. 15-75 GB), and the valve's bounded overshoot. The valve charges
# every byte as it is actually written (and refunds each archive once its VASP members
# are extracted) — nothing is predicted from a decompression ratio — so `staged <= this`
# holds exactly, whatever the real expansion turns out to be.
MAX_DISK_BYTES="${MAX_DISK_BYTES:-800000000000}"
# hpc-work is ALSO capped at 1 million files, and measured on real Zenodo records this
# binds FIRST: extracted VASP trees ran ~270 KiB mean / 7.6 KiB MEDIAN per file (screening
# uploads are thousands of tiny per-calc dirs), so 1M files arrives near ~0.3 TB. 800k
# leaves headroom for the dataset + manifests and for the valve's in-flight overshoot.
MAX_DISK_FILES="${MAX_DISK_FILES:-800000}"
# Guard: refuse to ATTEMPT a primary output bigger than this (0 = attempt everything).
# pymatgen holds a whole ionic trajectory in RAM and can peak at several times the file
# size, so one huge vasprun.xml can get the job cgroup-killed, losing the run's progress.
# NB memory is allocated per core on CSD3 (~3.4 GB/core on icelake, ~6.8 GB on
# icelake-himem), so --cpus-per-task=5 above gives this job only ~17 GB — a 4 GB primary
# can approach that once expanded. If parse gets OOM-killed, either switch -p to
# icelake-himem, raise --cpus-per-task, or lower this. Check the real peak with
# `sacct -j <jobid> --format=MaxRSS` and tune from there.
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-4000000000}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-8}"      # resubmission chain guard
ATTEMPT="${ATTEMPT:-1}"
# --------------------------------------------------------------------------------

mkdir -p logs
MAN="$ZENODO_HARVEST_DATA/manifests"
if [[ ! -s "$MAN/keep.jsonl" ]]; then
    echo "ERROR: $MAN/keep.jsonl missing — run 10_discover.sh first." >&2
    exit 2
fi

echo "=== pipeline attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="
df -h "$ZENODO_HARVEST_DATA" || true
quota -s 2>/dev/null || true

rc=0
python -m zenodo_harvest.cli -v pipeline \
    --in "$MAN/keep.jsonl" \
    --parts "$PARTS" --workers "$WORKERS" \
    --max-bytes "$MAX_BYTES" \
    --max-member-bytes "$MAX_MEMBER_BYTES" \
    --max-disk-bytes "$MAX_DISK_BYTES" \
    --max-disk-files "$MAX_DISK_FILES" \
    --max-primary-bytes "$MAX_PRIMARY_BYTES" \
    --raw-dir "$ZENODO_HARVEST_DATA/raw" \
    --dataset-dir "$ZENODO_HARVEST_DATA/dataset" \
    | tee "logs/zh-pipeline-${SLURM_JOB_ID:-local}.summary.json" || rc=$?

echo "=== pipeline exit=$rc $(date -Is) ==="
# Staged file count vs the 1M-inode quota on hpc-work (bytes are only half the limit).
echo "staged files under raw/: $(find "$ZENODO_HARVEST_DATA/raw" -type f 2>/dev/null | wc -l)"

# A non-zero exit means "not finished / something to look at" — the pipeline reports
# process_errors + verify in its JSON summary. Since every stage is resumable, chain a
# follow-on job when RESUBMIT=1 (bounded by MAX_ATTEMPTS so a real error cannot spin).
if [[ "$rc" -ne 0 && "${RESUBMIT:-0}" == "1" && "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]]; then
    echo "resubmitting (attempt $((ATTEMPT + 1))) to continue the harvest"
    # Pass the counter explicitly: --export=ALL,... so the chained job sees it even if
    # the site default for --export is not ALL.
    sbatch --dependency="afterany:${SLURM_JOB_ID}" \
           --export="ALL,ATTEMPT=$((ATTEMPT + 1)),RESUBMIT=1" "$0"
fi
exit "$rc"

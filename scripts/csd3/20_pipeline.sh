#!/bin/bash
#SBATCH -J zh-pipeline
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # 6760 MiB/core: parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # bought for RAM (~26 GiB), not compute — see MAX_PRIMARY_BYTES
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH --signal=B:USR1@600            # SIGUSR1 to the batch shell 10 min before wallclock
#SBATCH -o logs/zh-pipeline-%j.out     #   -> lets RESUBMIT=1 queue a resume job before the
#SBATCH -e logs/zh-pipeline-%j.err     #      hard SIGKILL (see the run/resubmit block below)
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address.
#   The RESUBMIT chain re-runs $0 (this file), so you get one END/FAIL email PER round (<= MAX_ATTEMPTS).
#
# Stages 2-4 in ONE overlapped, disk-paced command: fetch(batch i+1) runs while
# parse+purge(batch i) runs, so the network is never idle during parsing. Ends with
# `verify` (metadata<->shard bijection + coverage stats).
#
# The harvest can outlast one job's wallclock (a full run typically needs several 12 h
# jobs). Everything is resumable, so either re-submit this script by hand, or set
# RESUBMIT=1 to chain follow-on jobs automatically across wallclock kills:
#   RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh
# RESUBMIT is an ON/OFF switch, NOT a count — MAX_ATTEMPTS (default 8) bounds the number
# of rounds, so the usual 3-4 rounds need no extra flags. The chain survives the wallclock
# limit via #SBATCH --signal=B:USR1@600 + a trap (see the run block below), because at the
# limit SLURM SIGKILLs the job and a resubmit placed only AFTER the pipeline would never
# run on the very timeout it is meant for.
#
# NB: create logs/ BEFORE you submit — SLURM opens the -o/-e paths above before this
# script body runs, so a missing logs/ makes the job fail to start. From the repo root,
# once: `mkdir -p logs`.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# Activate the harvest env BEFORE `sbatch` — sbatch captures your submit environment by
# default (--export=ALL), and it is carried through the RESUBMIT chain too. Keep it OUT of
# this script: an uncommented `module load` that fails would abort the job under `set -e`.
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
# Parse copies each OUTCAR into $TMPDIR before ASE reads it (parse.py). Point TMPDIR at fast
# node-local scratch (/local: 57-131 GB, auto-removed at job end) so a large temp copy (up to
# --max-primary-bytes) neither eats the RAM budget that cap is sized against — a tmpfs /tmp
# would — nor the /rds quota (the disk valve does not track $TMPDIR). Fall back to /rds scratch.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
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
# pymatgen holds a whole ionic trajectory in RAM: MEASURED peak RSS is ~10x the
# vasprun.xml/OUTCAR file size (CSD3 icelake-himem: a 534 MB file peaks at ~5.6 GB). An
# over-budget parse is a cgroup SIGKILL of the whole job (taking the in-flight fetch progress
# with it), NOT a catchable error, so we cap the file size to cap the RAM. Memory is per core
# on CSD3 (icelake-himem = 6760 MiB/core), so --cpus-per-task=4 above gives ~26 GiB; after
# leaving room for the concurrent fetch (~1 GiB), the safe cap is ~0.85 x 26 GiB / 10 ~= 2 GB.
# To parse bigger primaries, raise --cpus-per-task (more RAM) and this value together. Re-check
# on REAL data (synthetic samples read a touch high) with `scripts/csd3/csd3_parse_memory.py`,
# then verify the peak: `sacct -j <jobid> --format=MaxRSS`.
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-2000000000}"
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
# RDS usage vs the 1 TB / 1M-file quota. NB `df -h` shows the whole shared Lustre pool (not
# your quota) and plain `quota` misses Lustre — prefer the CSD3 `quota` wrapper / `lfs quota`.
# The pipeline's own peak_staged_bytes/peak_staged_files (in its JSON summary) is authoritative.
quota 2>/dev/null || lfs quota -u "$USER" "$ZENODO_HARVEST_DATA" 2>/dev/null \
    || df -h "$ZENODO_HARVEST_DATA" 2>/dev/null || true

# Targeted ZIP member fetch is ON by default (pulls only VASP files out of a .zip over
# HTTP Range, skipping heavy CHGCAR/WAVECAR bulk and never staging the archive). Pass
# --no-zip-stream to disable, or --zip-stream-max-files N to tune the per-archive request
# budget. --max-member-bytes below only bounds the whole-download fallback + tar/rar/7z;
# targeted ZIP members are always wanted VASP files and are bounded solely by the disk valve.
SUMMARY="logs/zh-pipeline-${SLURM_JOB_ID:-local}.summary.json"
NEXT_JOBID=""
submit_successor() {
    # Queue ONE resume job (idempotent: USR1 may fire more than once, and the exit-code
    # path below may also call this). --export=ALL,... so the chained job sees the
    # incremented counter even if the site default for --export is not ALL. Enabled by any
    # non-zero RESUBMIT (so RESUBMIT=1 AND RESUBMIT=4 both work — the ROUND count is
    # MAX_ATTEMPTS, not this value); RESUBMIT=0 or unset disables.
    if [[ "${RESUBMIT:-0}" != "0" && -z "$NEXT_JOBID" && "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]]; then
        echo "=== queueing resume job (attempt $((ATTEMPT + 1))/$MAX_ATTEMPTS) $(date -Is) ==="
        NEXT_JOBID=$(sbatch --parsable --dependency="afterany:${SLURM_JOB_ID}" \
            --export="ALL,ATTEMPT=$((ATTEMPT + 1)),RESUBMIT=1" "$0") || NEXT_JOBID=""
        echo "  -> successor job: ${NEXT_JOBID:-<sbatch failed; resubmit by hand>}"
    fi
}
# SIGUSR1 arrives ~10 min before wallclock (see #SBATCH --signal=B:USR1@600): queue the
# successor NOW, then keep harvesting until SLURM hard-kills this job. B: signals only the
# batch shell, so the pipeline itself is not interrupted; a mid-batch kill loses no data
# (every stage is resumable + idempotent — the successor continues where this job stopped).
trap 'submit_successor' USR1

# Run the pipeline in the BACKGROUND: bash defers a trap until the current FOREGROUND
# command returns, so a foreground pipeline would swallow USR1 until the (too-late) kill.
# stdout (the JSON summary) -> tee to $SUMMARY + the job .out; stderr (-v logs) -> job .err.
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
    > >(tee "$SUMMARY") &
PIPELINE_PID=$!
# Wait for the pipeline. The USR1 trap interrupts `wait` (which then returns >128 while the
# child is still alive), so re-wait until it truly exits and rc is its real status (incl.
# 137/143 if SLURM SIGKILL/SIGTERM the child at the hard limit). Two bash subtleties:
#   * `wait` must be a PLAIN statement, not a loop/if CONDITION — in a condition a
#     trap-interrupted re-wait wrongly returns 0 (validated: `if wait` breaks, this works);
#   * so it must run under `set +e`, since a non-zero `wait` would otherwise abort the job.
set +e
while true; do
    wait "$PIPELINE_PID"; rc=$?
    # rc<=128: real exit status. rc>128: either the child died from a signal (SIGKILL=137
    # / SIGTERM=143 at the hard limit) OR `wait` was interrupted by our USR1 trap while the
    # child runs on — the liveness check tells the two apart.
    if [[ "$rc" -le 128 ]] || ! kill -0 "$PIPELINE_PID" 2>/dev/null; then break; fi
done
set -e

echo "=== pipeline exit=$rc $(date -Is) ==="
# Staged file count vs the 1M-inode quota on hpc-work (bytes are only half the limit).
echo "staged files under raw/: $(find "$ZENODO_HARVEST_DATA/raw" -type f 2>/dev/null | wc -l)"

# The USR1 trap covers the wallclock-timeout case. This covers a hard NON-ZERO EXIT before
# the signal (a caught fetch/parse failure — pipeline reports it in the JSON summary): the
# harvest is resumable, so chain a follow-on. submit_successor is idempotent, so if the
# trap already queued one this is a no-op. On a CLEAN finish (rc==0), cancel any successor
# the trap pre-queued in the last 10 min — the harvest completed inside this job.
if [[ "$rc" -ne 0 ]]; then
    submit_successor
elif [[ -n "$NEXT_JOBID" ]]; then
    echo "harvest finished cleanly; cancelling pre-queued successor $NEXT_JOBID"
    scancel "$NEXT_JOBID" 2>/dev/null || true
fi
exit "$rc"

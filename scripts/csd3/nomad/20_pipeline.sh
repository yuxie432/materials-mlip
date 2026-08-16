#!/bin/bash
#SBATCH -J nomad-pipeline
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # 6760 MiB/core: parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # bought for RAM (~26 GiB), not compute — see MAX_PRIMARY_BYTES
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH --signal=B:USR1@600            # SIGUSR1 to the batch shell 10 min before wallclock
#SBATCH -o logs_nomad/nomad-pipeline-%j.out  #   -> lets RESUBMIT=1 queue a resume job before the
#SBATCH -e logs_nomad/nomad-pipeline-%j.err  #      hard SIGKILL (see the run/resubmit block below)
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address.
#
# NOMAD stages 2-4 in ONE overlapped, disk-paced command: fetch(batch i+1) runs while
# parse+purge(batch i) runs, so the network is never idle during parsing. Ends with
# `verify` (metadata<->shard bijection + coverage stats). Reuses the SHARED
# zenodo_harvest parse/store/verify + the disk/inode valve unchanged — only the fetch
# (nomad_harvest) is source-specific.
#
# STANDALONE NOMAD TREE (kept OUT of the Zenodo dir). Everything NOMAD writes — raw staging,
# manifests, the dataset — lands under $NOMAD_HARVEST_DATA (a sibling of the Zenodo scratch
# root), so a concurrent Zenodo (re)harvest never shares a staging tree with this job and the
# two disk valves never count each other's files. Fold the finished NOMAD dataset into the
# combined one later with `zenodo_harvest.cli merge-datasets`. The ONLY Zenodo path this job
# reads is the dataset/metadata.jsonl used for cross-source dedup at discover time (read-only).
#
# The full 7.1M direct-upload set is Zenodo-scale: the bulk fetch is bandwidth-bound, not
# fetch-rate-bound (~47k requests total — see docs/NOMAD_HARVEST.md), so at Zenodo-like
# throughput this is a ~1-2 week single self-resubmitting campaign, NOT a multi-job one.
# Scoping with 10_discover.sh's MAX_ENTRIES is optional (an early checkpoint), not required.
# Everything is resumable, so re-submit by hand OR set RESUBMIT=1 to self-chain across
# wallclock kills:  RESUBMIT=1 sbatch scripts/csd3/nomad/20_pipeline.sh
# RESUBMIT is ON/OFF, not a count — MAX_ATTEMPTS (default 8) bounds the rounds.
#
# NB: create logs_nomad/ BEFORE you submit (SLURM opens -o/-e before the body runs): `mkdir -p logs_nomad`.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
# ZENODO_HARVEST_DATA = the Zenodo scratch root (read-only here, for dedup metadata).
# NOMAD_HARVEST_DATA  = NOMAD's OWN sibling root — all NOMAD raw/manifests/dataset live here,
#                       fully separate from Zenodo. Both are read at IMPORT time (set first).
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
# Activate the harvest env BEFORE `sbatch` (captured via --export=ALL, carried through the
# RESUBMIT chain). Keep it OUT of this script (a failed `module load` would abort under set -e):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
# Parse copies each OUTCAR into $TMPDIR before ASE reads it, and the bulk fetch writes transient
# batch zips there. Point TMPDIR at fast node-local scratch (/local, auto-removed at job end) so
# neither eats the RAM budget nor the /rds quota (the disk valve does not track $TMPDIR). Fall
# back to NOMAD-root scratch (NOT the Zenodo tree) so a fallback still stays out of Zenodo.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$NOMAD_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# ---- HARVEST PARAMETERS ---------------------------------------------------------
PARTS="${PARTS:-40}"                   # batches; more parts = smaller peak staging
WORKERS="${WORKERS:-4}"                # concurrent downloads (NOMAD allows ~10 concurrent)
# DISK/INODE PARTITION (fixed-slice design). The valve bounds only THIS job's raw staging
# ($NOMAD_HARVEST_DATA/raw); it cannot see the Zenodo tree or another job's staging. The 1 TB /
# 1M-inode hpc-work quota is SHARED across everything you run, so give each job a fixed slice
# that SUMS under quota after reserving the fixed/growing consumers. Worked budget (co-running
# NOMAD + ONE small Zenodo recovery), quota ~1000 GB / 1.0M inodes:
#     reserve  existing Zenodo raw+dataset ~100 GB / ~100k   (measured)
#     reserve  NOMAD final dataset+manifests ~40-60 GB / ~few-k (grows during the run)
#     NOMAD raw valve            600 GB / 600k   (this job, below)
#     Zenodo-recovery raw valve  150 GB / 150k   (override MAX_DISK_* on scripts/csd3/46_*.sh)
#     headroom                  ~100 GB / ~50k
# Running NOMAD SOLO? You may raise these to ~800 GB / 800k. The valve charges every byte as
# written + every inode as created and refunds on delete, so `staged <= limit` holds exactly.
MAX_DISK_BYTES="${MAX_DISK_BYTES:-600000000000}"
# hpc-work is ALSO capped at 1M inodes and this binds FIRST: each NOMAD entry stages ~3-4 inodes
# (<entry_id>/extracted/calc/vasprun.xml), so ~150-200k entries fill 600k before bytes.
MAX_DISK_FILES="${MAX_DISK_FILES:-600000}"
# RAM guard: refuse to ATTEMPT a primary bigger than this (0 = attempt everything). pymatgen
# peak RSS is ~10x the vasprun size; an over-budget parse is a cgroup SIGKILL, not a catchable
# error. icelake-himem = 6760 MiB/core, so --cpus-per-task=4 gives ~26 GiB -> ~0.85x/10 ~= 2 GB.
# NOMAD vaspruns are small (median 0.36 MB, max ~14 MB), so this rarely bites — but AIMD tails
# can be larger, and the guard is cheap. An over-cap primary is skipped (primary_too_large,
# non-terminal) and re-tried on a bigger-RAM run.
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-2000000000}"
# Hard-kill a single calc's parse after this many seconds (0 = off), so one non-terminating
# pymatgen/ASE parse can't silently freeze the whole overlapped pipeline until wallclock.
PARSE_TIMEOUT="${PARSE_TIMEOUT:-1200}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-8}"      # resubmission chain guard
ATTEMPT="${ATTEMPT:-1}"
# --------------------------------------------------------------------------------

mkdir -p logs_nomad
MAN="$NOMAD_HARVEST_DATA/manifests"
DATASET_DIR="$NOMAD_HARVEST_DATA/dataset"
RAW="$NOMAD_HARVEST_DATA/raw"
if [[ ! -s "$MAN/nomad_keep.jsonl" ]]; then
    echo "ERROR: $MAN/nomad_keep.jsonl missing — run scripts/csd3/nomad/10_discover.sh first." >&2
    exit 2
fi

echo "=== nomad pipeline attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="
echo "    NOMAD tree: $NOMAD_HARVEST_DATA  (raw=$RAW dataset=$DATASET_DIR)"
# RDS usage vs the 1 TB / 1M-file quota (SHARED across all your jobs). NB `df -h` shows the
# shared pool, not your quota; prefer the CSD3 `quota` wrapper / `lfs quota`. lfs quota reports
# the whole hpc-work filesystem regardless of the path given. The pipeline's own peak_staged_*
# (in its per-fetch logs) is authoritative for THIS job's slice.
quota 2>/dev/null || lfs quota -u "$USER" "$NOMAD_HARVEST_DATA" 2>/dev/null \
    || df -h "$NOMAD_HARVEST_DATA" 2>/dev/null || true

# ---- CLEAR A STALE PARSE LOCK ---------------------------------------------------
# The RESUBMIT chain is STRICTLY SEQUENTIAL (--dependency=afterany), so when THIS job starts
# no other job of this harvest is running. A parse SIGKILLed at the previous job's wallclock
# cannot release its DatasetLock, and a successor on a different node can never auto-reclaim it
# (cross-node liveness is uncheckable — see store.py:DatasetLock). Any lock present at startup
# is therefore provably stale. NB safe ONLY because the chain is sequential — do NOT run a
# separate parse/array job against this SAME --dataset-dir alongside the pipeline.
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock (sequential resubmit chain => stale): $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

SUMMARY="logs_nomad/nomad-pipeline-${SLURM_JOB_ID:-local}.summary.json"
NEXT_JOBID=""
submit_successor() {
    # Queue ONE resume job (idempotent). --export=ALL,... so the chained job sees the
    # incremented counter. Enabled by any non-zero RESUBMIT; RESUBMIT=0/unset disables.
    if [[ "${RESUBMIT:-0}" != "0" && -z "$NEXT_JOBID" && "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]]; then
        echo "=== queueing resume job (attempt $((ATTEMPT + 1))/$MAX_ATTEMPTS) $(date -Is) ==="
        NEXT_JOBID=$(sbatch --parsable --dependency="afterany:${SLURM_JOB_ID}" \
            --export="ALL,ATTEMPT=$((ATTEMPT + 1)),RESUBMIT=1" "$0") || NEXT_JOBID=""
        echo "  -> successor job: ${NEXT_JOBID:-<sbatch failed; resubmit by hand>}"
    fi
}
# SIGUSR1 arrives ~10 min before wallclock: queue the successor NOW, then keep harvesting
# until SLURM hard-kills this job. B: signals only the batch shell, so the pipeline is not
# interrupted; a mid-batch kill loses no data (every stage is resumable + idempotent).
trap 'submit_successor' USR1

# Run the pipeline in the BACKGROUND: bash defers a trap until the current FOREGROUND command
# returns, so a foreground pipeline would swallow USR1 until the (too-late) kill. NOMAD needs
# no --max-bytes/--max-member-bytes (it stages single small vasprun files, no archives) — the
# disk/inode valve is the only staging bound.
rc=0
python -m nomad_harvest.cli -v pipeline \
    --in "$MAN/nomad_keep.jsonl" \
    --parts "$PARTS" --workers "$WORKERS" \
    --max-disk-bytes "$MAX_DISK_BYTES" \
    --max-disk-files "$MAX_DISK_FILES" \
    --max-primary-bytes "$MAX_PRIMARY_BYTES" \
    --parse-timeout "$PARSE_TIMEOUT" \
    --raw-dir "$RAW" \
    --dataset-dir "$DATASET_DIR" \
    > >(tee "$SUMMARY") &
PIPELINE_PID=$!
# Wait for the pipeline. The USR1 trap interrupts `wait` (returns >128 while the child lives),
# so re-wait until it truly exits and rc is its real status (incl. 137/143 on SLURM kill).
# `wait` must be a PLAIN statement under `set +e` (a trap-interrupted re-wait in an `if`
# CONDITION wrongly returns 0).
set +e
while true; do
    wait "$PIPELINE_PID"; rc=$?
    if [[ "$rc" -le 128 ]] || ! kill -0 "$PIPELINE_PID" 2>/dev/null; then break; fi
done
set -e

echo "=== nomad pipeline exit=$rc $(date -Is) ==="
echo "staged files under raw/: $(find "$RAW" -type f 2>/dev/null | wc -l)"

# USR1 covers the wallclock-timeout case. This covers a hard NON-ZERO EXIT before the signal
# (a caught fetch/parse failure — reported in the JSON summary): the harvest is resumable, so
# chain a follow-on. submit_successor is idempotent. On a CLEAN finish (rc==0), cancel any
# successor the trap pre-queued in the last 10 min.
if [[ "$rc" -ne 0 ]]; then
    submit_successor
elif [[ -n "$NEXT_JOBID" ]]; then
    echo "harvest finished cleanly; cancelling pre-queued successor $NEXT_JOBID"
    scancel "$NEXT_JOBID" 2>/dev/null || true
fi
exit "$rc"

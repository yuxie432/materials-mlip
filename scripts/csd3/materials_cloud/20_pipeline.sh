#!/bin/bash
#SBATCH -J mc-pipeline
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # 6760 MiB/core: parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16             # bought mostly for RAM (~106 GiB): 3 parse workers x ~12x RSS
                                       # x a 2.5 GB cap = 90 GiB + ~8 GiB overhead. Rule (check
                                       # mc_bench.json's worst_rss_ratio / suggested_caps first):
                                       # cpus x 6760 MiB >= PARSE_WORKERS x ratio x MAX_PRIMARY_BYTES + ~8 GiB
                                       # Primaries above the cap (long-AIMD vaspruns) are deferred, kept
                                       # staged, and parsed afterwards by 30_bigparse.sh on a fat node.
#SBATCH --time=12:00:00                # SL3 max; the MC harvest is expected to fit ONE job
#SBATCH --signal=B:USR1@600            # SIGUSR1 10 min before wallclock -> RESUBMIT=1 queues a resume
#SBATCH -o logs_mc/mc-pipeline-%j.out
#SBATCH -e logs_mc/mc-pipeline-%j.err
#SBATCH --mail-type=END,FAIL
#
# Materials Cloud stages 2-4 in ONE overlapped, disk-paced command: fetch(part i+1) runs while
# parse+purge(part i) runs. The fetch is the SHARED zenodo_harvest fetch driven with an anonymous
# MC session (files come from CSCS S3 via MC's 302 redirect; md5-verified, Range-resumable, targeted
# zip members where worthwhile, legacy AiiDA exports extracted as zips); parse/store/verify are the
# shared, unmodified code. Ends with `verify`. Everything is resumable: re-submit by hand or set
# RESUBMIT=1 to self-chain across wallclock kills (MAX_ATTEMPTS bounds the rounds).
#
# STANDALONE MC TREE ($MC_HARVEST_DATA, a sibling of the Zenodo/NOMAD roots) — the disk valve walks
# only its own raw dir, so it never counts the other harvests' files. The /rds quota itself is
# SHARED: the defaults below assume ~800 GB / ~900k inodes are free for THIS job (check `quota`).
# Run 15_bench.sh first: its mc_speed.json / mc_bench.json set WORKERS, PARSE_WORKERS and the cap.
#
# NB: `mkdir -p logs_mc` BEFORE you submit (SLURM opens -o/-e before the body runs).
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
# Activate the harvest env BEFORE `sbatch` (captured via --export=ALL, carried through RESUBMIT):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
# Parse copies each OUTCAR into $TMPDIR before ASE reads it: node-local /local keeps that off the
# RAM budget (tmpfs /tmp) and off the /rds quota. Fall back to MC-root scratch.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$MC_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# ---- HARVEST PARAMETERS ---------------------------------------------------------
PARTS="${PARTS:-8}"                    # batches of fetch units (multi-archive records are split per
                                       # archive by triage, so a part never has to hold a whole 44 GB record)
WORKERS="${WORKERS:-4}"                # concurrent fetch units; set from mc_speed.json's
                                       # recommended_fetch_workers (S3 stream scaling). MC: 500 req/min.
MAX_BYTES="${MAX_BYTES:-0}"            # 0 = uncapped per-file download (the disk valve is the bound)
MAX_MEMBER_BYTES="${MAX_MEMBER_BYTES:-0}"   # 0 = uncapped extracted member (the valve is the bomb guard)
# Staging budget for the WHOLE MC raw dir (both concurrently-staged parts + deferred big primaries).
# Sized for a DEDICATED ~800 GB / ~900k-inode slice of /rds (no other job running): ~85% of each,
# leaving room for the MC dataset + manifests (a few GB, not valve-tracked) and Lustre overhead.
# Charged per byte/inode as written and refunded on delete, so `staged <= this` holds exactly.
# If other jobs share the quota again, lower both to what `quota` shows free.
MAX_DISK_BYTES="${MAX_DISK_BYTES:-680000000000}"
MAX_DISK_FILES="${MAX_DISK_FILES:-765000}"
# Parse: many small calcs (e.g. ~8k Bosoni EOS points) are PARSE-THROUGHPUT-bound -> parallel
# workers; a few huge AIMD vaspruns are RAM-bound -> deferred above the cap to 30_bigparse.sh.
# RAM guard is on the UNCOMPRESSED primary size (pymatgen peaks ~10-12x). A deferred
# (primary_too_large) calc stays staged; purge-raw never deletes it.
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-2500000000}"
PARSE_WORKERS="${PARSE_WORKERS:-3}"
PARSE_TIMEOUT="${PARSE_TIMEOUT:-1800}" # hard-kill a single non-terminating parse after 30 min
MAX_ATTEMPTS="${MAX_ATTEMPTS:-4}"
ATTEMPT="${ATTEMPT:-1}"
# --------------------------------------------------------------------------------

mkdir -p logs_mc
MAN="$MC_HARVEST_DATA/manifests"
IN="${IN:-$MAN/mc_keep.jsonl}"
RAW_DIR="${RAW_DIR:-$MC_HARVEST_DATA/raw}"
DATASET_DIR="${DATASET_DIR:-$MC_HARVEST_DATA/dataset}"
if [[ ! -s "$IN" ]]; then
    echo "ERROR: keep-list $IN missing — run scripts/csd3/materials_cloud/10_discover.sh first." >&2
    exit 2
fi

echo "=== mc pipeline attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="
quota 2>/dev/null || lfs quota -u "$USER" "$MC_HARVEST_DATA" 2>/dev/null || true

# The RESUBMIT chain is strictly sequential (afterany), so a .parse.lock present at startup can only
# be a SIGKILLed predecessor's (untrappable) lock on another node — provably stale. Do NOT run a
# separate parse against this SAME dataset dir concurrently.
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock (sequential resubmit chain => stale): $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

SUMMARY="logs_mc/mc-pipeline-${SLURM_JOB_ID:-local}.summary.json"
NEXT_JOBID=""
submit_successor() {
    if [[ "${RESUBMIT:-0}" != "0" && -z "$NEXT_JOBID" && "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]]; then
        echo "=== queueing resume job (attempt $((ATTEMPT + 1))/$MAX_ATTEMPTS) $(date -Is) ==="
        NEXT_JOBID=$(sbatch --parsable --dependency="afterany:${SLURM_JOB_ID}" \
            --export="ALL,ATTEMPT=$((ATTEMPT + 1)),RESUBMIT=1" "$0") || NEXT_JOBID=""
        echo "  -> successor job: ${NEXT_JOBID:-<sbatch failed; resubmit by hand>}"
    fi
}
trap 'submit_successor' USR1

rc=0
python -m materials_cloud_harvest.cli -v pipeline \
    --in "$IN" --parts "$PARTS" --workers "$WORKERS" \
    --max-bytes "$MAX_BYTES" --max-member-bytes "$MAX_MEMBER_BYTES" \
    --max-disk-bytes "$MAX_DISK_BYTES" --max-disk-files "$MAX_DISK_FILES" \
    --max-primary-bytes "$MAX_PRIMARY_BYTES" --parse-workers "$PARSE_WORKERS" \
    --parse-timeout "$PARSE_TIMEOUT" \
    --raw-dir "$RAW_DIR" --dataset-dir "$DATASET_DIR" \
    > >(tee "$SUMMARY") &
PIPELINE_PID=$!
# `wait` must be a plain statement under `set +e` (a trap-interrupted wait in a condition would
# wrongly return 0; a non-zero wait would abort under set -e) — see scripts/csd3/20_pipeline.sh.
set +e
while true; do
    wait "$PIPELINE_PID"; rc=$?
    if [[ "$rc" -le 128 ]] || ! kill -0 "$PIPELINE_PID" 2>/dev/null; then break; fi
done
set -e

echo "=== pipeline exit=$rc $(date -Is) ==="
echo "staged files under raw/: $(find "$RAW_DIR" -type f 2>/dev/null | wc -l)"
if [[ "$rc" -ne 0 ]]; then
    submit_successor
elif [[ -n "$NEXT_JOBID" ]]; then
    echo "harvest finished cleanly; cancelling pre-queued successor $NEXT_JOBID"
    scancel "$NEXT_JOBID" 2>/dev/null || true
fi
exit "$rc"

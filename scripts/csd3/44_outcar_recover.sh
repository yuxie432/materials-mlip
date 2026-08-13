#!/bin/bash
#SBATCH -J zh-outcar-recover
# Account is NOT hardcoded (keeps per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # the OUTCAR-header parse is light, but keep RAM headroom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH --signal=B:USR1@600            # SIGUSR1 ~10 min before wallclock -> queue a resume job
#SBATCH -o logs/zh-outcar-recover-%j.out
#SBATCH -e logs/zh-outcar-recover-%j.err
#SBATCH --mail-type=END,FAIL
#
# Targeted OUTCAR metadata recovery (docs: zenodo_harvest/outcar_recover.py). The first
# harvest left ~93k OUTCAR-parsed calcs with run_type/functional/INCAR = null (ASE reads the
# trajectory, not the header). This re-fetches ONLY the records that still hold an
# OUTCAR-parsed calc, re-parses each OUTCAR HEADER, and overwrites ONLY those records'
# calc_parameters in metadata.jsonl. The extxyz shards are NEVER touched (the physical data is
# already correct), so this is far cheaper than a re-parse — and targeted ZIP fetch pulls just
# the OUTCAR out of a .zip over HTTP Range instead of downloading the whole archive.
#
# One round = fetch(paced) -> refresh-outcar(--only-missing) -> purge-raw. The round is
# resubmitted (wallclock via the USR1 trap, or when OUTCAR calcs remain) until every OUTCAR
# calc has a functional; then it runs enrich-metadata + verify once and stops.
#
#   export SBATCH_ACCOUNT=<...>; source <path>/.venv/bin/activate; mkdir -p logs
#   RESUBMIT=1 sbatch scripts/csd3/44_outcar_recover.sh
# RESUBMIT is an ON/OFF switch (MAX_ATTEMPTS bounds the rounds). Everything is resumable +
# idempotent, so a mid-round kill loses no work.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   # BEFORE sbatch
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# ---- PARAMETERS -----------------------------------------------------------------
WORKERS="${WORKERS:-4}"                                # concurrent downloads (Zenodo 100 req/min)
MAX_DISK_BYTES="${MAX_DISK_BYTES:-800000000000}"       # ~0.8 x 1 TB hpc-work quota
MAX_DISK_FILES="${MAX_DISK_FILES:-800000}"             # inode quota binds first on Lustre
MAX_MEMBER_BYTES="${MAX_MEMBER_BYTES:-30000000000}"    # bomb guard on the whole-download fallback
MAX_ATTEMPTS="${MAX_ATTEMPTS:-8}"
ATTEMPT="${ATTEMPT:-1}"
# --------------------------------------------------------------------------------

mkdir -p logs
MAN="$ZENODO_HARVEST_DATA/manifests"
DS="$ZENODO_HARVEST_DATA/dataset"
RAW="$ZENODO_HARVEST_DATA/raw"
KEEP="${KEEP:-$MAN/keep.jsonl}"                        # the ORIGINAL keep-list (has file URLs)
OUTCAR_KEEP="$MAN/outcar_keep.jsonl"
OUTCAR_FETCHED="$MAN/outcar_fetched.jsonl"

if [[ ! -s "$DS/metadata.jsonl" ]]; then echo "ERROR: $DS/metadata.jsonl missing" >&2; exit 2; fi
if [[ ! -s "$KEEP" ]]; then echo "ERROR: $KEEP missing (need the original keep-list's file URLs)" >&2; exit 2; fi

echo "=== outcar-recover attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="

# A refresh/enrich SIGKILLed at the previous wallclock cannot release the dataset .parse.lock,
# and the successor usually lands on a different node (cross-node liveness uncheckable), so a
# lock present at startup of this STRICTLY SEQUENTIAL chain is provably stale — clear it.
if [[ -e "$DS/.parse.lock" ]]; then
    echo "clearing leftover parse lock (sequential chain => stale): $(cat "$DS/.parse.lock" 2>/dev/null)"
    rm -f "$DS/.parse.lock"
fi

# Stage 0: build the OUTCAR-only keep-list (idempotent; recomputed each round is cheap).
python -m zenodo_harvest.cli outcar-keeplist --dataset-dir "$DS" --keep "$KEEP" --out "$OUTCAR_KEEP"

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

# One round of work, in the background so the USR1 trap can fire mid-round (see 20_pipeline.sh).
run_round() {
    # Re-fetch (paced): targeted ZIP member fetch is ON by default, so a .zip record transfers
    # only its OUTCAR files. Archives are deleted right after extraction; the persistent
    # footprint is the extracted OUTCARs. Resumable: recids already in $OUTCAR_FETCHED skip.
    python -m zenodo_harvest.cli -v fetch --in "$OUTCAR_KEEP" --out "$OUTCAR_FETCHED" \
        --raw-dir "$RAW" --max-bytes 0 --max-member-bytes "$MAX_MEMBER_BYTES" \
        --max-disk-bytes "$MAX_DISK_BYTES" --max-disk-files "$MAX_DISK_FILES" --workers "$WORKERS"
    # Refresh metadata from whatever is staged. --only-missing skips calcs already refreshed in
    # a prior round (whose raw purge-raw has since reclaimed), so it never re-reads a purged file.
    python -m zenodo_harvest.cli refresh-outcar --dataset-dir "$DS" \
        --fetched "$OUTCAR_FETCHED" --raw-dir "$RAW" --only-missing
    # Reclaim the staged OUTCARs (their calcs are all in the dataset) so the next round's fetch
    # has room under the disk valve.
    python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" --fetched "$OUTCAR_FETCHED"
}

rc=0
run_round &
ROUND_PID=$!
set +e
while true; do
    wait "$ROUND_PID"; rc=$?
    if [[ "$rc" -le 128 ]] || ! kill -0 "$ROUND_PID" 2>/dev/null; then break; fi
done
set -e
echo "=== round exit=$rc $(date -Is) ==="

# Completion check: how many OUTCAR calcs still lack a functional (run_type null)?
REMAINING=$(python - "$DS/metadata.jsonl" <<'PY'
import json, sys
n = 0
for line in open(sys.argv[1]):
    r = json.loads(line)
    if r.get("parser") == "ase.OUTCAR" and (r.get("calc_parameters") or {}).get("run_type") is None:
        n += 1
print(n)
PY
)
echo "OUTCAR calcs still missing a functional: $REMAINING"

# Finalize when the recovery is DONE (REMAINING==0) OR a CLEAN round made NO progress
# (REMAINING unchanged from the previous round) — the latter means the leftover calcs are
# unrecoverable (recid missing from keep.jsonl, an unsupported/oversized archive, or a
# corrupt/unreadable OUTCAR header) and will never decrease, so the chain must stop cleanly
# instead of burning all MAX_ATTEMPTS. A round cut short by wallclock/USR1 (rc!=0) NEVER
# finalizes: its successor keeps going. PREV persists across the resubmit chain in $MAN.
PREV_FILE="$MAN/.outcar_remaining.prev"
PREV="$(cat "$PREV_FILE" 2>/dev/null || echo -1)"
echo "$REMAINING" > "$PREV_FILE"
if [[ "$rc" -eq 0 && ( "$REMAINING" -eq 0 || "$REMAINING" == "$PREV" ) ]]; then
    if [[ "$REMAINING" -ne 0 ]]; then
        echo "WARNING: $REMAINING OUTCAR calc(s) still null after a no-progress round — likely"
        echo "  unrecoverable (missing from keep / unsupported archive / unreadable header)."
        echo "  Finalizing anyway. Inspect: parser=='ase.OUTCAR' and calc_parameters.run_type null."
    fi
    echo "=== finalizing: enrich-metadata + verify $(date -Is) ==="
    python -m zenodo_harvest.cli enrich-metadata --dataset-dir "$DS"
    python -m zenodo_harvest.cli verify --dataset-dir "$DS"
    rm -f "$PREV_FILE"
    if [[ -n "$NEXT_JOBID" ]]; then
        echo "done; cancelling pre-queued successor $NEXT_JOBID"
        scancel "$NEXT_JOBID" 2>/dev/null || true
    fi
else
    # More to do (disk valve paused the fetch, or wallclock, or a transient error): chain on.
    submit_successor
fi
exit "$rc"

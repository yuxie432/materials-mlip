#!/bin/bash
#SBATCH -J zh-vasprun-recover
# Account is NOT hardcoded (keeps per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # the vasprun-HEADER parse is light, but keep RAM headroom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH --signal=B:USR1@600            # SIGUSR1 ~10 min before wallclock -> queue a resume job
#SBATCH -o logs/zh-vasprun-recover-%j.out
#SBATCH -e logs/zh-vasprun-recover-%j.err
#SBATCH --mail-type=END,FAIL
#
# Targeted vasprun `parameters` recovery (docs: zenodo_harvest/vasprun_recover.py). The first
# harvest parsed vasprun.xml BEFORE parse._calc_parameters stored VASP's RESOLVED `parameters`
# block, so the ~83k pymatgen.Vasprun calcs carry only the user INCAR — tags left at default are
# absent, and integer tags with no stable default (ISIF) are unrecoverable by the resolver. This
# re-fetches ONLY the records that hold a Vasprun calc, re-reads each vasprun HEADER (the
# <parameters> element, before the ionic trajectory -> cheap regardless of file size), and writes
# the authoritative `parameters` block into ONLY those records' calc_parameters. The extxyz
# shards are NEVER touched and run_type/functional/incar/... are left byte-identical (all already
# pymatgen-authoritative); only the missing `parameters` block is added and the stale `resolved`
# cache dropped. Targeted ZIP fetch pulls just the vasprun.xml out of a .zip; .tar falls back to a
# whole download (no tail index) and dominates transfer time.
#
# It runs a BATCHED loop of fetch(BATCH records) -> refresh-vasprun(--only-missing) -> purge-raw,
# so the missing-parameters count drops every batch (visible in this .out) and staging stays tiny.
# The loop ends when no Vasprun calc lacks a `parameters` block (or a small unrecoverable floor is
# reached), then runs enrich-metadata + verify ONCE (regenerating `resolved` from the now
# authoritative parameters, incl. OUTCAR calcs' resolver-filled tags like ADDGRID). A wallclock
# kill just resumes in the successor job.
#
# Staging is ISOLATED in a DEDICATED raw dir ($RAW below, default raw_vasprun) and rejections in a
# SEPARATE $MAN/vasprun_rejections.jsonl, so the main harvest's raw/ + the OUTCAR campaign's
# raw_outcar/ and their rejection logs are left untouched.
#
# NOTE: enrich-metadata is run only in the FINAL finalize step (once all parameters are in) — do
# NOT run it before/between jobs; the recovery drops the stale `resolved` cache and the finalize
# regenerates it authoritatively. If a job is killed before finalize, just let the successor run
# (or run enrich-metadata + verify by hand once remaining_missing has reached its floor).
#
#   export SBATCH_ACCOUNT=<...>; source <path>/.venv/bin/activate; mkdir -p logs
#   RESUBMIT=1 sbatch scripts/csd3/45_vasprun_recover.sh
# RESUBMIT is an ON/OFF switch (MAX_ATTEMPTS bounds the rounds). Everything is resumable +
# idempotent, so a mid-batch kill loses no work.
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
BATCH="${BATCH:-20}"                                   # records per fetch->refresh->purge cycle
                                                       # (smaller = more frequent progress + tinier staging)
MAX_DISK_BYTES="${MAX_DISK_BYTES:-800000000000}"       # ~0.8 x 1 TB hpc-work quota
MAX_DISK_FILES="${MAX_DISK_FILES:-800000}"             # inode quota binds first on Lustre
MAX_MEMBER_BYTES="${MAX_MEMBER_BYTES:-30000000000}"    # bomb guard on the whole-download fallback
MAX_ATTEMPTS="${MAX_ATTEMPTS:-8}"
ATTEMPT="${ATTEMPT:-1}"
# --------------------------------------------------------------------------------

mkdir -p logs
MAN="$ZENODO_HARVEST_DATA/manifests"
DS="$ZENODO_HARVEST_DATA/dataset"
# DEDICATED raw dir (NOT the main harvest's raw, NOR the OUTCAR campaign's raw_outcar): keeps this
# recovery's staging fully isolated, so both are never fetched into, walked, or purged, and the
# recovery starts against an empty inode/byte budget.
RAW="${RAW_DIR:-$ZENODO_HARVEST_DATA/raw_vasprun}"
KEEP="${KEEP:-$MAN/keep.jsonl}"                        # the ORIGINAL keep-list (has file URLs)
VASPRUN_KEEP="$MAN/vasprun_keep.jsonl"
VASPRUN_FETCHED="$MAN/vasprun_fetched.jsonl"           # recovery resume state (recids fetched)

if [[ ! -s "$DS/metadata.jsonl" ]]; then echo "ERROR: $DS/metadata.jsonl missing" >&2; exit 2; fi
if [[ ! -s "$KEEP" ]]; then echo "ERROR: $KEEP missing (need the original keep-list's file URLs)" >&2; exit 2; fi

echo "=== vasprun-recover attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="

# A refresh/enrich SIGKILLed at the previous wallclock cannot release the dataset .parse.lock, and
# the successor usually lands on a different node (cross-node liveness uncheckable), so a lock
# present at startup of this STRICTLY SEQUENTIAL chain is provably stale — clear it.
if [[ -e "$DS/.parse.lock" ]]; then
    echo "clearing leftover parse lock (sequential chain => stale): $(cat "$DS/.parse.lock" 2>/dev/null)"
    rm -f "$DS/.parse.lock"
fi

# Stage 0: build the vasprun-only keep-list fresh each round (truncate first so re-runs do not
# accumulate duplicate lines — JsonlWriter appends). TOTAL is the true unique-record target.
rm -f "$VASPRUN_KEEP"
python -m zenodo_harvest.cli vasprun-keeplist --dataset-dir "$DS" --keep "$KEEP" --out "$VASPRUN_KEEP"
TOTAL=$(sort -u "$VASPRUN_KEEP" | wc -l)
echo "vasprun keep-list: $TOTAL unique records to re-fetch"

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

# Vasprun calcs still lacking a resolved `parameters` block — the live progress metric.
count_remaining() {
    python - "$DS/metadata.jsonl" <<'PY'
import json, sys
print(sum(1 for l in open(sys.argv[1]) if (r := json.loads(l)).get("parser") == "pymatgen.Vasprun"
          and not (r.get("calc_parameters") or {}).get("parameters")))
PY
}

# BATCHED work loop: fetch a SMALL batch of records, add their parameters block, purge their raw,
# repeat — so the missing count drops every batch and staging never grows beyond one batch.
# Refresh/purge run ONLY when the batch fetched something new. Termination: no Vasprun calc left
# without a parameters block (done), or 3 consecutive batches fetched nothing new (the
# unrecoverable floor: recid missing from keep.jsonl / unsupported-or-oversized archive / a
# corrupt-unreadable vasprun.xml). A wallclock SIGKILL just ends the loop mid-batch; the successor
# resumes it (fetch skips done recids, refresh --only-missing skips done calcs).
work_loop() {
    local dry=0 before after remaining
    while :; do
        before=$([[ -f "$VASPRUN_FETCHED" ]] && wc -l < "$VASPRUN_FETCHED" || echo 0)
        # Targeted ZIP fetch is ON by default (a .zip transfers only its vasprun.xml). --max-records
        # bounds the batch; fetch is resume-aware (recids already in $VASPRUN_FETCHED skip).
        python -m zenodo_harvest.cli -v fetch --in "$VASPRUN_KEEP" --out "$VASPRUN_FETCHED" \
            --raw-dir "$RAW" --rejections "$MAN/vasprun_rejections.jsonl" \
            --max-bytes 0 --max-member-bytes "$MAX_MEMBER_BYTES" \
            --max-disk-bytes "$MAX_DISK_BYTES" --max-disk-files "$MAX_DISK_FILES" \
            --workers "$WORKERS" --max-records "$BATCH"
        after=$([[ -f "$VASPRUN_FETCHED" ]] && wc -l < "$VASPRUN_FETCHED" || echo 0)
        if [[ "$after" -gt "$before" ]]; then
            # Add ONLY these Vasprun calcs' `parameters` block (--only-missing skips ones a prior
            # batch already did and whose raw is now purged), then reclaim their raw.
            python -m zenodo_harvest.cli refresh-vasprun --dataset-dir "$DS" \
                --fetched "$VASPRUN_FETCHED" --raw-dir "$RAW" --only-missing
            python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" \
                --fetched "$VASPRUN_FETCHED"
            dry=0
        else
            dry=$((dry + 1))
        fi
        remaining=$(count_remaining)
        echo "[batch] $(date -Is) fetched=${after}/${TOTAL} remaining_missing=${remaining} dry=${dry}"
        [[ "$remaining" -eq 0 ]] && return 0
        [[ "$dry" -ge 3 ]] && return 0
    done
}

rc=0
work_loop &
LOOP_PID=$!
set +e
while true; do
    wait "$LOOP_PID"; rc=$?
    if [[ "$rc" -le 128 ]] || ! kill -0 "$LOOP_PID" 2>/dev/null; then break; fi
done
set -e
echo "=== work loop exit=$rc $(date -Is) ==="

remaining=$(count_remaining)
if [[ "$rc" -eq 0 ]]; then
    # Loop returned cleanly => done or the unrecoverable floor reached. Finalize once.
    if [[ "$remaining" -ne 0 ]]; then
        echo "WARNING: $remaining Vasprun calc(s) still lack a parameters block — unrecoverable"
        echo "  (recid missing from keep.jsonl / unsupported-or-oversized archive / corrupt vasprun.xml)."
        echo "  Finalizing anyway. Inspect: parser=='pymatgen.Vasprun' and no calc_parameters.parameters."
    fi
    echo "=== finalizing: enrich-metadata + verify $(date -Is) ==="
    python -m zenodo_harvest.cli enrich-metadata --dataset-dir "$DS"
    python -m zenodo_harvest.cli verify --dataset-dir "$DS"
    if [[ -n "$NEXT_JOBID" ]]; then
        echo "done; cancelling pre-queued successor $NEXT_JOBID"
        scancel "$NEXT_JOBID" 2>/dev/null || true
    fi
else
    # Wallclock SIGKILL (or a hard error) mid-batch: the successor resumes the loop.
    submit_successor
fi
exit "$rc"

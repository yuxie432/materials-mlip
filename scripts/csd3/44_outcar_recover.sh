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
# It runs a BATCHED loop of fetch(BATCH records) -> refresh-outcar(--only-missing) -> purge-raw,
# so the null-count drops every batch (visible in this .out) and staging stays tiny. The loop
# ends when no OUTCAR calc is left null (or a small unrecoverable floor is reached), then runs
# enrich-metadata + verify once. A wallclock kill just resumes in the successor job.
#
# Staging is ISOLATED in a DEDICATED raw dir ($RAW below, default raw_outcar) and rejections in
# a SEPARATE $MAN/outcar_rejections.jsonl — so the main harvest's raw/ and rejections.jsonl are
# left untouched (keep them for a later skipped/error-record campaign).
#
# NOTE: records whose OUTCAR sits inside a .rar/.7z/.tar.zst need the `archives` extra AND an
# `unrar`/`bsdtar` (rar) binary on PATH — otherwise those extract as a logged rejection and
# those few calcs won't recover. Install: `pip install -e .[archives]` + `module load` (or a
# conda) that provides unrar; check with `python -c "import rarfile,py7zr,zstandard"` and
# `which unrar bsdtar`.
#
#   export SBATCH_ACCOUNT=<...>; source <path>/.venv/bin/activate; mkdir -p logs
#   RESUBMIT=1 sbatch scripts/csd3/44_outcar_recover.sh
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
BATCH="${BATCH:-25}"                                   # records per fetch->refresh->purge cycle
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
# DEDICATED raw dir (NOT the main harvest's raw): keeps the recovery's staging fully isolated,
# so the main `raw/` — which you may keep for a later skipped/error-record campaign — is never
# fetched into, walked, or purged, and the recovery starts against an empty inode/byte budget.
RAW="${RAW_DIR:-$ZENODO_HARVEST_DATA/raw_outcar}"
KEEP="${KEEP:-$MAN/keep.jsonl}"                        # the ORIGINAL keep-list (has file URLs)
OUTCAR_KEEP="$MAN/outcar_keep.jsonl"
OUTCAR_FETCHED="$MAN/outcar_fetched.jsonl"             # recovery resume state (recids fetched)

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
count_remaining() {
    # OUTCAR calcs still lacking a functional (run_type null) — the live progress metric.
    python - "$DS/metadata.jsonl" <<'PY'
import json, sys
print(sum(1 for l in open(sys.argv[1]) if (r := json.loads(l)).get("parser") == "ase.OUTCAR"
          and (r.get("calc_parameters") or {}).get("run_type") is None))
PY
}

# BATCHED work loop: fetch a SMALL batch of records, refresh their metadata, purge their raw,
# repeat — so the null count drops every batch (visible in this .out) and staging never grows
# beyond one batch (the disk/inode valve is barely touched; each batch's raw walk stays cheap).
# Refresh/purge run ONLY when the batch fetched something new, so idle passes are cheap.
# Termination: no OUTCAR calc left null (done), or 3 consecutive batches fetched nothing new
# (everything left is already fetched or terminally rejected -> the unrecoverable floor). A
# wallclock SIGKILL just ends the loop mid-batch; the successor resumes it (fetch skips done
# recids, refresh --only-missing skips done calcs).
work_loop() {
    local dry=0 before after remaining
    while :; do
        before=$(wc -l < "$OUTCAR_FETCHED" 2>/dev/null || echo 0)
        # Targeted ZIP fetch is ON by default (a .zip transfers only its OUTCARs). --max-records
        # bounds the batch; fetch is resume-aware (recids already in $OUTCAR_FETCHED skip).
        python -m zenodo_harvest.cli -v fetch --in "$OUTCAR_KEEP" --out "$OUTCAR_FETCHED" \
            --raw-dir "$RAW" --rejections "$MAN/outcar_rejections.jsonl" \
            --max-bytes 0 --max-member-bytes "$MAX_MEMBER_BYTES" \
            --max-disk-bytes "$MAX_DISK_BYTES" --max-disk-files "$MAX_DISK_FILES" \
            --workers "$WORKERS" --max-records "$BATCH"
        after=$(wc -l < "$OUTCAR_FETCHED" 2>/dev/null || echo 0)
        if [[ "$after" -gt "$before" ]]; then
            # Overwrite ONLY these OUTCAR calcs' calc_parameters (--only-missing skips ones a
            # prior batch already did and whose raw is now purged), then reclaim their raw.
            python -m zenodo_harvest.cli refresh-outcar --dataset-dir "$DS" \
                --fetched "$OUTCAR_FETCHED" --raw-dir "$RAW" --only-missing
            python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" \
                --fetched "$OUTCAR_FETCHED"
            dry=0
        else
            dry=$((dry + 1))
        fi
        remaining=$(count_remaining)
        echo "[batch] $(date -Is) fetched=${after}/230 remaining_null=${remaining} dry=${dry}"
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
        echo "WARNING: $remaining OUTCAR calc(s) still null — unrecoverable (recid missing from"
        echo "  keep.jsonl / unsupported-or-oversized archive / corrupt-unreadable OUTCAR header)."
        echo "  Finalizing anyway. Inspect: parser=='ase.OUTCAR' and calc_parameters.run_type null."
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

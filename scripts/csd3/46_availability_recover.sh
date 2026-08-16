#!/bin/bash
#SBATCH -J zh-availability-recover
# Account is NOT hardcoded (keeps per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # the embedded probe is a light streaming scan; RAM headroom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH --signal=B:USR1@600            # SIGUSR1 ~10 min before wallclock -> queue a resume job
#SBATCH -o logs/zh-availability-recover-%j.out
#SBATCH -e logs/zh-availability-recover-%j.err
#SBATCH --mail-type=END,FAIL
#
# Targeted per-calc `availability` recovery (docs: zenodo_harvest/availability_recover.py). The
# first harvest recorded availability PER RECORD and from FILENAMES ONLY, so heavy-file flags were
# OR'd across every calc in a record (over-count: one DOSCAR anywhere flagged all its calcs) and
# DOS/eigenvalues/projected embedded IN a vasprun.xml with no standalone DOSCAR/EIGENVAL/PROCAR
# file were flagged False (under-count). The fetch+parse code now scopes availability to each
# calc-unit directory AND probes the vasprun/vaspout content. This applies the corrected
# availability to the ALREADY-BUILT dataset WITHOUT rebuilding shards: it re-fetches every record
# (targeted ZIP member fetch pulls just the vasprun.xml + the central-directory LISTING out of a
# .zip; the ~88 .tar records fall back to a whole download and dominate transfer time), then
# recomputes each calc's availability (per-calc filename flags from the listing UNION the embedded
# vasprun/vaspout probe, plus spin_density/magnetization re-derived from the record's own
# spin_polarized/site_magmoms_present) and overwrites ONLY that record's `availability`. The extxyz
# shards are NEVER touched and calc_id/frame_ids/shards/calc_parameters stay byte-identical, so
# `verify` still passes.
#
# It runs a BATCHED loop of fetch(BATCH records) -> refresh-availability -> purge-raw, so staging
# stays tiny (one batch) and progress (fetched/TOTAL) is visible in this .out. refresh-availability
# only touches calcs whose primary is STILL STAGED, so a purged (already-done) batch is skipped and
# a wallclock kill just resumes in the successor job — no metadata marker needed, presence of the
# source file IS the resume state. Finalizes with `verify` ONCE at the end (no enrich-metadata —
# availability does not feed the resolver).
#
# Staging is ISOLATED in a DEDICATED raw dir ($RAW below, default raw_availability) and rejections
# in a SEPARATE $MAN/availability_rejections.jsonl, so the main harvest's raw/ and the OUTCAR/
# vasprun campaigns' raw_outcar/ + raw_vasprun/ and their rejection logs are left untouched.
#
# A one-time backup of metadata.jsonl is taken BEFORE any refresh (belt-and-braces on top of the
# refresh's own internal metadata.jsonl.bak.pre_availability_refresh snapshot).
#
#   export SBATCH_ACCOUNT=<...>; source <path>/.venv/bin/activate; mkdir -p logs
#   RESUBMIT=1 sbatch scripts/csd3/46_availability_recover.sh
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
# DEDICATED raw dir (NOT the main harvest's raw, NOR the OUTCAR/vasprun campaigns' raw_*): keeps
# this recovery's staging fully isolated, so none are fetched into, walked, or purged, and the
# recovery starts against an empty inode/byte budget.
RAW="${RAW_DIR:-$ZENODO_HARVEST_DATA/raw_availability}"
KEEP="${KEEP:-$MAN/keep.jsonl}"                        # the ORIGINAL keep-list (has file URLs)
AVAIL_KEEP="$MAN/availability_keep.jsonl"
AVAIL_FETCHED="$MAN/availability_fetched.jsonl"        # recovery resume state (recids fetched)
JOB_BACKUP="$DS/metadata.jsonl.bak.pre_availability_job"

if [[ ! -s "$DS/metadata.jsonl" ]]; then echo "ERROR: $DS/metadata.jsonl missing" >&2; exit 2; fi
if [[ ! -s "$KEEP" ]]; then echo "ERROR: $KEEP missing (need the original keep-list's file URLs)" >&2; exit 2; fi

echo "=== availability-recover attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="

# Explicit one-time backup of the CURRENT metadata BEFORE any refresh (in addition to the
# refresh's own metadata.jsonl.bak.pre_availability_refresh). Only the first attempt writes it, so
# a resubmit never overwrites the true pre-job state.
if [[ ! -e "$JOB_BACKUP" ]]; then
    cp -p "$DS/metadata.jsonl" "$JOB_BACKUP"
    echo "backed up current metadata -> $JOB_BACKUP ($(wc -l < "$JOB_BACKUP") records)"
else
    echo "pre-job metadata backup already present: $JOB_BACKUP"
fi

# A refresh/verify SIGKILLed at the previous wallclock cannot release the dataset .parse.lock, and
# the successor usually lands on a different node (cross-node liveness uncheckable), so a lock
# present at startup of this STRICTLY SEQUENTIAL chain is provably stale — clear it.
if [[ -e "$DS/.parse.lock" ]]; then
    echo "clearing leftover parse lock (sequential chain => stale): $(cat "$DS/.parse.lock" 2>/dev/null)"
    rm -f "$DS/.parse.lock"
fi

# Stage 0: build the availability keep-list fresh each round (truncate first so re-runs do not
# accumulate duplicate lines — JsonlWriter appends). TOTAL is the true unique-record target.
rm -f "$AVAIL_KEEP"
python -m zenodo_harvest.cli availability-keeplist --dataset-dir "$DS" --keep "$KEEP" --out "$AVAIL_KEEP"
TOTAL=$(sort -u "$AVAIL_KEEP" | wc -l)
echo "availability keep-list: $TOTAL unique records to re-fetch"

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

# Whether the dedicated raw dir currently holds any staged record dir (=> unrefreshed files
# present, e.g. a batch fetched-but-not-yet-refreshed before a kill). Used to run refresh+purge
# even on a round that fetched nothing new, so the interrupted tail is never left unrefreshed.
raw_has_staged() {
    [[ -n "$(find "$RAW" -mindepth 1 -maxdepth 1 -type d -print -quit 2>/dev/null)" ]]
}

# BATCHED work loop: fetch a SMALL batch of records, recompute their availability, purge their raw,
# repeat — so staging never grows beyond one batch and fetched/TOTAL climbs every batch (visible
# in this .out). refresh-availability runs whenever anything is staged (new fetch OR a resumed
# unrefreshed batch) and only touches calcs whose primary is still on disk. Termination: every
# keep-list record fetched (=> every calc refreshed), or 3 consecutive batches fetched nothing new
# (the unrecoverable floor: recid missing from keep.jsonl / unsupported-or-oversized archive / a
# corrupt-unreadable archive). A wallclock SIGKILL just ends the loop mid-batch; the successor
# resumes it (fetch skips done recids; refresh skips purged calcs by the present-file filter).
work_loop() {
    local dry=0 before after
    while :; do
        before=$([[ -f "$AVAIL_FETCHED" ]] && wc -l < "$AVAIL_FETCHED" || echo 0)
        # Targeted ZIP fetch is ON by default (a .zip transfers only its vasprun.xml + listing).
        # --max-records bounds the batch; fetch is resume-aware (recids already in
        # $AVAIL_FETCHED skip). --max-bytes 0 = no per-file cap (the disk valve is the bound).
        python -m zenodo_harvest.cli -v fetch --in "$AVAIL_KEEP" --out "$AVAIL_FETCHED" \
            --raw-dir "$RAW" --rejections "$MAN/availability_rejections.jsonl" \
            --max-bytes 0 --max-member-bytes "$MAX_MEMBER_BYTES" \
            --max-disk-bytes "$MAX_DISK_BYTES" --max-disk-files "$MAX_DISK_FILES" \
            --workers "$WORKERS" --max-records "$BATCH"
        after=$([[ -f "$AVAIL_FETCHED" ]] && wc -l < "$AVAIL_FETCHED" || echo 0)
        if raw_has_staged; then
            # Recompute availability for the staged calcs (filename flags ∪ embedded probe + spin),
            # overwriting ONLY their metadata `availability`, then reclaim their raw. Purged calcs
            # from earlier batches are skipped by refresh's present-file filter.
            python -m zenodo_harvest.cli refresh-availability --dataset-dir "$DS" \
                --fetched "$AVAIL_FETCHED" --raw-dir "$RAW"
            python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" \
                --fetched "$AVAIL_FETCHED"
        fi
        if [[ "$after" -gt "$before" ]]; then dry=0; else dry=$((dry + 1)); fi
        echo "[batch] $(date -Is) fetched=${after}/${TOTAL} dry=${dry}"
        [[ "$after" -ge "$TOTAL" ]] && return 0
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

if [[ "$rc" -eq 0 ]]; then
    # Loop returned cleanly => every record re-fetched+refreshed, or the unrecoverable floor
    # reached. Finalize once: verify the bijection still holds (availability is metadata-only,
    # so no shard was touched). No enrich-metadata — availability does not feed the resolver.
    fetched_now=$([[ -f "$AVAIL_FETCHED" ]] && wc -l < "$AVAIL_FETCHED" || echo 0)
    if [[ "$fetched_now" -lt "$TOTAL" ]]; then
        echo "WARNING: only $fetched_now/$TOTAL records re-fetched — the rest are unrecoverable"
        echo "  (recid missing from keep.jsonl / unsupported-or-oversized archive / corrupt archive)."
        echo "  Those calcs keep their prior availability. Inspect $MAN/availability_rejections.jsonl."
    fi
    echo "=== finalizing: verify $(date -Is) ==="
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

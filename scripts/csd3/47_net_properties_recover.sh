#!/bin/bash
#SBATCH -J zh-net-properties-recover
# Account is NOT hardcoded (keeps per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # compute is a light parse (occupancy method only when spin)
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00                # SL3 max; SL1/SL2 may use up to 36:00:00
#SBATCH --signal=B:USR1@600            # SIGUSR1 ~10 min before wallclock -> queue a resume job
#SBATCH -o logs/zh-net-properties-recover-%j.out
#SBATCH -e logs/zh-net-properties-recover-%j.err
#SBATCH --mail-type=END,FAIL
#
# Net moment/charge + OUTCAR SCF-convergence recovery (docs: zenodo_harvest/net_properties_recover.py).
# The first harvest stored neither the net moment nor the net charge, stored per-atom
# dft_magmom/dft_charge on the final frame of vasprun+OUTCAR calcs, AND left OUTCAR-parsed calcs
# without per-frame scf_dE/electronic_converged (the old OUTCAR path could not read the SCF trace).
# This retrofits the ALREADY-BUILT dataset to the new schema in ONE shard pass: every frame gains
# total_magnetization/total_charge (per-atom arrays stripped, metadata gains an `electronic` block,
# site_*_present dropped); AND every frame of an OUTCAR-parsed calc gains that ionic step's own
# scf_dE (free-energy basis) + electronic_converged from the OUTCAR trace, with the calc's metadata
# `quality` convergence fields overwritten (vasprun calcs already have σ→0 scf_dE, so untouched).
#
# UNLIKE the OUTCAR/vasprun/availability recoveries, this one REWRITES SHARDS (the values live on
# the frames). A shard interleaves frames from many calcs, so it cannot be rewritten until every one
# of its calcs' values is known — hence TWO PHASES:
#   Phase 1 (compute): a BATCHED fetch(BATCH records) -> compute-net-properties -> purge-raw loop,
#     re-fetching the primaries (targeted ZIP member fetch pulls just the vasprun.xml/OUTCAR out of a
#     .zip) and computing each calc's net moment/charge — plus, for OUTCAR calcs, its per-frame SCF
#     convergence — into a net_properties.jsonl MAP. No shard is touched; resumable via the map
#     (computed calc_ids skip). Occupancy-method eigen parse runs only for collinear spin-polarised
#     vasprun-without-OUTCAR calcs (cheap ISPIN pre-scan gates it). compute-net-properties needs
#     --dataset-dir to know each calc's parser (only ase.OUTCAR calcs get the convergence scan).
#   Phase 2 (apply): ONCE, after Phase 1 is COMPLETE — apply-net-properties drives the map into the
#     dataset (text-level shard edit: append the two totals + OUTCAR frames' scf_dE/electronic_converged
#     per frame, strip dft_* columns, leaving every untouched value byte-identical; then an atomic
#     metadata rewrite that ALSO backfills calc-level ionic_converged for OUTCAR calcs from each
#     record's own calc_parameters + n_ionic_steps — metadata-only, no re-fetch). Resumable via a
#     .net_properties_applied marker + idempotent metadata. `verify` still passes (frame_ids/shards/
#     calc_id untouched). (The ionic-convergence *magnitude*/last-two-frames ΔE is intentionally NOT
#     stored — not a per-frame MLIP-training label-quality signal; see zenodo_harvest/convergence.py.)
# Phase 2 runs ONLY when the Phase-1 loop returns cleanly (map complete), so it never rewrites a
# shard against a partial map. A wallclock kill mid-Phase-1 resumes the loop in the successor; a kill
# mid-Phase-2 resumes apply from the marker with the now-complete map.
#
# Staging is ISOLATED in a DEDICATED raw dir ($RAW, default raw_net_properties) and rejections in a
# SEPARATE $MAN/net_properties_rejections.jsonl, so the main harvest's raw/ and the other campaigns'
# raw_*/ and rejection logs are untouched. A one-time backup of metadata.jsonl is taken BEFORE Phase 2
# (belt-and-braces on top of apply's own metadata.jsonl.bak.pre_net_properties snapshot); the shards
# themselves are NOT backed up (11.87M frames), so trust the offline tests + the byte-identical
# idempotency guarantee, or snapshot the dataset dir yourself first if you want a rollback.
#
#   export SBATCH_ACCOUNT=<...>; source <path>/.venv/bin/activate; mkdir -p logs
#   RESUBMIT=1 sbatch scripts/csd3/47_net_properties_recover.sh
# RESUBMIT is an ON/OFF switch (MAX_ATTEMPTS bounds the rounds). Everything is resumable + idempotent.
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
BATCH="${BATCH:-20}"                                   # records per fetch->compute->purge cycle
MAX_DISK_BYTES="${MAX_DISK_BYTES:-800000000000}"       # ~0.8 x 1 TB hpc-work quota
MAX_DISK_FILES="${MAX_DISK_FILES:-800000}"             # inode quota binds first on Lustre
MAX_MEMBER_BYTES="${MAX_MEMBER_BYTES:-30000000000}"    # bomb guard on the whole-download fallback
MAX_ATTEMPTS="${MAX_ATTEMPTS:-8}"
ATTEMPT="${ATTEMPT:-1}"
# --------------------------------------------------------------------------------

mkdir -p logs
MAN="$ZENODO_HARVEST_DATA/manifests"
DS="$ZENODO_HARVEST_DATA/dataset"
RAW="${RAW_DIR:-$ZENODO_HARVEST_DATA/raw_net_properties}"   # DEDICATED raw dir (isolated staging)
KEEP="${KEEP:-$MAN/keep.jsonl}"                        # the ORIGINAL keep-list (has file URLs)
NP_KEEP="$MAN/net_properties_keep.jsonl"
NP_FETCHED="$MAN/net_properties_fetched.jsonl"         # fetch resume state (recids fetched)
NP_MAP="$MAN/net_properties.jsonl"                     # Phase-1 map (compute resume) + Phase-2 input
JOB_BACKUP="$DS/metadata.jsonl.bak.pre_net_properties_job"

if [[ ! -s "$DS/metadata.jsonl" ]]; then echo "ERROR: $DS/metadata.jsonl missing" >&2; exit 2; fi
if [[ ! -s "$KEEP" ]]; then echo "ERROR: $KEEP missing (need the original keep-list's file URLs)" >&2; exit 2; fi

echo "=== net-properties-recover attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is) on $(hostname) ==="

# A compute/apply/verify SIGKILLed at the previous wallclock cannot release the dataset .parse.lock,
# and the successor usually lands on a different node, so a lock present at startup of this STRICTLY
# SEQUENTIAL chain is provably stale — clear it. (Phase 1's compute takes no lock; only Phase 2 does.)
if [[ -e "$DS/.parse.lock" ]]; then
    echo "clearing leftover parse lock (sequential chain => stale): $(cat "$DS/.parse.lock" 2>/dev/null)"
    rm -f "$DS/.parse.lock"
fi

# Stage 0: build the keep-list fresh each round (truncate first — JsonlWriter appends).
rm -f "$NP_KEEP"
python -m zenodo_harvest.cli net-properties-keeplist --dataset-dir "$DS" --keep "$KEEP" --out "$NP_KEEP"
TOTAL=$(sort -u "$NP_KEEP" | wc -l)
echo "net-properties keep-list: $TOTAL unique records to re-fetch"

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

raw_has_staged() {
    [[ -n "$(find "$RAW" -mindepth 1 -maxdepth 1 -type d -print -quit 2>/dev/null)" ]]
}

# PHASE 1: batched fetch -> compute-net-properties -> purge. Same disk-paced loop as the
# availability recovery, but compute-net-properties APPENDS to the net_properties.jsonl map instead
# of touching metadata. Termination: every record fetched, or 3 consecutive dry batches (the
# unrecoverable floor). Resumable: fetch skips done recids; compute skips computed calc_ids.
work_loop() {
    local dry=0 before after
    while :; do
        before=$([[ -f "$NP_FETCHED" ]] && wc -l < "$NP_FETCHED" || echo 0)
        python -m zenodo_harvest.cli -v fetch --in "$NP_KEEP" --out "$NP_FETCHED" \
            --raw-dir "$RAW" --rejections "$MAN/net_properties_rejections.jsonl" \
            --max-bytes 0 --max-member-bytes "$MAX_MEMBER_BYTES" \
            --max-disk-bytes "$MAX_DISK_BYTES" --max-disk-files "$MAX_DISK_FILES" \
            --workers "$WORKERS" --max-records "$BATCH"
        after=$([[ -f "$NP_FETCHED" ]] && wc -l < "$NP_FETCHED" || echo 0)
        if raw_has_staged; then
            python -m zenodo_harvest.cli compute-net-properties --fetched "$NP_FETCHED" \
                --raw-dir "$RAW" --out "$NP_MAP" --dataset-dir "$DS"
            python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DS" \
                --fetched "$NP_FETCHED"
        fi
        if [[ "$after" -gt "$before" ]]; then dry=0; else dry=$((dry + 1)); fi
        echo "[batch] $(date -Is) fetched=${after}/${TOTAL} map=$([[ -f "$NP_MAP" ]] && wc -l < "$NP_MAP" || echo 0) dry=${dry}"
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
echo "=== Phase 1 (compute) exit=$rc $(date -Is) ==="

if [[ "$rc" -eq 0 ]]; then
    # Phase 1 complete (map stable) => run Phase 2 ONCE. Back up metadata first (shards are NOT
    # backed up; rely on the offline tests + byte-identical idempotency).
    fetched_now=$([[ -f "$NP_FETCHED" ]] && wc -l < "$NP_FETCHED" || echo 0)
    if [[ "$fetched_now" -lt "$TOTAL" ]]; then
        echo "WARNING: only $fetched_now/$TOTAL records re-fetched — the rest are unrecoverable"
        echo "  (recid missing from keep.jsonl / unsupported-or-oversized archive / corrupt archive)."
        echo "  Those calcs' frames get dft_* stripped but no totals. Inspect $MAN/net_properties_rejections.jsonl."
    fi
    if [[ ! -e "$JOB_BACKUP" ]]; then
        cp -p "$DS/metadata.jsonl" "$JOB_BACKUP"
        echo "backed up current metadata -> $JOB_BACKUP ($(wc -l < "$JOB_BACKUP") records)"
    fi
    echo "=== Phase 2 (apply — REWRITES SHARDS) $(date -Is) ==="
    python -m zenodo_harvest.cli apply-net-properties --dataset-dir "$DS" --net-properties "$NP_MAP"
    echo "=== finalizing: verify $(date -Is) ==="
    python -m zenodo_harvest.cli verify --dataset-dir "$DS"
    if [[ -n "$NEXT_JOBID" ]]; then
        echo "done; cancelling pre-queued successor $NEXT_JOBID"
        scancel "$NEXT_JOBID" 2>/dev/null || true
    fi
else
    # Wallclock SIGKILL (or a hard error) mid-Phase-1: the successor resumes the loop.
    submit_successor
fi
exit "$rc"

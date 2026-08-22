#!/bin/bash
#SBATCH -J nomad-pipeline
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # 6760 MiB/core: parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8              # cores drive --parse-workers (parallel parse). 8 = 6 workers +2
                                       # (fetch thread + forkserver + RAM headroom). Briefly tried 6/4
                                       # when only the ~5-6 MB/s daytime fetch had been measured, but
                                       # 2026-08-22 the OVERNIGHT fetch runs 10-14 MB/s (up to 17
                                       # entries/s) and at 4 workers parse ran level with the fastest
                                       # batch + peak_staged crept up — so back to 8/6 for margin.
                                       # Raise further only if staged_files_now stays PINNED at the
                                       # disk valve (=parse-bound); it is not today.
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
# The full 7.1M direct-upload set is fetched from each upload's PRE-PACKED zip
# (GET /uploads/{id}/raw — see docs/NOMAD_HARVEST.md §3), per upload choosing WHOLE-STREAM (one
# transfer-bound request) for low-bloat uploads or TARGETED multi-range for high-bloat ones. The
# endpoint is REQUEST-bound (~5 s/request, one connection per IP; CSD3 compute shares one NAT IP
# so it is intrinsically SERIAL, no --workers). The hybrid makes it a **~2-4 day** single
# self-resubmitting run (vs ~6-9 days all-targeted); an exemption would take it sub-day.
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
# Parse copies each OUTCAR into $TMPDIR before ASE reads it. Point TMPDIR at fast node-local
# scratch (/local, auto-removed at job end) so it neither eats the RAM budget nor the /rds quota.
# (The targeted fetch reads Range responses in memory / streams big members straight to raw/, so
# it needs no scratch.) Fall back to NOMAD-root scratch (NOT the Zenodo tree) if /local is absent.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$NOMAD_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# ---- HARVEST PARAMETERS ---------------------------------------------------------
PARTS="${PARTS:-160}"                  # batches; each part holds WHOLE uploads (split_by_upload).
                                       # Sized so a part is MUCH smaller than the inode valve, so it
                                       # fetches in one window AND leaves valve headroom for the next
                                       # part to fetch WHILE this one parses+purges (real overlap).
                                       # 7.1M/160 = ~44k entries = ~176k inodes << the 500k valve
                                       # (leaves ~324k for the concurrent next part). PARTS=40 gave
                                       # 708k-inode parts > the valve, so each was fetched in one
                                       # long valve-filling installment with NO concurrent parse
                                       # (the "parse frozen for hours" symptom). Changing PARTS is
                                       # data-safe (dedup by calc_id; every upload still assigned to
                                       # exactly one part) and, with fetch's global dataset-skip, does
                                       # NOT re-download already-parsed entries. Higher (e.g. 256) is
                                       # fine too; the only cost is a little more per-part overhead.
# DISK/INODE valve — bounds only THIS job's raw staging ($NOMAD_HARVEST_DATA/raw); it cannot see the
# Zenodo tree. **BOTH bytes AND inodes bind** — the earlier "inode-bound, ~0.26 MB/entry" note was
# wrong: measured 2026-08-22, peak staging hit 133 GB at 357k inodes (~1.5 MB/entry mean, up to
# ~2.4 MB/entry on byte-heavy overnight batches full of big AIMD/large-cell vaspruns), so at the old
# 200 GB byte ceiling there was only ~67 GB headroom. NOMAD now runs SOLO (no concurrent Zenodo
# recovery), so it takes the whole raw budget: **700 GB / 700k inodes** (~0.7x the 1 TB / 1M hpc-work
# quota — deliberately conservative, leaving ~300 GB / ~300k for the growing NOMAD dataset ~tens of GB
# + the Zenodo dataset + comfortable headroom). More headroom = more parts can co-stage before the
# valve trips = better fetch/parse overlap, which matters now that the overnight fetch is fast
# (~10-14 MB/s) and staging fills quicker. 700k inodes ~= ~4 parts (a part is ~176k inodes at PARTS=160).
# The valve charges every byte + inode as created and refunds on delete, so `staged <= limit` holds
# exactly; a trip just stops cleanly and the pacing loop drains+resumes.
# (If a Zenodo recovery is ever co-run again, drop these back so the two raw valves sum to <= ~800 GB / 800k.)
MAX_DISK_BYTES="${MAX_DISK_BYTES:-700000000000}"
MAX_DISK_FILES="${MAX_DISK_FILES:-700000}"
# PARSE_WORKERS: parse this many calc units CONCURRENTLY (N worker THREADS parse; the MAIN thread is
# the SOLE writer of shards+metadata, so workers never interleave a shard/metadata write — the
# one-calc-at-a-time crash-safety invariant holds, parse.py:1514/1546). Back to 6 (was briefly 4):
# measured 2026-08-22 the OVERNIGHT fetch runs 10-14 MB/s (~2x the daytime 5-6), and at 4 workers
# parse (~13-16 calc/s) ran level with the fastest fetch batch (17 entries/s) and peak_staged crept up
# — so 6 (~25-30 calc/s) restores a comfortable margin at the peak rate and keeps staging at a steady
# ~2 parts. Extra workers beyond this only help while the disk valve is pinned (parse-bound drains).
# Override without editing: `PARSE_WORKERS=4 sbatch ...` (keep cpus-per-task >= PARSE_WORKERS+2).
PARSE_WORKERS="${PARSE_WORKERS:-6}"
# RAM guard: refuse to ATTEMPT a primary bigger than this (0 = attempt everything). pymatgen peak
# RSS is ~10x the (uncompressed) primary size, and PARSE_WORKERS parse AT ONCE, so RAM ~=
# PARSE_WORKERS x MAX_PRIMARY_BYTES x 10 must fit the job. NOMAD vaspruns are tiny (median 0.36 MB),
# RAM budget: 6 workers x 0.8 GB x 10 (pymatgen peak ~10x the uncompressed primary) = 48 GiB <
# 8 cores x 6.76 GiB = 54 GiB. 800 MB covers even large AIMD vaspruns so few are needlessly skipped
# (only ~30 in the whole corpus; primary_too_large is non-terminal — those records are NOT purged
# (their calc is unparsed), so a single end-of-run sweep `parse --parse-workers 1 --max-primary-bytes 0`
# on a himem node captures them from raw/ with NO re-fetch, giving one worker all the RAM for a big
# vasprun). Keep PARSE_WORKERS x MAX_PRIMARY_BYTES x 10 under the job RAM if you change either.
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-800000000}"
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
    --parts "$PARTS" \
    --max-disk-bytes "$MAX_DISK_BYTES" \
    --max-disk-files "$MAX_DISK_FILES" \
    --max-primary-bytes "$MAX_PRIMARY_BYTES" \
    --parse-timeout "$PARSE_TIMEOUT" \
    --parse-workers "$PARSE_WORKERS" \
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

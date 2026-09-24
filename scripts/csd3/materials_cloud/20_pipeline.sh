#!/bin/bash
#SBATCH -J mc-pipeline
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake-himem               # 6760 MiB/core: parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=20             # ~132 GiB. 8 fetch workers (download + tar/bz2 decompression)
                                       # + 6 parse children + the main process fit 20 cores; the RAM
                                       # is shared by the parses through a MEMORY BUDGET (below), so
                                       # the primary cap can be ~10 GB without cutting parse workers.
#SBATCH --time=12:00:00                # SL3 max; ~1.1 TB fetch (~3-4 h at the measured S3 rate)
#SBATCH --signal=B:USR1@600            # SIGUSR1 10 min before wallclock -> RESUBMIT=1 queues a resume
#SBATCH -o logs_mc/mc-pipeline-%j.out
#SBATCH -e logs_mc/mc-pipeline-%j.err
#SBATCH --mail-type=END,FAIL
#
# Materials Cloud stages 2-4 in ONE overlapped, disk-paced command: fetch(part i+1) runs while
# parse+purge(part i) runs. The fetch is the SHARED zenodo_harvest fetch driven with an anonymous
# MC session (files come from CSCS S3 via MC's 302 redirect; md5-verified, Range-resumable, targeted
# zip members where worthwhile, AiiDA archives — legacy zip/tar and sqlite_zip — extracted by the
# shared AiiDA extractor); parse/store/verify are the shared code. Ends with `verify`. Everything is
# resumable: re-submit by hand or set RESUBMIT=1 to self-chain across wallclock kills.
#
# STANDALONE MC TREE ($MC_HARVEST_DATA, a sibling of the Zenodo/NOMAD roots) — the disk valve walks
# only its own raw dir, so it never counts the other harvests' files. The /rds quota itself is
# SHARED: the defaults below assume ~880 GB / ~990k inodes free and NO other job (2026-09-24:
# 115 GB / 6.6k files used of 1 TB / 1M).
# Sized from the 2026-09-24 CSD3 bench (mc_speed.json / mc_bench.json): S3 gives 52 MB/s on one
# stream and 96 MB/s over 8 (WORKERS=8); a small calc parses in 0.19 s (x3.8 with 4 workers);
# the largest pilot primaries peaked at 3.7-5.4x their size (trajectories: ~10x) -> ratio 12.
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
# The blind-fetched archives include .7z / .rar (and nested ones): py7zr/rarfile come from the
# `archives` extra; rarfile also needs an unrar binary — the static RARLAB one in ~/bin (as for the
# Zenodo rar recovery, scripts/csd3/49_rar_recover.sh).
export PATH="$HOME/bin:$PATH"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

# ---- HARVEST PARAMETERS ---------------------------------------------------------
PARTS="${PARTS:-24}"                   # batches of fetch units (~1.2k units / ~1.1 TB -> ~45 GB each;
                                       # records are split per archive by triage, so a part never has
                                       # to hold a whole multi-archive record)
WORKERS="${WORKERS:-8}"                # concurrent fetch units: 8 S3 streams gave the best aggregate
                                       # (96 MB/s vs 57 at 4); MC's 500 req/min is never approached
MAX_BYTES="${MAX_BYTES:-0}"            # 0 = uncapped per-file download (the disk valve is the bound)
MAX_MEMBER_BYTES="${MAX_MEMBER_BYTES:-0}"   # 0 = uncapped extracted member (the valve is the bomb guard)
# Staging budget for the WHOLE MC raw dir: both concurrently-staged parts, the archives being
# downloaded + extracted (the blind-fetched ones are deleted right after, keeping only VASP files),
# and any deferred primaries. ~88% / ~91% of the free space: the rest is for the MC dataset shards
# + manifests (tens of GB, not valve-tracked) and Lustre overhead. Charged per byte/inode as written
# and refunded on delete, so `staged <= this` holds exactly. Lower both if other jobs share the quota.
MAX_DISK_BYTES="${MAX_DISK_BYTES:-780000000000}"
MAX_DISK_FILES="${MAX_DISK_FILES:-900000}"
# Parse. Tens of thousands of small calcs are PARSE-THROUGHPUT-bound -> 6 workers. RAM is shared
# through a MEMORY BUDGET: each parse first reserves ~RSS_RATIO x its (uncompressed) primary, FIFO,
# so small calcs run 6-way while a multi-GB AIMD vasprun waits, then runs alone — the cap is then a
# per-FILE bound (RSS_RATIO x cap <= budget), not workers x ratio x cap <= RAM. Budget = the job's
# RAM minus 20 GiB for the main process + 8 fetch workers (a blind-fetched zip with ~10M members,
# e.g. SSSP's 13 GB AiiDA exports, holds ~7 GB of zipfile directory; big tars hold member lists).
# Anything over the cap stays staged as primary_too_large for the optional 30_bigparse.sh.
RSS_RATIO="${RSS_RATIO:-12}"           # INTEGER: pymatgen peak / primary (bench 3.7-5.4x; trajectories ~10x)
PARSE_WORKERS="${PARSE_WORKERS:-6}"
PARSE_MEM_BUDGET="${PARSE_MEM_BUDGET:-$(( ${SLURM_CPUS_PER_TASK:-20} * 6760 * 1048576 - 20 * 1073741824 ))}"
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-$(( (PARSE_MEM_BUDGET - 536870912) / RSS_RATIO ))}"   # ~10 GB at 20 cpus
PARSE_TIMEOUT="${PARSE_TIMEOUT:-3600}" # hard-kill one non-terminating parse after 1 h (a 10 GB
                                       # vasprun parses in minutes; the bench saw 8-19 s/GB)
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
echo "    parts=$PARTS fetch_workers=$WORKERS parse_workers=$PARSE_WORKERS" \
     "mem_budget=$(( PARSE_MEM_BUDGET / 1073741824 )) GiB max_primary=$MAX_PRIMARY_BYTES B" \
     "(ratio $RSS_RATIO) valve=${MAX_DISK_BYTES} B/${MAX_DISK_FILES} inodes timeout=${PARSE_TIMEOUT}s"
quota 2>/dev/null || lfs quota -u "$USER" "$MC_HARVEST_DATA" 2>/dev/null || true
# Archive backends (a missing one only turns those archives into logged `archive_unsupported` /
# `extract_error` rejections — warn loudly, do not abort the harvest):
python - <<'PY' || true
import importlib.util, shutil
mods = {m: bool(importlib.util.find_spec(m)) for m in ("py7zr", "rarfile", "zstandard")}
tool = next((t for t in ("unrar", "unar", "bsdtar", "7z") if shutil.which(t)), None)
ok = all(mods.values()) and tool
print(f"    archive backends: {mods}, rar tool: {tool or 'NONE'}" + ("" if ok else
      "  <-- WARNING: install the 'archives' extra / put unrar in ~/bin, or .7z/.rar are skipped"))
PY

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
    --parse-mem-budget "$PARSE_MEM_BUDGET" --parse-rss-ratio "$RSS_RATIO" \
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

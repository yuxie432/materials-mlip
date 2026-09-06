#!/bin/bash
#SBATCH -J nomad-recover-bigvasprun
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core -> the cores ARE the RAM budget
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10             # RAM = 10 x 6760 MiB = ~66 GiB. SERIAL parse (1 worker).
                                       # Peak = resume frame_id set (~8 GiB, appending to the 7M
                                       # dataset) + ONE parse of the largest remaining file
                                       # (3.47 GB x ~12 pymatgen blow-up = ~42 GiB) = ~50 GiB
                                       # -> ~16 GiB headroom. The 8.51 GB monster is EXCLUDED
                                       # (recover_bigvasprun_manifest.py); do it separately with
                                       # cpus-per-task=24. If any single parse still overshoots,
                                       # --parse-timeout>0 runs each calc in a child, so a cgroup
                                       # OOM kills only that child (logged parse_worker_died,
                                       # retryable) — the job survives and the rest complete.
#SBATCH --time=04:00:00                # only ~24 small-ish calcs; serial finishes in well under this
#SBATCH -o logs_nomad/nomad-recover-bigvasprun-%j.out
#SBATCH -e logs_nomad/nomad-recover-bigvasprun-%j.err
#SBATCH --mail-type=END,FAIL
#
# Re-parse the primary_too_large vasprun.xml calcs (the long-AIMD trajectories deferred by the
# RAM cap) into the EXISTING dataset. No re-fetch: the staged files are still on disk (parse
# failures never purge). Steps: build the subset fetched manifest -> parse uncapped (serial) ->
# verify -> purge-raw those records. Idempotent + resumable.
#
# WHY SERIAL (parse-workers=1): only ~24 calcs, so throughput is irrelevant (serial finishes in
# minutes); serial removes any risk of two multi-GB parses colliding in RAM and makes the budget
# trivial to reason about. RAM — not CPU — is the binding constraint here.
#
# Create logs_nomad/ BEFORE submitting: mkdir -p logs_nomad
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
# Activate the harvest env BEFORE `sbatch`:
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$NOMAD_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

MAN="$NOMAD_HARVEST_DATA/manifests"
DATASET_DIR="$NOMAD_HARVEST_DATA/dataset"
RAW="$NOMAD_HARVEST_DATA/raw"
FETCHED="$MAN/nomad_bigvasprun_fetched.jsonl"

echo "=== recover-bigvasprun START $(date -Is) on $(hostname) ==="
quota 2>/dev/null || lfs quota -u "$USER" "$NOMAD_HARVEST_DATA" 2>/dev/null || true

if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

# ---- step 1: build the subset fetched manifest ----------------------------------
echo "=== step 1: build big-vasprun fetched manifest $(date -Is) ==="
python scripts/csd3/nomad/recover_bigvasprun_manifest.py
if [[ ! -s "$FETCHED" ]]; then
    echo "no records to re-parse; exiting 0."
    exit 0
fi
echo "records to re-parse: $(wc -l < "$FETCHED")"

# ---- step 2: parse uncapped (serial), into the existing dataset ------------------
# --max-primary-bytes 0 disables the cap, so ONLY the primary_too_large deferrals are re-attempted
# (terminal parse failures stay skipped via the rejection log; already-parsed calcs skip via resume).
echo "=== step 2: parse (uncapped, serial) $(date -Is) ==="
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DATASET_DIR" \
    --raw-dir "$RAW" \
    --rejections "$DATASET_DIR/rejections.jsonl" \
    --max-primary-bytes 0 \
    --parse-timeout 3600

# ---- step 3: verify + reclaim the now-parsed staging ----------------------------
echo "=== step 3: verify + purge-raw $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DATASET_DIR"
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DATASET_DIR" \
    --fetched "$FETCHED"

echo "=== recover-bigvasprun DONE $(date -Is) ==="
echo "staged files under raw/: $(find "$RAW" -type f 2>/dev/null | wc -l)"

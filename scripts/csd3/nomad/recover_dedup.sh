#!/bin/bash
#SBATCH -J nomad-recover-dedup
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core; parse (pymatgen) needs the RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12             # RAM budget = cpus x 6760 MiB = ~79 GiB. Same rule as the
                                       # main pipeline: parse-workers 3 x 12x x 1.6 GB + ~10 GiB
                                       # resume overhead = ~67 GiB < 79 -> SAFE. These LOBSTER-paper
                                       # calcs are normal-size vaspruns, so 3 workers is comfortable.
#SBATCH --time=12:00:00                # SL3 max; only ~18.6k entries, finishes well within this
#SBATCH -o logs_nomad/nomad-recover-dedup-%j.out
#SBATCH -e logs_nomad/nomad-recover-dedup-%j.err
#SBATCH --mail-type=END,FAIL
#
# STEP 2 of the dedup false-positive recovery: fetch+parse the corrected keep-list into the
# EXISTING NOMAD dataset. The global dataset-skip means only the newly-kept entries are fetched;
# the shared parse/store append new shards+metadata in the SAME schema as the existing 7M calcs
# (same pymatgen 2026.5.4, disjoint calc_ids), and the run ends with `verify` (bijection check).
#
# *** STEP 1 (build the keep-list) MUST BE RUN FIRST, on a login node: ***
#     module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#     cd ~/materials-mlip && python scripts/csd3/nomad/recover_dedup_keep.py
#   -> writes $NOMAD_HARVEST_DATA/manifests/nomad_recover_keep.jsonl (network-light metadata query;
#      confirm it reports ~18,615 kept before submitting this job).
#
# Everything is resumable: re-submitting this script continues where it stopped.
# Create logs_nomad/ BEFORE submitting: mkdir -p logs_nomad
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
# Activate the harvest env BEFORE `sbatch` (captured via --export=ALL):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$NOMAD_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

MAN="$NOMAD_HARVEST_DATA/manifests"
DATASET_DIR="$NOMAD_HARVEST_DATA/dataset"
RAW="$NOMAD_HARVEST_DATA/raw"
KEEP="$MAN/nomad_recover_keep.jsonl"

echo "=== recover-dedup (fetch+parse) START $(date -Is) on $(hostname) ==="
if [[ ! -s "$KEEP" ]]; then
    echo "ERROR: $KEEP missing/empty. Run step 1 on a login node first:" >&2
    echo "       python scripts/csd3/nomad/recover_dedup_keep.py" >&2
    exit 2
fi
echo "recovery keep-list entries: $(wc -l < "$KEEP")"
quota 2>/dev/null || lfs quota -u "$USER" "$NOMAD_HARVEST_DATA" 2>/dev/null || true

# The chain is one job, so any parse lock present at startup is stale.
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

python -m nomad_harvest.cli -v pipeline \
    --in "$KEEP" \
    --parts 8 \
    --max-disk-bytes 700000000000 \
    --max-disk-files 700000 \
    --max-primary-bytes 1600000000 \
    --parse-timeout 1200 \
    --parse-workers 3 \
    --raw-dir "$RAW" \
    --dataset-dir "$DATASET_DIR"
rc=$?

echo "=== recover-dedup exit=$rc $(date -Is) ==="
echo "staged files under raw/: $(find "$RAW" -type f 2>/dev/null | wc -l)"
exit "$rc"

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
# Recover the ~18,615 NOMAD VASP-DFT calcs wrongly dropped as duplicate_of_zenodo (they only
# CITE a Zenodo code release that holds no VASP data and was never harvested — see
# recover_dedup_keep.py). Two steps in one compute job:
#   1. rebuild a corrected keep-list (re-query metadata + narrowed dedup) -> nomad_recover_keep.jsonl
#   2. fetch+parse it into the EXISTING NOMAD dataset (the global dataset-skip touches only these
#      new entries; verify runs at the end).
# Everything is resumable: re-submitting this script continues both steps where they stopped.
# This does NOT change discover's dedup rule for future runs — that one-line fix in harvest.py is
# left for later (see the investigation notes); this recovery inlines the narrowed rule itself.
#
# Create logs_nomad/ BEFORE submitting (SLURM opens -o/-e before the body runs): mkdir -p logs_nomad
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

echo "=== recover-dedup START $(date -Is) on $(hostname) ==="
quota 2>/dev/null || lfs quota -u "$USER" "$NOMAD_HARVEST_DATA" 2>/dev/null || true

# ---- clear a stale parse lock (this is the only harvest job running) -------------
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

# ---- step 1: rebuild the corrected keep-list ------------------------------------
echo "=== step 1: rebuild recovery keep-list $(date -Is) ==="
python scripts/csd3/nomad/recover_dedup_keep.py
if [[ ! -s "$KEEP" ]]; then
    echo "recovery keep-list is empty (nothing to recover); exiting 0."
    exit 0
fi
echo "recovery keep-list entries: $(wc -l < "$KEEP")"

# ---- step 2: fetch + parse the recovered entries into the existing dataset -------
# Disk/inode valve sized generously (solo job) but bounded for the shared 1 TB / 1M quota.
echo "=== step 2: pipeline fetch+parse+verify $(date -Is) ==="
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

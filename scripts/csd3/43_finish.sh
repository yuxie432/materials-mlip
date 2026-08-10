#!/bin/bash
#SBATCH -J zh-finish
#SBATCH -p icelake-himem                # parse RAM (pymatgen); fetch part is light
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 4
#SBATCH -t 6:00:00
#SBATCH -o logs/zh-finish-%j.out
#SBATCH -e logs/zh-finish-%j.err
#SBATCH --mail-type=END,FAIL
#
# Fetch+parse ONLY the records in finish.jsonl (the last untouched record), NOT the whole
# keep-list. Unlike 20_pipeline.sh (which hardcodes --in keep.jsonl), this passes --in
# explicitly, so it can never wander into 18012696 or re-churn the 1,352-record harvest.
#
# Before submitting:
#   scancel every other harvest job first (a stale RESUBMIT chain re-runs the full keep-list)
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU; source <path>/.venv/bin/activate; mkdir -p logs
#   sbatch scripts/csd3/43_finish.sh
# NB: no RESUBMIT here on purpose — one record, one shot; if it can't finish it's a giant, drop it.

set -uo pipefail
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
if [[ -d /local && -w /local ]]; then export TMPDIR=/local; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; mkdir -p "$TMPDIR"; fi

MAN="$ZENODO_HARVEST_DATA/manifests"
IN="${FINISH_IN:-$MAN/finish.jsonl}"
if [[ ! -s "$IN" ]]; then echo "ERROR: $IN missing/empty — regenerate it first" >&2; exit 2; fi

echo "=== zh-finish $(date -Is) on $(hostname); IN=$IN ($(wc -l < "$IN") record(s)) ==="
python -m zenodo_harvest.cli -v pipeline --in "$IN" \
    --parts 1 --workers 1 \
    --max-bytes 0 --max-member-bytes 30000000000 \
    --max-disk-bytes 900000000000 --max-disk-files 950000 \
    --max-primary-bytes 2000000000 --parse-timeout 1200 \
    --raw-dir "$ZENODO_HARVEST_DATA/raw" --dataset-dir "$ZENODO_HARVEST_DATA/dataset"
rc=$?
echo "=== zh-finish exit=$rc $(date -Is) ==="
exit "$rc"

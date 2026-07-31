#!/bin/bash
#SBATCH -J zh-t25-safety
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --time=00:40:00
#SBATCH -o logs/zh-t25-safety-%j.out
#SBATCH -e logs/zh-t25-safety-%j.err
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address
#
# SMOKE TEST for the SAFETY MEASURES + RESUMABILITY. Runs smoke_safety.py, which drives the
# real fetch/parse/purge/verify functions on the tiny keep-list with valve limits sized
# ADAPTIVELY from the data's measured footprint, so each valve is guaranteed to trip:
#   1. --max-disk-bytes  valve (fetch->parse->purge->resume pacing; peak bytes <= limit)
#   2. --max-disk-files  valve (inode budget; CSD3's binding limit; peak inodes <= limit)
#   3. --max-primary-bytes RAM guard (primary_too_large skip, recovered by an uncapped re-parse)
#   4. fetch record-level resume     5. parse resume
# The job exit code IS the verdict (0 = all PASS). See the .out log for the PASS/FAIL table.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo_smoketest}"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   (before sbatch)
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

mkdir -p logs
MAN="$ZENODO_HARVEST_DATA/manifests"
if [[ ! -s "$MAN/keep.jsonl" ]]; then
    echo "ERROR: $MAN/keep.jsonl missing — run t10_smoke_discover.sh first." >&2
    exit 2
fi

echo "=== [t25] safety + resumability suite $(date -Is) ==="
python scripts/csd3/test/smoke_safety.py \
    --keep "$MAN/keep.jsonl" \
    --work-dir "$ZENODO_HARVEST_DATA/safety_work"
rc=$?
echo "=== [t25] smoke_safety exit=$rc (0 = all PASS) $(date -Is) ==="
exit "$rc"

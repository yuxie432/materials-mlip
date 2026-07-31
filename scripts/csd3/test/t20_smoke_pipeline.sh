#!/bin/bash
#SBATCH -J zh-t20-pipeline
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake                     # small parse needs little RAM; icelake schedules fast.
#SBATCH --nodes=1                      #   switch to icelake-himem to also exercise the prod partition.
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --time=00:40:00
#SBATCH -o logs/zh-t20-pipeline-%j.out
#SBATCH -e logs/zh-t20-pipeline-%j.err
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address
#
# SMOKE TEST stages 2-4 HAPPY PATH: run the REAL `pipeline` command (overlapped fetch ||
# parse+purge, then verify) on the tiny keep-list, with PRODUCTION-sized limits so nothing
# trips — this proves the end-to-end pipeline works on CSD3 and produces a clean dataset.
# Then re-check with `status` and inspect the actual frames + metadata. (Safety valves are
# exercised separately, at TIGHT limits, by t25_smoke_safety.sh.)
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo_smoketest}"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   (before sbatch)
# Parse copies each OUTCAR into $TMPDIR; keep it on node-local scratch (/local), not tmpfs /tmp,
# not the /rds quota (see 20_pipeline.sh). Falls back to /rds scratch if /local is absent.
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
RAW="$ZENODO_HARVEST_DATA/raw"
DS="$ZENODO_HARVEST_DATA/dataset"
# Fresh dataset/raw so the smoke result is unambiguous; the smoke test is disposable.
rm -rf "$RAW" "$DS" "$MAN/keep.pipeline_parts" "$MAN/keep_t20_parts"

echo "=== [t20] pipeline (real command, production-sized limits) $(date -Is) ==="
# Production-sized valves (won't trip on the tiny data) so this is a clean happy-path run.
# Own --parts-dir so it never collides with t30's or a rerun's fetched sidecars.
python -m zenodo_harvest.cli -v pipeline \
    --in "$MAN/keep.jsonl" \
    --parts-dir "$MAN/keep_t20_parts" \
    --parts 2 --workers 2 \
    --max-bytes 0 --max-member-bytes 30000000000 \
    --max-disk-bytes 800000000000 --max-disk-files 800000 \
    --max-primary-bytes 2000000000 \
    --raw-dir "$RAW" --dataset-dir "$DS" \
    | tee "logs/zh-t20-${SLURM_JOB_ID:-local}.summary.json"

echo ""
echo "=== [t20] status snapshot $(date -Is) ==="
python -m zenodo_harvest.cli status --manifests-dir "$MAN" \
    --raw-dir "$RAW" --dataset-dir "$DS" --keep "$MAN/keep.jsonl" \
    --max-disk-bytes 800000000000 --max-disk-files 800000

echo ""
echo "=== [t20] inspect frames + metadata (label fidelity + integrity) $(date -Is) ==="
python scripts/csd3/test/inspect_dataset.py --dataset-dir "$DS"
echo "=== [t20] done $(date -Is) ==="

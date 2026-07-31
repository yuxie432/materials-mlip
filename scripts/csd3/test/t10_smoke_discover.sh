#!/bin/bash
#SBATCH -J zh-t10-discover
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # rate-limited to 30 req/min: one core is plenty
#SBATCH --time=01:00:00                # small discover (~1 min) + triage peeks (429-backoff -> a few min)
#SBATCH -o logs/zh-t10-discover-%j.out
#SBATCH -e logs/zh-t10-discover-%j.err
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address
#
# SMOKE TEST stage 0-1: a SMALL, real discover + triage -> a tiny keep-list for the rest of
# the smoke test. Unlike the production 10_discover.sh this is NOT --exhaustive and is capped
# with --max-records, so it finishes in minutes. Writes to a SEPARATE test data dir so it can
# never touch a real harvest. Needs outbound HTTPS (run 00_check_network.sh first).
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
# A SEPARATE data root for the smoke test (never the production ZENODO_HARVEST_DATA):
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo_smoketest}"
# Activate the harvest env BEFORE sbatch (sbatch captures your submit env; --export=ALL):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
cd "${SLURM_SUBMIT_DIR:-.}"            # repo root (holds .env with ZENODO_TOKEN)
# --------------------------------------------------------------------------------

MAXREC="${MAXREC:-150}"                # candidate records to scan (windowed, not exhaustive)
COUNT="${COUNT:-8}"                    # how many small confirmed records to keep for the test
mkdir -p logs "$ZENODO_HARVEST_DATA/manifests"
MAN="$ZENODO_HARVEST_DATA/manifests"

echo "=== [t10] discover (max-records $MAXREC, windowed) $(date -Is) ==="
python -m zenodo_harvest.cli -v discover --max-records "$MAXREC" \
    --out "$MAN/candidates.jsonl" --fresh

echo "=== [t10] triage (peek ON) $(date -Is) ==="
python -m zenodo_harvest.cli -v triage --in "$MAN/candidates.jsonl" \
    --out "$MAN/keep_full.jsonl" --min-rank 3

echo "=== [t10] shrink to $COUNT smallest confirmed records -> keep.jsonl $(date -Is) ==="
python scripts/csd3/test/make_test_keeplist.py \
    --in "$MAN/keep_full.jsonl" --out "$MAN/keep.jsonl" --count "$COUNT"

echo "=== [t10] done $(date -Is): keep.jsonl has $(wc -l < "$MAN/keep.jsonl") records ==="
echo "Next: sbatch scripts/csd3/test/t20_smoke_pipeline.sh   (and t25_smoke_safety.sh, t30_smoke_array.sh)"

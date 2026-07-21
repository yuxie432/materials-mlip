#!/bin/bash
#SBATCH -J zen-sample
#SBATCH -A YOUR_ACCOUNT-SL2-CPU        # <-- your CSD3 project account
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # pymatgen parse benefits from a few cores
#SBATCH --time=02:00:00
#SBATCH -o sample-%j.out
#SBATCH -e sample-%j.err

# Measure raw->dataset storage ratios + fetch yield on a size-stratified sample of
# the relevant (rank>=3) records. Downloads happen here, so run on a compute node
# with scratch space. Raise --cap-gb / --n / --max-total-gb for a tighter estimate
# (bigger sample, higher cap -> higher yield, captures large trajectories).
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO"

module load python/3.11 2>/dev/null || true
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-$PWD/data}"
MANIFEST="$ZENODO_HARVEST_DATA/manifests/candidates_full.jsonl"
WORK="$ZENODO_HARVEST_DATA/estimate_sample"

python scripts/estimate/sample_storage.py "$MANIFEST" "$WORK" \
    --n 200 --cap-gb 2 --max-total-gb 100

# then project:
python scripts/estimate/census.py "$MANIFEST" --json "$WORK/census.json"
python scripts/estimate/project.py "$WORK/census.json" "$WORK/ratios.json"

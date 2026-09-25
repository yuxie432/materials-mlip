#!/bin/bash
#SBATCH -J zc-census
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # API-paced (30 search req/min): one core is plenty
#SBATCH --time=12:00:00                # measured need ~3.5 h (5.8k pages); resumable if killed
#SBATCH -o logs_census/zc-census-%j.out
#SBATCH -e logs_census/zc-census-%j.err
#SBATCH --mail-type=END,FAIL
#
# Zenodo census, stage 0' (docs/ZENODO_CENSUS.md): enumerate EVERY Zenodo record that holds an
# archive or a loose VASP primary (583k records, 2026-09-25) with the token's 100-per-page search,
# into $ZENODO_CENSUS_DATA/census.jsonl — plus, concurrently (other hosts, no Zenodo budget), the
# two link channels: Crossref->Zenodo references from DataCite's event store and Europe PMC
# full-text mentions. Needs ZENODO_TOKEN (the repo's .env) for 100-record pages.
#
# Resumable: finished created-windows are skipped (census.jsonl.windows.jsonl); the DataCite pull
# keeps its cursor; Europe PMC skips papers already written. Just resubmit if the job dies.
# NB: `mkdir -p logs_census` BEFORE the first submit (SLURM opens -o/-e before the body runs).
set -euo pipefail

# ---- ENV SETUP ------------------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export ZENODO_CENSUS_DATA="${ZENODO_CENSUS_DATA:-$ZENODO_HARVEST_DATA/census}"
# Activate the harvest env BEFORE `sbatch`:
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
cd "${SLURM_SUBMIT_DIR:-.}"            # repo root (holds .env with ZENODO_TOKEN)
# --------------------------------------------------------------------------------

mkdir -p logs_census "$ZENODO_CENSUS_DATA"
JOB="${SLURM_JOB_ID:-local}"
if ! grep -qs '^ *\(export \)\?ZENODO_TOKEN=' .env && [[ -z "${ZENODO_TOKEN:-}" ]]; then
    echo "WARNING: no ZENODO_TOKEN (.env or env): pages of 25 -> ~4x the requests (~14 h)" >&2
fi

echo "=== links (background): DataCite Crossref->Zenodo refs + Europe PMC $(date -Is) ==="
python -m zenodo_census.cli -v links > "logs_census/zc-links-$JOB.json" \
    2> "logs_census/zc-links-$JOB.err" &
LINKS_PID=$!

echo "=== census $(date -Is) on $(hostname) -> $ZENODO_CENSUS_DATA ==="
set +e
python -m zenodo_census.cli -v census > "logs_census/zc-census-$JOB.summary.json"
rc=$?
wait "$LINKS_PID"; lrc=$?
set -e
echo "=== census exit=$rc, links exit=$lrc $(date -Is) ==="
if [[ "$lrc" -ne 0 ]]; then echo "links incomplete — 20_score.sh re-runs it (resumable)" >&2; fi
python -m zenodo_census.cli status
exit "$rc"

#!/bin/bash
#SBATCH -J zc-score
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2              # ~2 GB of seed/name tables + 4 lookup threads
#SBATCH --time=12:00:00                # links top-up minutes; OpenAlex ~1-3 h at 8/s; score ~20 min
#SBATCH -o logs_census/zc-score-%j.out
#SBATCH -e logs_census/zc-score-%j.err
#SBATCH --mail-type=END,FAIL
#
# Zenodo census, stage 0'' (docs/ZENODO_CENSUS.md), after 10_census.sh:
#   1. links    — finish the DataCite / Europe PMC pulls if 10_census.sh left them incomplete
#                 (both resumable; a no-op when complete); then resolve — the Zenodo VERSION ids
#                 those links cite -> concept ids (~250 batched searches, ~9 min);
#   2. openalex — one free singleton lookup per paper DOI linked to / citing a still-T2/T3
#                 record: does it cite the VASP method papers, which field is it in (cached);
#   3. score    — every census record -> tier (T1 strong / T2 plausible / T3 low / T0 other
#                 domain), minus the records already in the dataset or already evaluated by the
#                 keyword harvest (candidates_full / keep / nc_* / byid_10579527 manifests).
# Then REVIEW $ZENODO_CENSUS_DATA/score_report.json (tier sizes, peek workload, the known-miss
# probes) — send it over — before 30_triage.sh.
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export ZENODO_CENSUS_DATA="${ZENODO_CENSUS_DATA:-$ZENODO_HARVEST_DATA/census}"
# Materials Cloud's creators are extra seed names when its dataset exists (read-only).
export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   (before sbatch)
cd "${SLURM_SUBMIT_DIR:-.}"

mkdir -p logs_census
JOB="${SLURM_JOB_ID:-local}"
if [[ ! -s "$ZENODO_CENSUS_DATA/census.jsonl" ]]; then
    echo "ERROR: no census at $ZENODO_CENSUS_DATA/census.jsonl — run 10_census.sh first" >&2
    exit 2
fi

echo "=== links (top-up) $(date -Is) ==="
python -m zenodo_census.cli -v links > "logs_census/zc-links-$JOB.json" \
    || echo "links still incomplete; scoring with what there is" >&2
echo "=== resolve cited version ids -> concepts $(date -Is) ==="
python -m zenodo_census.cli -v resolve > "logs_census/zc-resolve-$JOB.json"
echo "=== openalex $(date -Is) ==="
python -m zenodo_census.cli -v openalex > "logs_census/zc-openalex-$JOB.json"
echo "=== score $(date -Is) ==="
python -m zenodo_census.cli -v score > "logs_census/zc-score-$JOB.json"
python -m zenodo_census.cli status
echo "=== done $(date -Is): review $ZENODO_CENSUS_DATA/score_report.json before 30_triage.sh ==="

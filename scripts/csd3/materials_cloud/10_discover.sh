#!/bin/bash
#SBATCH -J mc-discover
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2              # single-stream API + Range peeks: CPU is idle; 2 for the JSON
#SBATCH --time=06:00:00                # census ~minutes; ~1.5k central-directory peeks (4 in parallel)
#SBATCH -o logs_mc/mc-discover-%j.out
#SBATCH -e logs_mc/mc-discover-%j.err
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address
#
# Stages 0-1 (Materials Cloud): a FULL census of every public MC Archive record (no keyword recall
# limit), the access + licence gates, overlap FLAGS against the harvested Zenodo + NOMAD datasets,
# then triage = a Range peek of every .zip / .aiida central directory and the evidence policy
# (docs/MATERIALS_CLOUD_HARVEST.md). Output: mc_keep.jsonl (fetch units) + mc_keep.report.json
# (the census report — review it before running 20_pipeline.sh).
#
# Resumable: discover is cheap to redo; triage caches every completed peek in
# mc_keep.jsonl.peeks.jsonl, so a resubmitted job skips them.
#
# NB: create logs_mc/ BEFORE you submit (SLURM opens -o/-e before the body runs): `mkdir -p logs_mc`.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
# ZENODO_HARVEST_DATA / NOMAD_HARVEST_DATA: read-only here — their dataset/metadata.jsonl feed the
# overlap FLAGS. MC_HARVEST_DATA: Materials Cloud's OWN sibling tree (never mixed with the others).
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
# Activate the harvest env BEFORE `sbatch` (sbatch captures your submit env by default):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
cd "${SLURM_SUBMIT_DIR:-.}"      # repo root
# --------------------------------------------------------------------------------

LICENCE_POLICY="${LICENCE_POLICY:-nc-ok}"     # nc-ok (decided 2026-09-23) | strict | none
NOMAD_OVERLAP="${NOMAD_OVERLAP:-1}"           # 0 = skip the ~7M-line NOMAD metadata scan
PEEK_WORKERS="${PEEK_WORKERS:-4}"             # concurrent central-directory peeks (request-bound)

mkdir -p logs_mc "$MC_HARVEST_DATA/manifests"
MAN="$MC_HARVEST_DATA/manifests"
NOMAD_ARG=()
if [[ "$NOMAD_OVERLAP" == "0" ]]; then
    NOMAD_ARG=(--nomad-metadata none)
elif [[ -s "$NOMAD_HARVEST_DATA/dataset/metadata.jsonl" ]]; then
    NOMAD_ARG=(--nomad-metadata "$NOMAD_HARVEST_DATA/dataset/metadata.jsonl")
fi

echo "=== mc discover (full census) $(date -Is) on $(hostname) ==="
echo "    MC tree: $MC_HARVEST_DATA   overlap flags vs $ZENODO_HARVEST_DATA/dataset + ${NOMAD_ARG[*]:-no NOMAD}"
python -m materials_cloud_harvest.cli -v discover \
    --out "$MAN/mc_candidates.jsonl" \
    --licence-policy "$LICENCE_POLICY" \
    --zenodo-metadata "$ZENODO_HARVEST_DATA/dataset/metadata.jsonl" \
    "${NOMAD_ARG[@]}"

echo "=== mc triage (zip + AiiDA-export peeks) $(date -Is) ==="
python -m materials_cloud_harvest.cli -v triage \
    --in "$MAN/mc_candidates.jsonl" \
    --out "$MAN/mc_keep.jsonl" \
    --peek-workers "$PEEK_WORKERS"

echo "=== done $(date -Is): $(wc -l < "$MAN/mc_keep.jsonl") fetch units ==="
echo "review: $MAN/mc_keep.report.json  (summary + per-record decisions/evidence/gaps)"

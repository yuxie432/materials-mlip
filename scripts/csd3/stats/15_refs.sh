#!/bin/bash
#SBATCH -J ds-refs
#SBATCH -p icelake                     # network + CPU; compute nodes have outbound internet (verified)
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH -t 04:00:00
#SBATCH -o logs_stats/ds-refs-%j.out
#SBATCH -e logs_stats/ds-refs-%j.err
#SBATCH --mail-type=END,FAIL
#
# Reference MLIP datasets for the side-by-side comparison, measured with the SAME code:
#   mptrj          Matbench Discovery's MPtrj extxyz zip (figshare, md5-checked)
#   omat24_val     OMat24 validation split, 11 x .aselmdb (~2 GB, Meta's file server)
#   salex_val      sAlex validation split (0.43 GB)
#   mp_materials   MP relaxed structures 2023-02-07 (figshare json.gz, md5-checked)
#   alexandria_pbe Alexandria PBE 3D 2025.07.02 (58 json.bz2, ~3.5 GB)
#   mp_elemental_refs  MP's per-element reference energies (fetch only; the report's formation-
#                      energy proxy reads them)
# Downloads resume over HTTP Range and are skipped when already complete; scans resume per chunk.
# Disk: ~10-15 GB under $DATASET_STATS_DATA/refs (delete afterwards with `rm -rf .../refs`).
# Reading .aselmdb needs `pip install ase-db-backends` (small; no torch) in the venv.
#
# Before submit: as 10_stats.sh, plus once:  pip install ase-db-backends
#   sbatch scripts/csd3/stats/15_refs.sh
#   REFS="mptrj" sbatch scripts/csd3/stats/15_refs.sh          # just one

set -uo pipefail
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export DATASET_STATS_DATA="${DATASET_STATS_DATA:-/rds/user/$USER/hpc-work/stats}"
REFS="${REFS:-mp_elemental_refs mp_materials mptrj salex_val omat24_val alexandria_pbe}"
WORKERS="${WORKERS:-${SLURM_CPUS_PER_TASK:-4}}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"

echo "=== ds-refs START $(date -Is) on $(hostname): [$REFS] ==="
rc=0
for ref in $REFS; do
    echo "--- [$ref] fetch $(date -Is)"
    python -m dataset_stats.cli ref-fetch --ref "$ref" || { rc=1; continue; }
    echo "--- [$ref] scan $(date -Is)"
    python -m dataset_stats.cli ref-scan --ref "$ref" --workers "$WORKERS" || rc=1
done
echo "=== ds-refs DONE exit=$rc $(date -Is) ==="
exit "$rc"

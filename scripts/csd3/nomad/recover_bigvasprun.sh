#!/bin/bash
#SBATCH -J nomad-recover-bigvasprun
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core -> the cores ARE the RAM budget
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10             # RAM = 10 x 6760 MiB = ~66 GiB. SERIAL parse (1 worker).
                                       # Peak = resume frame_id set (~8 GiB, appending to the 7M
                                       # dataset) + ONE parse of the largest remaining file
                                       # (3.47 GB x ~12 pymatgen blow-up = ~42 GiB) = ~50 GiB
                                       # -> ~16 GiB headroom. The 8.51 GB monster is EXCLUDED when
                                       # you build the manifest; do it separately with cpus=24.
                                       # --parse-timeout>0 runs each calc in a child, so a cgroup
                                       # OOM kills only that child (parse_worker_died, retryable) —
                                       # the job survives and the rest complete.
#SBATCH --time=04:00:00                # only ~24 small-ish calcs; serial finishes well under this
#SBATCH -o logs_nomad/nomad-recover-bigvasprun-%j.out
#SBATCH -e logs_nomad/nomad-recover-bigvasprun-%j.err
#SBATCH --mail-type=END,FAIL
#
# STEP 2 of the primary_too_large recovery: re-parse the deferred long-AIMD vasprun.xml calcs
# (uncapped, SERIAL) into the EXISTING dataset, then verify + purge-raw. No re-fetch (the staged
# files are still on disk). Same pymatgen/parse/store as the 7M existing calcs -> consistent.
#
# WHY SERIAL (parse-workers=1, the standalone `parse` default): only ~24 calcs, so throughput is
# irrelevant (finishes in minutes); serial keeps peak RAM at ONE multi-GB parse and makes the
# budget trivial. RAM — not CPU — is the binding constraint.
#
# *** STEP 1 (build the subset manifest) MUST BE RUN FIRST, on a login node: ***
#     module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#     cd ~/materials-mlip && python scripts/csd3/nomad/build_reparse_manifest.py \
#         --reason primary_too_large --exclude dhOiawS2QOf-3IH1c7bYLS2RN4OR \
#         --out $NOMAD_HARVEST_DATA/manifests/nomad_bigvasprun_fetched.jsonl
#
# Create logs_nomad/ BEFORE submitting: mkdir -p logs_nomad
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
# Activate the harvest env BEFORE `sbatch`:
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$NOMAD_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

MAN="$NOMAD_HARVEST_DATA/manifests"
DATASET_DIR="$NOMAD_HARVEST_DATA/dataset"
RAW="$NOMAD_HARVEST_DATA/raw"
FETCHED="$MAN/nomad_bigvasprun_fetched.jsonl"

echo "=== recover-bigvasprun (parse) START $(date -Is) on $(hostname) ==="
if [[ ! -s "$FETCHED" ]]; then
    echo "ERROR: $FETCHED missing/empty. Run step 1 on a login node first:" >&2
    echo "       python scripts/csd3/nomad/build_reparse_manifest.py --reason primary_too_large \\" >&2
    echo "           --exclude dhOiawS2QOf-3IH1c7bYLS2RN4OR --out $FETCHED" >&2
    exit 2
fi
echo "records to re-parse: $(wc -l < "$FETCHED")"
quota 2>/dev/null || lfs quota -u "$USER" "$NOMAD_HARVEST_DATA" 2>/dev/null || true

if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

# --max-primary-bytes 0 disables the cap -> ONLY the primary_too_large deferrals are re-attempted
# (terminal parse failures stay skipped via the rejection log; already-parsed calcs skip via resume).
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DATASET_DIR" \
    --raw-dir "$RAW" \
    --rejections "$DATASET_DIR/rejections.jsonl" \
    --max-primary-bytes 0 \
    --parse-timeout 3600

echo "=== verify + purge-raw $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DATASET_DIR"
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DATASET_DIR" \
    --fetched "$FETCHED"

echo "=== recover-bigvasprun DONE $(date -Is) ==="
echo "staged files under raw/: $(find "$RAW" -type f 2>/dev/null | wc -l)"

#!/bin/bash
#SBATCH -J nomad-recover-int-algo
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10             # RAM = ~66 GiB. IN-PROCESS serial parse (so the pymatgen
                                       # monkeypatch is in effect — a forkserver child would load
                                       # clean pymatgen). Peak = resume frame_id set (~8 GiB) + one
                                       # parse (<=1.6 GB cap x ~12 = ~19 GiB) = ~27 GiB -> big margin.
#SBATCH --time=04:00:00                # ~1,380 normal-size vaspruns, ~0.3 s each -> minutes
#SBATCH -o logs_nomad/nomad-recover-int-algo-%j.out
#SBATCH -e logs_nomad/nomad-recover-int-algo-%j.err
#SBATCH --mail-type=END,FAIL
#
# STEP 2 of the numeric-ALGO recovery: monkeypatch pymatgen 2026.5.4 (coerce a numeric INCAR
# ALGO to str) and re-parse the affected calcs IN-PROCESS into the EXISTING dataset, then verify.
# Same pymatgen + parse/store as the 7M existing calcs (the patch only fixes the crash) -> the
# recovered frames are consistent with the rest. No re-fetch (files still staged).
#
# NB: NO --parse-timeout here — the parse must run in-process so the monkeypatch applies. The set
# is scoped to the ALGO calcs (they failed FAST at __init__, they do not hang), and
# max_primary_bytes caps RAM, so in-process is safe.
#
# *** STEP 1 (build the subset manifest) MUST BE RUN FIRST, on a login node: ***
#     module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#     cd ~/materials-mlip && python scripts/csd3/nomad/build_reparse_manifest.py \
#         --reason vasprun_parse_error --signature "has no attribute 'lower'" \
#         --out $NOMAD_HARVEST_DATA/manifests/nomad_int_algo_fetched.jsonl
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
FETCHED="$MAN/nomad_int_algo_fetched.jsonl"

echo "=== recover-int-algo (patch+parse) START $(date -Is) on $(hostname) ==="
if [[ ! -s "$FETCHED" ]]; then
    echo "ERROR: $FETCHED missing/empty. Run step 1 on a login node first:" >&2
    echo "       python scripts/csd3/nomad/build_reparse_manifest.py --reason vasprun_parse_error \\" >&2
    echo "           --signature \"has no attribute 'lower'\" --out $FETCHED" >&2
    exit 2
fi
echo "records to re-parse: $(wc -l < "$FETCHED")"

if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

python scripts/csd3/nomad/recover_int_algo.py

echo "=== verify + purge-raw $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DATASET_DIR"
python -m zenodo_harvest.cli purge-raw --raw-dir "$NOMAD_HARVEST_DATA/raw" \
    --dataset-dir "$DATASET_DIR" --fetched "$FETCHED"

echo "=== recover-int-algo DONE $(date -Is) ==="
echo "staged files under raw/: $(find "$NOMAD_HARVEST_DATA/raw" -type f 2>/dev/null | wc -l)"

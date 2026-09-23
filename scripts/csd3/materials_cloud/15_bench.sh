#!/bin/bash
#SBATCH -J mc-bench
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core: profiling a multi-GB AIMD vasprun needs RAM
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16             # ~106 GiB: RSS-profiles primaries up to ~7 GB; 4-way parse timing
#SBATCH --time=06:00:00
#SBATCH -o logs_mc/mc-bench-%j.out
#SBATCH -e logs_mc/mc-bench-%j.err
#SBATCH --mail-type=END,FAIL
#
# Sizing job for the Materials Cloud harvest — run ONCE, after 10_discover.sh, before 20_pipeline.sh.
# Three measurements on REAL data from a CSD3 compute node (the WSL link is not representative):
#
#   1. csd3_mc_probe.py --speed  : per-request cost (API 302 hop, small Range read) + S3 throughput
#                                  scaling over 1/2/4/8 streams + rate-limit headers -> --workers
#   2. csd3_mc_probe.py --aiida  : sqlite_zip AiiDA census (db.sqlite3 per export -> any aiida-vasp?)
#   3. csd3_mc_bench.py          : fetch + parse a stratified ~12 GB sample with the REAL code ->
#                                  MB/s incl. extraction, s/calc serial vs parallel, big-file RSS ratio
#                                  and s/GB, and a fetch-hours vs parse-hours projection
#
# Outputs (send these back): $MC_HARVEST_DATA/manifests/{mc_speed.json,mc_probe_aiida.json,mc_bench.json}.
# The bench sample is staged under $MC_HARVEST_DATA/bench and deleted afterwards (KEEP_WORK=1 keeps it).
set -euo pipefail

export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
# Activate the harvest env BEFORE `sbatch`:
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$MC_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR" logs_mc
cd "${SLURM_SUBMIT_DIR:-.}"
MAN="$MC_HARVEST_DATA/manifests"
[[ -s "$MAN/mc_keep.jsonl" ]] || { echo "ERROR: run 10_discover.sh first ($MAN/mc_keep.jsonl missing)" >&2; exit 2; }

echo "=== 1/3 speed probe $(date -Is) on $(hostname) ==="
python scripts/csd3/materials_cloud/csd3_mc_probe.py --speed --out "$MAN/mc_speed.json" || \
    echo "WARNING: speed probe failed (continuing)"

echo "=== 2/3 sqlite_zip AiiDA census $(date -Is) ==="
python scripts/csd3/materials_cloud/csd3_mc_probe.py --aiida \
    --candidates "$MAN/mc_candidates.jsonl" --out "$MAN/mc_probe_aiida.json" || \
    echo "WARNING: AiiDA census failed (continuing; partial rows in mc_probe_aiida.json.rows.jsonl)"

echo "=== 3/3 fetch+parse pilot benchmark $(date -Is) ==="
KEEP_ARG=()
[[ "${KEEP_WORK:-0}" != "0" ]] && KEEP_ARG=(--keep-work)
python scripts/csd3/materials_cloud/csd3_mc_bench.py \
    --keep "$MAN/mc_keep.jsonl" --report "$MAN/mc_keep.report.json" \
    --work "$MC_HARVEST_DATA/bench" --out "$MAN/mc_bench.json" \
    --budget-gb "${BUDGET_GB:-12}" --workers "${WORKERS:-4}" \
    --parse-workers "${PARSE_WORKERS:-4}" "${KEEP_ARG[@]}"

echo "=== done $(date -Is): mc_speed.json, mc_probe_aiida.json, mc_bench.json in $MAN ==="

#!/bin/bash
#SBATCH -J zc-recover
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=20             # ~132 GiB: the 8.15 GB AIMD vasprun of 13843222 needs ~80-100 GB
                                       # (staging: ~7 GB of re-fetched archives, ~10 GB extracted)
#SBATCH --time=06:00:00
#SBATCH -o logs/zc-recover-%j.out
#SBATCH -e logs/zc-recover-%j.err
#SBATCH --mail-type=END,FAIL
#
# Targeted recovery after the census T1 pipeline (docs/ZENODO_CENSUS.md §11): re-parse, in place,
# calcs the pipeline rejected for a reason that is now fixed or was only a resource limit — their
# files are still staged under RAW_DIR (purge-raw never deletes an unparsed unit's files):
#   * 7506565  — vaspruns with a numeric ALGO (pymatgen 'int'.lower() bug; parse.py now guards it)
#   * 13843222 — three AIMD vaspruns above the 8-core job's 3.8 GB primary_too_large cap
#   * 22084774 — one band-structure vasprun above the same cap
# and RE-FETCHES (FETCH_RECIDS) records a fetch-side gap closed, now that fetch can read them:
#   * 14809725 — one unreadable member aborted its 1.2 GB zip (~110 single points lost); fetch now
#                skips just that member
#   * 3359829  — a 5.4 GB zip written without ZIP64 (offsets truncated to 32 bits: "Truncated file
#                header"); fetch now finds each member at its offset +-k*4 GiB
# Re-fetched records are re-parsed from the fresh download (their already-stored calcs skipped).
# ONE dataset writer at a time: run it when no pipeline/parse job uses DATASET_DIR (the T2 triage
# may run alongside — it never touches the dataset). Re-running is safe: parsed calcs are skipped
# (no duplicates); calcs that still fail are re-rejected (one more rejections.jsonl line each).
#
#   sbatch scripts/csd3/census/40_recover_t1.sh            # the defaults above
#   RECIDS="..." FETCH_RECIDS="..." sbatch scripts/csd3/census/40_recover_t1.sh   # others ("" = none)
#
# The same script serves the T2 run (docs/ZENODO_CENSUS.md §11, 2026-10-02): 11234637 (OUTCARs with
# non-UTF-8 bytes) + 20403107 / 17254051 (VASPsol LAMBDA_D_K=**** in <parameters>) — both read by
# parse.py since 2026-10-02 —, 21316138 (three ~3.5 GB OUTCARs past the 20 min timeout), and a
# re-fetch of 18706703 (HTTP 429 on its 16.4 GB zip):
#   KEEP=$ZENODO_CENSUS_DATA/census_keep_t2.jsonl WORK=$ZENODO_CENSUS_DATA/recover_t2 \
#     RECIDS="11234637 20403107 17254051 21316138" FETCH_RECIDS="18706703" \
#     sbatch scripts/csd3/census/40_recover_t1.sh
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export ZENODO_CENSUS_DATA="${ZENODO_CENSUS_DATA:-$ZENODO_HARVEST_DATA/census}"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   (before sbatch)
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR" logs
export PATH="$HOME/bin:$PATH"
cd "${SLURM_SUBMIT_DIR:-.}"

KEEP="${KEEP:-$ZENODO_CENSUS_DATA/census_keep_t1.jsonl}"
PARTS_DIR="${PARTS_DIR:-${KEEP%.jsonl}.pipeline_parts}"
RAW_DIR="${RAW_DIR:-$ZENODO_HARVEST_DATA/raw_census}"
DATASET_DIR="${DATASET_DIR:-$ZENODO_HARVEST_DATA/dataset}"
PARSE_REJ="$DATASET_DIR/rejections.jsonl"                 # where the pipeline logs parse rejections
FETCH_REJ="$(dirname "$RAW_DIR")/manifests/rejections.jsonl"  # ... and fetch rejections
RECIDS="${RECIDS:-7506565 13843222 22084774}"
FETCH_RECIDS="${FETCH_RECIDS-14809725 3359829}"   # (22171731: still HTTP 403 on 2026-10-01 — not worth it)
WORK="${WORK:-$ZENODO_CENSUS_DATA/recover_t1}"
PARSE_WORKERS="${PARSE_WORKERS:-2}"
RSS_RATIO="${RSS_RATIO:-12}"
FETCH_RESERVE_GIB="${FETCH_RESERVE_GIB:-10}"
PARSE_TIMEOUT="${PARSE_TIMEOUT:-7200}"   # an 8 GB vasprun takes far longer than the pipeline's 20 min
# RAM -> parse budget -> per-file cap, exactly as 20_pipeline.sh (-c 20 himem: ~122 GiB, cap ~10.9 GB)
if [[ -n "${SLURM_MEM_PER_NODE:-}" ]]; then JOB_RAM_MIB="$SLURM_MEM_PER_NODE"
else JOB_RAM_MIB=$(( ${SLURM_CPUS_PER_TASK:-20} * ${SLURM_MEM_PER_CPU:-6760} )); fi
PARSE_MEM_BUDGET="${PARSE_MEM_BUDGET:-$(( JOB_RAM_MIB * 1048576 - FETCH_RESERVE_GIB * 1073741824 ))}"
MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-$(( (PARSE_MEM_BUDGET - 536870912) / RSS_RATIO ))}"

[[ -s "$KEEP" && -d "$PARTS_DIR" ]] || { echo "ERROR: $KEEP / $PARTS_DIR missing" >&2; exit 2; }
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "ERROR: $DATASET_DIR/.parse.lock exists — is a pipeline/parse still running? (squeue -u \$USER)" >&2
    exit 2
fi
mkdir -p "$WORK"
SUBSET="$WORK/recover.fetched.jsonl"
echo "=== census recovery ($(basename "$KEEP")) $(date -Is) on $(hostname): recids [$RECIDS]" \
     "refetch [${FETCH_RECIDS:-none}]"
echo "    parse_workers=$PARSE_WORKERS mem_budget=$(( PARSE_MEM_BUDGET / 1073741824 )) GiB" \
     "max_primary=$MAX_PRIMARY_BYTES B timeout=${PARSE_TIMEOUT}s"

# 1. the selected records' lines from the pipeline's own fetched manifests (paths relative to RAW_DIR)
python - "$PARTS_DIR" "$SUBSET" "$FETCH_RECIDS" $RECIDS <<'PY'
import glob, json, sys
parts, out, refetch = sys.argv[1], sys.argv[2], set(sys.argv[3].split())
want = set(sys.argv[4:]) - refetch          # a re-fetched record comes from its new manifest
lines = {}
for p in sorted(glob.glob(f"{parts}/*.fetched.jsonl")):
    for line in open(p):
        if line.strip():
            rec = json.loads(line)
            if str(rec.get("recid")) in want:
                lines[str(rec["recid"])] = line if line.endswith("\n") else line + "\n"
missing = sorted(want - set(lines))
if missing:
    print(f"    WARNING: not in any fetched manifest (never fetched?): {missing}", file=sys.stderr)
with open(out, "w") as fh:
    fh.writelines(lines.values())
print(f"    subset manifest: {len(lines)} record(s) -> {out}")
PY

# 2. optional re-fetch (records closed by a fetch error that may not recur, e.g. an HTTP 403)
if [[ -n "$FETCH_RECIDS" ]]; then
    REKEEP="$WORK/refetch.keep.jsonl"
    python - "$KEEP" "$REKEEP" $FETCH_RECIDS <<'PY'
import json, sys
keep, out, want = sys.argv[1], sys.argv[2], set(sys.argv[3:])
rows = [l for l in open(keep) if l.strip() and str(json.loads(l).get("recid")) in want]
open(out, "w").writelines(rows)
print(f"    re-fetch keep-list: {len(rows)} record(s)")
PY
    python -m zenodo_harvest.cli -v fetch --in "$REKEEP" --out "$WORK/refetch.fetched.jsonl" \
        --raw-dir "$RAW_DIR" --rejections "$FETCH_REJ" --retry-rejected --max-bytes 0 \
        --max-member-bytes 30000000000
    [[ -s "$WORK/refetch.fetched.jsonl" ]] && cat "$WORK/refetch.fetched.jsonl" >> "$SUBSET"
fi

# 3. re-parse: --retry-rejected re-attempts their rejected calcs (already-stored ones are skipped)
python -m zenodo_harvest.cli -v parse --in "$SUBSET" --dataset-dir "$DATASET_DIR" \
    --raw-dir "$RAW_DIR" --rejections "$PARSE_REJ" --retry-rejected \
    --max-primary-bytes "$MAX_PRIMARY_BYTES" --parse-timeout "$PARSE_TIMEOUT" \
    --parse-workers "$PARSE_WORKERS" --parse-mem-budget "$PARSE_MEM_BUDGET" \
    --parse-rss-ratio "$RSS_RATIO"

# 4. the metadata<->shard bijection must still hold; then free what is now parsed
python -m zenodo_harvest.cli verify --dataset-dir "$DATASET_DIR" > "$WORK/verify.json"
python - "$WORK/verify.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))
print(f"    verify ok={v.get('ok')} calcs={v.get('stats', {}).get('n_calcs')} "
      f"frames={v.get('integrity', {}).get('n_frames_metadata')}")
sys.exit(0 if v.get("ok") else 1)
PY
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW_DIR" --dataset-dir "$DATASET_DIR" \
    --fetched "$SUBSET" > "$WORK/purge.json"
echo "=== done $(date -Is)"

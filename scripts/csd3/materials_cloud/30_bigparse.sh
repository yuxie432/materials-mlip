#!/bin/bash
#SBATCH -J mc-bigparse
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32             # bought for RAM: 32 x 6760 MiB = ~211 GiB for ONE big parse
#SBATCH --time=12:00:00
#SBATCH -o logs_mc/mc-bigparse-%j.out
#SBATCH -e logs_mc/mc-bigparse-%j.err
#SBATCH --mail-type=END,FAIL
#
# Phase B of the Materials Cloud harvest (run AFTER 20_pipeline.sh has finished — never alongside
# it: both write the same dataset dir). The pipeline parses with several workers under a moderate
# --max-primary-bytes; any bigger vasprun/OUTCAR (long AIMD) was logged `primary_too_large` and KEPT
# staged (purge-raw never deletes an unparsed unit). This job re-runs the shared parse over every
# part's fetched manifest ONE worker at a time with a much larger cap: the parser's resume logic
# skips everything already parsed or deterministically failed, and re-attempts exactly the
# deferred calcs because the cap is now higher than the one they were refused under
# (parse._rejected_calc_ids). Then purge-raw reclaims their staging, and verify gates the result.
#
# RAM rule (pymatgen ~10-12x the uncompressed file; check mc_bench.json's worst_rss_ratio):
#   cpus x 6760 MiB >= ratio x MAX_PRIMARY_BYTES + ~8 GiB   ->  32 cpus: ~16 GB at 12x.
# Anything still above the cap stays staged + logged; rerun with more cpus and a higher cap.
set -euo pipefail

export MC_HARVEST_DATA="${MC_HARVEST_DATA:-/rds/user/$USER/hpc-work/materials_cloud}"
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$MC_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR" logs_mc
cd "${SLURM_SUBMIT_DIR:-.}"

MAX_PRIMARY_BYTES="${MAX_PRIMARY_BYTES:-16000000000}"
PARSE_TIMEOUT="${PARSE_TIMEOUT:-14400}"          # 4 h per calc: a multi-GB AIMD parse is slow
MAN="$MC_HARVEST_DATA/manifests"
RAW_DIR="${RAW_DIR:-$MC_HARVEST_DATA/raw}"
DATASET_DIR="${DATASET_DIR:-$MC_HARVEST_DATA/dataset}"
PARTS_DIR="${PARTS_DIR:-$MAN/mc_keep.pipeline_parts}"

shopt -s nullglob
FETCHED=("$PARTS_DIR"/*.fetched.jsonl)
if [[ ${#FETCHED[@]} -eq 0 ]]; then
    echo "ERROR: no fetched part manifests under $PARTS_DIR — run 20_pipeline.sh first." >&2
    exit 2
fi
N_DEFERRED=$(grep -h '"primary_too_large"' "$DATASET_DIR/rejections.jsonl" 2>/dev/null | wc -l || true)
echo "=== mc bigparse $(date -Is) on $(hostname): cap=$MAX_PRIMARY_BYTES, ${N_DEFERRED} primary_too_large line(s) logged ==="

# The pipeline must NOT be running; a lock left by a SIGKILLed pipeline job is stale.
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    if squeue -h -u "$USER" -n mc-pipeline 2>/dev/null | grep -q .; then
        echo "ERROR: an mc-pipeline job is still queued/running — wait for it to finish." >&2
        exit 3
    fi
    echo "clearing stale parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

for f in "${FETCHED[@]}"; do
    echo "--- $(basename "$f") $(date -Is)"
    python -m zenodo_harvest.cli -v parse --in "$f" --raw-dir "$RAW_DIR" \
        --dataset-dir "$DATASET_DIR" --rejections "$DATASET_DIR/rejections.jsonl" \
        --max-primary-bytes "$MAX_PRIMARY_BYTES" --parse-timeout "$PARSE_TIMEOUT"
    python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW_DIR" --dataset-dir "$DATASET_DIR" \
        --fetched "$f" > /dev/null
done

echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DATASET_DIR" > "logs_mc/mc-bigparse-${SLURM_JOB_ID:-local}.verify.json"
python - "$DATASET_DIR" <<'PY'
import json, sys
from pathlib import Path
from zenodo_harvest.manifest import read_jsonl
rej = Path(sys.argv[1]) / "rejections.jsonl"
last = {}
if rej.is_file():
    for r in read_jsonl(rej):
        if r.get("stage") == "parse":
            last[r["id"]] = r.get("reason")
meta = Path(sys.argv[1]) / "metadata.jsonl"
done = {r["calc_id"] for r in read_jsonl(meta)} if meta.is_file() else set()
left = sorted(c for c, why in last.items() if why == "primary_too_large" and c not in done)
print(f"still primary_too_large (not in dataset): {len(left)}")
for c in left[:20]:
    print("  ", c)
PY
echo "=== done $(date -Is) ==="

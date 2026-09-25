#!/bin/bash
#SBATCH -J zc-triage
# Account is NOT hardcoded. Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2              # request-paced peeks; RAM for AiiDA sqlite checks is small
#SBATCH --time=12:00:00
#SBATCH --signal=B:USR1@600            # 10 min before wallclock -> queue the resume job
#SBATCH -o logs_census/zc-triage-%j.out
#SBATCH -e logs_census/zc-triage-%j.err
#SBATCH --mail-type=END,FAIL
#
# Zenodo census, stage 1' (docs/ZENODO_CENSUS.md), after 20_score.sh (and a look at its report):
# peek the archives of the selected records — zip central directories (ZIP64-aware) and the first
# ~8 MB of tar-family streams — under ONE request pacer (INTERVAL s between request starts; 0.8 =
# 75/min = 4.5k/h, inside Zenodo's documented 100/min + 5,000/h), then decide per the 2026-09-25
# policy and write an ordinary Zenodo keep-list for scripts/csd3/20_pipeline.sh.
#
# Default run: every T1 + T2 record, a RESIDUAL_SAMPLE of non-software T3 records (the residual
# blind-spot measurement) and a NEGATIVE_SAMPLE of T0 records (filter check).
# Second run, ONLY if the report's residual rate justifies it (~1 in 2,000 or better):
#   TIERS=T3 RESIDUAL_SAMPLE=0 NEGATIVE_SAMPLE=0 OUT=$ZENODO_CENSUS_DATA/census_keep_t3.jsonl \
#     RESUBMIT=1 sbatch scripts/csd3/census/30_triage.sh
# (it skips everything the first keep-list already holds and reuses the shared peek cache).
#
# Resumable: every completed peek is cached ($ZENODO_CENSUS_DATA/peeks.jsonl); RESUBMIT=1 chains
# follow-on jobs across wallclock kills (MAX_ATTEMPTS bounds the chain). The keep-list + report
# are written once all peeks are done.
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export ZENODO_CENSUS_DATA="${ZENODO_CENSUS_DATA:-$ZENODO_HARVEST_DATA/census}"
# sqlite AiiDA databases pulled by the zip peek go to $TMPDIR: node-local /local, not RAM/quota.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_CENSUS_DATA/tmp"; fi
mkdir -p "$TMPDIR"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   (before sbatch)
cd "${SLURM_SUBMIT_DIR:-.}"            # repo root (.env with ZENODO_TOKEN)

TIERS="${TIERS:-T1 T2}"
TYPES="${TYPES:-}"                       # e.g. "dataset publication other" (default: all but software)
RESIDUAL_SAMPLE="${RESIDUAL_SAMPLE:-3000}"
NEGATIVE_SAMPLE="${NEGATIVE_SAMPLE:-300}"
INTERVAL="${INTERVAL:-0.8}"
PEEK_WORKERS="${PEEK_WORKERS:-3}"
OUT="${OUT:-$ZENODO_CENSUS_DATA/census_keep.jsonl}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-6}"
ATTEMPT="${ATTEMPT:-1}"

mkdir -p logs_census
JOB="${SLURM_JOB_ID:-local}"
# earlier census keep-lists (not their .licence_review / .rejections side files, not $OUT itself)
EXCL=()
for k in "$ZENODO_CENSUS_DATA"/census_keep.jsonl "$ZENODO_CENSUS_DATA"/census_keep_*.jsonl; do
    case "$k" in *.licence_review.jsonl|*.rejections.jsonl) continue ;; esac
    if [[ -f "$k" && "$k" != "$OUT" ]]; then EXCL+=("$k"); fi
done
TYPE_ARGS=()
if [[ -n "$TYPES" ]]; then
    # shellcheck disable=SC2206
    TYPE_ARGS=(--types $TYPES)
fi

NEXT_JOBID=""
submit_successor() {
    if [[ "${RESUBMIT:-0}" != "0" && -z "$NEXT_JOBID" && "$ATTEMPT" -lt "$MAX_ATTEMPTS" ]]; then
        NEXT_JOBID=$(sbatch --parsable --dependency="afterany:${SLURM_JOB_ID}" \
            --export="ALL,ATTEMPT=$((ATTEMPT + 1)),RESUBMIT=1" "$0") || NEXT_JOBID=""
        echo "=== queued resume job ${NEXT_JOBID:-<sbatch failed>} (attempt $((ATTEMPT + 1))) ==="
    fi
}
trap 'submit_successor' USR1

echo "=== triage attempt $ATTEMPT/$MAX_ATTEMPTS $(date -Is): tiers=[$TIERS] out=$OUT ==="
echo "    residual sample $RESIDUAL_SAMPLE, negative sample $NEGATIVE_SAMPLE, interval ${INTERVAL}s"
echo "    skipping records already in: ${EXCL[*]:-<none>}"
set +e
# shellcheck disable=SC2086
python -m zenodo_census.cli -v triage --tiers $TIERS "${TYPE_ARGS[@]}" \
    --residual-sample "$RESIDUAL_SAMPLE" --negative-sample "$NEGATIVE_SAMPLE" \
    --interval "$INTERVAL" --peek-workers "$PEEK_WORKERS" --out "$OUT" \
    --exclude-keep "${EXCL[@]}" > "logs_census/zc-triage-$JOB.summary.json" &
PID=$!
while true; do
    wait "$PID"; rc=$?
    if [[ "$rc" -le 128 ]] || ! kill -0 "$PID" 2>/dev/null; then break; fi
done
set -e
echo "=== triage exit=$rc $(date -Is) ==="
if [[ "$rc" -ne 0 ]]; then
    submit_successor
elif [[ -n "$NEXT_JOBID" ]]; then
    scancel "$NEXT_JOBID" 2>/dev/null || true
fi
python -m zenodo_census.cli status || true
if [[ "$rc" -eq 0 ]]; then
    echo "keep-list: $OUT"
    echo "  report: ${OUT%.jsonl}.report.json   licence review: ${OUT%.jsonl}.licence_review.jsonl"
    echo "  next (after a metadata backup): IN=$OUT RAW_DIR=$ZENODO_HARVEST_DATA/raw_census \\"
    echo "        RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh"
fi
exit "$rc"

#!/bin/bash
#SBATCH -J zh-t30-array
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --time=00:40:00
#SBATCH -o logs/zh-t30-array-%j.out
#SBATCH -e logs/zh-t30-array-%j.err
#
# SMOKE TEST for the ARRAY-JOB dataset path (split -> per-task parse -> merge-datasets ->
# verify -> purge-raw), plus merge RESUME idempotency. For the smoke test the N per-task
# parses are run SERIALLY in this one job (the code under test — split/merge/verify/purge —
# is identical; only the SLURM array fan-out differs). The real many-core flow is
# 30_parse_array.sh + 31_merge_verify.sh; see the README.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo_smoketest}"
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate   (before sbatch)
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

PARTS="${PARTS:-3}"
mkdir -p logs
MAN="$ZENODO_HARVEST_DATA/manifests"
if [[ ! -s "$MAN/keep.jsonl" ]]; then
    echo "ERROR: $MAN/keep.jsonl missing — run t10_smoke_discover.sh first." >&2
    exit 2
fi
A="$ZENODO_HARVEST_DATA/array"
rm -rf "$A"; mkdir -p "$A"
RAW="$A/raw"; FET="$A/fetched.jsonl"

echo "=== [t30] fetch keep-list (no purge; array parse needs the staged files) $(date -Is) ==="
python -m zenodo_harvest.cli fetch --in "$MAN/keep.jsonl" --raw-dir "$RAW" --out "$FET" \
    --rejections "$A/fetch_rej.jsonl" --max-bytes 0 \
    --max-disk-bytes 800000000000 --max-disk-files 800000 \
    | python -c "import sys,json;d=json.load(sys.stdin);print('  fetched=%s calc_units=%s'%(d['fetched'],d['calc_units']))"

echo "=== [t30] split fetched.jsonl into $PARTS parts $(date -Is) ==="
python -m zenodo_harvest.cli split --in "$FET" --parts "$PARTS" --out-dir "$A/parts" \
    | python -c "import sys,json;d=json.load(sys.stdin);print('  parts:',[(p['path'].split('/')[-1],p['lines']) for p in d['parts_written']])"

echo "=== [t30] parse each part into its OWN task dataset dir (array-task model) $(date -Is) ==="
for ((i=0; i<PARTS; i++)); do
    idx=$(printf "%03d" "$i")
    PART="$A/parts/fetched.part-$idx.jsonl"
    [[ -s "$PART" ]] || { echo "  part $idx empty; skip"; continue; }
    python -m zenodo_harvest.cli parse --in "$PART" \
        --dataset-dir "$A/task-$idx" --raw-dir "$RAW" --rejections "$A/task-$idx/rej.jsonl" \
        | python -c "import sys,json;d=json.load(sys.stdin);print('  task-$idx: calcs=%s frames=%s'%(d['calcs_parsed'],d['frames']))"
done

echo "=== [t30] merge-datasets -> one dir $(date -Is) ==="
python -m zenodo_harvest.cli -v merge-datasets --into "$A/merged" "$A"/task-* \
    | python -c "import sys,json;d=json.load(sys.stdin);print('  merge ok=%s shards_moved=%s records_appended=%s integrity.ok=%s'%(d['ok'],d['shards_moved'],d['records_appended'],d['integrity']['ok']))"

echo "=== [t30] merge RESUME idempotency: re-run merge (sources already merged -> skipped) ==="
python -m zenodo_harvest.cli merge-datasets --into "$A/merged" "$A"/task-* \
    | python -c "import sys,json;d=json.load(sys.stdin);print('  re-merge ok=%s skipped_already_merged=%s'%(d['ok'],len(d['sources_skipped_already_merged'])))"

echo "=== [t30] verify merged dataset $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$A/merged" \
    | python -c "import sys,json;d=json.load(sys.stdin);print('  verify ok=%s calcs=%s frames meta=%s disk=%s'%(d['ok'],d['stats']['n_calcs'],d['integrity']['n_frames_metadata'],d['integrity']['n_frames_on_disk']))"

echo "=== [t30] purge-raw (reclaim the array staging) $(date -Is) ==="
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$A/merged" --fetched "$FET" \
    | python -c "import sys,json;d=json.load(sys.stdin);print('  recids_purged=%s bytes_freed=%.1fMB files_removed=%s'%(d['recids_purged'],d['bytes_freed']/1e6,d['files_removed']))"
echo "  raw files remaining: $(find "$RAW" -type f 2>/dev/null | wc -l)  (expect 0)"

echo "=== [t30] inspect merged dataset $(date -Is) ==="
python scripts/csd3/test/inspect_dataset.py --dataset-dir "$A/merged"
echo "=== [t30] done $(date -Is) ==="

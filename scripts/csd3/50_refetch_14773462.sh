#!/bin/bash
#SBATCH -J zh-refetch-14773462
# Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core; normal-size COC/BPN vaspruns
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # ~27 GiB: resume frame_id set (~2 GiB) + one parse (<=2 GB cap x ~12)
#SBATCH --time=06:00:00
#SBATCH -o logs/zh-refetch-14773462-%j.out
#SBATCH -e logs/zh-refetch-14773462-%j.err
#SBATCH --mail-type=END,FAIL
#
# ============================================================================================
# Recover 14773462 ("Chemical functionalization of a 2D material — COC on BPN") — the record whose
# truncated vasprun once HUNG the pipeline in the ASE OUTCAR reader. It was fetched (279 calc_units),
# 58 calcs parsed into the dataset, then the remaining files were manually `rm`'d as the stall
# stopgap -> the other 221 calcs failed with FileNotFoundError (logged vasprun/outcar_parse_error).
# The `--parse-timeout` guard added afterwards now kills a hanging parse, so a clean re-fetch + parse
# recovers ~219 of the 221 (the 1-2 genuinely-hanging/truncated ones time out and are skipped).
#
# NO DUPLICATES, NO MISSING DATA (the two things to get right here):
#   * Re-fetch goes to a FRESH raw dir (raw_14773462); the SAME .tar extractor reproduces the SAME
#     member paths, hence the SAME calc_ids as the original parse.
#   * Parse writes DIRECTLY into the existing dataset. Its resume reads metadata.jsonl and SKIPS the
#     58 already-committed calc_ids -> those are NOT re-parsed (no duplicate frames). This is the exact
#     mechanism just proven by the RAR job, which skipped 274 already-present calcs of 13744522.
#   * `--retry-rejected` (+ a FRESH rejections file) forces the 221 previously-rejected calcs to be
#     re-attempted now that their files exist again -> no missing data.
#   * SANITY: the parse summary's `skipped_existing` should be ~58. If it is ~0, the re-fetch produced
#     DIFFERENT paths (calc_id mismatch) and the 58 would be about to duplicate -> STOP and investigate
#     before trusting the result (the final verify would still pass, but you'd have 58 dupes).
#
# All scratch on /rds (never /tmp/local). Fully resumable: re-`sbatch` if wallclock is hit (fetch skips
# the staged tar, parse skips everything already committed).
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs && sbatch scripts/csd3/50_refetch_14773462.sh
# ============================================================================================
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"

RECID="14773462"
ZH="$ZENODO_HARVEST_DATA"
MAN="$ZH/manifests"; DS="$ZH/dataset"; RAW="$ZH/raw_${RECID}"
KEEP="$MAN/keep.jsonl"
REKEEP="$MAN/refetch_${RECID}_keep.jsonl"
FETCHED="$MAN/refetch_${RECID}_fetched.jsonl"
REJ="$MAN/refetch_${RECID}_rejections.jsonl"

echo "=== refetch-$RECID START $(date -Is) on $(hostname) ==="
df -h "$ZH" 2>/dev/null | tail -1 || true
if [[ -e "$DS/.parse.lock" ]]; then echo "clearing stale lock"; rm -f "$DS/.parse.lock"; fi

# how many of this record's calcs are already committed (expected skipped_existing later)
PRE=$(RECID="$RECID" DS="$DS" python3 -c "import json,os;print(sum(1 for l in open(os.path.join(os.environ['DS'],'metadata.jsonl')) if json.loads(l)['provenance']['record_id']==os.environ['RECID']))")
echo "  $RECID already in dataset: $PRE calcs (these MUST be skipped, not re-parsed)"

# 1. extract EXACTLY this record's keep-list entry (exact recid match)
RECID="$RECID" KEEP="$KEEP" REKEEP="$REKEEP" python - <<'PY'
import json, os, sys
recid, keep, out = os.environ["RECID"], os.environ["KEEP"], os.environ["REKEEP"]
n = 0
with open(out, "w") as fo:
    for line in open(keep):
        line = line.strip()
        if line and str(json.loads(line).get("recid")) == recid:
            fo.write(line + "\n"); n += 1
print(f"keeplist: {n} entry for {recid}")
if n != 1:
    sys.exit(f"expected exactly 1 keep entry for {recid}, got {n}")
PY

# 2. re-fetch into a FRESH raw dir (whole 4.6 GB tar download + selective VASP extraction)
echo "=== fetch $(date -Is) ==="
python -m zenodo_harvest.cli fetch \
    --in "$REKEEP" \
    --out "$FETCHED" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-bytes 0

# 3. parse directly into the dataset: resume SKIPS the committed 58, --retry-rejected re-attempts
#    the 221; --parse-timeout kills the hanging one(s) that caused the original stall.
echo "=== parse (--retry-rejected) $(date -Is) ==="
if [[ ! -f "$DS/metadata.jsonl.bak.pre_refetch_${RECID}" ]]; then
    cp "$DS/metadata.jsonl" "$DS/metadata.jsonl.bak.pre_refetch_${RECID}"
    echo "backed up metadata.jsonl -> metadata.jsonl.bak.pre_refetch_${RECID}"
fi
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DS" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --retry-rejected \
    --max-primary-bytes 2000000000 \
    --parse-timeout 1200

# 4. verify
echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DS"

echo "=== refetch-$RECID DONE $(date -Is) ==="
RECID="$RECID" DS="$DS" PRE="$PRE" python - <<'PY'
import json, os
recid, pre = os.environ["RECID"], int(os.environ["PRE"])
n = c = 0
for line in open(os.path.join(os.environ["DS"], "metadata.jsonl")):
    d = json.loads(line)
    if d["provenance"]["record_id"] == recid:
        c += 1; n += (d.get("quality", {}) or {}).get("n_frames") or len(d.get("frame_ids", []))
print(f"  {recid} now in dataset: {c} calcs / {n} frames  (was {pre} calcs; +{c - pre} recovered)")
print("  NOTE: check the parse summary above said skipped_existing ~= %d (else investigate dupes)." % pre)
PY

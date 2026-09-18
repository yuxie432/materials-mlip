#!/bin/bash
#SBATCH -J zh-discover-nc
# Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # rate-limited to 30 req/min: one core is plenty
#SBATCH --time=12:00:00
#SBATCH -o logs/zh-discover-nc-%j.out
#SBATCH -e logs/zh-discover-nc-%j.err
#SBATCH --mail-type=END,FAIL
#
# ============================================================================================
# APPROACH 1 (stage 0-1) — LICENCE EXPANSION to NonCommercial (NC + NC-SA).
# ============================================================================================
# The main harvest's licence gate (models.is_reusable_license, applied at DISCOVER) drops NC/ND/
# no-licence records and never persists them, so recovering the NC ones needs a re-discover with
# the gate OFF, then a post-filter to exactly the NC set (the user's chosen scope: non-commercial,
# derivatives+redistribution allowed — cc-by-nc / cc-by-nc-sa; NOT ND, NOT no-licence).
#
# Steps:
#   0. discover --no-license-gate (exhaustive) -> ALL-licence candidates (still access_right=open only).
#   *  NC filter: keep licence with an `nc` token and NO `nd` token and not no-licence; DROP any recid
#      already in the dataset (diff vs metadata.jsonl) so the expansion adds only genuinely-new records.
#   1. triage (peek on) -> nc_keep.jsonl for the pipeline (53_submit_nc_pipeline.sh).
# The re-discover also sees records published since the first run; the NC filter keeps only the NC ones
# (new cc-by etc. are out of scope for a *licence* expansion and are dropped by the filter).
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs && sbatch scripts/csd3/52_discover_nc.sh
# Single-stream by design (Zenodo caps /api/records at 30 req/min even with a token). ~1 h.
# ============================================================================================
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
cd "${SLURM_SUBMIT_DIR:-.}"           # repo root (holds .env with ZENODO_TOKEN)
mkdir -p logs "$ZENODO_HARVEST_DATA/manifests"

ZH="$ZENODO_HARVEST_DATA"
MAN="$ZH/manifests"; DS="$ZH/dataset"
CAND="$MAN/candidates_nolicense.jsonl"     # discover output (all licences)
NC_CAND="$MAN/nc_candidates.jsonl"         # NC-filtered + diffed
NC_KEEP="$MAN/nc_keep.jsonl"               # triaged keep-list for the pipeline

echo "=== stage 0: discover (exhaustive, NO licence gate) $(date -Is) ==="
python -m zenodo_harvest.cli -v discover --exhaustive --no-license-gate \
    --resource-type dataset --resource-type software --resource-type publication \
    --out "$CAND"

echo "=== NC filter + dataset diff $(date -Is) ==="
CAND="$CAND" NC_CAND="$NC_CAND" DS="$DS" python - <<'PY'
import json, os, re, collections
cand, out, ds = os.environ["CAND"], os.environ["NC_CAND"], os.environ["DS"]
NO_LICENSE = {"", "notspecified", "all-rights-reserved", "arr", "closed",
              "restricted", "copyright", "none"}
def is_nc(lic):
    if lic is None:
        return False
    n = str(lic).strip().lower()
    if n in NO_LICENSE:
        return False
    toks = set(re.split(r"[-_.\s]+", n))
    return "nc" in toks and "nd" not in toks          # NonCommercial, but NOT NoDerivatives
# recids already in the dataset (so the expansion never re-does an existing record)
have = set()
meta = os.path.join(ds, "metadata.jsonl")
if os.path.isfile(meta):
    for line in open(meta):
        line = line.strip()
        if line:
            have.add(json.loads(line)["provenance"]["record_id"])
kept = 0; by_lic = collections.Counter(); skipped_have = 0
with open(out, "w") as fo:
    for line in open(cand):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        if not is_nc(d.get("license")):
            continue
        if str(d.get("recid")) in have:
            skipped_have += 1
            continue
        fo.write(line + "\n"); kept += 1; by_lic[str(d.get("license"))] += 1
print(f"NC candidates kept: {kept}  (already-in-dataset skipped: {skipped_have})")
for lic, c in by_lic.most_common():
    print(f"    {c:5d}  {lic}")
PY

echo "=== stage 1: triage (peek on) $(date -Is) ==="
python -m zenodo_harvest.cli -v triage --in "$NC_CAND" --out "$NC_KEEP" --min-rank 3

echo "=== done $(date -Is): $(wc -l < "$NC_KEEP") NC records to fetch -> $NC_KEEP ==="
echo "Next (fetch+parse DIRECT into the production dataset; back up metadata first):"
echo "  cp $DS/metadata.jsonl $DS/metadata.jsonl.bak.pre_nc"
echo "  IN=$NC_KEEP RAW_DIR=$ZH/raw_nc RESUBMIT=1 PARTS=8 sbatch scripts/csd3/20_pipeline.sh"

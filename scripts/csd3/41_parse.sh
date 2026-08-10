#!/bin/bash
#SBATCH -J zh-parse
#SBATCH -p icelake-himem                # pymatgen peak RSS ~10x the vasprun size — needs the RAM
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 4                            # ~26 GiB; sizes --max-primary-bytes (~0.85*c*6.76GB/10 ≈ 2 GB)
#SBATCH -t 12:00:00
#SBATCH -o logs/zh-parse-%j.out
#SBATCH -e logs/zh-parse-%j.err
#SBATCH --mail-type=END,FAIL
#
# Mop-up parse: collect frames from FETCHED-but-not-yet-parsed calcs (the ~498 "unaccounted"
# calcs harvest_audit reported) and re-attempt the non-terminal skips (primary_too_large,
# parse_timeout). NO fetch, NO network — it only parses raw already on disk into the dataset,
# skipping calcs already parsed or terminally rejected. Resumable: if it hits wallclock, just
# resubmit and it continues.
#
# Before submitting (STOP the frozen pipeline first — parse takes the dataset's .parse.lock):
#   scancel <pipeline-jobid>
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU     # from `mybalance`
#   source <path-to>/.venv/bin/activate          # the harvest env (pymatgen + ase)
#   mkdir -p logs
#   sbatch scripts/csd3/41_parse.sh

set -uo pipefail
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
cd "${SLURM_SUBMIT_DIR:-$PWD}"
# Parse decompresses/copies primaries to a temp dir; keep that on fast node-local disk.
if [[ -d /local && -w /local ]]; then export TMPDIR=/local; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; mkdir -p "$TMPDIR"; fi

MAN="$ZENODO_HARVEST_DATA/manifests"; DS="$ZENODO_HARVEST_DATA/dataset"; RAW="$ZENODO_HARVEST_DATA/raw"

# Concatenate the per-part fetched manifests, DEDUPED by recid: overlapping part manifests
# left by earlier resubmits would otherwise present a calc twice and, if it is not yet in the
# dataset, get parsed twice -> duplicate frames. Keeping the first line per recid avoids that.
DEDUP="$MAN/all_fetched.dedup.jsonl"
python - "$MAN" > "$DEDUP" <<'PY'
import json, sys, pathlib
man = pathlib.Path(sys.argv[1]); seen = set()
for p in sorted(man.rglob("*.fetched.jsonl")):
    with open(p) as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            try:
                r = json.loads(ln).get("recid")
            except Exception:
                continue
            if r and r in seen:
                continue
            if r:
                seen.add(r)
            print(ln)
PY

echo "=== zh-parse $(date -Is) on $(hostname); $(wc -l < "$DEDUP") records to scan ==="
python -m zenodo_harvest.cli -v parse --in "$DEDUP" \
    --dataset-dir "$DS" --raw-dir "$RAW" \
    --max-primary-bytes 2000000000 --parse-timeout 1200
rc=$?
echo "=== zh-parse exit=$rc $(date -Is) ==="
exit "$rc"

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
                                       # -> ~16 GiB headroom. The 8.51 GB monster is EXCLUDED
                                       # below; do it separately with cpus=24. --parse-timeout>0
                                       # runs each calc in a child, so a cgroup OOM kills only that
                                       # child (parse_worker_died, retryable) and the job survives.
#SBATCH --time=04:00:00                # ~24 small-ish calcs; serial finishes well under this
#SBATCH -o logs_nomad/nomad-recover-bigvasprun-%j.out
#SBATCH -e logs_nomad/nomad-recover-bigvasprun-%j.err
#SBATCH --mail-type=END,FAIL
#
# Re-parse the primary_too_large long-AIMD vasprun.xml calcs (deferred by the RAM cap) into the
# EXISTING dataset, uncapped + SERIAL, then verify + purge-raw. No re-fetch — the staged files are
# still on disk (a parse failure never purges). Same pymatgen/parse/store as the 7M existing calcs
# -> consistent. The subset fetched-manifest is built HERE (compute node) by a fast grep over the
# part manifests, so nothing needs running on the login node first.
#
# WHY SERIAL (the standalone `parse` default): only ~24 calcs, throughput is irrelevant; serial
# keeps peak RAM at ONE multi-GB parse and makes the budget trivial. RAM is the binding constraint.
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
PARTS_DIR="$MAN/nomad_keep.pipeline_parts"
FETCHED="$MAN/nomad_bigvasprun_fetched.jsonl"

# Build a fetched-manifest SUBSET for a class of parse rejections, FAST: a small python reads the
# (small) dataset rejections.jsonl to list the target recids, then `grep -F` pulls their fetched
# lines out of the part manifests without JSON-parsing all ~7M lines (that json.loads-every-line
# scan is what hung on the login node). Args: $1=reason $2=signature(may be "") $3=exclude-csv $4=out.
build_subset_manifest() {
    local reason="$1" sig="$2" excl="$3" out="$4"
    local patfile; patfile="$(mktemp)"
    echo "building subset manifest (reason=$reason${sig:+, signature=\"$sig\"}${excl:+, excl=$excl}) ..."
    REASON="$reason" SIG="$sig" EXCL="$excl" python - > "$patfile" <<'PY'
import json, os
reason = os.environ["REASON"]; sig = os.environ.get("SIG") or None
excl = {e for e in (os.environ.get("EXCL") or "").split(",") if e}
rej = os.path.join(os.environ["NOMAD_HARVEST_DATA"], "dataset", "rejections.jsonl")
seen = set()
for line in open(rej):
    line = line.strip()
    if not line:
        continue
    r = json.loads(line)
    if r.get("reason") != reason:
        continue
    if sig and sig not in str(r.get("detail", "")):
        continue
    eid = str(r["id"]).split(":")[1]
    if eid not in excl and eid not in seen:
        seen.add(eid)
        print(f'"recid": "{eid}"')   # exact JSON substring -> unambiguous grep -F pattern
PY
    echo "  target recids: $(wc -l < "$patfile")"
    shopt -s nullglob
    local parts=( "$PARTS_DIR"/*.fetched.jsonl )
    shopt -u nullglob
    if (( ${#parts[@]} == 0 )); then
        echo "ERROR: no part fetched manifests under $PARTS_DIR" >&2
        rm -f "$patfile"; exit 2
    fi
    : > "$out"
    grep -F -h -f "$patfile" "${parts[@]}" >> "$out" || true   # grep exits 1 on no match
    rm -f "$patfile"
    echo "  built $out: $(wc -l < "$out") records"
}

echo "=== recover-bigvasprun START $(date -Is) on $(hostname) ==="
quota 2>/dev/null || lfs quota -u "$USER" "$NOMAD_HARVEST_DATA" 2>/dev/null || true

if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

# ---- step 1: build the subset manifest (excludes the 8.51 GB monster) ------------
build_subset_manifest primary_too_large "" "dhOiawS2QOf-3IH1c7bYLS2RN4OR" "$FETCHED"
if [[ ! -s "$FETCHED" ]]; then
    echo "no records to re-parse (all already parsed?); exiting 0."
    exit 0
fi

# ---- step 2: parse uncapped (serial), into the existing dataset ------------------
# --max-primary-bytes 0 disables the cap -> ONLY the primary_too_large deferrals are re-attempted
# (terminal parse failures stay skipped via the rejection log; already-parsed calcs skip via resume).
echo "=== parse (uncapped, serial) $(date -Is) ==="
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

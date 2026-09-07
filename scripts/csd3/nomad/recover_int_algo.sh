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
# Recover the numeric-ALGO calcs (pymatgen 2026.5.4 bug): build the subset manifest HERE (fast
# grep over the part manifests), monkeypatch pymatgen + re-parse IN-PROCESS into the EXISTING
# dataset (recover_int_algo.py), then verify + purge-raw. Same pymatgen + parse/store as the 7M
# existing calcs (the patch only fixes the crash) -> consistent. No re-fetch (files still staged).
#
# NB the parse runs in-process (no --parse-timeout) so the monkeypatch applies. The set is scoped
# to the ALGO calcs (they failed FAST at __init__, they do not hang) and max_primary_bytes caps RAM.
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
FETCHED="$MAN/nomad_int_algo_fetched.jsonl"

# Fast subset-manifest build (see recover_bigvasprun.sh for the rationale): list target recids
# from the small dataset rejections.jsonl, then grep -F their fetched lines out of the part
# manifests WITHOUT json.loads-ing all ~7M lines. Args: $1=reason $2=signature $3=exclude-csv $4=out.
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
        print(f'"recid": "{eid}"')
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
    grep -F -h -f "$patfile" "${parts[@]}" >> "$out" || true
    rm -f "$patfile"
    echo "  built $out: $(wc -l < "$out") records"
}

echo "=== recover-int-algo START $(date -Is) on $(hostname) ==="
if [[ -e "$DATASET_DIR/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DATASET_DIR/.parse.lock" 2>/dev/null)"
    rm -f "$DATASET_DIR/.parse.lock"
fi

# ---- step 1: build the subset manifest -------------------------------------------
build_subset_manifest vasprun_parse_error "has no attribute 'lower'" "" "$FETCHED"
if [[ ! -s "$FETCHED" ]]; then
    echo "no records to re-parse (all already parsed?); exiting 0."
    exit 0
fi

# ---- step 2: monkeypatch + in-process parse --------------------------------------
echo "=== patch + parse $(date -Is) ==="
python scripts/csd3/nomad/recover_int_algo.py

echo "=== verify + purge-raw $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DATASET_DIR"
python -m zenodo_harvest.cli purge-raw --raw-dir "$RAW" --dataset-dir "$DATASET_DIR" \
    --fetched "$FETCHED"

echo "=== recover-int-algo DONE $(date -Is) ==="
echo "staged files under raw/: $(find "$RAW" -type f 2>/dev/null | wc -l)"

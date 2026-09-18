#!/bin/bash
#SBATCH -J zh-reparse-4272054
# Account is NOT hardcoded (keeps per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU     # find yours with: mybalance
#SBATCH -p icelake-himem               # 6760 MiB/core -> cores ARE the RAM budget
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16             # RAM = 16 x 6760 MiB = ~108 GiB. See RAM note below.
#SBATCH --time=06:00:00
#SBATCH -o logs/zh-reparse-4272054-%j.out
#SBATCH -e logs/zh-reparse-4272054-%j.err
#SBATCH --mail-type=END,FAIL
#
# ============================================================================================
# Recovery: re-parse the one worthwhile `primary_too_large` Zenodo vasprun into the dataset.
# ============================================================================================
# Record 4272054 ("Cooperative excitations within the superionic phase of PbF2", cc-zero) is ONE
# calc: `vasprun_re1_re26.xml.gz` — 4.85 GB uncompressed, **46,934 ionic frames** of PbF2 AIMD.
# The first harvest deferred it (`primary_too_large`) under the 2 GB `--max-primary-bytes` RAM cap;
# nothing about the file is wrong. Re-parsing it uncapped on a himem node recovers all 46,934 frames.
# It has 0 calcs in the dataset today, so parse simply APPENDS them (to fresh tail shards).
#
# WHY ONLY THIS RECORD (the other `primary_too_large` entries were evaluated and dropped):
#   * 20059197 (5x `magnetic_DOS/vasprun.xml.gz`, 2.69 GB each) are SINGLE-POINT DOS calcs =
#     5 frames total -> negligible for an 11.87M-frame dataset, not worth the RAM. Skipped.
#   * 15741825 (~2,640 `r2SCANY@r2SCANXs/OUTCAR_Y_*_X_*`) are `ALGO=Eigenval, NELM=1, IBRION=-1`
#     NON-self-consistent eigenvalue evaluations with **no TOTAL-FORCE block at all** (verified on
#     3 samples). No forces => useless as MLIP training data even with a perfect parser. Dropped
#     (this is why no new OUTCAR parser is being written).
#
# WHY `--parse-timeout 0` (in-process, NOT a timeout child): the timeout child returns frames by
# PICKLING them through a pipe; for a 46,934-frame calc that doubles memory and is slow. The file is
# known-complete (46,934 `<calculation>` blocks counted), so there is no hang to guard against;
# a generous 6 h wallclock covers a slow single parse.
#
# RAM note: peak ~= pymatgen Vasprun (~12 x 4.85 GB ~= 58 GiB) + the resume frame_id set
# (~1.2 GiB for the 11.87M committed ids) + the 46,934 ASE Atoms (~1-2 GiB) ~= ~62 GiB. 16 cores
# (~108 GiB) leaves ~1.7x headroom. PbF2 is closed-shell (ISPIN=1) and the unit has no OUTCAR, so
# the occupancy-method eigen parse does NOT trigger (no extra RAM).
#
# WRITES DIRECTLY INTO THE PRODUCTION DATASET (append-only to NEW shards; parse's resume/prune keeps
# it crash-safe — an interrupted run leaves at worst prunable orphan frames, never dangling metadata).
# A one-time metadata.jsonl backup is taken first (belt-and-braces). NOT self-resubmitting: it is a
# single parse of one calc; if wallclock is ever hit, just resubmit (resume skips the committed frames).
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs
#   sbatch scripts/csd3/48_reparse_bigvasprun.sh
# ============================================================================================
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# Activate the harvest env BEFORE `sbatch` (module load + venv). TMPDIR on node-local disk if present.
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

RECID="4272054"
ZH="$ZENODO_HARVEST_DATA"
MAN="$ZH/manifests"
RAW="$ZH/raw"
DS="$ZH/dataset"
SRC="$MAN/all_fetched.dedup.jsonl"          # the deduplicated fetched manifest (has 4272054)
FETCHED="$MAN/reparse_${RECID}.fetched.jsonl"
REJ="$MAN/reparse_${RECID}_rejections.jsonl"

echo "=== reparse-$RECID START $(date -Is) on $(hostname) ==="
lfs quota -u "$USER" "$ZH" 2>/dev/null || true

# 0. sanity: staged raw for the record must still be present (parse-only recovery, no re-fetch)
if [[ ! -f "$RAW/$RECID/extracted/vasprun_re1_re26.xml.gz" ]]; then
    echo "ERROR: staged vasprun missing: $RAW/$RECID/extracted/vasprun_re1_re26.xml.gz" >&2
    echo "       (raw/ was purged? this record needs a re-fetch then.)" >&2
    exit 2
fi

# 1. extract EXACTLY the record's fetched entry (exact recid match, not a substring grep)
RECID="$RECID" SRC="$SRC" FETCHED="$FETCHED" python - <<'PY'
import json, os, sys
recid, src, out = os.environ["RECID"], os.environ["SRC"], os.environ["FETCHED"]
n = 0
with open(out, "w") as fo:
    for line in open(src):
        line = line.strip()
        if not line:
            continue
        if json.loads(line).get("recid") == recid:
            fo.write(line + "\n"); n += 1
print(f"extracted {n} entry(ies) for {recid} -> {out}")
if n != 1:
    sys.exit(f"expected exactly 1 fetched entry for {recid}, got {n}")
PY

# 2. clear any stale parse lock (a prior job that died holding it)
if [[ -e "$DS/.parse.lock" ]]; then
    echo "clearing leftover parse lock: $(cat "$DS/.parse.lock" 2>/dev/null || true)"
    rm -f "$DS/.parse.lock"
fi

# 3. one-time metadata backup (belt-and-braces; parse only APPENDS, but cheap insurance)
if [[ ! -f "$DS/metadata.jsonl.bak.pre_reparse_${RECID}" ]]; then
    cp "$DS/metadata.jsonl" "$DS/metadata.jsonl.bak.pre_reparse_${RECID}"
    echo "backed up metadata.jsonl -> metadata.jsonl.bak.pre_reparse_${RECID}"
fi

# 4. parse UNCAPPED, in-process, directly into the existing dataset (appends the 46,934 frames)
echo "=== parse (uncapped, in-process) $(date -Is) ==="
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DS" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-primary-bytes 0 \
    --parse-timeout 0

# 5. verify the dataset integrity (streaming; scales to 11.9M+ frames)
echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DS"

echo "=== reparse-$RECID DONE $(date -Is) ==="
echo "record now in dataset:"
grep -c "\"record_id\": \"$RECID\"" "$DS/metadata.jsonl" || true

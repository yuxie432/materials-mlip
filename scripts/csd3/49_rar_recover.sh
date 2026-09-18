#!/bin/bash
#SBATCH -J zh-rar-recover
# Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core; parse of unknown-size vaspruns wants headroom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8              # ~54 GiB: resume frame_id set (~2 GiB) + one parse (<= ~3 GB cap x ~12)
#SBATCH --time=12:00:00               # dominated by the 17.3 GB download (17522462); generous
#SBATCH -o logs/zh-rar-recover-%j.out
#SBATCH -e logs/zh-rar-recover-%j.err
#SBATCH --mail-type=END,FAIL
#
# ============================================================================================
# Recover the RAR bucket: 14 DFT records whose .rar archives failed extraction in the first
# harvest ONLY because no unrar/bsdtar binary was on PATH (logged `extract_error: RarCannotExec`).
# The data was never corrupt/absent — just un-extracted. With a static `unrar` now in ~/bin,
# rarfile extracts them; this re-fetches + parses them into the dataset.
# ============================================================================================
# The 14 targets (all cc-by; ~23.7 GB, 17522462 alone is 17.3 GB; NON-DFT rar records — a clinical
# dose-calc set, a luminescence spectroscopy set, and a vague "Raw data" one — are deliberately
# EXCLUDED). Two are inputs-only (18243741 coords, 21341359 POSCARs) and will yield no frames
# (no vasprun/OUTCAR) — harmless (they log `no_calc_units`). 13744522 is already PARTLY in the
# dataset (its non-rar archives parsed before; the .rar failed) — parse's resume skips its committed
# calcs and adds only the rar-derived ones.
#
# MECHANISM: fetch (whole .rar download + selective VASP extraction; the archive is deleted after)
# into a DEDICATED raw_rar/ dir, then parse DIRECTLY into the existing dataset (parse's resume skips
# any already-committed calc_id and appends new calcs to fresh tail shards — no merge needed), then
# verify. Isolated raw dir + rejections file, so the main harvest's raw/ and logs are untouched.
# A one-time metadata backup is taken before parse.
#
# REQUIRES the unrar backend on PATH (this script prepends ~/bin, where the static RARLAB unrar was
# installed). Fails fast if rarfile can't find a backend.
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs && sbatch scripts/csd3/49_rar_recover.sh
# Fully resumable: re-`sbatch` if wallclock is hit (fetch skips staged records, parse skips committed).
# ============================================================================================
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
export PATH="$HOME/bin:$PATH"          # so the compute job sees the static unrar in ~/bin
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"
# --------------------------------------------------------------------------------

ZH="$ZENODO_HARVEST_DATA"
MAN="$ZH/manifests"; DS="$ZH/dataset"; RAW="$ZH/raw_rar"
KEEP="$MAN/keep.jsonl"
RARKEEP="$MAN/rar_recover_keep.jsonl"
FETCHED="$MAN/rar_recover_fetched.jsonl"
REJ="$MAN/rar_recover_rejections.jsonl"

# The 14 RAR-blocked DFT records (see the header; derived from the RarCannotExec rejections,
# minus the 3 non-DFT ones).
RECIDS="17522462 20404673 17984449 12810613 13744522 16748752 20835365 19602057 17346906 7029224 20179031 21538545 18243741 21341359"

echo "=== rar-recover START $(date -Is) on $(hostname) ==="
lfs quota -u "$USER" "$ZH" 2>/dev/null || true

# 0. fail fast unless rarfile has a working backend binary
python - <<'PY'
import rarfile, shutil, sys
rarfile.tool_setup()  # raises if no unrar/unar/bsdtar/7z on PATH
print("rar backend OK:", shutil.which("unrar") or shutil.which("unar") or shutil.which("bsdtar") or "(rarfile-detected)")
PY

# 1. build the keeplist: exact-recid extraction from keep.jsonl (not a substring grep)
RECIDS="$RECIDS" KEEP="$KEEP" RARKEEP="$RARKEEP" python - <<'PY'
import json, os
want = set(os.environ["RECIDS"].split())
seen = set()
with open(os.environ["RARKEEP"], "w") as fo:
    for line in open(os.environ["KEEP"]):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        r = str(d.get("recid"))
        if r in want and r not in seen:
            seen.add(r); fo.write(line + "\n")
missing = sorted(want - seen)
print(f"keeplist: {len(seen)}/{len(want)} records", (f"(MISSING {missing})" if missing else ""))
if missing:
    raise SystemExit(f"records not found in keep.jsonl: {missing}")
PY

if [[ -e "$DS/.parse.lock" ]]; then echo "clearing stale parse lock"; rm -f "$DS/.parse.lock"; fi

# 2. fetch: whole .rar download + selective VASP extraction into the dedicated raw_rar/ dir.
#    Disk valve is a generous backstop (this is ~24 GB; it should never trip) that also bounds the
#    iron-ML-DB inode count. --workers 3 overlaps the small downloads with the 17 GB one.
echo "=== fetch $(date -Is) ==="
python -m zenodo_harvest.cli fetch \
    --in "$RARKEEP" \
    --out "$FETCHED" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-bytes 0 \
    --workers 3 \
    --max-disk-bytes 300000000000 \
    --max-disk-files 400000

# 3. parse directly into the existing dataset (resume skips 13744522's committed calcs).
echo "=== parse $(date -Is) ==="
if [[ ! -f "$DS/metadata.jsonl.bak.pre_rar_recover" ]]; then
    cp "$DS/metadata.jsonl" "$DS/metadata.jsonl.bak.pre_rar_recover"
    echo "backed up metadata.jsonl -> metadata.jsonl.bak.pre_rar_recover"
fi
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DS" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-primary-bytes 3000000000 \
    --parse-timeout 1200

# 4. verify
echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DS"

echo "=== rar-recover DONE $(date -Is) ==="
echo "records from the RAR bucket now represented in the dataset:"
RECIDS="$RECIDS" DS="$DS" python - <<'PY'
import json, os, collections
want = set(os.environ["RECIDS"].split())
c = collections.Counter(); f = collections.Counter()
for line in open(os.path.join(os.environ["DS"], "metadata.jsonl")):
    d = json.loads(line); r = d["provenance"]["record_id"]
    if r in want:
        c[r] += 1; f[r] += (d.get("quality", {}) or {}).get("n_frames") or len(d.get("frame_ids", []))
for r in sorted(want):
    print(f"  {r}: {c[r]:5d} calcs / {f[r]:8d} frames" + ("" if c[r] else "   (no VASP recovered)"))
print(f"  TOTAL: {sum(c.values())} calcs / {sum(f.values())} frames across {sum(1 for r in want if c[r])} records")
PY
echo "NOTE: after verifying the yield, purge with:  python -m zenodo_harvest.cli purge-raw --raw-dir $RAW --dataset-dir $DS --fetched $FETCHED"

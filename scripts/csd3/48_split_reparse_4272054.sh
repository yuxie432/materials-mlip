#!/bin/bash
#SBATCH -J zh-split-4272054
# Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake                     # segments are small (~180 MB uncompressed each) -> no himem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # ~13.5 GiB: resume frame_id set (~2 GiB) + one small segment
#SBATCH --time=04:00:00
#SBATCH -o logs/zh-split-4272054-%j.out
#SBATCH -e logs/zh-split-4272054-%j.err
#SBATCH --mail-type=END,FAIL
#
# ============================================================================================
# Recover the FULL 4272054 PbF2-AIMD trajectory: split a CONCATENATED vasprun, then re-parse.
# ============================================================================================
# `vasprun_re1_re26.xml` is 27 concatenated, individually-TRUNCATED vasprun segments
# (probe: 27x `<?xml` / 27x `<modeling` / 0x `</modeling>`; 46,934 `<calculation>` vs 46,907
# `</calculation>` = one unclosed final calc per segment). A valid vasprun has ONE `<modeling>`
# root, so pymatgen (exception_on_bad_xml=False) parses only the FIRST root -> 499 frames, silently
# ignoring segments 2..27. An earlier plain uncapped re-parse therefore stored just 499 of ~46,907
# (leaving metadata.jsonl.bak.pre_reparse_4272054, which step 1 below uses to revert those 499).
#
# This script does the whole thing (idempotent):
#   1. REVERT the 499-frame partial calc: restore metadata from the pre-reparse backup FIRST
#      (atomic os.replace), THEN delete the shard(s) that calc alone owned. Restore-before-delete is
#      crash-safe: an interruption can only ever leave a harmless ORPHAN shard on disk, never a
#      dangling metadata ref. Skipped automatically if already reverted (old calc_id absent).
#   2. SPLIT the file at `<?xml` boundaries into 27 segment vaspruns, STREAMING gz->gz (no big
#      intermediate), each into raw/4272054/extracted/split_re/reNN/vasprun.xml.gz. Each keeps the full
#      header (<incar>/<parameters>/<atominfo>) so calc_parameters parse to parity; the truncated tail
#      is handled by exception_on_bad_xml=False.
#   3. PARSE the 27 units into the dataset (uncapped) -> ~46,907 frames as 27 calc_ids
#      `zenodo:4272054:split_re/reNN/vasprun.xml.gz`. 4. VERIFY.
# The old 1.28 GB concatenated file stays staged (harmless) until the next purge-raw.
#
# ALL scratch/temp lives on /rds (never /tmp or /local) — the compute node's /tmp is small and filled
# a prior run at ENOSPC. NOTE: 27 AIMD restart segments of ONE PbF2 system = HIGHLY correlated frames
# (deep but narrow); subsample at training time.
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs && sbatch scripts/csd3/48_split_reparse_4272054.sh
# ============================================================================================
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
# Keep ALL temp on /rds (the node's /tmp is small; /local may be tiny/absent). ~826 GB free here.
export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"

RECID="4272054"
ZH="$ZENODO_HARVEST_DATA"
MAN="$ZH/manifests"; RAW="$ZH/raw"; DS="$ZH/dataset"
SRC="$MAN/all_fetched.dedup.jsonl"
BIG="$RAW/$RECID/extracted/vasprun_re1_re26.xml.gz"
DEST="$RAW/$RECID/extracted/split_re"
FETCHED="$MAN/split_${RECID}.fetched.jsonl"
REJ="$MAN/split_${RECID}_rejections.jsonl"
BAK="$DS/metadata.jsonl.bak.pre_reparse_${RECID}"
OLD_CALC_ID="zenodo:${RECID}:vasprun_re1_re26.xml.gz"

echo "=== split-reparse $RECID START $(date -Is) on $(hostname) ==="
df -h "$ZH" 2>/dev/null | tail -1 || true
[[ -f "$BIG" ]] || { echo "ERROR: source missing: $BIG" >&2; exit 2; }
if [[ -e "$DS/.parse.lock" ]]; then echo "clearing stale lock"; rm -f "$DS/.parse.lock"; fi

# ---- step 1: REVERT the 499-frame partial parse (idempotent, crash-safe, all on /rds) --------
if grep -q "\"calc_id\": \"$OLD_CALC_ID\"" "$DS/metadata.jsonl"; then
    [[ -f "$BAK" ]] || { echo "ERROR: revert needs backup $BAK (from the earlier plain re-parse)" >&2; exit 2; }
    echo "reverting the 499-frame partial calc ($OLD_CALC_ID) ..."
    DS="$DS" RECID="$RECID" BAK="$BAK" python - <<'PY'
import json, os, shutil
ds, recid, bak = os.environ["DS"], os.environ["RECID"], os.environ["BAK"]
meta = os.path.join(ds, "metadata.jsonl")
mine, others = set(), set()
for line in open(meta):
    d = json.loads(line)
    (mine if d["provenance"]["record_id"] == recid else others).update(d.get("shards", []))
overlap = mine & others
assert not overlap, f"refusing to delete shards shared with other records: {sorted(overlap)}"
# (1) restore metadata FIRST (atomic replace) so a crash can only orphan a shard, never dangle a ref
tmp = meta + ".revert.tmp"
shutil.copy(bak, tmp)
os.replace(tmp, meta)
print(f"  restored metadata.jsonl from {os.path.basename(bak)}")
# (2) delete the now-unreferenced shard(s) the 499-frame calc alone owned
for s in sorted(mine):
    p = os.path.join(ds, s)
    if os.path.exists(p):
        os.remove(p); print("  deleted orphaned shard", s)
print(f"  reverted {recid}: {len(mine)} shard(s) removed")
PY
    echo "=== verify (post-revert, should be the pre-reparse state) ==="
    python -m zenodo_harvest.cli verify --dataset-dir "$DS"
else
    echo "revert already done ($OLD_CALC_ID absent from metadata); skipping."
fi

# ---- step 2: STREAM-split the concatenated file into 27 gz segment vaspruns (no big intermediate) --
echo "=== stream-splitting $BIG at <?xml boundaries $(date -Is) ==="
rm -rf "$DEST"; mkdir -p "$DEST"
BIG="$BIG" DEST="$DEST" python - <<'PY'
import gzip, os
big, dest = os.environ["BIG"], os.environ["DEST"]
n, out = -1, None
with gzip.open(big, "rt", errors="replace") as fh:
    for line in fh:
        if "<?xml" in line:
            n += 1
            if out is not None:
                out.close()
            d = os.path.join(dest, f"re{n:02d}"); os.makedirs(d, exist_ok=True)
            out = gzip.open(os.path.join(d, "vasprun.xml.gz"), "wt")
        if out is None:                      # content before the first <?xml (not expected)
            n = 0; d = os.path.join(dest, "re00"); os.makedirs(d, exist_ok=True)
            out = gzip.open(os.path.join(d, "vasprun.xml.gz"), "wt")
        out.write(line)
if out is not None:
    out.close()
print(f"wrote {n + 1} segment vaspruns to {dest}")
if n + 1 < 2:
    raise SystemExit(f"expected >=2 segments, got {n + 1}")
PY

# ---- step 3: build a fetched manifest (reuse the record's real provenance) -------------------
RECID="$RECID" SRC="$SRC" RAW="$RAW" DEST="$DEST" FETCHED="$FETCHED" python - <<'PY'
import json, os, glob
recid, src, raw, dest, out = (os.environ[k] for k in ("RECID", "SRC", "RAW", "DEST", "FETCHED"))
orig = None
for line in open(src):
    if json.loads(line).get("recid") == recid:
        orig = json.loads(line); break
assert orig, f"no original fetched entry for {recid} in {src}"
units = []
for d in sorted(glob.glob(os.path.join(dest, "re*"))):
    vp = os.path.join(d, "vasprun.xml.gz")
    if os.path.isfile(vp):
        units.append({"dir": os.path.relpath(d, raw), "vasprun": os.path.relpath(vp, raw)})
assert units, "no split units found"
orig["calc_units"] = units
orig["n_calc_units"] = len(units)
orig.pop("availability_files", None)   # was aligned to the single old unit; record-level availability kept
with open(out, "w") as fo:
    fo.write(json.dumps(orig) + "\n")
print(f"built {out} with {len(units)} split calc-units")
PY

# ---- step 4: parse the 27 segments into the dataset, then verify -----------------------------
echo "=== parse (uncapped) $(date -Is) ==="
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DS" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-primary-bytes 0 \
    --parse-timeout 600

echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DS"

echo "=== split-reparse $RECID DONE $(date -Is) ==="
RECID="$RECID" DS="$DS" python - <<'PY'
import json, os
recid = os.environ["RECID"]
n = c = 0
for line in open(os.path.join(os.environ["DS"], "metadata.jsonl")):
    d = json.loads(line)
    if d["provenance"]["record_id"] == recid:
        c += 1; n += (d.get("quality", {}) or {}).get("n_frames") or len(d.get("frame_ids", []))
print(f"  {recid} now in dataset: {c} calcs / {n} frames")
PY

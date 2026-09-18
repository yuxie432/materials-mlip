#!/bin/bash
#SBATCH -J zh-split-4272054
# Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake                     # segments are small (~180 MB each) -> no himem needed
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
# root, so pymatgen (exception_on_bad_xml=False) parsed only the FIRST root -> 499 frames, silently
# ignoring segments 2..27. An earlier plain uncapped re-parse therefore stored just 499 of ~46,907
# (leaving metadata.jsonl.bak.pre_reparse_4272054, which step 1 below uses to revert those 499).
#
# This script (user chose REVERT + reparse for clean, consistent calc_ids):
#   1. REVERT the 499-frame partial calc: delete the shard(s) it created (verified 4272054-only)
#      and restore metadata.jsonl from the pre-reparse backup. Idempotent (guarded on the old
#      concatenated-file calc_id still being present).
#   2. SPLIT the file at `<?xml` boundaries into 27 segment vaspruns, gzip each into its own
#      calc-unit dir under raw/4272054/extracted/split_re/reNN/vasprun.xml.gz. Each segment keeps
#      the full header (<incar>/<parameters>/<atominfo>) so calc_parameters parse to full parity;
#      the truncated tail is handled by exception_on_bad_xml=False (recovers the complete calcs).
#   3. PARSE the 27 units into the dataset (uncapped) -> ~46,907 frames as 27 calc_ids
#      `zenodo:4272054:split_re/reNN/vasprun.xml.gz`. 4. VERIFY.
# The old 1.28 GB concatenated file stays staged (harmless) until the next purge-raw.
#
# NOTE: 27 AIMD restart segments of ONE PbF2 system = HIGHLY correlated frames (deep but narrow);
# subsample at training time. This is the biggest single-record lever in the recovery sweep.
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs && sbatch scripts/csd3/48_split_reparse_4272054.sh
# ============================================================================================
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
if [[ -d /local && -w /local ]]; then export TMPDIR="/local"; else export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; fi
mkdir -p "$TMPDIR"
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
[[ -f "$BIG" ]] || { echo "ERROR: source missing: $BIG" >&2; exit 2; }

if [[ -e "$DS/.parse.lock" ]]; then echo "clearing stale lock"; rm -f "$DS/.parse.lock"; fi

# ---- step 1: REVERT the 499-frame partial parse (idempotent) --------------------------------
if grep -q "\"calc_id\": \"$OLD_CALC_ID\"" "$DS/metadata.jsonl"; then
    [[ -f "$BAK" ]] || { echo "ERROR: revert needs backup $BAK (from the earlier plain re-parse)" >&2; exit 2; }
    echo "reverting the 499-frame partial calc ($OLD_CALC_ID) ..."
    DS="$DS" RECID="$RECID" python - <<'PY'
import json, os
ds = os.environ["DS"]; recid = os.environ["RECID"]
mine, others = set(), set()
for line in open(os.path.join(ds, "metadata.jsonl")):
    d = json.loads(line)
    (mine if d["provenance"]["record_id"] == recid else others).update(d.get("shards", []))
overlap = mine & others
assert not overlap, f"refusing to delete shards shared with other records: {sorted(overlap)}"
with open("/tmp/_revert_shards.txt", "w") as fo:
    for s in sorted(mine):
        fo.write(s + "\n")
print(f"  {recid} owns {len(mine)} shard(s) to delete (none shared): {sorted(mine)}")
PY
    while read -r s; do [[ -n "$s" ]] && rm -f "$DS/$s" && echo "  deleted shard $s"; done < /tmp/_revert_shards.txt
    rm -f /tmp/_revert_shards.txt
    cp "$BAK" "$DS/metadata.jsonl"
    echo "  restored metadata.jsonl from $BAK"
    echo "=== verify (post-revert, should be the pre-reparse state) ==="
    python -m zenodo_harvest.cli verify --dataset-dir "$DS"
else
    echo "revert already done ($OLD_CALC_ID absent from metadata); skipping."
fi

# ---- step 2: SPLIT the concatenated file into 27 segment vaspruns ---------------------------
echo "=== splitting $BIG at <?xml boundaries $(date -Is) ==="
SPLIT="$TMPDIR/${RECID}_split"; rm -rf "$SPLIT"; mkdir -p "$SPLIT"
# each segment starts at a `<?xml` line; write each into re<NN>.xml
zcat -f "$BIG" | awk -v out="$SPLIT" '
    /<\?xml/ { n++; f=sprintf("%s/re%02d.xml", out, n) }
    { if (f=="") { n=0; f=sprintf("%s/re00.xml", out) } print > f }'
nseg=$(ls "$SPLIT"/re*.xml 2>/dev/null | wc -l)
echo "  wrote $nseg segment files"
[[ "$nseg" -ge 2 ]] || { echo "ERROR: expected >=2 segments, got $nseg" >&2; exit 3; }

rm -rf "$DEST"; mkdir -p "$DEST"
for x in "$SPLIT"/re*.xml; do
    b=$(basename "$x" .xml)                     # re00, re01, ...
    mkdir -p "$DEST/$b"
    gzip -c "$x" > "$DEST/$b/vasprun.xml.gz"
done
rm -rf "$SPLIT"
echo "  staged $nseg split calc-units under $DEST"

# ---- step 3: build a fetched manifest (reuse the record's real provenance) ------------------
RECID="$RECID" SRC="$SRC" RAW="$RAW" DEST="$DEST" FETCHED="$FETCHED" python - <<'PY'
import json, os, glob
recid, src, raw, dest, out = (os.environ[k] for k in ("RECID","SRC","RAW","DEST","FETCHED"))
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
orig.pop("availability_files", None)   # was aligned to the single old unit; drop (record-level availability kept)
with open(out, "w") as fo:
    fo.write(json.dumps(orig) + "\n")
print(f"built {out} with {len(units)} split calc-units")
PY

# ---- step 4: parse the 27 segments into the dataset, then verify ----------------------------
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
echo "4272054 frames now in dataset:"
python - <<PY
import json
n=sum((json.loads(l)['quality'].get('n_frames') or 0) for l in open("$DS/metadata.jsonl")
      if json.loads(l)['provenance']['record_id']=="$RECID")
c=sum(1 for l in open("$DS/metadata.jsonl") if json.loads(l)['provenance']['record_id']=="$RECID")
print(f"  {c} calcs / {n} frames")
PY

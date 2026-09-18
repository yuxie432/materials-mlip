#!/bin/bash
#SBATCH -J zh-fetch-10579527
# Before sbatch:  export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
#SBATCH -p icelake-himem               # 6760 MiB/core; defect-relaxation vaspruns, headroom for parse
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4              # ~27 GiB: resume frame_id set (~2 GiB) + one parse (<=2 GB cap x ~12)
#SBATCH --time=08:00:00               # 20.6 GB tar download + extract + parse many defect calcs
#SBATCH -o logs/zh-fetch-10579527-%j.out
#SBATCH -e logs/zh-fetch-10579527-%j.err
#SBATCH --mail-type=END,FAIL
#
# ============================================================================================
# APPROACH 2 — targeted by-ID harvest of record 10579527 (mentor-requested).
# ============================================================================================
# "Dataset for Machine-learning structural reconstructions for accelerated point defect
# calculations" — access_right=OPEN but license=None, so the license gate dropped it at DISCOVER
# and it is in no manifest. The mentor pointed to it as a valuable enrichment. 4 files / ~20.7 GB:
# defect_relaxations.tar.gz (20.6 GB, the payload = real VASP defect-relaxation trajectories),
# bulk_primitive_folders.tar.gz, vac_chalcogenides_...tar.gz, parsing_functions.py.
#
# MECHANISM: build the keep-list entry BY ID (client.get_record -> Candidate.from_record -> to_dict),
# which bypasses the discover-only license gate (fetch/parse never re-gate on license; access is open
# so no 403). It is then the ORDINARY fetch -> parse -> store path, so the record gets the EXACT SAME
# field-level detail as every other record: full calc_parameters (run_type/functional/INCAR/POTCAR/
# k-points/potcar_set_hash), quality (per-frame + calc-level convergence), per-calc availability,
# the `electronic` net moment/charge block, per-frame scf_dE/electronic_converged, and REF_energy/
# REF_forces/REF_stress. The ONLY provenance difference is `license: null` (the record has none) —
# recorded faithfully; this is a deliberate, mentor-endorsed inclusion of a no-license open record.
#
# raw/ was rm -rf'd (empty) so this stages there cleanly; ~886 GB / ~993k inodes free. New record
# (0 calcs in the dataset today) -> all-new calc_ids `zenodo:10579527:...`, no duplicate risk;
# parse's resume makes a re-run idempotent anyway.
#
# Run:
#   export SBATCH_ACCOUNT=<...>
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   mkdir -p logs && sbatch scripts/csd3/51_fetch_10579527.sh
# ============================================================================================
set -euo pipefail

export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export TMPDIR="$ZENODO_HARVEST_DATA/tmp"; mkdir -p "$TMPDIR"
cd "${SLURM_SUBMIT_DIR:-.}"   # repo root, where .env with ZENODO_TOKEN lives

RECID="10579527"
ZH="$ZENODO_HARVEST_DATA"
MAN="$ZH/manifests"; DS="$ZH/dataset"; RAW="$ZH/raw"
KEEP="$MAN/byid_${RECID}_keep.jsonl"
FETCHED="$MAN/byid_${RECID}_fetched.jsonl"
REJ="$MAN/byid_${RECID}_rejections.jsonl"

echo "=== byid-fetch $RECID START $(date -Is) on $(hostname) ==="
df -h "$ZH" 2>/dev/null | tail -1 || true
if [[ -e "$DS/.parse.lock" ]]; then echo "clearing stale lock"; rm -f "$DS/.parse.lock"; fi

PRE=$(RECID="$RECID" DS="$DS" python3 -c "import json,os;print(sum(1 for l in open(os.path.join(os.environ['DS'],'metadata.jsonl')) if json.loads(l)['provenance']['record_id']==os.environ['RECID']))")
echo "  $RECID already in dataset: $PRE calcs (expected 0 — it's a new record)"

# 1. build the keep-list entry BY ID (bypasses the discover license gate)
RECID="$RECID" KEEP="$KEEP" python - <<'PY'
import json, os
from zenodo_harvest import config
config.load_dotenv()                       # ZENODO_TOKEN (optional; record is open-access)
from zenodo_harvest.client import ZenodoClient
from zenodo_harvest.models import Candidate
recid = os.environ["RECID"]
rec = ZenodoClient().get_record(recid)
cand = Candidate.from_record(rec)
d = cand.to_dict()
assert str(d.get("recid")) == recid, f"recid mismatch: {d.get('recid')}"
assert d.get("files"), "record has no files"
missing = [f.get("key") for f in d["files"] if not f.get("download")]
assert not missing, f"files without a download URL (fetch would skip them): {missing}"
with open(os.environ["KEEP"], "w") as fo:
    fo.write(json.dumps(d) + "\n")
arch = [f["key"] for f in d["files"] if str(f.get("ext", "")).lower() in
        ("zip", "gz", "tgz", "tar", "bz2", "xz", "7z", "rar", "zst")]
print(f"  built keep entry: recid={d['recid']} license={d.get('license')!r} "
      f"access_right={d.get('access_right')!r} resource_type={d.get('resource_type')!r}")
print(f"  files={len(d['files'])} bytes_total={d.get('bytes_total')} category={d.get('vasp_category')}")
print(f"  archives={arch}")
PY

# 2. fetch (whole .tar.gz download + selective VASP extraction) into the (empty) raw/ dir
echo "=== fetch $(date -Is) ==="
python -m zenodo_harvest.cli fetch \
    --in "$KEEP" \
    --out "$FETCHED" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-bytes 0 \
    --max-disk-bytes 300000000000 \
    --max-disk-files 400000

# 3. parse into the dataset (standard path = full field parity), then verify
echo "=== parse $(date -Is) ==="
if [[ ! -f "$DS/metadata.jsonl.bak.pre_byid_${RECID}" ]]; then
    cp "$DS/metadata.jsonl" "$DS/metadata.jsonl.bak.pre_byid_${RECID}"
    echo "backed up metadata.jsonl -> metadata.jsonl.bak.pre_byid_${RECID}"
fi
python -m zenodo_harvest.cli parse \
    --in "$FETCHED" \
    --dataset-dir "$DS" \
    --raw-dir "$RAW" \
    --rejections "$REJ" \
    --max-primary-bytes 2000000000 \
    --parse-timeout 1200

echo "=== verify $(date -Is) ==="
python -m zenodo_harvest.cli verify --dataset-dir "$DS"

echo "=== byid-fetch $RECID DONE $(date -Is) ==="
RECID="$RECID" DS="$DS" python - <<'PY'
import json, os
recid = os.environ["RECID"]
n = c = 0
for line in open(os.path.join(os.environ["DS"], "metadata.jsonl")):
    d = json.loads(line)
    if d["provenance"]["record_id"] == recid:
        c += 1; n += (d.get("quality", {}) or {}).get("n_frames") or len(d.get("frame_ids", []))
print(f"  {recid} now in dataset: {c} calcs / {n} frames  (license recorded as null — no-license open record)")
PY
echo "NOTE: after verifying yield, reclaim scratch with:  python -m zenodo_harvest.cli purge-raw --raw-dir $RAW --dataset-dir $DS --fetched $FETCHED"

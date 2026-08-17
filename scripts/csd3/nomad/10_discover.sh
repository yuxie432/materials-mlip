#!/bin/bash
#SBATCH -J nomad-discover
# Account is NOT hardcoded (keeps per-machine/per-project accounts out of git). Before sbatch:
#   export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU   # find yours with: mybalance; propagates to resubmits
#SBATCH -p icelake
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1              # keyset scan is single-stream + self-throttled: one core
#SBATCH --time=02:00:00                # the whole 7.1M keyset is ~700 requests (minutes); a cap is less
#SBATCH -o logs_nomad/nomad-discover-%j.out
#SBATCH -e logs_nomad/nomad-discover-%j.err
#SBATCH --mail-type=END,FAIL           # email on job END/FAIL; SBATCH_MAIL_USER overrides the address
#
# Stage 0-1 (NOMAD): keyset-scan the direct-upload VASP+DFT query -> a license-gated,
# Zenodo-deduped keep-list for the fetch pipeline. NOMAD's indexed metadata collapses the
# Zenodo discover+triage funnel: no keyword recall, no zip-peek. Needs outbound HTTPS (see
# ../00_check_network.sh — verified on icelake). Single-stream by design: NOMAD's search
# endpoint self-throttles (~30 req/s floor, 5xx under load), so do NOT parallelise it.
#
# NB: create logs_nomad/ BEFORE you submit (SLURM opens -o/-e before the body runs): `mkdir -p logs_nomad`.
set -euo pipefail

# ---- ENV SETUP (edit me) --------------------------------------------------------
# ZENODO_HARVEST_DATA is the Zenodo scratch root — NOMAD reads its dataset/metadata.jsonl for
# cross-source dedup, so keep pointing it at the Zenodo tree. NOMAD's OWN raw/manifests/dataset
# live under a SEPARATE sibling root so the two harvests never share a staging tree (a shared
# raw dir would let each harvest's disk valve count the other's files). Both are read at IMPORT
# time, so set them before python starts.
export ZENODO_HARVEST_DATA="${ZENODO_HARVEST_DATA:-/rds/user/$USER/hpc-work/zenodo}"
export NOMAD_HARVEST_DATA="${NOMAD_HARVEST_DATA:-/rds/user/$USER/hpc-work/nomad}"
# Activate the harvest env BEFORE `sbatch` (sbatch captures your submit env by default):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
cd "${SLURM_SUBMIT_DIR:-.}"      # repo root
# --------------------------------------------------------------------------------

# ---- SCOPE (bounded-sample; edit me) --------------------------------------------
# The full direct-upload set is ~7.1M entries — far more than one CSD3 quota/job (see
# docs/NOMAD_HARVEST.md). MAX_ENTRIES caps the keep-list: a keyset scan is ordered by the
# entry_id hash, so a cap is a DIVERSE spread across uploads, not the first upload only.
# Raise it (or set to a huge number) to widen the sample; combine with ELEMENTS to focus.
# The full run is ~1.5-3 days (targeted pre-packed-zip fetch, docs/NOMAD_HARVEST.md §3), so a
# cap is OPTIONAL — an early checkpoint, not a requirement.
MAX_ENTRIES="${MAX_ENTRIES:-200000}"   # cap the keep-list; set EMPTY or 0 for the FULL 7.1M run
ELEMENTS="${ELEMENTS:-}"               # e.g. "Ti O" -> only materials containing ALL of these
# --------------------------------------------------------------------------------

mkdir -p logs_nomad "$NOMAD_HARVEST_DATA/manifests"
MAN="$NOMAD_HARVEST_DATA/manifests"
ELEM_ARG=()
[[ -n "$ELEMENTS" ]] && ELEM_ARG=(--elements $ELEMENTS)
# MAX_ENTRIES empty/0 -> omit the flag entirely (unbounded: the whole ~7.1M direct-upload set).
CAP_ARG=()
[[ -n "$MAX_ENTRIES" && "$MAX_ENTRIES" != "0" ]] && CAP_ARG=(--max-entries "$MAX_ENTRIES")

echo "=== nomad discover $(date -Is) on $(hostname): max_entries='${MAX_ENTRIES:-ALL}' elements='${ELEMENTS:-any}' ==="
echo "    NOMAD tree: $NOMAD_HARVEST_DATA   (dedup against $ZENODO_HARVEST_DATA/dataset/metadata.jsonl)"
# discover applies the license gate (keep only redistributable CC-BY/CC0/…) and dedups
# against the Zenodo dataset's metadata.jsonl inline; every drop is logged to
# manifests/nomad_rejections.jsonl (auditable recall). Resumable: entries already in the
# keep-list are skipped, so a re-run continues where an interrupted scan stopped.
python -m nomad_harvest.cli -v discover \
    --out "$MAN/nomad_keep.jsonl" \
    "${CAP_ARG[@]}" \
    "${ELEM_ARG[@]}" \
    --zenodo-metadata "$ZENODO_HARVEST_DATA/dataset/metadata.jsonl"

echo "=== done $(date -Is): $(wc -l < "$MAN/nomad_keep.jsonl") entries to fetch ==="
